"""
Portiere Polars Engine — Lightweight compute engine using Polars.

Polars is the default engine for Portiere, suitable for:
- Local development
- Small to medium datasets (up to a few GB)
- Fast iteration
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import structlog

from portiere.engines.base import AbstractEngine

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

logger = structlog.get_logger(__name__)


class PolarsEngine(AbstractEngine):
    """
    Polars-based compute engine.

    This is the default, lightweight engine that works on a single machine.
    For larger datasets, use SparkEngine.

    Example:
        from portiere.engines import PolarsEngine

        engine = PolarsEngine()
        df = engine.read_source("/data/*.csv")
        profile = engine.profile(df)
    """

    def __init__(self) -> None:
        """Initialize Polars engine."""
        try:
            import polars as pl

            self._pl = pl
        except ImportError:
            raise ImportError(
                "Polars is required for PolarsEngine. Install with: pip install portiere-health[polars]"
            )

        logger.info("PolarsEngine initialized")

    @property
    def engine_name(self) -> str:
        return "polars"

    def read_source(
        self,
        path: str,
        format: str = "csv",
        options: dict[str, Any] | None = None,
    ) -> pl.DataFrame:
        """Read source data from files."""
        options = options or {}

        path_obj = Path(path)
        if "*" in path:
            # Glob pattern
            if format == "csv":
                return self._pl.read_csv(path, **options)
            elif format == "parquet":
                return self._pl.read_parquet(path, **options)
            elif format == "json":
                return self._pl.read_json(path, **options)
        else:
            # Single file
            if format == "csv":
                return self._pl.read_csv(path_obj, **options)
            elif format == "parquet":
                return self._pl.read_parquet(path_obj, **options)
            elif format == "json":
                return self._pl.read_json(path_obj, **options)

        raise ValueError(f"Unsupported format: {format}")

    def profile(self, df: pl.DataFrame, *, empty_as_missing: bool = True) -> dict[str, Any]:
        """Profile a DataFrame.

        In addition to the base column stats, each column carries report
        enrichment keys used by the profile-report exporter: ``present_count``
        (non-null and, for string columns when ``empty_as_missing`` is set,
        non-empty-after-strip), ``min_len``/``max_len`` (char length over
        present string values), and ``example`` (first present value).
        """
        pl = self._pl
        columns = []
        for col in df.columns:
            col_data = df[col]
            dtype = str(col_data.dtype)

            profile = {
                "name": col,
                "type": dtype,
                "nullable": col_data.null_count() > 0,
                "null_count": col_data.null_count(),
                "null_pct": col_data.null_count() / len(df) * 100 if len(df) > 0 else 0,
            }

            # Cardinality for low-cardinality columns
            n_unique = col_data.n_unique()
            profile["n_unique"] = n_unique

            # Top values for categorical-like columns
            if n_unique <= 100 or dtype in ("Utf8", "Categorical"):
                top_values = df.group_by(col).len().sort("len", descending=True).head(10).to_dicts()
                profile["top_values"] = top_values

            # Report enrichment: present count, value length range, example
            if col_data.dtype in (pl.Utf8, pl.Categorical):
                s_str = col_data.cast(pl.Utf8)
                non_null = s_str.filter(s_str.is_not_null())
                present = (
                    non_null.filter(non_null.str.strip_chars() != "")
                    if empty_as_missing
                    else non_null
                )
                present_count = present.len()
                if present_count:
                    lengths = present.str.len_chars()
                    # len_chars() yields unsigned ints; min/max are non-None
                    # here because present_count > 0.
                    min_len = int(cast("int", lengths.min()))
                    max_len = int(cast("int", lengths.max()))
                    example = str(present[0])
                else:
                    min_len = max_len = 0
                    example = ""
            else:
                present = col_data.filter(col_data.is_not_null())
                present_count = present.len()
                min_len = max_len = 0
                example = str(present[0]) if present_count else ""
            # Top value + distinct over *present* values (excludes empty/null),
            # so the reported share never exceeds 100%.
            if present_count:
                vc = present.value_counts(sort=True)
                present_top_value = str(vc.row(0)[0])
                present_top_count = int(vc.row(0)[1])
                present_n_distinct = int(present.n_unique())
            else:
                present_top_value = ""
                present_top_count = 0
                present_n_distinct = 0
            profile["present_count"] = present_count
            profile["min_len"] = min_len
            profile["max_len"] = max_len
            profile["example"] = example
            profile["present_top_value"] = present_top_value
            profile["present_top_count"] = present_top_count
            profile["present_n_distinct"] = present_n_distinct

            # Numeric distribution stats (report enrichment; None for non-numeric)
            if col_data.dtype.is_numeric() and present_count:
                profile["num_min"] = float(cast("float", present.min()))
                profile["num_max"] = float(cast("float", present.max()))
                profile["num_mean"] = float(cast("float", present.mean()))
                # std() is typed float | timedelta; the dtype guard above makes
                # it numeric here.
                std = present.std()
                profile["num_std"] = float(cast("float", std)) if std is not None else None
            else:
                profile["num_min"] = None
                profile["num_max"] = None
                profile["num_mean"] = None
                profile["num_std"] = None

            columns.append(profile)

        return {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": columns,
        }

    def get_distinct_values(
        self,
        df: pl.DataFrame,
        column: str,
        limit: int | None = 1000,
    ) -> list[dict[str, Any]]:
        """Get distinct values with counts."""
        result = df.group_by(column).len().sort("len", descending=True)
        if limit is not None:
            result = result.head(limit)
        return [{"value": row[column], "count": row["len"]} for row in result.to_dicts()]

    def transform(
        self,
        df: pl.DataFrame,
        mapping_spec: dict[str, Any],
    ) -> pl.DataFrame:
        """Apply transformation based on mapping spec."""
        result = df

        # Apply column renames
        if "renames" in mapping_spec:
            for old_name, new_name in mapping_spec["renames"].items():
                if old_name in result.columns:
                    result = result.rename({old_name: new_name})

        # Apply type casts
        if "casts" in mapping_spec:
            for col, dtype in mapping_spec["casts"].items():
                if col in result.columns:
                    result = result.with_columns(self._pl.col(col).cast(dtype).alias(col))

        # Apply select (column projection)
        if "select" in mapping_spec:
            result = result.select(mapping_spec["select"])

        return result

    def write(
        self,
        df: pl.DataFrame,
        path: str,
        format: str = "parquet",
        mode: str = "overwrite",
    ) -> None:
        """Write DataFrame to file."""
        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)

        if format == "parquet":
            df.write_parquet(path_obj)
        elif format == "csv":
            df.write_csv(path_obj)
        elif format == "json":
            df.write_json(path_obj)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def sql(self, query: str) -> pl.DataFrame:
        """Execute SQL query using Polars SQL context."""
        ctx = self._pl.SQLContext()
        return ctx.execute(query).collect()

    def count(self, df: pl.DataFrame) -> int:
        """Count rows."""
        return len(df)

    def schema(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        """Return schema."""
        return [
            {
                "name": name,
                "type": str(dtype),
                "nullable": True,  # Polars doesn't track nullability in schema
            }
            for name, dtype in df.schema.items()
        ]

    def sample(self, df: pl.DataFrame, n: int) -> pl.DataFrame:
        """Return a random sample of n rows."""
        if n >= df.height:
            return df
        return df.sample(n=n)

    def map_column(
        self,
        df: pl.DataFrame,
        source_column: str,
        mapping: dict,
        target_column: str,
        default=0,
    ) -> pl.DataFrame:
        """Map values in source_column using a dictionary lookup."""
        return df.with_columns(
            self._pl.col(source_column)
            .replace_strict(mapping, default=default)
            .alias(target_column)
        )

    def read_database(
        self,
        connection_string: str,
        query: str | None = None,
        table: str | None = None,
        options: dict[str, Any] | None = None,
    ) -> pl.DataFrame:
        """Read data from a database using Polars."""
        options = options or {}
        if query:
            return self._pl.read_database_uri(query=query, uri=connection_string, **options)
        elif table:
            return self._pl.read_database_uri(
                query=f"SELECT * FROM {table}", uri=connection_string, **options
            )
        else:
            raise ValueError("Either 'query' or 'table' must be provided for database sources.")

    def from_records(self, records: list[dict]) -> pl.DataFrame:
        """Create a Polars DataFrame from a list of dicts."""
        return self._pl.DataFrame(records)

    def read_csv(self, path: str, **options: Any) -> pl.DataFrame:
        """Read a CSV file into a Polars DataFrame."""
        return self._pl.read_csv(path, **options)

    def write_csv(self, df: pl.DataFrame, path: str) -> None:
        """Write a Polars DataFrame to CSV."""
        df.write_csv(path)

    def to_pandas(self, df: pl.DataFrame) -> pd.DataFrame:
        """Convert to Pandas."""
        return df.to_pandas()

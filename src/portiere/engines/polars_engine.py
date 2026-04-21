"""
Portiere Polars Engine — Lightweight compute engine using Polars.

Polars is the default engine for Portiere, suitable for:
- Local development
- Small to medium datasets (up to a few GB)
- Fast iteration
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

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

    def profile(self, df: pl.DataFrame) -> dict[str, Any]:
        """Profile a DataFrame."""
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
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Get distinct values with counts."""
        result = df.group_by(column).len().sort("len", descending=True).head(limit)
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

    def read_csv(self, path: str) -> pl.DataFrame:
        """Read a CSV file into a Polars DataFrame."""
        return self._pl.read_csv(path)

    def write_csv(self, df: pl.DataFrame, path: str) -> None:
        """Write a Polars DataFrame to CSV."""
        df.write_csv(path)

    def to_pandas(self, df: pl.DataFrame) -> pd.DataFrame:
        """Convert to Pandas."""
        return df.to_pandas()

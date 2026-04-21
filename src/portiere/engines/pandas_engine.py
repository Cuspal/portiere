"""
Portiere Pandas Engine — Simple engine for small datasets.

Fallback engine using Pandas, suitable for:
- Very small datasets (MBs)
- Quick prototyping
- Environments where Polars is not available
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

from portiere.engines.base import AbstractEngine

if TYPE_CHECKING:
    import pandas as pd

logger = structlog.get_logger(__name__)


class PandasEngine(AbstractEngine):
    """
    Pandas-based compute engine.

    Use this for small datasets or as a fallback.
    For anything larger than a few hundred MB, use PolarsEngine or SparkEngine.

    Example:
        from portiere.engines import PandasEngine

        engine = PandasEngine()
        df = engine.read_source("/data/small.csv")
    """

    def __init__(self) -> None:
        """Initialize Pandas engine."""
        try:
            import pandas as pd

            self._pd = pd
        except ImportError:
            raise ImportError(
                "Pandas is required for PandasEngine. Install with: pip install portiere[pandas]"
            )

        logger.info("PandasEngine initialized")

    @property
    def engine_name(self) -> str:
        return "pandas"

    def read_source(
        self,
        path: str,
        format: str = "csv",
        options: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Read source data from files."""
        options = options or {}

        if format == "csv":
            return self._pd.read_csv(path, **options)
        elif format == "parquet":
            return self._pd.read_parquet(path, **options)
        elif format == "json":
            return self._pd.read_json(path, **options)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def profile(self, df: pd.DataFrame) -> dict[str, Any]:
        """Profile a DataFrame."""
        columns = []
        for col in df.columns:
            col_data = df[col]
            dtype = str(col_data.dtype)

            profile = {
                "name": col,
                "type": dtype,
                "nullable": col_data.isnull().any(),
                "null_count": int(col_data.isnull().sum()),
                "null_pct": col_data.isnull().mean() * 100,
                "n_unique": col_data.nunique(),
            }

            # Top values for categorical-like columns
            if col_data.nunique() <= 100:
                top_values = col_data.value_counts().head(10).to_dict()
                profile["top_values"] = [{col: k, "count": v} for k, v in top_values.items()]

            columns.append(profile)

        return {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": columns,
        }

    def get_distinct_values(
        self,
        df: pd.DataFrame,
        column: str,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Get distinct values with counts."""
        value_counts = df[column].value_counts().head(limit)
        return [{"value": k, "count": v} for k, v in value_counts.items()]

    def transform(
        self,
        df: pd.DataFrame,
        mapping_spec: dict[str, Any],
    ) -> pd.DataFrame:
        """Apply transformation based on mapping spec."""
        result = df.copy()

        # Apply column renames
        if "renames" in mapping_spec:
            result = result.rename(columns=mapping_spec["renames"])

        # Apply type casts
        if "casts" in mapping_spec:
            for col, dtype in mapping_spec["casts"].items():
                if col in result.columns:
                    result[col] = result[col].astype(dtype)

        # Apply select (column projection)
        if "select" in mapping_spec:
            result = result[mapping_spec["select"]]

        return result

    def write(
        self,
        df: pd.DataFrame,
        path: str,
        format: str = "parquet",
        mode: str = "overwrite",
    ) -> None:
        """Write DataFrame to file."""
        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)

        if format == "parquet":
            df.to_parquet(path_obj, index=False)
        elif format == "csv":
            df.to_csv(path_obj, index=False)
        elif format == "json":
            df.to_json(path_obj, orient="records")
        else:
            raise ValueError(f"Unsupported format: {format}")

    def sql(self, query: str) -> pd.DataFrame:
        """Execute SQL query (not supported in Pandas)."""
        raise NotImplementedError(
            "SQL queries are not supported in PandasEngine. "
            "Use PolarsEngine or SparkEngine for SQL support."
        )

    def count(self, df: pd.DataFrame) -> int:
        """Count rows."""
        return len(df)

    def schema(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """Return schema."""
        return [
            {
                "name": col,
                "type": str(df[col].dtype),
                "nullable": df[col].isnull().any(),
            }
            for col in df.columns
        ]

    def sample(self, df: pd.DataFrame, n: int) -> pd.DataFrame:
        """Return a random sample of n rows."""
        if n >= len(df):
            return df
        return df.sample(n=n)

    def map_column(
        self,
        df: pd.DataFrame,
        source_column: str,
        mapping: dict,
        target_column: str,
        default=0,
    ) -> pd.DataFrame:
        """Map values in source_column using a dictionary lookup."""
        result = df.copy()
        result[target_column] = result[source_column].map(mapping).fillna(default)
        return result

    def read_database(
        self,
        connection_string: str,
        query: str | None = None,
        table: str | None = None,
        options: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Read data from a database using Pandas + SQLAlchemy."""
        try:
            import sqlalchemy
        except ImportError:
            raise ImportError(
                "SQLAlchemy is required for database sources. Install with: pip install sqlalchemy"
            )
        engine = sqlalchemy.create_engine(connection_string)
        if query:
            return self._pd.read_sql(query, engine, **(options or {}))
        elif table:
            return self._pd.read_sql_table(table, engine, **(options or {}))
        else:
            raise ValueError("Either 'query' or 'table' must be provided for database sources.")

    def from_records(self, records: list[dict]) -> pd.DataFrame:
        """Create a Pandas DataFrame from a list of dicts."""
        return self._pd.DataFrame(records)

    def read_csv(self, path: str) -> pd.DataFrame:
        """Read a CSV file into a Pandas DataFrame."""
        return self._pd.read_csv(path)

    def write_csv(self, df: pd.DataFrame, path: str) -> None:
        """Write a Pandas DataFrame to CSV."""
        df.to_csv(path, index=False)

    def to_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Already Pandas, return as-is."""
        return df

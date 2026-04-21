"""
Portiere Engine — Abstract base class for compute engines.

All compute engines must implement this interface. This abstraction
allows the SDK to run on PySpark, Polars, DuckDB, or Pandas without
code changes.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd


class AbstractEngine(ABC):
    """
    Abstract base class for compute engine adapters.

    The engine provides a unified interface for:
    - Reading source data (files, databases)
    - Profiling data (schema, statistics)
    - Transforming data (applying mappings)
    - Writing output data

    Implementations:
    - SparkEngine: PySpark / Databricks
    - PolarsEngine: Polars (default, lightweight)
    - DuckDBEngine: DuckDB
    - PandasEngine: Pandas (small datasets)
    """

    @property
    @abstractmethod
    def engine_name(self) -> str:
        """Return engine name for artifact generation (spark/polars/...)."""
        ...

    @abstractmethod
    def read_source(
        self,
        path: str,
        format: str = "csv",
        options: dict[str, Any] | None = None,
    ) -> Any:
        """
        Read source data from files or database.

        Args:
            path: File path or glob pattern
            format: File format (csv, parquet, json)
            options: Format-specific options

        Returns:
            DataFrame in the engine's native format
        """
        ...

    @abstractmethod
    def profile(self, df: Any) -> dict[str, Any]:
        """
        Profile a DataFrame.

        Returns statistics about the data:
        - Column names and types
        - Row count
        - Cardinality per column
        - Null percentages
        - Top values for categorical columns

        Args:
            df: DataFrame to profile

        Returns:
            Profile dictionary
        """
        ...

    @abstractmethod
    def get_distinct_values(
        self,
        df: Any,
        column: str,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        Get distinct values with counts for a column.

        Used for extracting code lists (drug codes, diagnosis codes, etc.)

        Args:
            df: DataFrame
            column: Column name
            limit: Maximum distinct values to return

        Returns:
            List of {value, count} dicts
        """
        ...

    @abstractmethod
    def transform(
        self,
        df: Any,
        mapping_spec: dict[str, Any],
    ) -> Any:
        """
        Apply a mapping specification to transform the DataFrame.

        Args:
            df: Source DataFrame
            mapping_spec: Transformation specification

        Returns:
            Transformed DataFrame
        """
        ...

    @abstractmethod
    def write(
        self,
        df: Any,
        path: str,
        format: str = "parquet",
        mode: str = "overwrite",
    ) -> None:
        """
        Write DataFrame to target location.

        Args:
            df: DataFrame to write
            path: Output path
            format: Output format (parquet, csv, etc.)
            mode: Write mode (overwrite, append)
        """
        ...

    @abstractmethod
    def sql(self, query: str) -> Any:
        """
        Execute SQL query and return DataFrame.

        Args:
            query: SQL query string

        Returns:
            Result DataFrame
        """
        ...

    @abstractmethod
    def count(self, df: Any) -> int:
        """
        Count rows in DataFrame.

        Args:
            df: DataFrame

        Returns:
            Row count
        """
        ...

    @abstractmethod
    def schema(self, df: Any) -> list[dict[str, Any]]:
        """
        Return schema as list of column definitions.

        Args:
            df: DataFrame

        Returns:
            List of {name, type, nullable} dicts
        """
        ...

    @abstractmethod
    def to_pandas(self, df: Any) -> "pd.DataFrame":
        """
        Convert to Pandas DataFrame.

        Used for small results that need to be sent to the SaaS API.

        Args:
            df: DataFrame

        Returns:
            Pandas DataFrame
        """
        ...

    @abstractmethod
    def sample(self, df: Any, n: int) -> Any:
        """
        Return a random sample of n rows from the DataFrame.

        If n >= total rows, returns the full DataFrame unchanged.

        Args:
            df: DataFrame
            n: Number of rows to sample

        Returns:
            Sampled DataFrame
        """
        ...

    @abstractmethod
    def map_column(
        self,
        df: Any,
        source_column: str,
        mapping: dict,
        target_column: str,
        default: Any = 0,
    ) -> Any:
        """
        Map values in source_column using a dictionary lookup, writing to target_column.

        Used for concept ID lookups (source_code → concept_id).

        Args:
            df: DataFrame
            source_column: Column containing source values
            mapping: Dict mapping source values to target values
            target_column: Name of new column to create
            default: Default value for unmapped entries (0 = OMOP unmapped)

        Returns:
            DataFrame with new target_column added
        """
        ...

    @abstractmethod
    def read_database(
        self,
        connection_string: str,
        query: str | None = None,
        table: str | None = None,
        options: dict[str, Any] | None = None,
    ) -> Any:
        """
        Read data from a database.

        Args:
            connection_string: Database connection URI
                (e.g., postgresql://user:pass@host/db)
            query: SQL query to execute (mutually exclusive with table)
            table: Table name to read (mutually exclusive with query)
            options: Engine-specific options

        Returns:
            DataFrame in the engine's native format
        """
        ...

    def from_records(self, records: list[dict]) -> Any:
        """
        Create a native DataFrame from a list of dicts.

        Returns:
            DataFrame in the engine's native format.
        """
        import pandas as pd

        return pd.DataFrame(records)

    def read_csv(self, path: str) -> Any:
        """
        Read a CSV file into a native DataFrame.

        Args:
            path: Path to CSV file.

        Returns:
            DataFrame in the engine's native format.
        """
        import pandas as pd

        return pd.read_csv(path)

    def write_csv(self, df: Any, path: str) -> None:
        """
        Write a DataFrame to CSV.

        Args:
            df: DataFrame to write.
            path: Output file path.
        """
        pdf = self.to_pandas(df)
        pdf.to_csv(path, index=False)

    def to_dict_records(self, df: Any, limit: int = 1000) -> list[dict]:
        """
        Convert DataFrame rows to list of dicts.

        Args:
            df: DataFrame
            limit: Maximum rows

        Returns:
            List of row dicts
        """
        pdf = self.to_pandas(df)
        return pdf.head(limit).to_dict(orient="records")

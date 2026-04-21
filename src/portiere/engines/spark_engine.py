"""
Portiere Spark Engine — PySpark compute engine.

For large-scale data processing. Works with:
- Local PySpark
- Databricks
- EMR
- Dataproc
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

from portiere.engines.base import AbstractEngine

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import DataFrame, SparkSession

logger = structlog.get_logger(__name__)


class SparkEngine(AbstractEngine):
    """
    PySpark-based compute engine.

    Use this engine for large datasets (GB to TB scale).
    Works with Databricks, EMR, Dataproc, or local Spark.

    Example:
        from pyspark.sql import SparkSession
        from portiere.engines import SparkEngine

        spark = SparkSession.builder.appName("portiere").getOrCreate()
        engine = SparkEngine(spark)

        df = engine.read_source("/data/*.csv")
        profile = engine.profile(df)
    """

    def __init__(self, spark: SparkSession | None = None) -> None:
        """
        Initialize Spark engine.

        Args:
            spark: Existing SparkSession. If None, will create one.
        """
        try:
            from pyspark.sql import SparkSession

            self._SparkSession = SparkSession
        except ImportError:
            raise ImportError(
                "PySpark is required for SparkEngine. Install with: pip install portiere[spark]"
            )

        if spark is None:
            self._spark = SparkSession.builder.appName("portiere").getOrCreate()
            self._owns_spark = True
        else:
            self._spark = spark
            self._owns_spark = False

        logger.info(
            "SparkEngine initialized",
            app_name=self._spark.sparkContext.appName,
            spark_version=self._spark.version,
        )

    @property
    def spark(self) -> SparkSession:
        """Return the SparkSession."""
        return self._spark

    @property
    def engine_name(self) -> str:
        return "spark"

    def read_source(
        self,
        path: str,
        format: str = "csv",
        options: dict[str, Any] | None = None,
    ) -> DataFrame:
        """Read source data from files."""
        options = options or {}

        reader = self._spark.read

        # Apply options
        for key, value in options.items():
            reader = reader.option(key, value)

        if format == "csv":
            return reader.option("header", "true").csv(path)
        elif format == "parquet":
            return reader.parquet(path)
        elif format == "json":
            return reader.json(path)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def profile(self, df: DataFrame) -> dict[str, Any]:
        """Profile a DataFrame."""
        from pyspark.sql import functions as F

        # Get basic stats
        row_count = df.count()
        columns = []

        for field in df.schema.fields:
            col_name = field.name
            col_type = str(field.dataType)

            # Calculate stats for this column
            stats = df.agg(
                F.count(F.col(col_name)).alias("count"),
                F.countDistinct(F.col(col_name)).alias("n_unique"),
                F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias("null_count"),
            ).collect()[0]

            profile = {
                "name": col_name,
                "type": col_type,
                "nullable": field.nullable,
                "null_count": stats["null_count"],
                "null_pct": (stats["null_count"] / row_count * 100 if row_count > 0 else 0),
                "n_unique": stats["n_unique"],
            }

            # Top values for low-cardinality columns
            if stats["n_unique"] <= 100:
                top_values = (
                    df.groupBy(col_name).count().orderBy(F.desc("count")).limit(10).collect()
                )
                profile["top_values"] = [
                    {col_name: row[col_name], "count": row["count"]} for row in top_values
                ]

            columns.append(profile)

        return {
            "row_count": row_count,
            "column_count": len(df.columns),
            "columns": columns,
        }

    def get_distinct_values(
        self,
        df: DataFrame,
        column: str,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Get distinct values with counts."""
        from pyspark.sql import functions as F

        result = df.groupBy(column).count().orderBy(F.desc("count")).limit(limit).collect()
        return [{"value": row[column], "count": row["count"]} for row in result]

    def transform(
        self,
        df: DataFrame,
        mapping_spec: dict[str, Any],
    ) -> DataFrame:
        """Apply transformation based on mapping spec."""
        from pyspark.sql import functions as F

        result = df

        # Apply column renames
        if "renames" in mapping_spec:
            for old_name, new_name in mapping_spec["renames"].items():
                if old_name in result.columns:
                    result = result.withColumnRenamed(old_name, new_name)

        # Apply type casts
        if "casts" in mapping_spec:
            for col, dtype in mapping_spec["casts"].items():
                if col in result.columns:
                    result = result.withColumn(col, F.col(col).cast(dtype))

        # Apply select (column projection)
        if "select" in mapping_spec:
            result = result.select(*mapping_spec["select"])

        return result

    def write(
        self,
        df: DataFrame,
        path: str,
        format: str = "parquet",
        mode: str = "overwrite",
    ) -> None:
        """Write DataFrame to file."""
        writer = df.write.mode(mode)

        if format == "parquet":
            writer.parquet(path)
        elif format == "csv":
            writer.option("header", "true").csv(path)
        elif format == "json":
            writer.json(path)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def sql(self, query: str) -> DataFrame:
        """Execute SQL query."""
        return self._spark.sql(query)

    def count(self, df: DataFrame) -> int:
        """Count rows."""
        return df.count()

    def schema(self, df: DataFrame) -> list[dict[str, Any]]:
        """Return schema."""
        return [
            {
                "name": field.name,
                "type": str(field.dataType),
                "nullable": field.nullable,
            }
            for field in df.schema.fields
        ]

    def sample(self, df: DataFrame, n: int) -> DataFrame:
        """Return a sample of n rows (deterministic limit for Spark)."""
        total = df.count()
        if n >= total:
            return df
        return df.limit(n)

    def map_column(
        self,
        df: DataFrame,
        source_column: str,
        mapping: dict,
        target_column: str,
        default=0,
    ) -> DataFrame:
        """Map values in source_column using a dictionary lookup."""
        from itertools import chain

        from pyspark.sql import functions as F

        if not mapping:
            return df.withColumn(target_column, F.lit(default))

        # Build a Spark map literal from the dict
        mapping_expr = F.create_map(
            *list(chain.from_iterable((F.lit(k), F.lit(v)) for k, v in mapping.items()))
        )
        return df.withColumn(
            target_column,
            F.coalesce(mapping_expr[F.col(source_column)], F.lit(default)),
        )

    def read_database(
        self,
        connection_string: str,
        query: str | None = None,
        table: str | None = None,
        options: dict[str, Any] | None = None,
    ) -> DataFrame:
        """Read data from a database using Spark JDBC."""
        options = options or {}
        reader = self._spark.read.format("jdbc").option("url", connection_string)
        for key, value in options.items():
            reader = reader.option(key, value)
        if table:
            return reader.option("dbtable", table).load()
        elif query:
            return reader.option("dbtable", f"({query}) AS subq").load()
        else:
            raise ValueError("Either 'query' or 'table' must be provided for database sources.")

    def from_records(self, records: list[dict]) -> DataFrame:
        """Create a Spark DataFrame from a list of dicts."""
        return self._spark.createDataFrame(records)

    def read_csv(self, path: str) -> DataFrame:
        """Read a CSV file into a Spark DataFrame."""
        return self._spark.read.option("header", True).option("inferSchema", True).csv(path)

    def write_csv(self, df: DataFrame, path: str) -> None:
        """Write a Spark DataFrame to CSV."""
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)

    def to_pandas(self, df: DataFrame) -> pd.DataFrame:
        """Convert to Pandas."""
        return df.toPandas()

    def close(self) -> None:
        """Stop SparkSession if we own it."""
        if self._owns_spark:
            self._spark.stop()

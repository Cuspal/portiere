"""
GXProfiler — Data profiling using Great Expectations.

Profiles source DataFrames to generate column-level statistics,
value distributions, and auto-generated expectations.

Supports both pandas and PySpark DataFrames. Polars DataFrames
must be converted to pandas before passing to the profiler
(GX does not support Polars natively).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from portiere.config import QualityConfig

from portiere.quality.models import ProfileReport
from portiere.quality.utils import SPARK_NUMERIC_TYPES, _detect_df_type

logger = structlog.get_logger(__name__)


def _require_gx():
    """Import GX or raise a helpful error."""
    try:
        import great_expectations as gx

        return gx
    except ImportError:
        raise ImportError(
            "Great Expectations is required for data profiling. "
            "Install it with: pip install portiere-health[quality]"
        )


class GXProfiler:
    """Data profiling using Great Expectations."""

    def __init__(self, config: QualityConfig) -> None:
        self.config = config

    def profile(self, df: Any, source_name: str) -> dict:
        """
        Profile a DataFrame using Great Expectations.

        Args:
            df: Pandas or PySpark DataFrame to profile.
            source_name: Name of the source (used for naming).

        Returns:
            Profile report dict.
        """
        gx = _require_gx()
        df_type = _detect_df_type(df)

        context = gx.get_context()

        if df_type == "spark":
            datasource = context.data_sources.add_spark(name=source_name)
        else:
            datasource = context.data_sources.add_pandas(name=source_name)

        data_asset = datasource.add_dataframe_asset(name=f"{source_name}_asset")
        batch_def = data_asset.add_batch_definition_whole_dataframe("batch")
        batch = batch_def.get_batch(batch_parameters={"dataframe": df})

        # Build expectation suite with column-level expectations
        suite = gx.ExpectationSuite(name=f"{source_name}_profile")

        if df_type == "spark":
            dtypes_map = dict(df.dtypes)
            for col in df.columns:
                suite.add_expectation(gx.expectations.ExpectColumnToExist(column=col))
                if dtypes_map.get(col) in SPARK_NUMERIC_TYPES:
                    from pyspark.sql import functions as F

                    stats = df.agg(F.min(col), F.max(col)).collect()[0]
                    col_min, col_max = stats[0], stats[1]
                    if col_min is not None and col_max is not None:
                        suite.add_expectation(
                            gx.expectations.ExpectColumnValuesToBeBetween(
                                column=col,
                                min_value=float(col_min),
                                max_value=float(col_max),
                            )
                        )
        else:
            for col in df.columns:
                suite.add_expectation(gx.expectations.ExpectColumnToExist(column=col))
                if df[col].dtype in ("float64", "int64", "float32", "int32"):
                    col_min = df[col].min()
                    col_max = df[col].max()
                    if col_min is not None and col_max is not None:
                        suite.add_expectation(
                            gx.expectations.ExpectColumnValuesToBeBetween(
                                column=col,
                                min_value=float(col_min),
                                max_value=float(col_max),
                            )
                        )

        suite = context.suites.add(suite)
        result = batch.validate(suite)

        # Extract column stats
        columns = self._extract_column_stats(df)

        row_count = df.count() if df_type == "spark" else len(df)

        report = ProfileReport(
            source_name=source_name,
            columns=columns,
            gx_result=result.to_json_dict(),
            expectations=suite.to_json_dict(),
            row_count=row_count,
        )

        logger.info(
            "gx_profiler.profiled",
            source=source_name,
            columns=len(columns),
            rows=row_count,
        )

        return report.to_dict()

    def _extract_column_stats(self, df: Any) -> list[dict]:
        """Extract column-level statistics from a pandas or Spark DataFrame."""
        df_type = _detect_df_type(df)

        if df_type == "spark":
            return self._extract_column_stats_spark(df)
        return self._extract_column_stats_pandas(df)

    def _extract_column_stats_pandas(self, df: Any) -> list[dict]:
        """Extract column-level statistics from a pandas DataFrame."""
        columns = []
        for col in df.columns:
            stats: dict[str, Any] = {
                "name": col,
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isnull().sum()),
                "null_pct": float(df[col].isnull().mean()),
                "unique_count": int(df[col].nunique()),
            }

            # Add numeric stats
            if df[col].dtype in ("float64", "int64", "float32", "int32"):
                stats["min"] = float(df[col].min()) if not df[col].isnull().all() else None
                stats["max"] = float(df[col].max()) if not df[col].isnull().all() else None
                stats["mean"] = float(df[col].mean()) if not df[col].isnull().all() else None
                stats["std"] = float(df[col].std()) if not df[col].isnull().all() else None

            # Sample values
            non_null = df[col].dropna()
            if len(non_null) > 0:
                stats["sample_values"] = [str(v) for v in non_null.head(5).tolist()]
            else:
                stats["sample_values"] = []

            columns.append(stats)

        return columns

    def _extract_column_stats_spark(self, df: Any) -> list[dict]:
        """Extract column-level statistics from a PySpark DataFrame."""
        from pyspark.sql import functions as F

        total_count = df.count()
        dtypes_map = dict(df.dtypes)

        columns = []
        for col in df.columns:
            null_count = df.select(F.sum(F.col(col).isNull().cast("int"))).collect()[0][0] or 0

            unique_count = df.select(F.countDistinct(col)).collect()[0][0]

            stats: dict[str, Any] = {
                "name": col,
                "dtype": dtypes_map[col],
                "null_count": int(null_count),
                "null_pct": float(null_count / total_count) if total_count > 0 else 0.0,
                "unique_count": int(unique_count),
            }

            # Add numeric stats
            if dtypes_map[col] in SPARK_NUMERIC_TYPES:
                if null_count < total_count:
                    agg_row = df.select(
                        F.min(col), F.max(col), F.mean(col), F.stddev(col)
                    ).collect()[0]
                    stats["min"] = float(agg_row[0]) if agg_row[0] is not None else None
                    stats["max"] = float(agg_row[1]) if agg_row[1] is not None else None
                    stats["mean"] = float(agg_row[2]) if agg_row[2] is not None else None
                    stats["std"] = float(agg_row[3]) if agg_row[3] is not None else None
                else:
                    stats["min"] = None
                    stats["max"] = None
                    stats["mean"] = None
                    stats["std"] = None

            # Sample values
            rows = df.select(col).filter(F.col(col).isNotNull()).limit(5).collect()
            stats["sample_values"] = [str(row[0]) for row in rows]

            columns.append(stats)

        return columns

"""
Quality utilities — shared helpers for GXProfiler and GXValidator.
"""

from __future__ import annotations

from typing import Any

# Spark numeric type strings (used for dtype checks without importing pyspark)
SPARK_NUMERIC_TYPES = frozenset(
    ("int", "bigint", "long", "float", "double", "decimal", "short", "byte", "smallint", "tinyint")
)


def _detect_df_type(df: Any) -> str:
    """
    Detect DataFrame type without importing engine libraries.

    Returns:
        "spark" if df is a PySpark DataFrame, "pandas" otherwise.
    """
    type_name = type(df).__module__ + "." + type(df).__qualname__
    if "pyspark" in type_name:
        return "spark"
    return "pandas"

"""
Portiere Engines — Compute engine adapters.

Provides a unified interface for different compute backends:
- PySpark / Databricks
- Polars
- DuckDB
- Pandas
"""

from portiere.engines.base import AbstractEngine

__all__ = ["AbstractEngine", "PandasEngine", "PolarsEngine", "SparkEngine", "get_engine"]

# Engine imports are deferred to avoid import errors when
# optional dependencies are not installed


def __getattr__(name: str):
    """Lazy-import engine classes so missing optional deps don't break the package."""
    if name == "SparkEngine":
        from portiere.engines.spark_engine import SparkEngine

        return SparkEngine
    if name == "PolarsEngine":
        from portiere.engines.polars_engine import PolarsEngine

        return PolarsEngine
    if name == "PandasEngine":
        from portiere.engines.pandas_engine import PandasEngine

        return PandasEngine
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def get_engine(engine_type: str, **kwargs):
    """
    Factory function to create an engine instance.

    Args:
        engine_type: One of "spark", "polars", "duckdb", "pandas"
        **kwargs: Engine-specific arguments

    Returns:
        Engine instance

    Raises:
        ImportError: If required dependencies are not installed
    """
    if engine_type == "spark":
        from portiere.engines.spark_engine import SparkEngine

        return SparkEngine(**kwargs)
    elif engine_type == "polars":
        from portiere.engines.polars_engine import PolarsEngine

        return PolarsEngine(**kwargs)
    elif engine_type == "pandas":
        from portiere.engines.pandas_engine import PandasEngine

        return PandasEngine(**kwargs)
    else:
        raise ValueError(f"Unknown engine type: {engine_type}")

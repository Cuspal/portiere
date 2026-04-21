"""
Portiere Stage 1 — Data Ingestion and Profiling.

This stage:
1. Connects to source data
2. Profiles the data (schema, statistics)
3. Detects code columns for mapping
4. Detects PHI columns for de-identification
"""

from typing import Any

import structlog

from portiere.engines.base import AbstractEngine

logger = structlog.get_logger(__name__)


def ingest_source(
    engine: AbstractEngine,
    path: str,
    format: str = "csv",
    options: dict | None = None,
    sample_n: int | None = None,
) -> dict[str, Any]:
    """
    Ingest and profile source data.

    Args:
        engine: Compute engine to use
        path: Path to source data (file or glob)
        format: Data format (csv, parquet, json)
        options: Format-specific options
        sample_n: If set, use a sample of n rows for column-level
            exploration. Row count is always exact from full data.

    Returns:
        Profile dict with schema, stats, detected columns
    """
    logger.info("Stage 1: Ingesting source", path=path, format=format, sample_n=sample_n)

    # Read full data — needed for exact row_count
    df = engine.read_source(path, format=format, options=options)
    full_row_count = engine.count(df)

    # Sample for column-level exploration if requested
    if sample_n is not None:
        df_profile = engine.sample(df, sample_n)
    else:
        df_profile = df

    # Profile the (possibly sampled) data
    profile = engine.profile(df_profile)

    # Always use exact row_count from full data
    profile["row_count"] = full_row_count

    # Detect code columns
    code_columns = _detect_code_columns(profile)

    # Detect PHI columns
    phi_columns = _detect_phi_columns(profile)

    result = {
        "row_count": profile["row_count"],
        "column_count": profile["column_count"],
        "columns": profile["columns"],
        "code_columns": code_columns,
        "phi_columns": phi_columns,
        "sample_n": sample_n,
    }

    logger.info(
        "Stage 1 complete",
        rows=result["row_count"],
        columns=result["column_count"],
        code_columns=len(code_columns),
        phi_columns=len(phi_columns),
        sample_n=sample_n,
    )

    return result


def _detect_code_columns(profile: dict) -> list[str]:
    """Detect columns likely containing clinical codes."""
    code_patterns = [
        "code",
        "id",
        "type",
        "icd",
        "snomed",
        "loinc",
        "rxnorm",
        "ndc",
        "cpt",
        "drg",
        "atc",
        "diagnosis",
        "procedure",
        "drug",
        "medication",
        "lab",
        "test",
        "unit",
        "vocab",
    ]

    detected = []
    for col in profile.get("columns", []):
        col_name = col["name"].lower()
        col_type = col.get("type", "").lower()

        # Match code patterns
        if any(pattern in col_name for pattern in code_patterns):
            detected.append(col["name"])
        # String columns with moderate cardinality
        elif "str" in col_type or "utf" in col_type or "object" in col_type:
            n_unique = col.get("n_unique", 0)
            row_count = profile.get("row_count", 1)
            if 10 < n_unique < row_count * 0.5:
                detected.append(col["name"])

    return detected


def _detect_phi_columns(profile: dict) -> list[str]:
    """Detect columns likely containing PHI."""
    phi_patterns = [
        "name",
        "first_name",
        "last_name",
        "dob",
        "birth",
        "ssn",
        "social",
        "address",
        "street",
        "city",
        "zip",
        "phone",
        "email",
        "mrn",
        "patient_id",
        "national_id",
        "passport",
        "license",
        "age",
    ]

    detected = []
    for col in profile.get("columns", []):
        col_name = col["name"].lower()
        if any(pattern in col_name for pattern in phi_patterns):
            detected.append(col["name"])

    return detected


def extract_code_values(
    engine: AbstractEngine,
    path: str,
    column: str,
    format: str = "csv",
    limit: int = 5000,
    sample_n: int | None = None,
) -> list[dict]:
    """
    Extract distinct code values from a column.

    Args:
        engine: Compute engine
        path: Data path
        column: Column name
        format: Data format
        limit: Max distinct values
        sample_n: If set, extract from a sample of n rows

    Returns:
        List of {value, count} dicts
    """
    df = engine.read_source(path, format=format)
    if sample_n is not None:
        df = engine.sample(df, sample_n)
    return engine.get_distinct_values(df, column, limit=limit)

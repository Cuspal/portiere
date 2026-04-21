"""
Portiere Stage 5 — Validation.

This stage:
1. Validates transformed output against OMOP CDM spec
2. Checks referential integrity
3. Validates concept mappings
4. Generates QA report
"""

from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

try:
    import pandas as pd
except ImportError:
    pd = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from portiere.engines.base import AbstractEngine

logger = structlog.get_logger(__name__)


def _find_table_path(output_path: str, table_name: str) -> Path | None:
    """Find a table file/directory in the output path."""
    base = Path(output_path)
    # Check exact name (directory — Spark output)
    if (base / table_name).exists():
        return base / table_name
    # Check with common extensions
    for ext in (".parquet", ".csv", ".json"):
        candidate = base / f"{table_name}{ext}"
        if candidate.exists():
            return candidate
    return None


def validate_output(
    engine: "AbstractEngine",
    output_path: str,
    target_model: str = "omop_cdm_v5.4",
) -> dict[str, Any]:
    """
    Validate transformed output.

    Args:
        engine: Compute engine
        output_path: Path to transformed data
        target_model: Target data model for validation (omop_cdm_v5.4, fhir_r4, etc.)

    Returns:
        Validation result with issues and stats
    """
    from portiere.models.target_model import get_target_model

    logger.info("Stage 5: Validating output", output_path=output_path, target_model=target_model)

    # For FHIR models, delegate to TargetModel.validate_output()
    if target_model.lower().startswith("fhir"):
        model = get_target_model(target_model)
        return model.validate_output(engine, output_path)

    # For OMOP models, use existing validation logic below
    issues = []
    stats = {
        "tables_checked": 0,
        "rows_checked": 0,
        "valid_rows": 0,
        "invalid_rows": 0,
    }

    # Check required tables
    required_tables = ["person", "visit_occurrence", "condition_occurrence"]
    for table in required_tables:
        table_path = _find_table_path(output_path, table)
        if table_path is not None:
            fmt = table_path.suffix.lstrip(".") or "parquet"
            df = engine.read_source(str(table_path), format=fmt)
            row_count = engine.count(df)

            stats["tables_checked"] += 1
            stats["rows_checked"] += row_count
            stats["valid_rows"] += row_count

            # Validate table
            table_issues = _validate_table(engine, df, table, target_model)
            issues.extend(table_issues)
        else:
            issues.append(
                {
                    "table": table,
                    "type": "missing_required_table",
                    "message": f"Required table '{table}' not found in {output_path}",
                    "severity": "error",
                }
            )

    # Validate concept IDs
    concept_issues = _validate_concepts(engine, output_path)
    issues.extend(concept_issues)

    # Validate date ranges
    date_issues = _validate_dates(engine, output_path)
    issues.extend(date_issues)

    # Validate referential integrity
    ref_issues = _validate_referential_integrity(engine, output_path)
    issues.extend(ref_issues)

    # Validate completeness
    completeness_issues = _validate_completeness(engine, output_path)
    issues.extend(completeness_issues)

    # Calculate overall validity
    if stats["rows_checked"] > 0:
        validity_rate = stats["valid_rows"] / stats["rows_checked"] * 100
    else:
        validity_rate = 0

    result = {
        "valid": len([i for i in issues if i["severity"] == "error"]) == 0,
        "issues": issues,
        "stats": stats,
        "validity_rate": validity_rate,
    }

    logger.info(
        "Stage 5 complete",
        valid=result["valid"],
        issues=len(issues),
        validity_rate=f"{validity_rate:.1f}%",
    )

    return result


def _validate_table(
    engine: "AbstractEngine",
    df: Any,
    table: str,
    target_model: str,
) -> list[dict]:
    """Validate a single table against schema."""
    issues = []

    # Get table schema
    schema = engine.schema(df)

    # Required columns by OMOP table
    required_columns = {
        "person": ["person_id", "gender_concept_id", "year_of_birth"],
        "visit_occurrence": [
            "visit_occurrence_id",
            "person_id",
            "visit_concept_id",
            "visit_start_date",
        ],
        "condition_occurrence": [
            "condition_occurrence_id",
            "person_id",
            "condition_concept_id",
            "condition_start_date",
        ],
        "drug_exposure": [
            "drug_exposure_id",
            "person_id",
            "drug_concept_id",
            "drug_exposure_start_date",
        ],
        "measurement": [
            "measurement_id",
            "person_id",
            "measurement_concept_id",
            "measurement_date",
        ],
    }

    # Check required columns
    existing_cols = [col["name"] for col in schema]
    for req_col in required_columns.get(table, []):
        if req_col not in existing_cols:
            issues.append(
                {
                    "table": table,
                    "type": "missing_column",
                    "message": f"Required column '{req_col}' not found",
                    "severity": "error",
                }
            )

    return issues


def _validate_concepts(
    engine: "AbstractEngine",
    output_path: str,
) -> list[dict]:
    """Validate concept ID mappings."""
    issues = []

    # Tables with concept_id columns to check for unmapped (concept_id = 0)
    concept_tables = {
        "condition_occurrence": "condition_concept_id",
        "drug_exposure": "drug_concept_id",
        "measurement": "measurement_concept_id",
        "procedure_occurrence": "procedure_concept_id",
    }

    for table, concept_col in concept_tables.items():
        table_path = _find_table_path(output_path, table)
        if table_path is None:
            continue

        try:
            fmt = table_path.suffix.lstrip(".") or "parquet"
            df = engine.read_source(str(table_path), format=fmt)
            total = engine.count(df)
            if total == 0:
                continue

            schema_cols = [col["name"] for col in engine.schema(df)]
            if concept_col not in schema_cols:
                continue

            # Count rows where concept_id = 0 (unmapped)
            pdf = engine.to_pandas(df)
            unmapped_count = int((pdf[concept_col] == 0).sum())
            unmapped_pct = unmapped_count / total * 100

            if unmapped_pct > 20:
                issues.append(
                    {
                        "table": table,
                        "type": "unmapped_concepts",
                        "message": f"{unmapped_count:,} rows ({unmapped_pct:.1f}%) have {concept_col} = 0 (unmapped)",
                        "severity": "error" if unmapped_pct > 50 else "warning",
                    }
                )
            elif unmapped_pct > 0:
                issues.append(
                    {
                        "table": table,
                        "type": "unmapped_concepts",
                        "message": f"{unmapped_count:,} rows ({unmapped_pct:.1f}%) have {concept_col} = 0 (unmapped)",
                        "severity": "warning",
                    }
                )
        except Exception as e:
            logger.warning(f"Could not validate concepts for {table}: {e}")

    return issues


def _validate_dates(
    engine: "AbstractEngine",
    output_path: str,
) -> list[dict]:
    """Validate date columns are within reasonable range (1900-2100)."""
    issues = []

    date_columns = {
        "person": ["birth_datetime"],
        "visit_occurrence": ["visit_start_date", "visit_end_date"],
        "condition_occurrence": ["condition_start_date", "condition_end_date"],
        "drug_exposure": ["drug_exposure_start_date", "drug_exposure_end_date"],
        "measurement": ["measurement_date"],
    }

    for table, cols in date_columns.items():
        table_path = _find_table_path(output_path, table)
        if table_path is None:
            continue

        try:
            fmt = table_path.suffix.lstrip(".") or "parquet"
            df = engine.read_source(str(table_path), format=fmt)
            schema_cols = [col["name"] for col in engine.schema(df)]
            pdf = engine.to_pandas(df)

            for col in cols:
                if col not in schema_cols:
                    continue
                try:
                    dates = pd.to_datetime(pdf[col], errors="coerce")
                    out_of_range = ((dates.dt.year < 1900) | (dates.dt.year > 2100)).sum()
                    if out_of_range > 0:
                        issues.append(
                            {
                                "table": table,
                                "type": "invalid_date",
                                "message": f"{out_of_range} rows in {col} have dates outside 1900-2100 range",
                                "severity": "warning",
                            }
                        )
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"Could not validate dates for {table}: {e}")

    return issues


def _validate_referential_integrity(
    engine: "AbstractEngine",
    output_path: str,
) -> list[dict]:
    """Verify person_id in clinical tables exists in person table."""
    issues: list[dict] = []

    person_path = _find_table_path(output_path, "person")
    if person_path is None:
        return issues

    try:
        fmt = person_path.suffix.lstrip(".") or "parquet"
        person_df = engine.read_source(str(person_path), format=fmt)
        person_pdf = engine.to_pandas(person_df)
        person_ids = set(person_pdf["person_id"])
    except Exception as e:
        logger.warning(f"Could not load person table: {e}")
        return issues

    clinical_tables = ["visit_occurrence", "condition_occurrence", "drug_exposure", "measurement"]

    for table in clinical_tables:
        table_path = _find_table_path(output_path, table)
        if table_path is None:
            continue

        try:
            fmt = table_path.suffix.lstrip(".") or "parquet"
            df = engine.read_source(str(table_path), format=fmt)
            pdf = engine.to_pandas(df)
            if "person_id" not in pdf.columns:
                continue

            orphaned = (~pdf["person_id"].isin(person_ids)).sum()
            if orphaned > 0:
                total = len(pdf)
                issues.append(
                    {
                        "table": table,
                        "type": "referential_integrity",
                        "message": f"{orphaned:,} rows ({orphaned / total * 100:.1f}%) have person_id not found in person table",
                        "severity": "error",
                    }
                )
        except Exception as e:
            logger.warning(f"Could not validate referential integrity for {table}: {e}")

    return issues


def _validate_completeness(
    engine: "AbstractEngine",
    output_path: str,
) -> list[dict]:
    """Calculate non-null percentages for required columns."""
    issues = []

    required_columns = {
        "person": ["person_id", "gender_concept_id", "year_of_birth"],
        "visit_occurrence": [
            "visit_occurrence_id",
            "person_id",
            "visit_concept_id",
            "visit_start_date",
        ],
        "condition_occurrence": [
            "condition_occurrence_id",
            "person_id",
            "condition_concept_id",
            "condition_start_date",
        ],
    }

    for table, cols in required_columns.items():
        table_path = _find_table_path(output_path, table)
        if table_path is None:
            continue

        try:
            fmt = table_path.suffix.lstrip(".") or "parquet"
            df = engine.read_source(str(table_path), format=fmt)
            total = engine.count(df)
            if total == 0:
                continue
            pdf = engine.to_pandas(df)

            for col in cols:
                if col not in pdf.columns:
                    continue
                null_count = int(pdf[col].isna().sum())
                if null_count > 0:
                    null_pct = null_count / total * 100
                    issues.append(
                        {
                            "table": table,
                            "type": "missing_values",
                            "message": f"Required column '{col}' has {null_count:,} nulls ({null_pct:.1f}%)",
                            "severity": "error" if null_pct > 10 else "warning",
                        }
                    )
        except Exception as e:
            logger.warning(f"Could not validate completeness for {table}: {e}")

    return issues


def generate_qa_report(
    validation_result: dict[str, Any],
    output_path: str | None = None,
) -> str:
    """
    Generate human-readable QA report.

    Args:
        validation_result: Result from validate_output
        output_path: Optional path to save report

    Returns:
        Report content as string
    """
    issues = validation_result.get("issues", [])
    stats = validation_result.get("stats", {})
    valid = validation_result.get("valid", False)

    errors = [i for i in issues if i["severity"] == "error"]
    warnings = [i for i in issues if i["severity"] == "warning"]

    report = f"""
================================================================================
                        Portiere Validation Report
================================================================================

Overall Status: {"✓ PASSED" if valid else "✗ FAILED"}

Statistics:
  - Tables checked:  {stats.get("tables_checked", 0)}
  - Rows checked:    {stats.get("rows_checked", 0):,}
  - Valid rows:      {stats.get("valid_rows", 0):,}
  - Validity rate:   {validation_result.get("validity_rate", 0):.1f}%

Issues:
  - Errors:   {len(errors)}
  - Warnings: {len(warnings)}

"""

    if errors:
        report += "ERRORS:\n"
        for error in errors:
            report += f"  ✗ [{error['table']}] {error['message']}\n"
        report += "\n"

    if warnings:
        report += "WARNINGS:\n"
        for warning in warnings:
            report += f"  ⚠ [{warning['table']}] {warning['message']}\n"
        report += "\n"

    if valid and not issues:
        report += "No issues found. Data is ready for use.\n"

    report += "=" * 80 + "\n"

    if output_path:
        Path(output_path).write_text(report)
        logger.info(f"QA report saved: {output_path}")

    return report

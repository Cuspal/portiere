"""
GXValidator — Post-ETL data quality validation using Great Expectations.

Validates ETL output against target model expectations for completeness,
conformance, and plausibility.  Works with any YAML-defined standard
(OMOP CDM, FHIR R4, HL7 v2.5.1, OpenEHR 1.0.4, or custom).

Supports both pandas and PySpark DataFrames. Polars DataFrames
must be converted to pandas before passing to the validator
(GX does not support Polars natively).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from portiere.config import QualityConfig, ThresholdsConfig

from portiere.quality.models import ValidationReport
from portiere.quality.utils import SPARK_NUMERIC_TYPES, _detect_df_type

logger = structlog.get_logger(__name__)


def _require_gx():
    """Import GX or raise a helpful error."""
    try:
        import great_expectations as gx

        return gx
    except ImportError:
        raise ImportError(
            "Great Expectations is required for validation. "
            "Install it with: pip install portiere-health[quality]"
        )


class GXValidator:
    """Post-mapping / post-ETL data quality validation."""

    def __init__(self, config: QualityConfig, thresholds: ThresholdsConfig) -> None:
        self.config = config
        self.thresholds = thresholds

    def validate(
        self,
        df: Any,
        table_name: str,
        target_model: str,
        *,
        ref_tables: dict[str, Any] | None = None,
    ) -> dict:
        """
        Validate a DataFrame against target model expectations.

        Args:
            df: Pandas or PySpark DataFrame to validate (ETL output table).
            table_name: Entity name (e.g., "person", "Patient", "PID").
            target_model: Target model identifier (e.g., "omop_cdm_v5.4", "fhir_r4").
            ref_tables: Optional reference tables for FK plausibility checks
                (e.g., ``{"concept": concept_df}``). Without these, FK and
                domain-match rules skip rather than fail.

        Returns:
            Validation report dict including plausibility rule outcomes.
        """
        gx = _require_gx()
        df_type = _detect_df_type(df)

        context = gx.get_context()
        suite = self._build_expectation_suite(gx, context, table_name, target_model, df)

        if df_type == "spark":
            datasource = context.data_sources.add_spark(name=f"validate_{table_name}")
        else:
            datasource = context.data_sources.add_pandas(name=f"validate_{table_name}")

        asset = datasource.add_dataframe_asset(name=table_name)
        batch_def = asset.add_batch_definition_whole_dataframe("batch")
        batch = batch_def.get_batch(batch_parameters={"dataframe": df})

        result = batch.validate(suite)

        completeness = self._compute_completeness(result)
        conformance = self._compute_conformance(result)
        overall_success = self._compute_overall_success(result)

        # Plausibility — Kahn-style cross-table and domain-rule checks via
        # the plausibility runner. Distinct from completeness/conformance.
        rule_results = self._run_plausibility_checks(
            df, table_name, target_model, ref_tables=ref_tables
        )
        plausibility = self._compute_plausibility(rule_results)

        thresholds = {
            "min_completeness": self.thresholds.validation.min_completeness,
            "min_conformance": self.thresholds.validation.min_conformance,
            "min_plausibility": self.thresholds.validation.min_plausibility,
        }

        # An error-tier plausibility rule failure fails validation regardless
        # of the score thresholds (warn-tier failures only affect reporting).
        error_failures = any(
            r.severity == "error" and not r.passed and r.total_rows > 0 for r in rule_results
        )

        passed = (
            completeness >= thresholds["min_completeness"]
            and conformance >= thresholds["min_conformance"]
            and plausibility >= thresholds["min_plausibility"]
            and not error_failures
        )

        report = ValidationReport(
            table_name=table_name,
            passed=passed,
            completeness_score=completeness,
            conformance_score=conformance,
            plausibility_score=plausibility,
            overall_success_score=overall_success,
            plausibility_rule_results=[
                {
                    "rule_id": r.rule_id,
                    "severity": r.severity,
                    "passed": r.passed,
                    "total_rows": r.total_rows,
                    "failed_count": r.failed_count,
                    "detail": r.detail,
                }
                for r in rule_results
            ],
            gx_result=result.to_json_dict(),
            thresholds=thresholds,
        )

        logger.info(
            "gx_validator.validated",
            table=table_name,
            passed=passed,
            completeness=f"{completeness:.2f}",
            conformance=f"{conformance:.2f}",
            plausibility=f"{plausibility:.2f}",
            overall_success=f"{overall_success:.2f}",
            n_rules=len(rule_results),
        )

        return report.to_dict()

    def _build_expectation_suite(
        self,
        gx: Any,
        context: Any,
        table_name: str,
        target_model: str,
        df: Any = None,
    ) -> Any:
        """Build GX expectation suite from the target model's field metadata.

        Uses ``get_field_types()`` to derive conformance checks for any
        YAML-defined standard (OMOP, FHIR, HL7 v2, OpenEHR, custom).
        """
        from portiere.models.target_model import get_target_model

        model = get_target_model(target_model)
        schema = model.get_schema()

        # Columns actually present in the DataFrame
        actual_cols = set(df.columns) if df is not None else set()

        suite = gx.ExpectationSuite(name=f"validate_{table_name}")

        # Completeness: required columns exist
        required_cols = schema.get(table_name, [])
        for col in required_cols:
            suite.add_expectation(gx.expectations.ExpectColumnToExist(column=col))

        # Detect DataFrame type for numeric checks
        df_type = _detect_df_type(df) if df is not None else "pandas"

        # Standards-aware conformance checks via field type metadata
        field_types = {}
        if hasattr(model, "get_field_types"):
            field_types = model.get_field_types(table_name)

        code_cols = [c for c in required_cols if field_types.get(c) == "code"]
        temporal_cols = [c for c in required_cols if field_types.get(c) == "temporal"]

        # Conformance: code/vocabulary columns should be non-negative integers
        # (only applies when the column is actually numeric, e.g. OMOP concept_id)
        for col in code_cols:
            if col not in actual_cols:
                continue
            if df is not None and not self._is_numeric_column(df, col, df_type):
                continue
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column=col,
                    min_value=0,
                    mostly=0.95,
                )
            )

        # Conformance: temporal columns should not be null
        for col in temporal_cols:
            if col not in actual_cols:
                continue
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToNotBeNull(
                    column=col,
                    mostly=0.90,
                )
            )

        suite = context.suites.add(suite)
        return suite

    # Backward-compatible alias
    _build_omop_suite = _build_expectation_suite

    @staticmethod
    def _is_numeric_column(df: Any, col: str, df_type: str) -> bool:
        """Check if a column is numeric, handling both pandas and Spark."""
        if df_type == "spark":
            dtypes_map = dict(df.dtypes)
            return dtypes_map.get(col, "") in SPARK_NUMERIC_TYPES
        else:
            import pandas as pd

            return pd.api.types.is_numeric_dtype(df[col])

    def _compute_completeness(self, result: Any) -> float:
        """Compute completeness score from GX result."""
        results = result.to_json_dict().get("results", [])
        if not results:
            return 1.0

        # Completeness = proportion of "column exists" expectations that pass
        exist_results = [
            r
            for r in results
            if r.get("expectation_config", {}).get("type", "") == "expect_column_to_exist"
        ]
        if not exist_results:
            return 1.0
        passed = sum(1 for r in exist_results if r.get("success", False))
        return passed / len(exist_results)

    def _compute_conformance(self, result: Any) -> float:
        """Compute conformance score from GX result."""
        results = result.to_json_dict().get("results", [])
        if not results:
            return 1.0

        # Conformance = proportion of value-level expectations that pass
        value_results = [
            r
            for r in results
            if r.get("expectation_config", {}).get("type", "") != "expect_column_to_exist"
        ]
        if not value_results:
            return 1.0
        passed = sum(1 for r in value_results if r.get("success", False))
        return passed / len(value_results)

    def _compute_overall_success(self, result: Any) -> float:
        """Fraction of GX expectations that succeeded (any expectation type).

        This is the v0.1.0 ``_compute_plausibility`` behaviour preserved
        under a more accurate name. It is *not* plausibility in the
        Kahn-et-al. sense — see :meth:`_compute_plausibility` for that.
        """
        results = result.to_json_dict().get("results", [])
        if not results:
            return 1.0
        passed = sum(1 for r in results if r.get("success", False))
        return passed / len(results)

    # Backwards-compatible alias — kept so any external caller of the old
    # name still gets the same numeric meaning. New code should use
    # ``_compute_overall_success`` for the GX success rate and
    # ``_compute_plausibility`` for the rule-result plausibility score.
    _legacy_compute_plausibility = _compute_overall_success

    def _compute_plausibility(self, rule_results: list[Any]) -> float:
        """Plausibility score from plausibility rule results.

        Counts only error-severity rules with ``total_rows > 0`` (skipped
        rules — e.g., for absent optional columns — don't drag the score
        down). Warn-severity rules are reported but excluded from the
        score.

        Returns 1.0 when no error-severity rules ran (nothing to fail).
        """
        scored = [
            r
            for r in rule_results
            if getattr(r, "severity", None) == "error" and getattr(r, "total_rows", 0) > 0
        ]
        if not scored:
            return 1.0
        passed = sum(1 for r in scored if r.passed)
        return passed / len(scored)

    def _run_plausibility_checks(
        self,
        df: Any,
        entity: str,
        target_model: str,
        *,
        ref_tables: dict[str, Any] | None = None,
    ) -> list:
        """Run YAML DSL rules + standards-specific Python rules for ``entity``.

        Failures are returned as a list of :class:`RuleResult`. Missing
        target models, missing rule blocks, and mocked models all return
        an empty list rather than raising — validation stays robust.
        """
        from portiere.models.target_model import get_target_model
        from portiere.quality.plausibility.dsl import FkExistsRule
        from portiere.quality.plausibility.registry import run_python_rules
        from portiere.quality.plausibility.runner import run_column_rule, run_fk_rule

        ref = ref_tables or {}
        results: list = []

        # YAML DSL rules
        try:
            model = get_target_model(target_model)
        except Exception:
            return []  # unknown / mocked target — skip
        if not hasattr(model, "get_plausibility_rules"):
            return []
        try:
            dsl_rules = model.get_plausibility_rules(entity)
        except Exception:
            dsl_rules = []
        for rule in dsl_rules:
            try:
                if isinstance(rule, FkExistsRule):
                    results.append(run_fk_rule(df, rule, ref_tables=ref))
                else:
                    results.append(run_column_rule(df, rule))
            except Exception as exc:
                logger.warning(
                    "plausibility.dsl_rule_error",
                    rule_id=getattr(rule, "id", "?"),
                    error=str(exc),
                )

        # Standards-specific Python rules
        try:
            results.extend(run_python_rules(target_model, entity, df, ref_tables=ref))
        except Exception as exc:
            logger.warning("plausibility.python_rules_error", error=str(exc))

        return results

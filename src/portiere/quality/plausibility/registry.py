"""Dispatch table for standards-specific Python plausibility rules.

Built-in standards (OMOP, FHIR) declare their non-DSL rules here. Rules
that need joins or aggregates live in ``omop.py`` / ``fhir.py``; this
module decides which ones to invoke for a given (target_model, entity)
pair.

Custom standards declare YAML DSL rules in their own files; they cannot
ship Python rules without contributing them upstream (the
``[quality]``-shipped Python rules are part of the SDK).
"""

from __future__ import annotations

import pandas as pd

from portiere.quality.plausibility import fhir, omop
from portiere.quality.plausibility.runner import RuleResult


def run_python_rules(
    target_model_name: str,
    entity: str,
    df: pd.DataFrame,
    *,
    ref_tables: dict[str, pd.DataFrame] | None = None,
) -> list[RuleResult]:
    """Run standards-specific Python plausibility rules for an entity.

    Returns an empty list when no rules are registered for the
    ``(target_model, entity)`` pair, or when the model name does not
    match any built-in standard.
    """
    ref_tables = ref_tables or {}
    name = (target_model_name or "").lower()

    if "omop" in name:
        return _omop_rules(entity, df, ref_tables)
    if "fhir" in name:
        return _fhir_rules(entity, df)
    return []


def _omop_rules(
    entity: str,
    df: pd.DataFrame,
    ref_tables: dict[str, pd.DataFrame],
) -> list[RuleResult]:
    if entity == "person":
        return [omop.birth_before_death(df), omop.age_in_range(df)]
    if entity == "condition_occurrence":
        results: list[RuleResult] = [omop.condition_dates_consistent(df)]
        if "concept" in ref_tables:
            results.extend(
                omop.concept_id_fk(df, ref_tables["concept"], columns=["condition_concept_id"])
            )
            results.extend(
                omop.domain_match(
                    df,
                    ref_tables["concept"],
                    expected_domain_per_column={
                        "condition_concept_id": "Condition",
                    },
                )
            )
        return results
    if entity == "drug_exposure" and "concept" in ref_tables:
        return list(omop.concept_id_fk(df, ref_tables["concept"], columns=["drug_concept_id"]))
    if entity == "measurement" and "concept" in ref_tables:
        return list(
            omop.concept_id_fk(df, ref_tables["concept"], columns=["measurement_concept_id"])
        )
    return []


def _fhir_rules(entity: str, df: pd.DataFrame) -> list[RuleResult]:
    if entity == "Patient":
        return [fhir.patient_birthdate_not_future(df)]
    if entity == "Observation":
        return [fhir.observation_status_in_valueset(df)]
    if entity == "MedicationRequest":
        return [fhir.medication_request_intent_in_valueset(df)]
    return []

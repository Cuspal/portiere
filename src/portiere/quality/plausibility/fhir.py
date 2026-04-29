"""FHIR R4 plausibility rules implemented in Python.

These checks operate on ETL output shaped as flat tables (one row per
resource, columns matching the resource's flat element paths).
ValueSet bindings are the most common case: FHIR ``code``-typed fields
must be one of the values from a fixed-vocabulary ValueSet.
"""

from __future__ import annotations

import pandas as pd

from portiere.quality.plausibility.runner import RuleResult

# FHIR observation-status ValueSet (R4)
# http://hl7.org/fhir/ValueSet/observation-status
OBSERVATION_STATUS_VALUES = {
    "registered",
    "preliminary",
    "final",
    "amended",
    "corrected",
    "cancelled",
    "entered-in-error",
    "unknown",
}

# FHIR medicationrequest-intent ValueSet (R4)
# http://hl7.org/fhir/ValueSet/medicationrequest-intent
MEDICATION_REQUEST_INTENT_VALUES = {
    "proposal",
    "plan",
    "order",
    "original-order",
    "reflex-order",
    "filler-order",
    "instance-order",
    "option",
}


def patient_birthdate_not_future(
    patient_df: pd.DataFrame,
    *,
    reference_date: str = "2026-01-01",
) -> RuleResult:
    """``Patient.birthDate`` must not be in the future relative to
    ``reference_date``.
    """
    rule_id = "patient_birthdate_not_future"
    if "birthDate" not in patient_df.columns:
        return RuleResult(rule_id, "error", True, 0, 0, detail="birthDate column not present")
    s = patient_df["birthDate"].dropna()
    parsed = pd.to_datetime(s, errors="coerce")
    ref = pd.to_datetime(reference_date)
    valid = parsed.notna()
    bad = valid & (parsed > ref)
    return RuleResult(
        rule_id,
        "error",
        passed=int(bad.sum()) == 0,
        total_rows=int(valid.sum()),
        failed_count=int(bad.sum()),
    )


def observation_status_in_valueset(observation_df: pd.DataFrame) -> RuleResult:
    """``Observation.status`` must be in the FHIR observation-status ValueSet."""
    rule_id = "observation_status_in_valueset"
    if "status" not in observation_df.columns:
        return RuleResult(rule_id, "error", True, 0, 0, detail="status column not present")
    s = observation_df["status"].dropna().astype(str)
    bad = ~s.isin(OBSERVATION_STATUS_VALUES)
    return RuleResult(
        rule_id,
        "error",
        passed=int(bad.sum()) == 0,
        total_rows=len(s),
        failed_count=int(bad.sum()),
    )


def medication_request_intent_in_valueset(medreq_df: pd.DataFrame) -> RuleResult:
    """``MedicationRequest.intent`` must be in the FHIR medicationrequest-intent ValueSet."""
    rule_id = "medication_request_intent_in_valueset"
    if "intent" not in medreq_df.columns:
        return RuleResult(rule_id, "error", True, 0, 0, detail="intent column not present")
    s = medreq_df["intent"].dropna().astype(str)
    bad = ~s.isin(MEDICATION_REQUEST_INTENT_VALUES)
    return RuleResult(
        rule_id,
        "error",
        passed=int(bad.sum()) == 0,
        total_rows=len(s),
        failed_count=int(bad.sum()),
    )

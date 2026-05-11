"""US Core 6.1.0 validation orchestrator."""

from __future__ import annotations

import importlib.resources
import json

from portiere.quality.fhir_profile.report import ProfileValidationReport, ResourceFailure
from portiere.quality.fhir_profile.validator import validate_invariant, validate_resource_schema

_PROFILE = "us-core-6.1.0"

_SUPPORTED_RESOURCE_TYPES = {
    "Patient",
    "Practitioner",
    "Organization",
    "Encounter",
    "Condition",
    "Observation",
    "MedicationRequest",
    "AllergyIntolerance",
    "Procedure",
    "DocumentReference",
}


def _load_sd(resource_type: str) -> dict:
    pkg = importlib.resources.files("portiere.standards.fhir_profiles.us_core_6_1_0")
    data = (pkg / f"{resource_type}.json").read_text(encoding="utf-8")
    return json.loads(data)


def _check_required_fields(resource: dict, sd: dict, resource_index: int) -> list[ResourceFailure]:
    failures = []
    resource_type = resource.get("resourceType", "Unknown")
    for elem in sd.get("snapshot", {}).get("element", []):
        elem_id = elem.get("id", "")
        parts = elem_id.split(".")
        if len(parts) != 2:
            continue
        if elem.get("min", 0) < 1:
            continue
        field_name = parts[1]
        if not resource.get(field_name):
            failures.append(
                ResourceFailure(
                    resource_type=resource_type,
                    resource_index=resource_index,
                    invariant_id=f"us-core-required-{field_name}",
                    message=f"{elem_id} is required (min={elem['min']}) by US Core",
                )
            )
    return failures


def _collect_us_core_invariants(sd: dict) -> list[dict]:
    invariants = []
    for elem in sd.get("snapshot", {}).get("element", []):
        elem_id = elem.get("id", "")
        for c in elem.get("constraint", []):
            key = c.get("key", "")
            expr = c.get("expression", "")
            if key.startswith("us-core-") and expr:
                invariants.append(
                    {
                        "id": key,
                        "expression": expr,
                        "severity": c.get("severity", "error"),
                        "human": c.get("human", ""),
                        "elem_id": elem_id,
                    }
                )
    return invariants


def validate_against_us_core(resources: list[dict]) -> ProfileValidationReport:
    """Validate a list of FHIR resource dicts against US Core 6.1.0 profiles."""
    failures: list[ResourceFailure] = []
    skipped: list[str] = []

    for idx, resource in enumerate(resources):
        resource_type = resource.get("resourceType", "Unknown")

        if resource_type not in _SUPPORTED_RESOURCE_TYPES:
            if resource_type not in skipped:
                skipped.append(resource_type)
            continue

        # 1. Schema check
        failures.extend(validate_resource_schema(resource, resource_index=idx))

        # 2. Required-field cardinality check
        sd = _load_sd(resource_type)
        failures.extend(_check_required_fields(resource, sd, idx))

        # 3. US Core FHIRPath invariants (evaluated in the element's context)
        for inv in _collect_us_core_invariants(sd):
            elem_id = inv.get("elem_id", "")
            parts = elem_id.split(".")
            # Determine context nodes: root resource or a child field
            if len(parts) <= 1:
                context_nodes = [resource]
            else:
                field_name = parts[1]
                field_value = resource.get(field_name)
                if field_value is None:
                    context_nodes = []
                elif isinstance(field_value, list):
                    context_nodes = field_value
                else:
                    context_nodes = [field_value]

            failed = False
            for node in context_nodes:
                result = validate_invariant(
                    node, expression=inv["expression"], invariant_id=inv["id"]
                )
                if result.passed is False:
                    failed = True
                    break

            if failed:
                failures.append(
                    ResourceFailure(
                        resource_type=resource_type,
                        resource_index=idx,
                        invariant_id=inv["id"],
                        message=inv["human"] or inv["expression"],
                        severity=inv["severity"],
                    )
                )

    return ProfileValidationReport(
        profile=_PROFILE,
        total_resources=len(resources),
        failures=failures,
        skipped=skipped,
    )

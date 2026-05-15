"""mCODE STU3 2.0.0 validation orchestrator.

Unlike US Core (which is keyed on resourceType), mCODE uses ``meta.profile`` to
claim the profile — one base ``Patient`` resource is also an mCODE CancerPatient
when ``meta.profile`` contains the CancerPatient canonical URL. This module
matches resources to mCODE profiles via ``meta.profile`` URL substring, then
runs the same schema + cardinality + FHIRPath-invariant pipeline as
``us_core.py``.
"""

from __future__ import annotations

import importlib.resources
import json

from portiere.quality.fhir_profile.report import ProfileValidationReport, ResourceFailure
from portiere.quality.fhir_profile.validator import validate_invariant, validate_resource_schema

_PROFILE = "mcode-2.0.0"
_PKG = "portiere.standards.fhir_profiles.mcode_2_0_0"

# Map SD stem -> the canonical-URL substring that identifies it in meta.profile.
_MCODE_PROFILE_URL_STEM = {
    "CancerPatient": "mcode-cancer-patient",
    "PrimaryCancerCondition": "mcode-primary-cancer-condition",
    "CancerDiseaseStatus": "mcode-cancer-disease-status",
    "CancerStage": "mcode-cancer-stage",
    "TNMStageGroup": "mcode-tnm-stage-group",
}


def _load_sd(sd_stem: str) -> dict:
    pkg = importlib.resources.files(_PKG)
    return json.loads((pkg / f"{sd_stem}.json").read_text(encoding="utf-8"))


def _select_sd_for_resource(resource: dict) -> str | None:
    """Return the bundled SD stem this resource claims via meta.profile, or None."""
    profile_urls = (resource.get("meta") or {}).get("profile", [])
    for url in profile_urls:
        url_lc = url.lower()
        for sd_stem, url_token in _MCODE_PROFILE_URL_STEM.items():
            # Match longest tokens first so e.g. "primary-cancer-condition"
            # wins over the shorter substrings of related URLs.
            if url_token in url_lc:
                return sd_stem
    return None


def _check_required_fields(resource: dict, sd: dict, resource_index: int) -> list[ResourceFailure]:
    """Check required (min>=1) top-level fields from the SD snapshot."""
    failures: list[ResourceFailure] = []
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
                    invariant_id=f"mcode-required-{field_name}",
                    message=f"{elem_id} is required (min={elem['min']}) by mCODE",
                )
            )
    return failures


def _collect_mcode_invariants(sd: dict) -> list[dict]:
    invariants: list[dict] = []
    for elem in sd.get("snapshot", {}).get("element", []):
        elem_id = elem.get("id", "")
        for c in elem.get("constraint", []):
            key = c.get("key", "")
            expr = c.get("expression", "")
            if key.startswith("mcode-") and expr:
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


def validate_against_mcode(resources: list[dict]) -> ProfileValidationReport:
    """Validate a list of FHIR resource dicts against mCODE 2.0.0 profiles."""
    failures: list[ResourceFailure] = []
    skipped: list[str] = []

    for idx, resource in enumerate(resources):
        sd_stem = _select_sd_for_resource(resource)
        if sd_stem is None:
            rt = resource.get("resourceType", "Unknown")
            if rt not in skipped:
                skipped.append(rt)
            continue

        # 1. Base schema check
        failures.extend(validate_resource_schema(resource, resource_index=idx))

        # 2. Required-field cardinality from the mCODE SD
        sd = _load_sd(sd_stem)
        failures.extend(_check_required_fields(resource, sd, idx))

        # 3. mCODE FHIRPath invariants (evaluated in the element's context)
        for inv in _collect_mcode_invariants(sd):
            elem_id = inv.get("elem_id", "")
            parts = elem_id.split(".")
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
                        resource_type=resource.get("resourceType", "Unknown"),
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

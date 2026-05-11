"""Low-level validators: schema (fhir.resources) and FHIRPath invariants (fhirpathpy)."""

from __future__ import annotations

from pydantic import BaseModel

from portiere.quality.fhir_profile.report import ResourceFailure


def validate_resource_schema(
    resource: dict,
    *,
    resource_index: int = 0,
) -> list[ResourceFailure]:
    """Validate a FHIR resource dict against its fhir.resources Pydantic model.

    Returns a list of ResourceFailure objects (empty = valid).
    """
    import fhir.resources  # noqa: F401 — importorskip guard in callers

    resource_type = resource.get("resourceType")
    if not resource_type:
        return [
            ResourceFailure(
                resource_type="Unknown",
                resource_index=resource_index,
                invariant_id="schema-missing-resourcetype",
                message="resourceType is required",
            )
        ]

    try:
        import importlib

        module = importlib.import_module(f"fhir.resources.{resource_type.lower()}")
        model_cls = getattr(module, resource_type)
        model_cls.model_validate(resource)
        return []
    except Exception as exc:
        return [
            ResourceFailure(
                resource_type=resource_type,
                resource_index=resource_index,
                invariant_id="schema-validation-error",
                message=str(exc),
            )
        ]


class InvariantResult(BaseModel):
    passed: bool | None
    expression: str
    message: str = ""


def validate_invariant(
    resource: dict,
    *,
    expression: str,
    invariant_id: str = "invariant",
    severity: str = "error",
) -> InvariantResult:
    """Evaluate a FHIRPath expression against a resource.

    Returns InvariantResult with passed=True/False/None (None = unsupported expression).
    """
    try:
        import fhirpathpy

        result = fhirpathpy.compile(expression)(resource, {})  # type: ignore[call-arg]
        if isinstance(result, list):
            passed = bool(result and result[0])
        else:
            passed = bool(result)
        return InvariantResult(passed=passed, expression=expression)
    except Exception as exc:
        return InvariantResult(passed=None, expression=expression, message=str(exc))

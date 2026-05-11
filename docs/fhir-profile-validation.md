# FHIR Profile Validation (US Core 6.1.0)

v0.3.0 adds conformance validation for generated FHIR resources against the [HL7 US Core 6.1.0](https://hl7.org/fhir/us/core/STU6.1/) Implementation Guide.

## Overview

Validation runs three composable checks per resource:

1. **Schema** — every resource is parsed through the corresponding [`fhir.resources`](https://pypi.org/project/fhir.resources/) Pydantic v2 model. Catches malformed types, bad date formats, and missing-from-the-base-spec fields.
2. **Required-field cardinality** — top-level elements with `min ≥ 1` in the US Core snapshot StructureDefinition (e.g. `Patient.identifier`, `Patient.name`, `Patient.gender`) are checked against the input. Missing fields surface as `us-core-required-<field>` failures.
3. **FHIRPath invariants** — every constraint keyed `us-core-*` in the bundled StructureDefinition is compiled by [`fhirpathpy`](https://pypi.org/project/fhirpathpy/) and evaluated in the context of the element it's defined on (e.g. `us-core-6` evaluates against each `Patient.name` entry, not the root Patient).

Failures with `severity="error"` mark the report as failing; `severity="warning"` does not.

## Coverage

10 resource types are validated. Anything outside this set passes through with its `resourceType` recorded in `report.skipped`.

| Resource              | Profile |
|-----------------------|---------|
| Patient               | `us-core-patient` |
| Practitioner          | `us-core-practitioner` |
| Organization          | `us-core-organization` |
| Encounter             | `us-core-encounter` |
| Condition             | `us-core-condition-problems-health-concerns` |
| Observation           | `us-core-observation-lab` |
| MedicationRequest     | `us-core-medicationrequest` |
| AllergyIntolerance    | `us-core-allergyintolerance` |
| Procedure             | `us-core-procedure` |
| DocumentReference     | `us-core-documentreference` |

Source: HL7 US Core IG 6.1.0 snapshot StructureDefinitions, bundled in `src/portiere/standards/fhir_profiles/us_core_6_1_0/`. The fetch recipe is `scripts/build_us_core_profiles.py`.

## Permissive ValueSet binding (v0.3.0 default)

v0.3.0 does **not** enforce ValueSet binding strictness. Codes outside a bound ValueSet pass validation. Strict mode (failing on out-of-VS codes) lands in v0.3.x as `--strict-bindings`.

Rationale: real-world clinical data routinely uses local extensions and pre-coordinated codes that don't map cleanly to the bound VS. Failing those at validation time would block the export path for most users on day one. The permissive default lets you discover and triage drift; strict mode is opt-in once you know your dataset.

## Usage

### Install the extra

```bash
pip install "portiere-health[fhir]"
```

This adds `fhir.resources>=7.0.0` and `fhirpathpy>=0.1.0`.

### Python API

```python
from portiere.models.project import Project

project = Project(id="example", name="us-core-demo")

resources = [
    {
        "resourceType": "Patient",
        "id": "p1",
        "identifier": [{"system": "urn:oid:2.16.840.1.113883.4.1", "value": "111-22-3333"}],
        "name": [{"family": "Doe", "given": ["Jane"]}],
        "gender": "female",
    },
]

report = project.validate(fhir_profile="us-core-6.1.0", resources=resources)

print(report.profile)         # "us-core-6.1.0"
print(report.total_resources) # 1
print(report.passed)          # True
print(report.failures)        # []
print(report.skipped)         # []
```

`ProfileValidationReport` is a Pydantic model — `report.model_dump(mode="json")` for serialization.

### CLI

```bash
portiere validate \
  --fhir-profile us-core-6.1.0 \
  --input resources.json
```

Input is a JSON array of FHIR resource dicts. The command exits non-zero on any error-severity failure and prints each failure to stderr.

## Failure shape

```python
ResourceFailure(
    resource_type="Patient",
    resource_index=0,
    invariant_id="us-core-required-identifier",
    message="Patient.identifier is required (min=1) by US Core",
    severity="error",
)
```

`invariant_id` prefixes:
- `schema-*` — `fhir.resources` couldn't parse the resource.
- `us-core-required-<field>` — top-level required field missing.
- `us-core-<n>` — a US Core FHIRPath invariant evaluated `false`.

## Direct module use

If you want validation without going through `Project`:

```python
from portiere.quality.fhir_profile.us_core import validate_against_us_core

report = validate_against_us_core(resources)
```

The lower-level building blocks (`validate_resource_schema`, `validate_invariant`) are also exported from `portiere.quality.fhir_profile.validator` if you're composing a custom profile.

## Future profiles

- **mCODE** (oncology) and **IPS** (International Patient Summary) — v0.3.x.
- **Custom profiles** — drop a snapshot StructureDefinition JSON next to the US Core ones and follow the `validate_against_us_core` pattern. Documented examples will land alongside mCODE/IPS support.

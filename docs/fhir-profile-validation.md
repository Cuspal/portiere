# FHIR Profile Validation (US Core 6.1.0, mCODE 2.0.0)

v0.3.0 adds conformance validation against the [HL7 US Core 6.1.0](https://hl7.org/fhir/us/core/STU6.1/) Implementation Guide. v0.3.1 extends the framework with [mCODE STU3 2.0.0](https://hl7.org/fhir/us/mcode/STU3/) (minimal Common Oncology Data Elements).

## Overview

Validation runs three composable checks per resource:

1. **Schema** — every resource is parsed through the corresponding [`fhir.resources`](https://pypi.org/project/fhir.resources/) Pydantic v2 model. Catches malformed types, bad date formats, and missing-from-the-base-spec fields.
2. **Required-field cardinality** — top-level elements with `min ≥ 1` in the US Core snapshot StructureDefinition (e.g. `Patient.identifier`, `Patient.name`, `Patient.gender`) are checked against the input. Missing fields surface as `us-core-required-<field>` failures.
3. **FHIRPath invariants** — every constraint keyed `us-core-*` in the bundled StructureDefinition is compiled by [`fhirpathpy`](https://pypi.org/project/fhirpathpy/) and evaluated in the context of the element it's defined on (e.g. `us-core-6` evaluates against each `Patient.name` entry, not the root Patient).

Failures with `severity="error"` mark the report as failing; `severity="warning"` does not.

## Coverage

### US Core 6.1.0 — 10 resource types

Validation keys on `resourceType`. Anything outside this set passes through with its `resourceType` recorded in `report.skipped`.

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

### mCODE STU3 2.0.0 — 5 core oncology profiles

mCODE uses `meta.profile` (not `resourceType`) to claim a profile — one base `Patient` is also an mCODE `CancerPatient` only when `meta.profile` contains the CancerPatient canonical URL. Resources without an mCODE `meta.profile` claim pass through to `report.skipped`.

| Profile (stem)            | Claimed via `meta.profile` URL substring             | Base resource type |
|---------------------------|------------------------------------------------------|--------------------|
| `CancerPatient`           | `mcode-cancer-patient`                               | Patient            |
| `PrimaryCancerCondition`  | `mcode-primary-cancer-condition`                     | Condition          |
| `CancerDiseaseStatus`     | `mcode-cancer-disease-status`                        | Observation        |
| `CancerStage`             | `mcode-cancer-stage`                                 | Observation        |
| `TNMStageGroup`           | `mcode-tnm-stage-group`                              | Observation        |

Source: HL7 IG snapshot StructureDefinitions, bundled in `src/portiere/standards/fhir_profiles/{us_core_6_1_0,mcode_2_0_0}/`. The fetch recipe is `scripts/build_fhir_profiles.py` (single script covers both profile sets).

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

# mCODE — same shape:
portiere validate \
  --fhir-profile mcode-2.0.0 \
  --input oncology_resources.json
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

- **mCODE expanded** (treatments, staging beyond the v0.3.1 core 5: medication-administration, radiotherapy, surgical-procedure, additional staging systems) — v0.3.2.
- **IPS** (International Patient Summary) — v0.3.2.
- **Strict ValueSet binding** (`--strict-bindings`) — v0.3.x.
- **Custom profiles** — drop a snapshot StructureDefinition JSON under `src/portiere/standards/fhir_profiles/<my_profile>/` and write a thin orchestrator mirroring `us_core.py` or `mcode.py` (the two existing reference implementations).

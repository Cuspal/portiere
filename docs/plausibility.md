# Plausibility Validation

Portiere's data-quality validation follows Kahn et al.'s three-category framework: **completeness**, **conformance**, and **plausibility**. v0.2.0 introduces a real plausibility score replacing v0.1.0's tautological metric (which was just the GX expectation success rate under a different name).

Plausibility is the "does this clinical record make sense?" check: birth ≤ death, condition_concept_id references a row in the `concept` table, observation status is in the FHIR ValueSet. Failures don't necessarily mean the data is wrong — sometimes a real-world edge case violates the rule — but they always merit attention.

## Architecture

Plausibility uses a **hybrid grammar**:

- **YAML DSL** in standards files declares column-level rules drawn from a fixed set of 5 rule types. Custom standards can declare rules without writing Python.
- **Python rules** in `omop.py` / `fhir.py` handle multi-table joins, aggregates, and ValueSet checks the DSL doesn't express.

Both run during `GXValidator.validate()` and feed into the same plausibility score.

## YAML DSL

### Rule types (locked for v0.2.0)

The DSL has exactly five rule types. Adding a sixth is a v0.3.0 conversation, not a v0.2.0 patch.

| Type | Purpose | Example |
|---|---|---|
| `range` | Numeric constraint | `year_of_birth in [1900, 2030]` |
| `regex` | String pattern match | ICD-10 code format `^[A-Z]\d{2}(\.\d+)?$` |
| `enum` | Value in a fixed list | `Observation.status in [final, preliminary, ...]` |
| `temporal_order` | Per-row date ordering | `birth_datetime <= death_datetime` |
| `fk_exists` | Referential integrity | `condition_concept_id` exists in `concept.concept_id` |

### Severity

Two levels: `error` (default) and `warn`.

- **`error`** — failure means the row is genuinely problematic. Fails validation; CI fails; integrators investigate.
- **`warn`** — failure is a flag, not a hard reject. Reported in the rule-results list but doesn't fail validation.

`info` is intentionally not a level — three levels become a sorting problem.

### Authoring rules

Add a `plausibility:` block to any entity in a standard YAML file. Example from OMOP `condition_occurrence`:

```yaml
condition_occurrence:
  fields:
    # ... existing fields ...
  plausibility:
    - id: condition_concept_fk
      type: fk_exists
      column: condition_concept_id
      ref_table: concept
      ref_column: concept_id
      severity: error
    - id: condition_start_before_end
      type: temporal_order
      before: condition_start_date
      after: condition_end_date
      severity: error
```

### Rule grammar reference

```yaml
- id: <unique_string>           # required, used in result IDs and error messages
  type: range | regex | enum | temporal_order | fk_exists
  severity: error | warn        # default: error

  # type=range
  column: <field_name>
  min: <number>                 # optional
  max: <number>                 # optional

  # type=regex
  column: <field_name>
  pattern: <python_regex>

  # type=enum
  column: <field_name>
  values: [<v1>, <v2>, ...]

  # type=temporal_order
  before: <field_name>
  after: <field_name>

  # type=fk_exists
  column: <field_name>
  ref_table: <table_name>
  ref_column: <column_name>
```

### Behaviour

- **Missing optional column → skipped.** If the column referenced by the rule isn't present in the validated DataFrame, the rule passes with `detail="column 'X' not in DataFrame (skipped)"`. Optional columns are common in healthcare (a hospital that doesn't collect `death_date`); completeness checks already cover required-column presence.
- **Null values are excluded.** Plausibility rules only consider non-null values. A column that's 100% null produces `total_rows=0` and passes trivially.
- **`fk_exists` requires the reference table.** The validator's `validate(df, table, model, ref_tables=...)` parameter accepts a `dict[str, pd.DataFrame]` mapping table names to DataFrames. If the referenced table isn't supplied, the rule skips.

## Python rules (built-in, OMOP/FHIR)

Some checks don't fit the DSL — they need joins or aggregates or domain knowledge. These live in `src/portiere/quality/plausibility/omop.py` and `fhir.py`.

### Currently shipped

**OMOP CDM v5.4** (`src/portiere/quality/plausibility/omop.py`):

| Rule | Severity | What it checks |
|---|---|---|
| `birth_before_death` | error | `birth_datetime <= death_datetime` for any person with both fields |
| `condition_dates_consistent` | error | `condition_start_date <= condition_end_date` |
| `concept_id_fk` | error | Every `*_concept_id` references a row in `concept` (DuckDB join) |
| `domain_match` | error | `condition_concept_id` references concepts with `domain_id == 'Condition'`; same pattern for drug/measurement |
| `age_in_range` | warn | Derived age (`reference_year - year_of_birth`) is in [0, 125] |

**FHIR R4** (`src/portiere/quality/plausibility/fhir.py`):

| Rule | Severity | What it checks |
|---|---|---|
| `patient_birthdate_not_future` | error | `Patient.birthDate` ≤ today |
| `observation_status_in_valueset` | error | `Observation.status` in the FHIR observation-status ValueSet |
| `medication_request_intent_in_valueset` | error | `MedicationRequest.intent` in the FHIR medicationrequest-intent ValueSet |

### Dispatch

`src/portiere/quality/plausibility/registry.py` decides which Python rules to invoke for a given `(target_model, entity)` pair. The validator calls `run_python_rules(target_model_name, entity, df, ref_tables=...)` and gets back a list of `RuleResult` objects merged with the YAML DSL results.

### Adding a Python rule

For built-in standards (OMOP / FHIR), contribute upstream:

1. Add a function to `omop.py` or `fhir.py` returning a `RuleResult` (or list thereof).
2. Wire it into the registry's dispatch table.
3. Test it in `tests/test_plausibility_omop.py` or `_fhir.py`.

Custom standards can't ship Python rules without contributing upstream — the `[quality]` extra ships only the built-in dispatch. If you need a custom standard's plausibility check that doesn't fit the DSL, the v0.2.0 path is to compute it in your own code and feed the result into the validator alongside the standard one. Plugin-style custom Python rules are a v0.3.0 conversation.

## FK validation backend: DuckDB

`fk_exists` rules and `concept_id_fk` / `domain_match` Python rules use DuckDB to join the validated table against the reference table. DuckDB registers pandas DataFrames as views with no copy, so this scales to full-Athena-sized vocabulary tables (~10M concepts) without OOM.

DuckDB is part of the `portiere-health[quality]` extra.

## Score computation

`GXValidator.validate()` returns a `ValidationReport` with:

| Score | Computed from | Bucket |
|---|---|---|
| `completeness_score` | `expect_column_to_exist` GX expectations only | Required columns present |
| `conformance_score` | other GX expectations (type, range, etc.) | Type/format constraints |
| `plausibility_score` | error-tier plausibility rules with `total_rows > 0` | Cross-table / domain rules |
| `overall_success_score` | all GX expectations (renamed from v0.1.0's misleading `plausibility_score`) | Smoke metric |

`passed = completeness >= threshold AND conformance >= threshold AND plausibility >= threshold AND no error-tier rule failed`

A failing **warn**-tier rule never makes `passed = false` — but it still appears in `plausibility_rule_results` for review.

## Inspecting results

```python
report = validator.validate(df, "person", "omop_cdm_v5.4")
for r in report["plausibility_rule_results"]:
    if not r["passed"]:
        print(
            f"  {r['severity']:5s}  {r['rule_id']:30s}  "
            f"{r['failed_count']}/{r['total_rows']} failed  {r['detail']}"
        )
```

## See also

- [`GXValidator` reference](documentations/02-unified-api-reference.md)
- [Spec §4.1](../specs/2026-04-29-v0.2.0-release-design.md) — design rationale + alternatives considered
- Kahn et al., *A Harmonized Data Quality Assessment Terminology and Framework for the Secondary Use of Electronic Health Record Data*, eGEMs 2016 — the conceptual basis for the three-category model

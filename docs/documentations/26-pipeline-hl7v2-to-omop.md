# Pipeline Tutorial: HL7 v2 → OMOP CDM

The supported route is **two explicit hops** — HL7 v2 → FHIR R4 → OMOP — plus
offline concept normalization of the carried diagnosis codes. This mirrors
real-world practice (v2 feeds usually land as FHIR first) and keeps each hop
auditable.

Runnable notebook: [`25_pipeline_hl7v2_to_omop.ipynb`](https://github.com/Cuspal/portiere/blob/main/docs/notebooks_examples/25_pipeline_hl7v2_to_omop.ipynb).

## The route

```
ADT feed ──parse──► segments ──hop 1──► FHIR resources ──hop 2──► OMOP rows ──► concept map
                              hl7v2 → fhir_r4          fhir_r4 → omop         (Stage 3, offline)
```

Portiere ships `hl7v2 → fhir_r4` and `fhir_r4 → omop` crossmaps; there is
deliberately no direct `hl7v2 → omop` map — chaining the two shipped maps is
the supported route.

## Hop 1 — HL7 v2 → FHIR

Identical to the [HL7 v2 → FHIR tutorial](25-pipeline-hl7v2-to-fhir.md): parse
ER7 into segment dicts, `map_record` per segment, inject `resourceType` from
the entity map. The notebook adds a batch splitter for multi-message feeds
(split on `MSH` lines).

## Hop 2 — FHIR → OMOP

```python
fhir_to_omop = CrossStandardMapper("fhir_r4", "omop_cdm_v5.4")
fhir_to_omop.get_entity_map()
# {'Patient': 'person', 'Encounter': 'visit_occurrence',
#  'Condition': 'condition_occurrence', 'MedicationRequest': 'drug_exposure',
#  'Observation': 'measurement', ...}

omop_rows = {}
for r in fhir_resources:
    table = fhir_to_omop.get_entity_map()[r["resourceType"]]
    omop_rows.setdefault(table, []).append(
        fhir_to_omop.map_record(r["resourceType"], r))
```

Write each table list to CSV (the notebook uses polars) and you have
structurally-OMOP data: `person`, `visit_occurrence`, `condition_occurrence`.

## Concept normalization — the step chaining can't do

Crossmaps move **structure**; the diagnosis codes ride through as source
values. Landing standard `condition_concept_id`s is Stage-3 work:

```python
concept_map = project.map_concepts(codes=[
    {"code": "I10", "description": "Essential (primary) hypertension", "count": 1},
    {"code": "E11.9", "description": "Type 2 diabetes mellitus …", "count": 1},
])
```

The notebook runs this against the bundled offline vocabulary (BM25 +
`offline=True`); production uses your full Athena export with SNOMED so ICD-10
codes resolve to SNOMED standard concepts via `Maps to`. Join the resulting
`source_code → target_concept_id` lookup onto `condition_occurrence`.

## Production notes

- **Persist the intermediate FHIR.** Hop-1 output is a legitimate artifact —
  auditable, re-playable into other targets, and the natural place for profile
  validation.
- **Determinism**: both crossmaps are pure YAML transforms — same input, same
  output; version the crossmap files with your ETL release.
- **person de-duplication**: an ADT feed re-states PID per message; dedupe
  `person` rows on the source MRN before loading (the notebook's demo feed has
  distinct patients, so it elides this).

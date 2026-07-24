# Pipeline Tutorial: Custom EHR → FHIR R4

The standardize pipeline aimed at FHIR R4 — same five stages as the
[OMOP tutorial](23-pipeline-custom-ehr-to-omop.md), different target universe —
ending in a transaction **Bundle** and **NDJSON** export.

Runnable notebook: [`23_pipeline_custom_ehr_to_fhir.ipynb`](https://github.com/Cuspal/portiere/blob/main/docs/notebooks_examples/23_pipeline_custom_ehr_to_fhir.ipynb)
(fully offline, executed with outputs).

## The route

```
lab_results.csv ──ingest──► schema map (FHIR) ──► concept map (LOINC) ──► ETL ──► Bundle / NDJSON
```

## What changes vs the OMOP pipeline

Exactly one line:

```python
project = portiere.init(
    name="ehr-to-fhir",
    target_model="fhir_r4",        # ← the whole target universe swaps
    vocabularies=["LOINC"],
    config=config,                  # identical offline config
)
```

Schema mapping now aims at FHIR resources/elements — `lab_code →
Observation.code`, `lab_value → Observation.valueQuantity`, `patient_id →
Patient.id`. Portiere ships 18 R4 resources; coverage matrix:
[multi-standard support](20-multi-standard-support.md).

> Since v0.4.0 the local schema mapper honors non-OMOP targets correctly —
> earlier versions silently mapped against OMOP regardless of `target_model`
> (fixed; regression-tested).

## Source column naming

The offline configuration (no embedding model) relies on the standard's
`source_patterns` for schema matching. The notebook's columns (`patient_id`,
`lab_code`, `lab_value`, `lab_unit`, `lab_date`) are real FHIR source
patterns. With SapBERT enabled (production default), cryptic names like
`obs_val_num` match semantically as well.

## Concept mapping

```python
concept_map = project.map_concepts(source=src_obs, code_columns=["lab_code"])
```

LOINC codes resolve against the bundled subset offline; against your full
Athena export in production.

## Bundle + NDJSON export

```python
from portiere.export.fhir.bundle import to_transaction_bundle
from portiere.export.fhir.ndjson import to_ndjson_files

bundle = to_transaction_bundle(resources)          # POST-ready transaction
paths = to_ndjson_files(resources, out_dir=out)    # bulk-import files
```

Each resource dict must carry `resourceType`; the notebook assembles
Observation resources from the ETL rows. Validate against US Core / mCODE
before shipping:

```bash
portiere validate --fhir-profile us-core-6.1.0 --input resources.json
```

([FHIR profile validation](../fhir-profile-validation.md) ·
[Bundle export](../fhir-bundle-export.md))

## Going to production

Same checklist as the [OMOP tutorial](23-pipeline-custom-ehr-to-omop.md#going-to-production),
plus: pick the FHIR profiles you must conform to **first** — profile
constraints (required elements, bindings) shape which source columns are
mandatory, and `portiere export --fhir-profile ...` can gate writes on
validation.

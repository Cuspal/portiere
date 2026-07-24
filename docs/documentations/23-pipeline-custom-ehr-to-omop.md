# Pipeline Tutorial: Custom EHR → OMOP CDM v5.4

End-to-end standardization of a messy hospital EHR export to OMOP — ingest →
schema mapping → concept mapping → ETL → validation — **fully offline**.

Runnable notebook: [`22_pipeline_custom_ehr_to_omop.ipynb`](https://github.com/Cuspal/portiere/blob/main/docs/notebooks_examples/22_pipeline_custom_ehr_to_omop.ipynb)
(executed top-to-bottom with outputs; no network, no model downloads, no keys).

## The route

```
custom EHR CSVs ──ingest/profile──► schema map ──► concept map ──► ETL ──► OMOP tables ──► validate
                     (Stage 1)       (Stage 2)      (Stage 3)    (Stage 4)                (Stage 5)
```

## Prerequisites

```bash
pip install "portiere-health[polars]"        # core pipeline
pip install "portiere-health[quality]"       # optional: Stage-5 validation
pip install "portiere-health[xlsx]"          # optional: Excel deliverables
```

## Step 1 — The source

Real exports have inconsistent names and string-typed everything. The notebook
synthesizes a `patients.csv` (patient_id, sex, date_of_birth, race) and a
`diagnoses.csv` (patient_id, diagnosis_code, diagnosis_date) — column names a
hospital extract plausibly uses. Swap in your own files; nothing else changes.

## Step 2 — Offline project configuration

The reliability core of the tutorial:

```python
from portiere._demo_data import vocabulary_dir
from portiere.config import (
    EmbeddingConfig, KnowledgeLayerConfig, PortiereConfig, RerankerConfig,
)
from portiere.knowledge import build_knowledge_layer

knowledge_paths = build_knowledge_layer(
    athena_path=str(vocabulary_dir()),          # bundled ICD10CM/LOINC/RxNorm subset
    output_path=str(WORK / "knowledge_index"),
    backend="bm25s",
    vocabularies=["ICD10CM", "LOINC", "RxNorm"],
)

config = PortiereConfig(
    local_project_dir=WORK / "project",
    knowledge_layer=KnowledgeLayerConfig(backend="bm25s", **knowledge_paths),
    embedding=EmbeddingConfig(provider="none"),   # no SapBERT download
    reranker=RerankerConfig(provider="none", model=""),
    offline=True,                                  # hard no-egress guarantee
)
```

Three deliberate choices:

| Choice | Tutorial reason | Production setting |
|---|---|---|
| `embedding=none` | zero downloads; mapping rides source patterns + BM25 | SapBERT (`huggingface`, default) widens coverage for cryptic names |
| bundled vocabulary | license-clean, ships in the wheel | your own Athena export incl. SNOMED ([vocabulary setup](15-vocabulary-setup.md)) |
| `offline=True` | provable: `portiere doctor --assert-no-egress` | keep it on unless you opt into BYO-LLM |

## Step 3-5 — Ingest, schema map, concept map

```python
project = portiere.init(name="ehr-to-omop", target_model="omop_cdm_v5.4",
                        vocabularies=["ICD10CM", "LOINC", "RxNorm"], config=config)

src_dx = project.add_source(str(src_dir / "diagnoses.csv"), name="diagnoses")
schema_map = project.map_schema(src_dx)
concept_map = project.map_concepts(source=src_dx, code_columns=["diagnosis_code"])
```

Every schema item lands with a confidence and routing status
(`AUTO_ACCEPTED` / `NEEDS_REVIEW` / `UNMAPPED`) — review workflow:
[Mapping Review UI](18-mapping-review-workflow.md). Concept items carry
auto/review/manual routing per the configured thresholds.

`code_columns` is explicit for determinism; omitting it lets Stage-1
auto-detection pick likely code columns.

## Step 6-7 — ETL and validation

```python
result = project.run_etl(src_dx, output_dir=str(etl_out),
                         schema_mapping=schema_map, concept_mapping=concept_map)
report = project.validate(output_path=str(etl_out))   # requires [quality]
```

The ETL emits per-table CSVs (e.g. `condition_occurrence.csv`) plus standalone
scripts + lookup tables that run **without Portiere installed**. Validation
scores completeness / conformance / plausibility (Kahn-aligned).

## Step 8 — Deliverables & reproducibility

Every run writes `manifest.lock.json` (replayable: `portiere replay`). The
stakeholder-facing Excel deliverables come from the same objects:

```python
from portiere.reports import build_schema_workbook
build_schema_workbook(schema_map, "schema_working.xlsx", standard="omop_cdm_v5.4")
```

See [Deliverable workbooks](../workbooks.md).

## Going to production

1. Build the knowledge layer from your full Athena export (SNOMED included).
2. Re-enable SapBERT (`EmbeddingConfig()` default) — or keep offline-lexical if
   your governance requires zero downloads.
3. Tune thresholds (`ThresholdsConfig`) against a labeled sample.
4. Route `NEEDS_REVIEW` items through the [Review UI](18-mapping-review-workflow.md).
5. Scrub PHI from profile artifacts: `scrub_phi=True` ([PHI scrubbing](../phi-scrubbing.md)).

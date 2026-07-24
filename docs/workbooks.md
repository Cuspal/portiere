# Excel Deliverable Workbooks (v0.4.0)

OMOP conversion teams hand-build three artifacts today: a White Rabbit scan
report, a per-table schema-mapping "working document", and a
`source_to_concept_map` workbook with a status sheet. `portiere workbook`
generates all three — styled, navigable, provenance-stamped, and impossible to
drift from the CDM version Portiere actually validates against.

```bash
pip install "portiere-health[polars,xlsx]"
```

## 1. Data-scan workbook (White-Rabbit parity+)

```bash
portiere workbook profile data/*.csv -o scan.xlsx --system HIS --scrub-phi
```

Sheets: hyperlinked **TOC** · **Table Overview** (rows/fields/completeness data
bars) · **Field Overview** (type, present %, distinct, top value + share,
length range, example, and min/max/mean/std for numerics) · one
**value-frequency sheet per source** (top-10 per field, truncation labeled —
not silent). `--scrub-phi` runs the value-level [PHI scrubber](phi-scrubbing.md)
over examples and value lists first.

## 2. Schema-mapping working workbook

```bash
portiere workbook schema --mapping schema_mapping_reviewed.json -o schema.xlsx
```

One sheet per target table, rows = **all** fields of the standard
(`--standard omop_cdm_v5.4` by default): spec M/O, data type, vocabulary, and
description come from the standard YAML — never hand-copied. Mapped rows carry
source table/column, confidence, and color-coded status; unmapped required
fields appear explicitly as `UNMAPPED` — the work queue, not a silent blank.
The **Summary** sheet tracks required-field coverage per table.

Input: the `schema_mapping_reviewed.json` written by the
[Mapping Review UI](mapping-review-ui.md) (or the original
`schema_mapping.yaml`).

## 3. Concept-mapping workbook

```bash
portiere workbook concepts --mapping concept_mapping.json -o concepts.xlsx \
    --vocab-reference "HIS2_popupentry.popupkey"
```

Sheets: **Source_to_Concept_Map** (OMOP-standard columns, plus
`target_concept_name`, `confidence`, `method` color-coded to the right) ·
**vocabulary** · **status** (generated per-vocabulary routing counts —
auto/review/manual and auto %) · **readme** (tool version, timestamp, method
legend, regeneration command).

Input: a JSON dump of a `ConceptMapping` —
`Path("concept_mapping.json").write_text(concept_map.model_dump_json())`.

## Python API

```python
from portiere.reports import (
    build_profile_workbook, build_schema_workbook, build_concept_workbook,
)

build_profile_workbook(profiles, "scan.xlsx", title="HIS scan")
build_schema_workbook(schema_mapping, "schema.xlsx", standard="omop_cdm_v5.4")
build_concept_workbook(concept_map, "concepts.xlsx")
```

## Design notes

- These are **generated deliverables** — edit mappings in the review workflow,
  then regenerate; don't hand-edit the workbooks.
- Value sheets show the engine's top-10 values per field. For a deeper scan of
  a specific column, use `engine.get_distinct_values`.
- Numeric quartiles (25/50/75%) are not yet included (White Rabbit has them);
  planned alongside the workbook-import round-trip.

# Mapping Review Workflow

This guide covers the complete review workflow for schema and concept mappings — how to browse mapping results, approve or reject individual items, override with manual corrections, export to CSV for clinical SME review, and reload edited files back into the SDK.

---

## Table of Contents

1. [Overview](#overview)
2. [Confidence Routing](#confidence-routing)
3. [Schema Mapping Review](#schema-mapping-review)
4. [Concept Mapping Review](#concept-mapping-review)
5. [Export to CSV for SME Review](#export-to-csv-for-sme-review)
6. [Reload from Edited CSV](#reload-from-edited-csv)
7. [Bulk Operations](#bulk-operations)
8. [Save Reviewed Mappings](#save-reviewed-mappings)
9. [End-to-End Workflow](#end-to-end-workflow)
10. [Quick Reference](#quick-reference)

---

## Overview

After Portiere runs the mapping pipeline, each mapping item has a **confidence score** and an automatically assigned **status**. The review workflow lets you:

1. **Browse** — See what was auto-accepted, what needs review, and what is unmapped
2. **Approve** — Accept the AI suggestion as-is
3. **Reject** — Mark an item as not mappable
4. **Override** — Replace the AI suggestion with a manual correction
5. **Export** — Send mappings to a CSV file for clinical SME review in Excel or Google Sheets
6. **Reload** — Import the edited CSV back into the SDK
7. **Save** — Persist reviewed mappings to local project storage

---

## Confidence Routing

After `map_schema()` or `map_concepts()`, each item is assigned a status based on its confidence score:

| Confidence Range | Status | Meaning |
|-----------------|--------|---------|
| >= 0.95 | `auto_accepted` / `auto_mapped` | High confidence — accepted automatically |
| 0.70 – 0.95 | `needs_review` | Medium confidence — needs human review |
| < 0.70 | `unmapped` / `manual_required` | Low confidence — needs manual mapping |

These thresholds are configurable via `ThresholdsConfig`:

```python
from portiere.config import (
    PortiereConfig,
    ThresholdsConfig,
    SchemaMappingThresholds,
    ConceptMappingThresholds,
)

config = PortiereConfig(
    thresholds=ThresholdsConfig(
        schema_mapping=SchemaMappingThresholds(
            auto_accept=0.90,    # Lower auto-accept threshold
            needs_review=0.70,   # Lower review threshold
        ),
        concept_mapping=ConceptMappingThresholds(
            auto_accept=0.95,
            needs_review=0.70,
        ),
    )
)
```

---

## Schema Mapping Review

### Run the Mapping

```python
schema_map = project.map_schema(patients_source)
```

### Browse Results

```python
# Summary statistics
summary = schema_map.summary()
print(f"Total:          {summary['total']}")
print(f"Auto-accepted:  {summary['auto_accepted']}")
print(f"Needs review:   {summary['needs_review']}")
print(f"Unmapped:       {summary['unmapped']}")
print(f"Auto rate:      {summary['auto_rate']:.1f}%")
```

### Filter by Status

```python
# High-confidence items accepted automatically
for item in schema_map.auto_accepted():
    print(f"{item.source_column} → {item.target_table}.{item.target_column} ({item.confidence:.2f})")

# Items that need human review
for item in schema_map.needs_review():
    print(f"{item.source_column} → {item.target_table}.{item.target_column} ({item.confidence:.2f})")
    # Show alternative candidates
    for c in (item.candidates or [])[:3]:
        print(f"  alt: {c['target_table']}.{c['target_column']} ({c['confidence']:.2f})")
```

### Approve a Mapping

Accept the AI suggestion as-is:

```python
schema_map.approve("patient_id")

item = schema_map.get_item("patient_id")
print(f"Status: {item.status.value}")  # → "approved"
```

### Reject a Mapping

Mark a column as not mappable to the target model:

```python
schema_map.reject("zip_code")

item = schema_map.get_item("zip_code")
print(f"Status: {item.status.value}")  # → "rejected"
```

### Override a Mapping

Replace the AI suggestion with a different target:

```python
schema_map.override(
    "date_of_birth",
    target_table="person",
    target_column="birth_datetime",
)

item = schema_map.get_item("date_of_birth")
print(f"Status: {item.status.value}")  # → "approved" (with override)
print(f"Override target: {item.override_target_table}.{item.override_target_column}")
```

The `effective_target_table` and `effective_target_column` properties always return the active target, whether it's the original suggestion or the override.

### Get a Specific Item

```python
item = schema_map.get_item("patient_id")
# Raises KeyError if not found
```

---

## Concept Mapping Review

### Run the Mapping

```python
concept_map = project.map_concepts(source=diagnoses_source)
```

### Browse Results

```python
summary = concept_map.summary()
print(f"Total:           {summary['total']}")
print(f"Auto-mapped:     {summary['auto_mapped']}")
print(f"Needs review:    {summary['needs_review']}")
print(f"Manual required: {summary['manual_required']}")
```

### Filter by Status

```python
# Auto-mapped concepts
for item in concept_map.auto_mapped():
    print(f"{item.source_code} → {item.target_concept_name} ({item.confidence:.2f})")

# Concepts needing review
for item in concept_map.needs_review():
    print(f"{item.source_code}: {item.source_description} ({item.confidence:.2f})")
```

### Approve

```python
concept_map.approve("E11.9")

item = concept_map.get_item("E11.9")
print(f"Method: {item.method.value}")  # → "auto"
```

### Reject

```python
concept_map.reject("Z87.891")

item = concept_map.get_item("Z87.891")
print(f"Method: {item.method.value}")  # → "unmapped"
```

### Override

Specify an exact target concept:

```python
concept_map.override(
    "I10",
    concept_id=320128,
    concept_name="Essential hypertension",
    vocabulary_id="SNOMED",
)

item = concept_map.get_item("I10")
print(f"Method: {item.method.value}")           # → "override"
print(f"Target: {item.target_concept_name}")     # → "Essential hypertension"
print(f"ID:     {item.target_concept_id}")       # → 320128
```

---

## Export to CSV for SME Review

Export mappings to CSV so clinical subject matter experts can review them in Excel or Google Sheets.

### Schema Mapping

```python
# Export to pandas DataFrame
df = schema_map.to_dataframe()
print(df)

# Export directly to CSV
schema_map.to_csv("schema_review.csv")
```

The CSV contains columns:

| Column | Description |
|--------|-------------|
| `source_column` | Original column name from source data |
| `target_table` | Suggested OMOP table |
| `target_column` | Suggested OMOP column |
| `confidence` | AI confidence score (0.0 – 1.0) |
| `status` | Current status (auto_accepted, needs_review, approved, rejected, unmapped) |

### Concept Mapping

```python
# Export to pandas DataFrame
concept_df = concept_map.to_dataframe()
print(concept_df)

# Export to CSV
concept_map.to_csv("concept_review.csv")

# Export in OMOP source_to_concept_map format
stcm = concept_map.to_source_to_concept_map()
print(f"OMOP source_to_concept_map: {len(stcm)} rows")
```

The DataFrame contains all columns from `to_dataframe()`:

| source_code | source_description | source_column | source_count | target_concept_id | target_concept_name | target_vocabulary_id | target_domain_id | confidence | method |
|---|---|---|---|---|---|---|---|---|---|
| E11.9 | Type 2 diabetes mellitus | diagnosis | 42 | 201826 | Type 2 diabetes mellitus | SNOMED | Condition | 0.98 | auto |
| R51 | Headache | diagnosis | 18 | 378253 | Headache | SNOMED | Condition | 0.96 | auto |
| Z87.891 | Personal history of NTD | diagnosis | 7 | 4099154 | History of nicotine dep. | SNOMED | Condition | 0.74 | review |
| X42.LOCAL | Custom lab code | lab_result | 3 | None | None | None | None | 0.00 | unmapped |

---

## Reload from Edited CSV

After the SME edits the CSV (changes statuses, corrects targets), reload it back:

### Schema Mapping

```python
from portiere.models.schema_mapping import SchemaMapping

reviewed_schema = SchemaMapping.from_csv("schema_review_edited.csv")
print(f"Reloaded {len(reviewed_schema.items)} mappings")
print(reviewed_schema.summary())
```

### Concept Mapping

```python
from portiere.models.concept_mapping import ConceptMapping

reviewed_concepts = ConceptMapping.from_csv("concept_review_edited.csv")
print(f"Reloaded {len(reviewed_concepts.items)} mappings")
print(reviewed_concepts.summary())
```

### CSV Editing Tips for SMEs

- **To approve**: Change `status` to `approved`
- **To reject**: Change `status` to `rejected`
- **To override a schema mapping**: Change `target_table` and/or `target_column`, set `status` to `approved`
- **To override a concept mapping**: Change `target_concept_id` and `target_concept_name`, set `method` to `override`
- Do not modify `confidence` — it is the original AI score

---

## Bulk Operations

### Approve All Remaining

Approve all items currently in `needs_review` status:

```python
# Schema mapping
remaining = len(schema_map.needs_review())
schema_map.approve_all()
print(f"Bulk approved {remaining} schema mappings")

# Concept mapping
remaining = len(concept_map.needs_review())
concept_map.approve_all()
print(f"Bulk approved {remaining} concept mappings")
```

### Finalize

Lock the mapping to prevent further changes. This is typically done before ETL generation:

```python
concept_map.finalize()
print(f"Finalized: {concept_map.finalized}")
```

After finalizing, `approve()`, `reject()`, and `override()` will raise an error.

---

## Save Reviewed Mappings

Save reviewed mappings back to the project's local storage:

```python
# Save schema mapping
project.save_schema_mapping(schema_map)

# Save concept mapping
project.save_concept_mapping(concept_map)

# Verify by reloading
reloaded = project.load_schema_mapping()
print(reloaded.summary())
```

In hybrid mode, saved artifacts can then be pushed to the cloud:

```python
project.push()  # Uploads all local artifacts to Portiere Cloud
```

---

## End-to-End Workflow

```
                         ┌──────────────┐
                         │   map_schema  │
                         │ map_concepts  │
                         └──────┬───────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
            auto_accepted()          needs_review()
            (>= 0.95)               (0.70 – 0.95)
                    │                       │
                    │              ┌────────┼────────┐
                    │              ▼        ▼        ▼
                    │          approve   reject   override
                    │              │        │        │
                    ▼              ▼        ▼        ▼
              ┌─────────────────────────────────────────┐
              │          to_csv("review.csv")           │
              │    (send to SME for review in Excel)    │
              └─────────────────┬───────────────────────┘
                                │
                         SME edits CSV
                                │
              ┌─────────────────┴───────────────────────┐
              │   SchemaMapping.from_csv("edited.csv")  │
              │  ConceptMapping.from_csv("edited.csv")  │
              └─────────────────┬───────────────────────┘
                                │
              ┌─────────────────┴───────────────────────┐
              │   project.save_schema_mapping(mapping)  │
              │  project.save_concept_mapping(mapping)  │
              └─────────────────┬───────────────────────┘
                                │
                     project.push()  (hybrid mode)
```

---

## Quick Reference

### Schema Mapping

```python
# Browse
schema_map.summary()                    # Status counts
schema_map.auto_accepted()              # High-confidence items
schema_map.needs_review()               # Items needing review
schema_map.rejected()                   # Rejected items
schema_map.overridden()                 # Overridden items
schema_map.get_item("column_name")      # Specific item by source column

# Review
schema_map.approve("column_name")       # Accept AI suggestion
schema_map.reject("column_name")        # Mark as not mappable
schema_map.override("column_name",      # Manual correction
    target_table="...",
    target_column="...",
)
schema_map.approve_all()                # Bulk approve all needs_review

# Export / Import
schema_map.to_dataframe()               # pandas DataFrame
schema_map.to_csv("review.csv")         # CSV for SME review
SchemaMapping.from_csv("edited.csv")    # Reload edited CSV

# Save
project.save_schema_mapping(schema_map)
```

### Concept Mapping

```python
# Browse
concept_map.summary()                   # Status counts
concept_map.auto_mapped()               # Auto-mapped items
concept_map.needs_review()              # Items needing review
concept_map.unmapped()                  # Unmapped items
concept_map.get_item("E11.9")           # Specific item by source code

# Review
concept_map.approve("E11.9")            # Accept AI suggestion
concept_map.reject("Z87.891")           # Mark as unmapped
concept_map.override("I10",             # Manual correction
    concept_id=320128,
    concept_name="Essential hypertension",
    vocabulary_id="SNOMED",
)
concept_map.approve_all()               # Bulk approve all needs_review

# Export / Import
concept_map.to_dataframe()              # pandas DataFrame
concept_map.to_csv("review.csv")        # CSV for SME review
ConceptMapping.from_csv("edited.csv")   # Reload edited CSV
concept_map.to_source_to_concept_map()  # OMOP source_to_concept_map format

# Save
project.save_concept_mapping(concept_map)
```

---

## See Also

- [07-data-models.md](./07-data-models.md) — Schema and concept mapping data models
- [17-hybrid-mode.md](./17-hybrid-mode.md) — Push/pull sync for team collaboration
- [19-review-api-endpoints.md](./19-review-api-endpoints.md) — REST API for review actions
- Notebook: `12_mapping_review_workflow.ipynb` — Interactive tutorial

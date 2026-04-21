# Cross-Standard Mapping Guide

Map data between any two clinical standards using Portiere's CrossStandardMapper. This guide covers the crossmap YAML format, built-in transforms, and the VocabularyBridge.

## Overview

Cross-standard mapping transforms data already in one standard (e.g., OMOP CDM) into another standard (e.g., FHIR R4). This is separate from source-to-standard mapping — here, both source and target are defined clinical standards.

## Quick Start

### Project-Level (Recommended)

Create a cross-map project and let Portiere infer source/target from project settings:

```python
import portiere
import polars as pl
from portiere.engines import PolarsEngine

project = portiere.init(
    name="OMOP to FHIR Export",
    task="cross_map",
    source_standard="omop_cdm_v5.4",
    target_model="fhir_r4",
    engine=PolarsEngine(),
)

# source/target inferred — just specify entity and data
omop_df = pl.read_csv("./omop_output/person.csv")
fhir_df = project.cross_map(source_entity="person", data=omop_df)
```

### Standalone (Low-Level)

```python
from portiere.local.cross_mapper import CrossStandardMapper

# Map OMOP person → FHIR Patient
mapper = CrossStandardMapper("omop_cdm_v5.4", "fhir_r4")
result = mapper.map_record("person", {
    "person_id": 12345,
    "gender_concept_id": 8507,
    "birth_datetime": "1990-05-15",
})
# {'id': '12345', 'gender': 'male', 'birthDate': '1990-05-15'}
```

## Available Crossmaps

Built-in crossmap definitions:

| Source | Target | File |
|--------|--------|------|
| OMOP CDM v5.4 | FHIR R4 | `omop_to_fhir_r4.yaml` |
| FHIR R4 | OMOP CDM v5.4 | `fhir_r4_to_omop.yaml` |
| HL7 v2.5.1 | FHIR R4 | `hl7v2_to_fhir_r4.yaml` |
| FHIR R4 | OpenEHR 1.0.4 | `fhir_r4_to_openehr.yaml` |
| OMOP CDM v5.4 | OpenEHR 1.0.4 | `omop_to_openehr.yaml` |

```python
from portiere.local.cross_mapper import list_crossmaps
for cm in list_crossmaps():
    print(f"{cm['source']} → {cm['target']}")
```

## Crossmap YAML Format

Crossmap definitions are in `packages/sdk/src/portiere/standards/crossmaps/`.

```yaml
source: "omop_cdm_v5.4"
target: "fhir_r4"

entity_map:
  person: "Patient"
  visit_occurrence: "Encounter"
  condition_occurrence: "Condition"

field_map:
  person.person_id:
    target: "Patient.id"
    transform: "str"
  person.gender_concept_id:
    target: "Patient.gender"
    transform: "omop_gender"    # references transforms section
  person.birth_datetime:
    target: "Patient.birthDate"
    transform: "fhir_date"

transforms:
  omop_gender:
    type: "value_map"
    mapping:
      8507: "male"
      8532: "female"
      8551: "other"
      8570: "unknown"
```

### Sections

- **entity_map**: Maps source entities to target entities
- **field_map**: Maps individual fields with transforms (key format: `entity.field`)
- **transforms**: Named transform definitions (referenced by field_map entries)

## Built-in Transforms

| Transform | Description | Config |
|-----------|-------------|--------|
| `passthrough` | Copy value as-is | — |
| `str` | Convert to string | — |
| `int` | Convert to integer | — |
| `float` | Convert to float | — |
| `bool` | Convert to boolean | — |
| `value_map` | Static lookup table | `mapping`, `default` |
| `format` | Date/string formatting | `pattern`, `input_format` |
| `codeable_concept` | FHIR CodeableConcept | `system`, `display_field` |
| `fhir_reference` | FHIR Reference | `resource_type` |
| `fhir_date` | Convert to FHIR date | — |
| `fhir_period` | FHIR Period from fields | `start_field`, `end_field` |
| `hl7v2_field` | HL7 v2 field format | — |
| `dv_quantity` | OpenEHR DV_QUANTITY | `unit_field`, `units` |
| `dv_coded_text` | OpenEHR DV_CODED_TEXT | `terminology_id`, `display_field` |
| `vocabulary_lookup` | Cross-vocab via bridge | `target_vocabulary`, `output` |

### Custom Transforms

```python
from portiere.local.transforms import TransformRegistry

def my_transform(value, config=None, record=None, vocabulary_bridge=None):
    return value.upper() if isinstance(value, str) else value

registry = TransformRegistry()
registry.register("uppercase", my_transform)
```

## VocabularyBridge

Cross-vocabulary concept mapping using Athena CONCEPT_RELATIONSHIP.csv.

```python
from portiere.knowledge.vocabulary_bridge import VocabularyBridge

bridge = VocabularyBridge("./data/athena/")

# Map a concept to another vocabulary
results = bridge.map_concept(4329847, target_vocabulary="SNOMED")

# Build a full crosswalk table
crosswalk = bridge.get_crosswalk("ICD10CM", "SNOMED")

# Convert to FHIR CodeableConcept
cc = bridge.concept_to_codeable_concept(4329847)

# Convert to OpenEHR DV_CODED_TEXT
dv = bridge.concept_to_dv_coded_text(4329847)
```

### Using VocabularyBridge with CrossStandardMapper

```python
bridge = VocabularyBridge("./data/athena/")
mapper = CrossStandardMapper(
    "omop_cdm_v5.4", "fhir_r4",
    vocabulary_bridge=bridge,
)
```

The `vocabulary_lookup` transform in crossmap YAMLs will automatically use the bridge:

```yaml
field_map:
  condition_occurrence.condition_concept_id:
    target: "Condition.code"
    transform: "vocabulary_lookup"
    config:
      target_vocabulary: "SNOMED"
      output: "codeable_concept"
```

## Batch and DataFrame Mapping

```python
# Multiple records
records = [
    {"person_id": 1, "gender_concept_id": 8507},
    {"person_id": 2, "gender_concept_id": 8532},
]
results = mapper.map_records("person", records)

# Pandas DataFrame
import pandas as pd
df = pd.DataFrame(records)
mapped_df = mapper.map_dataframe("person", df)
```

## Engine-Native DataFrame Support

`map_dataframe()` detects the input DataFrame type and returns the same type. This keeps cross-mapping consistent with your pipeline engine.

### Polars

```python
import polars as pl

omop_df = pl.read_csv("./omop_output/person.csv")
fhir_df = mapper.map_dataframe("person", omop_df)
# Returns: polars.DataFrame
fhir_df.write_parquet("./fhir_output/Patient.parquet")
```

### Spark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("portiere").getOrCreate()
omop_df = spark.read.parquet("./omop_output/person.parquet")
fhir_df = mapper.map_dataframe("person", omop_df)
# Returns: pyspark.sql.DataFrame
fhir_df.write.mode("overwrite").parquet("./fhir_output/Patient")
```

### Pandas

```python
import pandas as pd

omop_df = pd.read_csv("./omop_output/person.csv")
fhir_df = mapper.map_dataframe("person", omop_df)
# Returns: pandas.DataFrame
fhir_df.to_parquet("./fhir_output/Patient.parquet")
```

## Cross-Mapping in a Data Pipeline

The typical pattern is: run ETL to one standard, then cross-map the output to another.

### Full Pipeline: Source → OMOP → FHIR R4 (Polars)

```python
import portiere
import polars as pl
from portiere.engines import PolarsEngine
from portiere.config import PortiereConfig, KnowledgeLayerConfig
from portiere.local.cross_mapper import CrossStandardMapper

# Step 1: ETL to OMOP using Polars engine
config = PortiereConfig(
    target_model="omop_cdm_v5.4",
    knowledge_layer=KnowledgeLayerConfig(backend="bm25s"),
)
project = portiere.init(
    name="Hospital Pipeline",
    engine=PolarsEngine(),
    target_model="omop_cdm_v5.4",
    vocabularies=["SNOMED", "LOINC", "RxNorm", "ICD10CM"],
    config=config,
)
source = project.add_source("patients.csv")
schema_map = project.map_schema(source)
concept_map = project.map_concepts(source=source)
project.run_etl(source, output_dir="./omop_output",
                schema_mapping=schema_map, concept_mapping=concept_map)

# Step 2: Cross-map OMOP output → FHIR R4 (stays as Polars)
mapper = CrossStandardMapper("omop_cdm_v5.4", "fhir_r4")
for omop_table, fhir_resource in mapper.get_entity_map().items():
    path = f"./omop_output/{omop_table}.csv"
    df = pl.read_csv(path)
    fhir_df = mapper.map_dataframe(omop_table, df)
    fhir_df.write_parquet(f"./fhir_output/{fhir_resource}.parquet")
```

### Full Pipeline: Source → OMOP → FHIR R4 (Spark)

```python
from pyspark.sql import SparkSession
from portiere.engines import SparkEngine

spark = SparkSession.builder.appName("portiere").master("local[*]").getOrCreate()
project = portiere.init(
    name="Hospital Pipeline (Spark)",
    engine=SparkEngine(spark),
    target_model="omop_cdm_v5.4",
    config=config,
)
source = project.add_source("patients.csv")
schema_map = project.map_schema(source)
concept_map = project.map_concepts(source=source)
project.run_etl(source, output_dir="./omop_output",
                schema_mapping=schema_map, concept_mapping=concept_map,
                output_format="parquet")

# Cross-map with Spark DataFrames
mapper = CrossStandardMapper("omop_cdm_v5.4", "fhir_r4")
omop_df = spark.read.parquet("./omop_output/person.parquet")
fhir_df = mapper.map_dataframe("person", omop_df)  # Returns Spark DF
fhir_df.write.mode("overwrite").parquet("./fhir_output/Patient")
```

### Multi-Hop: OMOP → FHIR → OpenEHR

Chain cross-maps to reach standards without a direct crossmap definition:

```python
mapper_fhir = CrossStandardMapper("omop_cdm_v5.4", "fhir_r4")
mapper_oehr = CrossStandardMapper("fhir_r4", "openehr_1.0.4")

omop_df = pl.read_csv("./omop_output/condition_occurrence.csv")
fhir_df = mapper_fhir.map_dataframe("condition_occurrence", omop_df)
oehr_df = mapper_oehr.map_dataframe("Condition", fhir_df)
# All three DataFrames are Polars
```

## Project Integration

### Cross-Map Projects (task="cross_map")

For projects declared as cross-map tasks, source and target standards are inferred:

```python
project = portiere.init(
    name="OMOP to FHIR",
    task="cross_map",
    source_standard="omop_cdm_v5.4",
    target_model="fhir_r4",
    engine=PolarsEngine(),
)

# No need to repeat source/target — inferred from project settings
result = project.cross_map(
    source_entity="person",
    data={"person_id": 1, "gender_concept_id": 8507},
)

# DataFrame (Polars, Pandas, or Spark — matches engine)
import polars as pl
omop_df = pl.read_csv("./omop_output/person.csv")
fhir_df = project.cross_map(source_entity="person", data=omop_df)
```

### Standardize Projects (explicit source/target)

For standardize projects, you must provide source_standard and target_standard:

```python
project = portiere.init(name="My Project", engine=PolarsEngine())

fhir_df = project.cross_map(
    source_standard="omop_cdm_v5.4",
    target_standard="fhir_r4",
    source_entity="person",
    data=omop_df,
)
```

### Backward Compatibility

Old code with all positional args still works:

```python
fhir_df = project.cross_map("omop_cdm_v5.4", "fhir_r4", "person", omop_df)
```

## Mapping Report

```python
report = mapper.get_mapping_report()
print(f"Entity mappings: {len(report['entity_mappings'])}")
print(f"Field mappings: {report['field_mappings']}")
print(f"Unmapped source: {len(report['unmapped_source_fields'])}")
print(f"Unmapped target: {len(report['unmapped_target_fields'])}")
```

## Server-Side Tracking

When using `project.cross_map()`, every run is automatically persisted to storage — locally as YAML or to the cloud API when using a cloud backend. This gives you an audit trail of all cross-mapping operations.

### SDK — Automatic Tracking

```python
project = portiere.init(name="My Project", engine=PolarsEngine(), ...)

# Every cross_map() call is tracked
result = project.cross_map(
    source_standard="omop_cdm_v5.4",
    target_standard="fhir_r4",
    source_entity="person",
    data=omop_df,
)

# Load cross-mapping history
from portiere.models.cross_mapping import CrossMapping
history = project._storage.load_cross_mapping(project.name)
print(f"Total runs: {len(history.runs)}")
for run in history.runs:
    print(f"  {run.source_standard} → {run.target_standard} | "
          f"{run.source_entity} | {run.record_count} records | {run.status}")

# Summary
print(history.summary())
# {'total_runs': 3, 'total_records': 1500, 'standard_pairs': ['omop_cdm_v5.4 → fhir_r4']}
```

### Local Storage

In local mode, cross-mapping runs are stored in the project directory:

```
{project_dir}/
├── cross_mappings/
│   └── cross_mapping.yaml    # ← run history
├── schema_mappings/
├── concept_mappings/
└── ...
```

The YAML file contains a list of run records:

```yaml
- source_standard: omop_cdm_v5.4
  target_standard: fhir_r4
  source_entity: person
  target_entity: Patient
  record_count: 500
  status: completed
  created_at: null
- source_standard: omop_cdm_v5.4
  target_standard: fhir_r4
  source_entity: condition_occurrence
  target_entity: Condition
  record_count: 1200
  status: completed
  created_at: null
```

### Cloud Storage

In cloud mode, runs are synced to the Portiere API via the sync endpoints, consistent with how schema and concept mappings are synced:

```python
from portiere.client import Client

client = Client(api_key="pt_sk_live_...")
project = portiere.init(
    name="Hospital Pipeline",
    client=client,         # Enables cloud storage
    engine=PolarsEngine(),
    target_model="omop_cdm_v5.4",
)

# This cross-map is automatically persisted to both local and cloud
fhir_df = project.cross_map(
    source_standard="omop_cdm_v5.4",
    target_standard="fhir_r4",
    source_entity="person",
    data=omop_df,
)
```

## API Endpoints

### Stateless Cross-Map (no tracking)

```
POST /api/v1/cross-mapping/map
{
    "source_standard": "omop_cdm_v5.4",
    "target_standard": "fhir_r4",
    "source_entity": "person",
    "records": [
        {"person_id": 1, "gender_concept_id": 8507}
    ]
}
```

Response:
```json
{
    "results": [{"id": "1", "gender": "male"}],
    "source_standard": "omop_cdm_v5.4",
    "target_standard": "fhir_r4",
    "source_entity": "person",
    "target_entity": "Patient",
    "total": 1
}
```

### Tracked Cross-Map (project-scoped)

Maps data **and** persists the run to the project's cross-mapping history:

```
POST /api/v1/cross-mapping/projects/{project_id}/map
{
    "source_standard": "omop_cdm_v5.4",
    "target_standard": "fhir_r4",
    "source_entity": "person",
    "records": [
        {"person_id": 1, "gender_concept_id": 8507},
        {"person_id": 2, "gender_concept_id": 8532}
    ]
}
```

Response: same as stateless, but the run is recorded in the database.

### Cross-Mapping History

```
GET /api/v1/cross-mapping/projects/{project_id}
```

Response:
```json
{
    "runs": [
        {
            "id": "a1b2c3d4-...",
            "source_standard": "omop_cdm_v5.4",
            "target_standard": "fhir_r4",
            "source_entity": "person",
            "target_entity": "Patient",
            "record_count": 2,
            "status": "completed",
            "crossmap_file": null,
            "created_at": "2026-03-02T10:30:00"
        }
    ],
    "total": 1
}
```

### List Available Crossmaps

```
GET /api/v1/cross-mapping
```

Lists all available crossmap definitions.

### Sync Endpoints

Used by the SDK to push/pull cross-mapping history between local and cloud:

```
POST /api/v1/sync/projects/{project_id}/cross-mappings/bulk
GET  /api/v1/sync/projects/{project_id}/cross-mappings
```

These follow the same pattern as schema and concept mapping sync endpoints.

## Creating Custom Crossmaps

1. Create a YAML file following the crossmap format
2. Place it in `packages/sdk/src/portiere/standards/crossmaps/`
3. Or pass it directly:

```python
from pathlib import Path
mapper = CrossStandardMapper(
    "my_source", "my_target",
    custom_crossmap=Path("./my_crossmap.yaml"),
)
```

## See Also

- [20-multi-standard-support.md](./20-multi-standard-support.md) — Multi-standard overview and engine + standard combinations
- [08-pipeline-architecture.md](./08-pipeline-architecture.md) — Pipeline stages and cross-mapping as post-pipeline step
- [05-knowledge-layer.md](./05-knowledge-layer.md) — VocabularyBridge for cross-vocabulary concept mapping
- [13. Cross-Standard Mapping Notebook](../notebooks_examples/13_cross_standard_mapping.ipynb) — Interactive examples with Polars, Spark, and Pandas
- [14. Multi-Standard Overview Notebook](../notebooks_examples/14_multi_standard_overview.ipynb) — Same pipeline, different standards and engines

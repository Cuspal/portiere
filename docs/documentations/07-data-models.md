# Data Models

Portiere's mapping pipeline produces structured data models that represent the results of schema
mapping (source columns to target model columns) and concept mapping (source codes to standard
vocabulary concepts). These models provide methods for reviewing, approving, rejecting, and
exporting mappings programmatically.

---

## Table of Contents

1. [Target Model Definitions](#target-model-definitions)
   - [YAMLTargetModel](#yamltargetmodel)
   - [Supported Standards](#supported-standards)
   - [Standard YAML Schema](#standard-yaml-schema)
2. [Schema Mapping Models](#schema-mapping-models)
   - [MappingStatus](#mappingstatus)
   - [SchemaMappingItem](#schemamappingitem)
   - [SchemaMapping](#schemamapping)
3. [Concept Mapping Models](#concept-mapping-models)
   - [ConceptMappingMethod](#conceptmappingmethod)
   - [ConceptCandidate](#conceptcandidate)
   - [ConceptMappingItem](#conceptmappingitem)
   - [ConceptMapping](#conceptmapping)
4. [Cross-Standard Mapping Models](#cross-standard-mapping-models)
5. [Approval Workflows](#approval-workflows)
6. [Export Formats](#export-formats)

---

## Target Model Definitions

Target models define the clinical data standards that Portiere maps source data into. Standard
definitions are stored as YAML files and loaded via the `YAMLTargetModel` class.

### YAMLTargetModel

`YAMLTargetModel` is a generic loader that reads any standard definition from a YAML file.
It replaces the need for separate Python classes per standard.

```python
from portiere.models.target_model import get_target_model

# Load a built-in standard
model = get_target_model("omop_cdm_v5.4")

# Key methods
model.get_schema()              # {table: [columns]} or {resource: [fields]}
model.get_target_descriptions() # {table.column: description}
model.get_source_patterns()     # {pattern: (table, column)}
model.generate_ddl()            # SQL DDL for relational standards
model.validate_output(engine, path)  # Validate ETL output
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | str | Standard identifier (e.g., `"omop_cdm_v5.4"`) |
| `version` | str | Standard version (e.g., `"v5.4"`) |
| `standard_type` | str | One of `"relational"`, `"resource"`, `"segment"`, `"archetype"` |

### Supported Standards

| Standard | Identifier | Type | Description |
|----------|-----------|------|-------------|
| OMOP CDM v5.4 | `"omop_cdm_v5.4"` | relational | OHDSI Observational Medical Outcomes Partnership |
| FHIR R4 | `"fhir_r4"` | resource | HL7 Fast Healthcare Interoperability Resources |
| HL7 v2.5.1 | `"hl7v2_2.5.1"` | segment | HL7 Version 2 messaging |
| OpenEHR 1.0.4 | `"openehr_1.0.4"` | archetype | openEHR archetype-based EHR |

### Standard YAML Schema

Each standard definition follows this structure:

```yaml
name: "omop_cdm_v5.4"
version: "v5.4"
standard_type: "relational"    # relational | resource | segment | archetype
organization: "OHDSI"

entities:
  person:                       # table / resource / segment / archetype
    description: "Patient demographics"
    fields:
      person_id:
        type: "integer"
        required: true
        description: "Unique patient identifier"
      gender_concept_id:
        type: "integer"
        required: true
        description: "Patient gender"
        vocabulary: "Gender"
    source_patterns:            # Patterns that match source column names
      - "patient_id"
      - "subject_id"
    embedding_descriptions:     # For semantic schema mapping
      person_id: "unique patient identifier, subject ID, MRN"

vocabulary_systems: {}          # vocabulary_id → URI mapping
```

Standard YAML files are located in `packages/sdk/src/portiere/standards/`. You can provide
a custom YAML file via `custom_standard_path` in `PortiereConfig` or by passing
`"custom:/path/to/file.yaml"` to `get_target_model()`.

See [Multi-Standard Support](./20-multi-standard-support.md) for a comprehensive guide.

---

## Schema Mapping Models

Schema mapping connects source data columns to target data model tables and columns (e.g.,
mapping a CSV column `patient_dob` to the OMOP `person.birth_datetime` field).

### MappingStatus

The `MappingStatus` enum tracks the lifecycle state of each mapping item. It is shared between
schema and concept mapping.

```python
from portiere.models import MappingStatus

class MappingStatus(str, Enum):
    AUTO_ACCEPTED = "auto_accepted"
    NEEDS_REVIEW  = "needs_review"
    APPROVED      = "approved"
    REJECTED      = "rejected"
    OVERRIDDEN    = "overridden"
    UNMAPPED      = "unmapped"
```

| Status | Description |
|--------|-------------|
| `AUTO_ACCEPTED` | Confidence score met the auto-acceptance threshold. No human review required. |
| `NEEDS_REVIEW` | Confidence score is in the review band. Human review recommended. |
| `APPROVED` | A reviewer has explicitly approved this mapping. |
| `REJECTED` | A reviewer has explicitly rejected this mapping. |
| `OVERRIDDEN` | A reviewer has overridden the suggested mapping with a different target. |
| `UNMAPPED` | Confidence score was too low to suggest a mapping, or no suitable candidates were found. |

### SchemaMappingItem

Represents a single source-column-to-target-column mapping.

```python
from portiere.models import SchemaMappingItem

class SchemaMappingItem(BaseModel):
    source_column: str
    source_table: str = ""
    target_table: str
    target_column: str
    confidence: float
    status: MappingStatus
    candidates: List[dict] = []
    override_target_table: Optional[str] = None
    override_target_column: Optional[str] = None
```

**Note:** The `source_table` field defaults to an empty string. This allows tests and simple
use cases to omit it when the source is a single file.

#### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `source_column` | str | (required) | Name of the column in the source data |
| `source_table` | str | `""` | Name of the source table (optional for single-file sources) |
| `target_table` | str | (required) | Suggested target table in the data model |
| `target_column` | str | (required) | Suggested target column in the data model |
| `confidence` | float | (required) | Mapping confidence score (0.0 to 1.0) |
| `status` | MappingStatus | (required) | Current mapping lifecycle status |
| `candidates` | List[dict] | `[]` | Alternative mapping candidates with scores |
| `override_target_table` | str | None | Reviewer-specified target table (when overridden) |
| `override_target_column` | str | None | Reviewer-specified target column (when overridden) |

#### Properties

```python
item = SchemaMappingItem(
    source_column="patient_dob",
    target_table="person",
    target_column="birth_datetime",
    confidence=0.92,
    status=MappingStatus.AUTO_ACCEPTED,
)

# effective_target_table returns the override if set, otherwise the original target
print(item.effective_target_table)  # "person"

# effective_target_column returns the override if set, otherwise the original target
print(item.effective_target_column)  # "birth_datetime"
```

When an override is applied:

```python
item.override_target_table = "observation"
item.override_target_column = "observation_date"

print(item.effective_target_table)   # "observation"
print(item.effective_target_column)  # "observation_date"
```

#### Methods

**`approve(target_table=None, target_column=None)`**

Approves the mapping. If `target_table` and `target_column` are provided, the mapping is
overridden to the specified target; otherwise the current suggestion is approved as-is.

```python
# Approve the suggested mapping
item.approve()
print(item.status)  # MappingStatus.APPROVED

# Approve with an override
item.approve(target_table="observation", target_column="observation_date")
print(item.status)  # MappingStatus.OVERRIDDEN
print(item.effective_target_table)   # "observation"
print(item.effective_target_column)  # "observation_date"
```

**`reject()`**

Rejects the mapping, marking it as rejected.

```python
item.reject()
print(item.status)  # MappingStatus.REJECTED
```

---

### SchemaMapping

A collection of `SchemaMappingItem` objects representing all column mappings for a source.

```python
from portiere.models import SchemaMapping

class SchemaMapping(BaseModel):
    items: List[SchemaMappingItem]
    project: str
    source: str
    finalized: bool = False
```

#### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `items` | List[SchemaMappingItem] | (required) | All mapping items for this source |
| `project` | str | (required) | Project identifier |
| `source` | str | (required) | Source file or table identifier |
| `finalized` | bool | `False` | Whether the mapping has been finalized |

#### Filter Methods

```python
schema_mapping = project.map_schema(source=source)

# Get items that need human review
review_items = schema_mapping.needs_review()
# Returns: List[SchemaMappingItem] where status == NEEDS_REVIEW

# Get items that were auto-accepted
auto_items = schema_mapping.auto_accepted()
# Returns: List[SchemaMappingItem] where status == AUTO_ACCEPTED
```

#### Batch Operations

```python
# Approve all items currently in NEEDS_REVIEW status
schema_mapping.approve_all()
```

#### Finalization

Finalization locks the mapping, preventing further changes. This signals that all mappings
have been reviewed and the ETL stage can proceed.

```python
schema_mapping.finalize()
print(schema_mapping.finalized)  # True

# Attempting to modify a finalized mapping raises an error
```

#### Summary

The `summary()` method returns a dictionary with aggregate statistics:

```python
stats = schema_mapping.summary()
print(stats)
# {
#     "total": 25,
#     "auto_accepted": 18,
#     "needs_review": 5,
#     "approved": 0,
#     "unmapped": 2,
#     "auto_rate": 0.72,
# }
```

| Key | Type | Description |
|-----|------|-------------|
| `total` | int | Total number of mapping items |
| `auto_accepted` | int | Items with AUTO_ACCEPTED status |
| `needs_review` | int | Items with NEEDS_REVIEW status |
| `approved` | int | Items with APPROVED status |
| `unmapped` | int | Items with UNMAPPED status |
| `auto_rate` | float | Fraction of items that were auto-accepted (`auto_accepted / total`) |

---

## Concept Mapping Models

Concept mapping links source codes and descriptions to standard vocabulary concepts (e.g.,
mapping source code `"250.00"` with description `"Diabetes mellitus"` to SNOMED concept
`201826` "Type 2 diabetes mellitus").

### ConceptMappingMethod

The `ConceptMappingMethod` enum describes how a concept mapping was established.

```python
from portiere.models import ConceptMappingMethod

class ConceptMappingMethod(str, Enum):
    AUTO     = "auto"
    REVIEW   = "review"
    MANUAL   = "manual"
    OVERRIDE = "override"
    UNMAPPED = "unmapped"
```

**Important:** This class is named `ConceptMappingMethod`, not `MappingMethod`.

| Method | Description |
|--------|-------------|
| `AUTO` | Mapping was auto-accepted (confidence >= 0.95). Also set when `approve()` is called without candidates. |
| `REVIEW` | Mapping was flagged for review (confidence 0.70-0.95). |
| `MANUAL` | Mapping requires manual intervention (confidence < 0.70). |
| `OVERRIDE` | A reviewer has overridden the mapping with a manually specified concept. |
| `UNMAPPED` | No suitable concept was found or the mapping was explicitly marked as unmappable. |

### ConceptCandidate

Represents a single candidate concept returned by the knowledge layer search.

```python
from portiere.models import ConceptCandidate

class ConceptCandidate(BaseModel):
    concept_id: int
    concept_name: str
    vocabulary_id: str
    domain_id: str
    concept_class_id: str
    standard_concept: str
    score: float
```

#### Fields

| Field | Type | Description |
|-------|------|-------------|
| `concept_id` | int | Unique concept identifier (e.g., OMOP concept_id) |
| `concept_name` | str | Human-readable concept name |
| `vocabulary_id` | str | Source vocabulary (e.g., `"SNOMED"`, `"LOINC"`, `"RxNorm"`) |
| `domain_id` | str | Concept domain (e.g., `"Condition"`, `"Drug"`, `"Measurement"`) |
| `concept_class_id` | str | Concept class (e.g., `"Clinical Finding"`, `"Ingredient"`) |
| `standard_concept` | str | Standard concept flag (`"S"` = Standard, `"C"` = Classification) |
| `score` | float | Relevance score from the knowledge layer (0.0 to 1.0) |

#### Candidate Scoring

Candidates are returned sorted by `score` in descending order. The score combines signals from
the knowledge layer backend:

- **BM25s/Elasticsearch**: BM25 token-overlap score, normalized to [0, 1]
- **FAISS**: Cosine similarity between source term and concept embeddings
- **Hybrid**: RRF-fused score combining dense and sparse signals
- **After reranking**: Cross-encoder relevance score replaces the initial score

```python
# Access candidates for a mapping item
for candidate in item.candidates:
    print(f"  {candidate.concept_id}: {candidate.concept_name} "
          f"({candidate.vocabulary_id}) score={candidate.score:.3f}")
```

---

### ConceptMappingItem

Represents a single source-code-to-concept mapping.

```python
from portiere.models import ConceptMappingItem

class ConceptMappingItem(BaseModel):
    source_code: str
    source_description: str
    source_column: str
    source_count: int
    target_concept_id: Optional[int]
    target_concept_name: Optional[str]
    target_vocabulary_id: Optional[str]
    target_domain_id: Optional[str]
    confidence: float
    method: ConceptMappingMethod
    candidates: List[ConceptCandidate] = []
    provenance: Optional[dict] = None
```

#### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `source_code` | str | (required) | Original code value from the source data |
| `source_description` | str | (required) | Description associated with the source code |
| `source_column` | str | (required) | Column in the source data containing this code |
| `source_count` | int | (required) | Number of occurrences in the source data |
| `target_concept_id` | int | None | Mapped target concept ID |
| `target_concept_name` | str | None | Mapped target concept name |
| `target_vocabulary_id` | str | None | Target concept vocabulary |
| `target_domain_id` | str | None | Target concept domain |
| `confidence` | float | (required) | Mapping confidence score (0.0 to 1.0) |
| `method` | ConceptMappingMethod | (required) | How this mapping was established |
| `candidates` | List[ConceptCandidate] | `[]` | Candidate concepts from knowledge layer search |
| `provenance` | dict | None | Metadata about how the mapping was produced (e.g., LLM verification details) |

#### Properties

```python
item = concept_mapping.items[0]

# is_mapped: True if a target concept has been assigned
print(item.is_mapped)  # True (if target_concept_id is not None)

# approved: True if method indicates the mapping has been approved
print(item.approved)

# rejected: True if the mapping has been rejected
print(item.rejected)
```

#### Methods

**`approve(candidate_index=0)`**

Approves the mapping using the candidate at the specified index. If no candidates exist,
sets the method to `AUTO`.

```python
# Approve using the top candidate (index 0)
item.approve()
print(item.method)             # ConceptMappingMethod.AUTO (if no candidates)
print(item.target_concept_id)  # Set from candidates[0].concept_id

# Approve using the second candidate
item.approve(candidate_index=1)
print(item.target_concept_id)  # Set from candidates[1].concept_id
```

**`reject()`**

Rejects the mapping, clearing the target concept.

```python
item.reject()
print(item.rejected)  # True
```

**`override(concept_id, concept_name="", vocabulary_id="")`**

Overrides the mapping with a manually specified concept. Sets the method to `OVERRIDE`
(not `MANUAL`).

```python
item.override(
    concept_id=4029098,
    concept_name="Atrial fibrillation",
    vocabulary_id="SNOMED",
)
print(item.method)             # ConceptMappingMethod.OVERRIDE
print(item.target_concept_id)  # 4029098
print(item.target_concept_name)  # "Atrial fibrillation"
```

**`mark_unmapped()`**

Marks the item as having no valid mapping.

```python
item.mark_unmapped()
print(item.method)  # ConceptMappingMethod.UNMAPPED
```

---

### ConceptMapping

A collection of `ConceptMappingItem` objects representing all concept mappings for a source.

```python
from portiere.models import ConceptMapping

class ConceptMapping(BaseModel):
    items: List[ConceptMappingItem]
    project: str
    source: str
    finalized: bool = False
```

#### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `items` | List[ConceptMappingItem] | (required) | All concept mapping items for this source |
| `project` | str | (required) | Project identifier |
| `source` | str | (required) | Source file or table identifier |
| `finalized` | bool | `False` | Whether the mapping has been finalized |

#### Filter Methods

```python
concept_mapping = project.map_concepts(
    source=source,
    schema_mapping=schema_mapping,
)

# Get items that need human review
review_items = concept_mapping.needs_review()
# Returns: List[ConceptMappingItem] where method == REVIEW

# Get items that were auto-mapped
auto_items = concept_mapping.auto_mapped()
# Returns: List[ConceptMappingItem] where method == AUTO

# Get items that have no mapping
unmapped_items = concept_mapping.unmapped()
# Returns: List[ConceptMappingItem] where method == UNMAPPED
```

#### Batch Operations

```python
# Approve all items currently in REVIEW status using their top candidate
concept_mapping.approve_all()
```

#### Finalization

```python
concept_mapping.finalize()
print(concept_mapping.finalized)  # True
```

#### Summary

The `summary()` method returns a dictionary with aggregate statistics:

```python
stats = concept_mapping.summary()
print(stats)
# {
#     "total": 150,
#     "auto_mapped": 120,
#     "needs_review": 20,
#     "manual_required": 10,
#     "auto_rate": 0.80,
#     "coverage": 0.93,
# }
```

**Important:** The summary keys for `ConceptMapping` differ from `SchemaMapping`:

| Key | Type | Description |
|-----|------|-------------|
| `total` | int | Total number of concept mapping items |
| `auto_mapped` | int | Items mapped automatically (method == AUTO) |
| `needs_review` | int | Items flagged for review (method == REVIEW) |
| `manual_required` | int | Items requiring manual mapping (method == MANUAL) |
| `auto_rate` | float | Fraction auto-mapped (`auto_mapped / total`) |
| `coverage` | float | Fraction of items with any mapping (`(total - unmapped) / total`) |

---

## Cross-Standard Mapping Models

Cross-standard mapping converts data already in one clinical standard to another standard
(e.g., OMOP to FHIR, HL7 v2 to FHIR, FHIR to OpenEHR). This is a post-pipeline capability
using `CrossStandardMapper`.

### CrossStandardMapper

```python
from portiere.local.cross_mapper import CrossStandardMapper

mapper = CrossStandardMapper("omop_cdm_v5.4", "fhir_r4")

# Map a single record
fhir_patient = mapper.map_record("person", {
    "person_id": 12345,
    "gender_concept_id": 8507,
    "birth_datetime": "1980-06-15",
})
# {"id": "12345", "gender": "male", "birthDate": "1980-06-15", ...}

# Map a list of records
fhir_patients = mapper.map_records("person", records_list)

# Map a DataFrame
fhir_df = mapper.map_dataframe("person", persons_df)
```

### Crossmap YAML Schema

Cross-standard mappings are defined in YAML files under `standards/crossmaps/`:

```yaml
source: "omop_cdm_v5.4"
target: "fhir_r4"

entity_map:
  person: "Patient"
  condition_occurrence: "Condition"

field_map:
  person.person_id:
    target: "Patient.id"
    transform: "str"
  person.gender_concept_id:
    target: "Patient.gender"
    transform: "omop_gender"

transforms:
  omop_gender:
    type: "value_map"
    mapping:
      8507: "male"
      8532: "female"
    default: "unknown"
```

### Available Crossmaps

| Source | Target | File |
|--------|--------|------|
| OMOP CDM v5.4 | FHIR R4 | `omop_to_fhir_r4.yaml` |
| FHIR R4 | OMOP CDM v5.4 | `fhir_r4_to_omop.yaml` |
| HL7 v2.5.1 | FHIR R4 | `hl7v2_to_fhir_r4.yaml` |
| FHIR R4 | OpenEHR 1.0.4 | `fhir_r4_to_openehr.yaml` |
| OMOP CDM v5.4 | OpenEHR 1.0.4 | `omop_to_openehr.yaml` |

### Transform Types

Built-in transform types available in crossmap YAML:

| Transform | Description |
|-----------|-------------|
| `passthrough` | Copy value as-is |
| `str` / `int` / `float` | Type casting |
| `value_map` | Static lookup table |
| `format` | Date/string formatting |
| `codeable_concept` | Wrap into FHIR CodeableConcept |
| `fhir_reference` | Create FHIR Reference |
| `dv_coded_text` | Create openEHR DV_CODED_TEXT |
| `dv_quantity` | Create openEHR DV_QUANTITY |
| `vocabulary_lookup` | Cross-vocabulary mapping via VocabularyBridge |

See [Cross-Standard Mapping](./21-cross-standard-mapping.md) for the complete reference.

### Project Integration

```python
# Cross-map via the project object
fhir_data = project.cross_map(
    source_standard="omop_cdm_v5.4",
    target_standard="fhir_r4",
    source_entity="person",
    data=persons_df,
)
```

---

## Approval Workflows

### Schema Mapping Review Workflow

```python
import portiere
from portiere.config import PortiereConfig
from portiere.engines import PolarsEngine

project = portiere.init(name="data_models_demo", engine=PolarsEngine(), config=PortiereConfig(...))
source = project.add_source("data/patients.csv")
profile = project.profile(source)
schema_mapping = project.map_schema(source=source)

# Step 1: Check the summary
print(schema_mapping.summary())
# {"total": 20, "auto_accepted": 15, "needs_review": 3, "approved": 0, "unmapped": 2, "auto_rate": 0.75}

# Step 2: Review items that need attention
for item in schema_mapping.needs_review():
    print(f"\nSource: {item.source_column}")
    print(f"Suggested: {item.target_table}.{item.target_column} "
          f"(confidence: {item.confidence:.2f})")
    print(f"Candidates: {item.candidates}")

    # Decision: approve, reject, or override
    if item.confidence > 0.85:
        item.approve()
    else:
        item.approve(
            target_table="measurement",
            target_column="value_as_number",
        )

# Step 3: Handle unmapped items
for item in [i for i in schema_mapping.items if i.status.value == "unmapped"]:
    print(f"Unmapped: {item.source_column}")
    # Either approve with a manual target or leave unmapped

# Step 4: Finalize
schema_mapping.finalize()
```

### Concept Mapping Review Workflow

```python
concept_mapping = project.map_concepts(
    source=source,
    schema_mapping=schema_mapping,
)

# Step 1: Check the summary
print(concept_mapping.summary())
# {"total": 150, "auto_mapped": 120, "needs_review": 20, "manual_required": 10,
#  "auto_rate": 0.80, "coverage": 0.93}

# Step 2: Review items flagged for review
for item in concept_mapping.needs_review():
    print(f"\nSource: {item.source_code} - {item.source_description}")
    print(f"Current mapping: {item.target_concept_id} - {item.target_concept_name}")
    print(f"Confidence: {item.confidence:.3f}")
    print("Candidates:")
    for i, c in enumerate(item.candidates):
        print(f"  [{i}] {c.concept_id}: {c.concept_name} "
              f"({c.vocabulary_id}, {c.domain_id}) score={c.score:.3f}")

# Step 3: Take action on each item
item = concept_mapping.needs_review()[0]

# Option A: Approve the top candidate
item.approve(candidate_index=0)

# Option B: Approve a different candidate
item.approve(candidate_index=2)

# Option C: Override with a known concept
item.override(
    concept_id=4029098,
    concept_name="Atrial fibrillation",
    vocabulary_id="SNOMED",
)

# Option D: Reject the mapping
item.reject()

# Option E: Mark as unmappable
item.mark_unmapped()

# Step 4: Batch approve remaining review items
concept_mapping.approve_all()

# Step 5: Finalize
concept_mapping.finalize()
```

---

## Export Formats

### to_source_to_concept_map()

The `ConceptMapping` class provides a `to_source_to_concept_map()` method that exports the
mapping in a format compatible with the OMOP `source_to_concept_map` table:

```python
source_to_concept_map = concept_mapping.to_source_to_concept_map()

# Returns a list of dictionaries, one per mapped item:
# [
#     {
#         "source_code": "250.00",
#         "source_concept_id": 0,
#         "source_vocabulary_id": "ICD9CM",
#         "source_code_description": "Diabetes mellitus type II",
#         "target_concept_id": 201826,
#         "target_vocabulary_id": "SNOMED",
#         "valid_start_date": "2024-01-01",
#         "valid_end_date": "2099-12-31",
#         "invalid_reason": None,
#     },
#     ...
# ]
```

This export format can be loaded directly into the OMOP CDM `source_to_concept_map` table
or used to build ETL transformation logic.

```python
import pandas as pd

# Convert to DataFrame for further processing
stcm_df = pd.DataFrame(source_to_concept_map)
stcm_df.to_csv("source_to_concept_map.csv", index=False)
```

---

## See Also

- [Knowledge Layer](05-knowledge-layer.md) -- How candidates are retrieved and scored
- [LLM Integration](06-llm-integration.md) -- LLM verification for review-band mappings
- [Pipeline Architecture](08-pipeline-architecture.md) -- How schema and concept mapping fit into the pipeline
- [Exceptions](09-exceptions.md) -- `MappingError` and `ValidationError` during mapping operations
- [Multi-Standard Support](20-multi-standard-support.md) -- YAML standard definitions and target model selection
- [Cross-Standard Mapping](21-cross-standard-mapping.md) -- CrossStandardMapper and transform reference

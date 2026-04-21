# Custom Standards

Define your own clinical data standard as a YAML file and use it with Portiere's full mapping pipeline. This is useful when your target schema is a hospital-internal CDM, a national registry, or any custom data model.

## Step 1 — Create Your YAML Definition

Create a YAML file that describes your target data model. Each file defines entities (tables), fields (columns), source patterns for fast matching, and embedding descriptions for AI-powered semantic matching.

```yaml
# my_hospital_cdm.yaml — Custom Hospital Data Model

name: "hospital_cdm_v1"
version: "v1.0"
standard_type: "relational"
organization: "City General Hospital"
description: "Internal clinical data model for City General Hospital"

# Default fallback entity for unmapped columns
default_entity: "clinical_notes"
default_field: "note_text"

entities:
  patients:
    description: "Patient demographics and registration"
    fields:
      patient_id:
        type: "integer"
        required: true
        description: "Unique patient identifier"
        ddl: "INTEGER PRIMARY KEY"
      medical_record_number:
        type: "string"
        required: true
        description: "Hospital MRN"
        ddl: "VARCHAR(20) NOT NULL"
      full_name:
        type: "string"
        required: true
        description: "Patient full name"
        ddl: "VARCHAR(200) NOT NULL"
      date_of_birth:
        type: "date"
        required: true
        description: "Patient date of birth"
        ddl: "DATE NOT NULL"
      sex:
        type: "string"
        required: true
        description: "Biological sex (M/F/O)"
        ddl: "VARCHAR(1) NOT NULL"
      insurance_id:
        type: "string"
        required: false
        description: "Insurance policy number"
        ddl: "VARCHAR(50)"

    # Fast column-name matching: source column → target field
    source_patterns:
      patient_id: "patient_id"
      subject_id: "patient_id"
      mrn: "medical_record_number"
      medical_record: "medical_record_number"
      name: "full_name"
      patient_name: "full_name"
      dob: "date_of_birth"
      birth_date: "date_of_birth"
      gender: "sex"
      sex: "sex"
      insurance: "insurance_id"
      policy_number: "insurance_id"

    # AI-optimized descriptions for embedding similarity
    embedding_descriptions:
      patient_id: "patients table: unique patient identifier number"
      medical_record_number: "patients table: hospital medical record number MRN"
      full_name: "patients table: patient full name first and last"
      date_of_birth: "patients table: patient date of birth birthday"
      sex: "patients table: biological sex gender male female"
      insurance_id: "patients table: insurance policy number payer"

  encounters:
    description: "Hospital visits, admissions, and encounters"
    fields:
      encounter_id:
        type: "integer"
        required: true
        description: "Unique encounter identifier"
        ddl: "INTEGER PRIMARY KEY"
      patient_id:
        type: "integer"
        required: true
        description: "Reference to patients table"
        ddl: "INTEGER NOT NULL"
        fk: "patients.patient_id"
      admission_date:
        type: "datetime"
        required: true
        description: "Admission date and time"
        ddl: "TIMESTAMP NOT NULL"
      discharge_date:
        type: "datetime"
        required: false
        description: "Discharge date and time"
        ddl: "TIMESTAMP"
      encounter_type:
        type: "string"
        required: true
        description: "Type of encounter (inpatient, outpatient, ED)"
        ddl: "VARCHAR(20) NOT NULL"
      department:
        type: "string"
        required: false
        description: "Hospital department or unit"
        ddl: "VARCHAR(100)"
    source_patterns:
      encounter_id: "encounter_id"
      visit_id: "encounter_id"
      hadm_id: "encounter_id"
      admission_date: "admission_date"
      admit_date: "admission_date"
      discharge_date: "discharge_date"
      disch_date: "discharge_date"
      visit_type: "encounter_type"
      encounter_type: "encounter_type"
      department: "department"
      unit: "department"
      ward: "department"
    embedding_descriptions:
      encounter_id: "encounters table: unique hospital visit admission identifier"
      patient_id: "encounters table: patient reference foreign key"
      admission_date: "encounters table: date time of hospital admission"
      discharge_date: "encounters table: date time of hospital discharge"
      encounter_type: "encounters table: type of visit inpatient outpatient ED emergency"
      department: "encounters table: hospital department unit ward"

  diagnoses:
    description: "Clinical diagnoses and conditions"
    fields:
      diagnosis_id:
        type: "integer"
        required: true
        description: "Unique diagnosis record identifier"
        ddl: "INTEGER PRIMARY KEY"
      encounter_id:
        type: "integer"
        required: true
        description: "Reference to encounters table"
        ddl: "INTEGER NOT NULL"
        fk: "encounters.encounter_id"
      icd_code:
        type: "string"
        required: true
        description: "ICD-10 diagnosis code"
        ddl: "VARCHAR(10) NOT NULL"
      description:
        type: "string"
        required: false
        description: "Diagnosis description text"
        ddl: "VARCHAR(500)"
      diagnosis_type:
        type: "string"
        required: false
        description: "Primary, secondary, or admitting diagnosis"
        ddl: "VARCHAR(20)"
    source_patterns:
      diagnosis_id: "diagnosis_id"
      icd_code: "icd_code"
      icd10: "icd_code"
      diagnosis_code: "icd_code"
      dx_code: "icd_code"
      diagnosis_description: "description"
      dx_description: "description"
      diagnosis_type: "diagnosis_type"
      dx_type: "diagnosis_type"
    embedding_descriptions:
      diagnosis_id: "diagnoses table: unique diagnosis record identifier"
      encounter_id: "diagnoses table: encounter visit reference"
      icd_code: "diagnoses table: ICD-10 diagnosis code clinical condition"
      description: "diagnoses table: diagnosis description text condition name"
      diagnosis_type: "diagnoses table: primary secondary admitting diagnosis type"

  clinical_notes:
    description: "Free-text clinical notes and documentation"
    fields:
      note_id:
        type: "integer"
        required: true
        description: "Unique note identifier"
        ddl: "INTEGER PRIMARY KEY"
      encounter_id:
        type: "integer"
        required: true
        description: "Reference to encounters table"
        ddl: "INTEGER NOT NULL"
        fk: "encounters.encounter_id"
      note_type:
        type: "string"
        required: true
        description: "Type of note (progress, discharge, consult)"
        ddl: "VARCHAR(50) NOT NULL"
      note_text:
        type: "text"
        required: true
        description: "Free-text note content"
        ddl: "TEXT NOT NULL"
      author:
        type: "string"
        required: false
        description: "Note author (provider name)"
        ddl: "VARCHAR(200)"
    source_patterns:
      note_id: "note_id"
      note_type: "note_type"
      note_text: "note_text"
      note_content: "note_text"
      clinical_note: "note_text"
      author: "author"
      provider: "author"
    embedding_descriptions:
      note_id: "clinical notes table: unique note document identifier"
      encounter_id: "clinical notes table: encounter visit reference"
      note_type: "clinical notes table: type of clinical note progress discharge consult"
      note_text: "clinical notes table: free text clinical note content documentation"
      author: "clinical notes table: note author provider physician name"
```

## Step 2 — Load Your Custom Standard

### Option A: Load by File Path (Recommended)

Use the `custom:` prefix to load from any file path:

```python
import portiere

project = portiere.init(
    name="Hospital Migration",
    target_model="custom:./standards/my_hospital_cdm.yaml",
    vocabularies=["ICD10CM", "SNOMED"],
)

# The pipeline now maps to YOUR schema
source = project.add_source("raw_patients.csv")
schema_map = project.map_schema(source)
print(schema_map.summary())
```

### Option B: Load Directly via YAMLTargetModel

For programmatic access to inspect your standard before running the pipeline:

```python
from pathlib import Path
from portiere.standards import YAMLTargetModel

# Load your custom standard
model = YAMLTargetModel(Path("./standards/my_hospital_cdm.yaml"))

# Inspect what it contains
print(f"Standard: {model.name} ({model.version})")
print(f"Entities: {list(model.get_schema().keys())}")

# View all source patterns (for debugging)
for pattern, (entity, field) in model.get_source_patterns().items():
    print(f"  '{pattern}' → {entity}.{field}")

# View AI descriptions (for debugging embedding matching)
for key, desc in model.get_target_descriptions().items():
    print(f"  {key}: {desc}")

# Generate SQL DDL for your standard
ddl = model.generate_ddl()
print(ddl)
```

## Step 3 — Run the Full Pipeline

Once loaded, your custom standard works with every pipeline stage — schema mapping, concept mapping, review, export, and ETL:

```python
import portiere

# Initialize with custom standard
project = portiere.init(
    name="Hospital Migration",
    target_model="custom:./standards/my_hospital_cdm.yaml",
    vocabularies=["ICD10CM", "SNOMED"],
)

# Add source data
source = project.add_source("raw_ehr_export.csv")

# Schema mapping — uses your source_patterns + embedding_descriptions
schema_map = project.map_schema(source)
print(schema_map.summary())
# {'total': 15, 'auto_accepted': 10, 'needs_review': 4, 'unmapped': 1}

# Review and approve
for item in schema_map.needs_review():
    print(f"  {item.source_column} → {item.target_table}.{item.target_column} ({item.confidence:.2f})")
schema_map.approve_all()

# Concept mapping
concept_map = project.map_concepts(source=source)

# Export for SME review
schema_map.to_csv("schema_review.csv")
concept_map.to_csv("concept_review.csv")

# Run ETL to produce output in YOUR standard's schema
result = project.run_etl(
    source, output_dir="./hospital_cdm_output/",
    schema_mapping=schema_map, concept_mapping=concept_map,
)
```

## YAML Schema Reference

Every standard YAML file must contain these **required** top-level keys:

| Key | Type | Description |
|-----|------|-------------|
| `name` | string | Unique identifier (e.g., `"hospital_cdm_v1"`) |
| `version` | string | Version string (e.g., `"v1.0"`) |
| `standard_type` | string | One of: `relational`, `resource`, `segment`, `archetype` |
| `entities` | dict | Entity definitions (see below) |

**Optional** top-level keys:

| Key | Type | Description |
|-----|------|-------------|
| `organization` | string | Organization name |
| `description` | string | Human-readable description |
| `default_entity` | string | Fallback entity for unmapped columns |
| `default_field` | string | Fallback field for unmapped columns |

**Entity definition:**

| Key | Type | Description |
|-----|------|-------------|
| `description` | string | Entity description |
| `fields` | dict | Field definitions (name → type/required/description/ddl) |
| `source_patterns` | dict | Column name patterns → target field name |
| `embedding_descriptions` | dict | Field name → AI-optimized semantic text |

**Field definition:**

| Key | Type | Description |
|-----|------|-------------|
| `type` | string | `string`, `integer`, `float`, `date`, `datetime`, `boolean`, `text` |
| `required` | bool | Whether the field is required |
| `description` | string | Human-readable field description |
| `ddl` | string | SQL DDL type (for `generate_ddl()`) |
| `fk` | string | Foreign key reference (e.g., `"patients.patient_id"`) |

## Validation

`project.validate()` works with any standard — built-in or custom YAML. The validator automatically derives conformance checks from the YAML field type metadata:

- **Code/vocabulary fields** (`type: integer` with `vocabulary`, `type: code`, `CodeableConcept`, `CE`, `DV_CODED_TEXT`) — numeric code fields get range checks (non-negative integers); string-valued code fields (e.g., FHIR `gender`) are safely skipped
- **Temporal fields** (`date`, `datetime`, `TS`, `DV_DATE`, `Period`) — checked for non-null values
- **Custom YAML fields** — add `vocabulary` or `valueset` keys to mark fields as coded; use standard type names (`date`, `datetime`, `integer`, `float`) for automatic classification

```python
# Validate ETL output — works for OMOP, FHIR, HL7, OpenEHR, or custom
result = project.validate(etl_result)
print(result["passed"])               # True/False
print(result["completeness_score"])   # 0.0–1.0
print(result["conformance_score"])    # 0.0–1.0
print(result["plausibility_score"])   # 0.0–1.0
```

## Tips for Writing Good Standards

1. **Source patterns** — Include common aliases for each field. The more patterns you add, the higher the auto-mapping rate. Think about abbreviations, naming conventions from different EHR vendors, and alternative spellings.

2. **Embedding descriptions** — Write natural-language descriptions that are semantically rich. Include synonyms and domain terms. These are used for AI-powered semantic matching when pattern matching fails.

3. **Standard type** — Choose the right type for DDL generation:
   - `relational` → SQL CREATE TABLE statements
   - `resource` → JSON StructureDefinition (FHIR-style)
   - `segment` → Segment definition summary (HL7 v2-style)
   - `archetype` → Archetype definition summary (OpenEHR-style)

4. **Default entity** — Set a meaningful default for unmapped columns. Without this, unmapped columns fall back to the first entity's first field.

5. **Iterate** — Run the pipeline, check the confidence scores, then add more source patterns for low-confidence matches. Each pattern you add is a guaranteed high-confidence match next time.

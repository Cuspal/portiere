# Pipeline Architecture

Portiere implements a 5-stage pipeline that transforms raw source data into validated,
standards-compliant output. Each stage is independently executable, produces persistent
artifacts, and feeds its results into the next stage. The pipeline is designed so that
stages can be re-run without repeating earlier work, and human review can be injected
at the appropriate points.

---

## Table of Contents

1. [Pipeline Overview](#pipeline-overview)
2. [Target Standard Selection](#target-standard-selection)
3. [Stage 1: Ingest](#stage-1-ingest)
4. [Stage 2: Profile](#stage-2-profile)
5. [Stage 3: Schema Map](#stage-3-schema-map)
6. [Stage 4: Concept Map](#stage-4-concept-map)
7. [Stage 5: ETL + Validate](#stage-5-etl--validate)
8. [Cross-Standard Mapping (Post-Pipeline)](#cross-standard-mapping-post-pipeline)
9. [Confidence Routing](#confidence-routing)
10. [Artifact Persistence](#artifact-persistence)
11. [Full Pipeline Example](#full-pipeline-example)

---

## Pipeline Overview

```
+----------+    +---------+    +------------+    +-------------+    +----------+
|  Ingest  | -> | Profile | -> | Schema Map | -> | Concept Map | -> |   ETL +  |
|          |    |         |    |            |    |             |    | Validate |
+----------+    +---------+    +------------+    +-------------+    +----------+
     |               |              |                  |                  |
     v               v              v                  v                  v
  Source          DataProfile   SchemaMapping    ConceptMapping      ETL Output
  Object          Object         Object            Object         + QA Report
```

**Data flow:**

1. **Ingest** reads raw files and produces a `Source` object with metadata and data references.
2. **Profile** analyzes the source data and produces column statistics, type detection, and
   code column identification.
3. **Schema Map** uses the profile to map source columns to target model tables and columns,
   producing a `SchemaMapping` with confidence-routed items.
4. **Concept Map** uses the schema mapping to identify code columns and map source codes to
   standard vocabulary concepts via the knowledge layer and optional LLM verification.
5. **ETL + Validate** transforms the source data according to the finalized mappings and
   validates the output using Great Expectations.

---

## Target Standard Selection

Before the pipeline runs, Portiere loads the target clinical data standard that defines the
schema, field descriptions, and source patterns used by Stage 3 (Schema Map) and Stage 5 (ETL).

### Supported Standards

| Standard | Identifier | Type |
|----------|-----------|------|
| OMOP CDM v5.4 | `"omop_cdm_v5.4"` | Relational (tables + columns) |
| FHIR R4 | `"fhir_r4"` | Resource (FHIR resources + elements) |
| HL7 v2.5.1 | `"hl7v2_2.5.1"` | Segment (HL7 segments + fields) |
| OpenEHR 1.0.4 | `"openehr_1.0.4"` | Archetype (openEHR archetypes + paths) |

The target standard is set via `target_model` in `PortiereConfig` (defaults to `"omop_cdm_v5.4"`):

```python
from portiere.config import PortiereConfig

# Use FHIR R4 as the target standard
config = PortiereConfig(target_model="fhir_r4")
```

Standard definitions are loaded from YAML files via `YAMLTargetModel`. This means schema
mapping patterns, target descriptions, and validation rules are all driven by the selected
standard -- no code changes needed to switch targets.

See [Multi-Standard Support](./20-multi-standard-support.md) for details on standard
definitions and custom YAML creation.

---

## Stage 1: Ingest

The Ingest stage reads source data files and creates a structured `Source` object that the
rest of the pipeline operates on.

### Supported Formats

| Format | Extension | Engine |
|--------|-----------|--------|
| CSV | `.csv` | pandas / polars |
| Parquet | `.parquet` | pandas / pyarrow |
| JSON | `.json`, `.jsonl` | pandas |
| Database | connection string | polars / pandas |

### Auto-Detection

The engine automatically detects the file format from the extension and selects the
appropriate reader. For ambiguous cases (e.g., JSON with nested structures), hints can be
provided:

```python
import portiere
from portiere.engines import PolarsEngine

project = portiere.init(name="my_project", engine=PolarsEngine())

# Auto-detect format from extension
source = project.add_source("data/patients.csv")

# Explicit format specification
source = project.add_source("data/export.dat", format="csv", delimiter="|")
```

### Engine Abstraction

The `Source` object abstracts over the underlying data engine. The engine is obtained via the
`get_engine()` factory function (not `create_engine()`), and the engine type is identified by
the `engine_name` property (not `name`):

```python
from portiere.engines import get_engine

engine = get_engine("pandas")
print(engine.engine_name)  # "pandas"
```

### Source Object

The `Source` object produced by ingestion contains:

- File path and format metadata
- Row count and column names
- Data type inferences
- A reference to the data (lazy-loaded for large files)

```python
source = project.add_source("data/patients.csv")
print(source.path)        # "data/patients.csv"
print(source.row_count)   # 50000
print(source.columns)     # ["patient_id", "dob", "gender", "diagnosis_code", ...]
```

### Database Sources

Portiere can also ingest data directly from databases. Provide a `connection_string` with
a `table` or `query` instead of a file path:

```python
# Read from a PostgreSQL table
source = project.add_source(
    connection_string="postgresql://user:pass@localhost:5432/ehr_db",
    table="patients"
)

# Or use a custom SQL query
source = project.add_source(
    connection_string="postgresql://user:pass@localhost:5432/ehr_db",
    query="SELECT * FROM patients WHERE admission_date >= '2024-01-01'",
    name="recent_patients"
)
```

Database sources produce the same `Source` object as file sources -- all downstream pipeline
stages (Profile, Schema Map, Concept Map, ETL) work identically regardless of the source type.

Supported databases: PostgreSQL (`postgresql://`), MySQL (`mysql://`), SQLite (`sqlite:///`),
SQL Server (`mssql://`), and any database supported by your engine's connector.

See [Database Connections](14-database-connections.md) for a complete guide.

---

## Stage 2: Profile

The Profile stage performs statistical analysis and structural detection on the ingested source
data. The output informs downstream mapping stages about column types, value distributions, and
which columns likely contain coded values.

### Column Statistics

For each column, the profiler computes:

| Statistic | Description |
|-----------|-------------|
| `dtype` | Detected data type (string, integer, float, date, boolean) |
| `null_count` | Number of null/missing values |
| `null_rate` | Fraction of null values |
| `unique_count` | Number of distinct values |
| `cardinality` | Unique count relative to total rows |
| `min` / `max` | Range for numeric and date columns |
| `mean` / `std` | Mean and standard deviation for numeric columns |
| `top_values` | Most frequent values with counts |
| `sample_values` | Random sample of non-null values |

### Code Column Detection

A critical function of the Profile stage is identifying columns that contain coded values
(diagnosis codes, procedure codes, drug codes, lab codes, etc.) that need concept mapping.
The detection algorithm uses:

- **Cardinality analysis**: Columns with moderate cardinality relative to row count are
  candidates (e.g., 500 unique values in 50,000 rows).
- **Value pattern matching**: Regex patterns for known code formats (ICD-10: `[A-Z]\d{2}\.?\d*`,
  LOINC: `\d{4,5}-\d`, NDC: `\d{5}-\d{4}-\d{2}`).
- **Column name heuristics**: Names containing "code", "icd", "ndc", "loinc", "cpt", "snomed".
- **Description column pairing**: If a "code" column has an adjacent "description" column, both
  are flagged.

### Great Expectations Profiling

The profiler can optionally generate Great Expectations (GX) expectation suites based on the
observed data patterns. These expectations are reused in Stage 5 for validation:

```python
profile = project.profile(source, generate_expectations=True)
print(profile.expectations)  # GX expectation suite
```

### Usage

```python
profile = project.profile(source)

# Inspect column statistics
for col_name, stats in profile.column_stats.items():
    print(f"{col_name}: {stats.dtype}, {stats.null_rate:.1%} null, "
          f"{stats.unique_count} unique")

# Check detected code columns
print(profile.code_columns)  # ["diagnosis_code", "procedure_code", "drug_ndc"]
```

---

## Stage 3: Schema Map

The Schema Map stage maps source data columns to the target data model (e.g., OMOP CDM). It
produces a `SchemaMapping` object containing one `SchemaMappingItem` per source column, each
with a confidence score and routing status.

### How Schema Mapping Works

1. **Target model loading**: The target data model schema (tables, columns, types, descriptions)
   is loaded.
2. **Feature extraction**: For each source column, features are extracted from the column name,
   detected type, value statistics, and profiling results.
3. **Candidate generation**: Potential target columns are scored based on name similarity,
   type compatibility, and semantic similarity.
4. **Confidence scoring**: Each candidate receives a confidence score combining multiple signals.
5. **Routing**: Based on the confidence score, each mapping is routed to auto-accept, review,
   or unmapped.

### Confidence Routing for Schema Mapping

| Confidence Range | Status | Action |
|-----------------|--------|--------|
| >= 0.90 | `AUTO_ACCEPTED` | Mapping is accepted without human review |
| 0.70 - 0.90 | `NEEDS_REVIEW` | Mapping is flagged for human review with candidates |
| < 0.70 | `UNMAPPED` | No confident mapping found; requires manual specification |

### Candidates

Each `SchemaMappingItem` includes a list of alternative candidates, sorted by confidence:

```python
schema_mapping = project.map_schema(source=source)

for item in schema_mapping.needs_review():
    print(f"\n{item.source_column} -> {item.target_table}.{item.target_column} "
          f"(confidence: {item.confidence:.2f})")
    for candidate in item.candidates:
        print(f"  Alternative: {candidate['target_table']}.{candidate['target_column']} "
              f"(confidence: {candidate['confidence']:.2f})")
```

### Review and Finalization

After mapping, items in the `NEEDS_REVIEW` status should be reviewed by a human. See
[Data Models -- Approval Workflows](07-data-models.md#approval-workflows) for detailed
review patterns.

```python
# Review, then finalize
for item in schema_mapping.needs_review():
    item.approve()  # or item.approve(target_table=..., target_column=...)

schema_mapping.finalize()
```

---

## Stage 4: Concept Map

The Concept Map stage maps source codes (diagnosis codes, drug codes, procedure codes, etc.)
to standard vocabulary concepts. This is the most complex stage, involving knowledge layer
search, LLM verification, and confidence-based routing.

### How Concept Mapping Works

1. **Code extraction**: Using the schema mapping's code column identification, the pipeline
   extracts unique (code, description) pairs from the source data along with occurrence counts.
2. **Knowledge layer search**: Each source term is searched against the vocabulary index using
   the configured knowledge backend (BM25s, FAISS, Elasticsearch, or Hybrid).
3. **Candidate retrieval**: The top-K candidates are retrieved and optionally reranked by a
   cross-encoder model.
4. **Confidence scoring**: The top candidate's score determines the confidence level.
5. **LLM verification**: For mappings in the verify/review band (0.70-0.95), the LLM reviews
   the mapping and may confirm, adjust, or reject it.
6. **Routing**: Each mapping is assigned a `ConceptMappingMethod` based on the final confidence.

### Confidence Routing for Concept Mapping

| Confidence Range | Method | LLM | Action |
|-----------------|--------|-----|--------|
| >= 0.95 | `AUTO` | No | Auto-accepted, highest confidence |
| 0.80 - 0.95 | `REVIEW` | Verify | LLM confirms or adjusts |
| 0.70 - 0.80 | `REVIEW` | Analyze | LLM provides analysis, human review flagged |
| < 0.70 | `MANUAL` | No | Directly routed to human review |

### Knowledge Layer Integration

The knowledge layer backend is selected via configuration and used transparently by the
concept mapping stage:

```python
import portiere
from portiere.config import PortiereConfig, KnowledgeLayerConfig
from portiere.engines import PolarsEngine

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="hybrid",
        faiss_index_path="/path/to/faiss.index",
        faiss_metadata_path="/path/to/faiss_metadata.json",
        bm25s_corpus_path="/path/to/bm25s_corpus/",
        fusion_method="rrf",
    )
)

project = portiere.init(name="my_project", engine=PolarsEngine(), config=config)
concept_mapping = project.map_concepts(
    source=source,
    schema_mapping=schema_mapping,
)
```

See [Knowledge Layer](05-knowledge-layer.md) for detailed backend configuration.

### LLM Verification

When the LLM is invoked for verification, the prompt includes the source term, top candidates,
and contextual information. The LLM returns:

- A confirmation or rejection of the top candidate
- An optional re-ranking of candidates
- A confidence adjustment

The LLM verification details are stored in the `provenance` field of the `ConceptMappingItem`.
See [LLM Integration](06-llm-integration.md) for provider configuration.

### Usage

```python
concept_mapping = project.map_concepts(
    source=source,
    schema_mapping=schema_mapping,
)

# Check results
stats = concept_mapping.summary()
print(f"Auto-mapped: {stats['auto_mapped']}/{stats['total']} "
      f"({stats['auto_rate']:.1%})")
print(f"Coverage: {stats['coverage']:.1%}")

# Review items
for item in concept_mapping.needs_review():
    print(f"{item.source_code}: {item.source_description}")
    print(f"  -> {item.target_concept_name} (confidence: {item.confidence:.3f})")
```

---

## Stage 5: ETL + Validate

The ETL + Validate stage transforms the source data according to the finalized schema and
concept mappings, then validates the output using Great Expectations.

### ETL Transformation

The ETL engine applies the following transformations:

1. **Column mapping**: Source columns are renamed and mapped to target table columns based on
   the finalized `SchemaMapping`.
2. **Concept substitution**: Source codes are replaced with target concept IDs based on the
   finalized `ConceptMapping`.
3. **Type casting**: Source values are cast to the target column types (e.g., string dates to
   datetime, string numbers to integers).
4. **Table splitting**: If the target model has multiple tables (e.g., OMOP CDM's `person`,
   `condition_occurrence`, `drug_exposure`), the source data is split into the appropriate
   target tables.

### Validation

Great Expectations (GX) is used to validate the transformed output against the project's target model. Validation rules are derived from the standard's YAML field type metadata — OMOP `concept_id` columns, FHIR `CodeableConcept` fields, HL7 `CE` segments, and OpenEHR `DV_CODED_TEXT` elements all receive appropriate conformance checks automatically.

- **Schema expectations**: Correct column names, types, and nullability
- **Referential integrity**: Foreign key relationships between tables
- **Value constraints**: Valid ranges, allowed values, format patterns (standards-aware)
- **Statistical expectations**: Distribution checks from the profiling stage

### Quality Reports

The validation produces a quality report with:

```python
result = project.run_etl(
    source=source,
    schema_mapping=schema_mapping,
    concept_mapping=concept_mapping,
)
result = project.validate(result)

print(result.success)          # True/False
print(result.statistics)       # Overall statistics
print(result.failed_expectations)  # List of failed checks

# Detailed results per target table
for table_name, table_result in result.table_results.items():
    print(f"\n{table_name}:")
    print(f"  Rows: {table_result.row_count}")
    print(f"  Passed: {table_result.passed}/{table_result.total_expectations}")
```

### Error Handling

If the ETL process encounters critical errors, an `ETLExecutionError` is raised with the
partial result accessible via the `result` attribute:

```python
from portiere.exceptions import ETLExecutionError

try:
    etl_output = project.run_etl(
        source=source,
        schema_mapping=schema_mapping,
        concept_mapping=concept_mapping,
    )
    result = project.validate(etl_output)
except ETLExecutionError as e:
    print(f"ETL failed: {e}")
    partial_result = e.result  # Access partial results for debugging
```

See [Exceptions](09-exceptions.md#etlexecutionerror) for details.

---

## Cross-Standard Mapping (Post-Pipeline)

After the pipeline produces validated output in one standard, Portiere can cross-map the data
to another standard. This is a post-pipeline operation that uses declarative crossmap YAML
definitions.

```
+----------+    +---------+    +------------+    +-------------+    +----------+
|  Ingest  | -> | Profile | -> | Schema Map | -> | Concept Map | -> |   ETL +  |
|          |    |         |    |            |    |             |    | Validate |
+----------+    +---------+    +------------+    +-------------+    +----------+
                                                                         |
                                                                         v
                                                                  +--------------+
                                                                  | Cross-Map to |
                                                                  | 2nd Standard |
                                                                  +--------------+
```

### Example: OMOP Pipeline + FHIR Cross-Map

```python
# Run the pipeline targeting OMOP
config = PortiereConfig(target_model="omop_cdm_v5.4")
project = portiere.init(name="demo", engine=PolarsEngine(), config=config)

source = project.add_source("patients.csv")
schema_mapping = project.map_schema(source)
concept_mapping = project.map_concepts(source=source)
etl_output = project.run_etl(source, schema_mapping=schema_mapping, concept_mapping=concept_mapping)

# Cross-map the OMOP output to FHIR R4
fhir_patients = project.cross_map(
    source_standard="omop_cdm_v5.4",
    target_standard="fhir_r4",
    source_entity="person",
    data=omop_persons_df,
)
```

### Available Cross-Maps

| Source | Target | Use Case |
|--------|--------|----------|
| OMOP CDM v5.4 | FHIR R4 | Clinical data exchange, FHIR API |
| FHIR R4 | OMOP CDM v5.4 | Research, observational studies |
| HL7 v2.5.1 | FHIR R4 | Legacy system modernization |
| FHIR R4 | OpenEHR 1.0.4 | EHR system integration |
| OMOP CDM v5.4 | OpenEHR 1.0.4 | Research to clinical bridge |

See [Cross-Standard Mapping](./21-cross-standard-mapping.md) for the full reference.

---

## Confidence Routing

Confidence routing is the mechanism that determines how each mapping is handled based on its
confidence score. This is applied at both the schema mapping and concept mapping stages.

### Schema Mapping Thresholds

```
Confidence: 0.0 -------- 0.70 -------- 0.90 -------- 1.0
             |  UNMAPPED  |  NEEDS_REVIEW  | AUTO_ACCEPTED |
```

| Threshold | Status | Description |
|-----------|--------|-------------|
| >= 0.90 | `AUTO_ACCEPTED` | High confidence, accepted without review |
| 0.70 - 0.90 | `NEEDS_REVIEW` | Moderate confidence, human review recommended |
| < 0.70 | `UNMAPPED` | Low confidence, no mapping suggested |

### Concept Mapping Thresholds

```
Confidence: 0.0 ---- 0.70 ---- 0.80 ---- 0.95 ---- 1.0
             | MANUAL |  REVIEW  |  VERIFY  |  AUTO  |
                       (+ LLM)    (+ LLM)
```

| Threshold | Method | LLM | Description |
|-----------|--------|-----|-------------|
| >= 0.95 | `AUTO` | No | Highest confidence, auto-accepted |
| 0.80 - 0.95 | `REVIEW` | Verify | LLM confirms the mapping |
| 0.70 - 0.80 | `REVIEW` | Analyze | LLM provides analysis, human review flagged |
| < 0.70 | `MANUAL` | No | Low confidence, routed to manual review |

### Why Confidence Routing Matters

Confidence routing optimizes the trade-off between mapping accuracy and human review effort:

- **High thresholds** (>= 0.95 for auto-accept) ensure that only highly certain mappings
  bypass human review, minimizing false positives.
- **LLM verification** in the middle band uses AI to confirm or reject borderline mappings,
  reducing the human review burden without sacrificing accuracy.
- **Low threshold routing** to manual review ensures that uncertain mappings receive proper
  human attention rather than being incorrectly auto-accepted.

---

## Artifact Persistence

Each pipeline stage produces artifacts that are persisted to the configured storage backend.
This enables:

- **Resumability**: If a stage fails or is interrupted, it can be re-run without repeating
  earlier stages.
- **Auditability**: All mapping decisions (auto-accepted, reviewed, overridden) are preserved
  with timestamps and provenance.
- **Reproducibility**: The same source data with the same configuration will produce identical
  artifacts.

### Storage at Each Stage

| Stage | Artifact | Contents |
|-------|----------|----------|
| Ingest | `Source` | File metadata, column names, row count, data reference |
| Profile | `DataProfile` | Column statistics, code column flags, GX expectations |
| Schema Map | `SchemaMapping` | Column-to-column mappings with confidence and status |
| Concept Map | `ConceptMapping` | Code-to-concept mappings with candidates and provenance |
| ETL + Validate | `ETLResult` | Transformed data files, validation report, quality metrics |

### Storage Backends

Artifacts can be stored locally (filesystem) or remotely (Portiere Cloud, S3, database):

```python
config = PortiereConfig(
    local_project_dir="/path/to/project/artifacts/",
)
# Portiere infers local mode (no api_key configured)
```

---

## Full Pipeline Example

A complete end-to-end pipeline execution:

```python
import portiere
from portiere.config import PortiereConfig, KnowledgeLayerConfig, LLMConfig
from portiere.engines import PolarsEngine

# Configure
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="hybrid",
        faiss_index_path="/data/indexes/faiss.index",
        faiss_metadata_path="/data/indexes/faiss_metadata.json",
        bm25s_corpus_path="/data/indexes/bm25s_corpus/",
        fusion_method="rrf",
    ),
    llm=LLMConfig(
        provider="openai",
        api_key="sk-...",
        model="gpt-4o",
    ),
)

project = portiere.init(name="hospital_encounters", engine=PolarsEngine(), config=config)

# Stage 1: Ingest
source = project.add_source("data/hospital_encounters.csv")
print(f"Ingested source: {source['name']} ({source['format']})")

# Stage 2: Profile
profile = project.profile(source)
print(f"Profile complete: {len(profile.get('columns', []))} columns analyzed")

# Stage 3: Schema Map
schema_mapping = project.map_schema(source)
print(f"Schema mapping: {len(schema_mapping.items)} columns mapped")

# Review schema mappings
for item in schema_mapping.items:
    if item.status.value == "needs_review":
        # In production, this would be a UI-driven review
        item.approve()

# Stage 4: Concept Map
concept_mapping = project.map_concepts(source=source)
summary = concept_mapping.summary()
print(f"Auto-mapped: {summary['auto_mapped']}, Needs review: {summary['needs_review']}")

# Stage 5: ETL + Validate
etl_output = project.run_etl(
    source,
    output_dir="./output",
    schema_mapping=schema_mapping,
    concept_mapping=concept_mapping,
)
result = project.validate(etl_result=etl_output)

if result["all_passed"]:
    print("Pipeline completed successfully!")
    print(f"Validated {result['total_tables']} tables")
else:
    print("Validation issues found:")
    for table in result["tables"]:
        if not table.get("passed"):
            print(f"  - {table.get('table_name', 'unknown')} failed")
```

---

## See Also

- [Knowledge Layer](05-knowledge-layer.md) -- Search backend configuration for Stage 4
- [LLM Integration](06-llm-integration.md) -- LLM provider configuration for Stage 4
- [Data Models](07-data-models.md) -- Detailed model reference for SchemaMapping and ConceptMapping
- [Exceptions](09-exceptions.md) -- Error handling at each pipeline stage
- [Multi-Standard Support](20-multi-standard-support.md) -- Target standard selection and YAML definitions
- [Cross-Standard Mapping](21-cross-standard-mapping.md) -- Post-pipeline cross-standard conversion

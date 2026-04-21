# Unified SDK API Reference

Complete reference for the Portiere SDK public API. All signatures, parameters, return types, and usage examples.

---

## Table of Contents

- [Module Entry Point: portiere.init()](#module-entry-point-portiereinit)
- [Project Class](#project-class)
  - [Properties](#properties)
  - [add\_source()](#add_source)
  - [profile()](#profile)
  - [map\_schema()](#map_schema)
  - [map\_concepts()](#map_concepts)
  - [run\_etl()](#run_etl)
  - [validate()](#validate)
  - [push()](#push)
  - [pull()](#pull)
  - [load\_schema\_mapping()](#load_schema_mapping)
  - [load\_concept\_mapping()](#load_concept_mapping)
  - [import\_concept\_mapping()](#import_concept_mapping)
  - [export\_concept\_mapping()](#export_concept_mapping)
- [SchemaMapping](#schemamapping)
- [ConceptMapping](#conceptmapping)
- [Related Configuration](#related-configuration)

---

## Module Entry Point: `portiere.init()`

Creates and returns a new `Project` instance.

### Signature

```python
def init(
    name: str,
    *,
    engine: AbstractEngine,
    task: str = "standardize",
    target_model: str = "omop_cdm_v5.4",
    source_standard: Optional[str] = None,
    vocabularies: Optional[list[str]] = None,
    config: Optional[PortiereConfig] = None
) -> Project
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | *required* | Human-readable project name. Used as the project identifier in local storage and cloud sync. |
| `engine` | `AbstractEngine` | *required* | Compute engine instance for data processing and ETL execution. Import from `portiere.engines` (e.g., `PolarsEngine()`, `SparkEngine(spark)`, `PandasEngine()`). |
| `task` | `str` | `"standardize"` | Project task type. `"standardize"` maps raw source data to a target standard (full pipeline). `"cross_map"` transforms between two clinical data standards. |
| `target_model` | `str` | `"omop_cdm_v5.4"` | Target CDM version. For standardize: the target standard. For cross_map: the target standard to transform into. |
| `source_standard` | `Optional[str]` | `None` | Source standard for cross_map tasks (e.g., `"omop_cdm_v5.4"`). Required when `task="cross_map"`. |
| `vocabularies` | `Optional[list[str]]` | `["SNOMED", "LOINC", "RxNorm", "ICD10CM"]` | Standard vocabularies to use for concept mapping. |
| `config` | `Optional[PortiereConfig]` | `None` | Configuration object. When `None`, auto-discovered via `PortiereConfig.discover()`. |

### Returns

`Project` -- A fully initialized project instance ready for pipeline operations.

### Examples

**Minimal initialization (all defaults):**

```python
import portiere
from portiere.engines import PolarsEngine

project = portiere.init(name="My Hospital Migration", engine=PolarsEngine())
```

**Custom vocabularies and target model:**

```python
import portiere
from portiere.engines import PolarsEngine

project = portiere.init(
    name="Lab Data Migration",
    engine=PolarsEngine(),
    target_model="omop_cdm_v5.4",
    vocabularies=["LOINC", "SNOMED", "UCUM"]
)
```

**Cross-map project:**

```python
import portiere
from portiere.engines import PolarsEngine

project = portiere.init(
    name="OMOP to FHIR Export",
    engine=PolarsEngine(),
    task="cross_map",
    source_standard="omop_cdm_v5.4",
    target_model="fhir_r4",
)

# source_standard and target are inferred from project settings
fhir_df = project.cross_map(source_entity="person", data=omop_df)
```

**Explicit configuration (cloud pipeline):**

```python
import portiere
from portiere.config import PortiereConfig, LLMConfig
from portiere.engines import PolarsEngine

config = PortiereConfig(
    api_key="pt_sk_your_api_key",
    llm=LLMConfig(provider="openai", api_key="sk-...", model="gpt-4o")
)

project = portiere.init(name="Cloud-Assisted Migration", engine=PolarsEngine(), config=config)
```

### Behavior

1. If `config` is `None`, calls `PortiereConfig.discover()` to resolve configuration from (in order): `portiere.yaml` in the current directory, environment variables with `PORTIERE_` prefix, built-in defaults.
2. Registers the provided `engine` instance (an `AbstractEngine` subclass) as the compute engine for the project.
3. Sets up the knowledge layer for concept search based on `config.knowledge_layer`.
4. Creates or loads a local project directory under `config.local_project_dir / <name>`.

---

## Project Class

The `Project` class is the central orchestrator for all pipeline operations. It is a plain Python class (not a Pydantic model).

**Important:** Do not instantiate `Project` directly. Always use `portiere.init()`.

---

### Properties

#### `engine`

The compute engine instance used for ETL operations.

```python
project.engine
# Returns the configured engine (Polars, Spark, DuckDB, Snowpark, or Pandas)
```

#### `client`

The API client for cloud operations. Only active when an `api_key` is configured (cloud or hybrid mode).

```python
project.client
# Returns the Portiere API client, or None in pure local mode
```

#### `storage`

The storage backend managing project artifacts.

```python
project.storage
# Returns the local or cloud storage handler
```

#### `config`

The resolved `PortiereConfig` for this project.

```python
project.config
# Returns PortiereConfig instance
print(project.config.effective_mode)       # "local"
print(project.config.llm.model)  # "gpt-4o"
```

---

### `add_source()`

Registers a data source with the project. Supports both file-based and database sources.

#### Signature

```python
def add_source(
    path: Optional[str] = None,
    name: Optional[str] = None,
    format: Optional[str] = None,
    *,
    connection_string: Optional[str] = None,
    table: Optional[str] = None,
    query: Optional[str] = None,
) -> dict
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | `Optional[str]` | `None` | Path to the source data file (CSV, Parquet, JSON, etc.). Mutually exclusive with `connection_string`. |
| `name` | `Optional[str]` | `None` | Human-readable name for this source. Defaults to the filename stem or table name. |
| `format` | `Optional[str]` | `None` | File format override. Auto-detected from extension when `None`. Set to `"database"` automatically for database sources. |
| `connection_string` | `Optional[str]` | `None` | Database connection URI (e.g., `postgresql://user:pass@host/db`). Mutually exclusive with `path`. |
| `table` | `Optional[str]` | `None` | Database table name to read. Requires `connection_string`. |
| `query` | `Optional[str]` | `None` | SQL query to execute. Requires `connection_string`. |

Either `path` or `connection_string` must be provided (not both). Database sources require at least one of `table` or `query`.

#### Returns

`dict` -- Source metadata dictionary containing:

| Key | Type | Description |
|-----|------|-------------|
| `name` | `str` | Source name |
| `path` | `str` | Resolved file path (file sources only) |
| `format` | `str` | Detected format (`"csv"`, `"parquet"`, `"database"`, etc.) |
| `connection_string` | `str` | Database URI (database sources only) |
| `table` | `str` | Table name (database sources with table) |
| `query` | `str` | SQL query (database sources with query) |

#### Examples

**Auto-detect format from extension:**

```python
source = project.add_source("patients.csv")
print(source["format"])
# "csv"
```

**Explicit name and format:**

```python
source = project.add_source(
    "data/raw/encounters_2024.tsv",
    name="Emergency Encounters",
    format="csv"  # TSV is parsed as CSV with tab delimiter
)
```

**Multiple sources in one project:**

```python
patients = project.add_source("patients.csv")
encounters = project.add_source("encounters.csv")
conditions = project.add_source("conditions.csv")
```

**Database source — read a table:**

```python
source = project.add_source(
    connection_string="postgresql://user:pass@localhost:5432/ehr_db",
    table="patients"
)
# source["format"] == "database"
# source["name"] == "patients" (auto-derived from table name)
```

**Database source — custom SQL query:**

```python
source = project.add_source(
    connection_string="postgresql://user:pass@localhost:5432/ehr_db",
    query="SELECT * FROM patients WHERE admission_date >= '2024-01-01'",
    name="recent_patients"
)
```

---

### `profile()`

Runs data quality profiling on a source using Great Expectations. Analyzes completeness, distributions, type consistency, and anomalies.

**Requires:** `pip install portiere-health[quality]`

#### Signature

```python
def profile(source: dict) -> dict
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source` | `dict` | *required* | Source metadata dictionary returned by `add_source()`. |

#### Returns

`dict` -- Profiling report containing:

| Key | Type | Description |
|-----|------|-------------|
| `completeness` | `float` | Overall data completeness score (0.0 -- 1.0) |
| `columns` | `list[dict]` | Per-column profiling results (null rate, unique count, distribution stats) |
| `anomalies` | `list[dict]` | Detected data quality anomalies |
| `expectations` | `list[dict]` | Generated Great Expectations suite |

#### Example

```python
source = project.add_source("patients.csv")
profile_report = project.profile(source)

print(f"Overall completeness: {profile_report['completeness']:.2%}")
# Overall completeness: 94.30%

for col in profile_report["columns"]:
    if col["null_rate"] > 0.1:
        print(f"  Warning: {col['name']} has {col['null_rate']:.1%} nulls")
```

---

### `map_schema()`

Maps source columns to OMOP CDM target tables and fields using AI-assisted matching.

#### Signature

```python
def map_schema(source: dict) -> SchemaMapping
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source` | `dict` | *required* | Source metadata dictionary returned by `add_source()`. |

#### Returns

`SchemaMapping` -- A mapping object containing all proposed column-to-CDM-field mappings with confidence scores and status.

#### Example

```python
source = project.add_source("patients.csv")
schema_map = project.map_schema(source)

# Inspect mappings
for item in schema_map.items:
    print(f"{item.source_column} -> {item.target_table}.{item.target_column} "
          f"(confidence: {item.confidence:.2f}, status: {item.status})")

# Output:
# patient_id -> person.person_id (confidence: 0.98, status: APPROVED)
# birth_date -> person.birth_datetime (confidence: 0.95, status: APPROVED)
# gender -> person.gender_concept_id (confidence: 0.87, status: NEEDS_REVIEW)
# zip_code -> location.zip (confidence: 0.72, status: NEEDS_REVIEW)
```

#### Confidence Routing (Default Thresholds)

| Confidence | Status | Action |
|------------|--------|--------|
| >= 0.90 | `APPROVED` | Auto-accepted |
| 0.70 -- 0.90 | `NEEDS_REVIEW` | Flagged for human review |
| < 0.70 | `UNMAPPED` | Requires manual mapping |

Thresholds are configurable via `PortiereConfig.thresholds.schema_mapping`. See [03-configuration.md](./03-configuration.md).

---

### `map_concepts()`

Maps clinical codes and terms to OMOP standard concepts using hybrid search (dense + sparse retrieval with RRF fusion).

#### Signature

```python
def map_concepts(
    source: Optional[dict] = None,
    codes: Optional[list[str]] = None,
    code_columns: Optional[list[str]] = None,
    vocabularies: Optional[list[str]] = None
) -> ConceptMapping
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source` | `Optional[dict]` | `None` | Source metadata dictionary. When provided, maps all code columns found in the source. |
| `codes` | `Optional[list[str]]` | `None` | Explicit list of clinical codes to map (e.g., `["E11.9", "I10"]`). |
| `code_columns` | `Optional[list[str]]` | `None` | Specific column names in the source to treat as code columns. |
| `vocabularies` | `Optional[list[str]]` | `None` | Vocabulary filter for this mapping. Overrides the project-level vocabulary list. |

At least one of `source` or `codes` must be provided.

#### Returns

`ConceptMapping` -- A mapping object containing resolved concept mappings with candidates, confidence scores, and approval status.

#### Examples

**Auto-discover and map all codes from a source (recommended):**

The simplest approach -- point `map_concepts()` at a source and let the knowledge layer find and map all clinical codes automatically. No need to list codes or specify target vocabularies; Portiere searches across all configured vocabularies (SNOMED, LOINC, RxNorm, ICD10CM by default).

```python
source = project.add_source("conditions.csv")
concept_map = project.map_concepts(source=source)

summary = concept_map.summary()
print(summary)
# {"auto_mapped": 142, "needs_review": 18, "manual_required": 3}
```

**Map specific code columns from a source:**

```python
source = project.add_source("encounters.csv")
concept_map = project.map_concepts(
    source=source,
    code_columns=["diagnosis_code", "procedure_code"]
)
```

**Map specific columns with vocabulary filter:**

```python
source = project.add_source("lab_results.csv")
concept_map = project.map_concepts(
    source=source,
    code_columns=["loinc_code", "result_unit"],
    vocabularies=["LOINC", "UCUM"]
)
```

**Map explicit codes (when you already know the codes):**

```python
concept_map = project.map_concepts(codes=["E11.9", "I10", "J45.0"])

summary = concept_map.summary()
print(summary)
# {"auto_mapped": 2, "needs_review": 1, "manual_required": 0}
```

#### Confidence Routing (Default Thresholds)

| Confidence | Category | Action |
|------------|----------|--------|
| >= 0.95 | `auto_mapped` | Auto-accepted, no review needed |
| 0.80 -- 0.95 | `needs_review` | High confidence but should be verified |
| 0.70 -- 0.80 | `needs_review` | Medium confidence, review recommended |
| < 0.70 | `manual_required` | Low confidence, manual resolution required |

#### Approval and Override

```python
# Approve a mapping without changing the candidate (sets method to AUTO)
concept_map.approve(code="E11.9")

# Override a mapping with a specific concept (sets method to OVERRIDE)
concept_map.override(code="I10", concept_id=320128, concept_name="Essential hypertension")
```

**Important:** `approve()` without candidates sets the mapping method to `AUTO`. `override()` sets the method to `OVERRIDE` (not `MANUAL`).

---

### `run_etl()`

Generates and executes ETL transformation scripts that convert source data into OMOP CDM-formatted output.

#### Signature

```python
def run_etl(
    source: dict,
    output_dir: str,
    schema_mapping: Optional[SchemaMapping] = None,
    concept_mapping: Optional[ConceptMapping] = None
) -> dict
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source` | `dict` | *required* | Source metadata dictionary returned by `add_source()`. |
| `output_dir` | `str` | *required* | Directory path for OMOP-formatted output files. |
| `schema_mapping` | `Optional[SchemaMapping]` | `None` | Schema mapping to apply. If `None`, loads the most recent mapping from the project. |
| `concept_mapping` | `Optional[ConceptMapping]` | `None` | Concept mapping to apply. If `None`, loads the most recent mapping from the project. |

#### Returns

`dict` -- ETL result dictionary containing:

| Key | Type | Description |
|-----|------|-------------|
| `output_path` | `str` | Path to the generated output directory |
| `tables` | `list[dict]` | Per-table output metadata (name, row count, file path) |
| `engine` | `str` | Compute engine used |
| `duration_seconds` | `float` | Total ETL execution time |

#### Example

```python
source = project.add_source("patients.csv")
schema_map = project.map_schema(source)
concept_map = project.map_concepts(source=source)

etl = project.run_etl(
    source,
    output_dir="./omop_output",
    schema_mapping=schema_map,
    concept_mapping=concept_map
)

print(f"ETL completed in {etl['duration_seconds']:.1f}s using {etl['engine']}")
for table in etl["tables"]:
    print(f"  {table['name']}: {table['row_count']} rows -> {table['file_path']}")

# ETL completed in 2.3s using polars
#   person: 15230 rows -> ./omop_output/person.parquet
#   condition_occurrence: 48102 rows -> ./omop_output/condition_occurrence.parquet
```

---

### `validate()`

Validates ETL output against OMOP CDM conformance rules, completeness checks, and plausibility constraints.

**Requires:** `pip install portiere-health[quality]`

#### Signature

```python
def validate(
    etl_result: Optional[dict] = None,
    output_path: Optional[str] = None
) -> dict
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `etl_result` | `Optional[dict]` | `None` | ETL result dictionary returned by `run_etl()`. |
| `output_path` | `Optional[str]` | `None` | Direct path to OMOP output directory. Use when validating previously generated output. |

At least one of `etl_result` or `output_path` must be provided.

#### Returns

`dict` -- Validation report containing:

| Key | Type | Description |
|-----|------|-------------|
| `completeness` | `float` | Data completeness score (0.0 -- 1.0) |
| `conformance` | `float` | CDM structural conformance score (0.0 -- 1.0) |
| `plausibility` | `float` | Clinical plausibility score (0.0 -- 1.0) |
| `passed` | `bool` | Whether all scores meet configured thresholds |
| `details` | `list[dict]` | Per-check results with pass/fail and messages |

#### Default Validation Thresholds

| Metric | Threshold |
|--------|-----------|
| `min_completeness` | 0.95 |
| `min_conformance` | 0.98 |
| `min_plausibility` | 0.90 |

#### Example

```python
etl = project.run_etl(source, output_dir="./output", schema_mapping=schema_map)

validation = project.validate(etl_result=etl)

if validation["passed"]:
    print("All validation checks passed.")
else:
    print("Validation issues found:")
    for check in validation["details"]:
        if not check["passed"]:
            print(f"  FAIL: {check['message']}")

# Validate a previously generated output directory
validation = project.validate(output_path="./output")
```

---

### `push()`

Pushes the current local project (mappings, configurations, and metadata) to Portiere Cloud. Enables collaboration, cloud-based review, and hybrid workflows.

> **Open-source SDK:** `push()` raises `NotImplementedError` in the open-source SDK. Cloud sync requires Portiere Cloud. See [https://portiere.io](https://portiere.io) for details.

#### Signature

```python
def push() -> str
```

#### Parameters

None.

#### Returns

`str` -- The cloud project ID assigned to (or already associated with) this project.

#### Requirements

- Portiere Cloud subscription (not available in the open-source SDK).
- `config.api_key` must be set (via config, environment variable `PORTIERE_API_KEY`, or `portiere.yaml`).
- The Portiere Cloud endpoint must be reachable.

#### Example

```python
import portiere
from portiere.config import PortiereConfig
from portiere.engines import PolarsEngine

config = PortiereConfig(
    api_key="pt_sk_your_api_key",
    storage="local",  # Keep artifacts local
)
project = portiere.init(name="Hospital Migration", engine=PolarsEngine(), config=config)

# ... perform local mapping work ...

# Push to cloud for team review
cloud_id = project.push()
print(f"Project synced to cloud: {cloud_id}")
# Project synced to cloud: proj_a1b2c3d4
```

See [04-operating-modes.md](./04-operating-modes.md) for detailed hybrid sync workflows.

---

### `pull()`

Pulls the latest project state from Portiere Cloud, updating local mappings and metadata. Used in hybrid workflows to sync changes made by collaborators or via the cloud review UI.

> **Open-source SDK:** `pull()` raises `NotImplementedError` in the open-source SDK. Cloud sync requires Portiere Cloud. See [https://portiere.io](https://portiere.io) for details.

#### Signature

```python
def pull() -> None
```

#### Parameters

None.

#### Returns

`None`. Updates the local project state in place.

#### Example

```python
# Pull latest changes from cloud (e.g., after a reviewer approves mappings)
project.pull()

# Load the updated mappings
schema_map = project.load_schema_mapping()
concept_map = project.load_concept_mapping()
```

---

### `load_schema_mapping()`

Loads the most recent schema mapping from the project's local storage. Useful for resuming work or applying previously computed mappings to a new ETL run.

#### Signature

```python
def load_schema_mapping() -> SchemaMapping
```

#### Parameters

None.

#### Returns

`SchemaMapping` -- The most recently saved schema mapping for this project.

#### Example

```python
from portiere.engines import PolarsEngine

# Resume work from a previous session
project = portiere.init(name="Hospital Migration", engine=PolarsEngine())

schema_map = project.load_schema_mapping()
print(f"Loaded {len(schema_map.items)} column mappings")
```

---

### `load_concept_mapping()`

Loads the most recent concept mapping from the project's local storage.

#### Signature

```python
def load_concept_mapping() -> ConceptMapping
```

#### Parameters

None.

#### Returns

`ConceptMapping` -- The most recently saved concept mapping for this project.

#### Example

```python
from portiere.engines import PolarsEngine

project = portiere.init(name="Hospital Migration", engine=PolarsEngine())

concept_map = project.load_concept_mapping()
summary = concept_map.summary()
print(f"Auto-mapped: {summary['auto_mapped']}, Needs review: {summary['needs_review']}")
```

---

### `import_concept_mapping()`

Import an existing concept mapping table into the project. Use this when you already have a mapping table (e.g., from a previous migration or manual curation).

#### Signature

```python
def import_concept_mapping(
    path: str | None = None,
    dataframe: Any = None,
    records: list[dict] | None = None,
) -> ConceptMapping
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | `str \| None` | `None` | Path to a CSV or JSON file containing mappings. |
| `dataframe` | `Any` | `None` | A Pandas, Polars, or Spark DataFrame with mapping data. |
| `records` | `list[dict] \| None` | `None` | A list of dicts, each with at least `source_code`. |

Provide exactly one of `path`, `dataframe`, or `records`.

#### Returns

`ConceptMapping` -- The imported mapping, persisted to project storage.

#### Examples

```python
# Import from CSV
concept_map = project.import_concept_mapping(path="my_mappings.csv")

# Import from a Polars DataFrame
concept_map = project.import_concept_mapping(dataframe=df)

# Import from records
concept_map = project.import_concept_mapping(records=[
    {"source_code": "E11.9", "target_concept_id": 201826, "target_concept_name": "Type 2 diabetes mellitus", "confidence": 0.98},
    {"source_code": "I10", "target_concept_id": 320128, "target_concept_name": "Essential hypertension", "confidence": 0.95},
])
```

---

### `export_concept_mapping()`

Export the project's concept mapping to a file.

#### Signature

```python
def export_concept_mapping(
    path: str,
    *,
    omop_format: bool = False,
) -> str
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | `str` | *required* | Output file path (`.csv` or `.json`). |
| `omop_format` | `bool` | `False` | If True, export as OMOP `source_to_concept_map` format. |

#### Returns

`str` -- The output file path.

#### Examples

```python
# Export to CSV for SME review
project.export_concept_mapping("mappings_for_review.csv")

# Export to JSON
project.export_concept_mapping("mappings.json")

# Export in OMOP source_to_concept_map format
project.export_concept_mapping("source_to_concept_map.csv", omop_format=True)
```

---

## SchemaMapping

Returned by `project.map_schema()` and `project.load_schema_mapping()`.

### Key Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `items` | `list[SchemaMappingItem]` | Individual column mappings |

### SchemaMappingItem Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `source_table` | `str` | Source table name (defaults to `""`) |
| `source_column` | `str` | Source column name |
| `target_table` | `str` | OMOP CDM target table |
| `target_column` | `str` | OMOP CDM target column |
| `confidence` | `float` | Mapping confidence score (0.0 -- 1.0) |
| `status` | `MappingStatus` | Current status: `APPROVED`, `NEEDS_REVIEW`, or `UNMAPPED` |

---

## ConceptMapping

Returned by `project.map_concepts()`, `project.load_concept_mapping()`, or `project.import_concept_mapping()`.

### Key Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `summary()` | `() -> dict` | Returns `{"auto_mapped": int, "needs_review": int, "manual_required": int}` |
| `approve()` | `(code: str) -> None` | Approves a mapping. Sets method to `AUTO` when no candidates specified. |
| `override()` | `(code: str, concept_id: int, concept_name: str) -> None` | Overrides mapping with a specific concept. Sets method to `OVERRIDE`. |
| `to_csv()` | `(path: str) -> None` | Export mappings to CSV. |
| `to_json()` | `(path: str) -> None` | Export mappings to JSON. |
| `to_dataframe()` | `() -> pd.DataFrame` | Export mappings as a pandas DataFrame. |
| `to_source_to_concept_map()` | `() -> list[dict]` | Export in OMOP source_to_concept_map format. |

### Class Methods (Import)

| Method | Signature | Description |
|--------|-----------|-------------|
| `from_csv()` | `(path: str) -> ConceptMapping` | Import from CSV file. Handles column aliases. |
| `from_json()` | `(path: str) -> ConceptMapping` | Import from JSON file. |
| `from_dataframe()` | `(df: Any) -> ConceptMapping` | Import from Pandas, Polars, or Spark DataFrame. |
| `from_records()` | `(records: list[dict]) -> ConceptMapping` | Import from list of dicts. |

### ConceptMappingMethod

| Value | When Set |
|-------|----------|
| `AUTO` | `approve()` without explicit candidates |
| `OVERRIDE` | `override()` with a specific concept |

---

## Knowledge Layer Backends

The knowledge layer provides concept search via pluggable backends. All backends implement a common interface with `search()`, `get_concept()`, and `index_concepts()` methods.

### `build_knowledge_layer()`

Factory function that creates and returns a configured knowledge layer backend instance.

#### Signature

```python
def build_knowledge_layer(
    config: KnowledgeLayerConfig,
    *,
    embedding_gateway: Optional[EmbeddingGateway] = None,
    hybrid_backends: Optional[list[str]] = None,
    **backend_kwargs: Any,
) -> AbstractKnowledgeBackend
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config` | `KnowledgeLayerConfig` | *required* | Knowledge layer configuration specifying the backend and its settings. |
| `embedding_gateway` | `Optional[EmbeddingGateway]` | `None` | Embedding gateway instance for backends that require dense vectors. When `None`, a default gateway is created from the project config. |
| `hybrid_backends` | `Optional[list[str]]` | `None` | Override for `config.hybrid_backends`. Explicit list of sub-backends to combine in hybrid mode. |
| `**backend_kwargs` | `Any` | | Additional keyword arguments passed to the backend constructor. |

---

### `BM25sBackend`

Sparse keyword-based retrieval using BM25s. Zero external dependencies.

```python
class BM25sBackend(AbstractKnowledgeBackend):
    def __init__(self, corpus_path: Optional[str] = None): ...
    def search(self, query: str, top_k: int = 10, vocabularies: Optional[list[str]] = None) -> list[ConceptCandidate]: ...
    def get_concept(self, concept_id: int) -> Optional[ConceptRecord]: ...
    def index_concepts(self, concepts: list[ConceptRecord]) -> None: ...
```

### `FAISSBackend`

Dense semantic search using FAISS indexes.

```python
class FAISSBackend(AbstractKnowledgeBackend):
    def __init__(self, index_path: str, metadata_path: str, embedding_gateway: EmbeddingGateway): ...
    def search(self, query: str, top_k: int = 10, vocabularies: Optional[list[str]] = None) -> list[ConceptCandidate]: ...
    def get_concept(self, concept_id: int) -> Optional[ConceptRecord]: ...
    def index_concepts(self, concepts: list[ConceptRecord]) -> None: ...
```

### `ElasticsearchBackend`

Full-text and structured search using Elasticsearch.

```python
class ElasticsearchBackend(AbstractKnowledgeBackend):
    def __init__(self, url: str, index: str = "portiere_concepts"): ...
    def search(self, query: str, top_k: int = 10, vocabularies: Optional[list[str]] = None) -> list[ConceptCandidate]: ...
    def get_concept(self, concept_id: int) -> Optional[ConceptRecord]: ...
    def index_concepts(self, concepts: list[ConceptRecord]) -> None: ...
```

### `ChromaDBBackend`

Vector search using ChromaDB (embedded or persistent).

```python
class ChromaDBBackend(AbstractKnowledgeBackend):
    def __init__(
        self,
        collection: str = "portiere_concepts",
        persist_path: Optional[Path] = None,
        embedding_gateway: Optional[EmbeddingGateway] = None,
    ): ...
    def search(self, query: str, top_k: int = 10, vocabularies: Optional[list[str]] = None) -> list[ConceptCandidate]: ...
    def get_concept(self, concept_id: int) -> Optional[ConceptRecord]: ...
    def index_concepts(self, concepts: list[ConceptRecord]) -> None: ...
```

**Install:** `pip install portiere-health[chromadb]`

### `PGVectorBackend`

PostgreSQL-native vector search using the pgvector extension.

```python
class PGVectorBackend(AbstractKnowledgeBackend):
    def __init__(
        self,
        connection_string: str,
        table: str = "portiere_concepts",
        embedding_gateway: Optional[EmbeddingGateway] = None,
    ): ...
    def search(self, query: str, top_k: int = 10, vocabularies: Optional[list[str]] = None) -> list[ConceptCandidate]: ...
    def get_concept(self, concept_id: int) -> Optional[ConceptRecord]: ...
    def index_concepts(self, concepts: list[ConceptRecord]) -> None: ...
```

**Install:** `pip install portiere-health[pgvector]`

### `MongoDBBackend`

MongoDB Atlas Vector Search backend.

```python
class MongoDBBackend(AbstractKnowledgeBackend):
    def __init__(
        self,
        connection_string: str,
        database: str = "portiere",
        collection: str = "concepts",
        embedding_gateway: Optional[EmbeddingGateway] = None,
    ): ...
    def search(self, query: str, top_k: int = 10, vocabularies: Optional[list[str]] = None) -> list[ConceptCandidate]: ...
    def get_concept(self, concept_id: int) -> Optional[ConceptRecord]: ...
    def index_concepts(self, concepts: list[ConceptRecord]) -> None: ...
```

**Install:** `pip install portiere-health[mongodb]`

### `QdrantBackend`

High-performance vector search using Qdrant.

```python
class QdrantBackend(AbstractKnowledgeBackend):
    def __init__(
        self,
        url: str,
        collection: str = "portiere_concepts",
        api_key: Optional[str] = None,
        embedding_gateway: Optional[EmbeddingGateway] = None,
    ): ...
    def search(self, query: str, top_k: int = 10, vocabularies: Optional[list[str]] = None) -> list[ConceptCandidate]: ...
    def get_concept(self, concept_id: int) -> Optional[ConceptRecord]: ...
    def index_concepts(self, concepts: list[ConceptRecord]) -> None: ...
```

**Install:** `pip install portiere-health[qdrant]`

### `MilvusBackend`

Scalable vector database for large-scale deployments.

```python
class MilvusBackend(AbstractKnowledgeBackend):
    def __init__(
        self,
        uri: str,
        collection: str = "portiere_concepts",
        embedding_gateway: Optional[EmbeddingGateway] = None,
    ): ...
    def search(self, query: str, top_k: int = 10, vocabularies: Optional[list[str]] = None) -> list[ConceptCandidate]: ...
    def get_concept(self, concept_id: int) -> Optional[ConceptRecord]: ...
    def index_concepts(self, concepts: list[ConceptRecord]) -> None: ...
```

**Install:** `pip install portiere-health[milvus]`

### `HybridBackend`

Combines multiple backends using Reciprocal Rank Fusion (RRF) or weighted fusion.

```python
class HybridBackend(AbstractKnowledgeBackend):
    def __init__(
        self,
        backends: list[AbstractKnowledgeBackend],
        fusion_method: Literal["rrf", "weighted"] = "rrf",
        rrf_k: int = 60,
    ): ...
    def search(self, query: str, top_k: int = 10, vocabularies: Optional[list[str]] = None) -> list[ConceptCandidate]: ...
    def get_concept(self, concept_id: int) -> Optional[ConceptRecord]: ...
    def index_concepts(self, concepts: list[ConceptRecord]) -> None: ...
```

See [17-hybrid-mode.md](./17-hybrid-mode.md) for hybrid search configuration examples.

---

### `KnowledgeLayerConfig` Reference

```python
class KnowledgeLayerConfig(BaseModel):
    backend: Literal["bm25s", "faiss", "elasticsearch", "hybrid",
                     "chromadb", "pgvector", "mongodb", "qdrant", "milvus"] = "bm25s"

    # Existing backend settings
    faiss_index_path: Optional[str] = None
    faiss_metadata_path: Optional[str] = None
    elasticsearch_url: Optional[str] = None
    elasticsearch_index: str = "portiere_concepts"
    bm25s_corpus_path: Optional[str] = None

    # Hybrid settings
    hybrid_backends: list[str] = ["bm25s", "faiss"]
    fusion_method: Literal["rrf", "weighted"] = "rrf"
    rrf_k: int = 60

    # ChromaDB
    chroma_collection: str = "portiere_concepts"
    chroma_persist_path: Optional[Path] = None

    # PGVector
    pgvector_connection_string: Optional[str] = None
    pgvector_table: str = "portiere_concepts"

    # MongoDB
    mongodb_connection_string: Optional[str] = None
    mongodb_database: str = "portiere"
    mongodb_collection: str = "concepts"

    # Qdrant
    qdrant_url: Optional[str] = None
    qdrant_collection: str = "portiere_concepts"
    qdrant_api_key: Optional[str] = None

    # Milvus
    milvus_uri: Optional[str] = None
    milvus_collection: str = "portiere_concepts"
```

See [03-configuration.md](./03-configuration.md) for the full field reference table.

---

## Related Configuration

For full details on configuring the SDK behavior -- thresholds, LLM providers, compute engines, and more -- see [03-configuration.md](./03-configuration.md).

# Quickstart: Getting Started in 5 Minutes

This guide walks you through installing Portiere, running your first mapping pipeline, and understanding what happens at each stage.

---

## Table of Contents

- [Installation](#installation)
- [Your First Mapping Pipeline](#your-first-mapping-pipeline)
- [What Just Happened?](#what-just-happened)
- [Next Steps](#next-steps)

---

## Installation

### Base Install

```bash
pip install portiere
```

The base package includes schema mapping, concept mapping, ETL generation, and local-mode support with the Polars compute engine.

### Optional Extras

Install additional capabilities as needed:

| Extra | Command | What It Adds |
|-------|---------|--------------|
| **quality** | `pip install portiere[quality]` | Great Expectations integration for data profiling and validation |
| **faiss** | `pip install portiere[faiss]` | FAISS vector search backend for the knowledge layer |
| **all** | `pip install portiere[all]` | Everything above, plus Spark and DuckDB engine support |

### Verify Installation

```bash
python -c "import portiere; print(portiere.__version__)"
```

---

## Your First Mapping Pipeline

The entire workflow fits in five lines of Python. No cloud account, no configuration files, and no API keys required -- Portiere runs fully locally by default.

### Standardize: Raw Data to Target Standard

```python
import portiere
from portiere.engines import PolarsEngine

# 1. Initialize a standardization project
project = portiere.init(
    name="Hospital OMOP Migration",
    task="standardize",                   # default
    target_model="omop_cdm_v5.4",        # default
    engine=PolarsEngine(),
)

# 2. Ingest a source file
source = project.add_source("patients.csv")

# 3. Map source columns to OMOP CDM tables and fields
schema_map = project.map_schema(source)

# 4. Map clinical codes to standard vocabularies
concept_map = project.map_concepts(codes=["E11.9", "I10"])

# 5. Generate ETL scripts and validate output
etl = project.run_etl(source, output_dir="./output", schema_mapping=schema_map, concept_mapping=concept_map)
```

### Cross-Map: Transform Between Standards

```python
import portiere
import polars as pl
from portiere.engines import PolarsEngine

# Initialize a cross-mapping project
project = portiere.init(
    name="OMOP to FHIR Export",
    task="cross_map",
    source_standard="omop_cdm_v5.4",
    target_model="fhir_r4",
    engine=PolarsEngine(),
)

# Cross-map — source/target inferred from project settings
omop_df = pl.read_csv("./omop_output/person.csv")
fhir_df = project.cross_map(source_entity="person", data=omop_df)
```

### Step-by-Step Breakdown

#### Step 1: Initialize a Project

```python
from portiere.engines import PolarsEngine

project = portiere.init(name="Hospital OMOP Migration", engine=PolarsEngine())
```

`portiere.init()` creates a new project targeting OMOP CDM v5.4 by default. The `task` parameter (default `"standardize"`) declares the project's purpose -- use `"cross_map"` for transforming between standards. The `engine` parameter is required and specifies the compute engine used for data processing and ETL execution. All configuration is auto-discovered -- Portiere looks for a `portiere.yaml` in your working directory, then falls back to environment variables, then defaults. In local mode, project artifacts are stored under `~/.portiere/projects/`.

#### Step 2: Ingest a Source File

```python
source = project.add_source("patients.csv")
```

`add_source()` registers your data file with the project. Portiere auto-detects the format from the file extension (CSV, Parquet, JSON, etc.). The returned `source` dictionary contains metadata about the file -- column names, inferred types, and row counts -- which downstream stages use.

#### Step 3: Map Schema

```python
schema_map = project.map_schema(source)
```

`map_schema()` analyzes each column in your source and proposes mappings to OMOP CDM target tables and fields. Each mapping includes a confidence score. By default:

- Scores >= 0.90 are auto-accepted.
- Scores between 0.70 and 0.90 are flagged for review.
- Scores below 0.70 require manual mapping.

The returned `SchemaMapping` object lets you inspect, approve, or override any mapping.

#### Step 4: Map Concepts

```python
concept_map = project.map_concepts(codes=["E11.9", "I10"])
```

`map_concepts()` resolves clinical codes (ICD-10, SNOMED, LOINC, RxNorm, etc.) to OMOP standard concept IDs. You can pass individual codes as shown above, or point it at a source to map all code columns automatically -- no need to list codes yourself:

```python
# Auto-detect and map all code columns in the source
concept_map = project.map_concepts(source=source)

# Or specify which columns contain codes
concept_map = project.map_concepts(source=source, code_columns=["diagnosis_code", "drug_code"])
```

The knowledge layer searches across all configured vocabularies (SNOMED, LOINC, RxNorm, ICD10CM by default) to find the best standard concept match for each source code. Confidence routing applies:

- >= 0.95: auto-mapped
- 0.80 -- 0.95: needs verification
- 0.70 -- 0.80: needs review
- < 0.70: requires manual resolution

#### Step 5: Run ETL

```python
etl = project.run_etl(source, output_dir="./output", schema_mapping=schema_map, concept_mapping=concept_map)
```

`run_etl()` generates and executes ETL transformation scripts using the Polars engine (default). Output lands in the specified directory as OMOP-formatted tables.

---

## Database Sources

Portiere can also read directly from databases. Instead of a file path, provide a `connection_string` with a `table` or `query`:

```python
import portiere
from portiere.engines import PolarsEngine

project = portiere.init(name="Hospital Migration", engine=PolarsEngine())

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

# The rest of the pipeline works the same
schema_map = project.map_schema(source)
concept_map = project.map_concepts(source=source, code_columns=["diagnosis_code"])
```

Supported connection URIs include PostgreSQL (`postgresql://`), MySQL (`mysql://`), SQLite (`sqlite:///`), and any database supported by your engine's connector.

---

## Configuring Vocabularies

The `vocabularies` parameter in `portiere.init()` controls which standard vocabularies Portiere searches when mapping clinical codes. It is **optional** -- if omitted, Portiere uses a sensible default set.

### Default Vocabularies

```python
# These two calls are equivalent
project = portiere.init(name="My Project", engine=PolarsEngine())
project = portiere.init(name="My Project", engine=PolarsEngine(), vocabularies=["SNOMED", "LOINC", "RxNorm", "ICD10CM"])
```

| Vocabulary | Covers | Examples |
|-----------|--------|----------|
| **SNOMED** | Clinical findings, procedures, body structures | Diabetes mellitus, Appendectomy |
| **LOINC** | Laboratory tests, clinical observations | Hemoglobin A1c, Blood pressure |
| **RxNorm** | Medications and drug ingredients | Metformin, Aspirin 325mg |
| **ICD10CM** | Diagnosis codes (US billing) | E11.9 (Type 2 diabetes), I10 (Hypertension) |

### Customizing Vocabularies

Select only the vocabularies relevant to your data domain:

```python
# Diagnosis-only mapping (no lab or drug codes)
project = portiere.init(
    name="Diagnosis Migration",
    engine=PolarsEngine(),
    vocabularies=["SNOMED", "ICD10CM"]
)

# Lab data only
project = portiere.init(
    name="Lab Results",
    engine=PolarsEngine(),
    vocabularies=["LOINC"]
)

# Include additional vocabularies
project = portiere.init(
    name="Full Migration",
    engine=PolarsEngine(),
    vocabularies=["SNOMED", "LOINC", "RxNorm", "ICD10CM", "CPT4", "HCPCS"]
)
```

### How Vocabularies Work Locally

When running the pipeline locally, vocabularies are resolved through the **knowledge layer backend**. The `vocabularies` parameter acts as a filter -- it tells Portiere which vocabulary IDs to search within the knowledge layer index.

The knowledge layer must be pre-indexed with vocabulary data before use. By default, Portiere uses a BM25s backend that ships with the base vocabulary set. For production use or custom vocabularies, configure a knowledge layer backend:

```python
from portiere.config import PortiereConfig, KnowledgeLayerConfig

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="bm25s",
        bm25s_corpus_path="/path/to/vocabulary/corpus",
    )
)

project = portiere.init(
    name="My Project",
    engine=PolarsEngine(),
    vocabularies=["SNOMED", "LOINC"],
    config=config,
)
```

For a complete guide on downloading, preparing, and indexing vocabularies, see [Vocabulary Setup](./15-vocabulary-setup.md).

---

## What Just Happened?

Portiere executed a **5-stage pipeline** behind the scenes:

```
Ingest --> Profile --> Schema Map --> ETL Generation --> Validate
```

| Stage | What It Does | Method |
|-------|-------------|--------|
| **Ingest** | Registers source data, detects format, extracts metadata | `add_source()` |
| **Profile** | Analyzes data quality -- completeness, distributions, anomalies | `profile()` (optional, requires `[quality]` extra) |
| **Schema Map** | Maps source columns to OMOP CDM target tables and fields | `map_schema()` |
| **Concept Map** | Resolves clinical codes to OMOP standard concepts | `map_concepts()` |
| **ETL + Validate** | Generates transformation code, executes it, validates output | `run_etl()` / `validate()` |

The knowledge layer powers concept mapping with hybrid search: dense vector retrieval (SapBERT embeddings) combined with sparse keyword matching (BM25), fused via Reciprocal Rank Fusion (RRF). This delivers high accuracy across diverse clinical terminologies without requiring manual dictionary management.

---

## Adding Data Profiling and Validation

If you installed the `quality` extra, you can add profiling and validation to the pipeline:

```python
import portiere
from portiere.engines import PolarsEngine

project = portiere.init(name="Hospital OMOP Migration", engine=PolarsEngine())
source = project.add_source("patients.csv")

# Profile source data quality before mapping
profile_report = project.profile(source)
print(f"Completeness: {profile_report['completeness']}")

# Run schema and concept mapping
schema_map = project.map_schema(source)
concept_map = project.map_concepts(source=source)

# Generate ETL output
etl = project.run_etl(source, output_dir="./output", schema_mapping=schema_map, concept_mapping=concept_map)

# Validate ETL output against OMOP CDM conformance rules
validation = project.validate(etl_result=etl)
print(f"Conformance: {validation['conformance']}")
```

---

## Next Steps

| Topic | Document |
|-------|----------|
| Full SDK API reference with all method signatures and examples | [02-unified-api-reference.md](./02-unified-api-reference.md) |
| Configuration deep dive -- LLM providers, thresholds, engines, YAML config | [03-configuration.md](./03-configuration.md) |
| Local, cloud, and hybrid operating modes | [04-operating-modes.md](./04-operating-modes.md) |

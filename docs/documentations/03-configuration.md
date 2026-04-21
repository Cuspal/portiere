# Configuration Deep Dive

Portiere's configuration system controls everything from AI model selection to compute engines, mapping thresholds, and storage backends. This guide covers every configuration option and the three ways to set them.

---

## Table of Contents

- [Configuration Methods](#configuration-methods)
- [Auto-Discovery: PortiereConfig.discover()](#auto-discovery-portiereconfigdiscover)
- [PortiereConfig (Root)](#portiereconfig-root)
- [LLMConfig -- BYO-LLM Provider Settings](#llmconfig----byo-llm-provider-settings)
- [ThresholdsConfig -- Confidence Routing](#thresholdsconfig----confidence-routing)
- [KnowledgeLayerConfig -- Search Backend](#knowledgelayerconfig----search-backend)
- [EngineConfig -- Compute Engine](#engineconfig----compute-engine)
- [QualityConfig -- Great Expectations Integration](#qualityconfig----great-expectations-integration)
- [ArtifactConfig -- ETL Output Settings](#artifactconfig----etl-output-settings)
- [Complete portiere.yaml Example](#complete-portiereyaml-example)

---

## Configuration Methods

Portiere supports three ways to configure the SDK, listed in precedence order (highest to lowest):

### 1. Python -- Direct Object Construction

Pass a `PortiereConfig` object to `portiere.init()`. This takes highest precedence and is ideal for programmatic control, testing, and notebook workflows.

```python
import portiere
from portiere.config import PortiereConfig, LLMConfig, EngineConfig
from portiere.engines import PolarsEngine

config = PortiereConfig(
    api_key="pt_sk_your_key",
    llm=LLMConfig(
        provider="openai",
        api_key="sk-...",
        model="gpt-4o",
        temperature=0.0
    ),
    engine=EngineConfig(type="polars")
)

project = portiere.init(name="My Project", engine=PolarsEngine(), config=config)
```

### 2. YAML File -- `portiere.yaml`

Place a `portiere.yaml` file in your project's working directory. Loaded automatically by `PortiereConfig.discover()` when no explicit config is passed.

```yaml
# portiere.yaml
# No need to set mode/pipeline — Portiere infers from your configuration
llm:
  provider: openai
  model: gpt-4o
engine:
  type: polars
```

Load explicitly:

```python
from portiere.config import PortiereConfig

config = PortiereConfig.from_yaml("path/to/portiere.yaml")
```

### 3. Environment Variables -- `PORTIERE_*` Prefix

All configuration fields can be set via environment variables using the `PORTIERE_` prefix. Nested fields use double underscores as separators.

```bash
export PORTIERE_MODE=local
export PORTIERE_PIPELINE=cloud
export PORTIERE_API_KEY=pt_sk_your_key
export PORTIERE_TARGET_MODEL=omop_cdm_v5.4
export PORTIERE_LLM__PROVIDER=openai
export PORTIERE_LLM__API_KEY=sk-...
export PORTIERE_LLM__MODEL=gpt-4o
export PORTIERE_ENGINE__TYPE=polars
```

Environment variables are resolved by Pydantic's `BaseSettings` with `env_prefix = "PORTIERE_"`.

### Precedence

When `portiere.init()` is called without an explicit `config`:

1. `PortiereConfig.discover()` is invoked.
2. It checks for `portiere.yaml` in the current working directory.
3. It merges YAML values with environment variables (env vars override YAML).
4. Any unset fields use built-in defaults.

If an explicit `config` is passed to `portiere.init()`, it is used as-is -- YAML and environment variables are not consulted.

---

## Auto-Discovery: `PortiereConfig.discover()`

```python
@classmethod
def discover() -> PortiereConfig
```

Automatically resolves configuration by scanning:

1. `portiere.yaml` in the current working directory
2. Environment variables with `PORTIERE_` prefix
3. Built-in defaults

```python
from portiere.config import PortiereConfig

# Explicit discovery (same as what portiere.init() does internally)
config = PortiereConfig.discover()
print(config.effective_mode)  # "local" (inferred)
print(config.llm.provider)   # "none" (default)
```

---

## PortiereConfig (Root)

The root configuration object. Inherits from Pydantic `BaseSettings`.

```python
class PortiereConfig(BaseSettings):
    storage: Literal["local", "cloud", "auto"] = "auto"   # NEW — explicit storage override
    mode: Literal["local", "cloud", "hybrid"] = "local"    # deprecated — use storage or let Portiere infer
    pipeline: Literal["local", "cloud"] = "local"          # deprecated — auto-inferred from config
    api_key: Optional[str] = None
    endpoint: str = "https://api.portiere.io"
    local_project_dir: Path = ~/.portiere/projects
    knowledge_layer: Optional[KnowledgeLayerConfig] = None
    embedding_model: str = "cambridgeltl/SapBERT-from-PubMedBERT-fulltext"  # legacy
    reranker_model: Optional[str] = "cross-encoder/ms-marco-MiniLM-L-6-v2"  # legacy
    embedding: EmbeddingConfig = EmbeddingConfig()   # preferred
    reranker: RerankerConfig = RerankerConfig()       # preferred
    model_cache_dir: Path = ~/.portiere/models
    llm: LLMConfig = LLMConfig()
    engine: EngineConfig = EngineConfig()
    thresholds: ThresholdsConfig = ThresholdsConfig()
    artifacts: ArtifactConfig = ArtifactConfig()
    target_model: str = "omop_cdm_v5.4"
    custom_standard_path: Optional[Path] = None
    quality: QualityConfig = QualityConfig()

    # Computed properties (read-only)
    effective_mode -> str       # Inferred storage mode
    effective_pipeline -> str   # Inferred pipeline mode

    class Config:
        env_prefix = "PORTIERE_"
```

> **Deprecation Notice:** The `mode` and `pipeline` fields are deprecated. Portiere now infers the correct mode from your configuration:
> - `api_key` set + local AI configured (knowledge_layer, embedding, llm) → `effective_mode="hybrid"`, `effective_pipeline="local"`
> - `api_key` set, no local AI → `effective_mode="cloud"`, `effective_pipeline="cloud"`
> - No `api_key` → `effective_mode="local"`, `effective_pipeline="local"`
>
> Use the `storage` field (values: `"local"`, `"cloud"`, `"auto"`) to explicitly override storage mode when needed. Setting `mode` or `pipeline` directly will emit a `DeprecationWarning`.

### Field Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `storage` | `Literal["local", "cloud", "auto"]` | `"auto"` | Explicit storage override. When `"auto"`, storage mode is inferred from `api_key` and local AI configuration. |
| `mode` | `Literal["local", "cloud", "hybrid"]` | `"local"` | **Deprecated.** Storage mode. Use `storage` or let Portiere infer from your configuration. |
| `pipeline` | `Literal["local", "cloud"]` | `"local"` | **Deprecated.** AI pipeline mode. Auto-inferred: local AI components configured → `"local"`, api_key without local AI → `"cloud"`. |
| `api_key` | `Optional[str]` | `None` | Portiere Cloud API key. Triggers cloud/hybrid mode inference. Format: `pt_sk_...`. **Note:** `api_key` is accepted but ignored in the open-source SDK with a deprecation warning. |
| `endpoint` | `str` | `"https://api.portiere.io"` | Portiere Cloud API endpoint URL. |
| `local_project_dir` | `Path` | `~/.portiere/projects` | Directory for local project storage. Each project gets a subdirectory. |
| `target_model` | `str` | `"omop_cdm_v5.4"` | Target clinical data standard. Supported: `"omop_cdm_v5.4"`, `"fhir_r4"`, `"hl7v2_2.5.1"`, `"openehr_1.0.4"`. See [Multi-Standard Support](./20-multi-standard-support.md). |
| `custom_standard_path` | `Optional[Path]` | `None` | Path to a custom YAML standard definition file. Overrides `target_model` when set. |
| `knowledge_layer` | `Optional[KnowledgeLayerConfig]` | `None` | Knowledge layer search backend configuration. When `None`, uses the default BM25s backend. |
| `embedding_model` | `str` | `"cambridgeltl/SapBERT-from-PubMedBERT-fulltext"` | Sentence-transformer model for dense concept embeddings. SapBERT is pretrained on biomedical text. |
| `reranker_model` | `Optional[str]` | `"cross-encoder/ms-marco-MiniLM-L-6-v2"` | Cross-encoder reranker model for refining search results. Set to `None` to disable reranking. |
| `model_cache_dir` | `Path` | `~/.portiere/models` | Local cache directory for downloaded model weights. |
| `llm` | `LLMConfig` | `LLMConfig()` | LLM provider and model settings. |
| `engine` | `EngineConfig` | `EngineConfig()` | Declarative engine selection (metadata only). The actual engine instance must be passed to `portiere.init(engine=...)`. |
| `thresholds` | `ThresholdsConfig` | `ThresholdsConfig()` | Confidence thresholds for auto-accept, review, and manual routing. |
| `artifacts` | `ArtifactConfig` | `ArtifactConfig()` | ETL output format and artifact settings. |
| `quality` | `QualityConfig` | `QualityConfig()` | Data quality and validation settings (Great Expectations). |

### Class Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `from_yaml` | `(path: str) -> PortiereConfig` | Loads configuration from a YAML file. |
| `discover` | `() -> PortiereConfig` | Auto-discovers configuration from YAML, env vars, and defaults. |

---

## Vocabularies

The `vocabularies` parameter in `portiere.init()` specifies which standard vocabulary IDs to search during concept mapping. This is **not** part of `PortiereConfig` -- it is a per-project parameter passed directly to `portiere.init()`.

### Parameter Reference

| Parameter | Type | Default | Required |
|-----------|------|---------|----------|
| `vocabularies` | `Optional[list[str]]` | `["SNOMED", "LOINC", "RxNorm", "ICD10CM"]` | No |

### Supported Vocabulary IDs

| ID | Full Name | Domain |
|----|-----------|--------|
| `SNOMED` | SNOMED CT | Clinical findings, procedures, anatomy |
| `LOINC` | Logical Observation Identifiers Names and Codes | Lab tests, clinical observations |
| `RxNorm` | RxNorm | Medications, drug ingredients |
| `ICD10CM` | ICD-10-CM | Diagnosis codes (US) |
| `CPT4` | CPT-4 | Procedure codes (US) |
| `HCPCS` | HCPCS | Healthcare procedures and supplies |
| `NDC` | National Drug Code | Drug packaging |

### How Vocabularies Are Used

1. **Project initialization**: `vocabularies` is stored in project metadata.
2. **Concept mapping**: The knowledge layer searches only within the specified vocabulary IDs.
3. **Cloud sync**: Vocabularies are sent to the API when syncing project metadata.

### Relationship to Knowledge Layer

The `vocabularies` parameter is a **filter** applied on top of the knowledge layer backend. The backend index must contain the vocabulary data; `vocabularies` tells Portiere which subset to search.

- **Default BM25s backend**: Ships with SNOMED, LOINC, RxNorm, ICD10CM pre-indexed.
- **Custom backends**: You must index your target vocabularies before use.

```python
from portiere.config import PortiereConfig, KnowledgeLayerConfig
from portiere.engines import PolarsEngine

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="bm25s",
        bm25s_corpus_path="/data/vocab/corpus",
    )
)

project = portiere.init(
    name="My Project",
    engine=PolarsEngine(),
    vocabularies=["SNOMED", "LOINC"],
    config=config,
)
```

See [Vocabulary Setup](./15-vocabulary-setup.md) for step-by-step instructions on building knowledge layer indexes.

---

## LLMConfig -- BYO-LLM Provider Settings

Portiere supports multiple LLM providers for AI-assisted mapping. Bring your own provider for local LLM verification, or just provide an `api_key` to let the Portiere API handle it (cloud pipeline inferred).

```python
class LLMConfig(BaseModel):
    provider: Literal["openai", "azure_openai", "anthropic", "bedrock", "ollama", "none"] = "none"
    endpoint: Optional[str] = None
    api_key: Optional[str] = None
    model: str = "gpt-4o"
    temperature: float = 0.0
    max_tokens: int = 1000
```

### Field Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `provider` | `Literal[...]` | `"none"` | LLM provider. `"none"` disables local LLM (provide `api_key` for managed cloud inference). |
| `endpoint` | `Optional[str]` | `None` | Custom endpoint URL. Required for `"azure_openai"` and `"ollama"`. |
| `api_key` | `Optional[str]` | `None` | Provider-specific API key. Not needed for `"ollama"`. |
| `model` | `str` | `"gpt-4o"` | Model identifier. Provider-specific (e.g., `"gpt-4o"`, `"claude-sonnet-4-20250514"`, `"llama3"` for Ollama). |
| `temperature` | `float` | `0.0` | Sampling temperature. `0.0` recommended for deterministic mapping results. |
| `max_tokens` | `int` | `1000` | Maximum tokens per LLM response. |

**Important:** The LLM is accessed via `LLMGateway`, which takes `config: LLMConfig` as its constructor argument (not a `model=` keyword argument). Access the current model name via `config.llm.model`.

### Provider Examples

**Cloud pipeline (default when api_key is set):**

```python
# No local LLM needed — server handles inference
PortiereConfig(api_key="pt_sk_...")
# → effective_pipeline="cloud", llm.provider="none"
```

**OpenAI:**

```python
LLMConfig(
    provider="openai",
    api_key="sk-...",
    model="gpt-4o",
    temperature=0.0
)
```

**Azure OpenAI:**

```python
LLMConfig(
    provider="azure_openai",
    endpoint="https://your-resource.openai.azure.com/",
    api_key="your-azure-key",
    model="your-deployment-name"
)
```

**Anthropic:**

```python
LLMConfig(
    provider="anthropic",
    api_key="sk-ant-...",
    model="claude-sonnet-4-20250514"
)
```

**AWS Bedrock:**

```python
LLMConfig(
    provider="bedrock",
    model="anthropic.claude-sonnet-4-20250514-v1:0"
    # Uses AWS credentials from environment (AWS_ACCESS_KEY_ID, etc.)
)
```

**Ollama (local):**

```python
LLMConfig(
    provider="ollama",
    endpoint="http://localhost:11434",
    model="llama3"
)
```

### Environment Variables

```bash
export PORTIERE_LLM__PROVIDER=openai
export PORTIERE_LLM__API_KEY=sk-...
export PORTIERE_LLM__MODEL=gpt-4o
export PORTIERE_LLM__TEMPERATURE=0.0
export PORTIERE_LLM__MAX_TOKENS=1000
```

---

## ThresholdsConfig -- Confidence Routing

Controls automatic accept/review/manual routing for schema and concept mappings, plus validation pass/fail criteria.

```python
class ThresholdsConfig(BaseModel):
    schema_mapping: SchemaMappingThresholds
    concept_mapping: ConceptMappingThresholds
    validation: ValidationThresholds
```

### SchemaMappingThresholds

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auto_accept` | `float` | `0.90` | Mappings at or above this score are automatically approved. |
| `needs_review` | `float` | `0.70` | Mappings between this value and `auto_accept` are flagged for review. Below this: `UNMAPPED`. |

### ConceptMappingThresholds

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auto_accept` | `float` | `0.95` | Concepts at or above this score are auto-mapped. |
| `needs_review` | `float` | `0.70` | Concepts between this and `auto_accept` need review. Below this: manual required. |

### ValidationThresholds

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `min_completeness` | `float` | `0.95` | Minimum acceptable data completeness score. |
| `min_conformance` | `float` | `0.98` | Minimum acceptable CDM structural conformance score. |
| `min_plausibility` | `float` | `0.90` | Minimum acceptable clinical plausibility score. |

### Example: Strict Thresholds

```python
from portiere.config import (
    PortiereConfig, ThresholdsConfig,
    SchemaMappingThresholds, ConceptMappingThresholds, ValidationThresholds
)

config = PortiereConfig(
    thresholds=ThresholdsConfig(
        schema_mapping=SchemaMappingThresholds(
            auto_accept=0.95,   # Higher bar for auto-accept
            needs_review=0.80
        ),
        concept_mapping=ConceptMappingThresholds(
            auto_accept=0.98,   # Nearly perfect match required
            needs_review=0.80
        ),
        validation=ValidationThresholds(
            min_completeness=0.99,
            min_conformance=0.99,
            min_plausibility=0.95
        )
    )
)
```

### YAML

```yaml
thresholds:
  schema_mapping:
    auto_accept: 0.95
    needs_review: 0.80
  concept_mapping:
    auto_accept: 0.98
    needs_review: 0.80
  validation:
    min_completeness: 0.99
    min_conformance: 0.99
    min_plausibility: 0.95
```

---

## KnowledgeLayerConfig -- Search Backend

Configures the knowledge layer used for concept search. Portiere supports multiple retrieval backends that can be used individually or combined via hybrid fusion.

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
    hybrid_backends: list[str] = ["bm25s", "faiss"]  # explicit sub-backend list
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

### Field Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `backend` | `Literal[...]` | `"bm25s"` | Search backend. `"hybrid"` combines multiple backends via Reciprocal Rank Fusion. |
| `faiss_index_path` | `Optional[str]` | `None` | Path to FAISS index file. Required when `backend` is `"faiss"`. |
| `faiss_metadata_path` | `Optional[str]` | `None` | Path to metadata file accompanying the FAISS index. |
| `elasticsearch_url` | `Optional[str]` | `None` | Elasticsearch cluster URL. Required when `backend` is `"elasticsearch"`. |
| `elasticsearch_index` | `str` | `"portiere_concepts"` | Elasticsearch index name for concept data. |
| `bm25s_corpus_path` | `Optional[str]` | `None` | Path to BM25s pre-built corpus. |
| `hybrid_backends` | `list[str]` | `["bm25s", "faiss"]` | Explicit list of sub-backends to combine when `backend="hybrid"`. |
| `fusion_method` | `Literal["rrf", "weighted"]` | `"rrf"` | Fusion strategy for hybrid search. RRF (Reciprocal Rank Fusion) is recommended. |
| `rrf_k` | `int` | `60` | RRF parameter `k`. Controls how much weight is given to lower-ranked results. |
| `chroma_collection` | `str` | `"portiere_concepts"` | ChromaDB collection name. |
| `chroma_persist_path` | `Optional[Path]` | `None` | Path to ChromaDB persistent storage directory. When `None`, uses in-memory storage. |
| `pgvector_connection_string` | `Optional[str]` | `None` | PostgreSQL connection string for pgvector. Required when `backend` is `"pgvector"`. |
| `pgvector_table` | `str` | `"portiere_concepts"` | Table name for pgvector concept storage. |
| `mongodb_connection_string` | `Optional[str]` | `None` | MongoDB connection string. Required when `backend` is `"mongodb"`. |
| `mongodb_database` | `str` | `"portiere"` | MongoDB database name. |
| `mongodb_collection` | `str` | `"concepts"` | MongoDB collection name. |
| `qdrant_url` | `Optional[str]` | `None` | Qdrant server URL. Required when `backend` is `"qdrant"`. |
| `qdrant_collection` | `str` | `"portiere_concepts"` | Qdrant collection name. |
| `qdrant_api_key` | `Optional[str]` | `None` | Qdrant API key for authentication. |
| `milvus_uri` | `Optional[str]` | `None` | Milvus server URI. Required when `backend` is `"milvus"`. |
| `milvus_collection` | `str` | `"portiere_concepts"` | Milvus collection name. |

### Backend Selection Guide

| Backend | Best For | Requirements |
|---------|----------|-------------|
| `bm25s` | Quick setup, keyword-heavy clinical codes | None (pure Python) |
| `faiss` | Dense semantic search, conceptual similarity | `pip install portiere[faiss]`, pre-built index |
| `elasticsearch` | Production scale, full-text + structured queries | Running Elasticsearch cluster |
| `chromadb` | Lightweight vector store, embedded or persistent | `pip install portiere[chromadb]` |
| `pgvector` | PostgreSQL-native vector search, existing Postgres infra | `pip install portiere[pgvector]`, PostgreSQL with pgvector extension |
| `mongodb` | MongoDB Atlas Vector Search, existing MongoDB infra | `pip install portiere[mongodb]` |
| `qdrant` | High-performance vector search, filtering | `pip install portiere[qdrant]` |
| `milvus` | Scalable vector database, large-scale deployments | `pip install portiere[milvus]` |
| `hybrid` | Highest accuracy, combines multiple retrieval strategies | Two or more backends configured |

### Example: Hybrid Search (BM25s + FAISS)

```python
from portiere.config import PortiereConfig, KnowledgeLayerConfig

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="hybrid",
        hybrid_backends=["bm25s", "faiss"],
        faiss_index_path="/data/portiere/faiss/concepts.index",
        faiss_metadata_path="/data/portiere/faiss/concepts_meta.json",
        bm25s_corpus_path="/data/portiere/bm25s/corpus",
        fusion_method="rrf",
        rrf_k=60
    )
)
```

### Example: ChromaDB

```python
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="chromadb",
        chroma_collection="portiere_concepts",
        chroma_persist_path="/data/portiere/chroma",
    )
)
```

### Example: PGVector

```python
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="pgvector",
        pgvector_connection_string="postgresql://user:pass@localhost:5432/portiere",
        pgvector_table="portiere_concepts",
    )
)
```

### YAML

```yaml
# BM25s + FAISS hybrid
knowledge_layer:
  backend: hybrid
  hybrid_backends: ["bm25s", "faiss"]
  faiss_index_path: /data/portiere/faiss/concepts.index
  faiss_metadata_path: /data/portiere/faiss/concepts_meta.json
  bm25s_corpus_path: /data/portiere/bm25s/corpus
  fusion_method: rrf
  rrf_k: 60

# ChromaDB
# knowledge_layer:
#   backend: chromadb
#   chroma_collection: portiere_concepts
#   chroma_persist_path: /data/portiere/chroma

# PGVector
# knowledge_layer:
#   backend: pgvector
#   pgvector_connection_string: postgresql://user:pass@localhost:5432/portiere
#   pgvector_table: portiere_concepts

# Qdrant
# knowledge_layer:
#   backend: qdrant
#   qdrant_url: http://localhost:6333
#   qdrant_collection: portiere_concepts

# MongoDB Atlas Vector Search
# knowledge_layer:
#   backend: mongodb
#   mongodb_connection_string: mongodb+srv://user:pass@cluster.mongodb.net
#   mongodb_database: portiere
#   mongodb_collection: concepts

# Milvus
# knowledge_layer:
#   backend: milvus
#   milvus_uri: http://localhost:19530
#   milvus_collection: portiere_concepts
```

---

## EngineConfig -- Compute Engine

Selects the dataframe engine used for ETL execution.

> **Note:** `EngineConfig` in the configuration file is declarative metadata -- it records which engine type is intended for the project. At runtime, you must pass an actual engine instance to `portiere.init(engine=...)`. The `engine` parameter in `portiere.init()` is **required** and takes precedence. `EngineConfig` is useful for YAML configuration files and environment variables where you want to document the intended engine, but it does not auto-create engine instances.

```python
class EngineConfig(BaseModel):
    type: Literal["spark", "polars", "duckdb", "snowpark", "pandas"] = "polars"
    config: dict[str, Any] = {}
```

### Field Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `Literal[...]` | `"polars"` | Compute engine. |
| `config` | `dict[str, Any]` | `{}` | Engine-specific configuration passed to the engine factory. |

**Important:** The engine is instantiated via `get_engine()` (not `create_engine()`). The engine exposes its type via the `engine_name` property (not `name`).

### Engine Selection Guide

| Engine | Best For | Notes |
|--------|----------|-------|
| `polars` | Default, fast local processing | Zero external dependencies, columnar, lazy evaluation |
| `pandas` | Compatibility with existing pandas workflows | Higher memory usage than Polars |
| `duckdb` | SQL-based ETL, analytical queries | Embedded database, good for complex joins |
| `spark` | Large-scale distributed processing | Requires Spark cluster or local Spark install |
| `snowpark` | Snowflake-native ETL | Requires Snowflake account and credentials |

### Examples

**Polars (default):**

```python
EngineConfig(type="polars")
```

**Spark with custom configuration:**

```python
EngineConfig(
    type="spark",
    config={
        "spark.master": "local[4]",
        "spark.driver.memory": "4g",
        "spark.sql.shuffle.partitions": "8"
    }
)
```

**DuckDB with memory limit:**

```python
EngineConfig(
    type="duckdb",
    config={"memory_limit": "2GB", "threads": 4}
)
```

**Snowpark:**

```python
EngineConfig(
    type="snowpark",
    config={
        "account": "your_account",
        "user": "your_user",
        "password": "your_password",
        "warehouse": "COMPUTE_WH",
        "database": "OMOP_DB",
        "schema": "CDM"
    }
)
```

### Passing Engine Instances to `portiere.init()`

The `engine` parameter in `portiere.init()` accepts an `AbstractEngine` instance imported from `portiere.engines`. This is separate from the `EngineConfig` in the configuration object -- the `engine` parameter is a required argument to `portiere.init()` that provides the actual compute engine instance.

**Polars (recommended default):**

```python
import portiere
from portiere.engines import PolarsEngine

project = portiere.init(name="My Project", engine=PolarsEngine())
```

**Spark (for large-scale distributed processing):**

```python
import portiere
from portiere.engines import SparkEngine
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PortiereETL") \
    .master("local[4]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

project = portiere.init(name="Large Scale Migration", engine=SparkEngine(spark))
```

**Pandas (for compatibility with existing pandas workflows):**

```python
import portiere
from portiere.engines import PandasEngine

project = portiere.init(name="Pandas Project", engine=PandasEngine())
```

| Engine Class | Import | Constructor | Best For |
|-------------|--------|-------------|----------|
| `PolarsEngine` | `from portiere.engines import PolarsEngine` | `PolarsEngine()` | Default, fast local processing with lazy evaluation |
| `SparkEngine` | `from portiere.engines import SparkEngine` | `SparkEngine(spark_session)` | Large-scale distributed processing |
| `PandasEngine` | `from portiere.engines import PandasEngine` | `PandasEngine()` | Compatibility with existing pandas workflows |

---

## QualityConfig -- Great Expectations Integration

Controls data profiling and validation behavior. Requires `pip install portiere[quality]`.

```python
class QualityConfig(BaseModel):
    enabled: bool = True
    profile_on_ingest: bool = True
    expectation_suite: Optional[str] = None
    output_format: Literal["json", "html"] = "json"
```

### Field Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `True` | Enable or disable quality checks globally. |
| `profile_on_ingest` | `bool` | `True` | Automatically profile data when `add_source()` is called. |
| `expectation_suite` | `Optional[str]` | `None` | Path to a custom Great Expectations suite JSON file. When `None`, Portiere generates expectations automatically. |
| `output_format` | `Literal["json", "html"]` | `"json"` | Format for validation reports. `"html"` generates a browsable report. |

### Example

```python
from portiere.config import PortiereConfig, QualityConfig

config = PortiereConfig(
    quality=QualityConfig(
        enabled=True,
        profile_on_ingest=True,
        expectation_suite="./expectations/omop_suite.json",
        output_format="html"
    )
)
```

---

## ArtifactConfig -- ETL Output Settings

Controls how ETL output artifacts are generated and stored.

```python
class ArtifactConfig(BaseModel):
    output_format: Literal["parquet", "csv", "json"] = "parquet"
    compression: Optional[str] = "snappy"
    partition_by: Optional[list[str]] = None
    overwrite: bool = True
```

### Field Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `output_format` | `Literal["parquet", "csv", "json"]` | `"parquet"` | File format for ETL output tables. Parquet is recommended for performance and type safety. |
| `compression` | `Optional[str]` | `"snappy"` | Compression codec. Options depend on format (e.g., `"snappy"`, `"gzip"`, `"zstd"` for Parquet). |
| `partition_by` | `Optional[list[str]]` | `None` | Columns to partition output by (e.g., `["year_of_birth"]`). |
| `overwrite` | `bool` | `True` | Whether to overwrite existing output files. |

### Example

```python
from portiere.config import PortiereConfig, ArtifactConfig

config = PortiereConfig(
    artifacts=ArtifactConfig(
        output_format="parquet",
        compression="zstd",
        partition_by=["year_of_birth"],
        overwrite=True
    )
)
```

---

## Complete `portiere.yaml` Example

The following file demonstrates every configuration section. Place it in your project's working directory for automatic discovery.

```yaml
# portiere.yaml -- Full configuration reference

# --- Storage Mode (optional, auto-inferred) ---
# storage: auto        # "local", "cloud", or "auto" (default: inferred from api_key)
# mode and pipeline are deprecated — Portiere infers from your config

# --- Cloud Settings (triggers cloud/hybrid mode) ---
api_key: pt_sk_your_api_key_here
endpoint: https://api.portiere.io

# --- Local Storage ---
local_project_dir: ~/.portiere/projects

# --- Target Clinical Data Standard ---
target_model: omop_cdm_v5.4   # "omop_cdm_v5.4", "fhir_r4", "hl7v2_2.5.1", "openehr_1.0.4"
# custom_standard_path: /path/to/my_custom_standard.yaml  # Optional: load a custom YAML definition

# --- Embedding and Reranker Models ---
# Option A: New structured config (preferred)
embedding:
  provider: huggingface       # "huggingface", "ollama", "openai"
  model: cambridgeltl/SapBERT-from-PubMedBERT-fulltext
  # endpoint: http://localhost:11434   # for ollama / openai-compatible
  # api_key: sk-...                     # for openai
  # batch_size: 64
reranker:
  provider: huggingface       # "huggingface", "none"
  model: cross-encoder/ms-marco-MiniLM-L-6-v2
  # endpoint: ...
  # api_key: ...

# Option B: Legacy string fields (still supported, auto-coerced to huggingface provider)
# embedding_model: cambridgeltl/SapBERT-from-PubMedBERT-fulltext
# reranker_model: cross-encoder/ms-marco-MiniLM-L-6-v2

model_cache_dir: ~/.portiere/models

# --- Smart Defaults ---
# If you only provide api_key (no explicit embedding/reranker config),
# pipeline auto-switches to "cloud" (server handles embedding/reranking).
# Without api_key, pipeline stays "local" with huggingface defaults.

# --- Provider Examples ---
# Ollama (local, no cloud dependency):
# embedding:
#   provider: ollama
#   model: nomic-embed-text
#   endpoint: http://localhost:11434
# reranker:
#   provider: none

# OpenAI:
# embedding:
#   provider: openai
#   model: text-embedding-3-small
#   api_key: ${OPENAI_API_KEY}
# reranker:
#   provider: none

# OpenAI-compatible (vLLM, LiteLLM):
# embedding:
#   provider: openai
#   model: BAAI/bge-large-en-v1.5
#   endpoint: http://localhost:8000/v1

# --- LLM Provider ---
llm:
  provider: openai           # "openai", "azure_openai", "anthropic", "bedrock", "ollama", "none"
  api_key: sk-your-openai-key
  model: gpt-4o
  temperature: 0.0
  max_tokens: 1000

# --- Compute Engine ---
engine:
  type: polars               # "polars", "pandas", "duckdb", "spark", "snowpark"
  config: {}                 # Engine-specific settings (see EngineConfig section)

# --- Confidence Thresholds ---
thresholds:
  schema_mapping:
    auto_accept: 0.90
    needs_review: 0.70
  concept_mapping:
    auto_accept: 0.95
    needs_review: 0.70
  validation:
    min_completeness: 0.95
    min_conformance: 0.98
    min_plausibility: 0.90

# --- Knowledge Layer ---
knowledge_layer:
  backend: bm25s             # "bm25s", "faiss", "elasticsearch", "hybrid",
                             # "chromadb", "pgvector", "mongodb", "qdrant", "milvus"
  # faiss_index_path: /data/portiere/faiss/concepts.index
  # faiss_metadata_path: /data/portiere/faiss/concepts_meta.json
  # elasticsearch_url: http://localhost:9200
  # elasticsearch_index: portiere_concepts
  # bm25s_corpus_path: /data/portiere/bm25s/corpus
  # hybrid_backends: ["bm25s", "faiss"]   # sub-backends for hybrid mode
  fusion_method: rrf
  rrf_k: 60
  # --- ChromaDB ---
  # chroma_collection: portiere_concepts
  # chroma_persist_path: /data/portiere/chroma
  # --- PGVector ---
  # pgvector_connection_string: postgresql://user:pass@localhost:5432/portiere
  # pgvector_table: portiere_concepts
  # --- MongoDB ---
  # mongodb_connection_string: mongodb+srv://user:pass@cluster.mongodb.net
  # mongodb_database: portiere
  # mongodb_collection: concepts
  # --- Qdrant ---
  # qdrant_url: http://localhost:6333
  # qdrant_collection: portiere_concepts
  # qdrant_api_key: null
  # --- Milvus ---
  # milvus_uri: http://localhost:19530
  # milvus_collection: portiere_concepts

# --- ETL Artifact Output ---
artifacts:
  output_format: parquet     # "parquet", "csv", "json"
  compression: snappy        # "snappy", "gzip", "zstd", or null
  partition_by: null         # List of columns, or null
  overwrite: true

# --- Data Quality (Great Expectations) ---
quality:
  enabled: true
  profile_on_ingest: true
  expectation_suite: null    # Path to custom GX suite, or null for auto-generated
  output_format: json        # "json" or "html"
```

---

## See Also

- [01-quickstart.md](./01-quickstart.md) -- Get running in 5 minutes
- [02-unified-api-reference.md](./02-unified-api-reference.md) -- Full SDK API reference
- [04-operating-modes.md](./04-operating-modes.md) -- Local, cloud, and hybrid operating modes
- [20-multi-standard-support.md](./20-multi-standard-support.md) -- Multi-standard support (OMOP, FHIR, HL7 v2, OpenEHR)
- [21-cross-standard-mapping.md](./21-cross-standard-mapping.md) -- Cross-standard mapping guide

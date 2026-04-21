# Building the Knowledge Layer

`build_knowledge_layer()` is the main entry point for creating search indexes from OHDSI Athena vocabulary downloads. It parses Athena CSV files and creates backend-specific indexes that power concept mapping.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Function Signature](#function-signature)
3. [BM25s (Lexical Search)](#bm25s-lexical-search)
4. [FAISS (Dense Vector Search)](#faiss-dense-vector-search)
5. [ChromaDB (Embedded Vector Database)](#chromadb-embedded-vector-database)
6. [PGVector (PostgreSQL)](#pgvector-postgresql)
7. [MongoDB Atlas Vector Search](#mongodb-atlas-vector-search)
8. [Qdrant](#qdrant)
9. [Milvus](#milvus)
10. [Hybrid (Multi-Backend Fusion)](#hybrid-multi-backend-fusion)
11. [Using the Built Index](#using-the-built-index)
12. [Custom Embedding Models](#custom-embedding-models)

---

## Prerequisites

1. **Download Athena vocabularies** from [athena.ohdsi.org](https://athena.ohdsi.org)
2. Extract the zip file — you need at minimum `CONCEPT.csv` (optionally `CONCEPT_SYNONYM.csv`)
3. Install the SDK: `pip install portiere-health`
4. Install backend-specific extras as needed (see each section below)

```
data/athena/
  CONCEPT.csv              # Required
  CONCEPT_SYNONYM.csv      # Optional (adds synonym matching)
  CONCEPT_RELATIONSHIP.csv # Not used by build_knowledge_layer
```

---

## Function Signature

```python
from portiere.knowledge import build_knowledge_layer

paths = build_knowledge_layer(
    athena_path="./data/athena/",       # Path to Athena download directory
    output_path="./data/vocab/",        # Where to save index files
    backend="bm25s",                    # Backend type (see sections below)
    vocabularies=["SNOMED", "LOINC"],   # Filter vocabularies (None = all)
    embedding_model="cambridgeltl/SapBERT-from-PubMedBERT-fulltext",  # For vector backends
    # Keyword arguments for specific backends:
    embedding_gateway=None,             # Pre-configured EmbeddingGateway
    hybrid_backends=None,               # Sub-backends for hybrid mode
    # **backend_kwargs for connection strings, collection names, etc.
)
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `athena_path` | `str \| Path` | required | Path to extracted Athena download directory |
| `output_path` | `str \| Path` | required | Directory to save index files |
| `backend` | `str` | `"bm25s"` | Backend type (see below) |
| `vocabularies` | `list[str] \| None` | `None` | Filter to specific vocabulary IDs |
| `embedding_model` | `str` | `"cambridgeltl/SapBERT-from-PubMedBERT-fulltext"` | Sentence-transformer model name |
| `embedding_gateway` | `EmbeddingGateway \| None` | `None` | Pre-configured embedding gateway |
| `hybrid_backends` | `list[str] \| None` | `None` | Sub-backends for hybrid mode |
| `**backend_kwargs` | | | Backend-specific connection parameters |

### Return Value

Returns a `dict` with backend-specific configuration keys that can be passed directly to `KnowledgeLayerConfig`:

```python
# Example return values by backend:
{"bm25s_corpus_path": "./data/vocab/concepts.json"}
{"faiss_index_path": "...", "faiss_metadata_path": "..."}
{"chroma_persist_path": "./data/vocab/chroma"}
{"pgvector_connection_string": "postgresql://..."}
{"mongodb_connection_string": "mongodb+srv://..."}
{"qdrant_url": ":memory:"}
{"milvus_uri": "./data/vocab/milvus_portiere.db"}
```

---

## BM25s (Lexical Search)

Pure Python BM25 implementation. No external dependencies, works offline, best for getting started quickly.

**Install:** No extras needed — included in base `pip install portiere-health`.

```python
from portiere.knowledge import build_knowledge_layer
from portiere.config import PortiereConfig, KnowledgeLayerConfig

# Step 1: Build the index
paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="bm25s",
    vocabularies=["SNOMED", "LOINC", "RxNorm", "ICD10CM"],
)
# paths = {"bm25s_corpus_path": "./data/vocab/concepts.json"}

# Step 2: Use in config
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="bm25s",
        **paths,  # bm25s_corpus_path="./data/vocab/concepts.json"
    )
)
```

**What it creates:**
- `concepts.json` — JSON file containing all filtered concepts with metadata

**Best for:** Small-to-medium vocabularies (<1M concepts), offline environments, keyword-level matching.

---

## FAISS (Dense Vector Search)

Local vector similarity search using Facebook AI Similarity Search. Embeds concept names into dense vectors and finds semantically similar concepts.

**Install:** `pip install portiere-health[faiss]`

```python
from portiere.knowledge import build_knowledge_layer
from portiere.config import PortiereConfig, KnowledgeLayerConfig

# Step 1: Build the index (downloads embedding model on first run)
paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="faiss",
    vocabularies=["SNOMED", "LOINC", "RxNorm"],
    embedding_model="cambridgeltl/SapBERT-from-PubMedBERT-fulltext",
)
# paths = {
#     "faiss_index_path": "./data/vocab/concepts.index",
#     "faiss_metadata_path": "./data/vocab/concepts.meta.json",
# }

# Step 2: Use in config
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="faiss",
        **paths,
    )
)
```

**What it creates:**
- `concepts.index` — FAISS binary index file
- `concepts.meta.json` — Metadata mapping index positions to concept records

**Best for:** High-accuracy semantic search, catching synonyms and paraphrases that BM25 misses.

---

## ChromaDB (Embedded Vector Database)

Lightweight embedded vector database. Persists to disk with zero infrastructure — great for local development with vector search capabilities.

**Install:** `pip install portiere-health[chromadb]`

```python
from portiere.knowledge import build_knowledge_layer
from portiere.config import PortiereConfig, KnowledgeLayerConfig

# Step 1: Build the index
paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="chromadb",
    vocabularies=["SNOMED", "LOINC", "RxNorm"],
    # Optional: customize collection name
    chroma_collection="my_concepts",
)
# paths = {"chroma_persist_path": "./data/vocab/chroma"}

# Step 2: Use in config
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="chromadb",
        chroma_persist_path="./data/vocab/chroma",
        chroma_collection="my_concepts",  # default: "portiere_concepts"
    )
)
```

**What it creates:**
- `chroma/` directory — ChromaDB persistent storage with embeddings and metadata

**Configuration options:**

| Kwarg | Default | Description |
|-------|---------|-------------|
| `chroma_collection` | `"portiere_concepts"` | Collection name |
| `chroma_persist_path` | `<output_path>/chroma` | Persistence directory |

**Best for:** Local development, simple setup, no server required, persistent vector search.

---

## PGVector (PostgreSQL)

Uses PostgreSQL with the pgvector extension. Ideal for teams already running PostgreSQL — adds vector search without introducing new infrastructure.

**Install:** `pip install portiere-health[pgvector]`

**Prerequisite:** PostgreSQL with `pgvector` extension installed.

```python
from portiere.knowledge import build_knowledge_layer
from portiere.config import PortiereConfig, KnowledgeLayerConfig

# Step 1: Build the index (inserts into PostgreSQL)
paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="pgvector",
    vocabularies=["SNOMED", "LOINC", "RxNorm"],
    pgvector_connection_string="postgresql://user:pass@localhost:5432/portiere",
    pgvector_table="portiere_concepts",  # default
)
# paths = {"pgvector_connection_string": "postgresql://user:pass@localhost:5432/portiere"}

# Step 2: Use in config
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="pgvector",
        pgvector_connection_string="postgresql://user:pass@localhost:5432/portiere",
        pgvector_table="portiere_concepts",
    )
)
```

**What it creates:**
- PostgreSQL table with columns: `concept_id`, `concept_name`, `vocabulary_id`, `domain_id`, `concept_class_id`, `standard_concept`, `embedding` (vector)
- IVFFlat index on the embedding column for fast cosine similarity search
- Automatically runs `CREATE EXTENSION IF NOT EXISTS vector`

**Configuration options:**

| Kwarg | Default | Description |
|-------|---------|-------------|
| `pgvector_connection_string` | required | PostgreSQL connection string |
| `pgvector_table` | `"portiere_concepts"` | Table name |

**Best for:** Teams already using PostgreSQL, moderate-scale vocabularies, unified database infrastructure.

---

## MongoDB Atlas Vector Search

Uses MongoDB with Atlas Vector Search for similarity search. Requires a MongoDB Atlas cluster with a vector search index configured.

**Install:** `pip install portiere-health[mongodb]`

**Prerequisite:** MongoDB Atlas cluster with vector search index named `concept_vector_index` on the `embedding` field.

```python
from portiere.knowledge import build_knowledge_layer
from portiere.config import PortiereConfig, KnowledgeLayerConfig

# Step 1: Build the index (upserts into MongoDB)
paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="mongodb",
    vocabularies=["SNOMED", "LOINC", "RxNorm"],
    mongodb_connection_string="mongodb+srv://user:pass@cluster.mongodb.net/",
    mongodb_database="portiere",
    mongodb_collection="concepts",
)
# paths = {"mongodb_connection_string": "mongodb+srv://..."}

# Step 2: Use in config
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="mongodb",
        mongodb_connection_string="mongodb+srv://user:pass@cluster.mongodb.net/",
        mongodb_database="portiere",
        mongodb_collection="concepts",
    )
)
```

**What it creates:**
- Documents in MongoDB with fields: `concept_id`, `concept_name`, `vocabulary_id`, `domain_id`, `concept_class_id`, `standard_concept`, `embedding`
- Indexes on `concept_id` (unique), `vocabulary_id`, and `domain_id`
- Uses bulk upsert (UpdateOne with upsert=True) for idempotent indexing

**Configuration options:**

| Kwarg | Default | Description |
|-------|---------|-------------|
| `mongodb_connection_string` | required | MongoDB connection URI |
| `mongodb_database` | `"portiere"` | Database name |
| `mongodb_collection` | `"concepts"` | Collection name |

**Best for:** Teams already using MongoDB Atlas, cloud-native deployments.

---

## Qdrant

High-performance vector database with rich filtering and payload storage. Supports in-memory mode for development and remote server for production.

**Install:** `pip install portiere-health[qdrant]`

```python
from portiere.knowledge import build_knowledge_layer
from portiere.config import PortiereConfig, KnowledgeLayerConfig

# Option A: In-memory (development / testing)
paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="qdrant",
    vocabularies=["SNOMED", "LOINC", "RxNorm"],
    # No qdrant_url → defaults to in-memory
)
# paths = {"qdrant_url": ":memory:"}

# Option B: Remote Qdrant server (production)
paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="qdrant",
    vocabularies=["SNOMED", "LOINC", "RxNorm"],
    qdrant_url="http://localhost:6333",
    qdrant_collection="portiere_concepts",
    qdrant_api_key="your-api-key",  # optional, for Qdrant Cloud
)
# paths = {"qdrant_url": "http://localhost:6333"}

# Step 2: Use in config
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="qdrant",
        qdrant_url="http://localhost:6333",
        qdrant_collection="portiere_concepts",
    )
)
```

**What it creates:**
- Qdrant collection with cosine distance vectors
- Payload indexes on `vocabulary_id` and `domain_id` for efficient filtering
- Points with concept metadata stored as payloads

**Configuration options:**

| Kwarg | Default | Description |
|-------|---------|-------------|
| `qdrant_url` | `None` (in-memory) | Qdrant server URL |
| `qdrant_collection` | `"portiere_concepts"` | Collection name |
| `qdrant_api_key` | `None` | API key for Qdrant Cloud |

**Best for:** High-performance production deployments, advanced filtering needs.

---

## Milvus

Distributed vector database designed for billion-scale datasets. Supports Milvus Lite (local file) for development and full Milvus for production.

**Install:** `pip install portiere-health[milvus]`

```python
from portiere.knowledge import build_knowledge_layer
from portiere.config import PortiereConfig, KnowledgeLayerConfig

# Option A: Milvus Lite (local file, no server needed)
paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="milvus",
    vocabularies=["SNOMED", "LOINC", "RxNorm"],
    # No milvus_uri → defaults to local file
)
# paths = {"milvus_uri": "./data/vocab/milvus_portiere.db"}

# Option B: Remote Milvus server (production)
paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="milvus",
    vocabularies=["SNOMED", "LOINC", "RxNorm"],
    milvus_uri="http://localhost:19530",
    milvus_collection="portiere_concepts",
)
# paths = {"milvus_uri": "http://localhost:19530"}

# Step 2: Use in config
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="milvus",
        milvus_uri="./data/vocab/milvus_portiere.db",
        milvus_collection="portiere_concepts",
    )
)
```

**What it creates:**
- Milvus collection with schema: `id` (INT64, primary), `concept_id`, `concept_name`, `vocabulary_id`, `domain_id`, `concept_class_id`, `standard_concept`, `embedding` (FLOAT_VECTOR)
- IVF_FLAT index with cosine metric

**Configuration options:**

| Kwarg | Default | Description |
|-------|---------|-------------|
| `milvus_uri` | `<output_path>/milvus_portiere.db` | Milvus URI (local file or server) |
| `milvus_collection` | `"portiere_concepts"` | Collection name |

**Best for:** Billion-scale distributed deployments, teams already using Milvus.

---

## Hybrid (Multi-Backend Fusion)

Combine multiple backends using Reciprocal Rank Fusion (RRF) for maximum recall and precision. You must explicitly specify which sub-backends to combine via `hybrid_backends`.

**Install:** Install extras for each sub-backend you plan to use.

```python
from portiere.knowledge import build_knowledge_layer
from portiere.config import PortiereConfig, KnowledgeLayerConfig

# Example 1: BM25s (lexical) + ChromaDB (semantic)
paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="hybrid",
    hybrid_backends=["bm25s", "chromadb"],
    vocabularies=["SNOMED", "LOINC", "RxNorm"],
)
# paths = {
#     "bm25s_corpus_path": "./data/vocab/concepts.json",
#     "chroma_persist_path": "./data/vocab/chroma",
#     "hybrid_backends": "bm25s,chromadb",
# }

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="hybrid",
        hybrid_backends=["bm25s", "chromadb"],
        **paths,
    )
)

# Example 2: BM25s + PGVector
paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="hybrid",
    hybrid_backends=["bm25s", "pgvector"],
    vocabularies=["SNOMED", "LOINC"],
    pgvector_connection_string="postgresql://user:pass@localhost:5432/portiere",
)

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="hybrid",
        hybrid_backends=["bm25s", "pgvector"],
        bm25s_corpus_path=paths["bm25s_corpus_path"],
        pgvector_connection_string=paths["pgvector_connection_string"],
    )
)

# Example 3: BM25s + Qdrant
paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="hybrid",
    hybrid_backends=["bm25s", "qdrant"],
    vocabularies=["SNOMED", "LOINC"],
    qdrant_url="http://localhost:6333",
)

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="hybrid",
        hybrid_backends=["bm25s", "qdrant"],
        bm25s_corpus_path=paths["bm25s_corpus_path"],
        qdrant_url="http://localhost:6333",
        fusion_method="rrf",  # default
        rrf_k=60,             # default
    )
)
```

**Hybrid configuration options:**

| Field | Default | Description |
|-------|---------|-------------|
| `hybrid_backends` | `["bm25s", "faiss"]` | List of sub-backend names to combine |
| `fusion_method` | `"rrf"` | Fusion method: `"rrf"` or `"weighted"` |
| `rrf_k` | `60` | RRF smoothing parameter |

**Recommended combinations:**

| Combination | Strengths |
|-------------|-----------|
| `bm25s` + `chromadb` | Lightweight, no server, good accuracy |
| `bm25s` + `faiss` | Classic dense+sparse, fully offline |
| `bm25s` + `pgvector` | Unified PostgreSQL infrastructure |
| `bm25s` + `qdrant` | Production-grade, high performance |
| `bm25s` + `milvus` | Billion-scale distributed search |

---

## Using the Built Index

After building the knowledge layer, pass the returned paths directly to your project config:

```python
import portiere
from portiere.config import PortiereConfig, KnowledgeLayerConfig, LLMConfig

# Build once
paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="chromadb",
    vocabularies=["SNOMED", "LOINC", "RxNorm", "ICD10CM"],
)

# Use in every session
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(backend="chromadb", **paths),
    llm=LLMConfig(provider="ollama", model="llama3"),
)

project = portiere.init("My OMOP Migration", config=config)
project.add_source("patient_data.csv")
schema = project.map_schema()
concepts = project.map_concepts()
```

You only need to call `build_knowledge_layer()` once. The created indexes persist on disk and are reused across sessions.

---

## Custom Embedding Models

By default, vector backends use `cambridgeltl/SapBERT-from-PubMedBERT-fulltext` — a biomedical embedding model optimized for clinical terminology. You can override this:

```python
# Option A: Different model name
paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="chromadb",
    embedding_model="BAAI/bge-base-en-v1.5",
)

# Option B: Pre-configured EmbeddingGateway (for OpenAI, Bedrock, etc.)
from portiere.config import EmbeddingConfig
from portiere.embedding import EmbeddingGateway

gateway = EmbeddingGateway(EmbeddingConfig(
    provider="openai",
    model="text-embedding-3-small",
    api_key="sk-...",
))

paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="qdrant",
    embedding_gateway=gateway,
    qdrant_url="http://localhost:6333",
)
```

> **Important:** Use the same embedding model for building and querying. If you build with SapBERT, configure the same model in your `EmbeddingConfig` at query time.

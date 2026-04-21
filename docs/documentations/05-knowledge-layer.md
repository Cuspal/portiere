# Knowledge Layer

The Knowledge Layer is the search backbone of Portiere's concept mapping pipeline. It indexes
standard vocabularies (SNOMED CT, LOINC, RxNorm, ICD-10, etc.) and retrieves the best candidate
concepts for each source code during mapping. Portiere ships with nine backends that can be used
independently or combined for maximum accuracy.

---

## Table of Contents

1. [Backend Overview](#backend-overview)
2. [BM25s (Default)](#bm25s-default)
3. [FAISS](#faiss)
4. [Elasticsearch](#elasticsearch)
5. [ChromaDB](#chromadb)
6. [PGVector](#pgvector)
7. [MongoDB](#mongodb)
8. [Qdrant](#qdrant)
9. [Milvus](#milvus)
10. [Hybrid](#hybrid)
11. [Embedding Models](#embedding-models)
12. [Cross-Encoder Reranking](#cross-encoder-reranking)
13. [VocabularyBridge -- Cross-Vocabulary Mapping](#vocabularybridge----cross-vocabulary-mapping)
14. [Configuration Reference](#configuration-reference)
15. [Performance Comparison](#performance-comparison)

---

## Backend Overview

| Backend         | Type            | Dependencies       | Offline | Best For                          |
|-----------------|-----------------|--------------------|---------|------------------------------------|
| **bm25s**       | Sparse (BM25)   | None (pure Python) | Yes     | Small-to-medium vocabularies (<1M) |
| **faiss**        | Dense (vectors)  | faiss-cpu/gpu, sentence-transformers | Yes | High-accuracy semantic search |
| **elasticsearch**| Sparse + fuzzy  | Running ES cluster | No      | Existing ES infrastructure         |
| **hybrid**       | Dense + Sparse  | faiss + ES/BM25s   | Depends | Maximum recall and precision       |
| **chromadb**     | Dense (vectors)  | chromadb           | Yes     | Embedded vector DB, simple setup   |
| **pgvector**     | Dense (vectors)  | psycopg, pgvector  | No      | Teams using PostgreSQL             |
| **mongodb**      | Dense (vectors)  | pymongo            | No      | Teams using MongoDB Atlas          |
| **qdrant**       | Dense (vectors)  | qdrant-client      | Depends | High-perf vector search, production |
| **milvus**       | Dense (vectors)  | pymilvus           | Depends | Billion-scale distributed vectors  |

All backends are configured through the `KnowledgeLayerConfig` model and are interchangeable
at runtime. Switching backends requires only a configuration change -- no code modifications.

---

## BM25s (Default)

BM25s is a pure-Python BM25 implementation that requires zero external dependencies. It tokenizes
concept names and descriptions, builds an inverted index, and scores candidates using the
Okapi BM25 ranking function.

### When to Use

- Getting started quickly with no infrastructure setup
- Working offline or in air-gapped environments
- Vocabulary size under 1 million concepts
- Keyword-level matching is sufficient (exact or near-exact term overlap)

### Configuration

```python
from portiere.config import PortiereConfig, KnowledgeLayerConfig

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="bm25s",
        bm25s_corpus_path="/path/to/bm25s_corpus/",
    )
)
```

> **Building the index?** See [Building the Knowledge Layer](./22-build-knowledge-layer.md#bm25s-lexical-search) for step-by-step instructions using `build_knowledge_layer()`.

### Limitations

- No semantic understanding -- relies on token overlap
- Performance degrades on paraphrased or abbreviated source terms
- Not recommended for vocabularies exceeding 1 million concepts (memory and latency)

---

## FAISS

FAISS (Facebook AI Similarity Search) provides dense vector search using sentence-transformer
embeddings. Source terms and vocabulary concepts are encoded into high-dimensional vectors, and
nearest-neighbor search retrieves the most semantically similar candidates.

### When to Use

- High-accuracy semantic matching is required
- Source data contains abbreviations, misspellings, or paraphrased terms
- You need to capture synonymy and relatedness beyond keyword overlap

### Dependencies

```bash
pip install portiere[faiss]
# or for GPU acceleration:
pip install portiere[faiss-gpu]
```

This installs `faiss-cpu` (or `faiss-gpu`) and `sentence-transformers`.

### Configuration

```python
from portiere.config import PortiereConfig, KnowledgeLayerConfig

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="faiss",
        faiss_index_path="/path/to/faiss.index",
        faiss_metadata_path="/path/to/faiss_metadata.json",
    )
)
```

> **Building the index?** See [Building the Knowledge Layer](./22-build-knowledge-layer.md#faiss-dense-vector-search) for step-by-step instructions using `build_knowledge_layer()`.

### GPU Acceleration

For large vocabularies (>1M concepts), GPU-accelerated FAISS significantly reduces both index
build time and query latency:

```python
import faiss

# Move an existing index to GPU
cpu_index = faiss.read_index("/path/to/faiss.index")
gpu_resource = faiss.StandardGpuResources()
gpu_index = faiss.index_cpu_to_gpu(gpu_resource, 0, cpu_index)
```

---

## Elasticsearch

The Elasticsearch backend delegates search to an existing ES cluster. It supports keyword
matching, fuzzy search, and custom analyzers for biomedical text.

### When to Use

- Your organization already operates an Elasticsearch cluster
- You need fuzzy matching, stemming, or custom tokenization
- You want to search across multiple vocabulary fields (name, synonyms, descriptions)

### Dependencies

An accessible Elasticsearch cluster (version 7.x or 8.x) is required. No additional Python
packages beyond the base SDK are needed.

### Configuration

```python
from portiere.config import PortiereConfig, KnowledgeLayerConfig

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="elasticsearch",
        elasticsearch_url="http://localhost:9200",
        elasticsearch_index="portiere_concepts",
    )
)
```

> **Setting up the index?** See the [Elasticsearch Backend](./16-elasticsearch-backend.md) guide for detailed index setup and custom analyzers.

---

## ChromaDB

ChromaDB is an embedded vector database that stores embeddings locally with minimal setup. It
handles embedding storage, indexing, and nearest-neighbor search in a single lightweight package,
making it an excellent choice for local development and small-to-medium deployments.

### When to Use

- You want a simple embedded vector store with zero infrastructure
- Local development and prototyping with persistent storage
- Projects that need a self-contained vector database without running external services
- Vocabulary size under 5 million concepts

### Dependencies

```bash
pip install portiere[chromadb]
```

This installs the `chromadb` package.

### Configuration

```python
from portiere.config import PortiereConfig, KnowledgeLayerConfig

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="chromadb",
        chroma_collection="portiere_concepts",
        chroma_persist_path="./data/chroma/",
    )
)
```

> **Building the index?** See [Building the Knowledge Layer](./22-build-knowledge-layer.md#chromadb-embedded-vector-database) for step-by-step instructions.

---

## PGVector

PGVector extends PostgreSQL with vector similarity search capabilities. If your team already
runs PostgreSQL, PGVector lets you store and search embeddings alongside your relational data
without introducing a separate vector database.

### When to Use

- Your team already uses PostgreSQL and wants to avoid adding another database
- You want vectors and relational data in the same database for transactional consistency
- Moderate-scale deployments (up to a few million concepts)

### Dependencies

```bash
pip install portiere[pgvector]
```

This installs `psycopg` and `pgvector`. You must also install the `pgvector` extension in your
PostgreSQL instance (`CREATE EXTENSION vector;`).

### Configuration

```python
from portiere.config import PortiereConfig, KnowledgeLayerConfig

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="pgvector",
        pgvector_connection_string="postgresql://user:pass@localhost:5432/portiere",
        pgvector_table="concept_embeddings",
    )
)
```

> **Building the index?** See [Building the Knowledge Layer](./22-build-knowledge-layer.md#pgvector-postgresql) for step-by-step instructions.

---

## MongoDB

The MongoDB backend uses MongoDB Atlas Vector Search to store and query embeddings. It is
well-suited for teams already using MongoDB who want to add semantic search without introducing
a separate vector database.

### When to Use

- Your team already uses MongoDB Atlas
- You want to combine document storage with vector search in a single platform
- You need flexible schema alongside vector queries

### Dependencies

```bash
pip install portiere[mongodb]
```

This installs the `pymongo` package. You must have a MongoDB Atlas cluster with Vector Search
enabled, or a self-hosted MongoDB 7.0+ instance with Atlas Search configured.

### Configuration

```python
from portiere.config import PortiereConfig, KnowledgeLayerConfig

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="mongodb",
        mongodb_connection_string="mongodb+srv://user:pass@cluster.mongodb.net/",
        mongodb_database="portiere",
        mongodb_collection="concept_embeddings",
    )
)
```

> **Building the index?** See [Building the Knowledge Layer](./22-build-knowledge-layer.md#mongodb-atlas-vector-search) for step-by-step instructions.

---

## Qdrant

Qdrant is a high-performance vector search engine built for production workloads. It supports
filtering, payload indexing, and horizontal scaling, making it a strong choice for large-scale
production deployments.

### When to Use

- Production deployments requiring high throughput and low latency
- You need advanced filtering (e.g., search within a specific vocabulary or domain)
- Large vocabularies (millions of concepts) where performance matters
- You want a managed cloud option (Qdrant Cloud) or self-hosted flexibility

### Dependencies

```bash
pip install portiere[qdrant]
```

This installs the `qdrant-client` package.

### Configuration

```python
from portiere.config import PortiereConfig, KnowledgeLayerConfig

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="qdrant",
        qdrant_url="http://localhost:6333",
        qdrant_collection="portiere_concepts",
        qdrant_api_key=None,  # set for Qdrant Cloud
    )
)
```

> **Building the index?** See [Building the Knowledge Layer](./22-build-knowledge-layer.md#qdrant) for step-by-step instructions.

---

## Milvus

Milvus is a distributed vector database designed for billion-scale similarity search. It offers
GPU acceleration, horizontal scaling, and is well-suited for the largest vocabulary deployments.

### When to Use

- Billion-scale vocabularies requiring distributed indexing
- GPU-accelerated vector search is needed
- Horizontal scaling across multiple nodes is required
- You need a battle-tested open-source vector database for massive datasets

### Dependencies

```bash
pip install portiere[milvus]
```

This installs the `pymilvus` package.

### Configuration

```python
from portiere.config import PortiereConfig, KnowledgeLayerConfig

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="milvus",
        milvus_uri="http://localhost:19530",
        milvus_collection="portiere_concepts",
    )
)
```

> **Building the index?** See [Building the Knowledge Layer](./22-build-knowledge-layer.md#milvus) for step-by-step instructions.

---

## Hybrid

The Hybrid backend combines multiple search backends and fuses the results using Reciprocal
Rank Fusion (RRF) or weighted score combination. This approach consistently produces the highest
recall and precision across diverse source data quality levels.

The `hybrid_backends` configuration field lets you explicitly specify which backends to combine.
Any combination of backends is supported -- you are not limited to the traditional dense + sparse
pairing.

### When to Use

- Maximum mapping accuracy is the priority
- Source data quality varies (mix of clean terms, abbreviations, and misspellings)
- You can afford the additional infrastructure and compute cost

### How RRF Works

Reciprocal Rank Fusion merges two or more ranked lists without requiring score normalization. For
each candidate concept appearing in any result list, the fused score is:

```
RRF_score(c) = sum(1 / (k + rank_i(c))) for each backend i
```

where `k` is a smoothing constant (default 60). Candidates that rank highly in *multiple* lists
receive the highest fused scores, while candidates appearing in only one list are still
considered but ranked lower.

### Configuration

Use the `hybrid_backends` field to specify which backends to combine. Each sub-backend's
configuration fields must also be provided.

#### Example: BM25s + ChromaDB (lightweight, offline)

```python
from portiere.config import PortiereConfig, KnowledgeLayerConfig

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="hybrid",
        hybrid_backends=["bm25s", "chromadb"],
        bm25s_corpus_path="./vocab/concepts.json",
        chroma_persist_path="./vocab/chroma/",
    )
)
```

#### Example: FAISS + Elasticsearch (classic dense + sparse)

```python
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="hybrid",
        hybrid_backends=["faiss", "elasticsearch"],
        faiss_index_path="/path/to/faiss.index",
        faiss_metadata_path="/path/to/faiss_metadata.json",
        elasticsearch_url="http://localhost:9200",
        elasticsearch_index="portiere_concepts",
        fusion_method="rrf",
        rrf_k=60,
    )
)
```

#### Example: Qdrant + BM25s (production vector + keyword)

```python
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="hybrid",
        hybrid_backends=["qdrant", "bm25s"],
        qdrant_url="http://localhost:6333",
        qdrant_collection="portiere_concepts",
        bm25s_corpus_path="./vocab/concepts.json",
        fusion_method="rrf",
        rrf_k=60,
    )
)
```

#### Example: Three-way fusion

```python
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="hybrid",
        hybrid_backends=["faiss", "bm25s", "chromadb"],
        faiss_index_path="/path/to/faiss.index",
        faiss_metadata_path="/path/to/faiss_metadata.json",
        bm25s_corpus_path="./vocab/concepts.json",
        chroma_persist_path="./vocab/chroma/",
        fusion_method="rrf",
        rrf_k=60,
    )
)
```

### Weighted Fusion Alternative

If you prefer explicit control over the contribution of each search modality:

```python
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="hybrid",
        hybrid_backends=["faiss", "bm25s"],
        faiss_index_path="/path/to/faiss.index",
        faiss_metadata_path="/path/to/faiss_metadata.json",
        bm25s_corpus_path="/path/to/bm25s_corpus/",
        fusion_method="weighted",
        # Weights are set as extra fields (KnowledgeLayerConfig allows extra)
    )
)
```

> **Building hybrid indexes?** See [Building the Knowledge Layer](./22-build-knowledge-layer.md#hybrid-multi-backend-fusion) for programmatic examples using `build_knowledge_layer()` with `hybrid_backends`.

---

## Embedding Models

The embedding model determines how concept names and source terms are encoded into vectors for
FAISS and hybrid search. The default model is optimized for biomedical text.

### Default: SapBERT

```
cambridgeltl/SapBERT-from-PubMedBERT-fulltext
```

SapBERT is a self-alignment pre-trained model built on PubMedBERT. It is specifically designed
for biomedical entity linking and produces embeddings that cluster synonymous medical terms
closely together in vector space.

### Multi-Provider Embedding Support

Portiere supports multiple embedding providers via the `EmbeddingConfig`:

| Provider | Description | Use Case |
|----------|-------------|----------|
| `huggingface` | Local sentence-transformers (default) | Privacy-first, no API calls |
| `ollama` | Local Ollama server | Self-hosted models, no cloud |
| `openai` | OpenAI or OpenAI-compatible endpoints | vLLM, LiteLLM, Together AI |
| `bedrock` | AWS Bedrock (Amazon Titan, Cohere Embed) | AWS-native, data stays in AWS |

> **Note:** For fully managed inference (no local models), just provide an `api_key` — Portiere
> infers cloud pipeline mode and the server handles embedding/reranking.

```python
from portiere.config import PortiereConfig, EmbeddingConfig, KnowledgeLayerConfig

# HuggingFace (default, local)
config = PortiereConfig(
    embedding=EmbeddingConfig(
        provider="huggingface",
        model="cambridgeltl/SapBERT-from-PubMedBERT-fulltext",
    ),
    knowledge_layer=KnowledgeLayerConfig(backend="faiss", ...),
)

# Ollama (local server)
config = PortiereConfig(
    embedding=EmbeddingConfig(
        provider="ollama",
        model="nomic-embed-text",
        endpoint="http://localhost:11434",
    ),
    knowledge_layer=KnowledgeLayerConfig(backend="faiss", ...),
)

# OpenAI / OpenAI-compatible
config = PortiereConfig(
    embedding=EmbeddingConfig(
        provider="openai",
        model="text-embedding-3-small",
        api_key="sk-...",
    ),
    knowledge_layer=KnowledgeLayerConfig(backend="faiss", ...),
)

# Legacy string field (still supported, uses huggingface provider)
config = PortiereConfig(
    embedding_model="your-org/custom-medical-embedder",
    knowledge_layer=KnowledgeLayerConfig(backend="faiss", ...),
)
```

### Model Selection Guidelines

| Model | Provider | Domain | Dimensions | Notes |
|-------|----------|--------|-----------|-------|
| `cambridgeltl/SapBERT-from-PubMedBERT-fulltext` | huggingface | Biomedical | 768 | Default, best for clinical/medical terms |
| `all-MiniLM-L6-v2` | huggingface | General | 384 | Lightweight, fast, good for non-medical |
| `BAAI/bge-base-en-v1.5` | huggingface | General | 768 | Strong general-purpose embeddings |
| `nomic-embed-text` | ollama | General | 768 | Good local alternative via Ollama |
| `text-embedding-3-small` | openai | General | 1536 | OpenAI cloud, high quality |
| `text-embedding-3-large` | openai | General | 3072 | OpenAI cloud, highest quality |

When switching embedding models, you **must** rebuild the FAISS index since vector dimensions
and the embedding space will differ between models.

---

## Cross-Encoder Reranking

After initial retrieval (from any backend), Portiere optionally applies a cross-encoder model
to rerank the top-N candidates. Cross-encoders process the (source_term, candidate_name) pair
jointly, producing more accurate relevance scores than bi-encoder similarity alone.

### Reranker Providers

| Provider | Description | Default Model |
|----------|-------------|---------------|
| `huggingface` | Local cross-encoder (default) | `cross-encoder/ms-marco-MiniLM-L-6-v2` |
| `none` | Disable reranking | — |

```python
from portiere.config import PortiereConfig, RerankerConfig

# Local HuggingFace reranker (default)
config = PortiereConfig(
    reranker=RerankerConfig(provider="huggingface", model="cross-encoder/ms-marco-MiniLM-L-6-v2"),
)

# Disable reranking
config = PortiereConfig(
    reranker=RerankerConfig(provider="none"),
)

# Portiere Cloud reranker
config = PortiereConfig(
    api_key="pt_sk_...",  # auto-selects portiere provider for all models
)
```

For improved biomedical accuracy, consider:

```
GanjinZero/coder_eng_pp
```

### How Reranking Fits the Pipeline

1. The selected backend retrieves the top-K candidates (e.g., K=50).
2. The cross-encoder scores each (source_term, candidate) pair.
3. Candidates are re-sorted by cross-encoder score.
4. The top candidates (e.g., top 5) are returned to the mapping pipeline.

This two-stage retrieval + reranking approach balances recall (broad initial retrieval) with
precision (accurate reranking).

---

## VocabularyBridge -- Cross-Vocabulary Mapping

The VocabularyBridge uses OHDSI Athena's `CONCEPT_RELATIONSHIP.csv` to map concepts between
vocabularies (e.g., OMOP concept IDs to SNOMED codes, ICD-10 to SNOMED, SNOMED to LOINC).
This is distinct from the search backends above -- VocabularyBridge uses known vocabulary
relationships rather than similarity search.

### When to Use

- Converting OMOP concept IDs to standard codes in another vocabulary
- Building crosswalk tables between two vocabularies (e.g., ICD10CM to SNOMED)
- Cross-standard mapping (e.g., OMOP to FHIR) where concept IDs need vocabulary translation
- Generating FHIR CodeableConcept or openEHR DV_CODED_TEXT structures from OMOP concept IDs

### Setup

VocabularyBridge requires an Athena download directory containing `CONCEPT.csv` and
`CONCEPT_RELATIONSHIP.csv`. See [Vocabulary Setup](./15-vocabulary-setup.md) for download
instructions.

```python
from portiere.knowledge import VocabularyBridge

bridge = VocabularyBridge(
    athena_path="./data/athena/",
    vocabularies=["SNOMED", "LOINC", "RxNorm", "ICD10CM"],  # optional filter
)
```

### Concept Lookup

```python
concept = bridge.get_concept(201826)
# {
#     "concept_id": 201826,
#     "concept_name": "Type 2 diabetes mellitus",
#     "vocabulary_id": "SNOMED",
#     "domain_id": "Condition",
#     "concept_class_id": "Clinical Finding",
#     "standard_concept": "S",
#     "concept_code": "44054006",
# }
```

### Cross-Vocabulary Mapping

```python
# Map an OMOP concept to SNOMED equivalents
results = bridge.map_concept(4329847, target_vocabulary="SNOMED")
# [{"concept_id": 4329847, "concept_name": "Blood pressure", "vocabulary_id": "SNOMED", ...}]
```

### Building a Crosswalk

```python
# Build a full ICD10CM → SNOMED crosswalk
crosswalk = bridge.get_crosswalk("ICD10CM", "SNOMED")
# [
#     {"source_concept_id": ..., "source_concept_name": "...", "target_concept_id": ..., ...},
#     ...
# ]
```

### FHIR and OpenEHR Output Formats

```python
# Convert concept to FHIR CodeableConcept
fhir_cc = bridge.concept_to_codeable_concept(201826)
# {"coding": [{"system": "http://snomed.info/sct", "code": "44054006", "display": "Type 2 diabetes mellitus"}], "text": "Type 2 diabetes mellitus"}

# Convert concept to openEHR DV_CODED_TEXT
ehr_ct = bridge.concept_to_dv_coded_text(201826)
# {"_type": "DV_CODED_TEXT", "value": "Type 2 diabetes mellitus", "defining_code": {"terminology_id": {"value": "SNOMED CT"}, "code_string": "44054006"}}
```

### Relationship Types

VocabularyBridge indexes these relationship types from `CONCEPT_RELATIONSHIP.csv`:

| Relationship | Description | Used For |
|-------------|-------------|----------|
| `Maps to` | Equivalence mapping | Default cross-vocabulary mapping |
| `Mapped from` | Reverse equivalence | Reverse lookups |
| `Is a` | Hierarchical parent | Hierarchy navigation |
| `Subsumes` | Hierarchical child | Hierarchy navigation |

By default, `map_concept()` uses only equivalence relationships (`Maps to`, `Mapped from`).
Pass `relationship_types` to include hierarchical relationships:

```python
results = bridge.map_concept(
    201826,
    target_vocabulary="SNOMED",
    relationship_types={"Maps to", "Mapped from", "Is a"},
)
```

### Integration with Cross-Standard Mapping

VocabularyBridge is automatically used by the `vocabulary_lookup` transform type in
crossmap YAML definitions. When a cross-standard mapping references a `vocabulary_lookup`
transform, the CrossStandardMapper delegates to VocabularyBridge for concept translation.

See [Cross-Standard Mapping](./21-cross-standard-mapping.md) for details.

### Statistics

```python
stats = bridge.stats()
# {"concepts": 450000, "forward_relationships": 1200000, "reverse_relationships": 1200000, "vocabularies": ["ICD10CM", "LOINC", "RxNorm", "SNOMED"]}
```

---

## Configuration Reference

The complete `KnowledgeLayerConfig` model:

```python
class KnowledgeLayerConfig(BaseModel):
    backend: Literal["faiss", "elasticsearch", "bm25s", "hybrid", "chromadb", "pgvector", "mongodb", "qdrant", "milvus"] = "bm25s"
    faiss_index_path: Optional[Path] = None
    faiss_metadata_path: Optional[Path] = None
    elasticsearch_url: Optional[str] = None
    elasticsearch_index: str = "portiere_concepts"
    bm25s_corpus_path: Optional[Path] = None
    chroma_collection: str = "portiere_concepts"
    chroma_persist_path: Optional[Path] = None
    pgvector_connection_string: Optional[str] = None
    pgvector_table: str = "concept_embeddings"
    mongodb_connection_string: Optional[str] = None
    mongodb_database: str = "portiere"
    mongodb_collection: str = "concept_embeddings"
    qdrant_url: Optional[str] = None
    qdrant_collection: str = "portiere_concepts"
    qdrant_api_key: Optional[str] = None
    milvus_uri: Optional[str] = None
    milvus_collection: str = "portiere_concepts"
    hybrid_backends: Optional[List[str]] = None
    fusion_method: Literal["rrf", "weighted"] = "rrf"
    rrf_k: int = 60
```

### Field Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `backend` | str | `"bm25s"` | Search backend: `"bm25s"`, `"faiss"`, `"elasticsearch"`, `"hybrid"`, `"chromadb"`, `"pgvector"`, `"mongodb"`, `"qdrant"`, `"milvus"` |
| `faiss_index_path` | Path | None | Path to the FAISS `.index` file |
| `faiss_metadata_path` | Path | None | Path to the FAISS metadata JSON file |
| `elasticsearch_url` | str | None | Elasticsearch cluster URL |
| `elasticsearch_index` | str | `"portiere_concepts"` | Name of the ES index |
| `bm25s_corpus_path` | Path | None | Path to the BM25s corpus directory |
| `chroma_collection` | str | `"portiere_concepts"` | ChromaDB collection name |
| `chroma_persist_path` | Path | None | Path to ChromaDB persistence directory |
| `pgvector_connection_string` | str | None | PostgreSQL connection string for pgvector |
| `pgvector_table` | str | `"concept_embeddings"` | Table name for pgvector embeddings |
| `mongodb_connection_string` | str | None | MongoDB Atlas connection string |
| `mongodb_database` | str | `"portiere"` | MongoDB database name |
| `mongodb_collection` | str | `"concept_embeddings"` | MongoDB collection name for embeddings |
| `qdrant_url` | str | None | Qdrant server URL |
| `qdrant_collection` | str | `"portiere_concepts"` | Qdrant collection name |
| `qdrant_api_key` | str | None | API key for Qdrant Cloud (optional) |
| `milvus_uri` | str | None | Milvus server URI |
| `milvus_collection` | str | `"portiere_concepts"` | Milvus collection name |
| `hybrid_backends` | List[str] | None | List of backend names to combine in hybrid mode (e.g., `["bm25s", "chromadb"]`) |
| `fusion_method` | str | `"rrf"` | Fusion method for hybrid: `"rrf"` or `"weighted"` |
| `rrf_k` | int | `60` | RRF smoothing constant (higher = more conservative fusion) |

---

## Performance Comparison

Benchmarked on a standard vocabulary of 500K concepts with 1,000 source terms of varying quality:

| Backend | Recall@10 | Precision@1 | P95 Latency (ms) | Index Size | Memory |
|---------|-----------|-------------|-------------------|------------|--------|
| BM25s | 0.72 | 0.58 | 12 | 180 MB | 400 MB |
| FAISS (SapBERT) | 0.86 | 0.71 | 45 | 1.5 GB | 2.2 GB |
| Elasticsearch | 0.75 | 0.62 | 25 | 220 MB | N/A (cluster) |
| Hybrid (FAISS + BM25s, RRF) | **0.92** | **0.79** | 55 | 1.7 GB | 2.6 GB |
| Hybrid + Reranking | **0.92** | **0.85** | 120 | 1.7 GB | 3.0 GB |

**Key observations:**

- **BM25s** is the fastest and lightest option. It works well when source terms closely match
  vocabulary names but struggles with abbreviations and paraphrases.
- **FAISS** provides a substantial accuracy improvement through semantic matching. The trade-off
  is higher memory usage and index build time.
- **Elasticsearch** performs comparably to BM25s with the added benefit of fuzzy matching and
  infrastructure scalability.
- **Hybrid** consistently achieves the highest recall by combining the strengths of both dense
  and sparse search.
- **Reranking** adds latency but significantly boosts Precision@1, which directly improves the
  auto-acceptance rate in the concept mapping pipeline.

### Choosing a Backend

- **Just getting started?** Use `bm25s` (default). No setup required.
- **Need better accuracy?** Switch to `faiss` with SapBERT embeddings.
- **Already have Elasticsearch?** Use `elasticsearch` to leverage existing infrastructure.
- **Already have PostgreSQL?** Use `pgvector` to keep vectors alongside your relational data.
- **Want a simple embedded vector DB?** Use `chromadb` for zero-infrastructure vector search.
- **Need production-grade vector search?** Use `qdrant` for high-performance deployments.
- **Billion-scale vocabularies?** Use `milvus` for distributed vector search.
- **Using MongoDB Atlas?** Use `mongodb` to add vector search to your existing cluster.
- **Maximum accuracy?** Use `hybrid` with reranking.

---

## See Also

- [Building the Knowledge Layer](22-build-knowledge-layer.md) -- Programmatic `build_knowledge_layer()` guide for each backend
- [LLM Integration](06-llm-integration.md) -- LLM verification for low-confidence mappings
- [Data Models](07-data-models.md) -- `ConceptCandidate` and scoring details
- [Pipeline Architecture](08-pipeline-architecture.md) -- How the knowledge layer fits into the full pipeline
- [Vocabulary Setup](15-vocabulary-setup.md) -- Downloading and indexing vocabularies from Athena
- [Cross-Standard Mapping](21-cross-standard-mapping.md) -- Using VocabularyBridge in cross-standard transforms

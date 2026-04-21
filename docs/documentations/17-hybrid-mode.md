# Hybrid Search Mode

Hybrid search combines multiple knowledge layer backends to improve concept retrieval accuracy. By setting `backend="hybrid"`, Portiere queries two or more backends in parallel and merges the results using Reciprocal Rank Fusion (RRF).

---

## Table of Contents

1. [Overview](#overview)
2. [How It Works](#how-it-works)
3. [Configuration](#configuration)
4. [Backend Combinations](#backend-combinations)
5. [Fusion Methods](#fusion-methods)
6. [Programmatic Usage with build_knowledge_layer()](#programmatic-usage-with-build_knowledge_layer)
7. [Performance Considerations](#performance-considerations)

---

## Overview

Clinical concept search benefits from combining different retrieval strategies:

- **Sparse retrieval** (BM25s) excels at exact code matches and keyword-heavy clinical terms
- **Dense retrieval** (FAISS, ChromaDB, PGVector, Qdrant, Milvus) excels at semantic similarity and finding conceptually related terms even when wording differs

Hybrid search runs both strategies in parallel and fuses the ranked results, giving you the best of both worlds.

---

## How It Works

1. The query is sent to each sub-backend listed in `hybrid_backends`
2. Each backend returns its own ranked list of concept candidates
3. The results are merged using the configured `fusion_method` (default: RRF)
4. The fused list is returned as a single ranked result set

### Reciprocal Rank Fusion (RRF)

RRF scores each candidate based on its rank position across all backends:

```
RRF_score(candidate) = sum(1 / (rrf_k + rank_i)) for each backend i
```

Where `rrf_k` is a smoothing constant (default: 60) that controls how much weight is given to lower-ranked results. Higher `rrf_k` values reduce the influence of rank position differences.

---

## Configuration

### YAML

```yaml
knowledge_layer:
  backend: hybrid
  hybrid_backends: ["bm25s", "faiss"]
  faiss_index_path: /data/portiere/faiss/concepts.index
  faiss_metadata_path: /data/portiere/faiss/concepts_meta.json
  bm25s_corpus_path: /data/portiere/bm25s/corpus
  fusion_method: rrf
  rrf_k: 60
```

### Python

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
        rrf_k=60,
    )
)
```

### Key Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `backend` | `str` | `"bm25s"` | Must be set to `"hybrid"` to enable hybrid search. |
| `hybrid_backends` | `list[str]` | `["bm25s", "faiss"]` | Explicit list of sub-backends to combine. Each must be a valid backend name. |
| `fusion_method` | `Literal["rrf", "weighted"]` | `"rrf"` | How to merge results from sub-backends. |
| `rrf_k` | `int` | `60` | RRF smoothing parameter. |

---

## Backend Combinations

### BM25s + FAISS (Classic Dense + Sparse)

The default combination. BM25s handles exact keyword matches while FAISS provides semantic similarity search.

```yaml
knowledge_layer:
  backend: hybrid
  hybrid_backends: ["bm25s", "faiss"]
  bm25s_corpus_path: /data/portiere/bm25s/corpus
  faiss_index_path: /data/portiere/faiss/concepts.index
  faiss_metadata_path: /data/portiere/faiss/concepts_meta.json
```

### BM25s + ChromaDB

Lightweight setup using ChromaDB as the dense retrieval backend. Good for development and smaller deployments.

```yaml
knowledge_layer:
  backend: hybrid
  hybrid_backends: ["bm25s", "chromadb"]
  bm25s_corpus_path: /data/portiere/bm25s/corpus
  chroma_collection: portiere_concepts
  chroma_persist_path: /data/portiere/chroma
```

### BM25s + PGVector

Ideal when you already run PostgreSQL. Keeps everything in one database system.

```yaml
knowledge_layer:
  backend: hybrid
  hybrid_backends: ["bm25s", "pgvector"]
  bm25s_corpus_path: /data/portiere/bm25s/corpus
  pgvector_connection_string: postgresql://user:pass@localhost:5432/portiere
  pgvector_table: portiere_concepts
```

### BM25s + Qdrant

High-performance combination for production deployments with filtering requirements.

```yaml
knowledge_layer:
  backend: hybrid
  hybrid_backends: ["bm25s", "qdrant"]
  bm25s_corpus_path: /data/portiere/bm25s/corpus
  qdrant_url: http://localhost:6333
  qdrant_collection: portiere_concepts
  # qdrant_api_key: your-api-key  # if authentication is enabled
```

---

## Fusion Methods

### RRF (Reciprocal Rank Fusion) -- Recommended

RRF is the default and recommended fusion method. It is robust, parameter-light, and does not require score normalization across backends.

```python
KnowledgeLayerConfig(
    backend="hybrid",
    hybrid_backends=["bm25s", "faiss"],
    fusion_method="rrf",
    rrf_k=60,  # default
)
```

**Tuning `rrf_k`:**
- Lower values (e.g., 10-30) amplify rank differences -- top results dominate more
- Higher values (e.g., 60-100) smooth rank differences -- more democratic merging
- Default of 60 works well for most clinical concept search scenarios

### Weighted Fusion

Weighted fusion normalizes scores from each backend and computes a weighted average. Useful when you want to explicitly control the contribution of each backend.

```python
KnowledgeLayerConfig(
    backend="hybrid",
    hybrid_backends=["bm25s", "faiss"],
    fusion_method="weighted",
)
```

---

## Programmatic Usage with `build_knowledge_layer()`

For advanced use cases, you can build the knowledge layer programmatically:

```python
from portiere.config import KnowledgeLayerConfig
from portiere.knowledge import build_knowledge_layer

config = KnowledgeLayerConfig(
    backend="hybrid",
    hybrid_backends=["bm25s", "chromadb"],
    bm25s_corpus_path="/data/portiere/bm25s/corpus",
    chroma_collection="portiere_concepts",
    chroma_persist_path="/data/portiere/chroma",
    fusion_method="rrf",
    rrf_k=60,
)

# Build with default embedding gateway
knowledge_layer = build_knowledge_layer(config)

# Or provide a custom embedding gateway
from portiere.embeddings import EmbeddingGateway

embedding_gw = EmbeddingGateway(provider="huggingface", model="cambridgeltl/SapBERT-from-PubMedBERT-fulltext")
knowledge_layer = build_knowledge_layer(config, embedding_gateway=embedding_gw)

# Override hybrid_backends at build time
knowledge_layer = build_knowledge_layer(
    config,
    hybrid_backends=["bm25s", "pgvector"],
    embedding_gateway=embedding_gw,
)

# Search
results = knowledge_layer.search("type 2 diabetes mellitus", top_k=10)
for r in results:
    print(f"{r.concept_id}: {r.concept_name} (score: {r.score:.3f})")
```

---

## Performance Considerations

- **Latency**: Hybrid search queries all sub-backends in parallel. Total latency is bounded by the slowest backend, not the sum.
- **Indexing**: Each backend must be indexed independently. Use `index_concepts()` on the hybrid layer to index all sub-backends at once.
- **Memory**: Running multiple backends increases memory usage. BM25s + one vector store is the most common and efficient combination.
- **Accuracy**: In benchmarks on clinical concept mapping, BM25s + dense retrieval with RRF fusion consistently outperforms either backend alone.

---

> **Note:** For cloud sync features (push/pull), see Portiere Cloud at [https://portiere.io](https://portiere.io).

## See Also

- [03-configuration.md](./03-configuration.md) -- Full KnowledgeLayerConfig reference
- [02-unified-api-reference.md](./02-unified-api-reference.md) -- Knowledge layer backend API reference
- [05-knowledge-layer.md](./05-knowledge-layer.md) -- Knowledge layer architecture
- [15-vocabulary-setup.md](./15-vocabulary-setup.md) -- Building knowledge layer indexes

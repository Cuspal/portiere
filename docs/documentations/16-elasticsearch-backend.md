# Elasticsearch Backend

The Elasticsearch backend connects Portiere to an external Elasticsearch cluster for concept search. It uses BM25 lexical matching with optional vocabulary and domain filtering, and supports batch search via the multi-search API (`_msearch`).

---

## Table of Contents

1. [When to Use](#when-to-use)
2. [Installation](#installation)
3. [Connecting to Elasticsearch](#connecting-to-elasticsearch)
4. [Indexing Concepts](#indexing-concepts)
5. [Searching Concepts](#searching-concepts)
6. [Batch Search](#batch-search)
7. [Concept Lookup by ID](#concept-lookup-by-id)
8. [Full Pipeline Integration](#full-pipeline-integration)
9. [Hybrid Mode: Elasticsearch + FAISS](#hybrid-mode-elasticsearch--faiss)
10. [Authentication Options](#authentication-options)
11. [Index Schema](#index-schema)
12. [Configuration Reference](#configuration-reference)
13. [Performance Notes](#performance-notes)

---

## When to Use

- Your team already runs an Elasticsearch cluster
- You need horizontal scalability for large vocabularies (millions of concepts)
- You need production-grade infrastructure with monitoring, replication, and backups
- BM25 lexical search is sufficient, or you plan to combine it with FAISS for hybrid search

### When NOT to Use

- You need a zero-dependency, offline-first setup → use **BM25s** instead
- You only need semantic (dense vector) search → use **FAISS** instead
- You want the simplest possible setup for a notebook → use **BM25s** or **FAISS**

---

## Installation

```bash
pip install portiere-health[elasticsearch]
```

This installs the `elasticsearch` Python client (8.x). You also need a running Elasticsearch instance (8.x recommended).

### Quick Start with Docker

```bash
docker run -d --name es-portiere \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  elasticsearch:8.11.0
```

Verify:

```bash
curl http://localhost:9200
```

---

## Connecting to Elasticsearch

```python
from portiere.knowledge.elasticsearch_backend import ElasticsearchBackend

# Local development (no auth)
backend = ElasticsearchBackend(
    url="http://localhost:9200",
    index_name="portiere_concepts",
    verify_certs=False,
)
```

The `index_name` controls which Elasticsearch index is used for concept storage and search. You can run multiple projects against different indices on the same cluster.

---

## Indexing Concepts

Before searching, you need to load your vocabulary concepts into the Elasticsearch index. Concepts should be a list of dictionaries with standard OMOP fields:

```python
import json

# Load concepts from a JSON file
with open("concepts.json") as f:
    concepts = json.load(f)

# Each concept should have at minimum:
# {
#     "concept_id": 201826,
#     "concept_name": "Type 2 diabetes mellitus",
#     "vocabulary_id": "SNOMED",
#     "domain_id": "Condition",
#     "concept_class_id": "Clinical Finding",
#     "standard_concept": "S"
# }

backend.index_concepts(concepts)
print(f"Indexed {len(concepts)} concepts")
```

The `index_concepts()` method:

1. Creates the index with the appropriate mapping if it does not exist
2. Bulk-indexes all concepts using the Elasticsearch `helpers.bulk` API
3. Refreshes the index so documents are immediately searchable

### Re-indexing

Calling `index_concepts()` on an existing index **adds** documents. If you need a clean re-index, delete the index first:

```python
backend.es.indices.delete(index="portiere_concepts", ignore_unavailable=True)
backend.index_concepts(concepts)
```

---

## Searching Concepts

The `search()` method executes a BM25 query across concept names and descriptions:

```python
results = backend.search("diabetes", limit=5)

for r in results:
    print(f"{r['concept_name']:45s}  {r['vocabulary_id']:10s}  score={r['score']:.2f}")
```

### Vocabulary Filtering

Restrict results to specific vocabularies:

```python
results = backend.search("hypertension", vocabularies=["SNOMED"], limit=5)
```

### Domain Filtering

Restrict results to a specific domain (Condition, Drug, Measurement, etc.):

```python
results = backend.search("aspirin", domain="Drug", limit=5)
```

### Combined Filters

```python
results = backend.search(
    "glucose",
    vocabularies=["LOINC"],
    domain="Measurement",
    limit=10,
)
```

---

## Batch Search

For mapping pipelines that need to search many terms at once, `batch_search()` uses the Elasticsearch multi-search API (`_msearch`) to execute all queries in a single round-trip:

```python
queries = ["diabetes", "hypertension", "headache", "metformin", "glucose"]
batch_results = backend.batch_search(queries, limit=3)

for query, results in zip(queries, batch_results):
    print(f"Query: '{query}'")
    for r in results:
        print(f"  → {r['concept_name']:40s}  score={r['score']:.2f}")
```

This is significantly faster than calling `search()` in a loop, especially with network latency.

---

## Concept Lookup by ID

Retrieve a single concept directly by its concept ID:

```python
concept = backend.get_concept(201826)
print(f"{concept['concept_id']}: {concept['concept_name']} ({concept['vocabulary_id']})")
```

---

## Full Pipeline Integration

Use Elasticsearch as the knowledge layer in the full Portiere pipeline by setting `backend="elasticsearch"` in the knowledge layer config:

```python
import portiere
from portiere.config import (
    PortiereConfig,
    KnowledgeLayerConfig,
    RerankerConfig,
)
from portiere.engines import PolarsEngine

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="elasticsearch",
        elasticsearch_url="http://localhost:9200",
        elasticsearch_index="portiere_concepts",
    ),
    reranker=RerankerConfig(provider="none"),
)
# Portiere infers: effective_mode="local", effective_pipeline="local"

project = portiere.init(
    name="ES Pipeline Demo",
    engine=PolarsEngine(),
    target_model="omop_cdm_v5.4",
    config=config,
)

# Add data and map as usual
patients = project.add_source("patients.csv")
diagnoses = project.add_source("diagnoses.csv")

schema_map = project.map_schema(patients)
concept_map = project.map_concepts(source=diagnoses)
```

---

## Hybrid Mode: Elasticsearch + FAISS

Combine Elasticsearch BM25 with FAISS dense vectors for maximum recall and precision. The hybrid backend runs both searches in parallel and merges results using Reciprocal Rank Fusion (RRF):

```python
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="hybrid",
        # BM25 via Elasticsearch
        elasticsearch_url="http://localhost:9200",
        elasticsearch_index="portiere_concepts",
        # Dense vectors via FAISS
        faiss_index_path="faiss/concepts.index",
        faiss_metadata_path="faiss/metadata.json",
        # Fusion settings
        fusion_method="rrf",
        rrf_k=60,
    ),
    embedding=EmbeddingConfig(
        provider="huggingface",
        model="cambridgeltl/SapBERT-from-PubMedBERT-fulltext",
    ),
    reranker=RerankerConfig(provider="none"),
)
```

### How RRF Works

Reciprocal Rank Fusion combines ranked lists from multiple retrieval methods:

```
RRF_score(d) = Σ  1 / (k + rank_i(d))
```

Where `k` is a smoothing parameter (default 60) and `rank_i(d)` is the rank of document `d` in retrieval method `i`. Higher `k` values give more weight to lower-ranked results.

### When to Use Hybrid

- Source terms are a mix of formal codes and free text
- You need both exact term matching (BM25) and semantic similarity (FAISS)
- Highest accuracy is more important than simplicity or latency

See also: [05-knowledge-layer.md](./05-knowledge-layer.md#hybrid) for a full comparison of all backends.

---

## Authentication Options

### No Authentication (Local Development)

```python
backend = ElasticsearchBackend(
    url="http://localhost:9200",
    index_name="portiere_concepts",
    verify_certs=False,
)
```

### Basic Authentication

```python
backend = ElasticsearchBackend(
    url="https://es.mycompany.com:9200",
    index_name="portiere_concepts",
    basic_auth=("elastic", "changeme"),
    verify_certs=True,
)
```

### API Key Authentication

```python
backend = ElasticsearchBackend(
    url="https://es.mycompany.com:9200",
    index_name="portiere_concepts",
    api_key="base64-encoded-api-key",
)
```

### Elastic Cloud

```python
backend = ElasticsearchBackend(
    url="https://my-deployment.es.us-east-1.aws.cloud.es.io:9243",
    index_name="portiere_concepts",
    api_key="cloud-api-key",
)
```

---

## Index Schema

The Elasticsearch backend creates an index with the following mapping:

| Field | Type | Analyzer | Notes |
|-------|------|----------|-------|
| `concept_id` | `integer` | — | Unique identifier |
| `concept_name` | `text` | standard | Boosted 2x in search |
| `vocabulary_id` | `keyword` | — | Used for filtering |
| `domain_id` | `keyword` | — | Used for filtering |
| `concept_class_id` | `keyword` | — | |
| `standard_concept` | `keyword` | — | `S`, `C`, or empty |
| `concept_code` | `keyword` | — | Source vocabulary code |
| `description` | `text` | standard | Boosted 1x in search |

The search query uses `multi_match` across `concept_name` (boosted) and `description` fields.

---

## Configuration Reference

### `KnowledgeLayerConfig` Fields for Elasticsearch

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `backend` | `str` | `"bm25s"` | Set to `"elasticsearch"` |
| `elasticsearch_url` | `str` | `"http://localhost:9200"` | Elasticsearch cluster URL |
| `elasticsearch_index` | `str` | `"portiere_concepts"` | Index name for concepts |

### `ElasticsearchBackend` Constructor

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | `str` | required | Elasticsearch URL |
| `index_name` | `str` | `"portiere_concepts"` | Index name |
| `verify_certs` | `bool` | `True` | Verify TLS certificates |
| `basic_auth` | `tuple` | `None` | `(username, password)` for basic auth |
| `api_key` | `str` | `None` | API key for token-based auth |

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `search` | `search(query, limit=10, vocabularies=None, domain=None)` | BM25 search with optional filters |
| `batch_search` | `batch_search(queries, limit=10, vocabularies=None, domain=None)` | Multi-search in single round-trip |
| `get_concept` | `get_concept(concept_id)` | Lookup by concept ID |
| `index_concepts` | `index_concepts(concepts)` | Bulk index a list of concept dicts |

---

## Performance Notes

| Metric | Elasticsearch | BM25s | FAISS |
|--------|--------------|-------|-------|
| **Index size** | Scales to billions | <1M recommended | <10M recommended |
| **Search latency** | 5-20ms per query | 1-5ms per query | 2-10ms per query |
| **Batch latency** | ~50ms for 100 queries | ~100ms | ~30ms |
| **Infrastructure** | Requires ES cluster | None | None |
| **Offline capable** | No | Yes | Yes |
| **Best accuracy** | Good (lexical) | Good (lexical) | Better (semantic) |
| **Hybrid capable** | Yes (as BM25 arm) | Yes (as BM25 arm) | Yes (as dense arm) |

For production deployments with large vocabularies, Elasticsearch with FAISS hybrid is the recommended configuration.

---

## See Also

- [05-knowledge-layer.md](./05-knowledge-layer.md) — Full knowledge layer documentation with all backends
- [17-hybrid-mode.md](./17-hybrid-mode.md) — Hybrid mode (local + cloud sync) documentation
- [03-configuration.md](./03-configuration.md) — Full configuration reference
- Notebook: `10_elasticsearch_backend.ipynb` — Interactive tutorial

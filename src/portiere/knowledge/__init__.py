"""
Portiere Knowledge Layer — Pluggable search backends for concept lookup.

Supports multiple backends:
- BM25s: Pure Python BM25 search (no external dependencies)
- FAISS: Local vector search with sentence-transformers
- Elasticsearch: Lexical search via ES cluster
- ChromaDB: Embedded vector database
- PGVector: PostgreSQL + pgvector extension
- MongoDB: Atlas Vector Search
- Qdrant: High-performance vector database
- Milvus: Distributed vector database
- Hybrid: RRF fusion of multiple backends
"""

from portiere.knowledge.athena import build_knowledge_layer, load_athena_concepts
from portiere.knowledge.base import KnowledgeLayerBackend
from portiere.knowledge.factory import (
    create_knowledge_backend,
    create_knowledge_backend_from_config,
)
from portiere.knowledge.rrfusion import reciprocal_rank_fusion
from portiere.knowledge.vocabulary_bridge import VocabularyBridge

__all__ = [
    "KnowledgeLayerBackend",
    "VocabularyBridge",
    "build_knowledge_layer",
    "create_knowledge_backend",
    "create_knowledge_backend_from_config",
    "load_athena_concepts",
    "reciprocal_rank_fusion",
]


def __getattr__(name: str):
    """Lazy-load backend classes to avoid importing heavy dependencies."""
    _backend_map = {
        "BM25sBackend": "portiere.knowledge.bm25s_backend",
        "LocalFAISSBackend": "portiere.knowledge.local_faiss_backend",
        "ElasticsearchBackend": "portiere.knowledge.elasticsearch_backend",
        "ChromaDBBackend": "portiere.knowledge.chroma_backend",
        "PGVectorBackend": "portiere.knowledge.pgvector_backend",
        "MongoDBBackend": "portiere.knowledge.mongodb_backend",
        "QdrantBackend": "portiere.knowledge.qdrant_backend",
        "MilvusBackend": "portiere.knowledge.milvus_backend",
        "HybridBackend": "portiere.knowledge.hybrid_backend",
    }
    if name in _backend_map:
        import importlib

        module = importlib.import_module(_backend_map[name])
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

"""
Knowledge Layer Factory — Creates the appropriate backend from config.

Instantiates the configured backend (BM25s, FAISS, Elasticsearch, ChromaDB,
PGVector, MongoDB, Qdrant, Milvus, or Hybrid) based on KnowledgeLayerConfig.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from portiere.config import KnowledgeLayerConfig, PortiereConfig
    from portiere.knowledge.base import KnowledgeLayerBackend

logger = structlog.get_logger(__name__)


def create_knowledge_backend(
    config: KnowledgeLayerConfig,
    *,
    embedding_model: str | None = None,
    embedding_gateway=None,
) -> KnowledgeLayerBackend:
    """
    Create a knowledge layer backend from configuration.

    Args:
        config: Knowledge layer configuration
        embedding_model: Override embedding model name (for FAISS/hybrid, legacy)
        embedding_gateway: EmbeddingGateway instance (preferred, overrides embedding_model)

    Returns:
        Initialized KnowledgeLayerBackend instance

    Raises:
        ValueError: If backend type is not supported or config is incomplete
    """
    backend = config.backend

    if backend == "bm25s":
        return _create_bm25s(config)
    elif backend == "faiss":
        return _create_faiss(config, embedding_model, embedding_gateway)
    elif backend == "elasticsearch":
        return _create_elasticsearch(config)
    elif backend == "chromadb":
        return _create_chromadb(config, embedding_gateway)
    elif backend == "pgvector":
        return _create_pgvector(config, embedding_gateway)
    elif backend == "mongodb":
        return _create_mongodb(config, embedding_gateway)
    elif backend == "qdrant":
        return _create_qdrant(config, embedding_gateway)
    elif backend == "milvus":
        return _create_milvus(config, embedding_gateway)
    elif backend == "hybrid":
        return _create_hybrid(config, embedding_model, embedding_gateway)
    else:
        raise ValueError(f"Unsupported knowledge backend: {backend}")


def create_knowledge_backend_from_config(
    portiere_config: PortiereConfig,
) -> KnowledgeLayerBackend:
    """
    Convenience: create backend directly from PortiereConfig.

    Uses portiere_config.knowledge_layer for backend config and
    portiere_config.embedding for the embedding provider.

    Raises:
        ValueError: If knowledge_layer is not configured
    """
    if portiere_config.knowledge_layer is None:
        raise ValueError(
            "knowledge_layer not configured. Set knowledge_layer in PortiereConfig "
            "or portiere.yaml to use local concept search."
        )

    # Build EmbeddingGateway for vector-based backends
    _VECTOR_BACKENDS = {"faiss", "hybrid", "chromadb", "pgvector", "mongodb", "qdrant", "milvus"}
    embedding_gateway = None
    kl_backend = portiere_config.knowledge_layer.backend
    if kl_backend in _VECTOR_BACKENDS:
        from portiere.embedding import EmbeddingGateway

        embedding_gateway = EmbeddingGateway(portiere_config.embedding)

    return create_knowledge_backend(
        portiere_config.knowledge_layer,
        embedding_model=portiere_config.embedding_model,
        embedding_gateway=embedding_gateway,
    )


def _create_bm25s(config: KnowledgeLayerConfig):
    """Create BM25s backend."""
    from portiere.knowledge.bm25s_backend import BM25sBackend

    if config.bm25s_corpus_path is None:
        raise ValueError(
            "bm25s_corpus_path is required for BM25s backend. "
            "Point it to a JSON file containing concept corpus."
        )

    logger.info("knowledge.creating_backend", backend="bm25s", path=str(config.bm25s_corpus_path))
    return BM25sBackend(corpus_path=config.bm25s_corpus_path)


def _create_faiss(
    config: KnowledgeLayerConfig, embedding_model: str | None, embedding_gateway=None
):
    """Create FAISS backend."""
    from portiere.knowledge.local_faiss_backend import LocalFAISSBackend

    if config.faiss_index_path is None or config.faiss_metadata_path is None:
        raise ValueError("faiss_index_path and faiss_metadata_path are required for FAISS backend.")

    model = embedding_model or "cambridgeltl/SapBERT-from-PubMedBERT-fulltext"
    logger.info("knowledge.creating_backend", backend="faiss", model=model)
    return LocalFAISSBackend(
        index_path=config.faiss_index_path,
        metadata_path=config.faiss_metadata_path,
        embedding_model=model,
        embedding_gateway=embedding_gateway,
    )


def _create_elasticsearch(config: KnowledgeLayerConfig):
    """Create Elasticsearch backend (lightweight BM25 wrapper)."""
    # For now, fall back to BM25s if ES URL is not set, since we don't
    # have a dedicated ES backend in the SDK (the API has one).
    if config.elasticsearch_url is None:
        raise ValueError("elasticsearch_url is required for Elasticsearch backend.")

    # Lazy import — Elasticsearch backend for SDK
    try:
        from portiere.knowledge.elasticsearch_backend import ElasticsearchBackend

        logger.info(
            "knowledge.creating_backend", backend="elasticsearch", url=config.elasticsearch_url
        )
        return ElasticsearchBackend(
            url=config.elasticsearch_url,
            index_name=config.elasticsearch_index,
        )
    except ImportError:
        raise ImportError(
            "elasticsearch package required for Elasticsearch backend. "
            "Install with: pip install elasticsearch"
        )


def _create_chromadb(config: KnowledgeLayerConfig, embedding_gateway=None):
    """Create ChromaDB backend."""
    from portiere.knowledge.chroma_backend import ChromaDBBackend

    logger.info(
        "knowledge.creating_backend", backend="chromadb", collection=config.chroma_collection
    )
    return ChromaDBBackend(
        collection_name=config.chroma_collection,
        persist_path=config.chroma_persist_path,
        embedding_gateway=embedding_gateway,
    )


def _create_pgvector(config: KnowledgeLayerConfig, embedding_gateway=None):
    """Create PGVector backend."""
    if config.pgvector_connection_string is None:
        raise ValueError("pgvector_connection_string is required for PGVector backend.")

    from portiere.knowledge.pgvector_backend import PGVectorBackend

    logger.info("knowledge.creating_backend", backend="pgvector")
    return PGVectorBackend(
        connection_string=config.pgvector_connection_string,
        table_name=config.pgvector_table,
        embedding_gateway=embedding_gateway,
    )


def _create_mongodb(config: KnowledgeLayerConfig, embedding_gateway=None):
    """Create MongoDB backend."""
    if config.mongodb_connection_string is None:
        raise ValueError("mongodb_connection_string is required for MongoDB backend.")

    from portiere.knowledge.mongodb_backend import MongoDBBackend

    logger.info("knowledge.creating_backend", backend="mongodb")
    return MongoDBBackend(
        connection_string=config.mongodb_connection_string,
        database=config.mongodb_database,
        collection=config.mongodb_collection,
        embedding_gateway=embedding_gateway,
    )


def _create_qdrant(config: KnowledgeLayerConfig, embedding_gateway=None):
    """Create Qdrant backend."""
    from portiere.knowledge.qdrant_backend import QdrantBackend

    logger.info("knowledge.creating_backend", backend="qdrant", url=config.qdrant_url)
    return QdrantBackend(
        url=config.qdrant_url,
        collection_name=config.qdrant_collection,
        api_key=config.qdrant_api_key,
        embedding_gateway=embedding_gateway,
    )


def _create_milvus(config: KnowledgeLayerConfig, embedding_gateway=None):
    """Create Milvus backend."""
    from portiere.knowledge.milvus_backend import MilvusBackend

    logger.info("knowledge.creating_backend", backend="milvus", uri=config.milvus_uri)
    return MilvusBackend(
        uri=config.milvus_uri,
        collection_name=config.milvus_collection,
        embedding_gateway=embedding_gateway,
    )


def _create_hybrid(
    config: KnowledgeLayerConfig, embedding_model: str | None, embedding_gateway=None
):
    """Create hybrid backend combining multiple search methods with RRF.

    Uses ``config.hybrid_backends`` to determine which backends to combine.
    """
    from portiere.knowledge.hybrid_backend import HybridBackend

    # Map backend name → factory function
    backend_factory = {
        "bm25s": lambda: _create_bm25s(config),
        "faiss": lambda: _create_faiss(config, embedding_model, embedding_gateway),
        "elasticsearch": lambda: _create_elasticsearch(config),
        "chromadb": lambda: _create_chromadb(config, embedding_gateway),
        "pgvector": lambda: _create_pgvector(config, embedding_gateway),
        "mongodb": lambda: _create_mongodb(config, embedding_gateway),
        "qdrant": lambda: _create_qdrant(config, embedding_gateway),
        "milvus": lambda: _create_milvus(config, embedding_gateway),
    }

    backends = []
    for name in config.hybrid_backends:
        if name not in backend_factory:
            raise ValueError(
                f"Unknown hybrid sub-backend: {name!r}. "
                f"Supported: {', '.join(sorted(backend_factory.keys()))}"
            )
        try:
            backends.append(backend_factory[name]())
        except (ImportError, ValueError) as e:
            logger.warning(f"Hybrid sub-backend {name!r} not available, skipping: {e}")

    if not backends:
        raise ValueError(
            "Hybrid backend requires at least one sub-backend. "
            f"Configured hybrid_backends={config.hybrid_backends} but none could be created. "
            "Ensure the required config fields and packages are available."
        )

    if len(backends) == 1:
        logger.warning(
            "knowledge.hybrid_single_backend",
            message="Only one backend available for hybrid, using it directly.",
        )
        return backends[0]

    logger.info(
        "knowledge.creating_backend",
        backend="hybrid",
        sub_backends=len(backends),
        fusion=config.fusion_method,
    )
    return HybridBackend(
        backends=backends,
        fusion_method=config.fusion_method,
        rrf_k=config.rrf_k,
    )

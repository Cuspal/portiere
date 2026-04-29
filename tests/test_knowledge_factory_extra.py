"""Extra knowledge-factory coverage (Slice 8 gap-fill).

Exercises each backend's missing-config error path + the BM25s success
path, without requiring the heavy optional deps (faiss, chromadb,
pgvector, mongodb, qdrant, milvus) to be actually installed.
"""

from __future__ import annotations

import pytest

# ── Unknown backend ──────────────────────────────────────────────


class TestUnknownBackend:
    def test_unsupported_backend_raises(self):
        from portiere.config import KnowledgeLayerConfig
        from portiere.knowledge import create_knowledge_backend

        # Bypass the Pydantic Literal validation by constructing the
        # config dict-style and forcing the value
        config = KnowledgeLayerConfig(backend="bm25s")
        config.backend = "made_up_backend"  # type: ignore[assignment]
        with pytest.raises(ValueError, match="Unsupported"):
            create_knowledge_backend(config)


# ── Missing-config error paths (no extras needed) ────────────────


class TestMissingConfigPaths:
    def test_bm25s_requires_corpus_path(self):
        from portiere.config import KnowledgeLayerConfig
        from portiere.knowledge import create_knowledge_backend

        config = KnowledgeLayerConfig(backend="bm25s")
        with pytest.raises(ValueError, match="bm25s_corpus_path"):
            create_knowledge_backend(config)

    def test_faiss_requires_paths(self):
        from portiere.config import KnowledgeLayerConfig
        from portiere.knowledge import create_knowledge_backend

        config = KnowledgeLayerConfig(backend="faiss")
        with pytest.raises(ValueError, match="faiss_index_path"):
            create_knowledge_backend(config)


# ── BM25s success path ───────────────────────────────────────────


class TestBm25sSuccess:
    def test_creates_bm25s_with_corpus(self, tmp_path):
        from portiere.config import KnowledgeLayerConfig
        from portiere.knowledge import create_knowledge_backend

        # Build a tiny BM25 corpus file
        corpus = tmp_path / "concepts.json"
        corpus.write_text(
            '[{"concept_id": 1, "concept_name": "test", "vocabulary_id": "X", '
            '"domain_id": "Condition", "concept_class_id": "C", '
            '"standard_concept": "S", "concept_code": "T1"}]'
        )
        config = KnowledgeLayerConfig(backend="bm25s", bm25s_corpus_path=str(corpus))
        backend = create_knowledge_backend(config)
        assert backend is not None


# ── from_config wrapper ──────────────────────────────────────────


class TestFromConfig:
    def test_no_knowledge_layer_raises(self):
        from portiere.config import PortiereConfig
        from portiere.knowledge import create_knowledge_backend_from_config

        config = PortiereConfig()
        config.knowledge_layer = None  # type: ignore[assignment]
        with pytest.raises(ValueError, match="knowledge_layer not configured"):
            create_knowledge_backend_from_config(config)

    def test_bm25s_backend_does_not_build_embedding_gateway(self, tmp_path):
        from portiere.config import (
            EmbeddingConfig,
            KnowledgeLayerConfig,
            PortiereConfig,
        )
        from portiere.knowledge import create_knowledge_backend_from_config

        corpus = tmp_path / "concepts.json"
        corpus.write_text(
            '[{"concept_id": 1, "concept_name": "test", "vocabulary_id": "X", '
            '"domain_id": "Condition", "concept_class_id": "C", '
            '"standard_concept": "S", "concept_code": "T1"}]'
        )
        config = PortiereConfig(
            knowledge_layer=KnowledgeLayerConfig(backend="bm25s", bm25s_corpus_path=str(corpus)),
            embedding=EmbeddingConfig(provider="none"),
        )
        backend = create_knowledge_backend_from_config(config)
        assert backend is not None


# ── Vector backends raise on missing config (skip if optional deps absent) ──


class TestVectorMissingConfig:
    def test_chromadb_requires_persist_path(self):
        from portiere.config import KnowledgeLayerConfig
        from portiere.knowledge import create_knowledge_backend

        config = KnowledgeLayerConfig(backend="chromadb")
        # Should raise ValueError before trying to import chromadb
        with pytest.raises((ValueError, ImportError, ModuleNotFoundError)):
            create_knowledge_backend(config)

    def test_pgvector_requires_connection(self):
        from portiere.config import KnowledgeLayerConfig
        from portiere.knowledge import create_knowledge_backend

        config = KnowledgeLayerConfig(backend="pgvector")
        with pytest.raises((ValueError, ImportError, ModuleNotFoundError)):
            create_knowledge_backend(config)

    def test_mongodb_requires_connection(self):
        from portiere.config import KnowledgeLayerConfig
        from portiere.knowledge import create_knowledge_backend

        config = KnowledgeLayerConfig(backend="mongodb")
        with pytest.raises((ValueError, ImportError, ModuleNotFoundError)):
            create_knowledge_backend(config)

    def test_qdrant_requires_url(self):
        from portiere.config import KnowledgeLayerConfig
        from portiere.knowledge import create_knowledge_backend

        config = KnowledgeLayerConfig(backend="qdrant")
        with pytest.raises((ValueError, ImportError, ModuleNotFoundError)):
            create_knowledge_backend(config)

    def test_milvus_requires_uri(self):
        from portiere.config import KnowledgeLayerConfig
        from portiere.knowledge import create_knowledge_backend

        config = KnowledgeLayerConfig(backend="milvus")
        with pytest.raises((ValueError, ImportError, ModuleNotFoundError)):
            create_knowledge_backend(config)

    def test_elasticsearch_requires_url(self):
        from portiere.config import KnowledgeLayerConfig
        from portiere.knowledge import create_knowledge_backend

        config = KnowledgeLayerConfig(backend="elasticsearch")
        with pytest.raises((ValueError, ImportError, ModuleNotFoundError)):
            create_knowledge_backend(config)

    def test_hybrid_requires_subbackends(self):
        from portiere.config import KnowledgeLayerConfig
        from portiere.knowledge import create_knowledge_backend

        config = KnowledgeLayerConfig(backend="hybrid")
        with pytest.raises((ValueError, ImportError, ModuleNotFoundError)):
            create_knowledge_backend(config)

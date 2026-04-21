"""
Tests for knowledge layer factory, RRF fusion, and hybrid backend.

Tests:
- create_knowledge_backend / create_knowledge_backend_from_config
- reciprocal_rank_fusion
- HybridBackend
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from portiere.config import KnowledgeLayerConfig, PortiereConfig

# ── Factory Tests ─────────────────────────────────────────────────


class TestCreateKnowledgeBackend:
    def test_bm25s_backend(self):
        from portiere.knowledge.factory import create_knowledge_backend

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(
                [
                    {
                        "concept_id": 1,
                        "concept_name": "Test",
                        "vocabulary_id": "SNOMED",
                        "domain_id": "Condition",
                    }
                ],
                f,
            )
            corpus_path = Path(f.name)

        try:
            config = KnowledgeLayerConfig(
                backend="bm25s",
                bm25s_corpus_path=corpus_path,
            )
            backend = create_knowledge_backend(config)
            assert backend is not None
            assert type(backend).__name__ == "BM25sBackend"
        finally:
            corpus_path.unlink(missing_ok=True)

    def test_bm25s_missing_path_raises(self):
        from portiere.knowledge.factory import create_knowledge_backend

        config = KnowledgeLayerConfig(
            backend="bm25s",
            bm25s_corpus_path=None,
        )
        with pytest.raises(ValueError, match="bm25s_corpus_path"):
            create_knowledge_backend(config)

    def test_faiss_missing_paths_raises(self):
        from portiere.knowledge.factory import create_knowledge_backend

        config = KnowledgeLayerConfig(
            backend="faiss",
            faiss_index_path=None,
            faiss_metadata_path=None,
        )
        with pytest.raises(ValueError, match="faiss_index_path"):
            create_knowledge_backend(config)

    def test_elasticsearch_missing_url_raises(self):
        from portiere.knowledge.factory import create_knowledge_backend

        config = KnowledgeLayerConfig(
            backend="elasticsearch",
            elasticsearch_url=None,
        )
        with pytest.raises(ValueError, match="elasticsearch_url"):
            create_knowledge_backend(config)

    def test_unsupported_backend_raises(self):
        from portiere.knowledge.factory import create_knowledge_backend

        config = MagicMock()
        config.backend = "unsupported"
        with pytest.raises(ValueError, match="Unsupported"):
            create_knowledge_backend(config)

    def test_hybrid_no_backends_raises(self):
        from portiere.knowledge.factory import create_knowledge_backend

        config = KnowledgeLayerConfig(
            backend="hybrid",
            faiss_index_path=None,
            faiss_metadata_path=None,
            bm25s_corpus_path=None,
            elasticsearch_url=None,
        )
        with pytest.raises(ValueError, match="at least one"):
            create_knowledge_backend(config)

    def test_hybrid_single_backend_returns_directly(self):
        """When hybrid has only one sub-backend, return it directly."""
        from portiere.knowledge.factory import create_knowledge_backend

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(
                [
                    {
                        "concept_id": 1,
                        "concept_name": "Test",
                        "vocabulary_id": "SNOMED",
                        "domain_id": "Condition",
                    }
                ],
                f,
            )
            corpus_path = Path(f.name)

        try:
            config = KnowledgeLayerConfig(
                backend="hybrid",
                bm25s_corpus_path=corpus_path,
            )
            backend = create_knowledge_backend(config)
            # Should return BM25s directly, not HybridBackend
            assert type(backend).__name__ == "BM25sBackend"
        finally:
            corpus_path.unlink(missing_ok=True)


class TestCreateKnowledgeBackendFromConfig:
    def test_no_knowledge_layer_raises(self):
        from portiere.knowledge.factory import create_knowledge_backend_from_config

        config = PortiereConfig(mode="local", knowledge_layer=None)
        with pytest.raises(ValueError, match="knowledge_layer not configured"):
            create_knowledge_backend_from_config(config)

    def test_uses_portiere_config_embedding_model(self):
        from portiere.knowledge.factory import create_knowledge_backend_from_config

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(
                [
                    {
                        "concept_id": 1,
                        "concept_name": "Test",
                        "vocabulary_id": "SNOMED",
                        "domain_id": "Condition",
                    }
                ],
                f,
            )
            corpus_path = Path(f.name)

        try:
            config = PortiereConfig(
                mode="local",
                knowledge_layer=KnowledgeLayerConfig(
                    backend="bm25s",
                    bm25s_corpus_path=corpus_path,
                ),
                embedding_model="custom-model",
            )
            backend = create_knowledge_backend_from_config(config)
            assert backend is not None
        finally:
            corpus_path.unlink(missing_ok=True)


# ── RRF Fusion Tests ──────────────────────────────────────────────


class TestReciprocalRankFusion:
    def test_empty_lists(self):
        from portiere.knowledge.rrfusion import reciprocal_rank_fusion

        result = reciprocal_rank_fusion()
        assert result == []

    def test_all_empty_lists(self):
        from portiere.knowledge.rrfusion import reciprocal_rank_fusion

        result = reciprocal_rank_fusion([], [], [])
        assert result == []

    def test_single_list_passthrough(self):
        from portiere.knowledge.rrfusion import reciprocal_rank_fusion

        items = [
            {"concept_id": 1, "concept_name": "A", "score": 0.9},
            {"concept_id": 2, "concept_name": "B", "score": 0.7},
        ]

        result = reciprocal_rank_fusion(items)

        assert len(result) == 2
        # Single list passthrough: rrf_score = original score
        assert result[0]["rrf_score"] == 0.9
        assert result[1]["rrf_score"] == 0.7

    def test_two_lists_fusion(self):
        from portiere.knowledge.rrfusion import reciprocal_rank_fusion

        list1 = [
            {"concept_id": 1, "concept_name": "A", "score": 0.9},
            {"concept_id": 2, "concept_name": "B", "score": 0.8},
        ]
        list2 = [
            {"concept_id": 2, "concept_name": "B", "score": 0.95},
            {"concept_id": 3, "concept_name": "C", "score": 0.7},
        ]

        result = reciprocal_rank_fusion(list1, list2, k=60)

        # Item 2 appears in both lists → higher RRF score
        ids = [r["concept_id"] for r in result]
        assert 2 in ids
        assert 1 in ids
        assert 3 in ids

        # Item 2 should have highest RRF score (appears in both lists)
        item2 = next(r for r in result if r["concept_id"] == 2)
        item1 = next(r for r in result if r["concept_id"] == 1)
        assert item2["rrf_score"] > item1["rrf_score"]

    def test_rrf_score_formula(self):
        from portiere.knowledge.rrfusion import reciprocal_rank_fusion

        list1 = [{"concept_id": 1, "score": 0.9}]
        list2 = [{"concept_id": 1, "score": 0.8}]

        result = reciprocal_rank_fusion(list1, list2, k=60)

        # Item 1 is rank 0 in both lists
        # RRF = 1/(60+0+1) + 1/(60+0+1) = 2/61
        expected = round(2.0 / 61.0, 6)
        assert result[0]["rrf_score"] == expected

    def test_keeps_highest_original_score(self):
        from portiere.knowledge.rrfusion import reciprocal_rank_fusion

        list1 = [{"concept_id": 1, "concept_name": "A", "score": 0.5}]
        list2 = [{"concept_id": 1, "concept_name": "A_better", "score": 0.9}]

        result = reciprocal_rank_fusion(list1, list2)

        # Should keep the version with higher original score
        assert result[0]["score"] == 0.9

    def test_items_without_id_skipped(self):
        from portiere.knowledge.rrfusion import reciprocal_rank_fusion

        list1 = [
            {"concept_id": 1, "score": 0.9},
            {"score": 0.5},  # No concept_id
        ]

        result = reciprocal_rank_fusion(list1, list1)
        # Item without id should be skipped
        assert all(r.get("concept_id") is not None for r in result)

    def test_custom_fields(self):
        from portiere.knowledge.rrfusion import reciprocal_rank_fusion

        items = [{"my_id": "abc", "my_score": 0.9}]

        result = reciprocal_rank_fusion(items, score_field="my_score", id_field="my_id")

        assert len(result) == 1
        assert result[0]["rrf_score"] == 0.9  # single list passthrough


# ── Hybrid Backend Tests ──────────────────────────────────────────


class TestHybridBackend:
    @pytest.fixture
    def mock_backends(self):
        backend1 = MagicMock()
        backend1.search.return_value = [
            {"concept_id": 1, "concept_name": "A", "score": 0.9},
            {"concept_id": 2, "concept_name": "B", "score": 0.7},
        ]

        backend2 = MagicMock()
        backend2.search.return_value = [
            {"concept_id": 2, "concept_name": "B", "score": 0.95},
            {"concept_id": 3, "concept_name": "C", "score": 0.6},
        ]

        return [backend1, backend2]

    def test_search_fuses_results(self, mock_backends):
        from portiere.knowledge.hybrid_backend import HybridBackend

        hybrid = HybridBackend(backends=mock_backends, fusion_method="rrf", rrf_k=60)
        results = hybrid.search("test query", limit=10)

        assert len(results) == 3  # 3 unique concepts
        # Item 2 appears in both → highest score
        assert results[0]["concept_id"] == 2

    def test_search_respects_limit(self, mock_backends):
        from portiere.knowledge.hybrid_backend import HybridBackend

        hybrid = HybridBackend(backends=mock_backends, rrf_k=60)
        results = hybrid.search("test", limit=1)

        assert len(results) == 1

    def test_search_passes_filters(self, mock_backends):
        from portiere.knowledge.hybrid_backend import HybridBackend

        hybrid = HybridBackend(backends=mock_backends, rrf_k=60)
        hybrid.search("test", vocabularies=["SNOMED"], domain="Condition", limit=5)

        for backend in mock_backends:
            backend.search.assert_called_once_with(
                "test",
                vocabularies=["SNOMED"],
                domain="Condition",
                limit=15,  # limit * 3
            )

    def test_search_handles_backend_failure(self):
        from portiere.knowledge.hybrid_backend import HybridBackend

        good = MagicMock()
        good.search.return_value = [
            {"concept_id": 1, "concept_name": "A", "score": 0.9},
        ]

        bad = MagicMock()
        bad.search.side_effect = RuntimeError("Backend down")

        hybrid = HybridBackend(backends=[good, bad], rrf_k=60)
        results = hybrid.search("test")

        # Should still return results from the working backend
        assert len(results) == 1

    def test_search_all_backends_fail(self):
        from portiere.knowledge.hybrid_backend import HybridBackend

        bad1 = MagicMock()
        bad1.search.side_effect = RuntimeError("fail1")
        bad2 = MagicMock()
        bad2.search.side_effect = RuntimeError("fail2")

        hybrid = HybridBackend(backends=[bad1, bad2])
        results = hybrid.search("test")

        assert results == []

    def test_get_concept_tries_backends(self, mock_backends):
        from portiere.knowledge.hybrid_backend import HybridBackend

        mock_backends[0].get_concept.side_effect = ValueError("not found")
        mock_backends[1].get_concept.return_value = {"concept_id": 2, "concept_name": "B"}

        hybrid = HybridBackend(backends=mock_backends)
        concept = hybrid.get_concept(2)

        assert concept["concept_id"] == 2
        mock_backends[0].get_concept.assert_called_once_with(2)
        mock_backends[1].get_concept.assert_called_once_with(2)

    def test_get_concept_not_found_anywhere(self, mock_backends):
        from portiere.knowledge.hybrid_backend import HybridBackend

        for b in mock_backends:
            b.get_concept.side_effect = ValueError("not found")

        hybrid = HybridBackend(backends=mock_backends)

        with pytest.raises(ValueError, match="not found in any backend"):
            hybrid.get_concept(999)

    def test_index_concepts_all_backends(self, mock_backends):
        from portiere.knowledge.hybrid_backend import HybridBackend

        hybrid = HybridBackend(backends=mock_backends)
        concepts = [{"concept_id": 1}]
        hybrid.index_concepts(concepts)

        for b in mock_backends:
            b.index_concepts.assert_called_once_with(concepts)

    def test_batch_search(self, mock_backends):
        from portiere.knowledge.hybrid_backend import HybridBackend

        hybrid = HybridBackend(backends=mock_backends, rrf_k=60)
        results = hybrid.batch_search(["query1", "query2"])

        assert len(results) == 2
        assert all(isinstance(r, list) for r in results)

    def test_rrf_score_used_as_primary_score(self, mock_backends):
        from portiere.knowledge.hybrid_backend import HybridBackend

        hybrid = HybridBackend(backends=mock_backends, rrf_k=60)
        results = hybrid.search("test")

        for r in results:
            assert "score" in r
            assert "rrf_score" in r
            assert r["score"] == r["rrf_score"]

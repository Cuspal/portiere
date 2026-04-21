"""
Tests for local cross-encoder reranker.

Tests the LocalReranker component used for reranking concept/schema
mapping candidates with a cross-encoder model.
"""

import math
from unittest.mock import MagicMock, patch

import pytest


class TestLocalRerankerInit:
    """Tests for LocalReranker initialization."""

    def test_default_model_name(self):
        from portiere.local.reranker import LocalReranker

        reranker = LocalReranker()
        assert reranker.model_name == "cross-encoder/ms-marco-MiniLM-L-6-v2"

    def test_custom_model_name(self):
        from portiere.local.reranker import LocalReranker

        reranker = LocalReranker(model_name="custom/model")
        assert reranker.model_name == "custom/model"

    def test_lazy_loading(self):
        from portiere.local.reranker import LocalReranker

        reranker = LocalReranker()
        assert reranker._model is None
        assert reranker._loaded is False

    @patch("portiere.local.reranker.CrossEncoder", create=True)
    def test_load_model_success(self, mock_ce_cls):
        """Test that model is loaded lazily on first access."""
        from portiere.local.reranker import LocalReranker

        mock_model = MagicMock()
        mock_ce_cls.return_value = mock_model

        with patch.dict(
            "sys.modules",
            {"sentence_transformers": MagicMock(CrossEncoder=mock_ce_cls)},
        ):
            reranker = LocalReranker()
            reranker._load_model()

        assert reranker._loaded is True

    def test_unavailable_when_import_fails(self):
        """Test reranker gracefully handles missing sentence-transformers."""
        import builtins

        from portiere.local.reranker import LocalReranker

        reranker = LocalReranker()
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if "sentence_transformers" in name:
                raise ImportError("no module named 'sentence_transformers'")
            return real_import(name, *args, **kwargs)

        with patch.object(builtins, "__import__", side_effect=mock_import):
            # Force a fresh load attempt
            reranker._loaded = False
            reranker._model = None
            reranker._load_model()

        assert reranker._loaded is True
        assert reranker._model is None
        assert reranker.available is False


class TestLocalRerankerRerank:
    """Tests for rerank method."""

    @pytest.fixture
    def reranker_with_mock_model(self):
        from portiere.local.reranker import LocalReranker

        reranker = LocalReranker()
        mock_model = MagicMock()
        reranker._model = mock_model
        reranker._loaded = True
        return reranker, mock_model

    def test_rerank_empty_candidates(self, reranker_with_mock_model):
        reranker, _ = reranker_with_mock_model
        result = reranker.rerank("diabetes", [])
        assert result == []

    def test_rerank_returns_scored_candidates(self, reranker_with_mock_model):
        reranker, mock_model = reranker_with_mock_model

        candidates = [
            {"concept_name": "Type 2 diabetes", "concept_id": 1},
            {"concept_name": "Type 1 diabetes", "concept_id": 2},
            {"concept_name": "Hypertension", "concept_id": 3},
        ]
        # CE scores: 2nd candidate scores highest
        mock_model.predict.return_value = [0.5, 0.9, 0.1]

        result = reranker.rerank("diabetes", candidates, top_k=3)

        assert len(result) == 3
        assert result[0]["concept_id"] == 2  # Highest CE score
        assert result[1]["concept_id"] == 1
        assert result[2]["concept_id"] == 3
        assert "cross_encoder_score" in result[0]

    def test_rerank_respects_top_k(self, reranker_with_mock_model):
        reranker, mock_model = reranker_with_mock_model

        candidates = [{"concept_name": f"Concept {i}", "concept_id": i} for i in range(5)]
        mock_model.predict.return_value = [0.1, 0.9, 0.5, 0.3, 0.7]

        result = reranker.rerank("query", candidates, top_k=2)
        assert len(result) == 2

    def test_rerank_custom_text_field(self, reranker_with_mock_model):
        reranker, mock_model = reranker_with_mock_model

        candidates = [
            {"display_name": "Diabetes Type 2", "concept_id": 1},
        ]
        mock_model.predict.return_value = [0.8]

        result = reranker.rerank("diabetes", candidates, text_field="display_name")

        # Verify the model was called with the correct text field
        call_args = mock_model.predict.call_args[0][0]
        assert call_args[0] == ("diabetes", "Diabetes Type 2")

    def test_rerank_no_model_returns_truncated(self):
        """When model isn't available, return original candidates truncated."""
        from portiere.local.reranker import LocalReranker

        reranker = LocalReranker()
        reranker._loaded = True
        reranker._model = None

        candidates = [{"concept_name": f"C{i}", "concept_id": i} for i in range(5)]
        result = reranker.rerank("query", candidates, top_k=3)

        assert len(result) == 3
        assert result[0]["concept_id"] == 0  # Original order preserved

    def test_rerank_does_not_mutate_input(self, reranker_with_mock_model):
        reranker, mock_model = reranker_with_mock_model

        candidates = [
            {"concept_name": "Original", "concept_id": 1},
        ]
        mock_model.predict.return_value = [0.8]

        result = reranker.rerank("query", candidates)

        # Original should not have cross_encoder_score
        assert "cross_encoder_score" not in candidates[0]
        assert "cross_encoder_score" in result[0]


class TestLocalRerankerBlending:
    """Tests for rerank_with_blending method."""

    @pytest.fixture
    def reranker_with_mock_model(self):
        from portiere.local.reranker import LocalReranker

        reranker = LocalReranker()
        mock_model = MagicMock()
        reranker._model = mock_model
        reranker._loaded = True
        return reranker, mock_model

    def test_blending_formula(self, reranker_with_mock_model):
        """Test 60% CE + 40% retrieval blending."""
        reranker, mock_model = reranker_with_mock_model

        candidates = [
            {"concept_name": "A", "concept_id": 1, "score": 0.8},
            {"concept_name": "B", "concept_id": 2, "score": 0.6},
        ]
        # CE raw scores (logits)
        mock_model.predict.return_value = [2.0, -1.0]

        result = reranker.rerank_with_blending("query", candidates, top_k=2)

        # Verify blending formula for first result
        for r in result:
            ce_raw = r["cross_encoder_score"]
            ce_norm = 1.0 / (1.0 + math.exp(-ce_raw))
            retrieval = candidates[0]["score"] if r["concept_id"] == 1 else candidates[1]["score"]
            expected = round(0.6 * ce_norm + 0.4 * retrieval, 4)
            assert r["score"] == expected

    def test_blending_uses_rrf_score_if_present(self, reranker_with_mock_model):
        reranker, mock_model = reranker_with_mock_model

        candidates = [
            {"concept_name": "A", "concept_id": 1, "score": 0.5, "rrf_score": 0.9},
        ]
        mock_model.predict.return_value = [0.0]

        result = reranker.rerank_with_blending("query", candidates)

        # Should use rrf_score (0.9) not score (0.5) for retrieval component
        ce_norm = 1.0 / (1.0 + math.exp(0))  # sigmoid(0) = 0.5
        expected = round(0.6 * ce_norm + 0.4 * 0.9, 4)
        assert result[0]["score"] == expected

    def test_blending_sorted_by_blended_score(self, reranker_with_mock_model):
        reranker, mock_model = reranker_with_mock_model

        candidates = [
            {"concept_name": "Low CE, high retrieval", "concept_id": 1, "score": 0.95},
            {"concept_name": "High CE, low retrieval", "concept_id": 2, "score": 0.1},
        ]
        # 2nd candidate has much higher CE score
        mock_model.predict.return_value = [-2.0, 5.0]

        result = reranker.rerank_with_blending("query", candidates)

        # Sorted by blended score
        assert result[0]["score"] >= result[1]["score"]


class TestLocalRerankerScorePair:
    """Tests for score_pair method."""

    def test_score_pair_returns_sigmoid_normalized(self):
        from portiere.local.reranker import LocalReranker

        reranker = LocalReranker()
        mock_model = MagicMock()
        mock_model.predict.return_value = [0.0]  # sigmoid(0) = 0.5
        reranker._model = mock_model
        reranker._loaded = True

        score = reranker.score_pair("patient_id", "person table: unique identifier")
        assert score == pytest.approx(0.5, abs=0.01)

    def test_score_pair_with_context(self):
        from portiere.local.reranker import LocalReranker

        reranker = LocalReranker()
        mock_model = MagicMock()
        mock_model.predict.return_value = [2.0]
        reranker._model = mock_model
        reranker._loaded = True

        score = reranker.score_pair("col", "target", context="schema mapping")

        call_args = mock_model.predict.call_args[0][0]
        assert call_args[0] == ("col [schema mapping]", "target")

    def test_score_pair_no_model_returns_default(self):
        from portiere.local.reranker import LocalReranker

        reranker = LocalReranker()
        reranker._loaded = True
        reranker._model = None

        score = reranker.score_pair("source", "target")
        assert score == 0.5

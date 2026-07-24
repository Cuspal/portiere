"""Regression tests for score-scale handling in rerank_with_blending.

Historical defect (pre-v0.4.0): the CE/retrieval blend used the *raw*
retrieval score. BM25 scores are unbounded (5-30+), FAISS cosine is 0-1, and
hybrid RRF scores max out around 2/(k+1) ~= 0.033 — so the advertised
60/40 blend was effectively retrieval-only for bm25s and CE-only for hybrid.
The three published benchmark backends were therefore measured under three
different blending regimes. Blending must be scale-invariant.
"""

import pytest

from portiere.local.reranker import LocalReranker


def _mk(concept_id: int, retrieval_score: float, ce_logit: float) -> dict:
    return {
        "concept_id": concept_id,
        "concept_name": f"concept {concept_id}",
        "score": retrieval_score,
        "cross_encoder_score": ce_logit,
    }


@pytest.fixture
def reranker(monkeypatch):
    """A LocalReranker whose CE pass-through is mocked (no model download)."""
    rr = LocalReranker.__new__(LocalReranker)  # bypass __init__ model config

    def fake_rerank(self, query, candidates, top_k=10, text_field="concept_name"):
        # CE scores are already injected by the test; just pass through.
        return list(candidates)

    monkeypatch.setattr(LocalReranker, "rerank", fake_rerank)
    return rr


class TestScaleInvariance:
    def test_bm25_scale_and_rrf_scale_produce_same_ranking(self, reranker):
        """Identical relative retrieval + identical CE ⇒ identical ranking,
        regardless of the retrieval score scale."""
        ce = [0.5, 0.2, -0.1]
        bm25_scale = [
            _mk(1, 30.0, ce[0]),
            _mk(2, 20.0, ce[1]),
            _mk(3, 10.0, ce[2]),
        ]
        rrf_scale = [
            _mk(1, 0.0328, ce[0]),
            _mk(2, 0.0246, ce[1]),
            _mk(3, 0.0164, ce[2]),
        ]
        out_a = reranker.rerank_with_blending("q", bm25_scale)
        out_b = reranker.rerank_with_blending("q", rrf_scale)
        assert [r["concept_id"] for r in out_a] == [r["concept_id"] for r in out_b]
        # Blended scores must be on the same scale too (0-1 blend space)
        for ra, rb in zip(out_a, out_b):
            assert ra["score"] == pytest.approx(rb["score"], abs=1e-6)

    def test_rrf_retrieval_opinion_survives_blending(self, reranker):
        """Regression: with raw RRF scores (~0.03), the 40% retrieval term was
        numerically negligible and a mild CE preference always won. After
        normalization, a strong retrieval preference outweighs a mild CE one."""
        candidates = [
            _mk(1, 0.0328, 0.0),  # retrieval's clear favourite; CE neutral
            _mk(2, 0.0164, 0.4),  # retrieval's clear loser; CE mildly prefers
        ]
        out = reranker.rerank_with_blending("q", candidates)
        assert out[0]["concept_id"] == 1

    def test_blended_scores_bounded_zero_one(self, reranker):
        candidates = [_mk(1, 25.0, 3.0), _mk(2, 5.0, -3.0)]
        out = reranker.rerank_with_blending("q", candidates)
        for r in out:
            assert 0.0 <= r["score"] <= 1.0

    def test_equal_retrieval_scores_let_ce_decide(self, reranker):
        candidates = [_mk(1, 0.5, -1.0), _mk(2, 0.5, 1.0)]
        out = reranker.rerank_with_blending("q", candidates)
        assert out[0]["concept_id"] == 2


def test_empty_candidates_returns_empty(reranker):
    """Regression: min()/max() on an empty list raised ValueError."""
    assert reranker.rerank_with_blending("q", []) == []

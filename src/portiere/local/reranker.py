"""
Local Cross-Encoder Reranker — High-precision reranking for local mode.

Supports multiple providers:
- huggingface: Local cross-encoder via sentence-transformers (default)
- none: Reranking disabled

Replicates the API-side reranker for local mode.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from portiere.config import RerankerConfig

logger = structlog.get_logger(__name__)


class LocalReranker:
    """
    Cross-encoder reranker for local concept/schema mapping.

    Loads a cross-encoder model and scores (query, candidate) pairs
    jointly for higher accuracy than bi-encoder similarity.
    """

    DEFAULT_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"

    def __init__(
        self,
        model_name: str | None = None,
        *,
        reranker_config: RerankerConfig | None = None,
    ):
        if reranker_config is not None:
            self._provider = reranker_config.provider
            self.model_name = reranker_config.model
        else:
            self._provider = "huggingface"
            self.model_name = model_name or self.DEFAULT_MODEL
        self._model = None
        self._loaded = False

    def _load_model(self):
        """Lazy load reranker model based on provider."""
        if self._loaded:
            return

        if self._provider == "none":
            self._loaded = True
            return

        # Default: huggingface cross-encoder
        try:
            from sentence_transformers import CrossEncoder

            logger.info("reranker.loading", model=self.model_name)
            self._model = CrossEncoder(self.model_name)
            self._loaded = True
            logger.info("reranker.loaded", provider="huggingface")
        except ImportError:
            logger.warning(
                "reranker.unavailable",
                message="sentence-transformers not installed. Reranking disabled.",
            )
            self._loaded = True  # Don't retry

    @property
    def available(self) -> bool:
        """Check if reranker model is available."""
        self._load_model()
        if self._provider == "none":
            return False
        return self._model is not None

    def rerank(
        self,
        query: str,
        candidates: list[dict],
        top_k: int = 10,
        text_field: str = "concept_name",
    ) -> list[dict]:
        """
        Rerank candidates using cross-encoder.

        Args:
            query: Source term / search query
            candidates: Retrieved candidates to rerank
            top_k: Number of top results to return
            text_field: Field in candidate dict containing text to compare

        Returns:
            Reranked candidates with 'cross_encoder_score' added
        """
        if not candidates:
            return []

        self._load_model()

        if self._model is None:
            return candidates[:top_k]

        # Create (query, candidate_text) pairs
        pairs = [(query, c.get(text_field, "")) for c in candidates]

        # Score pairs
        scores = self._model.predict(pairs)

        # Attach scores
        scored = []
        for candidate, score in zip(candidates, scores):
            reranked = candidate.copy()
            reranked["cross_encoder_score"] = float(score)
            scored.append(reranked)

        # Sort by cross-encoder score (descending)
        scored.sort(key=lambda x: x["cross_encoder_score"], reverse=True)

        return scored[:top_k]

    def rerank_with_blending(
        self,
        query: str,
        candidates: list[dict],
        top_k: int = 10,
        text_field: str = "concept_name",
        ce_weight: float = 0.6,
        retrieval_weight: float = 0.4,
    ) -> list[dict]:
        """
        Rerank and blend cross-encoder scores with retrieval scores.

        Same blending strategy as the server: 60% CE + 40% retrieval.

        Args:
            query: Source term
            candidates: Retrieved candidates
            top_k: Max results
            text_field: Candidate text field
            ce_weight: Weight for cross-encoder score (default 0.6)
            retrieval_weight: Weight for original retrieval score (default 0.4)

        Returns:
            Reranked candidates with blended 'score'
        """
        reranked = self.rerank(query, candidates, top_k=len(candidates), text_field=text_field)

        for r in reranked:
            ce_raw = r.get("cross_encoder_score", 0)
            retrieval_score = r.get("rrf_score", r.get("score", 0))
            # Sigmoid normalize CE score (raw logits can be negative/unbounded)
            ce_norm = 1.0 / (1.0 + math.exp(-ce_raw))
            # Blend
            r["score"] = round(ce_weight * ce_norm + retrieval_weight * retrieval_score, 4)

        # Re-sort by blended score
        reranked.sort(key=lambda x: x["score"], reverse=True)
        return reranked[:top_k]

    def score_pair(
        self,
        source_term: str,
        target_text: str,
        context: str | None = None,
    ) -> float:
        """
        Score a single (source, target) pair.

        Used for schema mapping reranking where we need individual scores.

        Args:
            source_term: Source column/term
            target_text: Target description
            context: Optional context appended to source

        Returns:
            Sigmoid-normalized score (0-1)
        """
        self._load_model()

        query = f"{source_term} [{context}]" if context else source_term

        if self._model is None:
            return 0.5

        score = self._model.predict([(query, target_text)])[0]

        # Sigmoid normalize
        return 1.0 / (1.0 + math.exp(-float(score)))

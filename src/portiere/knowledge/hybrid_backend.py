"""
Hybrid Backend — Combines multiple knowledge backends with result fusion.

Supports RRF (Reciprocal Rank Fusion) and weighted combination of results
from BM25s, FAISS, and Elasticsearch backends.
"""

from __future__ import annotations

from typing import Literal

import structlog

from portiere.knowledge.base import KnowledgeLayerBackend
from portiere.knowledge.rrfusion import reciprocal_rank_fusion

logger = structlog.get_logger(__name__)


class HybridBackend(KnowledgeLayerBackend):
    """
    Hybrid backend combining multiple search backends with RRF fusion.

    Best for: Maximum accuracy by combining lexical (BM25) and
    semantic (FAISS/embedding) search results.
    """

    def __init__(
        self,
        backends: list[KnowledgeLayerBackend],
        fusion_method: Literal["rrf", "weighted"] = "rrf",
        rrf_k: int = 60,
    ):
        self.backends = backends
        self.fusion_method = fusion_method
        self.rrf_k = rrf_k

    def search(
        self,
        query: str,
        vocabularies: list[str] | None = None,
        domain: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """Search all backends and fuse results."""
        # Collect results from all backends (over-fetch for fusion)
        all_results = []
        for backend in self.backends:
            try:
                results = backend.search(
                    query,
                    vocabularies=vocabularies,
                    domain=domain,
                    limit=limit * 3,
                )
                all_results.append(results)
            except Exception as e:
                logger.warning(
                    "hybrid.backend_failed",
                    backend=type(backend).__name__,
                    error=str(e),
                )

        if not all_results:
            return []

        # Fuse results
        fused = reciprocal_rank_fusion(
            *all_results,
            k=self.rrf_k,
            score_field="score",
            id_field="concept_id",
        )

        # Use rrf_score as the primary score
        for item in fused:
            item["score"] = item.get("rrf_score", item.get("score", 0))

        return fused[:limit]

    def get_concept(self, concept_id: int) -> dict:
        """Try each backend until one finds the concept."""
        for backend in self.backends:
            try:
                return backend.get_concept(concept_id)
            except (ValueError, RuntimeError):
                continue
        raise ValueError(f"Concept {concept_id} not found in any backend")

    def index_concepts(self, concepts: list[dict]) -> None:
        """Index concepts in all backends."""
        for backend in self.backends:
            try:
                backend.index_concepts(concepts)
            except Exception as e:
                logger.warning(
                    "hybrid.index_failed",
                    backend=type(backend).__name__,
                    error=str(e),
                )

    def batch_search(
        self,
        queries: list[str],
        vocabularies: list[str] | None = None,
        domain: str | None = None,
        limit: int = 10,
    ) -> list[list[dict]]:
        """Batch search with fusion per query."""
        return [
            self.search(query, vocabularies=vocabularies, domain=domain, limit=limit)
            for query in queries
        ]

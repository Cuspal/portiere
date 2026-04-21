"""
BM25s Backend — Pure Python BM25 search for concept lookup.

Uses the bm25s library for efficient BM25 ranking with optional stemming.
No external service dependencies — works completely offline.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import structlog

from portiere.knowledge.base import KnowledgeLayerBackend

logger = structlog.get_logger(__name__)


class BM25sBackend(KnowledgeLayerBackend):
    """
    Pure Python BM25 backend using bm25s library.

    Best for: Quick setup, offline use, small-to-medium vocabularies (<1M concepts).
    No external dependencies beyond bm25s and PyStemmer.
    """

    def __init__(self, corpus_path: Path, use_stemming: bool = True):
        """
        Initialize BM25s backend.

        Args:
            corpus_path: Path to JSON file containing concept corpus
            use_stemming: Whether to use English stemming (default: True)
        """
        self.corpus_path = Path(corpus_path)
        self.use_stemming = use_stemming
        self.concepts: list[dict] = []
        self.retriever: Any = None
        self._concept_id_index: dict[int, dict] = {}

        if self.corpus_path.exists():
            self._load_corpus()
        else:
            logger.warning(
                "bm25s.corpus_not_found",
                path=str(self.corpus_path),
                message="Corpus file not found. Call index_concepts() to create it.",
            )

    def _get_stemmer(self):
        """Get stemmer instance if stemming is enabled."""
        if self.use_stemming:
            try:
                import Stemmer

                return Stemmer.Stemmer("english")
            except ImportError:
                logger.warning(
                    "bm25s.stemmer_unavailable",
                    message="PyStemmer not installed. Install with: pip install PyStemmer",
                )
        return None

    def _load_corpus(self) -> None:
        """Load concept vocabulary and build BM25 index."""
        import bm25s

        with open(self.corpus_path) as f:
            self.concepts = json.load(f)

        # Build concept_id index for fast lookups
        self._concept_id_index = {c["concept_id"]: c for c in self.concepts}

        # Extract concept names for indexing
        corpus = [c["concept_name"] for c in self.concepts]

        # Tokenize with optional stemming
        stemmer = self._get_stemmer()
        corpus_tokens = bm25s.tokenize(corpus, stemmer=stemmer)

        # Build BM25 index
        self.retriever = bm25s.BM25()
        self.retriever.index(corpus_tokens)

        logger.info(
            "bm25s.corpus_loaded",
            concepts_count=len(self.concepts),
            path=str(self.corpus_path),
        )

    def search(
        self,
        query: str,
        vocabularies: list[str] | None = None,
        domain: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """BM25 search with optional vocabulary and domain filtering."""
        import bm25s

        if self.retriever is None:
            raise RuntimeError(
                "BM25s index not loaded. Ensure corpus file exists or call index_concepts()."
            )

        stemmer = self._get_stemmer()
        query_tokens = bm25s.tokenize([query], stemmer=stemmer)

        # Over-fetch to account for filtering
        fetch_k = min(limit * 3, len(self.concepts))
        results, scores = self.retriever.retrieve(query_tokens, k=fetch_k)

        # Format and filter results
        matches = []
        for idx, score in zip(results[0], scores[0]):
            if score <= 0:
                continue

            concept = self.concepts[idx]

            # Apply filters
            if vocabularies and concept.get("vocabulary_id") not in vocabularies:
                continue
            if domain and concept.get("domain_id") != domain:
                continue

            matches.append(
                {
                    "concept_id": concept["concept_id"],
                    "concept_name": concept["concept_name"],
                    "vocabulary_id": concept.get("vocabulary_id", ""),
                    "domain_id": concept.get("domain_id", ""),
                    "concept_class_id": concept.get("concept_class_id", ""),
                    "standard_concept": concept.get("standard_concept", ""),
                    "score": float(score),
                }
            )

            if len(matches) >= limit:
                break

        return matches

    def get_concept(self, concept_id: int) -> dict:
        """Lookup by concept_id."""
        concept = self._concept_id_index.get(concept_id)
        if concept is None:
            raise ValueError(f"Concept {concept_id} not found")
        return concept

    def index_concepts(self, concepts: list[dict]) -> None:
        """Save concepts to corpus file and rebuild index."""
        self.corpus_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.corpus_path, "w") as f:
            json.dump(concepts, f, indent=2)

        logger.info(
            "bm25s.concepts_indexed",
            count=len(concepts),
            path=str(self.corpus_path),
        )

        self._load_corpus()

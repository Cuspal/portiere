"""
Local FAISS Backend — Vector search for concept lookup.

Uses FAISS for efficient similarity search with sentence-transformer embeddings.
Requires faiss-cpu (or faiss-gpu) and sentence-transformers packages.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import structlog

from portiere.knowledge.base import KnowledgeLayerBackend

logger = structlog.get_logger(__name__)


class LocalFAISSBackend(KnowledgeLayerBackend):
    """
    Local FAISS backend for vector search.

    Best for: High accuracy semantic matching, large vocabularies.
    Requires: faiss-cpu, sentence-transformers.
    """

    def __init__(
        self,
        index_path: Path,
        metadata_path: Path,
        embedding_model: str = "cambridgeltl/SapBERT-from-PubMedBERT-fulltext",
        *,
        embedding_gateway=None,
    ):
        """
        Initialize FAISS backend.

        Args:
            index_path: Path to FAISS index file
            metadata_path: Path to JSON metadata file (concept details)
            embedding_model: Sentence-transformer model name or path (legacy)
            embedding_gateway: EmbeddingGateway instance (preferred, overrides embedding_model)
        """
        self.index_path = Path(index_path)
        self.metadata_path = Path(metadata_path)
        self.embedding_model_name = embedding_model
        self._embedding_gateway = embedding_gateway
        self.index: Any = None
        self.metadata: dict[str, dict] = {}
        self._concept_id_index: dict[int, dict] = {}
        self._model = None

        if self.index_path.exists() and self.metadata_path.exists():
            self._load_index()
        else:
            logger.warning(
                "faiss.index_not_found",
                index_path=str(self.index_path),
                metadata_path=str(self.metadata_path),
                message="Index files not found. Call index_concepts() to create them.",
            )

    def _get_model(self):
        """Return embedding model — prefer gateway, fall back to direct SentenceTransformer."""
        if self._embedding_gateway is not None:
            return self._embedding_gateway
        if self._model is None:
            from sentence_transformers import SentenceTransformer

            logger.info("faiss.loading_model", model=self.embedding_model_name)
            self._model = SentenceTransformer(self.embedding_model_name)
        return self._model

    def _load_index(self) -> None:
        """Load FAISS index and metadata from disk."""
        import faiss

        self.index = faiss.read_index(str(self.index_path))

        with open(self.metadata_path) as f:
            self.metadata = json.load(f)

        # Build concept_id index for fast lookups
        self._concept_id_index = {c["concept_id"]: c for c in self.metadata.values()}

        logger.info(
            "faiss.index_loaded",
            concepts_count=len(self.metadata),
            index_size=self.index.ntotal,
        )

    def search(
        self,
        query: str,
        vocabularies: list[str] | None = None,
        domain: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """Vector search with optional filtering."""
        if self.index is None:
            raise RuntimeError(
                "FAISS index not loaded. Ensure index files exist or call index_concepts()."
            )

        model = self._get_model()

        # Encode query
        query_embedding = model.encode([query], convert_to_numpy=True)
        query_embedding = query_embedding / np.linalg.norm(query_embedding, axis=1, keepdims=True)

        # Search (over-fetch to account for filtering)
        fetch_k = min(limit * 3, self.index.ntotal)
        distances, indices = self.index.search(query_embedding.astype("float32"), fetch_k)

        # Format and filter results
        matches = []
        for idx, distance in zip(indices[0], distances[0]):
            if idx == -1:
                continue

            concept = self.metadata.get(str(idx))
            if concept is None:
                continue

            # Apply filters
            if vocabularies and concept.get("vocabulary_id") not in vocabularies:
                continue
            if domain and concept.get("domain_id") != domain:
                continue

            # IndexFlatIP with normalized vectors: distance = cosine similarity
            # Range: -1.0 to 1.0 (1.0 = identical). Clamp negatives to 0.
            score = max(0.0, float(distance))

            matches.append(
                {
                    "concept_id": concept["concept_id"],
                    "concept_name": concept["concept_name"],
                    "vocabulary_id": concept.get("vocabulary_id", ""),
                    "domain_id": concept.get("domain_id", ""),
                    "concept_class_id": concept.get("concept_class_id", ""),
                    "standard_concept": concept.get("standard_concept", ""),
                    "score": score,
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
        """Build FAISS index from concept list."""
        import faiss

        model = self._get_model()

        # Encode all concept names
        concept_names = [c["concept_name"] for c in concepts]
        logger.info("faiss.encoding_concepts", count=len(concept_names))
        embeddings = model.encode(concept_names, show_progress_bar=True)
        embeddings = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)

        # Build index (using inner product for cosine similarity on normalized vectors)
        dimension = embeddings.shape[1]
        index = faiss.IndexFlatIP(dimension)
        index.add(embeddings.astype("float32"))

        # Save index
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        faiss.write_index(index, str(self.index_path))

        # Save metadata
        metadata = {str(i): c for i, c in enumerate(concepts)}
        with open(self.metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        logger.info(
            "faiss.concepts_indexed",
            count=len(concepts),
            dimension=dimension,
            index_path=str(self.index_path),
        )

        # Reload
        self._load_index()

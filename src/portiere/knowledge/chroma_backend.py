"""
ChromaDB Backend — Vector search using ChromaDB.

Uses ChromaDB for persistent or in-memory vector search with
built-in embedding support or external EmbeddingGateway.
Requires: ``pip install portiere[chromadb]``
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import structlog

from portiere.knowledge.base import KnowledgeLayerBackend

logger = structlog.get_logger(__name__)


class ChromaDBBackend(KnowledgeLayerBackend):
    """
    ChromaDB backend for vector search.

    Best for: Easy setup, persistent local storage, built-in embedding.
    Requires: chromadb
    """

    def __init__(
        self,
        collection_name: str = "portiere_concepts",
        persist_path: Path | None = None,
        *,
        embedding_gateway=None,
    ):
        try:
            import chromadb

            self._chromadb = chromadb
        except ImportError:
            raise ImportError(
                "chromadb is required for ChromaDB backend. "
                "Install with: pip install portiere[chromadb]"
            )

        self._embedding_gateway = embedding_gateway
        self._collection_name = collection_name
        self._persist_path: Path | None = None

        # Create client (persistent or in-memory)
        if persist_path:
            self._persist_path = Path(persist_path)
            self._persist_path.mkdir(parents=True, exist_ok=True)
            self._client = chromadb.PersistentClient(path=str(self._persist_path))
        else:
            self._client = chromadb.Client()

        # Get or create collection (no default embedding function — we manage embeddings)
        self._collection = self._client.get_or_create_collection(
            name=collection_name,
            metadata={"hnsw:space": "cosine"},
        )

        # Build concept_id index for fast lookups
        self._concept_id_index: dict[int, dict] = {}
        self._load_concept_index()

        logger.info(
            "chromadb.initialized",
            collection=collection_name,
            persist=persist_path is not None,
            count=self._collection.count(),
        )

    def _load_concept_index(self) -> None:
        """Build in-memory concept_id → metadata index from collection."""
        if self._collection.count() == 0:
            return
        result = self._collection.get(include=["metadatas"])
        for meta in result.get("metadatas", []):
            if meta and "concept_id" in meta:
                self._concept_id_index[int(meta["concept_id"])] = meta

    def _embed(self, texts: list[str]) -> list[list[float]]:
        """Embed texts using the gateway."""
        if self._embedding_gateway is None:
            raise RuntimeError(
                "ChromaDB backend requires an embedding_gateway for vector search. "
                "Pass embedding_gateway= to the constructor."
            )
        embeddings = self._embedding_gateway.encode(texts, convert_to_numpy=True)
        # Normalize for cosine similarity
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        norms[norms == 0] = 1
        normalized = embeddings / norms
        return normalized.tolist()

    def search(
        self,
        query: str,
        vocabularies: list[str] | None = None,
        domain: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """Vector search with optional filtering."""
        if self._collection.count() == 0:
            return []

        # Build where filter
        where_filter = None
        conditions: list[dict[str, Any]] = []
        if vocabularies:
            conditions.append({"vocabulary_id": {"$in": vocabularies}})
        if domain:
            conditions.append({"domain_id": domain})
        if len(conditions) == 1:
            where_filter = conditions[0]
        elif len(conditions) > 1:
            where_filter = {"$and": conditions}

        query_embedding = self._embed([query])

        results = self._collection.query(
            query_embeddings=query_embedding,
            n_results=min(limit, self._collection.count()),
            where=where_filter,
            include=["metadatas", "distances"],
        )

        matches = []
        for meta, distance in zip(
            results.get("metadatas", [[]])[0],
            results.get("distances", [[]])[0],
        ):
            # ChromaDB cosine distance: 0 = identical, 2 = opposite
            # Convert to similarity: 1 - (distance / 2)
            score = max(0.0, 1.0 - (distance / 2.0))
            matches.append(
                {
                    "concept_id": int(meta.get("concept_id", 0)),
                    "concept_name": meta.get("concept_name", ""),
                    "vocabulary_id": meta.get("vocabulary_id", ""),
                    "domain_id": meta.get("domain_id", ""),
                    "concept_class_id": meta.get("concept_class_id", ""),
                    "standard_concept": meta.get("standard_concept", ""),
                    "score": score,
                }
            )

        return matches

    def get_concept(self, concept_id: int) -> dict:
        """Lookup by concept_id."""
        concept = self._concept_id_index.get(concept_id)
        if concept is None:
            raise ValueError(f"Concept {concept_id} not found")
        return concept

    def index_concepts(self, concepts: list[dict]) -> None:
        """Bulk index concepts into ChromaDB."""
        if not concepts:
            return

        # Embed all concept names
        names = [c["concept_name"] for c in concepts]
        logger.info("chromadb.encoding_concepts", count=len(names))
        embeddings = self._embed(names)

        # Prepare batch data
        ids = [str(c["concept_id"]) for c in concepts]
        metadatas = [
            {
                "concept_id": c["concept_id"],
                "concept_name": c["concept_name"],
                "vocabulary_id": c.get("vocabulary_id", ""),
                "domain_id": c.get("domain_id", ""),
                "concept_class_id": c.get("concept_class_id", ""),
                "standard_concept": c.get("standard_concept", ""),
            }
            for c in concepts
        ]

        # ChromaDB has a batch limit, upsert in chunks
        batch_size = 5000
        for i in range(0, len(ids), batch_size):
            self._collection.upsert(
                ids=ids[i : i + batch_size],
                embeddings=embeddings[i : i + batch_size],
                metadatas=metadatas[i : i + batch_size],
            )

        # Rebuild concept_id index
        self._concept_id_index = {c["concept_id"]: m for c, m in zip(concepts, metadatas)}

        logger.info(
            "chromadb.concepts_indexed",
            count=len(concepts),
            collection=self._collection_name,
        )

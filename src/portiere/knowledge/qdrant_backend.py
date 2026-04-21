"""
Qdrant Backend — Vector search using Qdrant vector database.

Uses Qdrant for high-performance vector similarity search with
filtering and payload storage.
Requires: ``pip install portiere-health[qdrant]``
"""

from __future__ import annotations

import numpy as np
import structlog

from portiere.knowledge.base import KnowledgeLayerBackend

logger = structlog.get_logger(__name__)


class QdrantBackend(KnowledgeLayerBackend):
    """
    Qdrant backend for vector search.

    Best for: High-performance vector search, production deployments.
    Requires: qdrant-client
    """

    def __init__(
        self,
        url: str | None = None,
        collection_name: str = "portiere_concepts",
        *,
        api_key: str | None = None,
        embedding_gateway=None,
    ):
        try:
            from qdrant_client import QdrantClient
            from qdrant_client import models as qdrant_models

            self._QdrantClient = QdrantClient
            self._models = qdrant_models
        except ImportError:
            raise ImportError(
                "qdrant-client is required for Qdrant backend. "
                "Install with: pip install portiere-health[qdrant]"
            )

        self._embedding_gateway = embedding_gateway
        self._collection_name = collection_name

        # Connect to Qdrant (in-memory if no URL)
        if url:
            self._client = QdrantClient(url=url, api_key=api_key)
        else:
            self._client = QdrantClient(":memory:")

        logger.info("qdrant.initialized", url=url or ":memory:", collection=collection_name)

    def _embed(self, texts: list[str]) -> np.ndarray:
        """Embed texts using the gateway."""
        if self._embedding_gateway is None:
            raise RuntimeError(
                "Qdrant backend requires an embedding_gateway. "
                "Pass embedding_gateway= to the constructor."
            )
        embeddings = self._embedding_gateway.encode(texts, convert_to_numpy=True)
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        norms[norms == 0] = 1
        return embeddings / norms

    def _ensure_collection(self, dimension: int) -> None:
        """Create collection if not exists."""
        collections = [c.name for c in self._client.get_collections().collections]
        if self._collection_name not in collections:
            self._client.create_collection(
                collection_name=self._collection_name,
                vectors_config=self._models.VectorParams(
                    size=dimension,
                    distance=self._models.Distance.COSINE,
                ),
            )
            # Create payload indexes for filtering
            self._client.create_payload_index(
                collection_name=self._collection_name,
                field_name="vocabulary_id",
                field_schema=self._models.PayloadSchemaType.KEYWORD,
            )
            self._client.create_payload_index(
                collection_name=self._collection_name,
                field_name="domain_id",
                field_schema=self._models.PayloadSchemaType.KEYWORD,
            )

    def search(
        self,
        query: str,
        vocabularies: list[str] | None = None,
        domain: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """Vector search with optional filtering."""
        query_embedding = self._embed([query])[0].tolist()

        # Build filter conditions
        conditions = []
        if vocabularies:
            conditions.append(
                self._models.FieldCondition(
                    key="vocabulary_id",
                    match=self._models.MatchAny(any=vocabularies),
                )
            )
        if domain:
            conditions.append(
                self._models.FieldCondition(
                    key="domain_id",
                    match=self._models.MatchValue(value=domain),
                )
            )

        query_filter = None
        if conditions:
            query_filter = self._models.Filter(must=conditions)

        results = self._client.search(
            collection_name=self._collection_name,
            query_vector=query_embedding,
            query_filter=query_filter,
            limit=limit,
        )

        return [
            {
                "concept_id": int(hit.payload.get("concept_id", 0)),
                "concept_name": hit.payload.get("concept_name", ""),
                "vocabulary_id": hit.payload.get("vocabulary_id", ""),
                "domain_id": hit.payload.get("domain_id", ""),
                "concept_class_id": hit.payload.get("concept_class_id", ""),
                "standard_concept": hit.payload.get("standard_concept", ""),
                "score": max(0.0, float(hit.score)),
            }
            for hit in results
        ]

    def get_concept(self, concept_id: int) -> dict:
        """Lookup by concept_id."""
        results = self._client.scroll(
            collection_name=self._collection_name,
            scroll_filter=self._models.Filter(
                must=[
                    self._models.FieldCondition(
                        key="concept_id",
                        match=self._models.MatchValue(value=concept_id),
                    )
                ]
            ),
            limit=1,
        )
        points = results[0]
        if not points:
            raise ValueError(f"Concept {concept_id} not found")
        return dict(points[0].payload)

    def index_concepts(self, concepts: list[dict]) -> None:
        """Bulk index concepts into Qdrant."""
        if not concepts:
            return

        names = [c["concept_name"] for c in concepts]
        logger.info("qdrant.encoding_concepts", count=len(names))
        embeddings = self._embed(names)

        # Ensure collection exists
        self._ensure_collection(embeddings.shape[1])

        # Build points
        points = []
        for i, (concept, emb) in enumerate(zip(concepts, embeddings)):
            points.append(
                self._models.PointStruct(
                    id=i,
                    vector=emb.tolist(),
                    payload={
                        "concept_id": concept["concept_id"],
                        "concept_name": concept["concept_name"],
                        "vocabulary_id": concept.get("vocabulary_id", ""),
                        "domain_id": concept.get("domain_id", ""),
                        "concept_class_id": concept.get("concept_class_id", ""),
                        "standard_concept": concept.get("standard_concept", ""),
                    },
                )
            )

        # Batch upsert
        batch_size = 1000
        for i in range(0, len(points), batch_size):
            self._client.upsert(
                collection_name=self._collection_name,
                points=points[i : i + batch_size],
            )

        logger.info("qdrant.concepts_indexed", count=len(concepts))

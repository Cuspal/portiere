"""
Milvus Backend — Vector search using Milvus vector database.

Uses Milvus for distributed vector similarity search.
Requires: ``pip install portiere-health[milvus]``
"""

from __future__ import annotations

import numpy as np
import structlog

from portiere.knowledge.base import KnowledgeLayerBackend

logger = structlog.get_logger(__name__)


class MilvusBackend(KnowledgeLayerBackend):
    """
    Milvus backend for vector search.

    Best for: Large-scale distributed deployments, billion-scale vectors.
    Requires: pymilvus
    """

    def __init__(
        self,
        uri: str | None = None,
        collection_name: str = "portiere_concepts",
        *,
        embedding_gateway=None,
    ):
        try:
            from pymilvus import (
                Collection,
                CollectionSchema,
                DataType,
                FieldSchema,
                MilvusClient,
                connections,
                utility,
            )

            self._pymilvus_imports = {
                "Collection": Collection,
                "CollectionSchema": CollectionSchema,
                "DataType": DataType,
                "FieldSchema": FieldSchema,
                "MilvusClient": MilvusClient,
                "connections": connections,
                "utility": utility,
            }
        except ImportError:
            raise ImportError(
                "pymilvus is required for Milvus backend. "
                "Install with: pip install portiere-health[milvus]"
            )

        self._embedding_gateway = embedding_gateway
        self._collection_name = collection_name
        self._dimension: int | None = None

        # Use MilvusClient (simpler API, supports Milvus Lite)
        self._client = self._pymilvus_imports["MilvusClient"](uri=uri or "./milvus_portiere.db")

        logger.info("milvus.initialized", uri=uri or "local", collection=collection_name)

    def _embed(self, texts: list[str]) -> np.ndarray:
        """Embed texts using the gateway."""
        if self._embedding_gateway is None:
            raise RuntimeError(
                "Milvus backend requires an embedding_gateway. "
                "Pass embedding_gateway= to the constructor."
            )
        embeddings = self._embedding_gateway.encode(texts, convert_to_numpy=True)
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        norms[norms == 0] = 1
        return embeddings / norms

    def _ensure_collection(self, dimension: int) -> None:
        """Create collection if not exists."""
        if self._client.has_collection(self._collection_name):
            return

        DataType = self._pymilvus_imports["DataType"]
        FieldSchema = self._pymilvus_imports["FieldSchema"]
        CollectionSchema = self._pymilvus_imports["CollectionSchema"]

        schema = self._client.create_schema(auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("concept_id", DataType.INT64)
        schema.add_field("concept_name", DataType.VARCHAR, max_length=500)
        schema.add_field("vocabulary_id", DataType.VARCHAR, max_length=100)
        schema.add_field("domain_id", DataType.VARCHAR, max_length=100)
        schema.add_field("concept_class_id", DataType.VARCHAR, max_length=100)
        schema.add_field("standard_concept", DataType.VARCHAR, max_length=10)
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dimension)

        # Create collection
        self._client.create_collection(
            collection_name=self._collection_name,
            schema=schema,
        )

        # Create vector index
        self._client.create_index(
            collection_name=self._collection_name,
            field_name="embedding",
            index_params={
                "index_type": "IVF_FLAT",
                "metric_type": "COSINE",
                "params": {"nlist": 128},
            },
        )

        self._dimension = dimension

    def search(
        self,
        query: str,
        vocabularies: list[str] | None = None,
        domain: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """Vector search with optional filtering."""
        query_embedding = self._embed([query])[0].tolist()

        # Build filter expression
        filters = []
        if vocabularies:
            vocab_list = ", ".join(f'"{v}"' for v in vocabularies)
            filters.append(f"vocabulary_id in [{vocab_list}]")
        if domain:
            filters.append(f'domain_id == "{domain}"')
        filter_expr = " and ".join(filters) if filters else ""

        results = self._client.search(
            collection_name=self._collection_name,
            data=[query_embedding],
            limit=limit,
            output_fields=[
                "concept_id",
                "concept_name",
                "vocabulary_id",
                "domain_id",
                "concept_class_id",
                "standard_concept",
            ],
            filter=filter_expr or None,
        )

        matches = []
        for hits in results:
            for hit in hits:
                entity = hit.get("entity", hit)
                matches.append(
                    {
                        "concept_id": int(entity.get("concept_id", 0)),
                        "concept_name": entity.get("concept_name", ""),
                        "vocabulary_id": entity.get("vocabulary_id", ""),
                        "domain_id": entity.get("domain_id", ""),
                        "concept_class_id": entity.get("concept_class_id", ""),
                        "standard_concept": entity.get("standard_concept", ""),
                        "score": max(0.0, float(hit.get("distance", hit.get("score", 0.0)))),
                    }
                )

        return matches

    def get_concept(self, concept_id: int) -> dict:
        """Lookup by concept_id."""
        results = self._client.query(
            collection_name=self._collection_name,
            filter=f"concept_id == {concept_id}",
            output_fields=[
                "concept_id",
                "concept_name",
                "vocabulary_id",
                "domain_id",
                "concept_class_id",
                "standard_concept",
            ],
            limit=1,
        )
        if not results:
            raise ValueError(f"Concept {concept_id} not found")
        return dict(results[0])

    def index_concepts(self, concepts: list[dict]) -> None:
        """Bulk index concepts into Milvus."""
        if not concepts:
            return

        names = [c["concept_name"] for c in concepts]
        logger.info("milvus.encoding_concepts", count=len(names))
        embeddings = self._embed(names)

        # Ensure collection exists
        self._ensure_collection(embeddings.shape[1])

        # Prepare data
        data = []
        for i, (concept, emb) in enumerate(zip(concepts, embeddings)):
            data.append(
                {
                    "id": i,
                    "concept_id": concept["concept_id"],
                    "concept_name": concept["concept_name"],
                    "vocabulary_id": concept.get("vocabulary_id", ""),
                    "domain_id": concept.get("domain_id", ""),
                    "concept_class_id": concept.get("concept_class_id", ""),
                    "standard_concept": concept.get("standard_concept", ""),
                    "embedding": emb.tolist(),
                }
            )

        # Batch insert
        batch_size = 5000
        for i in range(0, len(data), batch_size):
            self._client.insert(
                collection_name=self._collection_name,
                data=data[i : i + batch_size],
            )

        logger.info("milvus.concepts_indexed", count=len(concepts))

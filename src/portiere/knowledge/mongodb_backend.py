"""
MongoDB Backend — Vector search using MongoDB Atlas Vector Search.

Uses MongoDB with Atlas Vector Search index for similarity search.
Requires: ``pip install portiere[mongodb]``
"""

from __future__ import annotations

from typing import Any

import numpy as np
import structlog

from portiere.knowledge.base import KnowledgeLayerBackend

logger = structlog.get_logger(__name__)


class MongoDBBackend(KnowledgeLayerBackend):
    """
    MongoDB Atlas Vector Search backend.

    Best for: Teams already using MongoDB, cloud-native deployments.
    Requires: pymongo (with Atlas Vector Search index configured).
    """

    def __init__(
        self,
        connection_string: str,
        database: str = "portiere",
        collection: str = "concepts",
        *,
        embedding_gateway=None,
    ):
        try:
            import pymongo

            self._pymongo = pymongo
        except ImportError:
            raise ImportError(
                "pymongo is required for MongoDB backend. "
                "Install with: pip install portiere[mongodb]"
            )

        self._embedding_gateway = embedding_gateway
        self._client = pymongo.MongoClient(connection_string)
        self._db = self._client[database]
        self._collection = self._db[collection]
        self._dimension: int | None = None

        logger.info("mongodb.initialized", database=database, collection=collection)

    def _embed(self, texts: list[str]) -> np.ndarray:
        """Embed texts using the gateway."""
        if self._embedding_gateway is None:
            raise RuntimeError(
                "MongoDB backend requires an embedding_gateway. "
                "Pass embedding_gateway= to the constructor."
            )
        embeddings = self._embedding_gateway.encode(texts, convert_to_numpy=True)
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        norms[norms == 0] = 1
        return embeddings / norms

    def search(
        self,
        query: str,
        vocabularies: list[str] | None = None,
        domain: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """Vector search using Atlas Vector Search."""
        query_embedding = self._embed([query])[0].tolist()

        # Build Atlas Vector Search pipeline
        vector_search = {
            "$vectorSearch": {
                "index": "concept_vector_index",
                "path": "embedding",
                "queryVector": query_embedding,
                "numCandidates": limit * 3,
                "limit": limit * 3,  # over-fetch for filtering
            }
        }

        # Add pre-filter if supported
        pre_filter: dict[str, Any] = {}
        if vocabularies:
            pre_filter["vocabulary_id"] = {"$in": vocabularies}
        if domain:
            pre_filter["domain_id"] = domain
        if pre_filter:
            vector_search["$vectorSearch"]["filter"] = pre_filter

        pipeline = [
            vector_search,
            {"$addFields": {"score": {"$meta": "vectorSearchScore"}}},
            {"$limit": limit},
            {
                "$project": {
                    "_id": 0,
                    "concept_id": 1,
                    "concept_name": 1,
                    "vocabulary_id": 1,
                    "domain_id": 1,
                    "concept_class_id": 1,
                    "standard_concept": 1,
                    "score": 1,
                }
            },
        ]

        results = list(self._collection.aggregate(pipeline))
        return [
            {
                "concept_id": int(r.get("concept_id", 0)),
                "concept_name": r.get("concept_name", ""),
                "vocabulary_id": r.get("vocabulary_id", ""),
                "domain_id": r.get("domain_id", ""),
                "concept_class_id": r.get("concept_class_id", ""),
                "standard_concept": r.get("standard_concept", ""),
                "score": max(0.0, float(r.get("score", 0.0))),
            }
            for r in results
        ]

    def get_concept(self, concept_id: int) -> dict:
        """Lookup by concept_id."""
        doc = self._collection.find_one(
            {"concept_id": concept_id},
            {"_id": 0, "embedding": 0},
        )
        if doc is None:
            raise ValueError(f"Concept {concept_id} not found")
        return dict(doc)

    def index_concepts(self, concepts: list[dict]) -> None:
        """Bulk index concepts into MongoDB."""
        if not concepts:
            return

        names = [c["concept_name"] for c in concepts]
        logger.info("mongodb.encoding_concepts", count=len(names))
        embeddings = self._embed(names)

        # Prepare documents
        docs = []
        for concept, emb in zip(concepts, embeddings):
            docs.append(
                {
                    "concept_id": concept["concept_id"],
                    "concept_name": concept["concept_name"],
                    "vocabulary_id": concept.get("vocabulary_id", ""),
                    "domain_id": concept.get("domain_id", ""),
                    "concept_class_id": concept.get("concept_class_id", ""),
                    "standard_concept": concept.get("standard_concept", ""),
                    "embedding": emb.tolist(),
                }
            )

        # Bulk upsert
        from pymongo import UpdateOne

        operations = [
            UpdateOne(
                {"concept_id": doc["concept_id"]},
                {"$set": doc},
                upsert=True,
            )
            for doc in docs
        ]

        batch_size = 5000
        for i in range(0, len(operations), batch_size):
            self._collection.bulk_write(operations[i : i + batch_size])

        # Create indexes
        self._collection.create_index("concept_id", unique=True)
        self._collection.create_index("vocabulary_id")
        self._collection.create_index("domain_id")

        logger.info("mongodb.concepts_indexed", count=len(concepts))

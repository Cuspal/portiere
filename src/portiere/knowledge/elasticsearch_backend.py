"""
Elasticsearch Backend — Lexical + optional vector search via Elasticsearch.

Connects to an external Elasticsearch cluster for concept lookup.
Supports BM25 text search and vocabulary/domain filtering.

Requires: pip install elasticsearch
"""

from __future__ import annotations

from typing import Any

import structlog

from portiere.knowledge.base import KnowledgeLayerBackend

logger = structlog.get_logger(__name__)

# Mapping body for creating the index
INDEX_SETTINGS: dict[str, Any] = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "concept_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding"],
                }
            }
        },
    },
    "mappings": {
        "properties": {
            "concept_id": {"type": "integer"},
            "concept_name": {
                "type": "text",
                "analyzer": "concept_analyzer",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "vocabulary_id": {"type": "keyword"},
            "domain_id": {"type": "keyword"},
            "concept_class_id": {"type": "keyword"},
            "standard_concept": {"type": "keyword"},
        }
    },
}


class ElasticsearchBackend(KnowledgeLayerBackend):
    """
    Elasticsearch backend for concept search.

    Best for: Teams with existing ES infrastructure, large vocabularies,
    production deployments needing horizontal scalability.
    """

    def __init__(
        self,
        url: str,
        index_name: str = "portiere_concepts",
        *,
        timeout: int = 30,
        verify_certs: bool = True,
        basic_auth: tuple[str, str] | None = None,
        api_key: str | None = None,
    ):
        """
        Initialize Elasticsearch backend.

        Args:
            url: Elasticsearch cluster URL (e.g. "http://localhost:9200")
            index_name: Name of the index to use
            timeout: Request timeout in seconds
            verify_certs: Whether to verify SSL certificates
            basic_auth: Optional (username, password) tuple
            api_key: Optional API key for authentication
        """
        from elasticsearch import Elasticsearch

        kwargs: dict[str, Any] = {
            "request_timeout": timeout,
            "verify_certs": verify_certs,
        }
        if basic_auth:
            kwargs["basic_auth"] = basic_auth
        if api_key:
            kwargs["api_key"] = api_key

        self._client = Elasticsearch(url, **kwargs)
        self._index = index_name

        # Verify connection
        if not self._client.ping():
            raise ConnectionError(f"Cannot connect to Elasticsearch at {url}")

        logger.info(
            "elasticsearch.connected",
            url=url,
            index=index_name,
        )

    def search(
        self,
        query: str,
        vocabularies: list[str] | None = None,
        domain: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """BM25 search with optional vocabulary and domain filtering."""
        # Build query
        must = [
            {
                "multi_match": {
                    "query": query,
                    "fields": ["concept_name^3", "concept_name.keyword^5"],
                    "type": "best_fields",
                    "fuzziness": "AUTO",
                }
            }
        ]

        filters: list[dict[str, Any]] = []
        if vocabularies:
            filters.append({"terms": {"vocabulary_id": vocabularies}})
        if domain:
            filters.append({"term": {"domain_id": domain}})

        body: dict[str, Any] = {
            "query": {
                "bool": {
                    "must": must,
                    "filter": filters,
                }
            },
            "size": limit,
        }

        response = self._client.search(index=self._index, body=body)

        results = []
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            results.append(
                {
                    "concept_id": source["concept_id"],
                    "concept_name": source["concept_name"],
                    "vocabulary_id": source.get("vocabulary_id", ""),
                    "domain_id": source.get("domain_id", ""),
                    "concept_class_id": source.get("concept_class_id", ""),
                    "standard_concept": source.get("standard_concept", ""),
                    "score": float(hit["_score"]),
                }
            )

        return results

    def get_concept(self, concept_id: int) -> dict:
        """Lookup by concept_id."""
        response = self._client.search(
            index=self._index,
            body={
                "query": {"term": {"concept_id": concept_id}},
                "size": 1,
            },
        )

        hits = response["hits"]["hits"]
        if not hits:
            raise ValueError(f"Concept {concept_id} not found")

        return hits[0]["_source"]

    def index_concepts(self, concepts: list[dict]) -> None:
        """Bulk index concepts into Elasticsearch."""
        from elasticsearch.helpers import bulk

        # Create index if it doesn't exist
        if not self._client.indices.exists(index=self._index):
            self._client.indices.create(index=self._index, body=INDEX_SETTINGS)
            logger.info("elasticsearch.index_created", index=self._index)

        # Prepare bulk actions
        actions = [
            {
                "_index": self._index,
                "_id": str(concept["concept_id"]),
                "_source": {
                    "concept_id": concept["concept_id"],
                    "concept_name": concept["concept_name"],
                    "vocabulary_id": concept.get("vocabulary_id", ""),
                    "domain_id": concept.get("domain_id", ""),
                    "concept_class_id": concept.get("concept_class_id", ""),
                    "standard_concept": concept.get("standard_concept", ""),
                },
            }
            for concept in concepts
        ]

        success, errors = bulk(self._client, actions, raise_on_error=False)

        # Refresh index to make documents searchable immediately
        self._client.indices.refresh(index=self._index)

        logger.info(
            "elasticsearch.concepts_indexed",
            indexed=success,
            errors=len(errors) if isinstance(errors, list) else 0,
            index=self._index,
        )

    def batch_search(
        self,
        queries: list[str],
        vocabularies: list[str] | None = None,
        domain: str | None = None,
        limit: int = 10,
    ) -> list[list[dict]]:
        """Efficient batch search using multi-search API."""
        searches: list[dict[str, Any]] = []
        for query in queries:
            # Header
            searches.append({"index": self._index})
            # Body
            must = [
                {
                    "multi_match": {
                        "query": query,
                        "fields": ["concept_name^3", "concept_name.keyword^5"],
                        "type": "best_fields",
                        "fuzziness": "AUTO",
                    }
                }
            ]
            filters: list[dict[str, Any]] = []
            if vocabularies:
                filters.append({"terms": {"vocabulary_id": vocabularies}})
            if domain:
                filters.append({"term": {"domain_id": domain}})

            searches.append(
                {
                    "query": {"bool": {"must": must, "filter": filters}},
                    "size": limit,
                }
            )

        response = self._client.msearch(body=searches)

        all_results = []
        for resp in response["responses"]:
            results = []
            for hit in resp.get("hits", {}).get("hits", []):
                source = hit["_source"]
                results.append(
                    {
                        "concept_id": source["concept_id"],
                        "concept_name": source["concept_name"],
                        "vocabulary_id": source.get("vocabulary_id", ""),
                        "domain_id": source.get("domain_id", ""),
                        "concept_class_id": source.get("concept_class_id", ""),
                        "standard_concept": source.get("standard_concept", ""),
                        "score": float(hit["_score"]),
                    }
                )
            all_results.append(results)

        return all_results

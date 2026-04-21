"""
Knowledge Layer Base — Abstract interface for concept search backends.

All knowledge layer backends must implement this interface to be used
with the Portiere SDK for concept lookup and indexing.
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class KnowledgeLayerBackend(ABC):
    """Abstract base class for knowledge layer backends."""

    @abstractmethod
    def search(
        self,
        query: str,
        vocabularies: list[str] | None = None,
        domain: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """
        Search for concepts matching query.

        Args:
            query: Search query (concept name, code, or description)
            vocabularies: Optional list of vocabulary IDs to filter by
            domain: Optional domain to filter by
            limit: Maximum number of results to return

        Returns:
            List of dicts with keys:
                concept_id, concept_name, vocabulary_id, domain_id, score
        """

    @abstractmethod
    def get_concept(self, concept_id: int) -> dict:
        """
        Retrieve single concept by ID.

        Args:
            concept_id: The concept ID to look up

        Returns:
            Dict with concept details

        Raises:
            ValueError: If concept not found
        """

    @abstractmethod
    def index_concepts(self, concepts: list[dict]) -> None:
        """
        Bulk index concepts (for building local index).

        Args:
            concepts: List of concept dicts with keys:
                concept_id, concept_name, vocabulary_id, domain_id
        """

    def batch_search(
        self,
        queries: list[str],
        vocabularies: list[str] | None = None,
        domain: str | None = None,
        limit: int = 10,
    ) -> list[list[dict]]:
        """
        Batch search for multiple queries.

        Default implementation calls search() for each query.
        Backends can override for more efficient batch processing.

        Args:
            queries: List of search queries
            vocabularies: Optional vocabulary filter
            domain: Optional domain filter
            limit: Max results per query

        Returns:
            List of result lists, one per query
        """
        return [
            self.search(query, vocabularies=vocabularies, domain=domain, limit=limit)
            for query in queries
        ]

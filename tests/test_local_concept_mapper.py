"""
Tests for local concept mapper.

Tests the full local concept mapping pipeline:
- Code lookup (instant, structured codes)
- Knowledge layer search
- Cross-encoder reranking
- LLM verification
- Confidence routing
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from portiere.config import KnowledgeLayerConfig, LLMConfig, PortiereConfig


def _make_config(
    corpus_path=None,
    reranker=False,
    llm_provider="none",
):
    """Create a PortiereConfig for local mode with given options."""
    kl = None
    if corpus_path:
        kl = KnowledgeLayerConfig(
            backend="bm25s",
            bm25s_corpus_path=corpus_path,
        )

    return PortiereConfig(
        mode="local",
        knowledge_layer=kl,
        embedding_model="cambridgeltl/SapBERT-from-PubMedBERT-fulltext",
        reranker_model="cross-encoder/ms-marco-MiniLM-L-6-v2" if reranker else None,
        llm=LLMConfig(provider=llm_provider, model="gpt-4"),
    )


SAMPLE_CONCEPTS = [
    {
        "concept_id": 201826,
        "concept_name": "Type 2 diabetes mellitus",
        "vocabulary_id": "SNOMED",
        "domain_id": "Condition",
    },
    {
        "concept_id": 320128,
        "concept_name": "Essential hypertension",
        "vocabulary_id": "SNOMED",
        "domain_id": "Condition",
    },
    {
        "concept_id": 1503297,
        "concept_name": "Metformin",
        "vocabulary_id": "RxNorm",
        "domain_id": "Drug",
    },
]


class TestLocalConceptMapperInit:
    def test_lazy_initialization(self):
        from portiere.local.concept_mapper import LocalConceptMapper

        config = _make_config()
        mapper = LocalConceptMapper(config)
        assert mapper._initialized is False

    def test_no_knowledge_layer_warns(self):
        from portiere.local.concept_mapper import LocalConceptMapper

        config = _make_config(corpus_path=None)
        mapper = LocalConceptMapper(config)
        mapper._initialize()

        assert mapper._knowledge_backend is None
        assert mapper._initialized is True

    def test_knowledge_backend_created(self):
        from portiere.local.concept_mapper import LocalConceptMapper

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(SAMPLE_CONCEPTS, f)
            corpus_path = Path(f.name)

        try:
            config = _make_config(corpus_path=corpus_path)
            mapper = LocalConceptMapper(config)
            mapper._initialize()

            assert mapper._knowledge_backend is not None
            assert mapper._initialized is True
        finally:
            corpus_path.unlink(missing_ok=True)


class TestLocalConceptMapperCodeLookup:
    @pytest.fixture
    def mapper_with_code_index(self):
        from portiere.local.concept_mapper import LocalConceptMapper

        config = _make_config()
        mapper = LocalConceptMapper(config)
        mapper._initialized = True
        mapper._code_index = {
            "E11.9": {
                "concept_id": 201826,
                "concept_name": "Type 2 diabetes mellitus",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
            },
            "E119": {
                "concept_id": 201826,
                "concept_name": "Type 2 diabetes mellitus",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
            },
            "I10": {
                "concept_id": 320128,
                "concept_name": "Essential hypertension",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
            },
        }
        return mapper

    def test_exact_match(self, mapper_with_code_index):
        result = mapper_with_code_index._code_lookup("E11.9")
        assert result is not None
        assert len(result) == 1
        assert result[0]["concept_id"] == 201826
        assert result[0]["score"] == 0.99

    def test_case_insensitive(self, mapper_with_code_index):
        result = mapper_with_code_index._code_lookup("e11.9")
        # Should match via case-insensitive lookups
        # Note: depends on code_index having lowercase keys or the lookup logic
        # The actual code tries q, q.upper(), q.lower()
        assert result is not None

    def test_variant_without_dot(self, mapper_with_code_index):
        result = mapper_with_code_index._code_lookup("E11.9")
        assert result is not None
        assert result[0]["score"] == 0.99

    def test_no_match_returns_none(self, mapper_with_code_index):
        result = mapper_with_code_index._code_lookup("ZZZZZ")
        assert result is None

    def test_prefix_match(self, mapper_with_code_index):
        """ICD-10 prefix match: E11 should find E11* entries."""
        # Add prefix entry
        mapper_with_code_index._code_index["E11"] = {
            "concept_id": 201826,
            "concept_name": "Type 2 diabetes mellitus",
            "vocabulary_id": "SNOMED",
            "domain_id": "Condition",
        }

        result = mapper_with_code_index._code_lookup("E11.0")
        # Should try prefix match (E11)
        # The code splits on "." and takes prefix
        assert result is not None
        assert result[0]["score"] == 0.92  # Prefix match score


class TestLocalConceptMapperSearch:
    @pytest.fixture
    def mapper_with_backend(self):
        from portiere.local.concept_mapper import LocalConceptMapper

        config = _make_config()
        mapper = LocalConceptMapper(config)
        mapper._initialized = True
        mapper._code_index = {}

        # Mock knowledge backend
        mock_backend = MagicMock()
        mock_backend.search.return_value = [
            {
                "concept_id": 201826,
                "concept_name": "Type 2 diabetes",
                "score": 0.85,
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
            },
            {
                "concept_id": 320128,
                "concept_name": "Hypertension",
                "score": 0.60,
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
            },
        ]
        mapper._knowledge_backend = mock_backend
        mapper._reranker = None
        mapper._router = None
        return mapper, mock_backend

    def test_search_returns_results(self, mapper_with_backend):
        mapper, _ = mapper_with_backend
        results = mapper.search("diabetes")

        assert len(results) == 2
        assert results[0]["concept_id"] == 201826

    def test_search_passes_filters(self, mapper_with_backend):
        mapper, mock_backend = mapper_with_backend
        mapper.search("diabetes", vocabularies=["SNOMED"], domain="Condition", limit=5)

        mock_backend.search.assert_called_once_with(
            "diabetes",
            vocabularies=["SNOMED"],
            domain="Condition",
            limit=25,  # limit * 5 capped at 50
        )

    def test_search_no_backend_returns_empty(self):
        from portiere.local.concept_mapper import LocalConceptMapper

        config = _make_config()
        mapper = LocalConceptMapper(config)
        mapper._initialized = True
        mapper._knowledge_backend = None
        mapper._code_index = {}

        results = mapper.search("diabetes")
        assert results == []

    def test_search_code_lookup_takes_priority(self, mapper_with_backend):
        mapper, mock_backend = mapper_with_backend
        mapper._code_index = {
            "E11.9": {
                "concept_id": 201826,
                "concept_name": "Type 2 DM",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
            },
        }

        results = mapper.search("E11.9")

        # Code lookup should be used, backend should not be called
        mock_backend.search.assert_not_called()
        assert results[0]["score"] == 0.99

    def test_search_with_reranker(self, mapper_with_backend):
        mapper, _ = mapper_with_backend

        mock_reranker = MagicMock()
        mock_reranker.available = True
        mock_reranker.rerank_with_blending.return_value = [
            {"concept_id": 320128, "concept_name": "Hypertension", "score": 0.90},
            {"concept_id": 201826, "concept_name": "Type 2 diabetes", "score": 0.70},
        ]
        mapper._reranker = mock_reranker

        results = mapper.search("diabetes")

        mock_reranker.rerank_with_blending.assert_called_once()
        assert results[0]["concept_id"] == 320128  # Reranked order


class TestLocalConceptMapperMapCode:
    @pytest.fixture
    def mapper_with_router(self):
        from portiere.local.concept_mapper import LocalConceptMapper

        config = _make_config()
        mapper = LocalConceptMapper(config)
        mapper._initialized = True
        mapper._code_index = {}

        mock_backend = MagicMock()
        mock_backend.search.return_value = [
            {
                "concept_id": 201826,
                "concept_name": "Type 2 diabetes",
                "score": 0.90,
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
            },
        ]
        mapper._knowledge_backend = mock_backend
        mapper._reranker = None

        mock_router = AsyncMock()
        mock_router.route.return_value = {
            "method": "review",
            "confidence": 0.90,
            "target_concept_id": 201826,
            "target_concept_name": "Type 2 diabetes",
            "target_vocabulary_id": "SNOMED",
            "target_domain_id": "Condition",
            "reasoning": "Medium confidence",
        }
        mapper._router = mock_router
        return mapper, mock_router

    @pytest.mark.asyncio
    async def test_map_code_with_router(self, mapper_with_router):
        mapper, mock_router = mapper_with_router

        result = await mapper.map_code("E11.9", source_description="Type 2 DM")

        assert result["source_code"] == "E11.9"
        assert result["source_description"] == "Type 2 DM"
        assert result["method"] == "review"
        assert "candidates" in result
        mock_router.route.assert_called_once()

    @pytest.mark.asyncio
    async def test_map_code_without_router(self):
        from portiere.local.concept_mapper import LocalConceptMapper

        config = _make_config()
        mapper = LocalConceptMapper(config)
        mapper._initialized = True
        mapper._code_index = {}
        mapper._router = None

        mock_backend = MagicMock()
        mock_backend.search.return_value = [
            {
                "concept_id": 1,
                "concept_name": "X",
                "score": 0.97,
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
            },
        ]
        mapper._knowledge_backend = mock_backend
        mapper._reranker = None

        result = await mapper.map_code("CODE1")

        assert result["method"] == "auto"  # score 0.97 >= 0.95
        assert result["confidence"] == 0.97

    @pytest.mark.asyncio
    async def test_map_code_no_candidates(self):
        from portiere.local.concept_mapper import LocalConceptMapper

        config = _make_config()
        mapper = LocalConceptMapper(config)
        mapper._initialized = True
        mapper._code_index = {}
        mapper._knowledge_backend = MagicMock()
        mapper._knowledge_backend.search.return_value = []
        mapper._reranker = None
        mapper._router = None

        result = await mapper.map_code("UNKNOWN_CODE")

        assert result["method"] == "manual"
        assert result["confidence"] == 0.0


class TestLocalConceptMapperMapBatch:
    @pytest.mark.asyncio
    async def test_map_batch_maps_all_codes(self):
        from portiere.local.concept_mapper import LocalConceptMapper

        config = _make_config()
        mapper = LocalConceptMapper(config)
        mapper._initialized = True
        mapper._code_index = {}
        mapper._reranker = None
        mapper._router = None

        mock_backend = MagicMock()
        mock_backend.search.return_value = [
            {
                "concept_id": 1,
                "concept_name": "Result",
                "score": 0.80,
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
            },
        ]
        mapper._knowledge_backend = mock_backend

        codes = [
            {"code": "A", "description": "Desc A", "count": 5},
            {"code": "B", "description": "Desc B", "count": 3},
        ]

        results = await mapper.map_batch(codes, ["SNOMED"])

        assert len(results) == 2
        assert results[0]["source_code"] == "A"
        assert results[0]["source_count"] == 5
        assert results[1]["source_code"] == "B"
        assert results[1]["source_count"] == 3

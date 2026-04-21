"""
Tests for local LLM verifier and confidence router.

Tests:
- LocalLLMVerifier: LLM-based mapping verification
- LocalConfidenceRouter: Confidence-based routing logic
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ── LocalLLMVerifier Tests ────────────────────────────────────────


class TestLocalLLMVerifierInit:
    def test_init_stores_config(self):
        from portiere.local.llm_verifier import LocalLLMVerifier

        mock_config = MagicMock()
        verifier = LocalLLMVerifier(mock_config)
        assert verifier._config is mock_config
        assert verifier._gateway is None

    def test_get_gateway_lazy_init(self):
        from portiere.local.llm_verifier import LocalLLMVerifier

        mock_config = MagicMock()
        verifier = LocalLLMVerifier(mock_config)

        with patch("portiere.llm.gateway.LLMGateway") as mock_gw_cls:
            mock_gw = MagicMock()
            mock_gw_cls.return_value = mock_gw

            gw = verifier._get_gateway()
            assert gw is mock_gw
            mock_gw_cls.assert_called_once_with(mock_config)

    def test_get_gateway_cached(self):
        from portiere.local.llm_verifier import LocalLLMVerifier

        mock_config = MagicMock()
        verifier = LocalLLMVerifier(mock_config)
        verifier._gateway = MagicMock()

        gw = verifier._get_gateway()
        assert gw is verifier._gateway


class TestLocalLLMVerifierVerifyMapping:
    @pytest.fixture
    def verifier_with_mock(self):
        from portiere.local.llm_verifier import LocalLLMVerifier

        mock_config = MagicMock()
        verifier = LocalLLMVerifier(mock_config)
        mock_gateway = AsyncMock()
        verifier._gateway = mock_gateway
        return verifier, mock_gateway

    @pytest.mark.asyncio
    async def test_verify_mapping_calls_gateway(self, verifier_with_mock):
        verifier, mock_gateway = verifier_with_mock

        mock_gateway.complete_structured.return_value = {
            "is_correct": True,
            "confidence": 0.92,
            "selected_concept_id": 201826,
            "reasoning": "Correct match",
        }

        proposed = {
            "concept_id": 201826,
            "concept_name": "Type 2 diabetes mellitus",
            "vocabulary_id": "SNOMED",
            "domain_id": "Condition",
        }
        alternatives = [
            {
                "concept_id": 320128,
                "concept_name": "Hypertension",
                "vocabulary_id": "SNOMED",
                "score": 0.7,
            },
        ]

        result = await verifier.verify_mapping(
            source_term="Type 2 DM",
            proposed_concept=proposed,
            alternatives=alternatives,
        )

        assert result["is_correct"] is True
        assert result["confidence"] == 0.92
        mock_gateway.complete_structured.assert_called_once()

    @pytest.mark.asyncio
    async def test_verify_mapping_error_fallback(self, verifier_with_mock):
        verifier, mock_gateway = verifier_with_mock

        mock_gateway.complete_structured.side_effect = Exception("LLM error")

        proposed = {
            "concept_id": 201826,
            "concept_name": "Type 2 diabetes",
            "vocabulary_id": "SNOMED",
            "domain_id": "Condition",
            "score": 0.85,
        }

        result = await verifier.verify_mapping(
            source_term="Type 2 DM",
            proposed_concept=proposed,
            alternatives=[],
        )

        # Should fall back gracefully
        assert result["is_correct"] is True
        assert result["selected_concept_id"] == 201826
        assert "error" in result["reasoning"].lower()


class TestLocalLLMVerifierDisambiguate:
    @pytest.fixture
    def verifier_with_mock(self):
        from portiere.local.llm_verifier import LocalLLMVerifier

        mock_config = MagicMock()
        verifier = LocalLLMVerifier(mock_config)
        mock_gateway = AsyncMock()
        verifier._gateway = mock_gateway
        return verifier, mock_gateway

    @pytest.mark.asyncio
    async def test_disambiguate_single_candidate(self, verifier_with_mock):
        verifier, _ = verifier_with_mock

        candidates = [{"concept_id": 100, "score": 0.9}]
        result = await verifier.disambiguate("term", candidates)

        assert result["selected_concept_id"] == 100
        assert result["reasoning"] == "Single candidate"

    @pytest.mark.asyncio
    async def test_disambiguate_empty_candidates(self, verifier_with_mock):
        verifier, _ = verifier_with_mock

        result = await verifier.disambiguate("term", [])
        assert result["selected_concept_id"] is None

    @pytest.mark.asyncio
    async def test_disambiguate_multiple_calls_llm(self, verifier_with_mock):
        verifier, mock_gateway = verifier_with_mock

        mock_gateway.complete_structured.return_value = {
            "selected_concept_id": 200,
            "confidence": 0.88,
            "reasoning": "More specific match",
        }

        candidates = [
            {
                "concept_id": 100,
                "concept_name": "A",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
                "score": 0.85,
            },
            {
                "concept_id": 200,
                "concept_name": "B",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
                "score": 0.83,
            },
        ]

        result = await verifier.disambiguate("term", candidates, domain="Condition")

        assert result["selected_concept_id"] == 200
        mock_gateway.complete_structured.assert_called_once()

    @pytest.mark.asyncio
    async def test_disambiguate_error_fallback(self, verifier_with_mock):
        verifier, mock_gateway = verifier_with_mock

        mock_gateway.complete_structured.side_effect = Exception("API timeout")

        candidates = [
            {"concept_id": 100, "concept_name": "A", "score": 0.85},
            {"concept_id": 200, "concept_name": "B", "score": 0.83},
        ]

        result = await verifier.disambiguate("term", candidates)

        # Falls back to first candidate
        assert result["selected_concept_id"] == 100


# ── LocalConfidenceRouter Tests ───────────────────────────────────


class TestLocalConfidenceRouter:
    @pytest.fixture
    def router_no_verifier(self):
        from portiere.local.llm_verifier import LocalConfidenceRouter

        return LocalConfidenceRouter(
            verifier=None,
            auto_threshold=0.95,
            verify_threshold=0.80,
            review_threshold=0.70,
        )

    @pytest.fixture
    def router_with_verifier(self):
        from portiere.local.llm_verifier import LocalConfidenceRouter, LocalLLMVerifier

        mock_verifier = AsyncMock(spec=LocalLLMVerifier)
        return LocalConfidenceRouter(
            verifier=mock_verifier,
            auto_threshold=0.95,
            verify_threshold=0.80,
            review_threshold=0.70,
        ), mock_verifier

    @pytest.mark.asyncio
    async def test_no_candidates_returns_manual(self, router_no_verifier):
        result = await router_no_verifier.route("diabetes", [])
        assert result["method"] == "manual"
        assert result["confidence"] == 0.0
        assert result["target_concept_id"] is None

    @pytest.mark.asyncio
    async def test_high_confidence_auto_accept(self, router_no_verifier):
        candidates = [
            {
                "concept_id": 1,
                "concept_name": "Diabetes",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
                "score": 0.97,
            },
        ]
        result = await router_no_verifier.route("diabetes", candidates)

        assert result["method"] == "auto"
        assert result["confidence"] == 0.97
        assert result["target_concept_id"] == 1

    @pytest.mark.asyncio
    async def test_medium_high_without_verifier_goes_to_review(self, router_no_verifier):
        """Without LLM verifier, scores 0.80-0.95 should be 'review' not 'verified'."""
        candidates = [
            {
                "concept_id": 1,
                "concept_name": "Diabetes",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
                "score": 0.88,
            },
        ]
        result = await router_no_verifier.route("diabetes", candidates)

        # Without verifier, falls to review threshold check
        assert result["method"] == "review"

    @pytest.mark.asyncio
    async def test_medium_high_with_verifier_verified(self, router_with_verifier):
        router, mock_verifier = router_with_verifier

        mock_verifier.verify_mapping.return_value = {
            "is_correct": True,
            "confidence": 0.91,
            "selected_concept_id": 1,
            "reasoning": "Correct match",
        }

        candidates = [
            {
                "concept_id": 1,
                "concept_name": "Diabetes",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
                "score": 0.88,
            },
            {
                "concept_id": 2,
                "concept_name": "Type 1 DM",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
                "score": 0.75,
            },
        ]
        result = await router.route("diabetes", candidates)

        assert result["method"] == "verified"
        assert result["confidence"] == 0.91
        mock_verifier.verify_mapping.assert_called_once()

    @pytest.mark.asyncio
    async def test_verifier_selects_different_concept(self, router_with_verifier):
        router, mock_verifier = router_with_verifier

        mock_verifier.verify_mapping.return_value = {
            "is_correct": False,
            "confidence": 0.85,
            "selected_concept_id": 2,  # LLM picks a different concept
            "reasoning": "Alternative is more specific",
        }

        candidates = [
            {
                "concept_id": 1,
                "concept_name": "A",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
                "score": 0.88,
            },
            {
                "concept_id": 2,
                "concept_name": "B",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
                "score": 0.82,
            },
        ]
        result = await router.route("term", candidates)

        assert result["method"] == "verified"
        assert result["target_concept_id"] == 2

    @pytest.mark.asyncio
    async def test_medium_confidence_review(self, router_no_verifier):
        candidates = [
            {
                "concept_id": 1,
                "concept_name": "A",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
                "score": 0.75,
            },
        ]
        result = await router_no_verifier.route("term", candidates)

        assert result["method"] == "review"
        assert result["confidence"] == 0.75

    @pytest.mark.asyncio
    async def test_low_confidence_manual(self, router_no_verifier):
        candidates = [
            {
                "concept_id": 1,
                "concept_name": "A",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
                "score": 0.50,
            },
        ]
        result = await router_no_verifier.route("term", candidates)

        assert result["method"] == "manual"
        assert result["confidence"] == 0.50
        # Score > 0.3, so concept should still be suggested
        assert result["target_concept_id"] == 1

    @pytest.mark.asyncio
    async def test_very_low_confidence_no_suggestion(self, router_no_verifier):
        candidates = [
            {"concept_id": 1, "concept_name": "A", "score": 0.20},
        ]
        result = await router_no_verifier.route("term", candidates)

        assert result["method"] == "manual"
        assert result["target_concept_id"] is None

    @pytest.mark.asyncio
    async def test_uses_rrf_score_field(self, router_no_verifier):
        """Test that router checks rrf_score if score is missing."""
        candidates = [
            {
                "concept_id": 1,
                "concept_name": "A",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
                "rrf_score": 0.97,
            },
        ]
        result = await router_no_verifier.route("term", candidates)

        assert result["method"] == "auto"
        assert result["confidence"] == 0.97

    @pytest.mark.asyncio
    async def test_custom_thresholds(self):
        from portiere.local.llm_verifier import LocalConfidenceRouter

        router = LocalConfidenceRouter(
            verifier=None,
            auto_threshold=0.99,  # Very strict
            review_threshold=0.50,  # Very lenient
        )

        # Score 0.97 would be auto with default, but review with strict threshold
        candidates = [
            {
                "concept_id": 1,
                "concept_name": "A",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
                "score": 0.97,
            },
        ]
        result = await router.route("term", candidates)

        assert result["method"] == "review"  # Not auto because 0.97 < 0.99

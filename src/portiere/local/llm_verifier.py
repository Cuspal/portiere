"""
Local LLM Verifier — LLM-based mapping verification for local mode.

Uses the LLMGateway (supporting OpenAI, Anthropic, Bedrock, Ollama, etc.)
to verify uncertain concept mappings. Replicates the API-side LLM verifier.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from portiere.config import LLMConfig

logger = structlog.get_logger(__name__)


# ── Prompts (same as server-side) ────────────────────────────────

CONCEPT_VERIFICATION_PROMPT = """You are a clinical terminology expert mapping hospital codes to OMOP CDM standard concepts. Your task is to verify if a source clinical term maps correctly to a standard concept.

Source term: "{source_term}"
Source context: {source_context}

Proposed mapping:
- Concept ID: {concept_id}
- Concept Name: "{concept_name}"
- Vocabulary: {vocabulary}
- Domain: {domain}

Alternative candidates:
{alternatives}

Here are examples of correct and incorrect mappings for reference:

CORRECT mappings:
- "Type 2 DM" -> SNOMED "Type 2 diabetes mellitus" (201826) - abbreviation matches
- "Acetaminophen 500mg tab" -> RxNorm "Acetaminophen 500 MG Oral Tablet" (1125315) - brand-generic match
- "HbA1c" -> LOINC "Hemoglobin A1c/Hemoglobin.total in Blood" (3004410) - lab code match

INCORRECT mappings:
- "Type 1 DM" -> SNOMED "Type 2 diabetes mellitus" - wrong diabetes type
- "Systolic BP" -> SNOMED "Diastolic blood pressure" - wrong measurement
- "Serum glucose" -> LOINC "Glucose [Mass/volume] in Urine" - wrong specimen

Instructions:
1. Evaluate if the proposed mapping is correct
2. If not, identify the better candidate from the alternatives
3. Consider clinical context, synonyms, and semantic equivalence

Respond in JSON format:
{{
    "is_correct": true/false,
    "confidence": 0.0-1.0,
    "selected_concept_id": <concept_id>,
    "reasoning": "brief explanation"
}}"""


DISAMBIGUATION_PROMPT = """You are a clinical terminology expert. Given a source term and multiple candidate mappings with similar scores, select the most appropriate mapping.

Source term: "{source_term}"
Domain hint: {domain}

Candidates:
{candidates}

Consider:
1. Semantic equivalence
2. Clinical context
3. Level of specificity (prefer more specific matches)
4. Standard concept preference

Respond in JSON format:
{{
    "selected_concept_id": <best_concept_id>,
    "confidence": 0.0-1.0,
    "reasoning": "brief explanation"
}}"""


class LocalLLMVerifier:
    """
    LLM-based verification for local concept mapping.

    Uses the SDK's LLMGateway to call any configured LLM provider
    (OpenAI, Anthropic, Bedrock, Ollama, etc.).
    """

    def __init__(self, llm_config: LLMConfig):
        self._config = llm_config
        self._gateway: Any = None

    def _get_gateway(self):
        """Lazy-init the LLM gateway."""
        if self._gateway is None:
            from portiere.llm.gateway import LLMGateway

            self._gateway = LLMGateway(self._config)
        return self._gateway

    async def verify_mapping(
        self,
        source_term: str,
        proposed_concept: dict,
        alternatives: list[dict],
        source_context: str | None = None,
    ) -> dict:
        """
        Verify a proposed concept mapping using LLM.

        Args:
            source_term: The source clinical term
            proposed_concept: The top candidate concept
            alternatives: Other candidate concepts
            source_context: Optional domain/context hint

        Returns:
            Verification result with confidence and reasoning
        """
        gateway = self._get_gateway()

        # Format alternatives
        alt_text = "\n".join(
            f"- [{c.get('concept_id')}] {c.get('concept_name')} "
            f"({c.get('vocabulary_id')}) - score: {c.get('score', 0):.2f}"
            for c in alternatives[:5]
        )

        prompt = CONCEPT_VERIFICATION_PROMPT.format(
            source_term=source_term,
            source_context=source_context or "Unknown",
            concept_id=proposed_concept.get("concept_id"),
            concept_name=proposed_concept.get("concept_name"),
            vocabulary=proposed_concept.get("vocabulary_id"),
            domain=proposed_concept.get("domain_id"),
            alternatives=alt_text or "None",
        )

        try:
            result = await gateway.complete_structured(
                prompt=prompt,
                schema={},
                system="You are a clinical terminology expert.",
            )
            return result

        except Exception as e:
            logger.error("llm_verifier.verification_failed", error=str(e))
            return {
                "is_correct": True,
                "confidence": proposed_concept.get("score", 0.8),
                "selected_concept_id": proposed_concept.get("concept_id"),
                "reasoning": f"Verification error: {e}",
            }

    async def disambiguate(
        self,
        source_term: str,
        candidates: list[dict],
        domain: str | None = None,
    ) -> dict:
        """
        Disambiguate between close candidate mappings.

        Args:
            source_term: Source clinical term
            candidates: Close candidates with similar scores
            domain: Optional domain hint

        Returns:
            Disambiguation result with selected concept
        """
        if len(candidates) <= 1:
            return {
                "selected_concept_id": candidates[0].get("concept_id") if candidates else None,
                "confidence": candidates[0].get("score", 0.5) if candidates else 0,
                "reasoning": "Single candidate",
            }

        gateway = self._get_gateway()

        candidates_text = "\n".join(
            f"{i + 1}. [{c.get('concept_id')}] {c.get('concept_name')}\n"
            f"   Vocabulary: {c.get('vocabulary_id')}, Domain: {c.get('domain_id')}\n"
            f"   Score: {c.get('score', 0):.3f}"
            for i, c in enumerate(candidates[:5])
        )

        prompt = DISAMBIGUATION_PROMPT.format(
            source_term=source_term,
            domain=domain or "Unknown",
            candidates=candidates_text,
        )

        try:
            result = await gateway.complete_structured(
                prompt=prompt,
                schema={},
                system="You are a clinical terminology expert.",
            )
            return result

        except Exception as e:
            logger.error("llm_verifier.disambiguation_failed", error=str(e))
            return {
                "selected_concept_id": candidates[0].get("concept_id"),
                "confidence": candidates[0].get("score", 0.5),
                "reasoning": f"Disambiguation error: {e}",
            }


class LocalConfidenceRouter:
    """
    Routes mappings based on confidence scores — local mode version.

    Thresholds (configurable via PortiereConfig.thresholds):
    - AUTO: >= auto_accept → Accept automatically
    - VERIFY: >= needs_review → LLM verification (if LLM configured)
    - REVIEW: 0.70-auto_accept → Human review
    - MANUAL: < 0.70 → Manual mapping required
    """

    def __init__(
        self,
        verifier: LocalLLMVerifier | None = None,
        auto_threshold: float = 0.95,
        verify_threshold: float = 0.80,
        review_threshold: float = 0.70,
    ):
        self.verifier = verifier
        self.auto_threshold = auto_threshold
        self.verify_threshold = verify_threshold
        self.review_threshold = review_threshold

    async def route(
        self,
        source_term: str,
        candidates: list[dict],
        domain: str | None = None,
    ) -> dict:
        """
        Route a mapping based on confidence score of top candidate.

        Returns:
            Mapping result with method and final confidence
        """
        if not candidates:
            return {
                "method": "manual",
                "confidence": 0.0,
                "target_concept_id": None,
                "reasoning": "No candidates found",
            }

        top = candidates[0]
        score = top.get("score", 0) or top.get("rrf_score", 0)

        # HIGH confidence → Auto-accept
        if score >= self.auto_threshold:
            return {
                "method": "auto",
                "confidence": score,
                "target_concept_id": top.get("concept_id"),
                "target_concept_name": top.get("concept_name"),
                "target_vocabulary_id": top.get("vocabulary_id"),
                "target_domain_id": top.get("domain_id"),
                "reasoning": f"High confidence ({score:.2f})",
            }

        # MEDIUM-HIGH → LLM verification (if verifier available)
        if score >= self.verify_threshold and self.verifier:
            result = await self.verifier.verify_mapping(
                source_term=source_term,
                proposed_concept=top,
                alternatives=candidates[1:5],
                source_context=domain,
            )

            selected_id = result.get("selected_concept_id", top.get("concept_id"))
            selected = next(
                (c for c in candidates if c.get("concept_id") == selected_id),
                top,
            )

            return {
                "method": "verified",
                "confidence": result.get("confidence", score),
                "target_concept_id": selected.get("concept_id"),
                "target_concept_name": selected.get("concept_name"),
                "target_vocabulary_id": selected.get("vocabulary_id"),
                "target_domain_id": selected.get("domain_id"),
                "reasoning": result.get("reasoning", "LLM verified"),
            }

        # MEDIUM → Needs review
        if score >= self.review_threshold:
            return {
                "method": "review",
                "confidence": score,
                "target_concept_id": top.get("concept_id"),
                "target_concept_name": top.get("concept_name"),
                "target_vocabulary_id": top.get("vocabulary_id"),
                "target_domain_id": top.get("domain_id"),
                "reasoning": f"Medium confidence ({score:.2f}), needs review",
            }

        # LOW → Manual
        return {
            "method": "manual",
            "confidence": score,
            "target_concept_id": top.get("concept_id") if score > 0.3 else None,
            "target_concept_name": top.get("concept_name") if score > 0.3 else None,
            "target_vocabulary_id": top.get("vocabulary_id") if score > 0.3 else None,
            "target_domain_id": top.get("domain_id") if score > 0.3 else None,
            "reasoning": f"Low confidence ({score:.2f}), manual mapping suggested",
        }

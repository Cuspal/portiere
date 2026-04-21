"""
Local Concept Mapper — Full AI pipeline for local mode.

Replicates the server-side ConceptMapperService pipeline:
1. Direct code lookup (instant, highest confidence)
2. Knowledge layer search (BM25s / FAISS / Hybrid via KnowledgeLayerBackend)
3. Cross-encoder reranking (optional)
4. LLM verification (optional, for medium-confidence)
5. Confidence routing (auto / verified / review / manual)

Users can customize every component:
- Knowledge backend: BM25s, FAISS, Elasticsearch, Hybrid
- Embedding model: Any sentence-transformer from HuggingFace
- Reranker model: Any cross-encoder from HuggingFace
- LLM provider: OpenAI, Anthropic, Bedrock, Ollama, etc.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from portiere.config import PortiereConfig
    from portiere.knowledge.base import KnowledgeLayerBackend
    from portiere.local.llm_verifier import LocalConfidenceRouter
    from portiere.local.reranker import LocalReranker

logger = structlog.get_logger(__name__)


class LocalConceptMapper:
    """
    Local concept mapping with pluggable backends.

    Pipeline mirrors the server:
    0. Code lookup (instant, for structured codes)
    1. Knowledge layer search (BM25s/FAISS/Hybrid)
    2. Cross-encoder reranking (optional)
    3. LLM verification (optional, for 0.80-0.95 confidence)
    4. Confidence routing → auto/verified/review/manual
    """

    def __init__(self, config: PortiereConfig):
        self._config = config
        self._knowledge_backend: KnowledgeLayerBackend | None = None
        self._reranker: LocalReranker | None = None
        self._router: LocalConfidenceRouter | None = None
        self._code_index: dict = {}
        self._initialized = False

    def _initialize(self):
        """Lazy initialization of all components."""
        if self._initialized:
            return

        # 1. Knowledge layer backend
        if self._config.knowledge_layer is not None:
            from portiere.knowledge.factory import create_knowledge_backend_from_config

            self._knowledge_backend = create_knowledge_backend_from_config(self._config)
        else:
            logger.warning(
                "local_concept_mapper.no_knowledge_layer",
                message="No knowledge_layer configured. Concept search will not work.",
            )

        # 2. Reranker (optional)
        if self._config.reranker.provider != "none":
            from portiere.local.reranker import LocalReranker

            self._reranker = LocalReranker(reranker_config=self._config.reranker)

        # 3. LLM verifier + confidence router (optional)
        from portiere.local.llm_verifier import LocalConfidenceRouter

        verifier = None
        if self._config.llm.provider != "none":
            # User has configured a BYO-LLM — use it for verification
            from portiere.local.llm_verifier import LocalLLMVerifier

            verifier = LocalLLMVerifier(self._config.llm)

        thresholds = self._config.thresholds.concept_mapping
        self._router = LocalConfidenceRouter(
            verifier=verifier,
            auto_threshold=thresholds.auto_accept,
            review_threshold=thresholds.needs_review,
        )

        # 4. Code index (optional, if file exists alongside knowledge layer data)
        self._load_code_index()

        self._initialized = True
        logger.info(
            "local_concept_mapper.initialized",
            knowledge_backend=type(self._knowledge_backend).__name__
            if self._knowledge_backend
            else None,
            reranker=self._reranker is not None,
            llm_verifier=verifier is not None,
            code_index_entries=len(self._code_index),
        )

    def _load_code_index(self):
        """Load pre-built code → concept lookup index if available."""
        search_paths = []

        kl = self._config.knowledge_layer
        if kl:
            if kl.bm25s_corpus_path:
                search_paths.append(Path(kl.bm25s_corpus_path).parent / "code_index.json")
            if kl.faiss_index_path:
                search_paths.append(Path(kl.faiss_index_path).parent / "code_index.json")

        for path in search_paths:
            if path.exists():
                try:
                    with open(path) as f:
                        self._code_index = json.load(f)
                    logger.info(
                        "local_concept_mapper.code_index_loaded", entries=len(self._code_index)
                    )
                    return
                except Exception as e:
                    logger.warning(f"Failed to load code index from {path}: {e}")

    def search(
        self,
        query: str,
        *,
        vocabularies: list[str] | None = None,
        domain: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """
        Search for concepts matching query.

        Uses the full pipeline: code lookup → knowledge search → reranking.

        Args:
            query: Search text (code, concept name, or description)
            vocabularies: Filter by vocabulary (SNOMED, LOINC, etc.)
            domain: Filter by domain (Drug, Condition, etc.)
            limit: Max results

        Returns:
            Ranked list of matching concepts with scores
        """
        self._initialize()

        # Fast path: direct code lookup
        if self._code_index:
            code_result = self._code_lookup(query)
            if code_result:
                return code_result[:limit]

        # Knowledge layer search
        if self._knowledge_backend is None:
            logger.warning("local_concept_mapper.no_backend", query=query[:50])
            return []

        candidate_limit = min(limit * 5, 50)
        candidates = self._knowledge_backend.search(
            query,
            vocabularies=vocabularies,
            domain=domain,
            limit=candidate_limit,
        )

        if not candidates:
            return []

        # Cross-encoder reranking (optional)
        if self._reranker and self._reranker.available and len(candidates) > 1:
            candidates = self._reranker.rerank_with_blending(
                query=query,
                candidates=candidates,
                top_k=limit,
                text_field="concept_name",
            )

        return candidates[:limit]

    def _code_lookup(self, query: str) -> list[dict] | None:
        """
        Direct code lookup — instant mapping for structured codes.

        Same logic as the server: exact match → variant match → prefix match.
        """
        q = query.strip()

        # Exact match (case-insensitive)
        match = (
            self._code_index.get(q)
            or self._code_index.get(q.upper())
            or self._code_index.get(q.lower())
        )
        if match:
            return [{**match, "score": 0.99, "standard_concept": "S"}]

        # Variant: with/without dots (E11.9 ↔ E119)
        if "." in q:
            nodot = q.replace(".", "")
            match = self._code_index.get(nodot) or self._code_index.get(nodot.upper())
        elif len(q) > 3 and q[:1].isalpha():
            dotted = q[:3] + "." + q[3:]
            match = self._code_index.get(dotted) or self._code_index.get(dotted.upper())
        else:
            match = None

        if match:
            return [{**match, "score": 0.97, "standard_concept": "S"}]

        # ICD-10 prefix match (E11 → E11.*)
        prefix = q.split(".")[0].upper()
        if len(prefix) >= 3:
            match = self._code_index.get(prefix)
            if match:
                return [{**match, "score": 0.92, "standard_concept": "S"}]

        return None

    async def map_code(
        self,
        source_code: str,
        source_description: str | None = None,
        domain: str | None = None,
        vocabularies: list[str] | None = None,
    ) -> dict:
        """
        Map a single source code to a standard concept.

        Full pipeline with confidence routing.

        Args:
            source_code: The source code to map
            source_description: Description of the code
            domain: Expected domain (Drug, Condition, etc.)
            vocabularies: Target vocabularies to search

        Returns:
            Mapping result with concept, confidence, method, and candidates
        """
        self._initialize()

        query = source_description or source_code

        # Get candidates
        candidates = self.search(
            query,
            vocabularies=vocabularies,
            domain=domain,
            limit=10,
        )

        # Route based on confidence
        if self._router:
            result = await self._router.route(
                source_term=query,
                candidates=candidates,
                domain=domain,
            )
        else:
            # Fallback routing (no LLM configured)
            if candidates:
                top = candidates[0]
                score = top.get("score", 0)
                result = {
                    "method": "auto" if score >= 0.95 else "review" if score >= 0.70 else "manual",
                    "confidence": score,
                    "target_concept_id": top.get("concept_id"),
                    "target_concept_name": top.get("concept_name"),
                    "target_vocabulary_id": top.get("vocabulary_id"),
                    "target_domain_id": top.get("domain_id"),
                }
            else:
                result = {
                    "method": "manual",
                    "confidence": 0.0,
                    "target_concept_id": None,
                }

        # Add source info and candidates
        result["source_code"] = source_code
        result["source_description"] = source_description
        result["candidates"] = candidates[:5]

        return result

    async def map_batch(
        self,
        codes: list[dict],
        vocabularies: list[str],
        domain: str | None = None,
    ) -> list[dict]:
        """
        Map a batch of codes to standard concepts.

        Args:
            codes: List of {code, description, count}
            vocabularies: Target vocabularies
            domain: Expected domain

        Returns:
            List of mapping results
        """
        results = []
        for code_entry in codes:
            result = await self.map_code(
                source_code=code_entry.get("code", ""),
                source_description=code_entry.get("description"),
                domain=domain,
                vocabularies=vocabularies,
            )
            result["source_count"] = code_entry.get("count", 1)
            results.append(result)

        return results

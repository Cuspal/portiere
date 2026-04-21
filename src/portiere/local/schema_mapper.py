"""
Local Schema Mapper — Embedding-enhanced schema mapping for local mode.

Supports any target standard via YAML-based definitions (OMOP, FHIR, HL7 v2, OpenEHR, etc.).
Pipeline:
1. Pattern matching (fast, exact, from standard's source_patterns)
2. Embedding similarity (SapBERT or custom model)
3. Score fusion (pattern authoritative, embedding calibrated)
4. Cross-encoder reranking (optional, for close candidates)

Users can customize the embedding model via PortiereConfig.embedding_model.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np
import structlog

if TYPE_CHECKING:
    from portiere.config import PortiereConfig
    from portiere.local.reranker import LocalReranker
    from portiere.models.target_model import TargetModel

logger = structlog.get_logger(__name__)


def _load_target_model(config: PortiereConfig) -> TargetModel:
    """Load the target model from config, defaulting to OMOP CDM v5.4."""
    from portiere.standards import YAMLTargetModel

    model_name = getattr(config, "target_model", "omop_cdm_v5.4")
    return YAMLTargetModel.from_name(model_name)


class LocalSchemaMapper:
    """
    Local schema mapper with pattern matching + embeddings.

    Supports any target standard via TargetModel abstraction.
    Pipeline:
    1. Pattern matching (fast path, high confidence)
    2. Embedding similarity (handles non-standard column names)
    3. Score fusion (pattern authoritative, embedding calibrated)
    4. Cross-encoder reranking (optional, for close candidates)
    """

    EMBEDDING_ONLY_THRESHOLD = 0.40
    RERANK_DELTA_THRESHOLD = 0.10

    def __init__(self, config: PortiereConfig, target_model: TargetModel | None = None):
        self._config = config
        self._model: Any = None
        self._reranker: LocalReranker | None = None
        self._target_embeddings: Any = None
        self._target_keys: Any = None
        self._target_descriptions: Any = None
        self._initialized = False

        # Load target model (determines patterns and descriptions)
        if target_model is not None:
            self._target_model = target_model
        else:
            self._target_model = _load_target_model(config)

        # Load patterns and descriptions from the target model
        self._source_patterns = self._load_source_patterns()
        self._desc_map = self._load_target_description_map()

        # Determine fallback entity/field
        self._default_entity, self._default_field = self._resolve_defaults()

    def _load_source_patterns(self) -> dict[str, tuple[str, str]]:
        """Load source column patterns from the target model."""
        if hasattr(self._target_model, "get_source_patterns"):
            return self._target_model.get_source_patterns()
        return {}

    def _load_target_description_map(self) -> dict[tuple[str, str], str]:
        """Load target descriptions keyed by (entity, field) tuples."""
        if hasattr(self._target_model, "get_target_descriptions_tupled"):
            return self._target_model.get_target_descriptions_tupled()

        # Fallback: convert "entity.field" → (entity, field) format
        descriptions = self._target_model.get_target_descriptions()
        tupled = {}
        for key, desc in descriptions.items():
            if "." in key:
                entity, field = key.split(".", 1)
                tupled[(entity, field)] = desc
        return tupled

    def _resolve_defaults(self) -> tuple[str, str]:
        """Get default fallback entity and field for unmapped columns."""
        if hasattr(self._target_model, "get_default_entity"):
            entity = self._target_model.get_default_entity()
            field = (
                self._target_model.get_default_field()
                if hasattr(self._target_model, "get_default_field")
                else ""
            )
            if entity and field:
                return entity, field

        # Fall back to first entity's first field
        schema = self._target_model.get_schema()
        if schema:
            first_entity = next(iter(schema))
            first_field = schema[first_entity][0] if schema[first_entity] else ""
            return first_entity, first_field

        return "observation", "observation_source_value"

    def _initialize(self):
        """Lazy initialization: load model, compute target embeddings."""
        if self._initialized:
            return

        try:
            from portiere.embedding import EmbeddingGateway

            logger.info(
                "local_schema_mapper.loading_model",
                provider=self._config.embedding.provider,
                model=self._config.embedding.model,
                target_standard=self._target_model.name,
            )
            self._model = EmbeddingGateway(self._config.embedding)

            # Pre-compute target embeddings from the target model
            self._target_keys = list(self._desc_map.keys())
            self._target_descriptions = list(self._desc_map.values())

            logger.info(
                "local_schema_mapper.computing_target_embeddings",
                targets=len(self._target_keys),
                standard=self._target_model.name,
            )
            self._target_embeddings = self._model.encode(
                self._target_descriptions,
                normalize_embeddings=True,
                show_progress_bar=False,
            )

            # Initialize reranker (optional)
            if self._config.reranker.provider != "none":
                from portiere.local.reranker import LocalReranker

                self._reranker = LocalReranker(reranker_config=self._config.reranker)

            self._initialized = True
            logger.info("local_schema_mapper.initialized", standard=self._target_model.name)

        except Exception as e:
            logger.warning(f"local_schema_mapper.init_failed: {e}")
            self._initialized = True  # Don't retry, fall back to pattern-only

    def suggest(self, columns: list[dict[str, Any]]) -> list[dict]:
        """
        Generate schema mapping suggestions.

        Args:
            columns: List of dicts with 'name', 'type', 'sample_values'

        Returns:
            List of suggestion dicts with source_column, target_table,
            target_column, confidence, reasoning, candidates
        """
        self._initialize()

        results = []

        for col in columns:
            col_name = col["name"]
            col_type = col.get("type", "")
            sample_values = col.get("sample_values", [])
            col_name_lower = col_name.lower()

            # Step 1: Pattern matching
            pattern_result = self._pattern_match(col_name_lower)

            # Step 2: Embedding similarity
            embedding_results = self._embedding_match(col_name, col_type, sample_values)

            # Step 3: Score fusion
            suggestion = self._fuse_scores(col_name, pattern_result, embedding_results)

            # Step 4: Optional cross-encoder reranking
            candidates = suggestion.get("candidates", [])
            has_pattern = suggestion.pop("_has_pattern", False)
            if (
                self._reranker
                and self._reranker.available
                and not has_pattern
                and len(candidates) >= 2
                and (candidates[0]["score"] - candidates[1]["score"]) < self.RERANK_DELTA_THRESHOLD
            ):
                suggestion = self._rerank_candidates(col_name, col_type, sample_values, suggestion)

            results.append(suggestion)

        # Deduplicate: same (table, column) target → keep highest confidence
        self._deduplicate_targets(results)

        return results

    def _pattern_match(self, col_name_lower: str) -> dict | None:
        """Pattern matching against the target model's source patterns."""
        for pattern, (table, column) in self._source_patterns.items():
            if pattern in col_name_lower:
                confidence = 0.95 if col_name_lower == pattern else 0.85
                return {
                    "table": table,
                    "column": column,
                    "confidence": confidence,
                    "pattern": pattern,
                }
        return None

    def _embedding_match(
        self,
        col_name: str,
        col_type: str,
        sample_values: list[str],
        top_k: int = 5,
    ) -> list[dict]:
        """Embedding similarity against target model descriptions."""
        if self._model is None or self._target_embeddings is None:
            return []

        source_text = self._build_source_text(col_name, col_type, sample_values)
        source_embedding = self._model.encode([source_text], normalize_embeddings=True)

        # Cosine similarity (both L2-normalized)
        similarities = np.dot(source_embedding, self._target_embeddings.T)[0]
        top_indices = np.argsort(similarities)[::-1][:top_k]

        results = []
        for idx in top_indices:
            table, column = self._target_keys[idx]
            results.append(
                {
                    "table": table,
                    "column": column,
                    "score": float(similarities[idx]),
                    "description": self._target_descriptions[idx],
                }
            )

        return results

    def _build_source_text(self, col_name: str, col_type: str, sample_values: list[str]) -> str:
        """Build descriptive text for embedding. Same as server."""
        parts = [col_name.replace("_", " ")]

        if col_type:
            type_hints = {
                "int": "numeric identifier",
                "float": "numeric value",
                "object": "text",
                "str": "text",
                "date": "date",
                "datetime": "date and time",
                "bool": "yes no flag",
            }
            type_lower = col_type.lower()
            for key, hint in type_hints.items():
                if key in type_lower:
                    parts.append(hint)
                    break

        if sample_values:
            parts.append(f"examples: {', '.join(str(s) for s in sample_values[:3])}")

        return " ".join(parts)

    def _fuse_scores(
        self,
        col_name: str,
        pattern_result: dict | None,
        embedding_results: list[dict],
    ) -> dict:
        """Fuse pattern and embedding scores. Same strategy as server."""
        if pattern_result:
            return {
                "source_column": col_name,
                "target_table": pattern_result["table"],
                "target_column": pattern_result["column"],
                "confidence": pattern_result["confidence"],
                "reasoning": f"Pattern match: '{pattern_result['pattern']}'",
                "candidates": embedding_results,
                "_has_pattern": True,
            }

        elif embedding_results:
            top = embedding_results[0]
            raw = top["score"]
            if raw >= self.EMBEDDING_ONLY_THRESHOLD:
                confidence = min(0.50 + raw, 0.95)
            else:
                confidence = raw * 0.7

            return {
                "source_column": col_name,
                "target_table": top["table"],
                "target_column": top["column"],
                "confidence": round(confidence, 4),
                "reasoning": f"Embedding match: {top['description']} (cosine={raw:.3f})",
                "candidates": embedding_results,
                "_has_pattern": False,
            }

        else:
            default_entity, default_field = self._default_entity, self._default_field
            return {
                "source_column": col_name,
                "target_table": default_entity,
                "target_column": default_field,
                "confidence": 0.30,
                "reasoning": f"No match found, defaulting to {default_entity}.{default_field}",
                "candidates": [],
            }

    def _rerank_candidates(
        self,
        col_name: str,
        col_type: str,
        sample_values: list[str],
        suggestion: dict,
    ) -> dict:
        """Cross-encoder reranking for close candidates."""
        source_text = self._build_source_text(col_name, col_type, sample_values)
        candidates = suggestion["candidates"][:5]
        original_confidence = suggestion["confidence"]

        scored = []
        for cand in candidates:
            ce_score = self._reranker.score_pair(  # type: ignore[union-attr]
                source_term=source_text,
                target_text=cand["description"],
                context="schema mapping",
            )
            scored.append({**cand, "ce_score": ce_score})

        scored.sort(key=lambda x: x["ce_score"], reverse=True)

        best = scored[0]
        original_top = candidates[0] if candidates else {}
        if (best["table"], best["column"]) != (
            original_top.get("table"),
            original_top.get("column"),
        ):
            final_confidence = round(original_confidence * 0.9, 4)
        else:
            final_confidence = original_confidence

        return {
            "source_column": col_name,
            "target_table": best["table"],
            "target_column": best["column"],
            "confidence": final_confidence,
            "reasoning": (
                f"Cross-encoder reranked: {best['description']} "
                f"(fused={original_confidence:.2f}, ce={best['ce_score']:.2f})"
            ),
            "candidates": [{k: v for k, v in c.items() if k != "ce_score"} for c in scored],
        }

    def _deduplicate_targets(self, results: list[dict]) -> None:
        """When multiple source columns map to same target, keep highest confidence."""
        seen_targets: dict[tuple[str, str], int] = {}
        for i, s in enumerate(results):
            key = (s["target_table"], s["target_column"])
            if key in seen_targets:
                prev_i = seen_targets[key]
                if s["confidence"] > results[prev_i]["confidence"]:
                    results[prev_i]["confidence"] = min(results[prev_i]["confidence"], 0.50)
                    results[prev_i]["reasoning"] = (
                        results[prev_i].get("reasoning") or ""
                    ) + " [demoted: duplicate target]"
                    seen_targets[key] = i
                else:
                    s["confidence"] = min(s["confidence"], 0.50)
                    s["reasoning"] = (s.get("reasoning") or "") + " [demoted: duplicate target]"
            else:
                seen_targets[key] = i

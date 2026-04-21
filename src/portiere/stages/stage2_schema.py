"""
Portiere Stage 2 — Schema Mapping.

This stage:
1. Analyzes source schema
2. Proposes mappings to target model (OMOP CDM)
3. Uses AI to suggest column → table/column mappings

Supports two modes:
- Cloud: Sends request to Portiere API for AI inference
- Local: Uses local embedding model + pattern matching (same pipeline as server)
"""

from typing import TYPE_CHECKING, Any, Optional

import structlog

if TYPE_CHECKING:
    from portiere.client import Client
    from portiere.config import PortiereConfig
    from portiere.models.source import SourceProfile

logger = structlog.get_logger(__name__)


def map_schema(
    client: Optional["Client"] = None,
    source_profile: Optional["SourceProfile"] = None,
    target_model: str = "omop_cdm_v5.4",
    *,
    config: Optional["PortiereConfig"] = None,
    columns: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Create AI-powered schema mapping.

    Supports two modes:
    - Cloud mode: Pass ``client`` and ``source_profile`` → calls Portiere API
    - Local mode: Pass ``config`` (with mode="local") and either ``columns``
      or ``source_profile`` → runs locally with embedding model

    Args:
        client: Portiere client for API calls (cloud mode)
        source_profile: Profile from Stage 1
        target_model: Target data model (omop_cdm_v5.4, fhir_r4, etc.)
        config: PortiereConfig for local mode
        columns: Column dicts (name, type, sample_values) for local mode

    Returns:
        Schema mapping with suggestions
    """
    use_local = _should_use_local(client, config)

    if use_local:
        assert config is not None
        return _map_schema_local(config, columns, source_profile, target_model)
    else:
        assert client is not None
        assert source_profile is not None
        return _map_schema_cloud(client, source_profile, target_model)


def _should_use_local(
    client: Optional["Client"],
    config: Optional["PortiereConfig"],
) -> bool:
    """Determine whether to use local or cloud mode."""
    if config is not None:
        # Use computed properties when available (PortiereConfig)
        eff_pipeline = getattr(config, "effective_pipeline", getattr(config, "pipeline", "local"))
        eff_mode = getattr(config, "effective_mode", getattr(config, "mode", "local"))

        if eff_pipeline == "local":
            return True
        if eff_pipeline == "cloud" and client is not None:
            return False
        if eff_mode == "local":
            return True
        if eff_mode == "hybrid" and client is None:
            return True
        return True
    return False


def _map_schema_cloud(
    client: "Client",
    source_profile: "SourceProfile",
    target_model: str,
) -> dict[str, Any]:
    """Cloud mode: call Portiere API for schema mapping."""
    from portiere.models.target_model import get_target_model

    logger.info("Stage 2: Schema mapping (cloud)", target_model=target_model)

    model = get_target_model(target_model)
    target_schema = model.get_schema()
    target_descriptions = model.get_target_descriptions()

    response = client._request(
        "POST",
        "/schema-mapping/suggest",
        json={
            "columns": source_profile.columns,
            "target_model": target_model,
            "target_schema": target_schema,
            "target_descriptions": target_descriptions,
        },
    )

    mappings = response.get("mappings", [])
    return _build_result(mappings)


def _map_schema_local(
    config: "PortiereConfig",
    columns: list[dict[str, Any]] | None,
    source_profile: Optional["SourceProfile"],
    target_model: str,
) -> dict[str, Any]:
    """
    Local mode: run schema mapping entirely on local machine.

    Uses the same pipeline as the server:
    1. Pattern matching (fast path, known OMOP patterns)
    2. Embedding similarity (SapBERT or custom model from config)
    3. Score fusion
    4. Optional cross-encoder reranking
    """
    from portiere.local.schema_mapper import LocalSchemaMapper

    logger.info(
        "Stage 2: Schema mapping (local)",
        target_model=target_model,
        embedding_provider=config.embedding.provider,
        embedding_model=config.embedding.model,
    )

    # Resolve columns from either direct parameter or source_profile
    if columns is None and source_profile is not None:
        if hasattr(source_profile, "columns"):
            columns = source_profile.columns
        elif isinstance(source_profile, dict):
            columns = source_profile.get("columns", [])
    if columns is None:
        raise ValueError(
            "Either 'columns' or 'source_profile' must be provided for local schema mapping."
        )

    mapper = LocalSchemaMapper(config)
    mappings = mapper.suggest(columns)

    return _build_result(mappings, config=config)


def _build_result(
    mappings: list[dict],
    config: Optional["PortiereConfig"] = None,
) -> dict[str, Any]:
    """Build standardized result from mapping list.

    Applies confidence routing to set status on each mapping dict
    using thresholds from config (or defaults: auto>=0.95, review>=0.70).
    """
    auto_threshold = 0.95
    review_threshold = 0.70
    if config and hasattr(config, "thresholds"):
        auto_threshold = config.thresholds.schema_mapping.auto_accept
        review_threshold = config.thresholds.schema_mapping.needs_review

    for m in mappings:
        confidence = m.get("confidence", 0)
        if confidence >= auto_threshold:
            m["status"] = "auto_accepted"
        elif confidence >= review_threshold:
            m["status"] = "needs_review"
        else:
            m["status"] = "unmapped"

    auto_accepted = [m for m in mappings if m["status"] == "auto_accepted"]
    needs_review = [m for m in mappings if m["status"] == "needs_review"]
    unmapped = [m for m in mappings if m["status"] == "unmapped"]

    result: dict[str, Any] = {
        "mappings": mappings,
        "stats": {
            "total": len(mappings),
            "auto_accepted": len(auto_accepted),
            "needs_review": len(needs_review),
            "unmapped": len(unmapped),
        },
    }

    logger.info(
        "Stage 2 complete",
        total=result["stats"]["total"],
        auto=result["stats"]["auto_accepted"],
        review=result["stats"]["needs_review"],
    )

    return result

"""
Portiere Stage 3 — Concept Mapping.

This is the core value proposition stage that:
1. Extracts unique codes from source data
2. Maps to standard vocabularies using AI (search + reranking + LLM)
3. Applies confidence routing (auto/review/manual)
4. Returns mapping results

Supports two modes:
- Cloud: Sends codes to Portiere API for AI mapping
- Local: Uses local knowledge layer (BM25s/FAISS/Hybrid) + optional
  cross-encoder reranker + optional LLM verification
"""

import asyncio
import concurrent.futures
from typing import TYPE_CHECKING, Any, Optional

import structlog

if TYPE_CHECKING:
    from portiere.client import Client
    from portiere.config import PortiereConfig
    from portiere.engines.base import AbstractEngine

logger = structlog.get_logger(__name__)


def _find_description_column(df_columns: list[str], code_column: str) -> str | None:
    """
    Find a companion description column for a code column.

    Uses naming conventions:
      diagnosis_code → diagnosis_description / diagnosis_name / diagnosis_desc
      drug_cd        → drug_description / drug_name / drug_desc
    """
    col_lower = code_column.lower()
    for suffix_from in ("_code", "_cd"):
        if col_lower.endswith(suffix_from):
            base = code_column[: len(code_column) - len(suffix_from)]
            for suffix_to in ("_description", "_name", "_desc", "_text"):
                candidate = base + suffix_to
                for actual_col in df_columns:
                    if actual_col.lower() == candidate.lower():
                        return actual_col
    return None


def _extract_description_map(df: Any, code_column: str, desc_column: str) -> dict[str, str]:
    """Extract a code → description mapping from a DataFrame (Polars or Pandas)."""
    mapping: dict[str, str] = {}
    try:
        if hasattr(df, "select"):
            # Polars
            rows = df.select([code_column, desc_column]).unique(subset=[code_column]).to_dicts()
            for row in rows:
                code = str(row[code_column]) if row[code_column] is not None else ""
                desc = str(row[desc_column]) if row[desc_column] is not None else ""
                if code:
                    mapping[code] = desc or code
        elif hasattr(df, "drop_duplicates"):
            # Pandas
            subset = df[[code_column, desc_column]].drop_duplicates(subset=[code_column])
            for _, row in subset.iterrows():
                code = str(row[code_column]) if row[code_column] is not None else ""
                desc = str(row[desc_column]) if row[desc_column] is not None else ""
                if code:
                    mapping[code] = desc or code
    except Exception as e:
        logger.warning("stage3.description_extraction_failed", error=str(e))
    return mapping


def _run_async(coro):
    """Run an async coroutine from sync code, compatible with Jupyter notebooks."""
    try:
        asyncio.get_running_loop()
        # Event loop already running (e.g. Jupyter) — run in a new thread
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result()
    except RuntimeError:
        # No running loop
        return asyncio.run(coro)


def map_concepts(
    client: Optional["Client"] = None,
    engine: Optional["AbstractEngine"] = None,
    source_path: str = "",
    code_columns: list[str] | None = None,
    vocabularies: list[str] | None = None,
    format: str = "csv",
    *,
    config: Optional["PortiereConfig"] = None,
    codes: list[dict] | None = None,
) -> dict[str, Any]:
    """
    AI-powered concept mapping for code columns.

    Supports two modes:
    - Cloud mode: Pass ``client`` and ``engine`` → extracts codes locally,
      sends to Portiere API for AI mapping
    - Local mode: Pass ``config`` and ``engine`` → runs the full pipeline
      locally with knowledge layer + optional reranker + optional LLM

    Args:
        client: Portiere client for API calls (cloud mode)
        engine: Compute engine for data extraction
        source_path: Path to source data
        code_columns: Columns containing codes to map
        vocabularies: Target vocabularies
        format: Data format
        config: PortiereConfig for local mode
        codes: Pre-extracted codes (optional, bypasses engine extraction)

    Returns:
        Mapping results by column
    """
    if code_columns is None:
        code_columns = []

    use_local = _should_use_local(client, config)

    if use_local:
        assert config is not None
        return _map_concepts_local(
            config, engine, source_path, code_columns, vocabularies, format, codes
        )
    else:
        assert client is not None
        assert engine is not None
        return _map_concepts_cloud(client, engine, source_path, code_columns, vocabularies, format)


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


def _map_concepts_cloud(
    client: "Client",
    engine: "AbstractEngine",
    source_path: str,
    code_columns: list[str],
    vocabularies: list[str] | None,
    format: str,
) -> dict[str, Any]:
    """Cloud mode: extract codes locally, send to Portiere API for mapping."""
    logger.info("Stage 3: Concept mapping (cloud)", columns=code_columns)

    if vocabularies is None:
        vocabularies = ["SNOMED", "LOINC", "RxNorm", "ICD10CM"]

    df = engine.read_source(source_path, format=format)

    all_mappings = {}
    total_stats = {
        "total_codes": 0,
        "auto_mapped": 0,
        "needs_review": 0,
        "manual": 0,
    }

    for column in code_columns:
        logger.info(f"Mapping column: {column}")

        distinct_values = engine.get_distinct_values(df, column, limit=5000)

        # Look for a companion description column (e.g. diagnosis_code → diagnosis_description)
        desc_column = _find_description_column(list(df.columns), column)
        code_to_desc: dict[str, str] = {}
        if desc_column:
            code_to_desc = _extract_description_map(df, column, desc_column)
            logger.info(
                f"Found description column: {desc_column}",
                code_column=column,
                desc_column=desc_column,
                mappings=len(code_to_desc),
            )

        codes = []
        for item in distinct_values:
            code = str(item.get("value", ""))
            if code:
                codes.append(
                    {
                        "code": code,
                        "description": code_to_desc.get(code, code),
                        "count": item.get("count", 1),
                    }
                )

        if not codes:
            logger.warning(f"No codes found in column: {column}")
            continue

        response = client._request(
            "POST",
            "/concepts/map",
            json={
                "source_id": source_path,
                "column": column,
                "codes": codes,
                "vocabularies": vocabularies,
            },
        )

        items = response.get("items", [])
        stats = response.get("stats", {})

        all_mappings[column] = {"items": items, "stats": stats}

        total_stats["total_codes"] += stats.get("total", 0)
        total_stats["auto_mapped"] += stats.get("auto", 0)
        total_stats["needs_review"] += stats.get("review", 0)
        total_stats["manual"] += stats.get("manual", 0)

    return _build_result(all_mappings, total_stats)


def _map_concepts_local(
    config: "PortiereConfig",
    engine: Optional["AbstractEngine"],
    source_path: str,
    code_columns: list[str],
    vocabularies: list[str] | None,
    format: str,
    pre_extracted_codes: list[dict] | None,
) -> dict[str, Any]:
    """
    Local mode: run concept mapping entirely on local machine.

    Uses the same pipeline as the server:
    1. Code lookup (instant, for structured codes like ICD-10)
    2. Knowledge layer search (BM25s / FAISS / Hybrid)
    3. Cross-encoder reranking (optional, uses config.reranker_model)
    4. LLM verification (optional, uses config.llm for medium-confidence)
    5. Confidence routing → auto/verified/review/manual
    """
    from portiere.local.concept_mapper import LocalConceptMapper

    logger.info(
        "Stage 3: Concept mapping (local)",
        columns=code_columns,
        knowledge_backend=config.knowledge_layer.backend if config.knowledge_layer else None,
    )

    if vocabularies is None:
        vocabularies = ["SNOMED", "LOINC", "RxNorm", "ICD10CM"]

    mapper = LocalConceptMapper(config)

    all_mappings = {}
    total_stats = {
        "total_codes": 0,
        "auto_mapped": 0,
        "needs_review": 0,
        "manual": 0,
    }

    # If pre-extracted codes are provided, map them directly (no engine needed)
    if pre_extracted_codes is not None:
        items = _run_async(mapper.map_batch(pre_extracted_codes, vocabularies))
        stats = _compute_stats(items)
        all_mappings["_direct"] = {"items": items, "stats": stats}
        total_stats["total_codes"] += stats["total"]
        total_stats["auto_mapped"] += stats["auto"]
        total_stats["needs_review"] += stats["review"]
        total_stats["manual"] += stats["manual"]
        return _build_result(all_mappings, total_stats)

    # Extract codes from source data using engine
    if engine is None:
        raise ValueError("Either 'engine' or 'codes' must be provided for concept mapping.")

    df = engine.read_source(source_path, format=format)

    for column in code_columns:
        logger.info(f"Mapping column: {column}")

        distinct_values = engine.get_distinct_values(df, column, limit=5000)

        # Look for a companion description column (e.g. diagnosis_code → diagnosis_description)
        desc_column = _find_description_column(list(df.columns), column)
        code_to_desc: dict[str, str] = {}
        if desc_column:
            code_to_desc = _extract_description_map(df, column, desc_column)
            logger.info(
                f"Found description column: {desc_column}",
                code_column=column,
                desc_column=desc_column,
                mappings=len(code_to_desc),
            )

        codes = []
        for item in distinct_values:
            code = str(item.get("value", ""))
            if code:
                codes.append(
                    {
                        "code": code,
                        "description": code_to_desc.get(code, code),
                        "count": item.get("count", 1),
                    }
                )

        if not codes:
            logger.warning(f"No codes found in column: {column}")
            continue

        # Map batch locally
        items = _run_async(mapper.map_batch(codes, vocabularies))

        # Add column info to each item
        for item in items:
            item["source_column"] = column

        stats = _compute_stats(items)

        all_mappings[column] = {"items": items, "stats": stats}

        total_stats["total_codes"] += stats["total"]
        total_stats["auto_mapped"] += stats["auto"]
        total_stats["needs_review"] += stats["review"]
        total_stats["manual"] += stats["manual"]

    return _build_result(all_mappings, total_stats)


def _compute_stats(items: list[dict]) -> dict:
    """Compute auto/review/manual stats from mapping items."""
    auto = sum(1 for i in items if i.get("method") in ("auto", "verified"))
    review = sum(1 for i in items if i.get("method") == "review")
    manual = sum(1 for i in items if i.get("method") == "manual")
    return {"total": len(items), "auto": auto, "review": review, "manual": manual}


def _build_result(
    all_mappings: dict[str, Any],
    total_stats: dict[str, int],
) -> dict[str, Any]:
    """Build standardized result dict."""
    result = {
        "mappings": all_mappings,
        "stats": total_stats,
        "auto_rate": (
            total_stats["auto_mapped"] / total_stats["total_codes"] * 100
            if total_stats["total_codes"] > 0
            else 0
        ),
    }

    logger.info(
        "Stage 3 complete",
        total_codes=total_stats["total_codes"],
        auto_rate=f"{result['auto_rate']:.1f}%",
    )

    return result


def get_mapping_summary(mappings: dict[str, Any]) -> str:
    """Generate human-readable mapping summary."""
    stats = mappings.get("stats", {})

    total = stats.get("total_codes", 0)
    auto = stats.get("auto_mapped", 0)
    review = stats.get("needs_review", 0)
    manual = stats.get("manual", 0)

    auto_rate = auto / total * 100 if total > 0 else 0

    summary = f"""
\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557
\u2551                 Concept Mapping Summary                       \u2551
\u2560\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2563
\u2551  Total codes processed:  {total:>6}                            \u2551
\u2551  Auto-mapped (\u226595%):     {auto:>6} ({auto_rate:5.1f}%)                    \u2551
\u2551  Needs review (70-95%):  {review:>6}                            \u2551
\u2551  Manual mapping (<70%):  {manual:>6}                            \u2551
\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d
"""
    return summary.strip()


def approve_all_review_items(
    client: "Client",
    project_id: str,
    mapping_id: str,
) -> dict:
    """
    Approve all 'needs review' items using AI's top suggestion.

    Args:
        client: Portiere client
        project_id: Project ID
        mapping_id: Mapping ID to approve

    Returns:
        Approval result
    """
    return client._request(
        "POST",
        f"/projects/{project_id}/concept-mapping/{mapping_id}/approve-all",
    )

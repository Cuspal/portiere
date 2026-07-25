"""
Dependency-free tool core for Portiere integrations.

Defines the framework-agnostic operations that the MCP server
(``integrations.mcp``) and LangChain adapter (``integrations.langchain``) both
wrap — so the operation logic is written and tested once, with no optional
dependencies. Each :class:`ToolSpec` carries a JSON Schema and a ``run`` callable
returning a JSON-serializable dict.

Safety: every operation that constructs a project runs with ``offline=True`` —
an agent driving these tools cannot make Portiere send data off-machine.
"""

from __future__ import annotations

import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ToolSpec:
    """One agent-callable operation: name, description, schema, and handler."""

    name: str
    description: str
    input_schema: dict
    run: Callable[..., dict]


def _obj(properties: dict, required: list[str] | None = None) -> dict:
    schema: dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


# ── operations ────────────────────────────────────────────────────────────────


def _list_standards() -> dict:
    from portiere.standards import list_standards

    return {"standards": sorted(list_standards())}


def _profile_source(path: str | None = None, format: str = "csv") -> dict:
    if not path:
        return {"error": "missing required argument: path"}
    if not Path(path).exists():
        return {"error": f"source not found: {path}"}
    from portiere.engines import get_engine

    engine = get_engine("polars")
    df = engine.read_source(path, format=format)
    profile = engine.profile(df)
    profile["row_count"] = engine.count(df)
    # Keep the payload compact for an agent: drop verbose top_values lists.
    for col in profile.get("columns", []):
        col.pop("top_values", None)
    return profile


def _suggest_schema_mapping(
    columns: list[dict] | None = None, target_model: str = "omop_cdm_v5.4"
) -> dict:
    if not columns:
        return {"error": "missing required argument: columns"}
    from portiere.config import EmbeddingConfig, PortiereConfig, RerankerConfig
    from portiere.stages.stage2_schema import map_schema

    config = PortiereConfig(
        offline=True,
        embedding=EmbeddingConfig(provider="none"),
        reranker=RerankerConfig(provider="none", model=""),
    )
    result = map_schema(client=None, target_model=target_model, config=config, columns=columns)
    mappings = [
        {
            "source_column": m.get("source_column", ""),
            "target_table": m.get("target_table"),
            "target_column": m.get("target_column"),
            "confidence": round(float(m.get("confidence", 0.0)), 3),
            "status": str(m.get("status", "")),
        }
        for m in result.get("mappings", [])
    ]
    return {"target_model": target_model, "mappings": mappings}


def _map_concepts(
    codes: list[dict] | None = None,
    vocabularies: list[str] | None = None,
    knowledge_path: str | None = None,
    athena_dir: str | None = None,
) -> dict:
    if not codes:
        return {"error": "missing required argument: codes"}
    import portiere
    from portiere.config import (
        EmbeddingConfig,
        KnowledgeLayerConfig,
        PortiereConfig,
        RerankerConfig,
    )
    from portiere.knowledge import build_knowledge_layer

    vocabularies = vocabularies or ["ICD10CM", "LOINC", "RxNorm"]
    work = Path(tempfile.mkdtemp(prefix="portiere_tool_"))
    note = None

    knowledge_paths: dict[str, Any]
    if knowledge_path:
        knowledge_paths = {"bm25s_corpus_path": knowledge_path}
    else:
        if athena_dir:
            src = athena_dir
        else:
            from portiere._demo_data import vocabulary_dir

            src = str(vocabulary_dir())
            note = "no athena_dir/knowledge_path given — used the bundled demo vocabulary subset"
        knowledge_paths = build_knowledge_layer(
            athena_path=src,
            output_path=str(work / "index"),
            backend="bm25s",
            vocabularies=vocabularies,
        )

    config = PortiereConfig(
        offline=True,
        local_project_dir=work / "project",
        knowledge_layer=KnowledgeLayerConfig(backend="bm25s", **knowledge_paths),
        embedding=EmbeddingConfig(provider="none"),
        reranker=RerankerConfig(provider="none", model=""),
    )
    project = portiere.init(
        name="portiere-tool-concepts",
        target_model="omop_cdm_v5.4",
        vocabularies=vocabularies,
        config=config,
    )
    normalized: list[dict | str] = [c if isinstance(c, dict) else {"code": str(c)} for c in codes]
    concept_map = project.map_concepts(codes=normalized)
    items = [
        {
            "source_code": it.source_code,
            "target_concept_id": it.target_concept_id,
            "target_concept_name": it.target_concept_name,
            "target_vocabulary_id": it.target_vocabulary_id,
            "confidence": round(float(it.confidence), 3),
            "method": it.method.value if hasattr(it.method, "value") else str(it.method),
        }
        for it in concept_map.items
    ]
    out: dict[str, Any] = {"items": items}
    if note:
        out["note"] = note
    return out


def _egress_posture(config_path: str | None = None) -> dict:
    from portiere.config import PortiereConfig
    from portiere.egress import egress_violations, endpoint_is_remote, provider_is_remote

    if config_path:
        config = PortiereConfig.from_yaml(config_path)
    else:
        config = PortiereConfig(offline=True)

    def line(kind: str, component: Any) -> dict:
        provider = getattr(component, "provider", None)
        endpoint = getattr(component, "endpoint", None)
        remote = (provider is not None and provider_is_remote(kind, provider)) or (
            endpoint_is_remote(endpoint)
        )
        return {"kind": kind, "provider": provider, "verdict": "REMOTE" if remote else "LOCAL"}

    components = [
        line("llm", config.llm),
        line("embedding", config.embedding),
        line("reranker", config.reranker),
    ]
    return {
        "offline": bool(config.offline),
        "components": components,
        "violations": egress_violations(config),
    }


def _safe(fn: Callable[..., dict]) -> Callable[..., dict]:
    """Wrap a handler so bad input yields ``{"error": ...}`` not an exception."""

    def wrapper(**kwargs: Any) -> dict:
        try:
            return fn(**kwargs)
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}

    return wrapper


PORTIERE_TOOLS: list[ToolSpec] = [
    ToolSpec(
        "portiere_list_standards",
        "List the target data standards Portiere can map to (OMOP CDM, FHIR R4, "
        "HL7 v2, OpenEHR, custom).",
        _obj({}),
        _safe(_list_standards),
    ),
    ToolSpec(
        "portiere_profile_source",
        "Profile a source data file: row/column counts and per-column stats "
        "(type, null %, distinct, example). Reads CSV/Parquet/JSON locally.",
        _obj(
            {
                "path": {"type": "string", "description": "Path to the source file."},
                "format": {"type": "string", "enum": ["csv", "parquet", "json"], "default": "csv"},
            },
            required=["path"],
        ),
        _safe(_profile_source),
    ),
    ToolSpec(
        "portiere_suggest_schema_mapping",
        "Suggest source-column → target-standard mappings (offline, pattern + "
        "lexical). Returns per-column target table/field, confidence, and routing status.",
        _obj(
            {
                "columns": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "type": {"type": "string"},
                        },
                        "required": ["name"],
                    },
                },
                "target_model": {"type": "string", "default": "omop_cdm_v5.4"},
            },
            required=["columns"],
        ),
        _safe(_suggest_schema_mapping),
    ),
    ToolSpec(
        "portiere_map_concepts",
        "Map local clinical codes to standard concepts via the knowledge layer "
        "(offline BM25). Provide a prebuilt knowledge_path or an athena_dir; if "
        "neither, the bundled demo vocabulary subset is used.",
        _obj(
            {
                "codes": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "code": {"type": "string"},
                            "description": {"type": "string"},
                        },
                        "required": ["code"],
                    },
                },
                "vocabularies": {"type": "array", "items": {"type": "string"}},
                "knowledge_path": {"type": "string"},
                "athena_dir": {"type": "string"},
            },
            required=["codes"],
        ),
        _safe(_map_concepts),
    ),
    ToolSpec(
        "portiere_egress_posture",
        "Report whether the current configuration could send data off-machine "
        "(per component: LOCAL/REMOTE) — an agent can self-verify no-egress "
        "before acting. Defaults to the offline configuration.",
        _obj({"config_path": {"type": "string", "description": "Optional portiere.yaml path."}}),
        _safe(_egress_posture),
    ),
]

_BY_NAME = {t.name: t for t in PORTIERE_TOOLS}


def get_tool(name: str) -> ToolSpec:
    """Return the :class:`ToolSpec` for ``name`` (KeyError if unknown)."""
    return _BY_NAME[name]

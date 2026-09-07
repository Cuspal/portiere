"""Pure load/save/decision helpers for the Mapping Review UI.

No Streamlit imports — these functions are testable in isolation. The
Streamlit pages call into this module rather than reaching into the
storage layer directly.

The SDK and UI share source-bound JSON snapshots and revision checks.
Legacy YAML is read without modification. A review session retains the
displayed revision until the user saves or explicitly reloads.
"""

from __future__ import annotations

from collections.abc import MutableMapping
from pathlib import Path
from typing import Literal

import yaml

from portiere.models.concept_mapping import (
    ConceptMapping,
    ConceptMappingMethod,
)
from portiere.models.mapping_policy import ConceptReviewDecision
from portiere.models.schema_mapping import MappingStatus, SchemaMapping
from portiere.storage.mapping_store import MappingStore

ReviewDecision = Literal["approve", "reject", "override"]


def list_review_sources(project_dir: Path) -> list[dict]:
    labels = {}
    for path in (project_dir / "sources").glob("*.yaml"):
        metadata = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if metadata.get("id"):
            labels[metadata["id"]] = metadata.get("name", metadata["id"])
    store = MappingStore(project_dir)
    for kind in ("schema", "concept"):
        for source_id in store.source_ids(kind):
            labels.setdefault(source_id, source_id)
    return [
        {"id": source_id, "name": name}
        for source_id, name in sorted(labels.items(), key=lambda pair: pair[1])
    ]


def _session_key(project_dir, kind, source_id):
    return f"portiere:review:{Path(project_dir).resolve()}:{kind}:{source_id}"


def load_review_session(
    session: MutableMapping, project_dir: Path, kind, source_id=None, *, reload=False
):
    key = _session_key(project_dir, kind, source_id)
    if reload or key not in session:
        session[key] = MappingStore(project_dir).load(kind, source_id)
    return session[key]


def save_review_session(session: MutableMapping, project_dir: Path, kind, source_id, mapping):
    context_source = MappingStore(project_dir).load(kind, source_id).source_id
    if mapping.source_id != context_source:
        raise ValueError("Mapping belongs to a different source than this review session.")
    path = MappingStore(project_dir).save(kind, mapping)
    session[_session_key(project_dir, kind, source_id)] = mapping
    return path


def load_schema_mapping(project_dir: Path, *, source_id: str | None = None) -> SchemaMapping:
    """Load the same source revision used by the SDK and ETL."""
    return MappingStore(project_dir).load("schema", source_id)


def save_reviewed_schema_mapping(mapping: SchemaMapping, project_dir: Path) -> Path:
    """Atomically save the current revision; reject stale edits."""
    return MappingStore(project_dir).save("schema", mapping)


# ── Decision application ─────────────────────────────────────────


def apply_user_decision(
    mapping: SchemaMapping,
    *,
    index: int,
    decision: ReviewDecision,
    target_table: str | None = None,
    target_column: str | None = None,
) -> SchemaMapping:
    """Apply a single review decision to ``mapping.items[index]``.

    Returns a new :class:`SchemaMapping` (the input is not mutated).

    Decisions:
        - ``"approve"``: status -> APPROVED, keeps AI-suggested target.
        - ``"reject"``: status -> REJECTED.
        - ``"override"``: status -> OVERRIDDEN; ``target_table`` and
          ``target_column`` overwrite the AI's choice.

    Raises:
        IndexError: ``index`` out of range.
        ValueError: unknown ``decision`` string.
    """
    if not (0 <= index < len(mapping.items)):
        raise IndexError(f"index {index} out of range for {len(mapping.items)} items")

    new_items = [item.model_copy(deep=True) for item in mapping.items]
    item = new_items[index]

    if decision == "approve":
        item.status = MappingStatus.APPROVED
    elif decision == "reject":
        item.status = MappingStatus.REJECTED
    elif decision == "override":
        item.status = MappingStatus.OVERRIDDEN
        if target_table is not None:
            item.override_target_table = target_table
        if target_column is not None:
            item.override_target_column = target_column
    else:
        raise ValueError(f"Unknown decision={decision!r}; expected approve/reject/override")

    return mapping.model_copy(update={"items": new_items})


# ── Concept mapping (Slice 5) ─────────────────────────────────────


def load_concept_mapping(project_dir: Path, *, source_id: str | None = None) -> ConceptMapping:
    """Load the same source revision used by the SDK and ETL."""
    return MappingStore(project_dir).load("concept", source_id)


def save_reviewed_concept_mapping(mapping: ConceptMapping, project_dir: Path) -> Path:
    """Atomically save the current revision; reject stale edits."""
    return MappingStore(project_dir).save("concept", mapping)


def apply_concept_decision(
    mapping: ConceptMapping,
    *,
    index: int,
    decision: ReviewDecision,
    candidate_index: int | None = None,
    target_concept_id: int | None = None,
    target_concept_name: str | None = None,
    target_vocabulary_id: str | None = None,
    reviewer_note: str | None = None,
) -> ConceptMapping:
    """Apply a single review decision to ``mapping.items[index]``.

    Decisions:
        - ``"approve"``: keeps the AI-suggested target; method -> AUTO.
        - ``"reject"``: method -> UNMAPPED.
        - ``"override"``: either pick a different candidate by index, or
          supply a free-form ``target_concept_id``. The override is
          persisted along with an optional ``reviewer_note`` in
          ``item.provenance``.
    """
    if not (0 <= index < len(mapping.items)):
        raise IndexError(f"index {index} out of range for {len(mapping.items)} items")

    new_items = [item.model_copy(deep=True) for item in mapping.items]
    item = new_items[index]

    if decision == "approve":
        item.method = ConceptMappingMethod.AUTO
        item.review_decision = ConceptReviewDecision.APPROVED
    elif decision == "reject":
        item.mark_unmapped()
    elif decision == "override":
        if candidate_index is not None:
            if not 0 <= candidate_index < len(item.candidates):
                raise ValueError("Select an available candidate index.")
            cand = item.candidates[candidate_index]
            item.target_concept_id = cand.concept_id
            item.target_concept_name = cand.concept_name
            item.target_vocabulary_id = cand.vocabulary_id
            item.target_domain_id = cand.domain_id
        elif target_concept_id is not None and target_concept_id > 0:
            item.target_concept_id = target_concept_id
            if target_concept_name is not None:
                item.target_concept_name = target_concept_name
            if target_vocabulary_id is not None:
                item.target_vocabulary_id = target_vocabulary_id
        else:
            raise ValueError("Select a candidate or a positive target concept ID.")
        if item.target_concept_id is None or item.target_concept_id <= 0:
            raise ValueError("Select a positive target concept ID.")
        item.method = ConceptMappingMethod.OVERRIDE
        item.review_decision = ConceptReviewDecision.OVERRIDDEN
        if reviewer_note is not None:
            item.provenance = {**(item.provenance or {}), "reviewer_note": reviewer_note}
    else:
        raise ValueError(f"Unknown decision={decision!r}; expected approve/reject/override")

    return mapping.model_copy(update={"items": new_items})


def sort_by_confidence_ascending(mapping: ConceptMapping) -> list[int]:
    """Return item indices sorted by ``confidence`` ascending.

    Surfaces the lowest-confidence mappings first — the items that most
    need human attention.
    """
    return sorted(range(len(mapping.items)), key=lambda i: mapping.items[i].confidence)

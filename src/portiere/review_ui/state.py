"""Pure load/save/decision helpers for the Mapping Review UI.

No Streamlit imports — these functions are testable in isolation. The
Streamlit pages call into this module rather than reaching into the
storage layer directly.

Persistence rule (locked in the v0.3.1 plan): reviewed mappings are
written to ``<project_dir>/schema_mappings/schema_mapping_reviewed.json``
next to the original ``schema_mapping.yaml``. The original is never
modified.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

import yaml

from portiere.models.concept_mapping import (
    ConceptCandidate,
    ConceptMapping,
    ConceptMappingItem,
    ConceptMappingMethod,
)
from portiere.models.schema_mapping import MappingStatus, SchemaMapping, SchemaMappingItem

ReviewDecision = Literal["approve", "reject", "override"]


# ── Paths ─────────────────────────────────────────────────────────


def _schema_dir(project_dir: Path) -> Path:
    return Path(project_dir) / "schema_mappings"


def _original_schema_path(project_dir: Path) -> Path:
    return _schema_dir(project_dir) / "schema_mapping.yaml"


def _reviewed_schema_path(project_dir: Path) -> Path:
    return _schema_dir(project_dir) / "schema_mapping_reviewed.json"


# ── Load ──────────────────────────────────────────────────────────


def load_schema_mapping(project_dir: Path) -> SchemaMapping:
    """Load the schema mapping for review.

    Preference order:
    1. ``schema_mapping_reviewed.json`` (if a prior session persisted edits).
    2. ``schema_mapping.yaml`` (the original AI output).
    3. Empty mapping if neither exists.
    """
    reviewed = _reviewed_schema_path(project_dir)
    if reviewed.exists():
        data = json.loads(reviewed.read_text())
        items_data = data.get("items", [])
        return SchemaMapping(items=[SchemaMappingItem(**it) for it in items_data])

    original = _original_schema_path(project_dir)
    if not original.exists():
        return SchemaMapping(items=[])

    items_data = yaml.safe_load(original.read_text()) or []
    return SchemaMapping(items=[SchemaMappingItem(**it) for it in items_data])


# ── Save ──────────────────────────────────────────────────────────


def save_reviewed_schema_mapping(mapping: SchemaMapping, project_dir: Path) -> Path:
    """Persist reviewed mapping to ``schema_mapping_reviewed.json``.

    Returns the written path. The original ``schema_mapping.yaml`` is
    never touched.
    """
    schema_dir = _schema_dir(project_dir)
    schema_dir.mkdir(parents=True, exist_ok=True)
    out = _reviewed_schema_path(project_dir)
    payload = {"items": [item.model_dump(mode="json") for item in mapping.items]}
    out.write_text(json.dumps(payload, indent=2))
    return out


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

    return SchemaMapping(items=new_items)


# ── Concept mapping (Slice 5) ─────────────────────────────────────


def _concept_dir(project_dir: Path) -> Path:
    return Path(project_dir) / "concept_mappings"


def _original_concept_path(project_dir: Path) -> Path:
    return _concept_dir(project_dir) / "concept_mapping.yaml"


def _reviewed_concept_path(project_dir: Path) -> Path:
    return _concept_dir(project_dir) / "concept_mapping_reviewed.json"


def load_concept_mapping(project_dir: Path) -> ConceptMapping:
    """Load the concept mapping for review.

    Same preference order as ``load_schema_mapping``:
    reviewed JSON > original YAML > empty.
    """
    reviewed = _reviewed_concept_path(project_dir)
    if reviewed.exists():
        data = json.loads(reviewed.read_text())
        return ConceptMapping(
            items=[
                ConceptMappingItem(
                    **{
                        **it,
                        "candidates": [ConceptCandidate(**c) for c in it.get("candidates", [])],
                    }
                )
                for it in data.get("items", [])
            ]
        )

    original = _original_concept_path(project_dir)
    if not original.exists():
        return ConceptMapping(items=[])

    raw = yaml.safe_load(original.read_text()) or []
    return ConceptMapping(
        items=[
            ConceptMappingItem(
                **{**it, "candidates": [ConceptCandidate(**c) for c in it.get("candidates", [])]}
            )
            for it in raw
        ]
    )


def save_reviewed_concept_mapping(mapping: ConceptMapping, project_dir: Path) -> Path:
    """Persist reviewed concept mapping; original YAML untouched."""
    out_dir = _concept_dir(project_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = _reviewed_concept_path(project_dir)
    payload = {"items": [item.model_dump(mode="json") for item in mapping.items]}
    out.write_text(json.dumps(payload, indent=2))
    return out


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
    elif decision == "reject":
        item.method = ConceptMappingMethod.UNMAPPED
    elif decision == "override":
        item.method = ConceptMappingMethod.OVERRIDE
        if candidate_index is not None and 0 <= candidate_index < len(item.candidates):
            cand = item.candidates[candidate_index]
            item.target_concept_id = cand.concept_id
            item.target_concept_name = cand.concept_name
            item.target_vocabulary_id = cand.vocabulary_id
            item.target_domain_id = cand.domain_id
        elif target_concept_id is not None:
            item.target_concept_id = target_concept_id
            if target_concept_name is not None:
                item.target_concept_name = target_concept_name
            if target_vocabulary_id is not None:
                item.target_vocabulary_id = target_vocabulary_id
        if reviewer_note is not None:
            item.provenance = {**(item.provenance or {}), "reviewer_note": reviewer_note}
    else:
        raise ValueError(f"Unknown decision={decision!r}; expected approve/reject/override")

    return ConceptMapping(items=new_items)


def sort_by_confidence_ascending(mapping: ConceptMapping) -> list[int]:
    """Return item indices sorted by ``confidence`` ascending.

    Surfaces the lowest-confidence mappings first — the items that most
    need human attention.
    """
    return sorted(range(len(mapping.items)), key=lambda i: mapping.items[i].confidence)

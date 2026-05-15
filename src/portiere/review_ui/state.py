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

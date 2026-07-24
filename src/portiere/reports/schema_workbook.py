"""Schema-mapping workbook — the OMOP "working document", generated.

One sheet per target table. Rows are ALL fields of the target standard —
mapped rows carry source/confidence/status; unmapped rows are the explicit
work queue (``UNMAPPED``), never silent blanks. Spec columns (required, type,
vocabulary, description) come from the standard YAML Portiere validates
against, so they cannot drift from the CDM version in use.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from portiere.reports._xlsx import (
    add_toc,
    autosize_columns,
    fill_status_cell,
    provenance_lines,
    require_openpyxl,
    safe_sheet_name,
    style_header_row,
)
from portiere.standards import STANDARDS_DIR

if TYPE_CHECKING:
    from portiere.models.schema_mapping import SchemaMapping

_HEADER = [
    "Reviewed",
    "OMOP Field",
    "Spec",
    "Data Type",
    "Vocabulary",
    "Description",
    "Source Table",
    "Source Column",
    "Confidence",
    "Status",
    "Notes",
]

_REVIEWED_STATUSES = {"approved", "overridden", "rejected"}


def _load_standard(standard: str) -> dict[str, Any]:
    path = STANDARDS_DIR / f"{standard}.yaml"
    if not path.exists():
        raise FileNotFoundError(
            f"Standard definition not found: {path}. "
            f"Available: {sorted(p.stem for p in STANDARDS_DIR.glob('*.yaml'))}"
        )
    return yaml.safe_load(path.read_text())


def build_schema_workbook(
    mapping: SchemaMapping,
    out: str | Path,
    *,
    standard: str = "omop_cdm_v5.4",
) -> Path:
    """Write the schema-mapping working workbook and return its path."""
    openpyxl = require_openpyxl()
    std = _load_standard(standard)
    entities: dict[str, Any] = std.get("entities", {})

    # Index mapping items by (target_table, target_column), honoring overrides.
    by_target: dict[tuple[str, str], Any] = {}
    extra_targets: set[str] = set()
    for item in mapping.items:
        table = (item.effective_target_table or "").lower()
        column = (item.effective_target_column or "").lower()
        if table:
            by_target[(table, column)] = item
            if table not in entities:
                extra_targets.add(table)

    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    used: set[str] = {"TOC", "Summary"}
    toc_entries: list[tuple[str, str]] = [("Summary", "Summary")]
    summary_rows: list[list[Any]] = []

    # Only emit sheets for tables that are mapping targets OR carry required
    # fields — a full standard dump would bury the working set.
    target_tables = sorted(
        {t for (t, _c) in by_target} | {t for t in entities if t in {tt for tt, _ in by_target}}
    ) or sorted(entities)

    for table in target_tables:
        fields: dict[str, Any] = (entities.get(table, {}) or {}).get("fields", {}) or {}
        sheet = safe_sheet_name(table, used)
        ws = wb.create_sheet(sheet)
        ws.append([f"{std.get('name', standard)} — {table.upper()}"])
        ws.append(_HEADER)

        n_required = n_required_mapped = n_mapped = n_reviewed = 0
        listed: set[str] = set()

        def _write_row(sheet_ws: Any, field: str, meta: dict[str, Any], item: Any) -> None:
            nonlocal n_required, n_required_mapped, n_mapped, n_reviewed
            required = bool(meta.get("required", False))
            status = (
                str(getattr(item, "status", "")).replace("MappingStatus.", "")
                if item
                else "UNMAPPED"
            )
            if hasattr(item, "status") and hasattr(item.status, "value"):
                status = item.status.value
            reviewed = "yes" if status in _REVIEWED_STATUSES else ""
            if required:
                n_required += 1
                if item:
                    n_required_mapped += 1
            if item:
                n_mapped += 1
                if reviewed:
                    n_reviewed += 1
            sheet_ws.append(
                [
                    reviewed,
                    field,
                    "M" if required else "O",
                    str(meta.get("type", "")),
                    str(meta.get("vocabulary", "") or ""),
                    str(meta.get("description", "") or ""),
                    getattr(item, "source_table", "") if item else "",
                    getattr(item, "source_column", "") if item else "",
                    round(float(getattr(item, "confidence", 0.0)), 2) if item else None,
                    status,
                    "",
                ]
            )
            fill_status_cell(
                sheet_ws.cell(row=sheet_ws.max_row, column=_HEADER.index("Status") + 1), status
            )

        # Standard fields in YAML order (required first for review ergonomics)
        ordered = sorted(fields.items(), key=lambda kv: (not kv[1].get("required", False),))
        for field, meta in ordered:
            _write_row(ws, field, meta or {}, by_target.get((table, field.lower())))
            listed.add(field.lower())
        # Mapped targets not present in the standard definition (custom fields)
        for (t, c), item in sorted(by_target.items()):
            if t == table and c not in listed:
                _write_row(ws, c, {"description": "(not in standard definition)"}, item)

        style_header_row(ws, len(_HEADER), row=2)
        autosize_columns(ws)
        toc_entries.append((table, sheet))
        summary_rows.append(
            [
                table,
                n_required,
                n_required_mapped,
                round(100 * n_required_mapped / n_required, 1) if n_required else 100.0,
                n_mapped,
                n_reviewed,
            ]
        )

    # ── Summary ──
    summ = wb.create_sheet("Summary", 0)
    summ.append(
        [
            "Target table",
            "Required fields",
            "Required mapped",
            "Required coverage %",
            "Fields mapped",
            "Reviewed",
        ]
    )
    for row in summary_rows:
        summ.append(row)
    style_header_row(summ, 6)
    autosize_columns(summ)
    for i, line in enumerate(provenance_lines({"Standard": standard})):
        summ.cell(row=summ.max_row + 3 + i, column=1, value=line)

    add_toc(wb, toc_entries)
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out)
    return out

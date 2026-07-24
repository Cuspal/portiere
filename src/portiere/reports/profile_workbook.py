"""Profile workbook — White-Rabbit-parity data-scan deliverable, generated.

Sheets: TOC · Table Overview · Field Overview (with numeric stats) · one
value-frequency sheet per source (top values with counts, truncation labeled).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from portiere.reports._xlsx import (
    add_percent_databar,
    add_toc,
    autosize_columns,
    provenance_lines,
    require_openpyxl,
    safe_sheet_name,
    style_header_row,
)

if TYPE_CHECKING:
    from portiere.quality.profile_report import SourceProfile

_FIELD_HEADER = [
    "System",
    "Table",
    "Field",
    "Type",
    "Present %",
    "N missing",
    "N distinct",
    "Top value",
    "Top %",
    "Len min",
    "Len max",
    "Example",
    "Min",
    "Max",
    "Mean",
    "Std",
]


def build_profile_workbook(
    profiles: list[SourceProfile],
    out: str | Path,
    *,
    title: str = "Data Profile",
) -> Path:
    """Write the profile workbook and return its path."""
    openpyxl = require_openpyxl()
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    used: set[str] = {"TOC", "Table Overview", "Field Overview"}

    # ── Table Overview ──
    to = wb.create_sheet("Table Overview")
    to.append(["System", "Table", "N rows", "N fields", "Avg completeness %"])
    for sp in profiles:
        to.append([sp.system, sp.source, sp.row_count, sp.column_count, sp.avg_completeness_pct])
    style_header_row(to, 5)
    add_percent_databar(to, "E", 2, to.max_row)
    autosize_columns(to)

    # ── Field Overview ──
    fo = wb.create_sheet("Field Overview")
    fo.append(_FIELD_HEADER)
    for sp in profiles:
        for c in sp.columns:
            fo.append(
                [
                    sp.system,
                    sp.source,
                    c.name,
                    c.col_type,
                    c.present_pct,
                    c.n_missing,
                    c.n_distinct,
                    c.top_value,
                    c.top_pct,
                    c.min_len,
                    c.max_len,
                    c.example,
                    c.num_min,
                    c.num_max,
                    c.num_mean,
                    c.num_std,
                ]
            )
    style_header_row(fo, len(_FIELD_HEADER))
    add_percent_databar(fo, "E", 2, fo.max_row)
    autosize_columns(fo)

    # ── Per-source value-frequency sheets ──
    toc_entries = [("Table Overview", "Table Overview"), ("Field Overview", "Field Overview")]
    for sp in profiles:
        name = safe_sheet_name(f"values {sp.source}", used)
        vs = wb.create_sheet(name)
        vs.append([f"Value frequencies — {sp.source} (top 10 per field; full data in source)"])
        header_row: list[str] = []
        for c in sp.columns:
            header_row.extend([c.name, "Frequency"])
        vs.append(header_row)
        max_vals = max((len(c.top_values or []) for c in sp.columns), default=0)
        for i in range(max_vals):
            row: list[object] = []
            for c in sp.columns:
                tv = c.top_values or []
                if i < len(tv):
                    row.extend([tv[i][0], tv[i][1]])
                else:
                    row.extend([None, None])
            vs.append(row)
        style_header_row(vs, max(len(header_row), 1), row=2)
        autosize_columns(vs)
        toc_entries.append((f"{sp.source} values ({sp.system or 'source'})", name))

    add_toc(wb, toc_entries)
    toc = wb["TOC"]
    for i, line in enumerate(provenance_lines({"Report": title})):
        toc.cell(row=len(toc_entries) + 5 + i, column=1, value=line)

    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out)
    return out

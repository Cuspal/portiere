"""Concept-mapping workbook — OMOP source_to_concept_map deliverable, generated.

Sheets: Source_to_Concept_Map (standard columns + confidence/method, color-
coded) · vocabulary · status (generated per-vocabulary routing counts — no
hand-typed "Awating Review") · readme (provenance).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

from portiere.reports._xlsx import (
    autosize_columns,
    fill_status_cell,
    provenance_lines,
    require_openpyxl,
    style_header_row,
)

if TYPE_CHECKING:
    from portiere.models.concept_mapping import ConceptMapping

_S2CM_HEADER = [
    "source_code",
    "source_concept_id",
    "source_vocabulary_id",
    "source_code_description",
    "target_concept_id",
    "target_vocabulary_id",
    "valid_start_date",
    "valid_end_date",
    "invalid_reason",
    # Portiere extensions (right of the standard columns)
    "target_concept_name",
    "target_domain_id",
    "confidence",
    "method",
]

_END_OF_TIME = "2099-12-31"


def build_concept_workbook(
    mapping: ConceptMapping,
    out: str | Path,
    *,
    vocab_reference: str = "",
    vocab_version: str | None = None,
) -> Path:
    """Write the concept-mapping workbook and return its path."""
    openpyxl = require_openpyxl()
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    today = date.today().isoformat()
    vocab_version = vocab_version or f"v1 {today}"

    # ── Source_to_Concept_Map ──
    ws = wb.create_sheet("Source_to_Concept_Map")
    ws.append(_S2CM_HEADER)
    method_col = _S2CM_HEADER.index("method") + 1
    for item in mapping.items:
        mapped = bool(item.target_concept_id)
        method = item.method.value if hasattr(item.method, "value") else str(item.method)
        ws.append(
            [
                item.source_code,
                0,  # source_concept_id: 0 for local codes per OMOP convention
                item.source_column or "",
                item.source_description or "",
                item.target_concept_id or 0,
                item.target_vocabulary_id or "",
                today if mapped else "",
                _END_OF_TIME if mapped else "",
                "",
                item.target_concept_name or "",
                item.target_domain_id or "",
                round(float(item.confidence), 3),
                method,
            ]
        )
        fill_status_cell(ws.cell(row=ws.max_row, column=method_col), method)
    style_header_row(ws, len(_S2CM_HEADER))
    autosize_columns(ws)

    # ── vocabulary ──
    vocabs = sorted({item.source_column or "(unspecified)" for item in mapping.items})
    vw = wb.create_sheet("vocabulary")
    vw.append(
        [
            "vocabulary_id",
            "vocabulary_name",
            "vocabulary_reference",
            "vocabulary_version",
            "vocabulary_concept_id",
        ]
    )
    for v in vocabs:
        vw.append([v, v, vocab_reference, vocab_version, 0])
    style_header_row(vw, 5)
    autosize_columns(vw)

    # ── status (generated) ──
    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for item in mapping.items:
        v = item.source_column or "(unspecified)"
        method = item.method.value if hasattr(item.method, "value") else str(item.method)
        counts[v]["n"] += 1
        counts[v][method] += 1
        if not item.target_concept_id:
            counts[v]["no_target"] += 1
    st = wb.create_sheet("status")
    st.append(
        [
            "source_vocabulary_id",
            "n codes",
            "auto",
            "review",
            "manual",
            "unmapped/no target",
            "auto %",
        ]
    )
    for v in vocabs:
        c = counts[v]
        st.append(
            [
                v,
                c["n"],
                c["auto"],
                c["review"],
                c["manual"],
                c["no_target"],
                round(100 * c["auto"] / c["n"], 1) if c["n"] else 0.0,
            ]
        )
    style_header_row(st, 7)
    autosize_columns(st)

    # ── readme ──
    rd = wb.create_sheet("readme")
    lines = [
        "Master Concept Mapping — generated deliverable",
        "",
        *provenance_lines(),
        "",
        "Sheet guide:",
        "  Source_to_Concept_Map — OMOP-standard columns; Portiere extensions "
        "(target_concept_name, target_domain_id, confidence, method) to the right.",
        "  vocabulary — one row per source vocabulary (source_column).",
        "  status — generated routing counts per vocabulary; auto = accepted at "
        "threshold, review = ranked candidates need a human, manual = no confident "
        "candidate.",
        "",
        "Method legend: auto (green) · review (amber) · manual (red).",
        "Re-generate: portiere workbook concepts --mapping <concept_mapping.json> -o <out.xlsx>",
        "This file is generated — edit mappings in the review workflow, not here.",
    ]
    for line in lines:
        rd.append([line])
    autosize_columns(rd, max_width=100)

    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out)
    return out

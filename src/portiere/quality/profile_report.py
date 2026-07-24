"""
Profile-report export — aggregate one or more source profiles into a
self-contained HTML report plus long/summary CSVs.

This is an *export layer* over the per-source profiles produced by the compute
engines (``engine.profile``). It does not compute statistics itself; it maps the
enriched profile dicts to small display models and renders them.

Public API:
- ``build_source_profile(profile, *, source, system="")`` -> ``SourceProfile``
- ``export_profile_report(profiles, out_dir, ...)`` -> ``dict[str, Path]``
"""

from __future__ import annotations

import csv
import html
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ColumnProfile:
    """One column's display row in the profile report."""

    name: str
    present_pct: float
    n_missing: int
    n_distinct: int
    top_value: str
    top_pct: float
    min_len: int
    max_len: int
    example: str
    # Numeric distribution stats (None for non-numeric columns)
    num_min: float | None = None
    num_max: float | None = None
    num_mean: float | None = None
    num_std: float | None = None
    # Column type + raw top values (value, count) for value-frequency sheets
    col_type: str = ""
    top_values: list[tuple[str, int]] | None = None


@dataclass
class SourceProfile:
    """One source's display block in the profile report."""

    source: str
    system: str
    row_count: int
    column_count: int
    avg_completeness_pct: float
    columns: list[ColumnProfile]


def build_source_profile(
    profile: dict,
    *,
    source: str,
    system: str = "",
    top_value_maxlen: int = 40,
    example_maxlen: int = 40,
) -> SourceProfile:
    """Map one enriched engine ``profile`` dict to a :class:`SourceProfile`.

    Args:
        profile: Output of ``engine.profile(df)`` (with the ``present_count`` /
            ``min_len`` / ``max_len`` / ``example`` enrichment keys). ``row_count``
            should already be the exact full-data count.
        source: Source name/label (e.g. file stem).
        system: Optional free-text system label (e.g. "HIS"); may be "".
        top_value_maxlen: Truncate the displayed top value to this many chars.
        example_maxlen: Truncate the displayed example value to this many chars.
    """
    row_count = int(profile.get("row_count", 0))
    raw_cols = profile.get("columns", [])
    column_count = int(profile.get("column_count", len(raw_cols)))

    cols: list[ColumnProfile] = []
    present_total = 0
    for pc in raw_cols:
        name = pc["name"]
        present = int(pc.get("present_count", 0))
        present_total += present

        # Prefer present-based top value/count (share stays <= 100%); fall back
        # to the raw ``top_values`` (computed over all values) when absent.
        if "present_top_value" in pc:
            top_value = str(pc.get("present_top_value", ""))[:top_value_maxlen]
            top_count = int(pc.get("present_top_count", 0))
        else:
            top_values = pc.get("top_values") or []
            if top_values:
                first = top_values[0]
                count_key = "len" if "len" in first else "count"
                top_value = str(first.get(name, ""))[:top_value_maxlen]
                top_count = int(first.get(count_key, 0))
            else:
                top_value, top_count = "", 0
        top_pct = round(100 * top_count / present, 1) if present else 0.0

        # Distinct over present values (matches the report), falling back to the
        # raw column cardinality when the present-based count is unavailable.
        n_distinct = int(pc.get("present_n_distinct", pc.get("n_unique", 0)))

        # Raw top values as (value, count) pairs for value-frequency sheets.
        # Engine shapes differ: polars uses {col: value, "len": count},
        # pandas/spark use {col: value, "count": count}.
        raw_tv: list[tuple[str, int]] = []
        for tv in pc.get("top_values") or []:
            count_key = "len" if "len" in tv else "count"
            raw_tv.append((str(tv.get(name, "")), int(tv.get(count_key, 0))))

        cols.append(
            ColumnProfile(
                name=name,
                present_pct=round(100 * present / row_count, 1) if row_count else 0.0,
                n_missing=row_count - present,
                n_distinct=n_distinct,
                top_value=top_value,
                top_pct=top_pct,
                min_len=int(pc.get("min_len", 0)),
                max_len=int(pc.get("max_len", 0)),
                example=str(pc.get("example", ""))[:example_maxlen],
                num_min=pc.get("num_min"),
                num_max=pc.get("num_max"),
                num_mean=pc.get("num_mean"),
                num_std=pc.get("num_std"),
                col_type=str(pc.get("type", "")),
                top_values=raw_tv or None,
            )
        )

    total_cells = row_count * column_count
    avg = round(100 * present_total / total_cells, 1) if total_cells else 0.0
    return SourceProfile(source, system, row_count, column_count, avg, cols)


# ── HTML rendering ────────────────────────────────────────────────────────────


def _cbar(p: float) -> str:
    """Render a coloured completeness bar (green >=90, amber >=50, else red)."""
    c = "#2e7d32" if p >= 90 else "#f9a825" if p >= 50 else "#c62828"
    return (
        '<div style="background:#eee;border-radius:3px;overflow:hidden">'
        f'<div style="width:{p}%;background:{c};color:#fff;font-size:11px;'
        f'padding:1px 4px">{p}%</div></div>'
    )


_STYLE = (
    '<meta charset="utf-8"><style>'
    "body{font:13px -apple-system,Segoe UI,sans-serif;margin:24px;color:#222}"
    "h1{font-size:22px}h2{margin-top:28px;border-bottom:2px solid #ddd;padding-bottom:4px}"
    "table{border-collapse:collapse;width:100%;margin:8px 0}"
    "th,td{border:1px solid #e2e2e2;padding:5px 8px;text-align:left;vertical-align:top}"
    "th{background:#f6f6f6}td.num{text-align:right;font-variant-numeric:tabular-nums}"
    "code{background:#f2f2f2;padding:1px 4px;border-radius:3px}</style>"
)


def _render_html(profiles: Sequence[SourceProfile], title: str, subtitle: str | None = None) -> str:
    parts = [_STYLE, f"<h1>{html.escape(title)}</h1>"]
    if subtitle:
        parts.append(f"<p>{html.escape(subtitle)}</p>")

    # Summary table
    parts.append(
        "<table><tr><th>System</th><th>Source</th><th>Rows</th><th>Cols</th>"
        "<th>Avg completeness</th></tr>"
    )
    for sp in profiles:
        parts.append(
            f"<tr><td>{html.escape(sp.system)}</td>"
            f"<td><code>{html.escape(sp.source)}</code></td>"
            f"<td class=num>{sp.row_count:,}</td><td class=num>{sp.column_count}</td>"
            f"<td style='min-width:120px'>{_cbar(sp.avg_completeness_pct)}</td></tr>"
        )
    parts.append("</table>")

    # Per-source sections
    for sp in profiles:
        syslabel = f"{html.escape(sp.system)}, " if sp.system else ""
        parts.append(
            f"<h2>{html.escape(sp.source)} "
            f"<span style='font-weight:400;color:#888'>"
            f"({syslabel}{sp.row_count:,} rows)</span></h2>"
        )
        parts.append(
            "<table><tr><th>Column</th><th>Present</th><th>Missing</th><th>Distinct</th>"
            "<th>Top value (share)</th><th>Len</th><th>Example</th></tr>"
        )
        for col in sp.columns:
            parts.append(
                f"<tr><td><code>{html.escape(col.name)}</code></td>"
                f"<td style='min-width:110px'>{_cbar(col.present_pct)}</td>"
                f"<td class=num>{col.n_missing:,}</td><td class=num>{col.n_distinct:,}</td>"
                f"<td>{html.escape(col.top_value)} "
                f"<span style='color:#888'>({col.top_pct}%)</span></td>"
                f"<td class=num>{col.min_len}-{col.max_len}</td>"
                f"<td style='color:#555'>{html.escape(col.example)}</td></tr>"
            )
        parts.append("</table>")
    return "\n".join(parts)


# ── CSV rendering ─────────────────────────────────────────────────────────────

_LONG_HEADER = [
    "system",
    "source",
    "column",
    "n_rows",
    "present_pct",
    "n_missing",
    "n_distinct",
    "top_value",
    "top_pct",
    "min_len",
    "max_len",
    "example",
]
_SUMMARY_HEADER = ["system", "source", "rows", "cols", "avg_completeness_pct"]


def _long_rows(profiles: Iterable[SourceProfile]) -> Iterator[list]:
    for sp in profiles:
        for c in sp.columns:
            yield [
                sp.system,
                sp.source,
                c.name,
                sp.row_count,
                c.present_pct,
                c.n_missing,
                c.n_distinct,
                c.top_value,
                c.top_pct,
                c.min_len,
                c.max_len,
                c.example,
            ]


def _summary_rows(profiles: Iterable[SourceProfile]) -> Iterator[list]:
    for sp in profiles:
        yield [sp.system, sp.source, sp.row_count, sp.column_count, sp.avg_completeness_pct]


def _write_csv(path: Path, header: Sequence[str], rows: Iterable[Sequence]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)


# ── Public exporter ───────────────────────────────────────────────────────────


def export_profile_report(
    profiles: Sequence[SourceProfile],
    out_dir: str | Path,
    *,
    title: str = "Data Profile",
    subtitle: str | None = None,
    formats: Sequence[str] = ("html", "csv", "summary"),
    html_name: str = "data_profile.html",
    csv_name: str = "data_profile.csv",
    summary_name: str = "data_profile_summary.csv",
) -> dict[str, Path]:
    """Write the requested report artifacts and return their paths.

    Args:
        profiles: Source profiles (from :func:`build_source_profile`).
        out_dir: Directory to write into (created if missing).
        title: Report H1 title.
        subtitle: Optional paragraph under the title.
        formats: Any of ``"html"``, ``"csv"`` (long, per-column),
            ``"summary"`` (per-source). Only requested files are written.

    Returns:
        Mapping of the written kinds to their :class:`~pathlib.Path`.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}

    if "html" in formats:
        p = out / html_name
        p.write_text(_render_html(profiles, title, subtitle), encoding="utf-8")
        written["html"] = p
    if "csv" in formats:
        p = out / csv_name
        _write_csv(p, _LONG_HEADER, _long_rows(profiles))
        written["csv"] = p
    if "summary" in formats:
        p = out / summary_name
        _write_csv(p, _SUMMARY_HEADER, _summary_rows(profiles))
        written["summary"] = p

    return written

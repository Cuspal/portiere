"""portiere profile-report — export an aggregated data-profile report.

Reads one or more source files, profiles each with the (default polars) engine,
and writes a self-contained HTML report plus long/summary CSVs — the same shape
as ``portiere_workspace`` data profiles, but from the library.
"""

from __future__ import annotations

import glob
from pathlib import Path

import click

from portiere.engines import get_engine
from portiere.quality.profile_report import build_source_profile, export_profile_report


@click.command(name="profile-report")
@click.argument("sources", nargs=-1, required=True)
@click.option("-o", "--out", "out_dir", required=True, help="Output directory.")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["csv", "parquet", "json"]),
    default="csv",
    show_default=True,
    help="Source read format.",
)
@click.option(
    "--sample-n",
    type=int,
    default=None,
    help="Profile column stats from a sample of N rows (row count stays exact).",
)
@click.option("--title", default="Data Profile", show_default=True, help="Report title.")
@click.option(
    "--system",
    default="",
    help="Optional system label applied to all given sources (e.g. HIS).",
)
@click.option(
    "--no-empty-as-missing",
    is_flag=True,
    default=False,
    help="Count only true nulls as missing (default: empty/whitespace also missing).",
)
def profile_report_command(
    sources: tuple[str, ...],
    out_dir: str,
    fmt: str,
    sample_n: int | None,
    title: str,
    system: str,
    no_empty_as_missing: bool,
) -> None:
    """Profile SOURCES and export an aggregated HTML + CSV data-profile report."""
    engine = get_engine("polars")
    empty_as_missing = not no_empty_as_missing

    # Expand globs; keep literal path if a pattern matches nothing.
    paths: list[str] = []
    for pattern in sources:
        paths.extend(sorted(glob.glob(pattern)) or [pattern])

    profiles = []
    for path in paths:
        if not Path(path).exists():
            raise click.ClickException(f"Source not found: {path}")
        df = engine.read_source(path, format=fmt)
        row_count = engine.count(df)
        df_profile = engine.sample(df, sample_n) if sample_n else df
        profile = engine.profile(df_profile, empty_as_missing=empty_as_missing)
        profile["row_count"] = row_count
        profiles.append(build_source_profile(profile, source=Path(path).stem, system=system))

    written = export_profile_report(profiles, out_dir, title=title)
    for kind, out_path in written.items():
        click.echo(f"wrote {kind}: {out_path}")

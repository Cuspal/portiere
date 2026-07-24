"""portiere workbook — generate the Excel deliverables of the mapping workflow.

Subcommands mirror the artifacts OMOP conversion teams hand-build today:

* ``portiere workbook profile`` — White-Rabbit-parity data-scan workbook
* ``portiere workbook schema`` — per-target-table mapping working document
* ``portiere workbook concepts`` — OMOP source_to_concept_map + status

Requires the ``xlsx`` extra: ``pip install "portiere-health[xlsx]"``.
"""

from __future__ import annotations

import glob
import json
from pathlib import Path

import click


@click.group(name="workbook")
def workbook_group() -> None:
    """Generate Excel deliverable workbooks (requires the xlsx extra)."""


@workbook_group.command("profile")
@click.argument("sources", nargs=-1, required=True)
@click.option("-o", "--out", "out_path", required=True, help="Output .xlsx path.")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["csv", "parquet", "json"]),
    default="csv",
    show_default=True,
    help="Source read format.",
)
@click.option("--sample-n", type=int, default=None, help="Sample N rows for column stats.")
@click.option("--title", default="Data Profile", show_default=True)
@click.option("--system", default="", help="System label applied to all sources.")
@click.option(
    "--scrub-phi",
    is_flag=True,
    default=False,
    help="Scrub detected PHI from examples/values before writing.",
)
def workbook_profile(
    sources: tuple[str, ...],
    out_path: str,
    fmt: str,
    sample_n: int | None,
    title: str,
    system: str,
    scrub_phi: bool,
) -> None:
    """Profile SOURCES and write the data-scan workbook."""
    from portiere.engines import get_engine
    from portiere.quality.profile_report import build_source_profile
    from portiere.reports import build_profile_workbook
    from portiere.stages.stage1_ingest import _scrub_profile_values

    engine = get_engine("polars")
    paths: list[str] = []
    for pattern in sources:
        paths.extend(sorted(glob.glob(pattern)) or [pattern])

    profiles = []
    for p in paths:
        if not Path(p).exists():
            raise click.ClickException(f"Source not found: {p}")
        df = engine.read_source(p, format=fmt)
        row_count = engine.count(df)
        df_profile = engine.sample(df, sample_n) if sample_n else df
        profile = engine.profile(df_profile)
        profile["row_count"] = row_count
        if scrub_phi:
            _scrub_profile_values(profile)
        profiles.append(build_source_profile(profile, source=Path(p).stem, system=system))

    written = build_profile_workbook(profiles, out_path, title=title)
    click.echo(f"wrote workbook: {written}")


def _load_items_payload(path: str) -> list[dict]:
    """Load an items list from a mapping JSON ({'items': [...]}) or YAML list."""
    text = Path(path).read_text()
    if path.endswith((".yaml", ".yml")):
        import yaml

        data = yaml.safe_load(text) or []
        return data if isinstance(data, list) else data.get("items", [])
    data = json.loads(text)
    return data.get("items", []) if isinstance(data, dict) else data


@workbook_group.command("schema")
@click.option(
    "--mapping",
    "mapping_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="schema_mapping_reviewed.json (or original schema_mapping.yaml).",
)
@click.option("-o", "--out", "out_path", required=True, help="Output .xlsx path.")
@click.option("--standard", default="omop_cdm_v5.4", show_default=True)
def workbook_schema(mapping_path: str, out_path: str, standard: str) -> None:
    """Write the schema-mapping working workbook from a persisted mapping."""
    from portiere.models.schema_mapping import SchemaMapping, SchemaMappingItem
    from portiere.reports import build_schema_workbook

    items = [SchemaMappingItem(**it) for it in _load_items_payload(mapping_path)]
    written = build_schema_workbook(SchemaMapping(items=items), out_path, standard=standard)
    click.echo(f"wrote workbook: {written}")


@workbook_group.command("concepts")
@click.option(
    "--mapping",
    "mapping_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Concept mapping JSON ({'items': [...]} — e.g. concept_map.model_dump_json()).",
)
@click.option("-o", "--out", "out_path", required=True, help="Output .xlsx path.")
@click.option("--vocab-reference", default="", help="vocabulary_reference column value.")
def workbook_concepts(mapping_path: str, out_path: str, vocab_reference: str) -> None:
    """Write the concept-mapping workbook from a persisted mapping."""
    from portiere.models.concept_mapping import ConceptMapping, ConceptMappingItem
    from portiere.reports import build_concept_workbook

    items = [ConceptMappingItem(**it) for it in _load_items_payload(mapping_path)]
    written = build_concept_workbook(
        ConceptMapping(items=items), out_path, vocab_reference=vocab_reference
    )
    click.echo(f"wrote workbook: {written}")

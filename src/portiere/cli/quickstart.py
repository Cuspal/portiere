"""``portiere quickstart`` — fully offline end-to-end demo (Slice 5 Task 5.6).

Runs the OMOP mapping pipeline against the bundled demo data:
~20 synthetic patients with deliberately messy column names, plus a
small Athena-format ICD-10-CM/LOINC/RxNorm vocabulary subset. No
network access required at any point.

Each pipeline stage is wrapped in try/except so a single failure
(e.g., missing ``[quality]`` extra) doesn't block the rest of the
demo. The manifest still emits — it records what succeeded, with
each missing stage flagged in the printed output.

Output goes to ``~/.cache/portiere/quickstart_run/`` by default.
Override with ``--output-dir`` or the ``PORTIERE_QUICKSTART_DIR``
environment variable.
"""

from __future__ import annotations

import os
from pathlib import Path

import click


def _default_output_dir() -> Path:
    env = os.environ.get("PORTIERE_QUICKSTART_DIR")
    if env:
        return Path(env).expanduser()
    return Path.home() / ".cache" / "portiere" / "quickstart_run"


@click.command(name="quickstart")
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(),
    default=None,
    help=(
        "Directory for quickstart artifacts. "
        "Default: ~/.cache/portiere/quickstart_run/ "
        "(or $PORTIERE_QUICKSTART_DIR if set)."
    ),
)
def quickstart_command(output_dir: str | None) -> None:
    """Run a full Portiere pipeline against bundled demo data.

    Demonstrates ingest → schema-map → concept-map → ETL → validate
    end-to-end, producing schema and concept mappings, ETL output, a
    validation report, and a reproducibility manifest. Fully offline.
    """
    out = Path(output_dir).expanduser() if output_dir else _default_output_dir()
    out.mkdir(parents=True, exist_ok=True)

    from portiere._demo_data import demo_data_dir, vocabulary_dir

    click.echo("=" * 64)
    click.echo("Portiere quickstart — fully offline demo")
    click.echo("=" * 64)
    click.echo(f"Demo data:  {demo_data_dir()}")
    click.echo(f"Vocabulary: {vocabulary_dir()}")
    click.echo(f"Output:     {out}")
    click.echo()

    success: dict[str, str] = {}
    skipped: dict[str, str] = {}

    # ── Knowledge layer: BM25s built from bundled vocab ───────────────
    knowledge_paths: dict = {}
    try:
        from portiere.knowledge import build_knowledge_layer

        knowledge_paths = build_knowledge_layer(
            athena_path=str(vocabulary_dir()),
            output_path=str(out / "knowledge_index"),
            backend="bm25s",
            vocabularies=["ICD10CM", "LOINC", "RxNorm"],
        )
        success["knowledge_layer"] = "built (bm25s)"
    except Exception as exc:
        skipped["knowledge_layer"] = f"build failed: {exc}"

    # ── Project setup ────────────────────────────────────────────────
    import portiere
    from portiere.config import EmbeddingConfig, KnowledgeLayerConfig, PortiereConfig

    config = PortiereConfig(
        local_project_dir=out,
        knowledge_layer=KnowledgeLayerConfig(backend="bm25s", **knowledge_paths),
        # Disable embeddings — the demo runs in pattern + BM25 mode, no SapBERT download
        embedding=EmbeddingConfig(provider="none"),
    )

    project = portiere.init(
        name="portiere-quickstart",
        target_model="omop_cdm_v5.4",
        vocabularies=["ICD10CM", "LOINC", "RxNorm"],
        config=config,
    )

    with project:
        # ── Stage 1: ingest ─────────────────────────────────────────
        try:
            source = project.add_source(str(demo_data_dir() / "synthetic_conditions.csv"))
            success["ingest"] = (
                f"{len(source.get('columns', []))} columns, {source.get('row_count', '?')} rows"
            )
        except Exception as exc:
            skipped["ingest"] = f"failed: {exc}"
            source = {"path": str(demo_data_dir() / "synthetic_conditions.csv")}

        # ── Stage 2: schema mapping ─────────────────────────────────
        schema_map = None
        try:
            schema_map = project.map_schema(source)
            n_items = len(schema_map.items)
            n_auto = sum(1 for i in schema_map.items if i.status == "auto_accepted")
            success["schema"] = f"{n_items} columns mapped ({n_auto} auto-accepted)"
        except Exception as exc:
            skipped["schema"] = f"failed: {type(exc).__name__}: {exc}"

        # ── Stage 3: concept mapping ────────────────────────────────
        try:
            concept_map = project.map_concepts(source=source)
            success["concept"] = f"{len(concept_map.items)} concepts mapped"
        except Exception as exc:
            skipped["concept"] = f"failed: {type(exc).__name__}: {exc}"

        # ── Stage 4: ETL ────────────────────────────────────────────
        if schema_map is not None:
            try:
                etl_out = out / "etl_output"
                project.run_etl(source, output_dir=str(etl_out))
                success["etl"] = f"output -> {etl_out}"
            except Exception as exc:
                skipped["etl"] = f"failed: {type(exc).__name__}: {exc}"
        else:
            skipped["etl"] = "skipped (schema mapping unavailable)"

        # ── Stage 5: validate ───────────────────────────────────────
        try:
            from portiere.quality.validator import GXValidator  # noqa: F401

            try:
                project.validate(output_path=str(out / "etl_output"))
                success["validate"] = "ran"
            except Exception as exc:
                skipped["validate"] = f"failed: {type(exc).__name__}: {exc}"
        except ImportError:
            skipped["validate"] = "skipped (install portiere-health[quality] for Stage 5)"

    # ── Summary ──────────────────────────────────────────────────────
    click.echo()
    click.echo("Pipeline summary:")
    click.echo("-" * 64)
    for stage in ("knowledge_layer", "ingest", "schema", "concept", "etl", "validate"):
        if stage in success:
            click.echo(f"  ✓ {stage:18s} {success[stage]}")
        elif stage in skipped:
            click.echo(f"  ⏭ {stage:18s} {skipped[stage]}")
    click.echo()

    # ── Locate the manifest ─────────────────────────────────────────
    runs_dir = out / "portiere-quickstart" / "runs"
    manifests = list(runs_dir.glob("*/manifest.lock.json")) if runs_dir.exists() else []
    if manifests:
        manifest = manifests[-1]
        click.echo(f"Manifest:  {manifest}")
        click.echo(f"Replay:    portiere replay {manifest}")
    else:
        click.echo("(no manifest produced — pipeline never reached the recorder)")

    click.echo()
    click.echo("Notes:")
    click.echo("  - Demo uses bundled ICD-10-CM / LOINC / RxNorm subsets only (no network).")
    click.echo(
        "  - For SNOMED CT (free with registration), see "
        "docs/documentations/15-vocabulary-setup.md."
    )
    click.echo(
        "  - Real Portiere usage: build a knowledge layer from your own "
        "Athena export, then portiere.init() with that path."
    )

"""``portiere quickstart`` — fully offline end-to-end demo (Slice 5 Task 5.6).

Runs the OMOP mapping pipeline against three synthetic patients and a
small bundled vocabulary. Demographics use explicit bundled review decisions;
diagnosis concepts are proposals for review, separate from demographic ETL.

Every required stage must succeed for exit code zero. A failed stage is
reported and its dependent stages are skipped. Optional models are disabled.

Output goes to ``~/.cache/portiere/quickstart_run/`` by default.
Override with ``--output-dir`` or the ``PORTIERE_QUICKSTART_DIR``
environment variable.
"""

from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import click

# Review decisions apply only to the bundled synthetic fixture. Zero-valued
# demographic concepts explicitly represent unknown values in OMOP.
_DEMO_PERSON_COLUMNS = {
    "patient_id": "person_id",
    "gender": "gender_concept_id",
    "birth_year": "year_of_birth",
    "birth_month": "month_of_birth",
    "birth_day": "day_of_birth",
    "birth_date": "birth_datetime",
    "race": "race_concept_id",
    "ethnicity": "ethnicity_concept_id",
    "source_id": "person_source_value",
}


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
    failures: dict[str, str] = {}
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
        failures["knowledge_layer"] = f"build failed: {exc}"

    # ── Project setup ────────────────────────────────────────────────
    import portiere
    from portiere.config import (
        EmbeddingConfig,
        EngineConfig,
        KnowledgeLayerConfig,
        LLMConfig,
        PortiereConfig,
        RerankerConfig,
    )
    from portiere.models.concept_mapping import ConceptMapping

    config = PortiereConfig(
        local_project_dir=out,
        storage="local",
        api_key=None,
        offline=True,
        engine=EngineConfig(type="polars"),
        knowledge_layer=KnowledgeLayerConfig(backend="bm25s", **knowledge_paths),
        embedding=EmbeddingConfig(provider="none"),
        reranker=RerankerConfig(provider="none"),
        llm=LLMConfig(provider="none"),
    )

    project = portiere.init(
        name="portiere-quickstart",
        target_model="omop_cdm_v5.4",
        vocabularies=["ICD10CM", "LOINC", "RxNorm"],
        config=config,
    )

    with project:
        # ── Stage 1: ingest ─────────────────────────────────────────
        source = None
        try:
            source = project.add_source(str(demo_data_dir() / "quickstart.csv"))
            success["ingest"] = (
                f"{len(source.get('columns', []))} columns, {source.get('row_count', '?')} rows"
            )
        except Exception as exc:
            failures["ingest"] = str(exc)

        # ── Stage 2: schema mapping ─────────────────────────────────
        schema_map = None
        if source is not None:
            try:
                candidate_map = project.map_schema(source)
                for item in candidate_map.items:
                    item.reject()
                for column, target in _DEMO_PERSON_COLUMNS.items():
                    candidate_map.get_item(column).approve("person", target)
                project.save_schema_mapping(candidate_map)
                schema_map = candidate_map
                success["schema"] = "9 bundled reviewed demographic mappings"
            except Exception as exc:
                failures["schema"] = f"{type(exc).__name__}: {exc}"
        else:
            skipped["schema"] = "ingest failed"

        # ── Stage 3: concept mapping ────────────────────────────────
        if source is not None and "knowledge_layer" in success:
            try:
                concept_map = project.map_concepts(source=source)
                success["concept"] = f"{len(concept_map.items)} proposals saved for review"
            except Exception as exc:
                failures["concept"] = f"{type(exc).__name__}: {exc}"
        else:
            skipped["concept"] = "ingest or knowledge layer failed"

        # ── Stage 4: ETL ────────────────────────────────────────────
        etl_out = out / "etl_output" / uuid4().hex
        if schema_map is not None and source is not None:
            try:
                result = project.run_etl(
                    source,
                    output_dir=str(etl_out),
                    schema_mapping=schema_map,
                    concept_mapping=ConceptMapping(items=[]),
                )
                if not result.success:
                    raise ValueError("; ".join(result.errors) or "ETL reported failure")
                success["etl"] = f"{result.total_rows_written} person rows -> {etl_out}"
            except Exception as exc:
                failures["etl"] = f"{type(exc).__name__}: {exc}"
        else:
            skipped["etl"] = "skipped (schema mapping unavailable)"

        # ── Stage 5: validate ───────────────────────────────────────
        if "etl" in success:
            try:
                report = project.validate(output_path=str(etl_out))
                if not report["all_passed"]:
                    raise ValueError(f"Validation checks failed; inspect {etl_out}")
                success["validate"] = f"{report['total_tables']} table(s) passed"
            except ImportError as exc:
                failures["validate"] = (
                    f'{exc}; install "portiere-health[polars,quality]" for validation'
                )
            except Exception as exc:
                failures["validate"] = f"{type(exc).__name__}: {exc}"
        else:
            skipped["validate"] = "ETL failed; no current output to validate"

    # ── Summary ──────────────────────────────────────────────────────
    click.echo()
    click.echo("Pipeline summary:")
    click.echo("-" * 64)
    for stage in ("knowledge_layer", "ingest", "schema", "concept", "etl", "validate"):
        if stage in success:
            click.echo(f"  PASS {stage:18s} {success[stage]}")
        elif stage in failures:
            click.echo(f"  FAIL {stage:18s} {failures[stage]}")
        elif stage in skipped:
            click.echo(f"  SKIP {stage:18s} {skipped[stage]}")
    click.echo()

    # ── Locate the manifest ─────────────────────────────────────────
    runs_dir = out / "portiere-quickstart" / "runs"
    manifests = list(runs_dir.glob("*/manifest.lock.json")) if runs_dir.exists() else []
    if manifests:
        manifest = max(manifests, key=lambda path: path.stat().st_mtime_ns)
        click.echo(f"Manifest:  {manifest}")
        click.echo(f"Replay:    portiere replay {manifest}")
    else:
        click.echo("(no manifest produced — pipeline never reached the recorder)")

    click.echo()
    click.echo("Notes:")
    click.echo("  - ETL writes reviewed synthetic demographics to person.csv.")
    click.echo("  - Diagnosis concept proposals are saved for review, not applied to ETL.")
    click.echo("  - Demo uses bundled ICD-10-CM / LOINC / RxNorm subsets only (no network).")
    click.echo(
        "  - For SNOMED CT (free with registration), see "
        "docs/documentations/15-vocabulary-setup.md."
    )
    click.echo(
        "  - Real Portiere usage: build a knowledge layer from your own "
        "Athena export, then portiere.init() with that path."
    )
    if failures or skipped:
        raise click.ClickException("Quickstart incomplete; resolve FAIL/SKIP stages above.")

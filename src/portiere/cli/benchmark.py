"""``portiere benchmark`` CLI — run published Portiere benchmarks.

Subcommands:

* ``portiere benchmark athena-icd-snomed`` — ICD-10-CM → SNOMED accuracy
  against the OHDSI Athena ``CONCEPT_RELATIONSHIP`` gold standard.
"""

from __future__ import annotations

from pathlib import Path

import click


@click.group(name="benchmark")
def benchmark_group() -> None:
    """Run published Portiere accuracy benchmarks."""


@benchmark_group.command("athena-icd-snomed")
@click.option(
    "--athena-dir",
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Path to your extracted Athena vocabulary export.",
)
@click.option(
    "--test-set",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help=(
        "Path to a gold_test_set.csv with column 'icd10cm_concept_id'. "
        "Optional: if omitted, the bundled set is used (or one is "
        "generated in-memory from the Athena export with seed=42)."
    ),
)
@click.option(
    "--backend",
    type=click.Choice(["bm25s", "faiss", "hybrid"], case_sensitive=False),
    default="hybrid",
    help=(
        "Retrieval backend: bm25s (sparse only), faiss (dense only), "
        "or hybrid (BM25 + FAISS via RRF — recommended)."
    ),
)
@click.option(
    "--stratify-by",
    type=click.Choice(["domain"], case_sensitive=False),
    default=None,
    help=(
        "Stratify the in-memory test set by 'domain'. Only applied when "
        "--test-set is not provided. Default: uniform random sampling."
    ),
)
@click.option(
    "--out",
    type=click.Path(),
    default="bench_run.json",
    help="Output JSON path. Each backend's run is appended to the 'runs' array.",
)
@click.option(
    "--athena-release-date",
    default=None,
    help=(
        "Athena release date string to record in the output. "
        "If omitted, inferred from the mtime of CONCEPT.csv."
    ),
)
@click.option(
    "--test-set-size",
    type=int,
    default=1000,
    help="Held-out test-set size when generating in-memory (default 1000).",
)
def athena_icd_snomed(
    athena_dir: str,
    test_set: str | None,
    backend: str,
    stratify_by: str | None,
    out: str,
    athena_release_date: str | None,
    test_set_size: int,
) -> None:
    """Run the ICD-10-CM → SNOMED concept mapping benchmark for one backend.

    Run with each of --backend bm25s, --backend faiss, --backend hybrid
    to populate the 3-row ablation table. Each invocation appends (or
    replaces) its row in the output JSON's "runs" array.
    """
    import datetime as _dt

    from portiere.benchmarks.athena_icd_snomed.runner import (
        append_run_to_expected_results,
        run_benchmark,
    )

    if test_set is None:
        bundled = (
            Path(__file__).resolve().parents[1]
            / "benchmarks"
            / "athena_icd_snomed"
            / "gold_test_set.csv"
        )
        if bundled.exists():
            test_set = str(bundled)
            click.echo(f"Using bundled test set: {test_set}")
        else:
            click.echo(
                "No bundled test set found — generating one in-memory from "
                f"the Athena export (seed=42, n={test_set_size})."
            )

    if athena_release_date is None:
        concept_csv = Path(athena_dir) / "CONCEPT.csv"
        if concept_csv.exists():
            mtime = _dt.datetime.fromtimestamp(concept_csv.stat().st_mtime)
            athena_release_date = mtime.strftime("%Y-%m-%d")
        else:
            athena_release_date = "user-supplied"

    click.echo(f"Athena dir:           {athena_dir}")
    click.echo(f"Athena release date:  {athena_release_date}")
    click.echo(f"Backend:              {backend}")
    if test_set:
        click.echo(f"Test set:             {test_set}")
    click.echo("Running benchmark...")
    click.echo()

    result = run_benchmark(
        athena_dir,
        test_set_path=test_set,
        test_set_size=test_set_size,
        backend=backend,
        stratify_by=stratify_by,
    )
    append_run_to_expected_results(
        result,
        backend=backend,
        athena_release_date=athena_release_date,
        out=out,
    )

    click.echo()
    click.echo(f"  N:     {result.n}")
    click.echo(f"  top-1: {result.top_1:.3f}")
    click.echo(f"  top-5: {result.top_5:.3f}")
    click.echo(f"  top-10:{result.top_10:.3f}")
    click.echo(f"  MRR:   {result.mrr:.3f}")
    click.echo()
    click.echo(f"Run appended to: {out}")

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
        "Path to gold_test_set.csv with column 'icd10cm_concept_id'. "
        "Defaults to the bundled test set in benchmarks/athena_icd_snomed/."
    ),
)
@click.option(
    "--out",
    type=click.Path(),
    default="bench_run.json",
    help="Output JSON path for the per-run results.",
)
@click.option(
    "--athena-release-date",
    default="user-supplied",
    help="Athena release date string to record in the output.",
)
def athena_icd_snomed(
    athena_dir: str,
    test_set: str | None,
    out: str,
    athena_release_date: str,
) -> None:
    """Run the ICD-10-CM → SNOMED concept mapping benchmark.

    Outputs a JSON file with top-1 / top-5 / top-10 / MRR. The
    repository's ``benchmarks/athena_icd_snomed/expected_results.json``
    contains the published reference numbers for comparison.
    """
    from benchmarks.athena_icd_snomed.runner import (
        run_benchmark,
        write_expected_results,
    )

    if test_set is None:
        # Default: the held-out test set committed in the repo
        repo_root = Path(__file__).resolve().parents[3]
        test_set = str(repo_root / "benchmarks" / "athena_icd_snomed" / "gold_test_set.csv")
        if not Path(test_set).exists():
            raise click.UsageError(
                f"Default test set not found at {test_set}. "
                f"Pass --test-set explicitly or run scripts/build_benchmark_test_set.py first."
            )

    click.echo(f"Athena dir: {athena_dir}")
    click.echo(f"Test set:   {test_set}")
    click.echo("Running benchmark...")

    result = run_benchmark(athena_dir, test_set_path=test_set)
    write_expected_results(result, athena_release_date=athena_release_date, out=out)

    click.echo("")
    click.echo(f"  N:     {result.n}")
    click.echo(f"  top-1: {result.top_1:.3f}")
    click.echo(f"  top-5: {result.top_5:.3f}")
    click.echo(f"  top-10:{result.top_10:.3f}")
    click.echo(f"  MRR:   {result.mrr:.3f}")
    click.echo("")
    click.echo(f"Full results: {out}")

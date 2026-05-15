"""``portiere replay <manifest>`` CLI command."""

from __future__ import annotations

import click

from portiere.repro.replay import ManifestReplayError, auto_replay, replay


@click.command(name="replay")
@click.argument("manifest", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(),
    default=None,
    help="Directory hint for the replay run output (advisory).",
)
@click.option(
    "--auto-replay",
    "auto_replay_flag",
    is_flag=True,
    default=False,
    help=(
        "Re-run each recorded pipeline stage and compare outputs to the manifest "
        "within ±1% tolerance. Exits non-zero on any drift beyond tolerance. "
        "Stages whose dependencies are unavailable (e.g., LLM not configured) "
        "are recorded as 'unavailable' and do not fail the run."
    ),
)
def replay_command(manifest: str, output_dir: str | None, auto_replay_flag: bool) -> None:
    """Replay a Portiere pipeline from a manifest.lock.json.

    Without ``--auto-replay``: validates that all referenced artifacts exist
    and match their recorded sha256, then reconstructs the project with the
    manifest's recorded configuration. Pipeline ops can be re-invoked from
    there.

    With ``--auto-replay``: additionally re-runs each recorded stage and
    compares outputs to the manifest within tolerance bands documented in
    docs/reproducibility.md.

    Outputs are NOT promised to be byte-identical to the original run
    (LLM sampling, BM25 ties).
    """
    try:
        if auto_replay_flag:
            report = auto_replay(manifest)
        else:
            result = replay(manifest, output_dir=output_dir)
    except ManifestReplayError as e:
        click.echo(f"Replay failed: {e}", err=True)
        raise click.exceptions.Exit(1) from e

    if auto_replay_flag:
        click.echo(f"Manifest: {report.manifest_path}")
        click.echo(f"Stages attempted: {len(report.per_stage)}")
        for s in report.per_stage:
            status = "PASS" if s.passed is True else "FAIL" if s.passed is False else "UNAVAILABLE"
            drift = f"  drift={s.drift_pct:.2f}%" if s.drift_pct is not None else ""
            reason = f"  ({s.reason})" if s.reason else ""
            click.echo(f"  [{status:<11}] {s.stage}{drift}{reason}")
        click.echo(f"Result: {'PASS' if report.passed else 'FAIL'}")
        if not report.passed:
            raise click.exceptions.Exit(1)
        return

    click.echo(f"Project:        {result['project_name']}")
    click.echo(f"Target model:   {result['target_model']}")
    click.echo(f"Replay run id:  {result['replay_run_id']}")
    if result.get("source_path"):
        click.echo(f"Source attached: {result['source_path']}")
    if result.get("output_dir"):
        click.echo(f"Output dir hint: {result['output_dir']}")

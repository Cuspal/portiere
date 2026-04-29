"""``portiere replay <manifest>`` CLI command (Slice 4 Task 4.5)."""

from __future__ import annotations

import click

from portiere.repro.replay import ManifestReplayError, replay


@click.command(name="replay")
@click.argument("manifest", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(),
    default=None,
    help="Directory hint for the replay run output (advisory).",
)
def replay_command(manifest: str, output_dir: str | None) -> None:
    """Replay a Portiere pipeline from a manifest.lock.json.

    Validates that all referenced artifacts (source data, vocabulary
    files) exist and match their recorded sha256, then reconstructs the
    project with the manifest's recorded configuration. Pipeline ops
    can be re-invoked from there.

    Outputs are NOT promised to be byte-identical to the original run
    (LLM sampling, BM25 ties).
    """
    try:
        result = replay(manifest, output_dir=output_dir)
    except ManifestReplayError as e:
        click.echo(f"Replay failed: {e}", err=True)
        raise click.exceptions.Exit(1) from e

    click.echo(f"Project:        {result['project_name']}")
    click.echo(f"Target model:   {result['target_model']}")
    click.echo(f"Replay run id:  {result['replay_run_id']}")
    if result.get("source_path"):
        click.echo(f"Source attached: {result['source_path']}")
    if result.get("output_dir"):
        click.echo(f"Output dir hint: {result['output_dir']}")

"""``portiere export`` CLI — write generated FHIR resources to Bundle or NDJSON."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click


@click.command(name="export")
@click.option(
    "--input",
    "input_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="JSON file containing a list of FHIR resource dicts.",
)
@click.option(
    "--format",
    "fmt",
    required=True,
    type=click.Choice(["bundle", "ndjson"], case_sensitive=False),
    help="Output shape: 'bundle' (single transaction JSON) or 'ndjson' (one file per type).",
)
@click.option(
    "--out",
    required=True,
    type=click.Path(),
    help=(
        "Output path. For --format bundle, a single .json file. "
        "For --format ndjson, a directory (created if absent) of <ResourceType>.ndjson files."
    ),
)
@click.option(
    "--fhir-profile",
    type=click.Choice(["us-core-6.1.0"], case_sensitive=False),
    default=None,
    help=(
        "Optional: validate resources against this profile BEFORE writing. "
        "On any error-severity failure, exit non-zero and write nothing."
    ),
)
def export_cmd(input_path: str, fmt: str, out: str, fhir_profile: str | None) -> None:
    """Export generated FHIR resources to a Bundle or NDJSON shape."""
    resources = json.loads(Path(input_path).read_text())
    if not isinstance(resources, list):
        click.echo(
            f"Error: --input file must contain a JSON array of resources, "
            f"got {type(resources).__name__}",
            err=True,
        )
        sys.exit(2)

    if fhir_profile == "us-core-6.1.0":
        from portiere.quality.fhir_profile.us_core import validate_against_us_core

        click.echo("Validating against us-core-6.1.0...")
        report = validate_against_us_core(resources)
        if not report.passed:
            click.echo(
                f"Validation failed: {len(report.failures)} error(s). "
                "No output written. "
                "Run `portiere validate --fhir-profile us-core-6.1.0` for details.",
                err=True,
            )
            for f in report.failures[:5]:
                click.echo(
                    f"  [{f.invariant_id}] {f.resource_type}#{f.resource_index}: {f.message}",
                    err=True,
                )
            if len(report.failures) > 5:
                click.echo(f"  ... and {len(report.failures) - 5} more", err=True)
            sys.exit(1)
        click.echo(f"  OK: {report.total_resources} resources validated")

    if fmt.lower() == "bundle":
        from portiere.export.fhir.bundle import to_transaction_bundle

        bundle = to_transaction_bundle(resources)
        Path(out).write_text(json.dumps(bundle, indent=2))
        click.echo(f"Wrote {len(bundle['entry'])}-entry transaction Bundle -> {out}")
    else:
        from portiere.export.fhir.ndjson import to_ndjson_files

        files = to_ndjson_files(resources, out_dir=Path(out))
        click.echo(f"Wrote {len(files)} NDJSON file(s) -> {out}/")

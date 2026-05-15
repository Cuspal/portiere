"""portiere validate — FHIR profile validation CLI command."""

from __future__ import annotations

import json
import sys

import click


@click.command("validate")
@click.option(
    "--fhir-profile",
    default=None,
    help="FHIR profile to validate against, e.g. 'us-core-6.1.0'.",
)
@click.option(
    "--input",
    "input_path",
    required=True,
    type=click.Path(exists=True),
    help="JSON file containing a list of FHIR resource dicts.",
)
def validate_command(fhir_profile: str | None, input_path: str) -> None:
    """Validate FHIR resources against a profile."""
    if fhir_profile is None:
        raise click.UsageError("--fhir-profile is required.")

    _SUPPORTED = {"us-core-6.1.0", "mcode-2.0.0"}
    if fhir_profile not in _SUPPORTED:
        raise click.BadParameter(
            f"Unsupported profile {fhir_profile!r}. Supported: {', '.join(sorted(_SUPPORTED))}",
            param_hint="--fhir-profile",
        )

    with open(input_path, encoding="utf-8") as fh:
        raw = json.load(fh)
    resources: list[dict] = raw if isinstance(raw, list) else [raw]

    if fhir_profile == "us-core-6.1.0":
        from portiere.quality.fhir_profile.us_core import validate_against_us_core

        report = validate_against_us_core(resources)
    else:
        from portiere.quality.fhir_profile.mcode import validate_against_mcode

        report = validate_against_mcode(resources)

    click.echo(f"Profile: {report.profile}")
    click.echo(f"Resources: {report.total_resources}  Skipped: {len(report.skipped)}")
    click.echo(f"Failures: {len(report.failures)}")
    click.echo(f"Result: {'PASS' if report.passed else 'FAIL'}")

    if not report.passed:
        for f in report.failures:
            click.echo(
                f"  [{f.severity.upper()}] {f.resource_type}[{f.resource_index}] "
                f"{f.invariant_id}: {f.message}",
                err=True,
            )
        sys.exit(1)

"""portiere doctor — environment and egress-posture preflight.

Reports where data *can* go under the current configuration. With
``--assert-no-egress`` it exits non-zero if any configured provider would send
data off the local machine — the executable form of the "PHI never leaves your
machine" guarantee, runnable by a security reviewer.
"""

from __future__ import annotations

import click

from portiere.egress import egress_violations, endpoint_is_remote, provider_is_remote


def _posture_line(kind: str, provider: str, endpoint: str | None, model: str | None) -> str:
    remote = provider_is_remote(kind, provider) or endpoint_is_remote(endpoint)
    verdict = "REMOTE" if remote else "LOCAL"
    detail = f"provider={provider}"
    if model:
        detail += f" model={model}"
    if endpoint:
        detail += f" endpoint={endpoint}"
    return f"  {kind:<10} {verdict:<7} {detail}"


@click.command(name="doctor")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="Path to portiere.yaml (default: same config discovery as portiere.init()).",
)
@click.option(
    "--assert-no-egress",
    is_flag=True,
    default=False,
    help="Exit 1 if any configured provider would send data off-machine.",
)
def doctor_command(config_path: str | None, assert_no_egress: bool) -> None:
    """Report environment and egress posture for the current configuration."""
    from portiere.config import PortiereConfig

    if config_path:
        config = PortiereConfig.from_yaml(config_path)
        click.echo(f"config: {config_path}")
    else:
        config = PortiereConfig.discover()
        click.echo("config: (SDK discovery: project/user config + PORTIERE_* environment)")

    click.echo(f"engine: {config.engine.type}")
    click.echo(f"model cache: {config.model_cache_dir}")
    click.echo(f"offline mode: {'ASSERTED' if config.offline else 'not asserted'}")

    click.echo("\nEgress posture:")
    click.echo(_posture_line("llm", config.llm.provider, config.llm.endpoint, config.llm.model))
    click.echo(
        _posture_line(
            "embedding",
            config.embedding.provider,
            config.embedding.endpoint,
            config.embedding.model,
        )
    )
    click.echo(
        _posture_line(
            "reranker",
            config.reranker.provider,
            config.reranker.endpoint,
            config.reranker.model if config.reranker.provider != "none" else None,
        )
    )
    if config.knowledge_layer is not None:
        from portiere.egress import _KNOWLEDGE_URL_FIELDS

        kl = config.knowledge_layer
        remote_urls = [
            f"{f}={getattr(kl, f)!r}"
            for f in _KNOWLEDGE_URL_FIELDS
            if getattr(kl, f, None) and endpoint_is_remote(str(getattr(kl, f)))
        ]
        verdict = "REMOTE" if remote_urls else "LOCAL"
        detail = f"backend={kl.backend}" + (" " + " ".join(remote_urls) if remote_urls else "")
        click.echo(f"  {'knowledge':<10} {verdict:<7} {detail}")

    violations = egress_violations(config)
    if violations:
        click.echo("\nOff-machine data paths:")
        for v in violations:
            click.echo(f"  ✗ {v}")
    else:
        click.echo("\nNo off-machine data paths configured — fully local stack.")

    if assert_no_egress:
        if violations:
            click.echo("\nASSERTION FAILED: configuration permits data egress.")
            raise SystemExit(1)
        click.echo("\nAssertion passed: no egress possible with this configuration.")


__all__ = ["doctor_command"]

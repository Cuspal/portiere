"""Portiere CLI — top-level Click group with subcommands.

Subcommands:

* ``portiere models`` — manage embedding/reranker model cache (Slice 1)
* ``portiere replay`` — replay a pipeline from a manifest (Slice 4)

The package's ``portiere`` console script points here.
"""

from __future__ import annotations

import click

from portiere.cli.models import models
from portiere.cli.replay import replay_command


@click.group()
def cli() -> None:
    """Portiere — AI-powered clinical data mapping SDK."""


cli.add_command(models)
cli.add_command(replay_command)


__all__ = ["cli", "models", "replay_command"]

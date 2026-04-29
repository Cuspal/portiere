"""Portiere CLI — top-level Click group with subcommands.

Subcommands:

* ``portiere models`` — manage embedding/reranker model cache
* ``portiere replay`` — replay a pipeline from a manifest (Slice 4)

The package's ``portiere`` console script points here.

Note on imports: we import the subcommand objects under aliases
(``_models_group`` etc.) so we don't shadow the auto-set submodule
attributes on the ``portiere.cli`` package — otherwise
``patch("portiere.cli.models.X")`` resolves to the Click group instead
of the module, which broke test_cli on Python 3.10 due to import-order
sensitivity.
"""

from __future__ import annotations

import click

from portiere.cli.models import models as _models_group
from portiere.cli.replay import replay_command as _replay_command


@click.group()
def cli() -> None:
    """Portiere — AI-powered clinical data mapping SDK."""


cli.add_command(_models_group)
cli.add_command(_replay_command)


__all__ = ["cli"]

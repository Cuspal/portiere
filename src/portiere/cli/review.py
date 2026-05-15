"""``portiere review <project-dir>`` — launch the Streamlit Mapping Review UI.

The CLI's only job is to spawn ``streamlit run app.py -- --project-dir <dir>``.
All actual UI work happens in :mod:`portiere.review_ui`.

Security note: defaults to ``127.0.0.1`` (local-only, no auth). Pass
``--host 0.0.0.0`` to expose on the LAN; the UI still has no auth, so
this is intended for demos / trusted networks only.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import click


@click.command(name="review")
@click.argument(
    "project_dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
)
@click.option(
    "--host",
    default="127.0.0.1",
    show_default=True,
    help="Address Streamlit binds to. Local-only by default; --host 0.0.0.0 exposes on the LAN.",
)
@click.option(
    "--port",
    type=int,
    default=8501,
    show_default=True,
    help="Streamlit port (default Streamlit value).",
)
def review_command(project_dir: str, host: str, port: int) -> None:
    """Launch the Streamlit Mapping Review UI against PROJECT_DIR.

    PROJECT_DIR is a Portiere project directory containing the
    ``schema_mappings/`` (and later ``concept_mappings/``) subdirectories
    written by ``project.map_schema()`` / ``project.map_concepts()``.
    """
    project_path = Path(project_dir).resolve()

    # Path to portiere.review_ui.app.py — resolved at runtime so the
    # CLI works whether portiere is installed or run from a source checkout.
    from portiere import review_ui

    app_path = Path(review_ui.__file__).parent / "app.py"

    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--server.address",
        host,
        "--server.port",
        str(port),
        "--",
        "--project-dir",
        str(project_path),
    ]

    click.echo(f"Launching review UI for {project_path}")
    click.echo(f"  http://{host}:{port}")
    if host != "127.0.0.1":
        click.echo(f"  WARNING: --host {host} exposes the UI beyond localhost (no auth).")

    proc = subprocess.Popen(cmd)
    try:
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()

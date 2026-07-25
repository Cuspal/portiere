"""portiere mcp — run the Portiere MCP server (stdio)."""

from __future__ import annotations

import click


@click.command(name="mcp")
@click.option("--name", default="portiere", show_default=True, help="MCP server name.")
def mcp_command(name: str) -> None:
    """Launch the Portiere MCP stdio server (requires the mcp extra).

    Exposes profiling, schema mapping, concept mapping, standards listing, and
    an egress-posture check as MCP tools — all offline. Point an MCP client
    (e.g. Claude Desktop) at `portiere mcp`.
    """
    from portiere.integrations.mcp import create_mcp_server

    try:
        server = create_mcp_server(name=name)
    except ImportError as exc:
        raise click.ClickException(str(exc)) from exc
    server.run()

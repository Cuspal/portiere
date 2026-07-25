"""MCP server — expose Portiere operations as Model Context Protocol tools.

Requires the ``mcp`` extra: ``pip install "portiere-health[mcp]"``.

Launch (stdio, e.g. from a Claude Desktop / MCP client config):

    portiere mcp

or programmatically:

    from portiere.integrations.mcp import create_mcp_server
    create_mcp_server().run()

Every tool runs Portiere with ``offline=True`` — the server cannot send data
off-machine.
"""

from __future__ import annotations

from typing import Any

from portiere.integrations.tools import PORTIERE_TOOLS, ToolSpec


def _require_mcp():
    try:
        from mcp.server.fastmcp import FastMCP  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "MCP integration requires the mcp extra. "
            'Install it with: pip install "portiere-health[mcp]"'
        ) from exc


def _make_handler(spec: ToolSpec):
    def handler(**kwargs: Any) -> dict:
        return spec.run(**kwargs)

    handler.__name__ = spec.name
    handler.__doc__ = spec.description
    return handler


def register_tools(server: Any) -> Any:
    """Register every Portiere tool on a FastMCP-like ``server``.

    Uses the ``@server.tool(name=..., description=...)`` decorator API. Kept
    separate from :func:`create_mcp_server` so it can be exercised without the
    ``mcp`` dependency installed.
    """
    for spec in PORTIERE_TOOLS:
        server.tool(name=spec.name, description=spec.description)(_make_handler(spec))
    return server


def create_mcp_server(name: str = "portiere") -> Any:
    """Build a FastMCP server with all Portiere tools registered."""
    _require_mcp()
    from mcp.server.fastmcp import FastMCP

    server = FastMCP(name)
    return register_tools(server)


def main() -> None:
    """Entry point for ``portiere mcp`` — run the stdio server."""
    create_mcp_server().run()


__all__ = ["create_mcp_server", "main", "register_tools"]

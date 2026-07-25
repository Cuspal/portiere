"""Tests for the MCP adapter (portiere.integrations.mcp).

The ``mcp`` SDK is not a test dependency; these tests cover the registration
logic against a minimal fake FastMCP and the import-guard message.
"""

import pytest

from portiere.integrations import mcp as mcp_mod


class _FakeFastMCP:
    """Records tools registered via the @server.tool() decorator pattern."""

    def __init__(self, name="portiere"):
        self.name = name
        self.registered: dict[str, dict] = {}

    def tool(self, name=None, description=None):
        def deco(fn):
            self.registered[name] = {"description": description, "fn": fn}
            return fn

        return deco


def test_register_tools_registers_all(monkeypatch):
    server = _FakeFastMCP()
    mcp_mod.register_tools(server)
    from portiere.integrations.tools import PORTIERE_TOOLS

    assert set(server.registered) == {t.name for t in PORTIERE_TOOLS}
    for name, entry in server.registered.items():
        assert entry["description"]


def test_registered_handler_runs(monkeypatch):
    server = _FakeFastMCP()
    mcp_mod.register_tools(server)
    out = server.registered["portiere_list_standards"]["fn"]()
    assert "standards" in out


def test_create_server_missing_mcp(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "mcp" or name.startswith("mcp."):
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ImportError, match=r"portiere-health\[mcp\]"):
        mcp_mod.create_mcp_server()


def test_mcp_cli_help():
    from click.testing import CliRunner

    from portiere.cli import cli

    res = CliRunner().invoke(cli, ["mcp", "--help"])
    assert res.exit_code == 0
    assert "MCP" in res.output

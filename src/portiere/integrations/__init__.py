"""Portiere integrations — drive the pipeline from MCP, LangChain, and dbt.

- ``portiere.integrations.tools`` — dependency-free tool core (shared by MCP + LangChain)
- ``portiere.integrations.mcp`` — MCP server (extra: ``mcp``)
- ``portiere.integrations.langchain`` — LangChain tools (extra: ``langchain``)
- ``portiere.integrations.dbt`` — generate a dbt project from mappings (no extra)
"""

from portiere.integrations.tools import PORTIERE_TOOLS, ToolSpec, get_tool

__all__ = ["PORTIERE_TOOLS", "ToolSpec", "get_tool"]

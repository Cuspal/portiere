# MCP Server

> **Runnable example:** [`27_integrations_mcp_langchain_dbt.ipynb`](https://github.com/Cuspal/portiere/blob/main/docs/notebooks_examples/27_integrations_mcp_langchain_dbt.ipynb) — all three integrations end-to-end, offline, executed with outputs.

Drive Portiere from any [Model Context Protocol](https://modelcontextprotocol.io)
client (Claude Desktop, MCP-aware agents). Every tool runs **offline** — the
server cannot send data off-machine.

```bash
pip install "portiere-health[mcp,polars]"
portiere mcp        # stdio server
```

## Tools exposed

| Tool | What it does |
|---|---|
| `portiere_list_standards` | list target standards (OMOP, FHIR, HL7 v2, OpenEHR, custom) |
| `portiere_profile_source` | profile a local CSV/Parquet/JSON — row/col counts, per-column stats |
| `portiere_suggest_schema_mapping` | source columns → target table/field, confidence, status (offline) |
| `portiere_map_concepts` | local codes → standard concepts via the knowledge layer |
| `portiere_egress_posture` | **safety** — report LOCAL/REMOTE per component so an agent can self-verify no-egress before acting |

## Wire it into Claude Desktop

Add to your MCP config (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "portiere": {
      "command": "portiere",
      "args": ["mcp"]
    }
  }
}
```

Now an agent can profile a hospital extract, propose an OMOP mapping, and map
codes — all locally, with `portiere_egress_posture` available so it can *prove*
nothing left the machine.

## Programmatic

```python
from portiere.integrations.mcp import create_mcp_server
server = create_mcp_server()
server.run()
```

The tools are the shared, dependency-free core in
`portiere.integrations.tools` (also used by the [LangChain adapter](langchain.md)) —
so behaviour is identical across frameworks.

# LangChain Tools

Expose Portiere's operations as LangChain tools for use in agents and chains.
Works with `langchain-core` alone. All tools run **offline**.

```bash
pip install "portiere-health[langchain,polars]"
```

```python
from portiere.integrations.langchain import get_langchain_tools

tools = get_langchain_tools()          # list[StructuredTool]

# e.g. a ReAct agent
from langchain.agents import create_react_agent   # your LLM setup
agent = create_react_agent(llm, tools, prompt)
```

## Tools

Same five operations as the [MCP server](mcp.md) (they share one core):
`portiere_list_standards`, `portiere_profile_source`,
`portiere_suggest_schema_mapping`, `portiere_map_concepts`,
`portiere_egress_posture`. Each returns a JSON string an agent can parse.

## Direct invocation

```python
tools = {t.name: t for t in get_langchain_tools()}

print(tools["portiere_list_standards"].invoke({}))
print(tools["portiere_suggest_schema_mapping"].invoke(
    {"columns": [{"name": "diagnosis_code"}], "target_model": "omop_cdm_v5.4"}))
print(tools["portiere_egress_posture"].invoke({}))   # verify no-egress
```

## Safety

Every tool constructs Portiere with `offline=True`; a remote-provider config is
rejected before any network call. `portiere_egress_posture` lets the agent (or
your guardrail) confirm the posture programmatically.

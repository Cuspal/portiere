"""LangChain adapter — expose Portiere operations as LangChain tools.

Requires the ``langchain`` extra: ``pip install "portiere-health[langchain]"``
(``langchain-core`` is sufficient).

    from portiere.integrations.langchain import get_langchain_tools
    tools = get_langchain_tools()          # list[StructuredTool]
    agent = create_react_agent(llm, tools)
"""

from __future__ import annotations

import json
from typing import Any

from portiere.integrations.tools import PORTIERE_TOOLS, ToolSpec


def _require_langchain():
    try:
        from langchain_core.tools import StructuredTool  # noqa: F401
        from pydantic import create_model  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "LangChain integration requires the langchain extra. "
            'Install it with: pip install "portiere-health[langchain]"'
        ) from exc


_JSON_TYPES = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
    "array": list,
    "object": dict,
}


def _args_model(spec: ToolSpec):
    """Build a pydantic args model from a ToolSpec's JSON schema."""
    from pydantic import Field, create_model

    props = spec.input_schema.get("properties", {})
    required = set(spec.input_schema.get("required", []))
    fields: dict[str, Any] = {}
    for name, sub in props.items():
        py = _JSON_TYPES.get(sub.get("type", "string"), str)
        default = sub.get("default", ... if name in required else None)
        fields[name] = (py, Field(default, description=sub.get("description", "")))
    return create_model(f"{spec.name}_Args", **fields)


def _to_structured_tool(spec: ToolSpec):
    from langchain_core.tools import StructuredTool

    def _call(**kwargs: Any) -> str:
        # LangChain passes declared-but-unset optionals as None; drop them so
        # the handler's own defaults apply.
        clean = {k: v for k, v in kwargs.items() if v is not None}
        return json.dumps(spec.run(**clean))

    return StructuredTool.from_function(
        func=_call,
        name=spec.name,
        description=spec.description,
        args_schema=_args_model(spec),
    )


def get_langchain_tools() -> list:
    """Return Portiere operations as a list of LangChain ``StructuredTool``s."""
    _require_langchain()
    return [_to_structured_tool(spec) for spec in PORTIERE_TOOLS]


__all__ = ["get_langchain_tools"]

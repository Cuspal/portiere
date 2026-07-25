"""Tests for the LangChain adapter (portiere.integrations.langchain)."""

import pytest

pytest.importorskip("langchain_core")

from portiere.integrations.langchain import get_langchain_tools


def test_returns_structured_tools():
    from langchain_core.tools import BaseTool

    tools = get_langchain_tools()
    assert len(tools) == 5
    assert all(isinstance(t, BaseTool) for t in tools)
    names = {t.name for t in tools}
    assert "portiere_list_standards" in names
    assert "portiere_egress_posture" in names


def test_tool_has_args_schema():
    tools = {t.name: t for t in get_langchain_tools()}
    profile = tools["portiere_profile_source"]
    schema = profile.args_schema.model_json_schema()
    assert "path" in schema["properties"]


def test_invoke_list_standards():
    tools = {t.name: t for t in get_langchain_tools()}
    out = tools["portiere_list_standards"].invoke({})
    assert "omop" in str(out)


def test_invoke_egress_posture_is_local():
    tools = {t.name: t for t in get_langchain_tools()}
    out = tools["portiere_egress_posture"].invoke({})
    assert "LOCAL" in str(out)
    assert "REMOTE" not in str(out)


def test_missing_langchain_gives_helpful_error(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("langchain"):
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    import importlib

    import portiere.integrations.langchain as lc

    importlib.reload(lc)
    with pytest.raises(ImportError, match=r"portiere-health\[langchain\]"):
        lc.get_langchain_tools()
    importlib.reload(lc)  # restore

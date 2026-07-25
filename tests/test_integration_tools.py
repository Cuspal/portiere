"""Tests for the dependency-free integration tool core (portiere.integrations.tools)."""

import polars as pl

from portiere.integrations.tools import PORTIERE_TOOLS, get_tool


def test_registry_shape():
    names = {t.name for t in PORTIERE_TOOLS}
    assert {
        "portiere_list_standards",
        "portiere_profile_source",
        "portiere_suggest_schema_mapping",
        "portiere_map_concepts",
        "portiere_egress_posture",
    } <= names
    for t in PORTIERE_TOOLS:
        assert t.name and t.description and callable(t.run)
        assert t.input_schema["type"] == "object"


def test_list_standards():
    out = get_tool("portiere_list_standards").run()
    assert "standards" in out
    assert any("omop" in s for s in out["standards"])


def test_profile_source(tmp_path):
    src = tmp_path / "p.csv"
    pl.DataFrame({"sex": ["M", "F", ""], "age": [30, 40, 50]}).write_csv(src)
    out = get_tool("portiere_profile_source").run(path=str(src))
    assert out["row_count"] == 3
    assert out["column_count"] == 2
    names = {c["name"] for c in out["columns"]}
    assert names == {"sex", "age"}


def test_suggest_schema_mapping_offline_omop():
    out = get_tool("portiere_suggest_schema_mapping").run(
        columns=[{"name": "diagnosis_code"}, {"name": "date_of_birth"}],
        target_model="omop_cdm_v5.4",
    )
    by_col = {m["source_column"]: m for m in out["mappings"]}
    assert by_col["diagnosis_code"]["target_table"] == "condition_occurrence"
    assert by_col["date_of_birth"]["target_table"] == "person"


def test_suggest_schema_mapping_fhir_target():
    out = get_tool("portiere_suggest_schema_mapping").run(
        columns=[{"name": "lab_code"}], target_model="fhir_r4"
    )
    assert out["mappings"][0]["target_table"] == "Observation"


def test_map_concepts_bundled_vocab():
    out = get_tool("portiere_map_concepts").run(
        codes=[{"code": "E11.9", "description": "Type 2 diabetes"}],
        vocabularies=["ICD10CM"],
    )
    assert out["items"], out
    item = out["items"][0]
    assert item["source_code"] == "E11.9"
    assert "confidence" in item and "method" in item


def test_egress_posture_local():
    out = get_tool("portiere_egress_posture").run()
    assert out["offline"] is True  # tools default to offline
    assert out["violations"] == []
    assert all(c["verdict"] == "LOCAL" for c in out["components"])


def test_tool_run_returns_error_dict_on_bad_input():
    # missing required 'path' → structured error, not an exception
    out = get_tool("portiere_profile_source").run()
    assert "error" in out

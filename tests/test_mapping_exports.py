"""Executable artifact paths must not promote pending or rejected mappings."""

import csv
import io

import pytest
import yaml

from portiere.artifacts.code_generator import CodeGenerator
from portiere.engines import PolarsEngine
from portiere.integrations.dbt import build_dbt_project
from portiere.models.concept_mapping import ConceptMapping
from portiere.models.schema_mapping import SchemaMapping
from portiere.runner import ETLRunner
from portiere.stages.stage4_transform import generate_etl


@pytest.fixture
def records():
    return [
        {
            "source_code": "accepted",
            "source_column": "code",
            "target_concept_id": 1001,
            "method": "auto",
        },
        {
            "source_code": "pending",
            "source_column": "code",
            "target_concept_id": 1002,
            "method": "review",
        },
        {
            "source_code": "rejected",
            "source_column": "code",
            "target_concept_id": 1003,
            "method": "auto",
            "review_decision": "rejected",
        },
    ]


def test_generated_csv_only_contains_executable_rows(records):
    text = CodeGenerator().generate_source_to_concept_csv(records)
    rows = list(csv.DictReader(io.StringIO(text)))
    assert [row["source_code"] for row in rows] == ["accepted"]


@pytest.mark.parametrize("engine", ["polars", "pandas", "spark"])
def test_generated_scripts_do_not_embed_rejected_targets(records, engine):
    script = CodeGenerator().generate_etl_script(engine, [], records)
    assert "1001" in script
    assert "1002" not in script
    assert "1003" not in script


def test_artifact_reload_respects_decision_columns(tmp_path, records):
    (tmp_path / "etl_config.yaml").write_text(
        yaml.safe_dump({"engine": "polars", "schema_mappings": []})
    )
    with (tmp_path / "source_to_concept_map.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "source_code",
                "source_column",
                "target_concept_id",
                "method",
                "review_decision",
            ],
        )
        writer.writeheader()
        writer.writerows(records)
    runner = ETLRunner.from_artifacts(str(tmp_path), engine=PolarsEngine())
    assert [item["source_code"] for item in runner.concept_items] == ["accepted"]


def test_dbt_seed_excludes_pending_and_rejected_rows(tmp_path, records):
    build_dbt_project(SchemaMapping(), ConceptMapping(items=records), tmp_path)
    with (tmp_path / "seeds" / "source_to_concept_map.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    assert [row["source_code"] for row in rows] == ["accepted"]


def test_dbt_regeneration_requires_fresh_output(tmp_path, records):
    concepts = ConceptMapping(items=records)
    build_dbt_project(SchemaMapping(), concepts, tmp_path)
    seed = tmp_path / "seeds" / "source_to_concept_map.csv"
    previous = seed.read_bytes()
    for item in concepts.items:
        item.reject()
    with pytest.raises(ValueError, match="empty output"):
        build_dbt_project(SchemaMapping(), concepts, tmp_path)
    assert seed.read_bytes() == previous


@pytest.mark.parametrize("nested", [True, False])
def test_stage_four_lookup_excludes_pending_and_rejected_rows(tmp_path, records, nested):
    concepts = {"mappings": {"code": {"items": records}}} if nested else {"items": records}
    generate_etl(PolarsEngine(), {"items": []}, concepts, "source.csv", "output", str(tmp_path))
    with (tmp_path / "concept_lookup.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    assert [row["source_code"] for row in rows] == ["accepted"]

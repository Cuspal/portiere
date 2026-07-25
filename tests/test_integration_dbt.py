"""Tests for the dbt project generator (portiere.integrations.dbt)."""

import csv

import yaml

from portiere.integrations.dbt import build_dbt_project
from portiere.models.concept_mapping import ConceptMapping, ConceptMappingItem
from portiere.models.schema_mapping import SchemaMapping, SchemaMappingItem


def _schema():
    return SchemaMapping(
        items=[
            SchemaMappingItem(
                source_table="kcis_patient",
                source_column="patient_no",
                target_table="person",
                target_column="person_id",
                confidence=0.95,
                status="approved",
            ),
            SchemaMappingItem(
                source_table="kcis_patient",
                source_column="sex",
                target_table="person",
                target_column="gender_concept_id",
                confidence=0.9,
                status="approved",
            ),
            SchemaMappingItem(
                source_table="kcis_patient",
                source_column="unknown_col",
                target_table="person",
                target_column=None,
                confidence=0.1,
                status="needs_review",
            ),
            SchemaMappingItem(
                source_table="kcis_dx",
                source_column="icd10",
                target_table="condition_occurrence",
                target_column="condition_source_value",
                confidence=0.88,
                status="approved",
            ),
        ]
    )


def _concepts():
    return ConceptMapping(
        items=[
            ConceptMappingItem(
                source_code="M",
                source_column="sex",
                target_concept_id=8507,
                target_concept_name="MALE",
                target_vocabulary_id="Gender",
                confidence=0.99,
                method="auto",
            ),
            ConceptMappingItem(
                source_code="F",
                source_column="sex",
                target_concept_id=8532,
                target_concept_name="FEMALE",
                target_vocabulary_id="Gender",
                confidence=0.99,
                method="auto",
            ),
        ]
    )


class TestDbtProjectStructure:
    def test_emits_project_files(self, tmp_path):
        out = build_dbt_project(_schema(), _concepts(), tmp_path / "proj")
        assert (out / "dbt_project.yml").exists()
        assert (out / "models" / "person.sql").exists()
        assert (out / "models" / "condition_occurrence.sql").exists()
        assert (out / "models" / "schema.yml").exists()
        assert (out / "seeds" / "source_to_concept_map.csv").exists()
        assert (out / "README.md").exists()

    def test_dbt_project_yml_valid(self, tmp_path):
        out = build_dbt_project(_schema(), None, tmp_path / "proj", project_name="my_omop")
        cfg = yaml.safe_load((out / "dbt_project.yml").read_text())
        assert cfg["name"] == "my_omop"
        assert "models" in cfg

    def test_person_model_sql(self, tmp_path):
        out = build_dbt_project(_schema(), _concepts(), tmp_path / "proj", source_schema="raw")
        sql = (out / "models" / "person.sql").read_text()
        assert "{{ source('raw', 'kcis_patient') }}" in sql
        assert "patient_no as person_id" in sql
        assert "sex as gender_concept_id" in sql
        # concept join for the sex column
        assert "{{ ref('source_to_concept_map') }}" in sql
        assert "gender_concept_id" in sql
        # unmapped column surfaced as a visible stub, not silently dropped
        assert "unknown_col" in sql and "UNMAPPED" in sql

    def test_seed_has_concept_rows(self, tmp_path):
        out = build_dbt_project(_schema(), _concepts(), tmp_path / "proj")
        with (out / "seeds" / "source_to_concept_map.csv").open() as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 2
        assert {r["source_code"] for r in rows} == {"M", "F"}
        assert rows[0]["target_concept_id"] in {"8507", "8532"}

    def test_schema_yml_has_tests(self, tmp_path):
        out = build_dbt_project(_schema(), _concepts(), tmp_path / "proj", standard="omop_cdm_v5.4")
        sy = yaml.safe_load((out / "models" / "schema.yml").read_text())
        models = {m["name"]: m for m in sy["models"]}
        assert "person" in models
        # person_id is a required OMOP field → not_null test present
        cols = {c["name"]: c for c in models["person"]["columns"]}
        assert "person_id" in cols
        assert any("not_null" in str(t) for t in cols["person_id"].get("tests", []))

    def test_no_concept_mapping_still_generates(self, tmp_path):
        out = build_dbt_project(_schema(), None, tmp_path / "proj")
        assert (out / "models" / "person.sql").exists()
        # no seed when no concepts
        assert not (out / "seeds" / "source_to_concept_map.csv").exists()


class TestDbtCli:
    def test_dbt_cli_generates(self, tmp_path):
        import json

        from click.testing import CliRunner

        from portiere.cli import cli

        sm = tmp_path / "schema.json"
        sm.write_text(
            json.dumps(
                {
                    "items": [
                        {
                            "source_table": "kcis_patient",
                            "source_column": "sex",
                            "target_table": "person",
                            "target_column": "gender_concept_id",
                            "confidence": 0.9,
                            "status": "approved",
                        },
                    ]
                }
            )
        )
        out = tmp_path / "proj"
        res = CliRunner().invoke(cli, ["dbt", "--schema-mapping", str(sm), "-o", str(out)])
        assert res.exit_code == 0, res.output
        assert (out / "models" / "person.sql").exists()

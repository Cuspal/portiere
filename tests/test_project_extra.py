"""Extra Project tests for Slice 8 coverage gap-fill.

Targets the cross_map() / import_concept_mapping / export_concept_mapping /
__repr__ / load helper paths in src/portiere/project.py that aren't
exercised by the manifest tests.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest


def _make_project(tmp_path: Path, **kwargs):
    import portiere
    from portiere.config import EmbeddingConfig, PortiereConfig

    config = PortiereConfig(
        local_project_dir=tmp_path,
        embedding=EmbeddingConfig(provider="none"),
    )
    return portiere.init(
        name=kwargs.get("name", "extra-test"),
        target_model=kwargs.get("target_model", "omop_cdm_v5.4"),
        task=kwargs.get("task", "standardize"),
        source_standard=kwargs.get("source_standard"),
        config=config,
    )


# ── cross_map() ──────────────────────────────────────────────────


class TestCrossMap:
    def test_dict_input_omop_to_fhir(self, tmp_path):
        project = _make_project(
            tmp_path,
            name="xmap-dict",
            target_model="fhir_r4",
            task="cross_map",
            source_standard="omop_cdm_v5.4",
        )
        record = {"person_id": 12345, "year_of_birth": 1985}
        result = project.cross_map(source_entity="person", data=record)
        assert isinstance(result, dict)

    def test_list_input(self, tmp_path):
        project = _make_project(
            tmp_path,
            name="xmap-list",
            target_model="fhir_r4",
            task="cross_map",
            source_standard="omop_cdm_v5.4",
        )
        result = project.cross_map(
            source_entity="person",
            data=[{"person_id": 1}, {"person_id": 2}],
        )
        assert isinstance(result, list)
        assert len(result) == 2

    def test_dataframe_input(self, tmp_path):
        project = _make_project(
            tmp_path,
            name="xmap-df",
            target_model="fhir_r4",
            task="cross_map",
            source_standard="omop_cdm_v5.4",
        )
        df = pd.DataFrame([{"person_id": 1}, {"person_id": 2}])
        result = project.cross_map(source_entity="person", data=df)
        assert hasattr(result, "__len__")

    def test_explicit_standards_for_standardize_project(self, tmp_path):
        project = _make_project(tmp_path, name="xmap-explicit")
        # Pass all args explicitly — backward-compat path
        result = project.cross_map("omop_cdm_v5.4", "fhir_r4", "person", {"person_id": 99})
        assert isinstance(result, dict)

    def test_standardize_without_target_raises(self, tmp_path):
        project = _make_project(tmp_path, name="xmap-nope")
        with pytest.raises(ValueError, match="target_standard"):
            project.cross_map(source_entity="person", data={"person_id": 1})


# ── import_concept_mapping ───────────────────────────────────────


class TestImportConceptMapping:
    def test_from_json(self, tmp_path):
        project = _make_project(tmp_path, name="imp-json")
        mapping_path = tmp_path / "mapping.json"
        # ConceptMapping.from_json expects a top-level list
        mapping_path.write_text(
            json.dumps(
                [
                    {
                        "source_code": "E11.9",
                        "source_description": "Diabetes",
                        "target_concept_id": 201826,
                        "target_concept_name": "Type 2 diabetes",
                        "target_vocabulary_id": "SNOMED",
                        "target_domain_id": "Condition",
                        "confidence": 0.95,
                        "method": "auto",
                    }
                ]
            )
        )
        result = project.import_concept_mapping(path=str(mapping_path))
        assert len(result.items) == 1
        assert result.items[0].source_code == "E11.9"

    def test_from_records(self, tmp_path):
        project = _make_project(tmp_path, name="imp-records")
        records = [
            {
                "source_code": "I10",
                "source_description": "Hypertension",
                "target_concept_id": 320128,
                "target_concept_name": "Essential hypertension",
                "target_vocabulary_id": "SNOMED",
                "target_domain_id": "Condition",
                "confidence": 0.99,
                "method": "auto",
            }
        ]
        result = project.import_concept_mapping(records=records)
        assert len(result.items) == 1
        assert result.items[0].source_code == "I10"

    def test_no_input_raises(self, tmp_path):
        project = _make_project(tmp_path, name="imp-empty")
        with pytest.raises(ValueError, match="Provide one of"):
            project.import_concept_mapping()


# ── export_concept_mapping ───────────────────────────────────────


class TestExportConceptMapping:
    def _import(self, project, tmp_path):
        records = [
            {
                "source_code": "E11.9",
                "source_description": "Diabetes",
                "target_concept_id": 201826,
                "target_concept_name": "Type 2 diabetes",
                "target_vocabulary_id": "SNOMED",
                "target_domain_id": "Condition",
                "confidence": 0.95,
                "method": "auto",
            }
        ]
        project.import_concept_mapping(records=records)

    def test_export_json(self, tmp_path):
        project = _make_project(tmp_path, name="exp-json")
        self._import(project, tmp_path)
        out = tmp_path / "out.json"
        project.export_concept_mapping(str(out))
        assert out.exists()
        loaded = json.loads(out.read_text())
        # to_json emits a top-level list
        assert isinstance(loaded, list)
        assert len(loaded) == 1

    def test_export_csv(self, tmp_path):
        project = _make_project(tmp_path, name="exp-csv")
        self._import(project, tmp_path)
        out = tmp_path / "out.csv"
        project.export_concept_mapping(str(out))
        assert out.exists()
        # CSV starts with a header line
        first_line = out.read_text().splitlines()[0]
        assert "source_code" in first_line

    def test_export_omop_format(self, tmp_path):
        project = _make_project(tmp_path, name="exp-omop")
        self._import(project, tmp_path)
        out = tmp_path / "out.csv"
        project.export_concept_mapping(str(out), omop_format=True)
        assert out.exists()


# ── Project __repr__ ─────────────────────────────────────────────


class TestRepr:
    def test_includes_name_and_target(self, tmp_path):
        project = _make_project(tmp_path, name="repr-test")
        r = repr(project)
        assert "repr-test" in r
        assert "omop_cdm_v5.4" in r

    def test_cross_map_project_repr_includes_source(self, tmp_path):
        project = _make_project(
            tmp_path,
            name="repr-xmap",
            target_model="fhir_r4",
            task="cross_map",
            source_standard="omop_cdm_v5.4",
        )
        r = repr(project)
        assert "fhir_r4" in r
        assert "omop_cdm_v5.4" in r


# ── load_schema_mapping / load_concept_mapping ──────────────────


class TestLoadHelpers:
    def test_load_schema_mapping_after_import_path(self, tmp_path):
        # First import a concept mapping to make sure storage exists,
        # then verify load helpers don't crash.
        project = _make_project(tmp_path, name="load-helper")
        records = [
            {
                "source_code": "E11.9",
                "source_description": "Diabetes",
                "target_concept_id": 201826,
                "target_concept_name": "T2DM",
                "target_vocabulary_id": "SNOMED",
                "target_domain_id": "Condition",
                "confidence": 0.95,
                "method": "auto",
            }
        ]
        project.import_concept_mapping(records=records)
        loaded = project.load_concept_mapping()
        assert len(loaded.items) == 1


# ── save_schema_mapping / save_concept_mapping ──────────────────


class TestSaveMappings:
    def test_save_schema_mapping(self, tmp_path):
        from portiere.models.schema_mapping import SchemaMapping, SchemaMappingItem

        project = _make_project(tmp_path, name="save-schema")
        m = SchemaMapping(
            items=[
                SchemaMappingItem(
                    source_column="dob",
                    source_table="patients",
                    target_table="person",
                    target_column="birth_datetime",
                    confidence=0.99,
                    status="auto_accepted",
                )
            ]
        )
        # Should not raise
        project.save_schema_mapping(m)

    def test_save_concept_mapping(self, tmp_path):
        from portiere.models.concept_mapping import ConceptMapping

        project = _make_project(tmp_path, name="save-concept")
        m = ConceptMapping(items=[])
        project.save_concept_mapping(m)


# ── Project.engine and storage properties ──────────────────────


class TestProperties:
    def test_engine_property(self, tmp_path):
        project = _make_project(tmp_path, name="props")
        assert project.engine is project._engine

    def test_storage_property(self, tmp_path):
        project = _make_project(tmp_path, name="props2")
        assert project.storage is project._storage

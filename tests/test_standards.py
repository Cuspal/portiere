"""Tests for YAMLTargetModel and standards loading."""

from pathlib import Path

import pytest

from portiere.standards import (
    YAMLTargetModel,
    get_standards_dir,
    list_standards,
)


class TestStandardsDirectory:
    def test_get_standards_dir_returns_path(self):
        d = get_standards_dir()
        assert isinstance(d, Path)
        assert d.exists()

    def test_list_standards_returns_nonempty(self):
        standards = list_standards()
        assert isinstance(standards, list)
        assert len(standards) >= 1

    def test_list_standards_includes_omop(self):
        standards = list_standards()
        assert any("omop" in s.lower() for s in standards)


class TestYAMLTargetModelFromName:
    def test_load_omop(self):
        model = YAMLTargetModel.from_name("omop_cdm_v5.4")
        assert model.name == "omop_cdm_v5.4"

    def test_load_fhir(self):
        model = YAMLTargetModel.from_name("fhir_r4")
        assert "fhir" in model.name.lower()

    def test_unknown_standard_raises(self):
        with pytest.raises(ValueError, match="not found"):
            YAMLTargetModel.from_name("nonexistent_standard_xyz")


class TestYAMLTargetModelProperties:
    @pytest.fixture
    def omop(self):
        return YAMLTargetModel.from_name("omop_cdm_v5.4")

    def test_name(self, omop):
        assert omop.name == "omop_cdm_v5.4"

    def test_version(self, omop):
        assert omop.version is not None
        assert len(omop.version) > 0

    def test_standard_type(self, omop):
        assert omop.standard_type in ("relational", "resource", "segment", "archetype")

    def test_description_is_string(self, omop):
        assert isinstance(omop.description, str)

    def test_organization_is_string(self, omop):
        assert isinstance(omop.organization, str)


class TestYAMLTargetModelSchema:
    @pytest.fixture
    def omop(self):
        return YAMLTargetModel.from_name("omop_cdm_v5.4")

    def test_get_schema_returns_dict(self, omop):
        schema = omop.get_schema()
        assert isinstance(schema, dict)
        assert len(schema) > 0

    def test_schema_has_person_table(self, omop):
        schema = omop.get_schema()
        assert "person" in schema

    def test_schema_person_has_fields(self, omop):
        schema = omop.get_schema()
        assert len(schema["person"]) > 0
        assert "person_id" in schema["person"]

    def test_get_target_descriptions_returns_dict(self, omop):
        descs = omop.get_target_descriptions()
        assert isinstance(descs, dict)
        assert len(descs) > 0
        assert all("." in k for k in descs.keys())

    def test_get_target_descriptions_tupled(self, omop):
        descs = omop.get_target_descriptions_tupled()
        assert isinstance(descs, dict)
        assert all(isinstance(k, tuple) and len(k) == 2 for k in descs.keys())

    def test_get_source_patterns(self, omop):
        patterns = omop.get_source_patterns()
        assert isinstance(patterns, dict)
        assert all(isinstance(v, tuple) for v in patterns.values())

    def test_get_required_fields(self, omop):
        req = omop.get_required_fields()
        assert isinstance(req, dict)

    def test_get_field_types(self, omop):
        schema = omop.get_schema()
        first_entity = next(iter(schema))
        ft = omop.get_field_types(first_entity)
        assert isinstance(ft, dict)


class TestYAMLTargetModelAllStandards:
    """Smoke-test that all built-in standards load and produce valid schemas."""

    @pytest.mark.parametrize("standard_name", list_standards())
    def test_standard_loads(self, standard_name):
        model = YAMLTargetModel.from_name(standard_name)
        schema = model.get_schema()
        assert isinstance(schema, dict)
        assert len(schema) > 0

    @pytest.mark.parametrize("standard_name", list_standards())
    def test_standard_has_descriptions(self, standard_name):
        model = YAMLTargetModel.from_name(standard_name)
        descs = model.get_target_descriptions()
        assert isinstance(descs, dict)


class TestYAMLTargetModelCustom:
    def test_from_custom_path(self, tmp_path):
        yaml_content = """
name: custom_v1.0
version: "1.0"
standard_type: relational
organization: Custom
description: Custom test standard
entities:
  patient:
    fields:
      patient_id:
        type: integer
        required: true
        description: Patient unique identifier
      name:
        type: string
        description: Patient name
    source_patterns:
      pat_id: patient_id
      patient_id: patient_id
    embedding_descriptions:
      patient_id: The unique identifier for the patient
      name: The patient full name
"""
        yaml_path = tmp_path / "custom_v1.0.yaml"
        yaml_path.write_text(yaml_content)

        model = YAMLTargetModel(yaml_path)
        assert model.name == "custom_v1.0"
        schema = model.get_schema()
        assert "patient" in schema
        assert "patient_id" in schema["patient"]

    def test_missing_required_keys_raises(self, tmp_path):
        yaml_content = "name: incomplete\nversion: '1.0'\n"
        yaml_path = tmp_path / "incomplete.yaml"
        yaml_path.write_text(yaml_content)
        with pytest.raises(ValueError, match="missing required keys"):
            YAMLTargetModel(yaml_path)

    def test_nonexistent_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            YAMLTargetModel(tmp_path / "nonexistent.yaml")

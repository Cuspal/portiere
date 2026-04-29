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


class TestSlice2NewOMOPClinicalEntities:
    """v0.2.0 Slice 2A — six newly-added OMOP CDM v5.4 clinical-data tables.

    Each smoke test asserts:
      * entity is present in the schema,
      * its key fields are defined,
      * at least 3 source_patterns map to it,
      * at least 3 embedding_descriptions exist for it.
    """

    @pytest.fixture
    def omop(self):
        return YAMLTargetModel.from_name("omop_cdm_v5.4")

    @staticmethod
    def _patterns_targeting(omop, entity):
        return [v for v in omop.get_source_patterns().values() if v[0] == entity]

    @staticmethod
    def _descs_for(omop, entity):
        return {k: v for k, v in omop.get_target_descriptions_tupled().items() if k[0] == entity}

    def test_provider_entity(self, omop):
        schema = omop.get_schema()
        assert "provider" in schema, "OMOP `provider` entity missing"
        for f in ("provider_id", "provider_name", "specialty_concept_id", "care_site_id"):
            assert f in schema["provider"], f"`provider.{f}` missing"
        assert len(self._patterns_targeting(omop, "provider")) >= 3
        assert len(self._descs_for(omop, "provider")) >= 3

    def test_care_site_entity(self, omop):
        schema = omop.get_schema()
        assert "care_site" in schema, "OMOP `care_site` entity missing"
        for f in ("care_site_id", "care_site_name", "place_of_service_concept_id", "location_id"):
            assert f in schema["care_site"], f"`care_site.{f}` missing"
        assert len(self._patterns_targeting(omop, "care_site")) >= 3
        assert len(self._descs_for(omop, "care_site")) >= 3

    def test_death_entity(self, omop):
        schema = omop.get_schema()
        assert "death" in schema, "OMOP `death` entity missing"
        for f in ("person_id", "death_date", "death_type_concept_id", "cause_concept_id"):
            assert f in schema["death"], f"`death.{f}` missing"
        assert len(self._patterns_targeting(omop, "death")) >= 3
        assert len(self._descs_for(omop, "death")) >= 3

    def test_observation_period_entity(self, omop):
        schema = omop.get_schema()
        assert "observation_period" in schema, "OMOP `observation_period` entity missing"
        for f in (
            "observation_period_id",
            "person_id",
            "observation_period_start_date",
            "observation_period_end_date",
            "period_type_concept_id",
        ):
            assert f in schema["observation_period"], f"`observation_period.{f}` missing"
        assert len(self._patterns_targeting(omop, "observation_period")) >= 3
        assert len(self._descs_for(omop, "observation_period")) >= 3

    def test_device_exposure_entity(self, omop):
        schema = omop.get_schema()
        assert "device_exposure" in schema, "OMOP `device_exposure` entity missing"
        for f in (
            "device_exposure_id",
            "person_id",
            "device_concept_id",
            "device_exposure_start_date",
            "device_type_concept_id",
        ):
            assert f in schema["device_exposure"], f"`device_exposure.{f}` missing"
        assert len(self._patterns_targeting(omop, "device_exposure")) >= 3
        assert len(self._descs_for(omop, "device_exposure")) >= 3

    def test_note_entity(self, omop):
        schema = omop.get_schema()
        assert "note" in schema, "OMOP `note` entity missing"
        for f in (
            "note_id",
            "person_id",
            "note_date",
            "note_type_concept_id",
            "note_class_concept_id",
            "note_text",
            "encoding_concept_id",
            "language_concept_id",
        ):
            assert f in schema["note"], f"`note.{f}` missing"
        assert len(self._patterns_targeting(omop, "note")) >= 3
        assert len(self._descs_for(omop, "note")) >= 3


class TestSlice2NewOMOPVocabularyTables:
    """v0.2.0 Slice 2B — OMOP CDM vocabulary tables.

    Vocabulary tables (concept, vocabulary, ...) are loaded directly from
    Athena, not user-mapped from EHR data, so we do NOT require ≥3
    source_patterns or embedding_descriptions per entity. The presence
    and key-field shape is the contract Slice 3 (FK validation) needs.
    """

    @pytest.fixture
    def omop(self):
        return YAMLTargetModel.from_name("omop_cdm_v5.4")

    def test_concept_table(self, omop):
        schema = omop.get_schema()
        assert "concept" in schema, "OMOP `concept` table missing"
        for f in (
            "concept_id",
            "concept_name",
            "domain_id",
            "vocabulary_id",
            "concept_class_id",
            "standard_concept",
            "concept_code",
            "valid_start_date",
            "valid_end_date",
            "invalid_reason",
        ):
            assert f in schema["concept"], f"`concept.{f}` missing"

    def test_vocabulary_table(self, omop):
        schema = omop.get_schema()
        assert "vocabulary" in schema, "OMOP `vocabulary` table missing"
        for f in (
            "vocabulary_id",
            "vocabulary_name",
            "vocabulary_reference",
            "vocabulary_version",
            "vocabulary_concept_id",
        ):
            assert f in schema["vocabulary"], f"`vocabulary.{f}` missing"

    def test_concept_relationship_table(self, omop):
        schema = omop.get_schema()
        assert "concept_relationship" in schema, "OMOP `concept_relationship` table missing"
        for f in (
            "concept_id_1",
            "concept_id_2",
            "relationship_id",
            "valid_start_date",
            "valid_end_date",
            "invalid_reason",
        ):
            assert f in schema["concept_relationship"], f"`concept_relationship.{f}` missing"

    def test_domain_table(self, omop):
        schema = omop.get_schema()
        assert "domain" in schema, "OMOP `domain` table missing"
        for f in ("domain_id", "domain_name", "domain_concept_id"):
            assert f in schema["domain"], f"`domain.{f}` missing"

    def test_concept_class_table(self, omop):
        schema = omop.get_schema()
        assert "concept_class" in schema, "OMOP `concept_class` table missing"
        for f in ("concept_class_id", "concept_class_name", "concept_class_concept_id"):
            assert f in schema["concept_class"], f"`concept_class.{f}` missing"


class TestSlice2NewFHIRResources:
    """v0.2.0 Slice 2C — ten newly-added FHIR R4 resources."""

    @pytest.fixture
    def fhir(self):
        return YAMLTargetModel.from_name("fhir_r4")

    @staticmethod
    def _patterns_targeting(fhir, entity):
        return [v for v in fhir.get_source_patterns().values() if v[0] == entity]

    @staticmethod
    def _descs_for(fhir, entity):
        return {k: v for k, v in fhir.get_target_descriptions_tupled().items() if k[0] == entity}

    def _assert_resource(self, fhir, name, required_fields, *, require_patterns=True):
        schema = fhir.get_schema()
        assert name in schema, f"FHIR `{name}` resource missing"
        for f in required_fields:
            assert f in schema[name], f"`{name}.{f}` missing"
        if require_patterns:
            assert len(self._patterns_targeting(fhir, name)) >= 3
        assert len(self._descs_for(fhir, name)) >= 3

    def test_practitioner_resource(self, fhir):
        self._assert_resource(
            fhir,
            "Practitioner",
            ["id", "identifier", "name", "telecom", "qualification", "gender", "active"],
        )

    def test_organization_resource(self, fhir):
        self._assert_resource(
            fhir, "Organization", ["id", "identifier", "active", "type", "name", "address"]
        )

    def test_location_resource(self, fhir):
        self._assert_resource(
            fhir, "Location", ["id", "identifier", "status", "name", "type", "address"]
        )

    def test_specimen_resource(self, fhir):
        self._assert_resource(
            fhir, "Specimen", ["id", "identifier", "status", "type", "subject", "collection"]
        )

    def test_immunization_resource(self, fhir):
        self._assert_resource(
            fhir, "Immunization", ["id", "status", "vaccineCode", "patient", "occurrenceDateTime"]
        )

    def test_service_request_resource(self, fhir):
        self._assert_resource(
            fhir, "ServiceRequest", ["id", "status", "intent", "code", "subject", "authoredOn"]
        )

    def test_medication_administration_resource(self, fhir):
        self._assert_resource(
            fhir,
            "MedicationAdministration",
            ["id", "status", "medicationCodeableConcept", "subject", "effectiveDateTime"],
        )

    def test_medication_dispense_resource(self, fhir):
        self._assert_resource(
            fhir,
            "MedicationDispense",
            ["id", "status", "medicationCodeableConcept", "subject", "whenHandedOver", "quantity"],
        )

    def test_bundle_resource(self, fhir):
        # Bundle is a container resource — no source_patterns expected
        self._assert_resource(
            fhir, "Bundle", ["id", "type", "entry", "total"], require_patterns=False
        )

    def test_document_reference_resource(self, fhir):
        self._assert_resource(
            fhir, "DocumentReference", ["id", "status", "type", "subject", "date", "content"]
        )


class TestSlice2EntityReachability:
    """v0.2.0 Slice 2D — verify the schema mapper can reach the new entities
    via source_patterns. Lighter than a full pipeline test (deferred to
    Slice 5 quickstart, which exercises map_schema end-to-end).
    """

    def test_at_least_two_new_omop_entities_reachable(self):
        omop = YAMLTargetModel.from_name("omop_cdm_v5.4")
        new = {"provider", "care_site", "death", "observation_period", "device_exposure", "note"}
        targets = {v[0] for v in omop.get_source_patterns().values()}
        hit = new & targets
        assert len(hit) >= 2, (
            f"Only {len(hit)} of the 6 newly-added OMOP clinical entities "
            f"are reachable via source_patterns: {sorted(hit)}"
        )

    def test_at_least_two_new_fhir_resources_reachable(self):
        fhir = YAMLTargetModel.from_name("fhir_r4")
        new = {
            "Practitioner",
            "Organization",
            "Location",
            "Specimen",
            "Immunization",
            "ServiceRequest",
            "MedicationAdministration",
            "MedicationDispense",
            "DocumentReference",
        }
        targets = {v[0] for v in fhir.get_source_patterns().values()}
        hit = new & targets
        assert len(hit) >= 2, (
            f"Only {len(hit)} of the newly-added FHIR resources "
            f"are reachable via source_patterns: {sorted(hit)}"
        )

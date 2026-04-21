"""
Tests for StorageBackend implementations.

Tests LocalStorageBackend and CloudStorageBackend:
- Project CRUD
- Schema/concept mapping save/load roundtrip
- Source management
- Profile and quality report storage
"""

from unittest.mock import MagicMock

import pytest


class TestLocalStorageBackend:
    """Tests for LocalStorageBackend (filesystem)."""

    def test_create_project(self, tmp_path):
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)
        metadata = storage.create_project("test_project", "omop_cdm_v5.4", ["SNOMED"])

        assert metadata["name"] == "test_project"
        assert metadata["target_model"] == "omop_cdm_v5.4"
        assert metadata["vocabularies"] == ["SNOMED"]
        assert "id" in metadata
        assert (tmp_path / "test_project" / "project.yaml").exists()

    def test_create_project_duplicate_raises(self, tmp_path):
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)
        storage.create_project("test_project", "omop_cdm_v5.4", ["SNOMED"])

        with pytest.raises(ValueError, match="already exists"):
            storage.create_project("test_project", "omop_cdm_v5.4", ["SNOMED"])

    def test_load_project(self, tmp_path):
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)
        created = storage.create_project("test_project", "omop_cdm_v5.4", ["SNOMED"])
        loaded = storage.load_project("test_project")

        assert loaded["id"] == created["id"]
        assert loaded["name"] == "test_project"

    def test_load_project_not_found_raises(self, tmp_path):
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)

        with pytest.raises(ValueError, match="not found"):
            storage.load_project("nonexistent")

    def test_list_projects(self, tmp_path):
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)
        storage.create_project("project_a", "omop_cdm_v5.4", ["SNOMED"])
        storage.create_project("project_b", "omop_cdm_v5.4", ["LOINC"])

        projects = storage.list_projects()
        names = {p["name"] for p in projects}
        assert names == {"project_a", "project_b"}

    def test_delete_project(self, tmp_path):
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)
        storage.create_project("to_delete", "omop_cdm_v5.4", ["SNOMED"])
        assert storage.project_exists("to_delete")

        storage.delete_project("to_delete")
        assert not storage.project_exists("to_delete")

    def test_project_exists(self, tmp_path):
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)
        assert not storage.project_exists("test_project")

        storage.create_project("test_project", "omop_cdm_v5.4", ["SNOMED"])
        assert storage.project_exists("test_project")

    def test_save_load_schema_mapping(self, tmp_path):
        from portiere.models.schema_mapping import SchemaMapping, SchemaMappingItem
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)
        storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        items = [
            SchemaMappingItem(
                source_column="patient_id",
                target_table="person",
                target_column="person_id",
                confidence=0.95,
            ),
            SchemaMappingItem(
                source_column="drug_code",
                target_table="drug_exposure",
                target_column="drug_source_value",
                confidence=0.85,
            ),
        ]
        mapping = SchemaMapping(items=items)
        storage.save_schema_mapping("test", mapping)

        loaded = storage.load_schema_mapping("test")
        assert len(loaded.items) == 2
        assert loaded.items[0].source_column == "patient_id"
        assert loaded.items[0].confidence == 0.95

    def test_load_schema_mapping_empty(self, tmp_path):
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)
        storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        loaded = storage.load_schema_mapping("test")
        assert len(loaded.items) == 0

    def test_save_load_concept_mapping(self, tmp_path):
        from portiere.models.concept_mapping import ConceptMapping, ConceptMappingItem
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)
        storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        items = [
            ConceptMappingItem(
                source_code="E11.9",
                source_description="Type 2 diabetes mellitus",
                target_concept_id=201826,
                target_concept_name="Type 2 diabetes mellitus",
                vocabulary_id="SNOMED",
                domain_id="Condition",
                confidence=0.97,
                method="auto",
            ),
        ]
        mapping = ConceptMapping(items=items)
        storage.save_concept_mapping("test", mapping)

        loaded = storage.load_concept_mapping("test")
        assert len(loaded.items) == 1
        assert loaded.items[0].source_code == "E11.9"
        assert loaded.items[0].confidence == 0.97

    def test_save_load_source(self, tmp_path):
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)
        storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        storage.save_source("test", "patients", {"path": "/data/patients.csv", "format": "csv"})
        sources = storage.list_sources("test")
        assert len(sources) == 1
        assert sources[0]["path"] == "/data/patients.csv"

    def test_save_load_profile(self, tmp_path):
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)
        storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        profile = {"source_name": "patients", "columns": [{"name": "id", "dtype": "int64"}]}
        storage.save_profile("test", "patients", profile)

        loaded = storage.load_profile("test", "patients")
        assert loaded is not None
        assert loaded["source_name"] == "patients"

    def test_load_profile_not_found(self, tmp_path):
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)
        storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        assert storage.load_profile("test", "nonexistent") is None

    def test_save_load_quality_report(self, tmp_path):
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)
        storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        report = {"table_name": "person", "passed": True, "completeness_score": 0.98}
        storage.save_quality_report("test", report)

        reports = storage.load_quality_reports("test")
        assert len(reports) == 1
        assert reports[0]["table_name"] == "person"

    def test_sync_metadata(self, tmp_path):
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)
        storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        assert storage.load_sync_metadata("test") is None

        storage.save_sync_metadata("test", "cloud-123")
        meta = storage.load_sync_metadata("test")
        assert meta is not None
        assert meta["cloud_project_id"] == "cloud-123"


class TestCloudStorageBackend:
    """Tests for CloudStorageBackend — open-source SDK raises NotImplementedError."""

    def test_init_raises_not_implemented(self, tmp_path):
        from portiere.storage.cloud_backend import CloudStorageBackend

        mock_client = MagicMock()
        with pytest.raises(NotImplementedError, match="Cloud storage"):
            CloudStorageBackend(client=mock_client, local_cache_dir=tmp_path / "cloud_cache")

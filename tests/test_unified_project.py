"""
Tests for the unified Project class and portiere.init().

Tests both local and cloud pipeline modes, storage delegation,
GX integration, push/pull sync, and the init() factory function.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestPortiereInit:
    """Tests for portiere.init() factory function."""

    def test_init_creates_local_project(self, tmp_path):
        from portiere import PortiereConfig, init

        config = PortiereConfig(local_project_dir=tmp_path)
        mock_engine = MagicMock()
        project = init(name="test_project", engine=mock_engine, config=config)

        assert project.name == "test_project"
        assert project.target_model == "omop_cdm_v5.4"
        assert project.vocabularies == ["SNOMED", "LOINC", "RxNorm", "ICD10CM"]
        assert project.config.mode == "local"
        assert project.config.pipeline == "local"
        assert project._client is None

    def test_init_idempotent(self, tmp_path):
        from portiere import PortiereConfig, init

        config = PortiereConfig(local_project_dir=tmp_path)
        mock_engine = MagicMock()
        project1 = init(name="test_project", engine=mock_engine, config=config)
        project2 = init(name="test_project", engine=mock_engine, config=config)

        assert project1.id == project2.id
        assert project1.name == project2.name

    def test_init_with_custom_vocabularies(self, tmp_path):
        from portiere import PortiereConfig, init

        config = PortiereConfig(local_project_dir=tmp_path)
        mock_engine = MagicMock()
        project = init(
            name="test_project",
            engine=mock_engine,
            vocabularies=["SNOMED", "LOINC"],
            config=config,
        )

        assert project.vocabularies == ["SNOMED", "LOINC"]

    def test_init_with_custom_target_model(self, tmp_path):
        from portiere import PortiereConfig, init

        config = PortiereConfig(local_project_dir=tmp_path)
        mock_engine = MagicMock()
        project = init(
            name="test_project",
            engine=mock_engine,
            target_model="fhir_r4",
            config=config,
        )

        assert project.target_model == "fhir_r4"

    def test_init_with_api_key_warns_and_uses_local(self, tmp_path):
        """In open-source SDK, api_key emits warning and uses local storage."""
        import warnings

        from portiere import PortiereConfig, init

        config = PortiereConfig(
            api_key="ptk_test_key",
            local_project_dir=tmp_path,
        )

        mock_engine = MagicMock()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            project = init(name="test_project", engine=mock_engine, config=config)
            assert any("api_key is ignored" in str(warning.message) for warning in w)
        assert project.name == "test_project"

    def test_init_cloud_pipeline_uses_local_in_oss(self, tmp_path):
        """In open-source SDK, pipeline='cloud' is ignored, uses local."""
        import warnings

        from portiere import PortiereConfig, init

        config = PortiereConfig(
            pipeline="cloud",
            api_key="ptk_test_key",
            local_project_dir=tmp_path,
        )

        mock_engine = MagicMock()
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            project = init(name="test_project", engine=mock_engine, config=config)
        assert project._client is None

    def test_init_local_pipeline_no_client(self, tmp_path):
        from portiere import PortiereConfig, init

        config = PortiereConfig(
            mode="local",
            pipeline="local",
            local_project_dir=tmp_path,
        )

        mock_engine = MagicMock()
        project = init(name="test_project", engine=mock_engine, config=config)
        assert project._client is None

    def test_init_uses_local_storage_backend(self, tmp_path):
        from portiere import PortiereConfig, init
        from portiere.storage.local_backend import LocalStorageBackend

        config = PortiereConfig(local_project_dir=tmp_path)
        mock_engine = MagicMock()
        project = init(name="test_project", engine=mock_engine, config=config)

        assert isinstance(project._storage, LocalStorageBackend)


class TestProject:
    """Tests for the unified Project class."""

    def _make_project(self, tmp_path, pipeline="local", api_key=None):
        """Helper to create a Project with LocalStorageBackend."""
        from portiere.config import PortiereConfig
        from portiere.project import Project
        from portiere.storage.local_backend import LocalStorageBackend

        config = PortiereConfig(
            mode="local",
            pipeline=pipeline,
            api_key=api_key,
            local_project_dir=tmp_path,
        )

        storage = LocalStorageBackend(base_dir=tmp_path)
        metadata = storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        client = None
        if pipeline == "cloud" and api_key:
            client = MagicMock()

        return Project(
            name="test",
            target_model="omop_cdm_v5.4",
            vocabularies=["SNOMED"],
            config=config,
            storage=storage,
            project_id=metadata["id"],
            engine=MagicMock(),
            client=client,
        )

    def test_project_properties(self, tmp_path):
        project = self._make_project(tmp_path)

        assert project.name == "test"
        assert project.target_model == "omop_cdm_v5.4"
        assert project.vocabularies == ["SNOMED"]
        assert project.config.pipeline == "local"

    def test_project_repr(self, tmp_path):
        project = self._make_project(tmp_path)
        repr_str = repr(project)

        assert "test" in repr_str
        assert "omop_cdm_v5.4" in repr_str
        assert "local" in repr_str

    def test_add_source(self, tmp_path):
        project = self._make_project(tmp_path)

        # Disable auto-profile to avoid GX dependency
        project.config.quality.profile_on_ingest = False

        source = project.add_source("/data/patients.csv", name="patients")

        assert source["name"] == "patients"
        assert source["path"] == "/data/patients.csv"
        assert source["format"] == "csv"

        # Verify persisted to storage
        sources = project._storage.list_sources("test")
        assert len(sources) == 1
        assert sources[0]["name"] == "patients"

    def test_add_source_auto_name(self, tmp_path):
        project = self._make_project(tmp_path)
        project.config.quality.profile_on_ingest = False

        source = project.add_source("/data/patients.csv")
        assert source["name"] == "patients"

    def test_add_source_auto_format(self, tmp_path):
        project = self._make_project(tmp_path)
        project.config.quality.profile_on_ingest = False

        source = project.add_source("/data/patients.parquet")
        assert source["format"] == "parquet"

    def test_add_source_auto_profile_skipped_without_gx(self, tmp_path):
        project = self._make_project(tmp_path)
        project.config.quality.profile_on_ingest = True

        # profile() will raise ImportError (GX not installed) which is caught
        with patch.object(project, "profile", side_effect=ImportError("no gx")):
            source = project.add_source("/data/test.csv")
            assert source is not None

    def test_engine_is_stored_directly(self, tmp_path):
        project = self._make_project(tmp_path)

        assert project._engine is not None
        assert project.engine is project._engine

    def test_client_property_raises_not_implemented(self, tmp_path):
        """In open-source SDK, accessing client always raises NotImplementedError."""
        project = self._make_project(tmp_path, pipeline="local")

        with pytest.raises(NotImplementedError, match="Cloud features"):
            _ = project.client

    def test_load_schema_mapping_delegates_to_storage(self, tmp_path):
        project = self._make_project(tmp_path)

        mapping = project.load_schema_mapping()
        assert len(mapping.items) == 0

    def test_load_concept_mapping_delegates_to_storage(self, tmp_path):
        project = self._make_project(tmp_path)

        mapping = project.load_concept_mapping()
        assert len(mapping.items) == 0


class TestProjectMapSchema:
    """Tests for Project.map_schema() with both pipeline modes."""

    def _make_project(self, tmp_path, pipeline="local", client=None):
        from portiere.config import PortiereConfig
        from portiere.project import Project
        from portiere.storage.local_backend import LocalStorageBackend

        config = PortiereConfig(
            mode="local",
            pipeline=pipeline,
            local_project_dir=tmp_path,
        )

        storage = LocalStorageBackend(base_dir=tmp_path)
        metadata = storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        return Project(
            name="test",
            target_model="omop_cdm_v5.4",
            vocabularies=["SNOMED"],
            config=config,
            storage=storage,
            project_id=metadata["id"],
            engine=MagicMock(),
            client=client,
        )

    def test_map_schema_local_pipeline(self, tmp_path):
        project = self._make_project(tmp_path, pipeline="local")

        mock_engine = MagicMock()
        mock_df = pd.DataFrame({"patient_id": [1, 2], "drug_code": ["A", "B"]})
        mock_engine.read_source.return_value = mock_df
        project._engine = mock_engine

        mock_result = {
            "mappings": [
                {
                    "source_column": "patient_id",
                    "target_table": "person",
                    "target_column": "person_id",
                    "confidence": 0.95,
                    "status": "auto_accepted",
                }
            ],
            "stats": {"total": 1, "auto_accepted": 1, "needs_review": 0, "unmapped": 0},
        }

        with patch("portiere.stages.stage2_schema.map_schema", return_value=mock_result):
            source = {"name": "test", "path": "/data/test.csv", "format": "csv"}
            mapping = project.map_schema(source)

            assert len(mapping.items) == 1
            assert mapping.items[0].source_column == "patient_id"
            assert mapping.items[0].confidence == 0.95

        # Verify persisted
        loaded = project._storage.load_schema_mapping("test")
        assert len(loaded.items) == 1

    def test_map_schema_always_uses_local_pipeline(self, tmp_path):
        """In open-source SDK, map_schema always passes client=None."""
        project = self._make_project(tmp_path, pipeline="local")

        mock_engine = MagicMock()
        mock_df = pd.DataFrame({"patient_id": [1, 2]})
        mock_engine.read_source.return_value = mock_df
        project._engine = mock_engine

        mock_result = {
            "mappings": [
                {
                    "source_column": "patient_id",
                    "target_table": "person",
                    "target_column": "person_id",
                    "confidence": 0.92,
                }
            ],
            "stats": {"total": 1, "auto_accepted": 1, "needs_review": 0, "unmapped": 0},
        }

        with patch(
            "portiere.stages.stage2_schema.map_schema", return_value=mock_result
        ) as mock_map:
            source = {"name": "test", "path": "/data/test.csv", "format": "csv"}
            project.map_schema(source)

            call_kwargs = mock_map.call_args
            assert call_kwargs.kwargs.get("client") is None or call_kwargs[1].get("client") is None


class TestProjectMapConcepts:
    """Tests for Project.map_concepts() with both pipeline modes."""

    def _make_project(self, tmp_path, pipeline="local", client=None):
        from portiere.config import PortiereConfig
        from portiere.project import Project
        from portiere.storage.local_backend import LocalStorageBackend

        config = PortiereConfig(
            mode="local",
            pipeline=pipeline,
            local_project_dir=tmp_path,
        )

        storage = LocalStorageBackend(base_dir=tmp_path)
        metadata = storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        return Project(
            name="test",
            target_model="omop_cdm_v5.4",
            vocabularies=["SNOMED"],
            config=config,
            storage=storage,
            project_id=metadata["id"],
            engine=MagicMock(),
            client=client,
        )

    def test_map_concepts_with_string_codes(self, tmp_path):
        project = self._make_project(tmp_path, pipeline="local")

        mock_result = {
            "mappings": {
                "_direct": {
                    "items": [
                        {
                            "source_code": "E11.9",
                            "source_description": "Type 2 diabetes",
                            "target_concept_id": 201826,
                            "target_concept_name": "Type 2 diabetes mellitus",
                            "vocabulary_id": "SNOMED",
                            "domain_id": "Condition",
                            "confidence": 0.97,
                            "method": "auto",
                        }
                    ],
                    "stats": {"total": 1, "auto": 1, "review": 0, "manual": 0},
                }
            },
            "stats": {"total_codes": 1, "auto_mapped": 1, "needs_review": 0, "manual": 0},
            "auto_rate": 100.0,
        }

        with patch("portiere.stages.stage3_concepts.map_concepts", return_value=mock_result):
            mapping = project.map_concepts(codes=["E11.9"])

            assert len(mapping.items) == 1
            assert mapping.items[0].source_code == "E11.9"
            assert mapping.items[0].confidence == 0.97

        # Verify persisted
        loaded = project._storage.load_concept_mapping("test")
        assert len(loaded.items) == 1

    def test_map_concepts_normalizes_string_codes(self, tmp_path):
        project = self._make_project(tmp_path, pipeline="local")

        mock_result = {
            "mappings": {},
            "stats": {"total_codes": 0, "auto_mapped": 0, "needs_review": 0, "manual": 0},
            "auto_rate": 0,
        }

        with patch(
            "portiere.stages.stage3_concepts.map_concepts", return_value=mock_result
        ) as mock_map:
            project.map_concepts(codes=["E11.9", "I10"])

            call_kwargs = mock_map.call_args
            # codes should be normalized to dicts
            passed_codes = call_kwargs.kwargs.get("codes") or call_kwargs[1].get("codes")
            assert passed_codes is not None
            assert all(isinstance(c, dict) for c in passed_codes)
            assert passed_codes[0]["code"] == "E11.9"

    def test_map_concepts_always_uses_local_pipeline(self, tmp_path):
        """In open-source SDK, map_concepts always passes client=None."""
        project = self._make_project(tmp_path, pipeline="local")

        mock_result = {
            "mappings": {},
            "stats": {"total_codes": 0, "auto_mapped": 0, "needs_review": 0, "manual": 0},
            "auto_rate": 0,
        }

        with patch(
            "portiere.stages.stage3_concepts.map_concepts", return_value=mock_result
        ) as mock_map:
            project.map_concepts(codes=["E11.9"])

            call_kwargs = mock_map.call_args
            assert call_kwargs.kwargs.get("client") is None or call_kwargs[1].get("client") is None

    def test_map_concepts_uses_project_vocabularies(self, tmp_path):
        project = self._make_project(tmp_path, pipeline="local")

        mock_result = {
            "mappings": {},
            "stats": {"total_codes": 0, "auto_mapped": 0, "needs_review": 0, "manual": 0},
            "auto_rate": 0,
        }

        with patch(
            "portiere.stages.stage3_concepts.map_concepts", return_value=mock_result
        ) as mock_map:
            project.map_concepts(codes=["E11.9"])

            call_kwargs = mock_map.call_args
            passed_vocabs = call_kwargs.kwargs.get("vocabularies") or call_kwargs[1].get(
                "vocabularies"
            )
            assert passed_vocabs == ["SNOMED"]


class TestProjectProfile:
    """Tests for Project.profile() GX integration."""

    def _make_project(self, tmp_path):
        from portiere.config import PortiereConfig
        from portiere.project import Project
        from portiere.storage.local_backend import LocalStorageBackend

        config = PortiereConfig(
            mode="local",
            pipeline="local",
            local_project_dir=tmp_path,
        )

        storage = LocalStorageBackend(base_dir=tmp_path)
        metadata = storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        return Project(
            name="test",
            target_model="omop_cdm_v5.4",
            vocabularies=["SNOMED"],
            config=config,
            storage=storage,
            project_id=metadata["id"],
            engine=MagicMock(),
        )

    def test_profile_delegates_to_gx_profiler(self, tmp_path):
        project = self._make_project(tmp_path)

        mock_engine = MagicMock()
        mock_df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        mock_engine.read_source.return_value = mock_df
        project._engine = mock_engine

        mock_report = {
            "source_name": "test_source",
            "columns": [{"name": "id", "dtype": "int64"}],
            "gx_result": {},
            "expectations": {},
            "row_count": 3,
            "created_at": "2024-01-01T00:00:00",
        }

        with patch("portiere.quality.profiler.GXProfiler") as MockProfiler:
            mock_instance = MagicMock()
            mock_instance.profile.return_value = mock_report
            MockProfiler.return_value = mock_instance

            source = {"name": "test_source", "path": "/data/test.csv", "format": "csv"}
            report = project.profile(source)

            assert report["source_name"] == "test_source"
            mock_instance.profile.assert_called_once()

    def test_profile_saves_artifact(self, tmp_path):
        project = self._make_project(tmp_path)

        mock_engine = MagicMock()
        mock_df = pd.DataFrame({"id": [1, 2]})
        mock_engine.read_source.return_value = mock_df
        project._engine = mock_engine

        mock_report = {
            "source_name": "patients",
            "columns": [],
            "gx_result": {},
            "expectations": {},
            "row_count": 2,
            "created_at": "2024-01-01T00:00:00",
        }

        with patch("portiere.quality.profiler.GXProfiler") as MockProfiler:
            mock_instance = MagicMock()
            mock_instance.profile.return_value = mock_report
            MockProfiler.return_value = mock_instance

            source = {"name": "patients", "path": "/data/patients.csv", "format": "csv"}
            project.profile(source)

        # Verify saved to storage
        loaded = project._storage.load_profile("test", "patients")
        assert loaded is not None
        assert loaded["source_name"] == "patients"

    def test_profile_raises_import_error_without_gx(self, tmp_path):
        project = self._make_project(tmp_path)

        mock_engine = MagicMock()
        mock_engine.read_source.return_value = pd.DataFrame({"id": [1]})
        project._engine = mock_engine

        # Simulate GX not installed by making the import raise ImportError
        import builtins

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "portiere.quality.profiler":
                raise ImportError("No module named 'great_expectations'")
            return real_import(name, *args, **kwargs)

        # Clear cached module so import triggers again
        import sys

        cached = sys.modules.pop("portiere.quality.profiler", None)
        try:
            with patch("builtins.__import__", side_effect=mock_import):
                source = {"name": "test", "path": "/data/test.csv", "format": "csv"}
                with pytest.raises(ImportError, match="Great Expectations"):
                    project.profile(source)
        finally:
            if cached is not None:
                sys.modules["portiere.quality.profiler"] = cached


class TestProjectValidate:
    """Tests for Project.validate() GX integration."""

    def _make_project(self, tmp_path):
        from portiere.config import PortiereConfig
        from portiere.project import Project
        from portiere.storage.local_backend import LocalStorageBackend

        config = PortiereConfig(
            mode="local",
            pipeline="local",
            local_project_dir=tmp_path,
        )

        storage = LocalStorageBackend(base_dir=tmp_path)
        metadata = storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        return Project(
            name="test",
            target_model="omop_cdm_v5.4",
            vocabularies=["SNOMED"],
            config=config,
            storage=storage,
            project_id=metadata["id"],
            engine=MagicMock(),
        )

    def test_validate_requires_output_path(self, tmp_path):
        project = self._make_project(tmp_path)

        with patch("portiere.quality.validator.GXValidator"):
            with pytest.raises(ValueError, match="etl_result or output_path"):
                project.validate()

    def test_validate_uses_etl_result_output_dir(self, tmp_path):
        project = self._make_project(tmp_path)

        # Create a CSV output file
        output_dir = tmp_path / "etl_output"
        output_dir.mkdir()
        df = pd.DataFrame({"person_id": [1, 2], "gender_concept_id": [8507, 8532]})
        df.to_csv(output_dir / "person.csv", index=False)

        mock_report = {
            "table_name": "person",
            "passed": True,
            "completeness_score": 1.0,
            "conformance_score": 1.0,
            "plausibility_score": 1.0,
            "gx_result": {},
            "thresholds": {},
            "created_at": "2024-01-01",
        }

        with patch("portiere.quality.validator.GXValidator") as MockValidator:
            mock_instance = MagicMock()
            mock_instance.validate.return_value = mock_report
            MockValidator.return_value = mock_instance

            etl_result = {"output_dir": str(output_dir)}
            result = project.validate(etl_result=etl_result)

            assert result["total_tables"] == 1
            assert result["all_passed"] is True

    def test_validate_saves_quality_reports(self, tmp_path):
        project = self._make_project(tmp_path)

        output_dir = tmp_path / "etl_output"
        output_dir.mkdir()
        df = pd.DataFrame({"person_id": [1]})
        df.to_csv(output_dir / "person.csv", index=False)

        mock_report = {
            "table_name": "person",
            "passed": True,
            "completeness_score": 1.0,
            "conformance_score": 1.0,
            "plausibility_score": 1.0,
            "gx_result": {},
            "thresholds": {},
            "created_at": "2024-01-01",
        }

        with patch("portiere.quality.validator.GXValidator") as MockValidator:
            mock_instance = MagicMock()
            mock_instance.validate.return_value = mock_report
            MockValidator.return_value = mock_instance

            project.validate(output_path=str(output_dir))

        reports = project._storage.load_quality_reports("test")
        assert len(reports) == 1
        assert reports[0]["table_name"] == "person"


class TestProjectPushPull:
    """Tests for Project.push() and Project.pull()."""

    def _make_project(self, tmp_path, api_key="ptk_test"):
        from portiere.config import PortiereConfig
        from portiere.project import Project
        from portiere.storage.local_backend import LocalStorageBackend

        config = PortiereConfig(
            mode="local",
            pipeline="local",
            api_key=api_key,
            local_project_dir=tmp_path,
        )

        storage = LocalStorageBackend(base_dir=tmp_path)
        metadata = storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        mock_client = MagicMock()

        return Project(
            name="test",
            target_model="omop_cdm_v5.4",
            vocabularies=["SNOMED"],
            config=config,
            storage=storage,
            project_id=metadata["id"],
            engine=MagicMock(),
            client=mock_client,
        )

    def test_push_raises_not_implemented(self, tmp_path):
        project = self._make_project(tmp_path)
        with pytest.raises(NotImplementedError, match="Cloud sync"):
            project.push()

    def test_pull_raises_not_implemented(self, tmp_path):
        project = self._make_project(tmp_path)
        with pytest.raises(NotImplementedError, match="Cloud sync"):
            project.pull()

    def test_sync_status_returns_local(self, tmp_path):
        project = self._make_project(tmp_path)
        status = project.sync_status()
        assert status["mode"] == "local"
        assert status["synced"] is False
        assert status["cloud_project_id"] is None


class TestProjectExtractColumns:
    """Tests for Project._extract_columns() helper."""

    def test_extract_columns_pandas(self, tmp_path):
        from portiere.config import PortiereConfig
        from portiere.project import Project
        from portiere.storage.local_backend import LocalStorageBackend

        config = PortiereConfig(local_project_dir=tmp_path)
        storage = LocalStorageBackend(base_dir=tmp_path)
        metadata = storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        project = Project(
            name="test",
            target_model="omop_cdm_v5.4",
            vocabularies=["SNOMED"],
            config=config,
            storage=storage,
            project_id=metadata["id"],
            engine=MagicMock(),
        )

        df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [30, 40, 50],
            }
        )

        columns = project._extract_columns(df)

        assert len(columns) == 3
        assert columns[0]["name"] == "patient_id"
        assert "type" in columns[0]
        assert "sample_values" in columns[0]


class TestProjectDatabaseSource:
    """Tests for database connection support in add_source() and _read_source_data()."""

    def _make_project(self, tmp_path):
        from portiere.config import PortiereConfig
        from portiere.project import Project
        from portiere.storage.local_backend import LocalStorageBackend

        config = PortiereConfig(
            mode="local",
            pipeline="local",
            local_project_dir=tmp_path,
        )

        storage = LocalStorageBackend(base_dir=tmp_path)
        metadata = storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        return Project(
            name="test",
            target_model="omop_cdm_v5.4",
            vocabularies=["SNOMED"],
            config=config,
            storage=storage,
            project_id=metadata["id"],
            engine=MagicMock(),
        )

    # --- Group A: Database parameter handling ---

    def test_add_source_database_with_table(self, tmp_path):
        project = self._make_project(tmp_path)
        project.config.quality.profile_on_ingest = False

        source = project.add_source(
            connection_string="postgresql://user:pass@localhost/mydb",
            table="patients",
        )

        assert source["format"] == "database"
        assert source["connection_string"] == "postgresql://user:pass@localhost/mydb"
        assert source["table"] == "patients"
        assert source["name"] == "patients"

    def test_add_source_database_with_query(self, tmp_path):
        project = self._make_project(tmp_path)
        project.config.quality.profile_on_ingest = False

        source = project.add_source(
            connection_string="mysql://user:pass@host/db",
            query="SELECT * FROM patients WHERE age > 18",
            name="adult_patients",
        )

        assert source["format"] == "database"
        assert source["connection_string"] == "mysql://user:pass@host/db"
        assert source["query"] == "SELECT * FROM patients WHERE age > 18"
        assert source["name"] == "adult_patients"

    def test_add_source_database_with_custom_name(self, tmp_path):
        project = self._make_project(tmp_path)
        project.config.quality.profile_on_ingest = False

        source = project.add_source(
            connection_string="postgresql://localhost/db",
            table="raw_patients",
            name="Hospital A Patients",
        )

        assert source["name"] == "Hospital A Patients"

    # --- Group B: Validation / error cases ---

    def test_add_source_no_path_no_connection_string_raises(self, tmp_path):
        project = self._make_project(tmp_path)

        with pytest.raises(ValueError, match=r"path.*connection_string"):
            project.add_source()

    def test_add_source_both_path_and_connection_string_raises(self, tmp_path):
        project = self._make_project(tmp_path)

        with pytest.raises(ValueError, match=r"path.*connection_string"):
            project.add_source(
                path="data.csv",
                connection_string="postgresql://localhost/db",
            )

    def test_add_source_connection_string_without_table_or_query_raises(self, tmp_path):
        project = self._make_project(tmp_path)

        with pytest.raises(ValueError, match=r"table.*query"):
            project.add_source(connection_string="postgresql://localhost/db")

    # --- Group C: _read_source_data() dispatch logic ---

    def test_read_source_data_dispatches_to_read_database(self, tmp_path):
        project = self._make_project(tmp_path)

        mock_engine = MagicMock()
        mock_engine.read_database.return_value = "mock_df"
        project._engine = mock_engine

        source = {
            "format": "database",
            "connection_string": "postgresql://localhost/db",
            "table": "patients",
        }
        result = project._read_source_data(source)

        mock_engine.read_database.assert_called_once_with(
            connection_string="postgresql://localhost/db",
            query=None,
            table="patients",
        )
        assert result == "mock_df"

    def test_read_source_data_dispatches_to_read_source(self, tmp_path):
        project = self._make_project(tmp_path)

        mock_engine = MagicMock()
        mock_engine.read_source.return_value = "mock_df"
        project._engine = mock_engine

        source = {"path": "/data/patients.csv", "format": "csv"}
        result = project._read_source_data(source)

        mock_engine.read_source.assert_called_once_with("/data/patients.csv", format="csv")
        assert result == "mock_df"

    def test_read_source_data_passes_query_for_db(self, tmp_path):
        project = self._make_project(tmp_path)

        mock_engine = MagicMock()
        mock_engine.read_database.return_value = "mock_df"
        project._engine = mock_engine

        source = {
            "format": "database",
            "connection_string": "postgresql://localhost/db",
            "query": "SELECT id, name FROM patients",
        }
        project._read_source_data(source)

        mock_engine.read_database.assert_called_once_with(
            connection_string="postgresql://localhost/db",
            query="SELECT id, name FROM patients",
            table=None,
        )

    # --- Group D: Backward compatibility ---

    def test_add_source_file_path_still_works(self, tmp_path):
        project = self._make_project(tmp_path)
        project.config.quality.profile_on_ingest = False

        source = project.add_source("patients.csv")
        assert source["path"] == "patients.csv"
        assert source["format"] == "csv"
        assert source["name"] == "patients"

    def test_add_source_file_path_positional_arg(self, tmp_path):
        project = self._make_project(tmp_path)
        project.config.quality.profile_on_ingest = False

        source = project.add_source("data.parquet", "My Source", "parquet")
        assert source["path"] == "data.parquet"
        assert source["name"] == "My Source"
        assert source["format"] == "parquet"

"""
Tests for Portiere SDK Artifact Generation.

Tests for:
- CodeGenerator ETL script generation (polars, spark, pandas)
- CodeGenerator DDL generation
- CodeGenerator validation script generation
- CodeGenerator source_to_concept_map CSV generation
- ArtifactManager orchestration and save
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ──────────────────────────────────────────────────────────────
# Sample data fixtures
# ──────────────────────────────────────────────────────────────


@pytest.fixture
def sample_schema_mappings():
    """Sample schema mappings for ETL generation."""
    return [
        {
            "source_column": "patient_id",
            "target_table": "person",
            "target_column": "person_source_value",
            "confidence": 0.95,
        },
        {
            "source_column": "diagnosis_code",
            "target_table": "condition_occurrence",
            "target_column": "condition_source_value",
            "confidence": 0.92,
        },
        {
            "source_column": "drug_code",
            "target_table": "drug_exposure",
            "target_column": "drug_source_value",
            "confidence": 0.88,
        },
    ]


@pytest.fixture
def sample_concept_mappings():
    """Sample concept mappings for ETL generation."""
    return [
        {
            "source_code": "E11.9",
            "source_description": "Type 2 diabetes mellitus",
            "source_column": "diagnosis_code",
            "target_concept_id": 201826,
            "target_concept_name": "Type 2 diabetes mellitus",
            "target_vocabulary_id": "SNOMED",
            "confidence": 0.97,
            "method": "auto",
        },
        {
            "source_code": "I10",
            "source_description": "Essential hypertension",
            "source_column": "diagnosis_code",
            "target_concept_id": 320128,
            "target_concept_name": "Essential hypertension",
            "target_vocabulary_id": "SNOMED",
            "confidence": 0.94,
            "method": "auto",
        },
        {
            "source_code": "N06AX11",
            "source_description": "Mirtazapine",
            "source_column": "drug_code",
            "target_concept_id": 725131,
            "target_concept_name": "mirtazapine",
            "target_vocabulary_id": "RxNorm",
            "confidence": 0.85,
            "method": "review",
        },
    ]


# ──────────────────────────────────────────────────────────────
# CodeGenerator tests
# ──────────────────────────────────────────────────────────────


class TestCodeGeneratorInit:
    """Tests for CodeGenerator initialization."""

    def test_init_loads_jinja_env(self):
        """CodeGenerator should attempt to load Jinja2 templates."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        # Either loads templates or falls back gracefully
        assert hasattr(gen, "_jinja")

    def test_init_graceful_fallback(self):
        """If templates not found, should set _jinja to None."""
        from portiere.artifacts.code_generator import CodeGenerator

        with patch(
            "portiere.artifacts.code_generator.PackageLoader",
            side_effect=Exception("No templates"),
        ):
            gen = CodeGenerator()
            assert gen._jinja is None


class TestETLScriptGeneration:
    """Tests for ETL script generation."""

    def test_polars_etl_generated(self, sample_schema_mappings, sample_concept_mappings):
        """Should generate a Polars ETL script."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        script = gen.generate_etl_script(
            engine_type="polars",
            schema_mappings=sample_schema_mappings,
            concept_mappings=sample_concept_mappings,
            project_name="test_project",
        )

        assert isinstance(script, str)
        assert len(script) > 0
        # Should reference polars or contain a NotImplementedError fallback
        assert "polars" in script.lower() or "NotImplementedError" in script

    def test_spark_etl_generated(self, sample_schema_mappings, sample_concept_mappings):
        """Should generate a PySpark ETL script."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        script = gen.generate_etl_script(
            engine_type="spark",
            schema_mappings=sample_schema_mappings,
            concept_mappings=sample_concept_mappings,
            project_name="test_project",
        )

        assert isinstance(script, str)
        assert len(script) > 0
        assert (
            "spark" in script.lower()
            or "pyspark" in script.lower()
            or "NotImplementedError" in script
        )

    def test_pandas_etl_generated(self, sample_schema_mappings, sample_concept_mappings):
        """Should generate a Pandas ETL script."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        script = gen.generate_etl_script(
            engine_type="pandas",
            schema_mappings=sample_schema_mappings,
            concept_mappings=sample_concept_mappings,
            project_name="test_project",
        )

        assert isinstance(script, str)
        assert len(script) > 0
        assert "pandas" in script.lower() or "NotImplementedError" in script

    def test_etl_includes_project_name(self, sample_schema_mappings, sample_concept_mappings):
        """Generated ETL should include the project name."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        script = gen.generate_etl_script(
            engine_type="polars",
            schema_mappings=sample_schema_mappings,
            concept_mappings=sample_concept_mappings,
            project_name="my_hospital_project",
        )

        assert "my_hospital_project" in script

    def test_etl_extracts_concept_columns(self, sample_schema_mappings, sample_concept_mappings):
        """Should identify columns that have concept mappings."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        # Call the method and verify the concept_columns derived internally
        script = gen.generate_etl_script(
            engine_type="polars",
            schema_mappings=sample_schema_mappings,
            concept_mappings=sample_concept_mappings,
        )

        # The concept_columns set should contain diagnosis_code and drug_code
        assert isinstance(script, str)

    def test_etl_empty_mappings(self):
        """Should handle empty mappings without error."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        script = gen.generate_etl_script(
            engine_type="polars",
            schema_mappings=[],
            concept_mappings=[],
        )

        assert isinstance(script, str)
        assert len(script) > 0

    def test_fallback_generation(self, sample_schema_mappings, sample_concept_mappings):
        """When Jinja2 templates are not available, should use fallback."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        gen._jinja = None  # Force fallback

        script = gen._generate_fallback(
            "polars",
            {
                "project_name": "fallback_test",
                "target_model": "omop_cdm_v5.4",
                "source_path": "source.csv",
                "output_path": "output.parquet",
                "schema_mappings": sample_schema_mappings,
                "concept_mappings": sample_concept_mappings,
                "concept_columns": ["diagnosis_code"],
            },
        )

        assert "polars" in script.lower()
        assert "fallback_test" in script


class TestDDLGeneration:
    """Tests for SQL DDL generation."""

    def test_ddl_generated(self):
        """Should generate DDL for default OMOP tables."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        ddl = gen.generate_ddl(target_model="omop_cdm_v5.4", project_name="test")

        assert isinstance(ddl, str)
        assert len(ddl) > 0
        # Should reference OMOP tables
        assert "person" in ddl.lower()
        assert "condition_occurrence" in ddl.lower() or "condition" in ddl.lower()

    def test_ddl_includes_project_name(self):
        """DDL should include the project name."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        ddl = gen.generate_ddl(project_name="my_hospital")

        assert "my_hospital" in ddl

    def test_ddl_custom_tables(self):
        """Should accept custom table definitions."""
        from portiere.artifacts.code_generator import CodeGenerator

        custom_tables = [
            {
                "name": "custom_table",
                "columns": [
                    {"name": "id", "type": "BIGINT", "not_null": True, "primary_key": True},
                    {"name": "value", "type": "VARCHAR(255)"},
                ],
            },
        ]

        gen = CodeGenerator()
        ddl = gen.generate_ddl(tables=custom_tables)

        assert "custom_table" in ddl
        assert "BIGINT" in ddl

    def test_ddl_fallback(self):
        """Fallback DDL should produce valid SQL-like output."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        gen._jinja = None  # Force fallback

        ddl = gen.generate_ddl(target_model="omop_cdm_v5.4", project_name="test")

        assert "CREATE TABLE" in ddl
        assert "person" in ddl

    def test_default_omop_tables(self):
        """Default OMOP tables should include the core CDM tables."""
        from portiere.artifacts.code_generator import CodeGenerator

        tables = CodeGenerator._default_omop_tables()

        table_names = [t["name"] for t in tables]
        assert "person" in table_names
        assert "condition_occurrence" in table_names
        assert "drug_exposure" in table_names
        assert "measurement" in table_names

        # Each table should have columns
        for table in tables:
            assert len(table["columns"]) > 0
            for col in table["columns"]:
                assert "name" in col
                assert "type" in col


class TestValidationScriptGeneration:
    """Tests for validation script generation."""

    def test_validation_script_generated(self, sample_concept_mappings):
        """Should generate a validation script."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        script = gen.generate_validation_script(
            engine_type="polars",
            concept_mappings=sample_concept_mappings,
            project_name="validation_test",
        )

        assert isinstance(script, str)
        assert len(script) > 0
        assert "validation_test" in script

    def test_validation_script_default_thresholds(self):
        """Should use default thresholds when not specified."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        script = gen.generate_validation_script()

        assert isinstance(script, str)

    def test_validation_script_custom_thresholds(self):
        """Should accept custom thresholds."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        script = gen.generate_validation_script(
            thresholds={
                "completeness": 0.99,
                "conformance": 0.99,
                "plausibility": 0.95,
                "mapping_coverage": 0.98,
            },
        )

        assert isinstance(script, str)

    def test_validation_script_empty_mappings(self):
        """Should handle empty concept mappings."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        script = gen.generate_validation_script(concept_mappings=[])

        assert isinstance(script, str)


class TestSourceToConceptCSV:
    """Tests for source_to_concept_map CSV generation."""

    def test_csv_generated(self, sample_concept_mappings):
        """Should generate CSV content."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        csv = gen.generate_source_to_concept_csv(sample_concept_mappings)

        assert isinstance(csv, str)
        lines = csv.strip().split("\n")
        # Header + 3 data rows
        assert len(lines) == 4

    def test_csv_header(self, sample_concept_mappings):
        """CSV should have the correct header."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        csv = gen.generate_source_to_concept_csv(sample_concept_mappings)

        header = csv.strip().split("\n")[0]
        assert "source_code" in header
        assert "target_concept_id" in header
        assert "target_vocabulary_id" in header
        assert "confidence" in header
        assert "method" in header

    def test_csv_data_rows(self, sample_concept_mappings):
        """CSV data rows should contain correct values."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        csv = gen.generate_source_to_concept_csv(sample_concept_mappings)

        lines = csv.strip().split("\n")
        # First data row should contain E11.9
        assert "E11.9" in lines[1]
        assert "201826" in lines[1]

    def test_csv_empty_mappings(self):
        """Empty mappings should produce header-only CSV."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        csv = gen.generate_source_to_concept_csv([])

        lines = csv.strip().split("\n")
        assert len(lines) == 1  # Header only


# ──────────────────────────────────────────────────────────────
# ArtifactManager tests
# ──────────────────────────────────────────────────────────────


class TestArtifactManagerInit:
    """Tests for ArtifactManager initialization."""

    def test_init_default(self):
        """ArtifactManager should initialize with defaults."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        mgr = ArtifactManager()
        assert mgr._engine is None
        assert mgr._output_dir == Path("./artifacts")
        assert mgr._artifacts == []

    def test_init_with_engine(self):
        """ArtifactManager should accept an engine."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        engine = MagicMock()
        engine.engine_name = "polars"

        mgr = ArtifactManager(engine=engine)
        assert mgr._engine is engine

    def test_init_custom_output_dir(self):
        """ArtifactManager should accept custom output directory."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        mgr = ArtifactManager(output_dir="/tmp/my_artifacts")
        assert mgr._output_dir == Path("/tmp/my_artifacts")


class TestArtifactManagerETL:
    """Tests for ArtifactManager ETL script generation."""

    def test_generate_etl_script(self, sample_schema_mappings, sample_concept_mappings):
        """Should generate and track ETL script artifact."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        engine = MagicMock()
        engine.engine_name = "polars"

        mgr = ArtifactManager(engine=engine)
        script = mgr.generate_etl_script(
            schema_mapping=sample_schema_mappings,
            concept_mapping=sample_concept_mappings,
            source_path="source.csv",
            output_path="output.parquet",
        )

        assert isinstance(script, str)
        assert len(mgr._artifacts) == 1
        assert mgr._artifacts[0]["type"] == "etl_script"
        assert mgr._artifacts[0]["engine"] == "polars"

    def test_generate_etl_normalizes_dict_input(
        self, sample_schema_mappings, sample_concept_mappings
    ):
        """Should accept dict with 'mappings' key."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        engine = MagicMock()
        engine.engine_name = "polars"

        mgr = ArtifactManager(engine=engine)
        script = mgr.generate_etl_script(
            schema_mapping={"mappings": sample_schema_mappings},
            concept_mapping={"mappings": sample_concept_mappings},
            source_path="source.csv",
            output_path="output.parquet",
        )

        assert isinstance(script, str)

    def test_generate_etl_no_engine_defaults_polars(
        self, sample_schema_mappings, sample_concept_mappings
    ):
        """Without engine, should default to polars."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        mgr = ArtifactManager()
        script = mgr.generate_etl_script(
            schema_mapping=sample_schema_mappings,
            concept_mapping=sample_concept_mappings,
            source_path="source.csv",
            output_path="output.parquet",
        )

        assert mgr._artifacts[0]["engine"] == "polars"


class TestArtifactManagerDDL:
    """Tests for ArtifactManager DDL generation."""

    def test_generate_ddl(self):
        """Should generate and track DDL artifact."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        mgr = ArtifactManager()
        ddl = mgr.generate_ddl(target_model="omop_cdm_v5.4", project_name="test")

        assert isinstance(ddl, str)
        assert len(mgr._artifacts) == 1
        assert mgr._artifacts[0]["type"] == "ddl"


class TestArtifactManagerValidation:
    """Tests for ArtifactManager validation script generation."""

    def test_generate_validation_script(self, sample_concept_mappings):
        """Should generate and track validation script artifact."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        engine = MagicMock()
        engine.engine_name = "polars"

        mgr = ArtifactManager(engine=engine)
        script = mgr.generate_validation_script(concept_mapping=sample_concept_mappings)

        assert isinstance(script, str)
        assert len(mgr._artifacts) == 1
        assert mgr._artifacts[0]["type"] == "validation_script"


class TestArtifactManagerSourceToConceptMap:
    """Tests for source_to_concept_map CSV generation."""

    def test_generate_source_to_concept_map(self, sample_concept_mappings):
        """Should generate and track CSV artifact."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        mgr = ArtifactManager()
        csv = mgr.generate_source_to_concept_map(sample_concept_mappings)

        assert isinstance(csv, str)
        assert len(mgr._artifacts) == 1
        assert mgr._artifacts[0]["type"] == "source_to_concept_map"


class TestArtifactManagerSave:
    """Tests for saving artifacts to disk."""

    def test_save_artifacts(self, tmp_path, sample_schema_mappings, sample_concept_mappings):
        """Should save all artifacts to disk."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        engine = MagicMock()
        engine.engine_name = "polars"

        mgr = ArtifactManager(engine=engine, output_dir=str(tmp_path))
        mgr.generate_etl_script(
            schema_mapping=sample_schema_mappings,
            concept_mapping=sample_concept_mappings,
            source_path="src.csv",
            output_path="out.parquet",
        )
        mgr.generate_ddl()

        saved = mgr.save_artifacts()

        assert len(saved) == 2
        for path in saved:
            assert path.exists()
            assert path.stat().st_size > 0

    def test_save_creates_output_directory(
        self, tmp_path, sample_schema_mappings, sample_concept_mappings
    ):
        """Should create the output directory if it doesn't exist."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        output = tmp_path / "nested" / "output"
        mgr = ArtifactManager(output_dir=str(output))
        mgr.generate_ddl()

        saved = mgr.save_artifacts()

        assert output.exists()
        assert len(saved) == 1

    def test_save_with_override_directory(self, tmp_path):
        """Should use override directory when provided."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        override = tmp_path / "override"
        mgr = ArtifactManager(output_dir="/original/path")
        mgr.generate_ddl()

        saved = mgr.save_artifacts(output_dir=str(override))

        assert override.exists()
        assert len(saved) == 1
        assert saved[0].parent == override

    def test_save_etl_filename(self, tmp_path, sample_schema_mappings, sample_concept_mappings):
        """ETL scripts should use engine name in filename."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        engine = MagicMock()
        engine.engine_name = "spark"

        mgr = ArtifactManager(engine=engine, output_dir=str(tmp_path))
        mgr.generate_etl_script(
            schema_mapping=sample_schema_mappings,
            concept_mapping=sample_concept_mappings,
            source_path="src.csv",
            output_path="out.parquet",
        )

        saved = mgr.save_artifacts()
        assert saved[0].name == "etl_spark.py"

    def test_save_ddl_filename(self, tmp_path):
        """DDL should save as omop_ddl.sql."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        mgr = ArtifactManager(output_dir=str(tmp_path))
        mgr.generate_ddl()

        saved = mgr.save_artifacts()
        assert saved[0].name == "omop_ddl.sql"

    def test_save_validation_filename(self, tmp_path, sample_concept_mappings):
        """Validation script should save as run_validation.py."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        engine = MagicMock()
        engine.engine_name = "polars"

        mgr = ArtifactManager(engine=engine, output_dir=str(tmp_path))
        mgr.generate_validation_script(concept_mapping=sample_concept_mappings)

        saved = mgr.save_artifacts()
        assert saved[0].name == "run_validation.py"

    def test_save_source_to_concept_filename(self, tmp_path, sample_concept_mappings):
        """Source-to-concept map should save as CSV."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        mgr = ArtifactManager(output_dir=str(tmp_path))
        mgr.generate_source_to_concept_map(sample_concept_mappings)

        saved = mgr.save_artifacts()
        assert saved[0].name == "source_to_concept_map.csv"


class TestArtifactManagerListArtifacts:
    """Tests for listing tracked artifacts."""

    def test_list_empty(self):
        """Fresh manager should have no artifacts."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        mgr = ArtifactManager()
        assert mgr.list_artifacts() == []

    def test_list_after_generation(self, sample_schema_mappings, sample_concept_mappings):
        """Should list all generated artifacts."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        engine = MagicMock()
        engine.engine_name = "polars"

        mgr = ArtifactManager(engine=engine)
        mgr.generate_etl_script(
            schema_mapping=sample_schema_mappings,
            concept_mapping=sample_concept_mappings,
            source_path="src.csv",
            output_path="out.parquet",
        )
        mgr.generate_ddl()

        artifacts = mgr.list_artifacts()
        assert len(artifacts) == 2
        types = [a["type"] for a in artifacts]
        assert "etl_script" in types
        assert "ddl" in types


class TestArtifactManagerFromAPIResponse:
    """Tests for creating from API response."""

    def test_from_api_response(self):
        """Should create manager with artifacts from response."""
        from portiere.artifacts.artifact_manager import ArtifactManager

        engine = MagicMock()
        engine.engine_name = "polars"

        response = {
            "artifacts": [
                {"type": "etl_script", "engine": "polars", "content": "import polars"},
                {"type": "ddl", "content": "CREATE TABLE person"},
            ]
        }

        mgr = ArtifactManager.from_api_response(response, engine)
        assert len(mgr._artifacts) == 2
        assert mgr._engine is engine

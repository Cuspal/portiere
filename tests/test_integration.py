"""
Portiere SDK Integration Tests.

Full pipeline test: CSV -> Stage 1 (profile) -> Stage 2 (schema map)
-> Stage 3 (concept map) -> Stage 4 (ETL gen) -> Stage 5 (validate)
"""

import os
import py_compile
from unittest.mock import MagicMock


class TestFullPipeline:
    """Test the complete 5-stage pipeline end-to-end."""

    def test_stage4_generates_compilable_pandas_script(self, tmp_path):
        """Stage 4 generates a valid Python script for Pandas."""
        from portiere.stages.stage4_transform import _generate_pandas_etl

        schema_mapping = {
            "items": [
                {"source_column": "diag_code", "target_column": "condition_source_value"},
                {"source_column": "patient_id", "target_column": "person_id"},
            ]
        }
        concept_mapping = {
            "items": [
                {
                    "source_code": "E11.9",
                    "source_column": "diag_code",
                    "target_concept_id": 201826,
                    "target_concept_name": "Type 2 diabetes mellitus",
                    "confidence": 0.97,
                    "method": "auto",
                }
            ]
        }

        script = _generate_pandas_etl(
            schema_mapping=schema_mapping,
            concept_mapping=concept_mapping,
            source_path="/data/input.csv",
            output_path="/data/output",
        )

        # Verify script is valid Python
        script_path = tmp_path / "test_etl.py"
        script_path.write_text(script)
        py_compile.compile(str(script_path), doraise=True)

        # Verify script contains expected elements
        assert "import pandas" in script
        assert "pd.read_csv" in script
        assert "to_parquet" in script
        assert "diag_code" in script
        assert "condition_source_value" in script

    def test_stage4_generates_compilable_spark_script(self, tmp_path):
        """Stage 4 generates a valid Python script for Spark."""
        from portiere.stages.stage4_transform import _generate_spark_etl

        script = _generate_spark_etl(
            schema_mapping={"items": []},
            concept_mapping={"items": []},
            source_path="/data/input.csv",
            output_path="/data/output",
        )

        script_path = tmp_path / "test_spark_etl.py"
        script_path.write_text(script)
        py_compile.compile(str(script_path), doraise=True)

        assert "from pyspark.sql import SparkSession" in script
        assert "spark.read.csv" in script

    def test_stage4_generates_compilable_polars_script(self, tmp_path):
        """Stage 4 generates a valid Python script for Polars."""
        from portiere.stages.stage4_transform import _generate_polars_etl

        script = _generate_polars_etl(
            schema_mapping={
                "items": [
                    {"source_column": "code", "target_column": "concept_code"},
                ]
            },
            concept_mapping={"items": []},
            source_path="/data/input.csv",
            output_path="/data/output",
        )

        script_path = tmp_path / "test_polars_etl.py"
        script_path.write_text(script)
        py_compile.compile(str(script_path), doraise=True)

        assert "import polars" in script
        assert "pl.read_csv" in script
        assert "concept_code" in script

    def test_stage4_lookup_table_flat_format(self, tmp_path):
        """Stage 4 generates lookup CSV from flat items list."""
        from portiere.stages.stage4_transform import _generate_lookup_table

        concept_mapping = {
            "items": [
                {
                    "source_code": "E11.9",
                    "source_column": "diagnosis_code",
                    "target_concept_id": 201826,
                    "target_concept_name": "Type 2 diabetes",
                    "confidence": 0.97,
                    "method": "auto",
                },
                {
                    "source_code": "I10",
                    "source_column": "diagnosis_code",
                    "target_concept_id": 320128,
                    "target_concept_name": "Essential hypertension",
                    "confidence": 0.96,
                    "method": "auto",
                },
            ]
        }

        lookup_path = tmp_path / "lookup.csv"
        _generate_lookup_table(concept_mapping, lookup_path)

        content = lookup_path.read_text()
        lines = content.strip().split("\n")
        assert (
            lines[0]
            == "source_code,source_column,target_concept_id,target_concept_name,confidence,method"
        )
        assert len(lines) == 3  # header + 2 rows
        assert "E11.9" in lines[1]
        assert "I10" in lines[2]

    def test_stage4_lookup_table_nested_format(self, tmp_path):
        """Stage 4 generates lookup CSV from nested mappings dict."""
        from portiere.stages.stage4_transform import _generate_lookup_table

        concept_mapping = {
            "mappings": {
                "diagnosis_code": {
                    "items": [
                        {
                            "source_code": "E11.9",
                            "target_concept_id": 201826,
                            "target_concept_name": "Type 2 diabetes",
                            "confidence": 0.97,
                            "method": "auto",
                        }
                    ]
                }
            }
        }

        lookup_path = tmp_path / "lookup_nested.csv"
        _generate_lookup_table(concept_mapping, lookup_path)

        content = lookup_path.read_text()
        assert "E11.9" in content
        assert "201826" in content

    def test_stage5_validate_generates_report(self):
        """Stage 5 generates a meaningful validation report."""
        from portiere.stages.stage5_validate import generate_qa_report

        validation_result = {
            "valid": True,
            "issues": [
                {
                    "table": "condition_occurrence",
                    "type": "unmapped_concepts",
                    "message": "10 rows have concept_id = 0",
                    "severity": "warning",
                }
            ],
            "stats": {
                "tables_checked": 3,
                "rows_checked": 1000,
                "valid_rows": 990,
                "invalid_rows": 10,
            },
            "validity_rate": 99.0,
        }

        report = generate_qa_report(validation_result)
        assert "PASSED" in report
        assert "Tables checked" in report
        assert "99.0%" in report
        assert "WARNINGS" in report
        assert "condition_occurrence" in report

    def test_stage5_validate_fails_on_errors(self):
        """Stage 5 report shows FAILED when there are errors."""
        from portiere.stages.stage5_validate import generate_qa_report

        validation_result = {
            "valid": False,
            "issues": [
                {
                    "table": "person",
                    "type": "missing_column",
                    "message": "Required column 'person_id' not found",
                    "severity": "error",
                }
            ],
            "stats": {
                "tables_checked": 1,
                "rows_checked": 0,
                "valid_rows": 0,
                "invalid_rows": 0,
            },
            "validity_rate": 0,
        }

        report = generate_qa_report(validation_result)
        assert "FAILED" in report
        assert "ERRORS" in report
        assert "person_id" in report


class TestConceptMappingModel:
    """Test ConceptMapping model integration."""

    def test_full_mapping_workflow(self):
        """Test approve -> summary -> finalize workflow."""
        from portiere.models.concept_mapping import (
            ConceptCandidate,
            ConceptMapping,
            ConceptMappingItem,
            ConceptMappingMethod,
        )

        mapping = ConceptMapping(
            items=[
                ConceptMappingItem(
                    source_code="E11.9",
                    source_description="Type 2 diabetes",
                    confidence=0.97,
                    method=ConceptMappingMethod.AUTO,
                    target_concept_id=201826,
                    target_concept_name="Type 2 diabetes mellitus",
                ),
                ConceptMappingItem(
                    source_code="J18.9",
                    source_description="Pneumonia",
                    confidence=0.84,
                    method=ConceptMappingMethod.REVIEW,
                    candidates=[
                        ConceptCandidate(
                            concept_id=255848,
                            concept_name="Pneumonia",
                            vocabulary_id="SNOMED",
                            domain_id="Condition",
                            concept_class_id="Clinical Finding",
                            standard_concept="S",
                            score=0.84,
                        ),
                    ],
                ),
                ConceptMappingItem(
                    source_code="CUSTOM_001",
                    source_description="Unknown code",
                    confidence=0.30,
                    method=ConceptMappingMethod.MANUAL,
                ),
            ]
        )

        # Check summary before approval
        summary = mapping.summary()
        assert summary["total"] == 3
        assert summary["auto_mapped"] == 1
        assert summary["needs_review"] == 1
        assert summary["manual_required"] == 1

        # Approve the review item
        review_items = mapping.needs_review()
        assert len(review_items) == 1
        review_items[0].approve(candidate_index=0)

        # Check properties
        assert review_items[0].approved is True
        assert review_items[0].target_concept_id == 255848

        # Reject the manual item
        manual_items = mapping.unmapped()
        manual_items[0].reject()
        assert manual_items[0].rejected is True

        # Export to OMOP format
        stcm = mapping.to_source_to_concept_map()
        assert len(stcm) == 2  # Only mapped items
        assert stcm[0]["source_code"] == "E11.9"

    def test_schema_mapping_workflow(self):
        """Test SchemaMapping approve/reject workflow."""
        from portiere.models.schema_mapping import (
            MappingStatus,
            SchemaMapping,
            SchemaMappingItem,
        )

        mapping = SchemaMapping(
            items=[
                SchemaMappingItem(
                    source_column="patient_id",
                    target_table="person",
                    target_column="person_id",
                    confidence=0.95,
                    status=MappingStatus.AUTO_ACCEPTED,
                ),
                SchemaMappingItem(
                    source_column="diag_code",
                    target_table="condition_occurrence",
                    target_column="condition_source_value",
                    confidence=0.80,
                    status=MappingStatus.NEEDS_REVIEW,
                ),
                SchemaMappingItem(
                    source_column="unknown_col",
                    confidence=0.20,
                    status=MappingStatus.UNMAPPED,
                ),
            ]
        )

        summary = mapping.summary()
        assert summary["total"] == 3
        assert summary["auto_accepted"] == 1
        assert summary["needs_review"] == 1
        assert summary["unmapped"] == 1

        # Approve review items
        mapping.approve_all()
        assert len(mapping.needs_review()) == 0

        summary2 = mapping.summary()
        assert summary2["approved"] == 1


class TestETLRunnerIntegration:
    """End-to-end: CSV → ETLRunner → parquet files per OMOP table."""

    def test_csv_to_parquet_via_runner(self, tmp_path):
        """Full pipeline: read CSV, route to tables, apply concepts, write parquet."""
        import pandas as pd

        from portiere.engines.pandas_engine import PandasEngine
        from portiere.runner.etl_runner import ETLRunner

        # Create source CSV
        source_path = str(tmp_path / "ehr_data.csv")
        pd.DataFrame(
            {
                "patient_id": ["P001", "P002", "P003", "P004"],
                "diagnosis_code": ["E11.9", "I10", "J18.9", "E11.9"],
                "drug_code": ["PARA500", "AMOX250", "PARA500", "IBU400"],
                "visit_date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
            }
        ).to_csv(source_path, index=False)

        engine = PandasEngine()
        output_dir = str(tmp_path / "omop_output")

        runner = ETLRunner(
            engine=engine,
            schema_items=[
                {
                    "source_column": "patient_id",
                    "target_table": "person",
                    "target_column": "person_source_value",
                },
                {
                    "source_column": "patient_id",
                    "target_table": "condition_occurrence",
                    "target_column": "person_id",
                },
                {
                    "source_column": "diagnosis_code",
                    "target_table": "condition_occurrence",
                    "target_column": "condition_source_value",
                },
                {
                    "source_column": "visit_date",
                    "target_table": "condition_occurrence",
                    "target_column": "condition_start_date",
                },
                {
                    "source_column": "patient_id",
                    "target_table": "drug_exposure",
                    "target_column": "person_id",
                },
                {
                    "source_column": "drug_code",
                    "target_table": "drug_exposure",
                    "target_column": "drug_source_value",
                },
            ],
            concept_items=[
                {
                    "source_code": "E11.9",
                    "source_column": "diagnosis_code",
                    "target_concept_id": 201826,
                },
                {
                    "source_code": "I10",
                    "source_column": "diagnosis_code",
                    "target_concept_id": 320128,
                },
                {
                    "source_code": "PARA500",
                    "source_column": "drug_code",
                    "target_concept_id": 19078461,
                },
                {
                    "source_code": "AMOX250",
                    "source_column": "drug_code",
                    "target_concept_id": 19073183,
                },
            ],
        )

        result = runner.run(source_path=source_path, output_path=output_dir, output_format="csv")

        assert result.success is True
        assert result.source_rows_read == 4
        assert len(result.tables) == 3

        # Verify person table
        person_df = pd.read_csv(os.path.join(output_dir, "person.csv"))
        assert len(person_df) == 4
        assert "person_source_value" in person_df.columns

        # Verify condition_occurrence with concept IDs
        cond_df = pd.read_csv(os.path.join(output_dir, "condition_occurrence.csv"))
        assert "condition_source_value_concept_id" in cond_df.columns
        concept_ids = cond_df["condition_source_value_concept_id"].tolist()
        assert concept_ids.count(201826) == 2  # Two E11.9 entries
        assert 0 in concept_ids  # J18.9 unmapped

        # Verify drug_exposure with concept IDs
        drug_df = pd.read_csv(os.path.join(output_dir, "drug_exposure.csv"))
        assert "drug_source_value_concept_id" in drug_df.columns

    def test_artifacts_roundtrip(self, tmp_path):
        """Save artifacts → from_artifacts → run → verify."""
        import pandas as pd

        from portiere.artifacts.artifact_manager import ArtifactManager
        from portiere.engines.pandas_engine import PandasEngine
        from portiere.models.concept_mapping import (
            ConceptMapping,
            ConceptMappingItem,
            ConceptMappingMethod,
        )
        from portiere.models.schema_mapping import MappingStatus, SchemaMapping, SchemaMappingItem
        from portiere.runner.etl_runner import ETLRunner

        engine = PandasEngine()

        schema = SchemaMapping(
            items=[
                SchemaMappingItem(
                    source_column="code",
                    target_table="condition",
                    target_column="cond_src",
                    status=MappingStatus.AUTO_ACCEPTED,
                ),
            ]
        )
        concept = ConceptMapping(
            items=[
                ConceptMappingItem(
                    source_code="E11.9",
                    source_column="code",
                    target_concept_id=201826,
                    method=ConceptMappingMethod.AUTO,
                ),
            ]
        )

        artifacts_dir = str(tmp_path / "artifacts")
        manager = ArtifactManager(engine=engine, output_dir=artifacts_dir)
        manager.generate_runner_config(schema, concept, project_name="roundtrip_test")
        manager.generate_source_to_concept_map(
            [
                {
                    "source_code": "E11.9",
                    "source_column": "code",
                    "target_concept_id": 201826,
                    "target_concept_name": "Diabetes",
                    "confidence": 0.97,
                    "method": "auto",
                },
            ]
        )
        manager.save_artifacts()

        source_path = str(tmp_path / "input.csv")
        pd.DataFrame({"code": ["E11.9", "I10"]}).to_csv(source_path, index=False)

        runner = ETLRunner.from_artifacts(artifacts_dir, engine=engine)
        result = runner.run(
            source_path=source_path, output_path=str(tmp_path / "output"), output_format="csv"
        )

        assert result.success is True
        assert len(result.tables) == 1
        assert result.tables[0].table_name == "condition"


class TestProjectModel:
    """Test Project model."""

    def test_project_validate_wired(self, tmp_path):
        """Project.validate() calls stage5."""
        from portiere.models.project import Project

        project = Project(id="test", name="Test", target_model="omop_cdm_v5.4")

        mock_engine = MagicMock()

        # Use a real empty directory so Path.exists() returns False naturally
        output_dir = tmp_path / "empty_output"
        output_dir.mkdir()

        result = project.validate(engine=mock_engine, output_path=str(output_dir))

        assert "valid" in result
        assert "issues" in result
        assert "stats" in result

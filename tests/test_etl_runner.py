"""
Portiere SDK — ETL Runner and Data Sampling Tests.

Tests for:
- Engine sample() and map_column() methods
- Source.profile(sample_n=...) with full/sampled data split
- ingest_source(sample_n=...) with sampling
- ETLRunner initialization, table routing, concept lookups
- ETLRunner.from_mappings() status filtering
- ETLRunner.from_artifacts() config loading + engine resolution
- ETLRunner.from_project() API fetching + engine resolution
- ETLRunner.run() execution with real data
- ETLRunner.dry_run() preview without writing
- ETLResult summary output
- Engine consistency (never silently default)
"""

import os
from unittest.mock import MagicMock, patch

import pytest

# ──────────────────────────────────────────────────────────────
# Engine: sample() and map_column()
# ──────────────────────────────────────────────────────────────


class TestSamplePandas:
    """Test PandasEngine.sample()."""

    def test_sample_returns_correct_count(self):
        import pandas as pd

        from portiere.engines.pandas_engine import PandasEngine

        engine = PandasEngine()
        df = pd.DataFrame({"a": range(100), "b": range(100)})

        result = engine.sample(df, 10)
        assert len(result) == 10

    def test_sample_capped_at_df_size(self):
        import pandas as pd

        from portiere.engines.pandas_engine import PandasEngine

        engine = PandasEngine()
        df = pd.DataFrame({"a": range(5)})

        result = engine.sample(df, 100)
        assert len(result) == 5

    def test_sample_preserves_columns(self):
        import pandas as pd

        from portiere.engines.pandas_engine import PandasEngine

        engine = PandasEngine()
        df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})

        result = engine.sample(df, 2)
        assert list(result.columns) == ["x", "y"]


class TestSamplePolars:
    """Test PolarsEngine.sample()."""

    def test_sample_returns_correct_count(self):
        import polars as pl

        from portiere.engines.polars_engine import PolarsEngine

        engine = PolarsEngine()
        df = pl.DataFrame({"a": range(100), "b": range(100)})

        result = engine.sample(df, 10)
        assert result.height == 10

    def test_sample_capped_at_df_size(self):
        import polars as pl

        from portiere.engines.polars_engine import PolarsEngine

        engine = PolarsEngine()
        df = pl.DataFrame({"a": range(5)})

        result = engine.sample(df, 100)
        assert result.height == 5


class TestMapColumnPandas:
    """Test PandasEngine.map_column()."""

    def test_maps_values_correctly(self):
        import pandas as pd

        from portiere.engines.pandas_engine import PandasEngine

        engine = PandasEngine()
        df = pd.DataFrame({"code": ["E11.9", "I10", "J18.9"]})
        mapping = {"E11.9": 201826, "I10": 320128}

        result = engine.map_column(df, "code", mapping, "concept_id", default=0)

        assert list(result["concept_id"]) == [201826, 320128, 0]

    def test_all_unmapped_uses_default(self):
        import pandas as pd

        from portiere.engines.pandas_engine import PandasEngine

        engine = PandasEngine()
        df = pd.DataFrame({"code": ["X", "Y"]})

        result = engine.map_column(df, "code", {}, "concept_id", default=0)

        assert list(result["concept_id"]) == [0, 0]

    def test_preserves_original_column(self):
        import pandas as pd

        from portiere.engines.pandas_engine import PandasEngine

        engine = PandasEngine()
        df = pd.DataFrame({"code": ["E11.9"]})

        result = engine.map_column(df, "code", {"E11.9": 100}, "cid")

        assert "code" in result.columns
        assert "cid" in result.columns


class TestMapColumnPolars:
    """Test PolarsEngine.map_column()."""

    def test_maps_values_correctly(self):
        import polars as pl

        from portiere.engines.polars_engine import PolarsEngine

        engine = PolarsEngine()
        df = pl.DataFrame({"code": ["E11.9", "I10", "J18.9"]})
        mapping = {"E11.9": 201826, "I10": 320128}

        result = engine.map_column(df, "code", mapping, "concept_id", default=0)

        assert result["concept_id"].to_list() == [201826, 320128, 0]


# ──────────────────────────────────────────────────────────────
# Sampled Profiling
# ──────────────────────────────────────────────────────────────


class TestSampledProfiling:
    """Test Source.profile(sample_n=...) — row_count from full, column stats from sample."""

    def test_profile_with_sample_n(self):
        from portiere.engines.base import AbstractEngine
        from portiere.models.source import Source

        mock_engine = MagicMock(spec=AbstractEngine)
        # Full data has 1000 rows
        mock_df = MagicMock()
        mock_engine.read_source.return_value = mock_df
        mock_engine.count.return_value = 1000

        # Sampled data (100 rows)
        mock_sampled = MagicMock()
        mock_engine.sample.return_value = mock_sampled

        # Profile returns stats from the sampled data
        mock_engine.profile.return_value = {
            "row_count": 100,  # This is sample size, will be overridden
            "column_count": 3,
            "columns": [
                {"name": "patient_id", "type": "int64", "n_unique": 80},
                {"name": "diagnosis_code", "type": "object", "n_unique": 15},
                {"name": "dob", "type": "object", "n_unique": 70},
            ],
        }

        source = Source(id="s1", name="test", path="/data/test.csv", engine=mock_engine)
        result = source.profile(sample_n=100)

        # row_count should be exact from full data (1000), not sample (100)
        assert result.row_count == 1000
        assert result.column_count == 3
        assert result.sample_n == 100

        # Verify engine.sample was called
        mock_engine.sample.assert_called_once_with(mock_df, 100)
        # Profile was called on the sampled data
        mock_engine.profile.assert_called_once_with(mock_sampled)

    def test_profile_without_sample_n(self):
        """Without sample_n, profile uses full data."""
        from portiere.engines.base import AbstractEngine
        from portiere.models.source import Source

        mock_engine = MagicMock(spec=AbstractEngine)
        mock_df = MagicMock()
        mock_engine.read_source.return_value = mock_df
        mock_engine.count.return_value = 500

        mock_engine.profile.return_value = {
            "row_count": 500,
            "column_count": 2,
            "columns": [
                {"name": "id", "type": "int64", "n_unique": 500},
                {"name": "name", "type": "object", "n_unique": 400},
            ],
        }

        source = Source(id="s1", name="test", path="/data/test.csv", engine=mock_engine)
        result = source.profile()

        assert result.row_count == 500
        assert result.sample_n is None
        mock_engine.sample.assert_not_called()
        mock_engine.profile.assert_called_once_with(mock_df)

    def test_sampled_profile_stores_sample_n(self):
        """sample_n is stored on SourceProfile for traceability."""
        from portiere.engines.base import AbstractEngine
        from portiere.models.source import Source

        mock_engine = MagicMock(spec=AbstractEngine)
        mock_engine.read_source.return_value = MagicMock()
        mock_engine.count.return_value = 2000
        mock_engine.sample.return_value = MagicMock()
        mock_engine.profile.return_value = {
            "row_count": 50,
            "column_count": 1,
            "columns": [],
        }

        source = Source(id="s1", name="test", path="/data.csv", engine=mock_engine)
        result = source.profile(sample_n=50)

        assert result.sample_n == 50


class TestSampledIngestion:
    """Test ingest_source(sample_n=...) — row_count from full, column stats from sample."""

    def test_ingest_with_sample_n(self):
        from portiere.stages.stage1_ingest import ingest_source

        mock_engine = MagicMock()
        mock_df = MagicMock()
        mock_engine.read_source.return_value = mock_df
        mock_engine.count.return_value = 5000

        mock_sampled = MagicMock()
        mock_engine.sample.return_value = mock_sampled

        mock_engine.profile.return_value = {
            "row_count": 200,
            "column_count": 2,
            "columns": [
                {"name": "id", "type": "int64", "n_unique": 180},
                {"name": "code", "type": "object", "n_unique": 20},
            ],
        }

        result = ingest_source(mock_engine, "/data/test.csv", sample_n=200)

        # row_count must be the exact full count
        assert result["row_count"] == 5000
        assert result["sample_n"] == 200
        mock_engine.sample.assert_called_once_with(mock_df, 200)

    def test_ingest_without_sample_n(self):
        from portiere.stages.stage1_ingest import ingest_source

        mock_engine = MagicMock()
        mock_df = MagicMock()
        mock_engine.read_source.return_value = mock_df
        mock_engine.count.return_value = 100

        mock_engine.profile.return_value = {
            "row_count": 100,
            "column_count": 1,
            "columns": [],
        }

        result = ingest_source(mock_engine, "/data/test.csv")

        assert result["row_count"] == 100
        assert result["sample_n"] is None
        mock_engine.sample.assert_not_called()


class TestSampledCodeExploration:
    """Test Source.get_code_columns(sample_n=...) — distinct values from sample."""

    def test_get_code_columns_with_sample(self):
        from portiere.engines.base import AbstractEngine
        from portiere.models.source import Source, SourceProfile

        mock_engine = MagicMock(spec=AbstractEngine)
        mock_df = MagicMock()
        mock_sampled = MagicMock()
        mock_engine.read_source.return_value = mock_df
        mock_engine.sample.return_value = mock_sampled
        mock_engine.get_distinct_values.return_value = [
            {"value": "E11.9", "count": 5},
        ]

        source = Source(id="s1", name="test", path="/data.csv", engine=mock_engine)
        source.profile_result = SourceProfile(
            row_count=1000,
            column_count=1,
            columns=[{"name": "code", "type": "object"}],
            code_columns_detected=["code"],
        )

        result = source.get_code_columns(mock_engine, sample_n=100)

        mock_engine.sample.assert_called_once_with(mock_df, 100)
        mock_engine.get_distinct_values.assert_called_once_with(mock_sampled, "code", limit=5000)
        assert "code" in result


# ──────────────────────────────────────────────────────────────
# ETLRunner Initialization
# ──────────────────────────────────────────────────────────────


class TestETLRunnerInit:
    """Test ETLRunner initialization and internal structures."""

    def test_table_routing(self):
        from portiere.runner.etl_runner import ETLRunner

        engine = MagicMock()
        engine.engine_name = "pandas"

        runner = ETLRunner(
            engine=engine,
            schema_items=[
                {
                    "source_column": "patient_id",
                    "target_table": "person",
                    "target_column": "person_source_value",
                },
                {
                    "source_column": "diagnosis_code",
                    "target_table": "condition_occurrence",
                    "target_column": "condition_source_value",
                },
                {
                    "source_column": "patient_id",
                    "target_table": "condition_occurrence",
                    "target_column": "person_id",
                },
            ],
            concept_items=[],
        )

        assert "person" in runner._table_routes
        assert "condition_occurrence" in runner._table_routes
        assert len(runner._table_routes["person"]) == 1
        assert len(runner._table_routes["condition_occurrence"]) == 2

    def test_concept_lookup_building(self):
        from portiere.runner.etl_runner import ETLRunner

        engine = MagicMock()
        engine.engine_name = "pandas"

        runner = ETLRunner(
            engine=engine,
            schema_items=[],
            concept_items=[
                {"source_code": "E11.9", "source_column": "diag_code", "target_concept_id": 201826},
                {"source_code": "I10", "source_column": "diag_code", "target_concept_id": 320128},
                {
                    "source_code": "PARA500",
                    "source_column": "drug_code",
                    "target_concept_id": 19078461,
                },
            ],
        )

        assert "diag_code" in runner._concept_lookups
        assert runner._concept_lookups["diag_code"]["E11.9"] == 201826
        assert runner._concept_lookups["diag_code"]["I10"] == 320128
        assert "drug_code" in runner._concept_lookups
        assert runner._concept_lookups["drug_code"]["PARA500"] == 19078461


# ──────────────────────────────────────────────────────────────
# ETLRunner.from_mappings()
# ──────────────────────────────────────────────────────────────


class TestETLRunnerFromMappings:
    """Test ETLRunner.from_mappings() status filtering."""

    def test_filters_schema_by_status(self):
        from portiere.models.concept_mapping import ConceptMapping
        from portiere.models.schema_mapping import MappingStatus, SchemaMapping, SchemaMappingItem
        from portiere.runner.etl_runner import ETLRunner

        schema = SchemaMapping(
            items=[
                SchemaMappingItem(
                    source_column="a",
                    target_table="t1",
                    target_column="col_a",
                    status=MappingStatus.AUTO_ACCEPTED,
                ),
                SchemaMappingItem(
                    source_column="b",
                    target_table="t1",
                    target_column="col_b",
                    status=MappingStatus.APPROVED,
                ),
                SchemaMappingItem(
                    source_column="c",
                    target_table="t1",
                    target_column="col_c",
                    status=MappingStatus.REJECTED,
                ),
                SchemaMappingItem(
                    source_column="d",
                    target_table="t1",
                    target_column="col_d",
                    status=MappingStatus.UNMAPPED,
                ),
                SchemaMappingItem(
                    source_column="e",
                    target_table="t1",
                    target_column="col_e",
                    status=MappingStatus.OVERRIDDEN,
                ),
            ]
        )
        concept = ConceptMapping(items=[])

        engine = MagicMock()
        engine.engine_name = "polars"

        runner = ETLRunner.from_mappings(engine, schema, concept)

        # Only AUTO_ACCEPTED, APPROVED, OVERRIDDEN should pass
        assert len(runner.schema_items) == 3
        sources = {item["source_column"] for item in runner.schema_items}
        assert sources == {"a", "b", "e"}

    def test_filters_concepts_by_mapped_status(self):
        from portiere.models.concept_mapping import (
            ConceptMapping,
            ConceptMappingItem,
            ConceptMappingMethod,
        )
        from portiere.models.schema_mapping import SchemaMapping
        from portiere.runner.etl_runner import ETLRunner

        schema = SchemaMapping(items=[])
        concept = ConceptMapping(
            items=[
                ConceptMappingItem(
                    source_code="E11.9",
                    source_column="diag",
                    target_concept_id=201826,
                    method=ConceptMappingMethod.AUTO,
                ),
                ConceptMappingItem(
                    source_code="CUSTOM", source_column="diag", method=ConceptMappingMethod.UNMAPPED
                ),
                ConceptMappingItem(
                    source_code="I10",
                    source_column="diag",
                    target_concept_id=320128,
                    method=ConceptMappingMethod.OVERRIDE,
                ),
            ]
        )

        engine = MagicMock()
        engine.engine_name = "polars"

        runner = ETLRunner.from_mappings(engine, schema, concept)

        # UNMAPPED and is_mapped=False should be filtered out
        assert len(runner.concept_items) == 2

    def test_uses_effective_target(self):
        """from_mappings should use effective_target_table/column (override if set)."""
        from portiere.models.concept_mapping import ConceptMapping
        from portiere.models.schema_mapping import MappingStatus, SchemaMapping, SchemaMappingItem
        from portiere.runner.etl_runner import ETLRunner

        schema = SchemaMapping(
            items=[
                SchemaMappingItem(
                    source_column="code",
                    target_table="original_table",
                    target_column="original_col",
                    override_target_table="override_table",
                    override_target_column="override_col",
                    status=MappingStatus.OVERRIDDEN,
                ),
            ]
        )
        concept = ConceptMapping(items=[])

        engine = MagicMock()
        engine.engine_name = "polars"

        runner = ETLRunner.from_mappings(engine, schema, concept)

        assert runner.schema_items[0]["target_table"] == "override_table"
        assert runner.schema_items[0]["target_column"] == "override_col"


# ──────────────────────────────────────────────────────────────
# ETLRunner.from_artifacts()
# ──────────────────────────────────────────────────────────────


class TestETLRunnerFromArtifacts:
    """Test ETLRunner.from_artifacts() config loading."""

    def test_loads_config_and_csv(self, tmp_path):
        import yaml

        config = {
            "version": "1.0",
            "engine": "pandas",
            "project_name": "test_proj",
            "target_model": "omop_cdm_v5.4",
            "schema_mappings": [
                {
                    "source_column": "code",
                    "target_table": "condition",
                    "target_column": "condition_source_value",
                },
            ],
            "concept_lookup_file": "source_to_concept_map.csv",
        }
        (tmp_path / "etl_config.yaml").write_text(yaml.dump(config))

        csv_path = tmp_path / "source_to_concept_map.csv"
        csv_path.write_text(
            "source_code,source_column,target_concept_id\nE11.9,code,201826\nI10,code,320128\n"
        )

        from portiere.runner.etl_runner import ETLRunner

        runner = ETLRunner.from_artifacts(str(tmp_path))

        assert runner.engine.engine_name == "pandas"
        assert len(runner.schema_items) == 1
        assert len(runner.concept_items) == 2
        assert runner.project_name == "test_proj"

    def test_uses_provided_engine_over_config(self, tmp_path):
        import yaml

        config = {
            "version": "1.0",
            "engine": "pandas",
            "schema_mappings": [],
            "concept_lookup_file": "source_to_concept_map.csv",
        }
        (tmp_path / "etl_config.yaml").write_text(yaml.dump(config))

        from portiere.runner.etl_runner import ETLRunner

        custom_engine = MagicMock()
        custom_engine.engine_name = "polars"

        runner = ETLRunner.from_artifacts(str(tmp_path), engine=custom_engine)

        # Should use the provided engine, not create pandas from config
        assert runner.engine is custom_engine

    def test_raises_on_missing_config(self, tmp_path):
        from portiere.runner.etl_runner import ETLRunner

        with pytest.raises(FileNotFoundError, match=r"etl_config\.yaml"):
            ETLRunner.from_artifacts(str(tmp_path))

    def test_raises_on_no_engine(self, tmp_path):
        import yaml

        config = {"version": "1.0", "schema_mappings": []}
        (tmp_path / "etl_config.yaml").write_text(yaml.dump(config))

        from portiere.runner.etl_runner import ETLRunner

        with pytest.raises(ValueError, match="No engine"):
            ETLRunner.from_artifacts(str(tmp_path))


# ──────────────────────────────────────────────────────────────
# ETLRunner.from_project()
# ──────────────────────────────────────────────────────────────


class TestETLRunnerFromProject:
    """Test ETLRunner.from_project() API fetching."""

    def test_fetches_and_filters_mappings(self):
        from portiere.runner.etl_runner import ETLRunner

        mock_client = MagicMock()
        mock_client._request.side_effect = [
            # GET /projects/{id}
            {"name": "API Project", "target_model": "omop_cdm_v5.4"},
            # GET /projects/{id}/schema-mapping
            {
                "items": [
                    {
                        "source_column": "a",
                        "target_table": "t1",
                        "target_column": "c1",
                        "status": "auto_accepted",
                    },
                    {
                        "source_column": "b",
                        "target_table": "t1",
                        "target_column": "c2",
                        "status": "rejected",
                    },
                ]
            },
            # GET /projects/{id}/concept-mapping
            {
                "items": [
                    {
                        "source_code": "X",
                        "source_column": "a",
                        "target_concept_id": 100,
                        "method": "auto",
                    },
                    {"source_code": "Y", "source_column": "a", "method": "unmapped"},
                ]
            },
        ]

        engine = MagicMock()
        engine.engine_name = "polars"

        runner = ETLRunner.from_project(mock_client, "proj_123", engine=engine)

        assert len(runner.schema_items) == 1  # Only auto_accepted
        assert len(runner.concept_items) == 1  # Only non-unmapped with concept_id
        assert runner.project_name == "API Project"

    def test_raises_without_engine(self):
        from portiere.runner.etl_runner import ETLRunner

        mock_client = MagicMock()

        with pytest.raises(ValueError, match="No engine provided"):
            ETLRunner.from_project(mock_client, "proj_123")


# ──────────────────────────────────────────────────────────────
# ETLRunner.run() and dry_run()
# ──────────────────────────────────────────────────────────────


class TestETLRunnerRun:
    """Test ETLRunner.run() execution with real PandasEngine."""

    def test_run_end_to_end(self, tmp_path):
        import pandas as pd

        from portiere.engines.pandas_engine import PandasEngine

        # Create source data
        source_path = str(tmp_path / "input.csv")
        pd.DataFrame(
            {
                "patient_id": ["P001", "P002", "P003"],
                "diagnosis_code": ["E11.9", "I10", "J18.9"],
                "visit_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            }
        ).to_csv(source_path, index=False)

        from portiere.runner.etl_runner import ETLRunner

        engine = PandasEngine()
        output_dir = str(tmp_path / "output")

        runner = ETLRunner(
            engine=engine,
            schema_items=[
                {
                    "source_column": "patient_id",
                    "target_table": "person",
                    "target_column": "person_source_value",
                },
                {
                    "source_column": "diagnosis_code",
                    "target_table": "condition_occurrence",
                    "target_column": "condition_source_value",
                },
                {
                    "source_column": "patient_id",
                    "target_table": "condition_occurrence",
                    "target_column": "person_id",
                },
                {
                    "source_column": "visit_date",
                    "target_table": "condition_occurrence",
                    "target_column": "condition_start_date",
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
            ],
        )

        result = runner.run(
            source_path=source_path,
            output_path=output_dir,
            source_format="csv",
            output_format="csv",
        )

        assert result.success is True
        assert result.source_rows_read == 3
        assert len(result.tables) == 2
        assert result.engine_name == "pandas"

        # Check person table
        person_table = next(t for t in result.tables if t.table_name == "person")
        assert person_table.rows_written == 3
        assert os.path.exists(person_table.output_path)

        # Check condition_occurrence table
        cond_table = next(t for t in result.tables if t.table_name == "condition_occurrence")
        assert cond_table.rows_written == 3
        assert "condition_source_value" in cond_table.concept_columns_mapped

        # Verify concept lookup was applied
        cond_df = pd.read_csv(cond_table.output_path)
        assert "condition_source_value_concept_id" in cond_df.columns
        # E11.9 -> 201826, I10 -> 320128, J18.9 -> 0 (unmapped)
        concept_ids = cond_df["condition_source_value_concept_id"].tolist()
        assert 201826 in concept_ids
        assert 320128 in concept_ids
        assert 0 in concept_ids  # J18.9 unmapped

    def test_run_with_empty_mappings(self, tmp_path):
        import pandas as pd

        from portiere.engines.pandas_engine import PandasEngine

        source_path = str(tmp_path / "input.csv")
        pd.DataFrame({"a": [1, 2]}).to_csv(source_path, index=False)

        from portiere.runner.etl_runner import ETLRunner

        runner = ETLRunner(
            engine=PandasEngine(),
            schema_items=[],
            concept_items=[],
        )

        result = runner.run(source_path=source_path, output_path=str(tmp_path / "out"))

        assert result.success is True
        assert len(result.tables) == 0
        assert result.source_rows_read == 2

    def test_run_tracks_unmapped_columns(self, tmp_path):
        import pandas as pd

        from portiere.engines.pandas_engine import PandasEngine

        source_path = str(tmp_path / "input.csv")
        pd.DataFrame(
            {
                "mapped_col": [1, 2],
                "unmapped_col": [3, 4],
            }
        ).to_csv(source_path, index=False)

        from portiere.runner.etl_runner import ETLRunner

        runner = ETLRunner(
            engine=PandasEngine(),
            schema_items=[
                {"source_column": "mapped_col", "target_table": "t1", "target_column": "c1"},
            ],
            concept_items=[],
        )

        result = runner.run(source_path=source_path, output_path=str(tmp_path / "out"))

        assert "unmapped_col" in result.unmapped_columns

    def test_progress_callback(self, tmp_path):
        import pandas as pd

        from portiere.engines.pandas_engine import PandasEngine

        source_path = str(tmp_path / "input.csv")
        pd.DataFrame({"a": [1], "b": [2]}).to_csv(source_path, index=False)

        from portiere.runner.etl_runner import ETLRunner

        progress_calls = []

        runner = ETLRunner(
            engine=PandasEngine(),
            schema_items=[
                {"source_column": "a", "target_table": "t1", "target_column": "c1"},
                {"source_column": "b", "target_table": "t2", "target_column": "c2"},
            ],
            concept_items=[],
        )

        runner.run(
            source_path=source_path,
            output_path=str(tmp_path / "out"),
            on_progress=lambda name, cur, tot: progress_calls.append((name, cur, tot)),
        )

        assert len(progress_calls) == 2
        assert progress_calls[0][1] == 1  # current
        assert progress_calls[1][2] == 2  # total


class TestETLRunnerDryRun:
    """Test ETLRunner.dry_run() preview."""

    def test_dry_run_returns_plan(self, tmp_path):
        import pandas as pd

        from portiere.engines.pandas_engine import PandasEngine

        source_path = str(tmp_path / "input.csv")
        pd.DataFrame(
            {
                "patient_id": ["P001"],
                "diag_code": ["E11.9"],
                "extra": ["x"],
            }
        ).to_csv(source_path, index=False)

        from portiere.runner.etl_runner import ETLRunner

        runner = ETLRunner(
            engine=PandasEngine(),
            schema_items=[
                {
                    "source_column": "patient_id",
                    "target_table": "person",
                    "target_column": "person_id",
                },
                {
                    "source_column": "diag_code",
                    "target_table": "condition",
                    "target_column": "cond_src",
                },
            ],
            concept_items=[
                {"source_code": "E11.9", "source_column": "diag_code", "target_concept_id": 201826},
            ],
        )

        plan = runner.dry_run(source_path=source_path)

        assert plan["source_rows"] == 1
        assert plan["table_count"] == 2
        assert "person" in plan["tables"]
        assert "condition" in plan["tables"]
        assert "extra" in plan["unmapped_columns"]
        assert plan["engine"] == "pandas"

        # Check concept lookup info
        cond_cols = plan["tables"]["condition"]["columns"]
        diag_col = next(c for c in cond_cols if c["source_column"] == "diag_code")
        assert diag_col["has_concept_lookup"] is True
        assert diag_col["concept_codes_count"] == 1


# ──────────────────────────────────────────────────────────────
# ETLResult
# ──────────────────────────────────────────────────────────────


class TestETLResult:
    """Test ETLResult model and summary."""

    def test_summary_success(self):
        from portiere.runner.result import ETLResult, TableResult

        result = ETLResult(
            success=True,
            duration_seconds=1.5,
            source_path="/data/input.csv",
            source_rows_read=100,
            output_path="/data/output",
            engine_name="polars",
            tables=[
                TableResult(table_name="person", rows_written=100, columns=["id", "name"]),
                TableResult(
                    table_name="condition",
                    rows_written=80,
                    columns=["id"],
                    concept_columns_mapped=["code"],
                ),
            ],
            total_rows_written=180,
            schema_mappings_applied=5,
            concept_mappings_applied=1,
        )

        summary = result.summary()
        assert "SUCCESS" in summary
        assert "polars" in summary
        assert "100 rows" in summary
        assert "person" in summary
        assert "condition" in summary

    def test_summary_failure(self):
        from portiere.runner.result import ETLResult

        result = ETLResult(
            success=False,
            errors=["Table t1: column not found"],
            engine_name="pandas",
        )

        summary = result.summary()
        assert "FAILED" in summary
        assert "column not found" in summary


# ──────────────────────────────────────────────────────────────
# Engine Consistency
# ──────────────────────────────────────────────────────────────


class TestEngineConsistency:
    """Verify ETLRunner uses provided engine, never silently defaults."""

    def test_from_project_requires_engine(self):
        from portiere.runner.etl_runner import ETLRunner

        with pytest.raises(ValueError, match="No engine provided"):
            ETLRunner.from_project(MagicMock(), "proj_123")

    def test_from_artifacts_requires_engine_or_config(self, tmp_path):
        import yaml

        config = {"version": "1.0", "schema_mappings": []}
        (tmp_path / "etl_config.yaml").write_text(yaml.dump(config))

        from portiere.runner.etl_runner import ETLRunner

        with pytest.raises(ValueError, match="No engine"):
            ETLRunner.from_artifacts(str(tmp_path))

    def test_run_etl_uses_source_engine(self):
        """Project.run_etl() passes source.engine to runner."""
        from portiere.engines.base import AbstractEngine
        from portiere.models.concept_mapping import ConceptMapping
        from portiere.models.project import Project
        from portiere.models.schema_mapping import MappingStatus, SchemaMapping, SchemaMappingItem
        from portiere.models.source import Source

        source_engine = MagicMock(spec=AbstractEngine)
        source_engine.engine_name = "polars"
        source_engine.read_source.return_value = MagicMock()
        source_engine.count.return_value = 0
        source_engine.schema.return_value = []

        source = Source(id="s1", name="test", path="/data.csv", engine=source_engine)

        project = Project(id="p1", name="Test Project")

        schema = SchemaMapping(
            items=[
                SchemaMappingItem(
                    source_column="a",
                    target_table="t1",
                    target_column="c1",
                    status=MappingStatus.AUTO_ACCEPTED,
                ),
            ]
        )
        concept = ConceptMapping(items=[])

        with patch("os.makedirs"):
            result = project.run_etl(
                source=source,
                schema_mapping=schema,
                concept_mapping=concept,
                output_path="/tmp/out",
            )

        # The engine used should be source.engine
        source_engine.read_source.assert_called_once()

    def test_run_etl_raises_without_engine_on_source(self):
        """run_etl raises if source has no engine."""
        from portiere.models.concept_mapping import ConceptMapping
        from portiere.models.project import Project
        from portiere.models.schema_mapping import SchemaMapping
        from portiere.models.source import Source

        source = Source(id="s1", name="test", path="/data.csv")
        project = Project(id="p1", name="Test")

        with pytest.raises(ValueError, match="no engine"):
            project.run_etl(source, SchemaMapping(items=[]), ConceptMapping(items=[]))


# ──────────────────────────────────────────────────────────────
# Artifact Manager: generate_runner_config()
# ──────────────────────────────────────────────────────────────


class TestGenerateRunnerConfig:
    """Test ArtifactManager.generate_runner_config()."""

    def test_generates_yaml_with_schema_mappings(self):
        import yaml

        from portiere.artifacts.artifact_manager import ArtifactManager
        from portiere.models.concept_mapping import ConceptMapping
        from portiere.models.schema_mapping import MappingStatus, SchemaMapping, SchemaMappingItem

        engine = MagicMock()
        engine.engine_name = "polars"

        manager = ArtifactManager(engine=engine)

        schema = SchemaMapping(
            items=[
                SchemaMappingItem(
                    source_column="patient_id",
                    target_table="person",
                    target_column="person_source_value",
                    status=MappingStatus.AUTO_ACCEPTED,
                ),
                SchemaMappingItem(
                    source_column="unknown",
                    status=MappingStatus.UNMAPPED,
                ),
            ]
        )
        concept = ConceptMapping(items=[])

        content = manager.generate_runner_config(
            schema,
            concept,
            source_path="/data/input.csv",
            output_path="./output",
            project_name="test_proj",
        )

        config = yaml.safe_load(content)
        assert config["engine"] == "polars"
        assert config["project_name"] == "test_proj"
        assert len(config["schema_mappings"]) == 1  # UNMAPPED filtered out
        assert config["schema_mappings"][0]["source_column"] == "patient_id"
        assert config["concept_lookup_file"] == "source_to_concept_map.csv"

    def test_saves_as_etl_config_yaml(self, tmp_path):
        from portiere.artifacts.artifact_manager import ArtifactManager
        from portiere.models.concept_mapping import ConceptMapping
        from portiere.models.schema_mapping import SchemaMapping

        engine = MagicMock()
        engine.engine_name = "pandas"

        manager = ArtifactManager(engine=engine)
        manager.generate_runner_config(
            SchemaMapping(items=[]),
            ConceptMapping(items=[]),
        )

        saved = manager.save_artifacts(output_dir=str(tmp_path))

        filenames = [p.name for p in saved]
        assert "etl_config.yaml" in filenames

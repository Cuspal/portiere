"""
Tests for the quality module (GXProfiler and GXValidator).

Tests profiling, validation, report dataclasses, and graceful
degradation when Great Expectations is not installed.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestProfileReport:
    """Tests for ProfileReport dataclass."""

    def test_create_profile_report(self):
        from portiere.quality.models import ProfileReport

        report = ProfileReport(
            source_name="patients",
            columns=[{"name": "id", "dtype": "int64"}],
            gx_result={"success": True},
            expectations={"expectations": []},
            row_count=100,
        )

        assert report.source_name == "patients"
        assert report.row_count == 100
        assert report.created_at is not None

    def test_profile_report_to_dict(self):
        from portiere.quality.models import ProfileReport

        report = ProfileReport(
            source_name="patients",
            columns=[{"name": "id", "dtype": "int64"}],
            gx_result={"success": True},
            expectations={"expectations": []},
            row_count=50,
            created_at="2024-01-01T00:00:00",
        )

        d = report.to_dict()
        assert d["source_name"] == "patients"
        assert d["row_count"] == 50
        assert d["created_at"] == "2024-01-01T00:00:00"
        assert isinstance(d["columns"], list)


class TestValidationReport:
    """Tests for ValidationReport dataclass."""

    def test_create_validation_report(self):
        from portiere.quality.models import ValidationReport

        report = ValidationReport(
            table_name="person",
            passed=True,
            completeness_score=0.98,
            conformance_score=0.99,
            plausibility_score=0.95,
            gx_result={"success": True},
            thresholds={"min_completeness": 0.95},
        )

        assert report.table_name == "person"
        assert report.passed is True
        assert report.completeness_score == 0.98

    def test_validation_report_to_dict(self):
        from portiere.quality.models import ValidationReport

        report = ValidationReport(
            table_name="person",
            passed=False,
            completeness_score=0.80,
            conformance_score=0.70,
            plausibility_score=0.60,
            gx_result={},
            thresholds={"min_completeness": 0.95},
            created_at="2024-01-01T00:00:00",
        )

        d = report.to_dict()
        assert d["table_name"] == "person"
        assert d["passed"] is False
        assert d["completeness_score"] == 0.80
        assert d["created_at"] == "2024-01-01T00:00:00"


class TestGXProfiler:
    """Tests for GXProfiler (with mocked GX)."""

    def test_profiler_extracts_column_stats(self):
        from portiere.config import QualityConfig
        from portiere.quality.profiler import GXProfiler

        profiler = GXProfiler(QualityConfig())

        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", None, "Dave", "Eve"],
                "score": [95.0, 87.5, 92.3, None, 88.1],
            }
        )

        columns = profiler._extract_column_stats(df)

        assert len(columns) == 3

        # Check id column
        id_col = next(c for c in columns if c["name"] == "id")
        assert id_col["null_count"] == 0
        assert id_col["unique_count"] == 5
        assert "min" in id_col  # numeric column

        # Check name column
        name_col = next(c for c in columns if c["name"] == "name")
        assert name_col["null_count"] == 1
        assert "min" not in name_col  # string column

        # Check score column
        score_col = next(c for c in columns if c["name"] == "score")
        assert score_col["null_count"] == 1
        assert score_col["min"] == pytest.approx(87.5)
        assert score_col["max"] == pytest.approx(95.0)

    def test_profiler_sample_values(self):
        from portiere.config import QualityConfig
        from portiere.quality.profiler import GXProfiler

        profiler = GXProfiler(QualityConfig())

        df = pd.DataFrame(
            {
                "id": range(100),
            }
        )

        columns = profiler._extract_column_stats(df)
        assert len(columns[0]["sample_values"]) <= 5

    def test_profiler_empty_column(self):
        from portiere.config import QualityConfig
        from portiere.quality.profiler import GXProfiler

        profiler = GXProfiler(QualityConfig())

        df = pd.DataFrame(
            {
                "empty": [None, None, None],
            }
        )

        columns = profiler._extract_column_stats(df)
        assert columns[0]["null_count"] == 3
        assert columns[0]["sample_values"] == []

    def test_profiler_profile_with_mocked_gx(self):
        from portiere.config import QualityConfig
        from portiere.quality.profiler import GXProfiler

        profiler = GXProfiler(QualityConfig())

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
            }
        )

        # Mock the entire GX chain
        mock_gx = MagicMock()
        mock_context = MagicMock()
        mock_gx.get_context.return_value = mock_context

        mock_datasource = MagicMock()
        mock_context.data_sources.add_pandas.return_value = mock_datasource

        mock_asset = MagicMock()
        mock_datasource.add_dataframe_asset.return_value = mock_asset

        mock_batch_def = MagicMock()
        mock_asset.add_batch_definition_whole_dataframe.return_value = mock_batch_def

        mock_batch = MagicMock()
        mock_batch_def.get_batch.return_value = mock_batch

        mock_suite = MagicMock()
        mock_context.suites.add.return_value = mock_suite

        mock_result = MagicMock()
        mock_result.to_json_dict.return_value = {"success": True, "results": []}
        mock_batch.validate.return_value = mock_result

        mock_suite.to_json_dict.return_value = {"expectations": []}

        with patch("portiere.quality.profiler._require_gx", return_value=mock_gx):
            report = profiler.profile(df, "test_source")

        assert report["source_name"] == "test_source"
        assert report["row_count"] == 3
        assert len(report["columns"]) == 2

    def test_profiler_require_gx_raises_without_package(self):
        from portiere.quality.profiler import _require_gx

        with patch.dict("sys.modules", {"great_expectations": None}):
            with pytest.raises(ImportError, match="Great Expectations"):
                _require_gx()


class TestGXValidator:
    """Tests for GXValidator (with mocked GX)."""

    def _make_validator(self):
        from portiere.config import QualityConfig, ThresholdsConfig
        from portiere.quality.validator import GXValidator

        return GXValidator(QualityConfig(), ThresholdsConfig())

    def test_compute_completeness_all_pass(self):
        validator = self._make_validator()

        mock_result = MagicMock()
        mock_result.to_json_dict.return_value = {
            "results": [
                {
                    "expectation_config": {"type": "expect_column_to_exist"},
                    "success": True,
                },
                {
                    "expectation_config": {"type": "expect_column_to_exist"},
                    "success": True,
                },
            ]
        }

        score = validator._compute_completeness(mock_result)
        assert score == 1.0

    def test_compute_completeness_partial(self):
        validator = self._make_validator()

        mock_result = MagicMock()
        mock_result.to_json_dict.return_value = {
            "results": [
                {
                    "expectation_config": {"type": "expect_column_to_exist"},
                    "success": True,
                },
                {
                    "expectation_config": {"type": "expect_column_to_exist"},
                    "success": False,
                },
            ]
        }

        score = validator._compute_completeness(mock_result)
        assert score == 0.5

    def test_compute_completeness_empty(self):
        validator = self._make_validator()

        mock_result = MagicMock()
        mock_result.to_json_dict.return_value = {"results": []}

        score = validator._compute_completeness(mock_result)
        assert score == 1.0

    def test_compute_conformance_all_pass(self):
        validator = self._make_validator()

        mock_result = MagicMock()
        mock_result.to_json_dict.return_value = {
            "results": [
                {
                    "expectation_config": {"type": "expect_column_to_exist"},
                    "success": True,
                },
                {
                    "expectation_config": {"type": "expect_column_values_to_be_between"},
                    "success": True,
                },
                {
                    "expectation_config": {"type": "expect_column_values_to_not_be_null"},
                    "success": True,
                },
            ]
        }

        score = validator._compute_conformance(mock_result)
        assert score == 1.0

    def test_compute_conformance_partial(self):
        validator = self._make_validator()

        mock_result = MagicMock()
        mock_result.to_json_dict.return_value = {
            "results": [
                {
                    "expectation_config": {"type": "expect_column_to_exist"},
                    "success": True,
                },
                {
                    "expectation_config": {"type": "expect_column_values_to_be_between"},
                    "success": True,
                },
                {
                    "expectation_config": {"type": "expect_column_values_to_not_be_null"},
                    "success": False,
                },
            ]
        }

        # Conformance = non-exist expectations only: 1 pass / 2 total
        score = validator._compute_conformance(mock_result)
        assert score == 0.5

    def test_compute_plausibility(self):
        validator = self._make_validator()

        mock_result = MagicMock()
        mock_result.to_json_dict.return_value = {
            "results": [
                {"success": True},
                {"success": True},
                {"success": False},
                {"success": True},
            ]
        }

        score = validator._compute_plausibility(mock_result)
        assert score == pytest.approx(0.75)

    def test_validate_with_mocked_gx(self):
        validator = self._make_validator()

        df = pd.DataFrame(
            {
                "person_id": [1, 2, 3],
                "gender_concept_id": [8507, 8532, 8507],
                "birth_datetime": ["1990-01-01", "1985-05-15", "2000-12-25"],
            }
        )

        mock_gx = MagicMock()
        mock_context = MagicMock()
        mock_gx.get_context.return_value = mock_context

        mock_suite = MagicMock()
        mock_context.suites.add.return_value = mock_suite

        mock_datasource = MagicMock()
        mock_context.data_sources.add_pandas.return_value = mock_datasource

        mock_asset = MagicMock()
        mock_datasource.add_dataframe_asset.return_value = mock_asset

        mock_batch_def = MagicMock()
        mock_asset.add_batch_definition_whole_dataframe.return_value = mock_batch_def

        mock_batch = MagicMock()
        mock_batch_def.get_batch.return_value = mock_batch

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.to_json_dict.return_value = {
            "success": True,
            "results": [
                {
                    "expectation_config": {"type": "expect_column_to_exist"},
                    "success": True,
                },
                {
                    "expectation_config": {"type": "expect_column_values_to_be_between"},
                    "success": True,
                },
            ],
        }
        mock_batch.validate.return_value = mock_result

        # Mock the target model
        mock_model = MagicMock()
        mock_model.get_schema.return_value = {
            "person": ["person_id", "gender_concept_id", "birth_datetime"]
        }

        with patch("portiere.quality.validator._require_gx", return_value=mock_gx):
            with patch(
                "portiere.quality.validator.GXValidator._build_expectation_suite",
                return_value=mock_suite,
            ):
                report = validator.validate(df, "person", "omop_cdm_v5.4")

        assert report["table_name"] == "person"
        assert report["passed"] is True
        assert report["completeness_score"] == 1.0
        assert report["conformance_score"] == 1.0

    def test_validate_fails_below_thresholds(self):
        from portiere.config import QualityConfig, ThresholdsConfig, ValidationThresholds
        from portiere.quality.validator import GXValidator

        thresholds = ThresholdsConfig(
            validation=ValidationThresholds(
                min_completeness=0.95,
                min_conformance=0.98,
                min_plausibility=0.90,
            )
        )
        validator = GXValidator(QualityConfig(), thresholds)

        df = pd.DataFrame({"person_id": [1]})

        mock_gx = MagicMock()
        mock_context = MagicMock()
        mock_gx.get_context.return_value = mock_context

        mock_suite = MagicMock()
        mock_context.suites.add.return_value = mock_suite

        mock_datasource = MagicMock()
        mock_context.data_sources.add_pandas.return_value = mock_datasource
        mock_asset = MagicMock()
        mock_datasource.add_dataframe_asset.return_value = mock_asset
        mock_batch_def = MagicMock()
        mock_asset.add_batch_definition_whole_dataframe.return_value = mock_batch_def
        mock_batch = MagicMock()
        mock_batch_def.get_batch.return_value = mock_batch

        mock_result = MagicMock()
        mock_result.to_json_dict.return_value = {
            "results": [
                {
                    "expectation_config": {"type": "expect_column_to_exist"},
                    "success": True,
                },
                {
                    "expectation_config": {"type": "expect_column_to_exist"},
                    "success": False,
                },
                {
                    "expectation_config": {"type": "expect_column_values_to_be_between"},
                    "success": False,
                },
            ]
        }
        mock_batch.validate.return_value = mock_result

        with patch("portiere.quality.validator._require_gx", return_value=mock_gx):
            with patch(
                "portiere.quality.validator.GXValidator._build_expectation_suite",
                return_value=mock_suite,
            ):
                report = validator.validate(df, "person", "omop_cdm_v5.4")

        # completeness = 1/2 = 0.5 < 0.95 → fails
        assert report["passed"] is False
        assert report["completeness_score"] == 0.5

    def test_validator_require_gx_raises_without_package(self):
        from portiere.quality.validator import _require_gx

        with patch.dict("sys.modules", {"great_expectations": None}):
            with pytest.raises(ImportError, match="Great Expectations"):
                _require_gx()

    def test_build_expectation_suite(self):
        import pandas as pd

        validator = self._make_validator()

        mock_gx = MagicMock()
        mock_context = MagicMock()
        mock_suite = MagicMock()
        mock_context.suites.add.return_value = mock_suite

        mock_model = MagicMock()
        mock_model.get_schema.return_value = {
            "person": [
                "person_id",
                "gender_concept_id",
                "birth_datetime",
                "person_source_value",
            ]
        }
        mock_model.get_field_types.return_value = {
            "person_id": "numeric",
            "gender_concept_id": "code",
            "birth_datetime": "temporal",
            "person_source_value": "string",
        }

        # Provide a DataFrame with numeric concept_id and date columns
        df = pd.DataFrame(
            {
                "person_id": [1, 2],
                "gender_concept_id": [8507, 8532],
                "birth_datetime": ["1990-01-01", "1985-06-15"],
                "person_source_value": ["P001", "P002"],
            }
        )

        # The suite created by gx.ExpectationSuite() receives add_expectation calls
        created_suite = mock_gx.ExpectationSuite.return_value

        with patch("portiere.models.target_model.get_target_model", return_value=mock_model):
            suite = validator._build_expectation_suite(
                mock_gx, mock_context, "person", "omop_cdm_v5.4", df
            )

        # 4 exist + 1 code (concept_id between) + 1 temporal (not null)
        assert created_suite.add_expectation.call_count == 6


def _make_mock_spark_df(**kwargs):
    """Create a mock that _detect_df_type identifies as a Spark DataFrame.

    We patch _detect_df_type instead of trying to fake __class__/__module__,
    since MagicMock's internal machinery fights class overrides.
    Returns a plain MagicMock with the given attributes set.
    """
    mock = MagicMock()
    for k, v in kwargs.items():
        setattr(mock, k, v)
    return mock


class TestDetectDfType:
    """Tests for _detect_df_type helper."""

    def test_detect_pandas(self):
        from portiere.quality.utils import _detect_df_type

        df = pd.DataFrame({"a": [1, 2]})
        assert _detect_df_type(df) == "pandas"

    def test_detect_spark(self):
        from portiere.quality.utils import _detect_df_type

        # Create a real object whose type reports pyspark module path
        # (MagicMock always reports unittest.mock, so we build a real class)
        FakeSparkDF = type(
            "DataFrame",
            (),
            {"__module__": "pyspark.sql.dataframe"},
        )
        fake_df = FakeSparkDF()
        assert _detect_df_type(fake_df) == "spark"

    def test_detect_unknown_defaults_to_pandas(self):
        from portiere.quality.utils import _detect_df_type

        obj = {"a": 1}
        assert _detect_df_type(obj) == "pandas"


class TestGXProfilerSpark:
    """Tests for GXProfiler with mocked Spark DataFrames."""

    def _make_spark_df_mock(self):
        return _make_mock_spark_df(
            columns=["id", "name", "score"],
            dtypes=[("id", "bigint"), ("name", "string"), ("score", "double")],
        )

    def test_profiler_uses_add_spark_for_spark_df(self):
        from portiere.config import QualityConfig
        from portiere.quality.profiler import GXProfiler

        profiler = GXProfiler(QualityConfig())
        mock_df = self._make_spark_df_mock()
        mock_df.count.return_value = 5

        # Mock df.agg().collect() for numeric columns (used in suite building)
        agg_row = MagicMock()
        agg_row.__getitem__ = MagicMock(side_effect=lambda i: [1.0, 5.0][i])
        mock_df.agg.return_value.collect.return_value = [agg_row]

        mock_gx = MagicMock()
        mock_context = MagicMock()
        mock_gx.get_context.return_value = mock_context

        mock_datasource = MagicMock()
        mock_context.data_sources.add_spark.return_value = mock_datasource

        mock_asset = MagicMock()
        mock_datasource.add_dataframe_asset.return_value = mock_asset

        mock_batch_def = MagicMock()
        mock_asset.add_batch_definition_whole_dataframe.return_value = mock_batch_def

        mock_batch = MagicMock()
        mock_batch_def.get_batch.return_value = mock_batch

        mock_suite = MagicMock()
        mock_context.suites.add.return_value = mock_suite

        mock_result = MagicMock()
        mock_result.to_json_dict.return_value = {"success": True, "results": []}
        mock_batch.validate.return_value = mock_result

        mock_suite.to_json_dict.return_value = {"expectations": []}

        mock_F = MagicMock()

        with (
            patch("portiere.quality.profiler._require_gx", return_value=mock_gx),
            patch("portiere.quality.profiler._detect_df_type", return_value="spark"),
            patch.object(profiler, "_extract_column_stats", return_value=[]),
            patch.dict(
                "sys.modules",
                {
                    "pyspark": MagicMock(),
                    "pyspark.sql": MagicMock(),
                    "pyspark.sql.functions": mock_F,
                },
            ),
        ):
            report = profiler.profile(mock_df, "spark_source")

        # Verify add_spark was called, NOT add_pandas
        mock_context.data_sources.add_spark.assert_called_once_with(name="spark_source")
        mock_context.data_sources.add_pandas.assert_not_called()
        assert report["source_name"] == "spark_source"
        assert report["row_count"] == 5

    def test_extract_column_stats_spark(self):
        from portiere.config import QualityConfig
        from portiere.quality.profiler import GXProfiler

        profiler = GXProfiler(QualityConfig())
        mock_df = self._make_spark_df_mock()
        mock_df.count.return_value = 5

        # Mock pyspark.sql.functions
        mock_F = MagicMock()

        # Configure the select/collect chain for each column:
        # For each column, _extract_column_stats_spark calls:
        #   1. df.select(F.sum(...)).collect()[0][0]  → null_count
        #   2. df.select(F.countDistinct(...)).collect()[0][0]  → unique_count
        #   3. df.select(col).filter(...).limit(5).collect()  → sample values (for non-numeric)
        #      OR df.select(F.min, F.max, F.mean, F.stddev).collect()[0]  → numeric agg (then sample)

        select_calls = []

        def make_collect_result(value):
            """Make a row mock so collect()[0][0] returns value."""
            row = MagicMock()
            row.__getitem__ = MagicMock(return_value=value)
            return [row]

        def make_agg_row(values):
            """Make an agg row: row[0]=min, row[1]=max, etc."""
            row = MagicMock()
            row.__getitem__ = MagicMock(side_effect=lambda i: values[i])
            return [row]

        def make_sample_rows(vals):
            rows = []
            for v in vals:
                r = MagicMock()
                r.__getitem__ = MagicMock(return_value=v)
                rows.append(r)
            return rows

        # For each column (id, name, score), we need:
        # select #1 → null count, select #2 → unique count, select #3 → sample/filter
        # For numeric cols (id=bigint, score=double), also df.select(min,max,mean,stddev)

        call_idx = {"n": 0}

        # Expected call sequence per column:
        # id (bigint/numeric): null_select, unique_select, agg_select, sample_select
        # name (string):       null_select, unique_select, sample_select
        # score (double/num):  null_select, unique_select, agg_select, sample_select
        responses = [
            # id: null count
            make_collect_result(0),
            # id: unique count
            make_collect_result(5),
            # id: agg (min, max, mean, std)
            make_agg_row([1.0, 5.0, 3.0, 1.5]),
            # id: sample values
            make_sample_rows(["1", "2", "3"]),
            # name: null count
            make_collect_result(1),
            # name: unique count
            make_collect_result(4),
            # name: sample values
            make_sample_rows(["Alice", "Bob"]),
            # score: null count
            make_collect_result(0),
            # score: unique count
            make_collect_result(5),
            # score: agg
            make_agg_row([10.0, 99.0, 55.0, 20.0]),
            # score: sample values
            make_sample_rows(["10.0", "99.0"]),
        ]

        def mock_select(*args):
            idx = call_idx["n"]
            call_idx["n"] += 1
            result = MagicMock()
            if idx in (3, 6, 10):
                # sample select → needs .filter().limit().collect()
                filter_result = MagicMock()
                limit_result = MagicMock()
                limit_result.collect.return_value = responses[idx]
                filter_result.limit.return_value = limit_result
                result.filter.return_value = filter_result
            else:
                result.collect.return_value = responses[idx]
            return result

        mock_df.select = mock_select

        with patch.dict(
            "sys.modules",
            {
                "pyspark": MagicMock(),
                "pyspark.sql": MagicMock(),
                "pyspark.sql.functions": mock_F,
            },
        ):
            columns = profiler._extract_column_stats_spark(mock_df)

        assert len(columns) == 3

        # Check id column (numeric)
        id_col = columns[0]
        assert id_col["name"] == "id"
        assert id_col["dtype"] == "bigint"
        assert id_col["null_count"] == 0
        assert "min" in id_col

        # Check name column (string, no numeric stats)
        name_col = columns[1]
        assert name_col["name"] == "name"
        assert name_col["null_count"] == 1
        assert "min" not in name_col

        # Check score column (numeric)
        score_col = columns[2]
        assert score_col["name"] == "score"
        assert "min" in score_col


class TestGXValidatorSpark:
    """Tests for GXValidator with mocked Spark DataFrames."""

    def _make_validator(self):
        from portiere.config import QualityConfig, ThresholdsConfig
        from portiere.quality.validator import GXValidator

        return GXValidator(QualityConfig(), ThresholdsConfig())

    def _make_spark_df_mock(self):
        return _make_mock_spark_df(
            columns=["person_id", "gender_concept_id", "birth_datetime"],
            dtypes=[
                ("person_id", "bigint"),
                ("gender_concept_id", "int"),
                ("birth_datetime", "string"),
            ],
        )

    def test_validator_uses_add_spark_for_spark_df(self):
        validator = self._make_validator()
        mock_df = self._make_spark_df_mock()

        mock_gx = MagicMock()
        mock_context = MagicMock()
        mock_gx.get_context.return_value = mock_context

        mock_suite = MagicMock()
        mock_context.suites.add.return_value = mock_suite

        mock_datasource = MagicMock()
        mock_context.data_sources.add_spark.return_value = mock_datasource

        mock_asset = MagicMock()
        mock_datasource.add_dataframe_asset.return_value = mock_asset

        mock_batch_def = MagicMock()
        mock_asset.add_batch_definition_whole_dataframe.return_value = mock_batch_def

        mock_batch = MagicMock()
        mock_batch_def.get_batch.return_value = mock_batch

        mock_result = MagicMock()
        mock_result.to_json_dict.return_value = {
            "success": True,
            "results": [
                {
                    "expectation_config": {"type": "expect_column_to_exist"},
                    "success": True,
                },
            ],
        }
        mock_batch.validate.return_value = mock_result

        with (
            patch("portiere.quality.validator._require_gx", return_value=mock_gx),
            patch("portiere.quality.validator._detect_df_type", return_value="spark"),
            patch(
                "portiere.quality.validator.GXValidator._build_expectation_suite",
                return_value=mock_suite,
            ),
        ):
            report = validator.validate(mock_df, "person", "omop_cdm_v5.4")

        # Verify add_spark was called, NOT add_pandas
        mock_context.data_sources.add_spark.assert_called_once()
        mock_context.data_sources.add_pandas.assert_not_called()
        assert report["table_name"] == "person"

    def test_build_expectation_suite_spark_numeric_check(self):
        validator = self._make_validator()
        mock_df = self._make_spark_df_mock()

        mock_gx = MagicMock()
        mock_context = MagicMock()
        mock_suite = MagicMock()
        mock_context.suites.add.return_value = mock_suite

        mock_model = MagicMock()
        mock_model.get_schema.return_value = {
            "person": [
                "person_id",
                "gender_concept_id",
                "birth_datetime",
            ]
        }
        mock_model.get_field_types.return_value = {
            "person_id": "numeric",
            "gender_concept_id": "code",
            "birth_datetime": "temporal",
        }

        created_suite = mock_gx.ExpectationSuite.return_value

        with (
            patch("portiere.models.target_model.get_target_model", return_value=mock_model),
            patch("portiere.quality.validator._detect_df_type", return_value="spark"),
        ):
            suite = validator._build_expectation_suite(
                mock_gx, mock_context, "person", "omop_cdm_v5.4", mock_df
            )

        # 3 exist + 1 code between (gender_concept_id is "int" → numeric in Spark) + 1 temporal not null
        assert created_suite.add_expectation.call_count == 5

        # Verify concept_id expectation was added
        calls = [str(c) for c in created_suite.add_expectation.call_args_list]
        concept_calls = [c for c in calls if "Between" in c]
        assert len(concept_calls) >= 1, (
            "Expected at least one ExpectColumnValuesToBeBetween for code column"
        )


class TestQualityStorageRoundtrip:
    """Tests for saving/loading quality artifacts via storage."""

    def test_profile_storage_roundtrip(self, tmp_path):
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)
        storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        profile = {
            "source_name": "patients",
            "columns": [{"name": "id", "dtype": "int64", "null_count": 0}],
            "gx_result": {"success": True},
            "expectations": {"expectations": []},
            "row_count": 100,
            "created_at": "2024-01-01T00:00:00",
        }

        storage.save_profile("test", "patients", profile)
        loaded = storage.load_profile("test", "patients")

        assert loaded is not None
        assert loaded["source_name"] == "patients"
        assert loaded["row_count"] == 100

    def test_quality_report_storage_roundtrip(self, tmp_path):
        from portiere.storage.local_backend import LocalStorageBackend

        storage = LocalStorageBackend(base_dir=tmp_path)
        storage.create_project("test", "omop_cdm_v5.4", ["SNOMED"])

        report = {
            "table_name": "person",
            "passed": True,
            "completeness_score": 0.98,
            "conformance_score": 0.99,
            "plausibility_score": 0.95,
            "gx_result": {"success": True},
            "thresholds": {"min_completeness": 0.95},
            "created_at": "2024-01-01T00:00:00",
        }

        storage.save_quality_report("test", report)
        reports = storage.load_quality_reports("test")

        assert len(reports) == 1
        assert reports[0]["table_name"] == "person"
        assert reports[0]["passed"] is True


class TestGetFieldTypes:
    """Tests for YAMLTargetModel.get_field_types() across all standards."""

    def test_omop_yaml_field_types(self):
        from portiere.standards import YAMLTargetModel

        model = YAMLTargetModel.from_name("omop_cdm_v5.4")
        types = model.get_field_types("person")

        # gender_concept_id has vocabulary: "Gender" → "code" (overrides integer type)
        assert types["gender_concept_id"] == "code"
        # birth_datetime has type: datetime → "temporal"
        assert types["birth_datetime"] == "temporal"
        # person_id has type: integer (no vocabulary) → "numeric"
        assert types["person_id"] == "numeric"

    def test_fhir_yaml_field_types(self):
        from portiere.standards import YAMLTargetModel

        model = YAMLTargetModel.from_name("fhir_r4")
        types = model.get_field_types("Patient")

        # gender has type: code → "code"
        assert types["gender"] == "code"
        # birthDate has type: date → "temporal"
        assert types["birthDate"] == "temporal"

    def test_hl7_yaml_field_types(self):
        from portiere.standards import YAMLTargetModel

        model = YAMLTargetModel.from_name("hl7v2_2.5.1")
        types = model.get_field_types("PID")

        # date_of_birth has type: TS → "temporal"
        assert types["date_of_birth"] == "temporal"
        # race has type: CE → "code"
        assert types["race"] == "code"

    def test_openehr_yaml_field_types(self):
        from portiere.standards import YAMLTargetModel

        model = YAMLTargetModel.from_name("openehr_1.0.4")
        types = model.get_field_types("demographics.person")

        # date_of_birth has type: DV_DATE → "temporal"
        assert types["date_of_birth"] == "temporal"
        # sex has type: DV_CODED_TEXT → "code"
        assert types["sex"] == "code"

    def test_vocabulary_override(self):
        """Fields with vocabulary key should be 'code' regardless of raw type."""
        from portiere.standards import YAMLTargetModel

        model = YAMLTargetModel.from_name("omop_cdm_v5.4")
        types = model.get_field_types("person")

        # gender_concept_id: type=integer + vocabulary=Gender → "code"
        assert types["gender_concept_id"] == "code"
        # person_id: type=integer, no vocabulary → "numeric"
        assert types["person_id"] == "numeric"

    def test_unknown_type_defaults_to_other(self):
        """Unknown raw types should default to 'other'."""
        import tempfile
        from pathlib import Path

        from portiere.standards import YAMLTargetModel

        yaml_content = """
name: test_custom
version: "1.0"
standard_type: relational
entities:
  test_table:
    fields:
      custom_field:
        type: "SomeUnknownType"
        description: "Field with unknown type"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            model = YAMLTargetModel(Path(f.name))

        types = model.get_field_types("test_table")
        assert types["custom_field"] == "other"


class TestValidatorMultiStandard:
    """Tests that the validator works with non-OMOP standards."""

    def _make_validator(self):
        from portiere.config import QualityConfig, ThresholdsConfig
        from portiere.quality.validator import GXValidator

        return GXValidator(QualityConfig(), ThresholdsConfig())

    def test_build_suite_fhir(self):
        """FHIR model: code fields get Between check, temporal get NotNull."""
        validator = self._make_validator()

        mock_gx = MagicMock()
        mock_context = MagicMock()
        mock_suite = MagicMock()
        mock_context.suites.add.return_value = mock_suite

        mock_model = MagicMock()
        mock_model.get_schema.return_value = {"Patient": ["id", "gender", "birthDate", "name"]}
        mock_model.get_field_types.return_value = {
            "id": "string",
            "gender": "code",
            "birthDate": "temporal",
            "name": "string",
        }

        # FHIR gender is a string code ("male"/"female"), not numeric
        df = pd.DataFrame(
            {
                "id": ["p1", "p2"],
                "gender": ["male", "female"],
                "birthDate": ["1990-01-01", "1985-05-15"],
                "name": ["Alice", "Bob"],
            }
        )

        created_suite = mock_gx.ExpectationSuite.return_value

        with patch("portiere.models.target_model.get_target_model", return_value=mock_model):
            validator._build_expectation_suite(mock_gx, mock_context, "Patient", "fhir_r4", df)

        # 4 exist + 0 code-between (gender is string, not numeric → skipped) + 1 temporal not-null
        assert created_suite.add_expectation.call_count == 5

    def test_build_suite_hl7_temporal(self):
        """HL7 v2: temporal fields without 'date' in name still get NotNull check."""
        validator = self._make_validator()

        mock_gx = MagicMock()
        mock_context = MagicMock()
        mock_suite = MagicMock()
        mock_context.suites.add.return_value = mock_suite

        mock_model = MagicMock()
        mock_model.get_schema.return_value = {"PV1": ["set_id", "admit_time", "patient_class"]}
        mock_model.get_field_types.return_value = {
            "set_id": "numeric",
            "admit_time": "temporal",
            "patient_class": "code",
        }

        df = pd.DataFrame(
            {
                "set_id": [1],
                "admit_time": ["20240101120000"],
                "patient_class": ["I"],
            }
        )

        created_suite = mock_gx.ExpectationSuite.return_value

        with patch("portiere.models.target_model.get_target_model", return_value=mock_model):
            validator._build_expectation_suite(mock_gx, mock_context, "PV1", "hl7v2_2.5.1", df)

        # 3 exist + 0 code-between (patient_class is string) + 1 temporal not-null
        assert created_suite.add_expectation.call_count == 4

    def test_build_suite_custom_yaml(self):
        """Custom YAML model works with the validator."""
        import tempfile
        from pathlib import Path

        from portiere.standards import YAMLTargetModel

        yaml_content = """
name: test_custom
version: "1.0"
standard_type: relational
entities:
  measurements:
    fields:
      measurement_id:
        type: integer
        description: "Unique ID"
      value:
        type: float
        description: "Measurement value"
      recorded_at:
        type: datetime
        description: "When recorded"
      category_code:
        type: integer
        vocabulary: "MeasurementCategory"
        description: "Category"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            model = YAMLTargetModel(Path(f.name))

        types = model.get_field_types("measurements")
        assert types["measurement_id"] == "numeric"
        assert types["value"] == "numeric"
        assert types["recorded_at"] == "temporal"
        assert types["category_code"] == "code"  # vocabulary override

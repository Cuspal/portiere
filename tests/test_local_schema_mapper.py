"""
Tests for local schema mapper.

Tests the full local schema mapping pipeline:
- Pattern matching against target model source_patterns
- Embedding similarity against target model descriptions
- Score fusion
- Cross-encoder reranking
- Deduplication
"""

from unittest.mock import MagicMock

import numpy as np
import pytest

from portiere.config import PortiereConfig
from portiere.local.schema_mapper import LocalSchemaMapper
from portiere.standards import YAMLTargetModel


def _make_config(reranker=False):
    return PortiereConfig(
        mode="local",
        embedding_model="test-model",
        reranker_model="cross-encoder/test" if reranker else None,
    )


@pytest.fixture
def omop_model():
    """Load the OMOP CDM v5.4 target model."""
    return YAMLTargetModel.from_name("omop_cdm_v5.4")


class TestTargetModelPatterns:
    """Verify the target model patterns and descriptions are well-formed."""

    def test_patterns_non_empty(self, omop_model):
        patterns = omop_model.get_source_patterns()
        assert len(patterns) > 0

    def test_patterns_are_tuples(self, omop_model):
        patterns = omop_model.get_source_patterns()
        for key, value in patterns.items():
            assert isinstance(key, str)
            assert isinstance(value, tuple)
            assert len(value) == 2

    def test_target_descriptions_non_empty(self, omop_model):
        descriptions = omop_model.get_target_descriptions_tupled()
        assert len(descriptions) > 0

    def test_target_descriptions_keys_are_tuples(self, omop_model):
        descriptions = omop_model.get_target_descriptions_tupled()
        for key in descriptions:
            assert isinstance(key, tuple)
            assert len(key) == 2


class TestPatternMatching:
    """Tests for _pattern_match method."""

    @pytest.fixture(autouse=True)
    def setup_mapper(self, omop_model):
        """Create a mapper with the real OMOP model but skip embedding init."""
        self.mapper = LocalSchemaMapper.__new__(LocalSchemaMapper)
        self.mapper._target_model = omop_model
        self.mapper._source_patterns = omop_model.get_source_patterns()
        self.mapper._desc_map = omop_model.get_target_descriptions_tupled()
        self.mapper._default_entity, self.mapper._default_field = (
            "observation",
            "observation_source_value",
        )

    def test_exact_match_high_confidence(self):
        result = self.mapper._pattern_match("patient_id")
        assert result is not None
        assert result["table"] == "person"
        assert result["column"] == "person_id"
        assert result["confidence"] == 0.95

    def test_substring_match_lower_confidence(self):
        result = self.mapper._pattern_match("patient_id_encrypted")
        assert result is not None
        assert result["table"] == "person"
        assert result["confidence"] == 0.85

    def test_no_match(self):
        result = self.mapper._pattern_match("completely_unknown_column")
        assert result is None

    def test_known_patterns(self):
        test_cases = [
            ("gender", "person", "gender_concept_id"),
            ("hadm_id", "visit_occurrence", "visit_occurrence_id"),
            ("icd_code", "condition_occurrence", "condition_source_value"),
            ("medication", "drug_exposure", "drug_source_value"),
            ("lab_code", "measurement", "measurement_source_value"),
            ("cpt_code", "procedure_occurrence", "procedure_source_value"),
        ]

        for col, expected_table, expected_column in test_cases:
            result = self.mapper._pattern_match(col)
            assert result is not None, f"Pattern not found for {col}"
            assert result["table"] == expected_table, f"Wrong table for {col}"
            assert result["column"] == expected_column, f"Wrong column for {col}"


class TestBuildSourceText:
    """Tests for _build_source_text method."""

    @pytest.fixture(autouse=True)
    def setup_mapper(self):
        self.mapper = LocalSchemaMapper.__new__(LocalSchemaMapper)

    def test_basic_column_name(self):
        text = self.mapper._build_source_text("patient_id", "", [])
        assert "patient id" in text

    def test_with_type_hint(self):
        text = self.mapper._build_source_text("age", "int", [])
        assert "numeric identifier" in text

    def test_with_date_type(self):
        text = self.mapper._build_source_text("visit_date", "datetime", [])
        assert "date" in text

    def test_with_sample_values(self):
        text = self.mapper._build_source_text("icd_code", "str", ["E11.9", "I10", "J45.0"])
        assert "examples: E11.9, I10, J45.0" in text

    def test_sample_values_truncated_to_3(self):
        text = self.mapper._build_source_text("code", "", ["A", "B", "C", "D", "E"])
        assert "A, B, C" in text
        assert "D" not in text


class TestScoreFusion:
    """Tests for _fuse_scores method."""

    @pytest.fixture(autouse=True)
    def setup_mapper(self, omop_model):
        self.mapper = LocalSchemaMapper.__new__(LocalSchemaMapper)
        self.mapper._default_entity = "observation"
        self.mapper._default_field = "observation_source_value"

    def test_pattern_match_takes_priority(self):
        pattern = {
            "table": "person",
            "column": "person_id",
            "confidence": 0.95,
            "pattern": "patient_id",
        }
        embedding = [{"table": "visit_occurrence", "column": "visit_occurrence_id", "score": 0.9}]

        result = self.mapper._fuse_scores("patient_id", pattern, embedding)

        assert result["target_table"] == "person"
        assert result["confidence"] == 0.95
        assert result["_has_pattern"] is True

    def test_embedding_only_high_score(self):
        embedding = [
            {
                "table": "measurement",
                "column": "value_as_number",
                "score": 0.55,
                "description": "lab result value",
            },
        ]

        result = self.mapper._fuse_scores("lab_result", None, embedding)

        # raw=0.55 >= 0.40 threshold → confidence = min(0.50 + 0.55, 0.95) = 0.95
        assert result["target_table"] == "measurement"
        assert result["confidence"] == 0.95
        assert result["_has_pattern"] is False

    def test_embedding_only_low_score(self):
        embedding = [
            {
                "table": "observation",
                "column": "observation_source_value",
                "score": 0.30,
                "description": "some obs",
            },
        ]

        result = self.mapper._fuse_scores("weird_col", None, embedding)

        # raw=0.30 < 0.40 → confidence = 0.30 * 0.7 = 0.21
        assert result["confidence"] == pytest.approx(0.21, abs=0.01)

    def test_no_embedding_results_default_observation(self):
        result = self.mapper._fuse_scores("unknown_col", None, [])

        assert result["target_table"] == "observation"
        assert result["target_column"] == "observation_source_value"
        assert result["confidence"] == 0.30


class TestDeduplication:
    """Tests for _deduplicate_targets method."""

    @pytest.fixture(autouse=True)
    def setup_mapper(self):
        self.mapper = LocalSchemaMapper.__new__(LocalSchemaMapper)

    def test_duplicate_targets_demoted(self):
        results = [
            {
                "source_column": "col1",
                "target_table": "person",
                "target_column": "person_id",
                "confidence": 0.95,
                "reasoning": "pattern",
            },
            {
                "source_column": "col2",
                "target_table": "person",
                "target_column": "person_id",
                "confidence": 0.85,
                "reasoning": "embedding",
            },
        ]

        self.mapper._deduplicate_targets(results)

        assert results[0]["confidence"] == 0.95
        assert results[1]["confidence"] <= 0.50
        assert "duplicate target" in results[1]["reasoning"]

    def test_no_duplicates_unchanged(self):
        results = [
            {
                "source_column": "col1",
                "target_table": "person",
                "target_column": "person_id",
                "confidence": 0.95,
                "reasoning": "",
            },
            {
                "source_column": "col2",
                "target_table": "measurement",
                "target_column": "value_as_number",
                "confidence": 0.85,
                "reasoning": "",
            },
        ]

        self.mapper._deduplicate_targets(results)

        assert results[0]["confidence"] == 0.95
        assert results[1]["confidence"] == 0.85

    def test_higher_confidence_duplicate_demotes_lower(self):
        results = [
            {
                "source_column": "col_low",
                "target_table": "measurement",
                "target_column": "measurement_date",
                "confidence": 0.70,
                "reasoning": "",
            },
            {
                "source_column": "col_high",
                "target_table": "measurement",
                "target_column": "measurement_date",
                "confidence": 0.90,
                "reasoning": "",
            },
        ]

        self.mapper._deduplicate_targets(results)

        assert results[0]["confidence"] <= 0.50
        assert "duplicate target" in results[0]["reasoning"]
        assert results[1]["confidence"] == 0.90


class TestSuggestIntegration:
    """Integration tests for suggest() with mocked model."""

    @pytest.fixture
    def mapper_with_mock_model(self, omop_model):
        config = _make_config()
        mapper = LocalSchemaMapper(config, target_model=omop_model)

        # Mock the embedding model
        mock_model = MagicMock()
        desc_map = omop_model.get_target_descriptions_tupled()
        n_targets = len(desc_map)
        dim = 768
        fake_embeddings = np.random.randn(n_targets, dim).astype("float32")
        fake_embeddings = fake_embeddings / np.linalg.norm(fake_embeddings, axis=1, keepdims=True)

        mapper._model = mock_model
        mapper._target_keys = list(desc_map.keys())
        mapper._target_descriptions = list(desc_map.values())
        mapper._target_embeddings = fake_embeddings
        mapper._reranker = None
        mapper._initialized = True

        def mock_encode(texts, **kwargs):
            v = np.random.randn(len(texts), dim).astype("float32")
            return v / np.linalg.norm(v, axis=1, keepdims=True)

        mock_model.encode = mock_encode

        return mapper

    def test_suggest_pattern_match_column(self, mapper_with_mock_model):
        mapper = mapper_with_mock_model

        columns = [{"name": "patient_id", "type": "int", "sample_values": ["1001", "1002"]}]
        results = mapper.suggest(columns)

        assert len(results) == 1
        assert results[0]["source_column"] == "patient_id"
        assert results[0]["target_table"] == "person"
        assert results[0]["target_column"] == "person_id"
        assert results[0]["confidence"] == 0.95

    def test_suggest_embedding_match_column(self, mapper_with_mock_model):
        mapper = mapper_with_mock_model

        columns = [{"name": "completely_new_column_xyz", "type": "str", "sample_values": []}]
        results = mapper.suggest(columns)

        assert len(results) == 1
        assert results[0]["source_column"] == "completely_new_column_xyz"
        assert results[0]["target_table"] is not None

    def test_suggest_multiple_columns(self, mapper_with_mock_model):
        mapper = mapper_with_mock_model

        columns = [
            {"name": "patient_id", "type": "int", "sample_values": []},
            {"name": "gender", "type": "str", "sample_values": ["M", "F"]},
            {"name": "admission_date", "type": "datetime", "sample_values": []},
        ]

        results = mapper.suggest(columns)

        assert len(results) == 3
        assert results[0]["target_table"] == "person"
        assert results[1]["target_table"] == "person"
        assert results[2]["target_table"] == "visit_occurrence"

    def test_suggest_without_model_pattern_only(self, omop_model):
        """When model fails to load, should still do pattern matching."""
        config = _make_config()
        mapper = LocalSchemaMapper(config, target_model=omop_model)
        mapper._model = None
        mapper._target_embeddings = None
        mapper._target_keys = None
        mapper._target_descriptions = None
        mapper._reranker = None
        mapper._initialized = True

        columns = [{"name": "patient_id", "type": "int", "sample_values": []}]
        results = mapper.suggest(columns)

        assert len(results) == 1
        assert results[0]["target_table"] == "person"
        assert results[0]["confidence"] == 0.95

    def test_suggest_without_model_unknown_column_defaults(self, omop_model):
        """Without model, unknown columns should default to observation."""
        config = _make_config()
        mapper = LocalSchemaMapper(config, target_model=omop_model)
        mapper._model = None
        mapper._target_embeddings = None
        mapper._target_keys = None
        mapper._target_descriptions = None
        mapper._reranker = None
        mapper._initialized = True

        columns = [{"name": "completely_unknown", "type": "", "sample_values": []}]
        results = mapper.suggest(columns)

        assert len(results) == 1
        assert results[0]["target_table"] == "observation"
        assert results[0]["confidence"] == 0.30

"""
Tests for SDK stages module.
"""

from unittest.mock import Mock

from portiere.stages.stage1_ingest import (
    _detect_code_columns,
    _detect_phi_columns,
    extract_code_values,
    ingest_source,
)


class TestDetectCodeColumns:
    """Tests for code column detection."""

    def test_detect_by_name_patterns(self):
        """Code columns detected by common naming patterns."""
        profile = {
            "row_count": 100,
            "columns": [
                {"name": "patient_id", "type": "int"},
                {"name": "diagnosis_code", "type": "str"},
                {"name": "icd_code", "type": "str"},
                {"name": "medication", "type": "str"},
            ],
        }

        result = _detect_code_columns(profile)

        assert "diagnosis_code" in result
        assert "icd_code" in result

    def test_detect_by_vocabulary_names(self):
        """Vocabulary-related columns detected."""
        profile = {
            "row_count": 100,
            "columns": [
                {"name": "snomed_code", "type": "str"},
                {"name": "loinc_id", "type": "str"},
                {"name": "rxnorm_code", "type": "str"},
                {"name": "ndc_code", "type": "str"},
            ],
        }

        result = _detect_code_columns(profile)

        assert len(result) == 4
        assert "snomed_code" in result
        assert "loinc_id" in result
        assert "rxnorm_code" in result
        assert "ndc_code" in result

    def test_detect_by_cardinality(self):
        """String columns with moderate cardinality detected."""
        profile = {
            "row_count": 1000,
            "columns": [
                {"name": "random_code", "type": "str", "n_unique": 100},  # Should detect
                {"name": "free_text", "type": "str", "n_unique": 950},  # Too high
                {"name": "flag", "type": "str", "n_unique": 2},  # Too low
            ],
        }

        result = _detect_code_columns(profile)

        assert "random_code" in result
        assert "free_text" not in result
        assert "flag" not in result


class TestDetectPHIColumns:
    """Tests for PHI column detection."""

    def test_detect_name_columns(self):
        """Name-related columns detected as PHI."""
        profile = {
            "columns": [
                {"name": "first_name", "type": "str"},
                {"name": "last_name", "type": "str"},
                {"name": "patient_name", "type": "str"},
            ],
        }

        result = _detect_phi_columns(profile)

        assert "first_name" in result
        assert "last_name" in result

    def test_detect_identifier_columns(self):
        """Identifier columns detected as PHI."""
        profile = {
            "columns": [
                {"name": "ssn", "type": "str"},
                {"name": "mrn", "type": "str"},
                {"name": "national_id", "type": "str"},
                {"name": "patient_id", "type": "str"},
            ],
        }

        result = _detect_phi_columns(profile)

        assert "ssn" in result
        assert "mrn" in result
        assert "national_id" in result
        assert "patient_id" in result

    def test_detect_contact_info(self):
        """Contact information detected as PHI."""
        profile = {
            "columns": [
                {"name": "email_address", "type": "str"},
                {"name": "phone_number", "type": "str"},
                {"name": "home_address", "type": "str"},
            ],
        }

        result = _detect_phi_columns(profile)

        assert "email_address" in result
        assert "phone_number" in result
        assert "home_address" in result


class TestIngestSource:
    """Tests for the ingest_source function."""

    def test_ingest_source_returns_profile(self):
        """Ingest source returns complete profile."""
        mock_engine = Mock()
        mock_engine.read_source.return_value = "mock_df"
        mock_engine.count.return_value = 1000
        mock_engine.profile.return_value = {
            "row_count": 1000,
            "column_count": 5,
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "diagnosis_code", "type": "str", "n_unique": 50},
                {"name": "patient_name", "type": "str"},
                {"name": "value", "type": "float"},
                {"name": "date", "type": "datetime"},
            ],
        }

        result = ingest_source(mock_engine, "/data/test.csv")

        assert result["row_count"] == 1000
        assert result["column_count"] == 5
        assert "diagnosis_code" in result["code_columns"]
        assert "patient_name" in result["phi_columns"]
        mock_engine.read_source.assert_called_once()
        mock_engine.profile.assert_called_once()

    def test_ingest_source_with_format_options(self):
        """Ingest source passes format and options."""
        mock_engine = Mock()
        mock_engine.read_source.return_value = "mock_df"
        mock_engine.count.return_value = 100
        mock_engine.profile.return_value = {
            "row_count": 100,
            "column_count": 2,
            "columns": [],
        }

        ingest_source(
            mock_engine,
            "/data/test.parquet",
            format="parquet",
            options={"columns": ["a", "b"]},
        )

        mock_engine.read_source.assert_called_once_with(
            "/data/test.parquet",
            format="parquet",
            options={"columns": ["a", "b"]},
        )


class TestExtractCodeValues:
    """Tests for code value extraction."""

    def test_extract_code_values_basic(self):
        """Extract distinct values from column."""
        mock_engine = Mock()
        mock_engine.read_source.return_value = "mock_df"
        mock_engine.get_distinct_values.return_value = [
            {"value": "A01", "count": 100},
            {"value": "B02", "count": 50},
            {"value": "C03", "count": 25},
        ]

        result = extract_code_values(mock_engine, "/data/test.csv", "code_column")

        assert len(result) == 3
        assert result[0]["value"] == "A01"
        mock_engine.get_distinct_values.assert_called_once()

    def test_extract_code_values_with_limit(self):
        """Extract respects limit parameter."""
        mock_engine = Mock()
        mock_engine.read_source.return_value = "mock_df"
        mock_engine.get_distinct_values.return_value = []

        extract_code_values(mock_engine, "/data/test.csv", "code_column", limit=100)

        mock_engine.get_distinct_values.assert_called_once_with("mock_df", "code_column", limit=100)

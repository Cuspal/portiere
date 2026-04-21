"""
Pytest configuration and shared fixtures for Portiere SDK tests.
"""

from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture
def tmp_data_path(tmp_path: Path) -> Path:
    """Provide a temporary directory for test data."""
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


@pytest.fixture
def sample_drug_data() -> pd.DataFrame:
    """
    Sample drug prescription data for testing concept mapping.

    Simulates hospital drug data that needs to be mapped to OMOP CDM.
    """
    return pd.DataFrame(
        {
            "prescription_id": [1001, 1002, 1003, 1004, 1005],
            "patient_id": [101, 102, 101, 103, 104],
            "drug_code": ["PARA500", "IBU400", "PARA500", "ASP300", "AMOX250"],
            "drug_name": [
                "Paracetamol 500mg Tablet",
                "Ibuprofen 400mg Capsule",
                "Paracetamol 500mg Tablet",
                "Aspirin 300mg Tablet",
                "Amoxicillin 250mg Capsule",
            ],
            "quantity": [30, 20, 60, 15, 21],
            "prescription_date": [
                "2024-01-15",
                "2024-01-16",
                "2024-01-17",
                "2024-01-18",
                "2024-01-19",
            ],
            "prescriber_id": ["DR001", "DR002", "DR001", "DR003", "DR001"],
        }
    )


@pytest.fixture
def sample_diagnosis_data() -> pd.DataFrame:
    """
    Sample diagnosis data for testing condition mapping.

    Simulates hospital diagnosis codes to be mapped to SNOMED/ICD10.
    """
    return pd.DataFrame(
        {
            "encounter_id": [2001, 2002, 2003, 2004],
            "patient_id": [101, 102, 103, 101],
            "diagnosis_code": ["J06.9", "E11.9", "I10", "M54.5"],
            "diagnosis_desc": [
                "Acute upper respiratory infection, unspecified",
                "Type 2 diabetes mellitus without complications",
                "Essential (primary) hypertension",
                "Low back pain",
            ],
            "diagnosis_type": ["primary", "primary", "secondary", "primary"],
            "diagnosis_date": [
                "2024-01-15",
                "2024-01-16",
                "2024-01-16",
                "2024-01-17",
            ],
        }
    )


@pytest.fixture
def sample_lab_data() -> pd.DataFrame:
    """
    Sample laboratory data for testing measurement mapping.

    Simulates lab results to be mapped to LOINC codes.
    """
    return pd.DataFrame(
        {
            "lab_id": [3001, 3002, 3003, 3004, 3005],
            "patient_id": [101, 102, 101, 103, 102],
            "test_code": ["FBS", "HBA1C", "CBC", "LIPID", "FBS"],
            "test_name": [
                "Fasting Blood Sugar",
                "Glycated Hemoglobin",
                "Complete Blood Count",
                "Lipid Profile",
                "Fasting Blood Sugar",
            ],
            "result_value": [95.0, 6.5, None, None, 110.0],
            "result_unit": ["mg/dL", "%", None, None, "mg/dL"],
            "reference_range": ["70-100", "4.0-5.6", None, None, "70-100"],
            "result_flag": ["normal", "high", None, None, "high"],
            "collection_date": [
                "2024-01-15",
                "2024-01-15",
                "2024-01-16",
                "2024-01-17",
                "2024-01-18",
            ],
        }
    )


@pytest.fixture
def sample_csv_file(tmp_data_path: Path, sample_drug_data: pd.DataFrame) -> Path:
    """Create a sample CSV file with drug data."""
    csv_path = tmp_data_path / "drugs.csv"
    sample_drug_data.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture
def sample_parquet_file(tmp_data_path: Path, sample_drug_data: pd.DataFrame) -> Path:
    """Create a sample Parquet file with drug data."""
    parquet_path = tmp_data_path / "drugs.parquet"
    sample_drug_data.to_parquet(parquet_path, index=False)
    return parquet_path


class MockAPIResponse:
    """Mock HTTP response for testing API interactions."""

    def __init__(self, data: dict, status_code: int = 200):
        self._data = data
        self.status_code = status_code
        self.text = str(data)

    def json(self) -> dict:
        return self._data


@pytest.fixture
def mock_api_response():
    """Factory fixture for creating mock API responses."""

    def _create_response(data: dict, status_code: int = 200) -> MockAPIResponse:
        return MockAPIResponse(data, status_code)

    return _create_response

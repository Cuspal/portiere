"""
Portiere Source Model — Represents a data source.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import structlog
from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from portiere.engines.base import AbstractEngine
    from portiere.models.project import Project

logger = structlog.get_logger(__name__)


def _numpy_default(obj: Any) -> Any:
    """JSON default handler for numpy/pandas types."""
    import numpy as np

    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class SourceProfile(BaseModel):
    """Profile statistics for a data source."""

    row_count: int
    column_count: int
    columns: list[dict[str, Any]]
    code_columns_detected: list[str] = Field(default_factory=list)
    phi_columns_detected: list[str] = Field(default_factory=list)
    sample_n: int | None = None


class Source(BaseModel):
    """
    A data source within a project.

    Sources can be:
    - File-based (CSV, Parquet, JSON)
    - Database (JDBC)
    - FHIR server
    """

    model_config = {"arbitrary_types_allowed": True}

    id: str
    name: str
    path: str
    format: str = "csv"

    # Internal references
    project: Project | None = Field(default=None, exclude=True)
    engine: AbstractEngine | None = Field(default=None, exclude=True)
    profile_result: SourceProfile | None = None

    def profile(self, sample_n: int | None = None) -> SourceProfile:
        """
        Profile the source data.

        Runs locally on the customer's engine. Extracts:
        - Schema information
        - Row/column counts (always exact from full data)
        - Cardinality per column (from sample when sample_n set)
        - Detects code columns (from sample — uses cardinality heuristics)
        - Detects PHI columns (schema-level — uses column name patterns)

        Args:
            sample_n: If set, use a sample of n rows for column-level
                exploration (n_unique, null_pct, top_values, code detection).
                Row count is always exact from the full dataset.

        Returns:
            SourceProfile with statistics
        """
        if self.engine is None:
            raise ValueError("No engine configured for source")

        logger.info("Profiling source", name=self.name, path=self.path, sample_n=sample_n)

        # Read full data — needed for exact row_count
        df = self.engine.read_source(self.path, format=self.format)
        full_row_count = self.engine.count(df)

        # Sample for column-level exploration if requested
        if sample_n is not None:
            df_profile = self.engine.sample(df, sample_n)
        else:
            df_profile = df

        # Profile the (possibly sampled) data for column-level stats
        raw_profile = self.engine.profile(df_profile)

        # Always use exact row_count from full data
        raw_profile["row_count"] = full_row_count

        # Detect code columns (high cardinality, string type, code-like patterns)
        code_columns = self._detect_code_columns(raw_profile)

        # Detect PHI columns (name, DOB, SSN, etc.) — schema-level, no data needed
        phi_columns = self._detect_phi_columns(raw_profile)

        # Convert numpy/pandas types to native Python for JSON serialization
        columns_native = json.loads(json.dumps(raw_profile["columns"], default=_numpy_default))

        self.profile_result = SourceProfile(
            row_count=raw_profile["row_count"],
            column_count=raw_profile["column_count"],
            columns=columns_native,
            code_columns_detected=code_columns,
            phi_columns_detected=phi_columns,
            sample_n=sample_n,
        )

        logger.info(
            "Profile complete",
            rows=self.profile_result.row_count,
            columns=self.profile_result.column_count,
            code_columns=len(code_columns),
            phi_columns=len(phi_columns),
            sample_n=sample_n,
        )

        return self.profile_result

    def _detect_code_columns(self, profile: dict) -> list[str]:
        """Detect columns that likely contain clinical codes.

        Excludes identifier columns (subject_id, hadm_id, etc.) and
        low-cardinality flag columns (icd_version, etc.).
        """
        # Known identifier column names — never clinical codes
        id_exclude = {
            "subject_id",
            "patient_id",
            "person_id",
            "hadm_id",
            "visit_id",
            "encounter_id",
            "admission_id",
            "record_id",
            "row_id",
            "note_id",
            "transfer_id",
            "stay_id",
        }

        # Patterns that indicate clinical code columns
        code_patterns = [
            "code",
            "icd",
            "snomed",
            "loinc",
            "rxnorm",
            "ndc",
            "cpt",
            "drg",
            "atc",
            "diagnosis",
            "procedure",
            "drug",
            "medication",
            "lab",
            "test",
        ]

        detected = []
        row_count = profile.get("row_count", 1)

        for col in profile.get("columns", []):
            col_name = col["name"].lower()
            col_type = col.get("type", "").lower()
            n_unique = col.get("n_unique", 0)

            # Skip known identifier columns
            if col_name in id_exclude:
                continue
            # Skip *_id columns with high cardinality (identifiers, not codes)
            if col_name.endswith("_id") and n_unique > row_count * 0.3:
                continue
            # Skip version/flag columns with very low cardinality
            if "version" in col_name and n_unique < 10:
                continue

            # Check if name matches clinical code patterns
            if any(pattern in col_name for pattern in code_patterns):
                detected.append(col["name"])
            # Check if it's a string column with moderate cardinality
            elif "str" in col_type or "utf" in col_type or "varchar" in col_type:
                # High cardinality but not unique per row = likely codes
                if 10 < n_unique < row_count * 0.5:
                    detected.append(col["name"])

        return detected

    def _detect_phi_columns(self, profile: dict) -> list[str]:
        """Detect columns that likely contain PHI."""
        phi_patterns = [
            "name",
            "first_name",
            "last_name",
            "dob",
            "birth",
            "ssn",
            "social",
            "address",
            "street",
            "city",
            "zip",
            "phone",
            "email",
            "mrn",
            "patient_id",
            "national_id",
            "passport",
            "license",
        ]

        detected = []
        for col in profile.get("columns", []):
            col_name = col["name"].lower()
            if any(pattern in col_name for pattern in phi_patterns):
                detected.append(col["name"])

        return detected

    def get_code_columns(
        self, engine: AbstractEngine, sample_n: int | None = None
    ) -> dict[str, list[dict]]:
        """
        Extract distinct code values from detected code columns.

        Args:
            engine: Compute engine to use
            sample_n: If set, extract distinct values from a sample of n rows

        Returns:
            Dict mapping column name to list of {code, description, count}
        """
        if self.profile_result is None:
            self.engine = engine
            self.profile(sample_n=sample_n)

        result = {}
        df = engine.read_source(self.path, format=self.format)

        # Sample for exploration if requested
        if sample_n is not None:
            df = engine.sample(df, sample_n)

        for col_name in self.profile_result.code_columns_detected if self.profile_result else []:
            distinct_values = engine.get_distinct_values(df, col_name, limit=5000)
            result[col_name] = [
                {
                    "code": v["value"],
                    "description": str(v["value"]),  # Placeholder
                    "count": v["count"],
                }
                for v in distinct_values
            ]

        return result

    def generate_artifact(self, engine_type: str = "spark"):
        """
        Generate ingestion artifact (Stage 1).

        Creates runnable script for data ingestion.
        """
        # Will be implemented in artifact generation
        raise NotImplementedError("Artifact generation will be implemented in Sprint 4")

    def __repr__(self) -> str:
        return f"Source(id={self.id!r}, name={self.name!r}, path={self.path!r})"

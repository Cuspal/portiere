"""
StorageBackend — Abstract base class for artifact storage.

Defines the contract that all storage backends must implement.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from portiere.models.concept_mapping import ConceptMapping
    from portiere.models.cross_mapping import CrossMapping
    from portiere.models.schema_mapping import SchemaMapping


class StorageBackend(ABC):
    """
    Abstract storage backend for Portiere project artifacts.

    Implementations:
    - LocalStorageBackend: Filesystem (YAML/CSV files)
    - CloudStorageBackend: Portiere Cloud API
    """

    # --- Project CRUD ---

    @abstractmethod
    def create_project(
        self,
        name: str,
        target_model: str,
        vocabularies: list[str],
        task: str = "standardize",
        source_standard: str | None = None,
    ) -> dict:
        """Create a new project. Returns project metadata dict with 'id'."""
        ...

    @abstractmethod
    def load_project(self, name: str) -> dict:
        """Load project metadata by name. Raises ValueError if not found."""
        ...

    @abstractmethod
    def list_projects(self) -> list[dict]:
        """List all projects. Returns list of metadata dicts."""
        ...

    @abstractmethod
    def delete_project(self, name: str) -> None:
        """Delete a project by name. Raises ValueError if not found."""
        ...

    @abstractmethod
    def project_exists(self, name: str) -> bool:
        """Check if a project exists by name."""
        ...

    # --- Sources ---

    @abstractmethod
    def save_source(self, project_name: str, source_name: str, metadata: dict) -> None:
        """Save source metadata."""
        ...

    @abstractmethod
    def list_sources(self, project_name: str) -> list[dict]:
        """List all sources for a project."""
        ...

    # --- Schema Mappings ---

    @abstractmethod
    def save_schema_mapping(self, project_name: str, mapping: SchemaMapping) -> None:
        """Save schema mapping artifacts."""
        ...

    @abstractmethod
    def load_schema_mapping(self, project_name: str) -> SchemaMapping:
        """Load schema mapping. Returns empty SchemaMapping if not found."""
        ...

    # --- Concept Mappings ---

    @abstractmethod
    def save_concept_mapping(self, project_name: str, mapping: ConceptMapping) -> None:
        """Save concept mapping artifacts."""
        ...

    @abstractmethod
    def load_concept_mapping(self, project_name: str) -> ConceptMapping:
        """Load concept mapping. Returns empty ConceptMapping if not found."""
        ...

    # --- Cross Mappings ---

    @abstractmethod
    def save_cross_mapping(self, project_name: str, mapping: CrossMapping) -> None:
        """Save cross-mapping run records."""
        ...

    @abstractmethod
    def load_cross_mapping(self, project_name: str) -> CrossMapping:
        """Load cross-mapping history. Returns empty CrossMapping if not found."""
        ...

    # --- ETL Scripts ---

    @abstractmethod
    def save_etl_script(self, project_name: str, script_name: str, content: str) -> None:
        """Save generated ETL script."""
        ...

    @abstractmethod
    def list_etl_scripts(self, project_name: str) -> list[str]:
        """List all ETL script filenames for a project."""
        ...

    # --- Validation Reports ---

    @abstractmethod
    def save_validation_report(self, project_name: str, report_name: str, report: dict) -> None:
        """Save validation report."""
        ...

    @abstractmethod
    def list_validation_reports(self, project_name: str) -> list[str]:
        """List all validation report names for a project."""
        ...

    # --- Profiles (GX profiling reports) ---

    @abstractmethod
    def save_profile(self, project_name: str, source_name: str, profile: dict) -> None:
        """Save profiling report for a source."""
        ...

    @abstractmethod
    def load_profile(self, project_name: str, source_name: str) -> dict | None:
        """Load profiling report for a source. Returns None if not found."""
        ...

    # --- Quality Reports (GX validation reports) ---

    @abstractmethod
    def save_quality_report(self, project_name: str, report: dict) -> None:
        """Save GX quality/validation report."""
        ...

    @abstractmethod
    def load_quality_reports(self, project_name: str) -> list[dict]:
        """Load all GX quality reports for a project."""
        ...

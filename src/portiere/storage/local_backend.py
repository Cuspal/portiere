"""
LocalStorageBackend — Filesystem-based artifact storage.

Stores project artifacts as YAML and CSV files in a directory structure:

    {base_dir}/{project_name}/
    ├── project.yaml
    ├── sources/
    ├── schema_mappings/
    ├── concept_mappings/
    ├── etl_scripts/
    ├── validation_reports/
    ├── profiles/
    └── quality_reports/

Used by the unified Project class via ``portiere.init()``.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import structlog
import yaml

from portiere.storage.base import StorageBackend

if TYPE_CHECKING:
    from portiere.models.concept_mapping import ConceptMapping
    from portiere.models.cross_mapping import CrossMapping
    from portiere.models.schema_mapping import SchemaMapping

logger = structlog.get_logger(__name__)

# Subdirectories created for each project
_PROJECT_SUBDIRS = [
    "sources",
    "schema_mappings",
    "concept_mappings",
    "cross_mappings",
    "etl_scripts",
    "validation_reports",
    "profiles",
    "quality_reports",
]


class LocalStorageBackend(StorageBackend):
    """
    Filesystem-based storage backend.

    All artifacts are stored as YAML/CSV/JSON files within the project directory.
    """

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = Path(base_dir).expanduser()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        logger.debug("local_storage.initialized", base_dir=str(self.base_dir))

    def _project_dir(self, name: str) -> Path:
        return self.base_dir / name

    def _ensure_subdirs(self, project_dir: Path) -> None:
        """Ensure all expected subdirectories exist (handles legacy projects)."""
        for subdir in _PROJECT_SUBDIRS:
            (project_dir / subdir).mkdir(exist_ok=True)

    # --- Project CRUD ---

    def create_project(
        self,
        name: str,
        target_model: str,
        vocabularies: list[str],
        task: str = "standardize",
        source_standard: str | None = None,
    ) -> dict:
        project_dir = self._project_dir(name)
        if project_dir.exists():
            raise ValueError(
                f"Project '{name}' already exists at {project_dir}. "
                "Use load_project() to open existing projects."
            )

        project_dir.mkdir(parents=False)
        self._ensure_subdirs(project_dir)

        metadata = {
            "id": str(uuid4()),
            "name": name,
            "task": task,
            "target_model": target_model,
            "source_standard": source_standard,
            "vocabularies": vocabularies,
            "created_at": datetime.now(tz=timezone.utc).isoformat(),
            "updated_at": datetime.now(tz=timezone.utc).isoformat(),
            "mode": "local",
            "version": "1.0",
        }

        with open(project_dir / "project.yaml", "w") as f:
            yaml.dump(metadata, f, default_flow_style=False, sort_keys=False)

        logger.info(
            "local_storage.project_created",
            project_id=metadata["id"],
            name=name,
        )
        return metadata

    def load_project(self, name: str) -> dict:
        project_dir = self._project_dir(name)
        yaml_path = project_dir / "project.yaml"

        if not project_dir.exists() or not yaml_path.exists():
            raise ValueError(f"Project '{name}' not found in {self.base_dir}.")

        with open(yaml_path) as f:
            metadata = yaml.safe_load(f)

        # Ensure new subdirs exist for legacy projects
        self._ensure_subdirs(project_dir)
        return metadata

    def list_projects(self) -> list[dict]:
        projects = []
        for project_dir in self.base_dir.iterdir():
            if project_dir.is_dir():
                yaml_path = project_dir / "project.yaml"
                if yaml_path.exists():
                    with open(yaml_path) as f:
                        projects.append(yaml.safe_load(f))
        return projects

    def delete_project(self, name: str) -> None:
        project_dir = self._project_dir(name)
        if not project_dir.exists():
            raise ValueError(f"Project '{name}' not found in {self.base_dir}")
        shutil.rmtree(project_dir)
        logger.info("local_storage.project_deleted", name=name)

    def project_exists(self, name: str) -> bool:
        project_dir = self._project_dir(name)
        return project_dir.exists() and (project_dir / "project.yaml").exists()

    # --- Sources ---

    def save_source(self, project_name: str, source_name: str, metadata: dict) -> None:
        source_path = self._project_dir(project_name) / "sources" / f"{source_name}.yaml"
        with open(source_path, "w") as f:
            yaml.dump(metadata, f, default_flow_style=False, sort_keys=False)
        self._update_timestamp(project_name)

    def list_sources(self, project_name: str) -> list[dict]:
        sources_dir = self._project_dir(project_name) / "sources"
        sources = []
        for source_file in sources_dir.glob("*.yaml"):
            with open(source_file) as f:
                sources.append(yaml.safe_load(f))
        return sources

    # --- Schema Mappings ---

    def save_schema_mapping(self, project_name: str, mapping: SchemaMapping) -> None:
        mapping_path = self._project_dir(project_name) / "schema_mappings" / "schema_mapping.yaml"
        items_data = [item.model_dump(mode="json") for item in mapping.items]
        with open(mapping_path, "w") as f:
            yaml.dump(items_data, f, default_flow_style=False, sort_keys=False)
        self._update_timestamp(project_name)
        logger.info(
            "local_storage.schema_mapping_saved",
            project=project_name,
            items_count=len(mapping.items),
        )

    def load_schema_mapping(self, project_name: str) -> SchemaMapping:
        from portiere.models.schema_mapping import SchemaMapping, SchemaMappingItem

        mapping_path = self._project_dir(project_name) / "schema_mappings" / "schema_mapping.yaml"
        if not mapping_path.exists():
            return SchemaMapping(items=[])

        with open(mapping_path) as f:
            items_data = yaml.safe_load(f) or []

        items = [SchemaMappingItem(**item) for item in items_data]
        return SchemaMapping(items=items)

    # --- Concept Mappings ---

    def save_concept_mapping(self, project_name: str, mapping: ConceptMapping) -> None:
        import pandas as pd

        project_dir = self._project_dir(project_name)

        # Save as YAML
        yaml_path = project_dir / "concept_mappings" / "concept_mapping.yaml"
        items_data = [item.model_dump(mode="json") for item in mapping.items]
        with open(yaml_path, "w") as f:
            yaml.dump(items_data, f, default_flow_style=False, sort_keys=False)

        # Also save as CSV for easy database import
        csv_path = project_dir / "concept_mappings" / "source_to_concept_map.csv"
        if items_data:
            df = pd.DataFrame(items_data)
            df.to_csv(csv_path, index=False)
        else:
            df = pd.DataFrame(
                columns=[
                    "source_code",
                    "source_description",
                    "target_concept_id",
                    "target_concept_name",
                    "vocabulary_id",
                    "domain_id",
                    "confidence",
                    "method",
                ]
            )
            df.to_csv(csv_path, index=False)

        self._update_timestamp(project_name)
        logger.info(
            "local_storage.concept_mapping_saved",
            project=project_name,
            items_count=len(mapping.items),
        )

    def load_concept_mapping(self, project_name: str) -> ConceptMapping:
        from portiere.models.concept_mapping import ConceptMapping, ConceptMappingItem

        mapping_path = self._project_dir(project_name) / "concept_mappings" / "concept_mapping.yaml"
        if not mapping_path.exists():
            return ConceptMapping(items=[])

        with open(mapping_path) as f:
            items_data = yaml.safe_load(f) or []

        items = [ConceptMappingItem(**item) for item in items_data]
        return ConceptMapping(items=items)

    # --- Cross Mappings ---

    def save_cross_mapping(self, project_name: str, mapping: CrossMapping) -> None:
        mapping_path = self._project_dir(project_name) / "cross_mappings" / "cross_mapping.yaml"
        runs_data = [run.model_dump(mode="json") for run in mapping.runs]
        with open(mapping_path, "w") as f:
            yaml.dump(runs_data, f, default_flow_style=False, sort_keys=False)
        self._update_timestamp(project_name)
        logger.info(
            "local_storage.cross_mapping_saved",
            project=project_name,
            runs_count=len(mapping.runs),
        )

    def load_cross_mapping(self, project_name: str) -> CrossMapping:
        from portiere.models.cross_mapping import CrossMapping, CrossMappingRun

        mapping_path = self._project_dir(project_name) / "cross_mappings" / "cross_mapping.yaml"
        if not mapping_path.exists():
            return CrossMapping(runs=[])

        with open(mapping_path) as f:
            runs_data = yaml.safe_load(f) or []

        runs = [CrossMappingRun(**run) for run in runs_data]
        return CrossMapping(runs=runs)

    # --- ETL Scripts ---

    def save_etl_script(self, project_name: str, script_name: str, content: str) -> None:
        script_path = self._project_dir(project_name) / "etl_scripts" / script_name
        with open(script_path, "w") as f:
            f.write(content)
        self._update_timestamp(project_name)

    def list_etl_scripts(self, project_name: str) -> list[str]:
        scripts_dir = self._project_dir(project_name) / "etl_scripts"
        return [f.name for f in scripts_dir.glob("*") if f.is_file()]

    # --- Validation Reports ---

    def save_validation_report(self, project_name: str, report_name: str, report: dict) -> None:
        report_path = self._project_dir(project_name) / "validation_reports" / f"{report_name}.yaml"
        with open(report_path, "w") as f:
            yaml.dump(report, f, default_flow_style=False, sort_keys=False)
        self._update_timestamp(project_name)

    def list_validation_reports(self, project_name: str) -> list[str]:
        reports_dir = self._project_dir(project_name) / "validation_reports"
        return [f.stem for f in reports_dir.glob("*.yaml")]

    # --- Profiles (GX) ---

    def save_profile(self, project_name: str, source_name: str, profile: dict) -> None:
        profile_path = self._project_dir(project_name) / "profiles" / f"{source_name}.json"
        with open(profile_path, "w") as f:
            json.dump(profile, f, indent=2, default=str)
        self._update_timestamp(project_name)

    def load_profile(self, project_name: str, source_name: str) -> dict | None:
        profile_path = self._project_dir(project_name) / "profiles" / f"{source_name}.json"
        if not profile_path.exists():
            return None
        with open(profile_path) as f:
            return json.load(f)

    # --- Quality Reports (GX) ---

    def save_quality_report(self, project_name: str, report: dict) -> None:
        reports_dir = self._project_dir(project_name) / "quality_reports"
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        report_name = report.get("table_name", "report")
        report_path = reports_dir / f"{report_name}_{timestamp}.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        self._update_timestamp(project_name)

    def load_quality_reports(self, project_name: str) -> list[dict]:
        reports_dir = self._project_dir(project_name) / "quality_reports"
        reports = []
        for report_file in sorted(reports_dir.glob("*.json")):
            with open(report_file) as f:
                reports.append(json.load(f))
        return reports

    # --- Sync Metadata ---

    def save_sync_metadata(
        self, project_name: str, cloud_id: str, timestamp: str | None = None
    ) -> None:
        """Save cloud sync metadata to project.yaml."""
        yaml_path = self._project_dir(project_name) / "project.yaml"
        with open(yaml_path) as f:
            metadata = yaml.safe_load(f)

        metadata["cloud_project_id"] = cloud_id
        metadata["last_synced"] = timestamp or datetime.now(tz=timezone.utc).isoformat()

        with open(yaml_path, "w") as f:
            yaml.dump(metadata, f, default_flow_style=False, sort_keys=False)

    def load_sync_metadata(self, project_name: str) -> dict | None:
        """Load cloud sync metadata. Returns None if not synced."""
        yaml_path = self._project_dir(project_name) / "project.yaml"
        with open(yaml_path) as f:
            metadata = yaml.safe_load(f)

        cloud_id = metadata.get("cloud_project_id")
        if not cloud_id:
            return None

        return {
            "cloud_project_id": cloud_id,
            "last_synced": metadata.get("last_synced"),
        }

    # --- Internal ---

    def _update_timestamp(self, project_name: str) -> None:
        """Update project.yaml with current timestamp."""
        yaml_path = self._project_dir(project_name) / "project.yaml"
        if not yaml_path.exists():
            return
        with open(yaml_path) as f:
            metadata = yaml.safe_load(f)
        metadata["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
        with open(yaml_path, "w") as f:
            yaml.dump(metadata, f, default_flow_style=False, sort_keys=False)

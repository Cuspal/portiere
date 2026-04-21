"""
CloudStorageBackend — Cloud API-based artifact storage.

Wraps the Portiere Client to implement StorageBackend, storing artifacts
via the sync API endpoints. Uses the same serialization contract as
SyncManager for backward compatibility.

ETL scripts, validation reports, profiles, and quality reports are stored
locally even in cloud mode (not part of the cloud API).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

from portiere.storage.base import StorageBackend

if TYPE_CHECKING:
    from portiere.client import Client
    from portiere.models.concept_mapping import ConceptMapping
    from portiere.models.cross_mapping import CrossMapping
    from portiere.models.schema_mapping import SchemaMapping

logger = structlog.get_logger(__name__)


class CloudStorageBackend(StorageBackend):
    """
    Cloud API-based storage backend.

    Schema/concept mappings are stored via the Portiere Cloud API.
    Local-only artifacts (ETL scripts, reports, profiles) fall back
    to filesystem storage in a local cache directory.
    """

    _CLOUD_MSG = (
        "Cloud storage is not available in the open-source SDK. "
        "Use LocalStorageBackend for local projects. "
        "For cloud storage and sync, see https://portiere.io"
    )

    _client: Client
    _local_cache_dir: Path
    _project_cache: dict[str, str]

    def __init__(
        self,
        client: Client,
        local_cache_dir: Path | None = None,
    ) -> None:
        """
        Raises:
            NotImplementedError: Always. Cloud storage requires Portiere Cloud.
        """
        raise NotImplementedError(self._CLOUD_MSG)

    def _get_cloud_id(self, name: str) -> str:
        """Get cloud project ID by name, using cache."""
        if name in self._project_cache:
            return self._project_cache[name]
        raise ValueError(
            f"Project '{name}' not found in cloud cache. Create or load the project first."
        )

    def _local_project_dir(self, name: str) -> Path:
        """Local cache directory for a cloud project's local-only artifacts."""
        d = self._local_cache_dir / name
        d.mkdir(parents=True, exist_ok=True)
        return d

    # --- Project CRUD ---

    def create_project(
        self,
        name: str,
        target_model: str,
        vocabularies: list[str],
        task: str = "standardize",
        source_standard: str | None = None,
    ) -> dict:
        request_body: dict[str, Any] = {
            "name": name,
            "target_model": target_model,
            "vocabularies": vocabularies,
            "task": task,
        }
        if source_standard:
            request_body["source_standard"] = source_standard
        response = self._client._request(
            "POST",
            "/projects",
            json=request_body,
        )
        cloud_id = response["id"]
        self._project_cache[name] = cloud_id
        logger.info(
            "cloud_storage.project_created",
            cloud_id=cloud_id,
            name=name,
            task=task,
        )
        return {
            "id": cloud_id,
            "name": name,
            "task": response.get("task", task),
            "target_model": target_model,
            "source_standard": response.get("source_standard", source_standard),
            "vocabularies": vocabularies,
            "created_at": response.get("created_at", datetime.now(tz=timezone.utc).isoformat()),
            "mode": "cloud",
        }

    def load_project(self, name: str) -> dict:
        # List projects and find by name
        response = self._client._request("GET", "/projects")
        for project in response.get("projects", []):
            if project.get("name") == name:
                self._project_cache[name] = project["id"]
                return project
        raise ValueError(f"Project '{name}' not found on Portiere Cloud.")

    def list_projects(self) -> list[dict]:
        response = self._client._request("GET", "/projects")
        projects = response.get("projects", [])
        # Update cache
        for p in projects:
            self._project_cache[p["name"]] = p["id"]
        return projects

    def delete_project(self, name: str) -> None:
        cloud_id = self._get_cloud_id(name)
        self._client._request("DELETE", f"/projects/{cloud_id}")
        self._project_cache.pop(name, None)
        logger.info("cloud_storage.project_deleted", name=name)

    def project_exists(self, name: str) -> bool:
        if name in self._project_cache:
            return True
        try:
            self.load_project(name)
            return True
        except (ValueError, Exception):
            return False

    # --- Sources ---

    def save_source(self, project_name: str, source_name: str, metadata: dict) -> None:
        # Sources are tracked locally for cloud projects
        import yaml

        source_dir = self._local_project_dir(project_name) / "sources"
        source_dir.mkdir(exist_ok=True)
        with open(source_dir / f"{source_name}.yaml", "w") as f:
            yaml.dump(metadata, f, default_flow_style=False, sort_keys=False)

    def list_sources(self, project_name: str) -> list[dict]:
        import yaml

        source_dir = self._local_project_dir(project_name) / "sources"
        if not source_dir.exists():
            return []
        sources = []
        for source_file in source_dir.glob("*.yaml"):
            with open(source_file) as f:
                sources.append(yaml.safe_load(f))
        return sources

    # --- Schema Mappings (Cloud API) ---

    def save_schema_mapping(self, project_name: str, mapping: SchemaMapping) -> None:
        cloud_id = self._get_cloud_id(project_name)
        items_data = [item.model_dump(mode="json") for item in mapping.items]
        self._client._request(
            "POST",
            f"/sync/projects/{cloud_id}/schema-mappings/bulk",
            json={"items": items_data},
        )
        logger.info(
            "cloud_storage.schema_mapping_saved",
            project=project_name,
            items_count=len(mapping.items),
        )

    def load_schema_mapping(self, project_name: str) -> SchemaMapping:
        from portiere.models.schema_mapping import SchemaMapping, SchemaMappingItem

        cloud_id = self._get_cloud_id(project_name)
        response = self._client._request(
            "GET",
            f"/sync/projects/{cloud_id}/schema-mappings",
        )
        items = [SchemaMappingItem(**item) for item in response.get("items", [])]
        return SchemaMapping(items=items)

    # --- Concept Mappings (Cloud API) ---

    def save_concept_mapping(self, project_name: str, mapping: ConceptMapping) -> None:
        cloud_id = self._get_cloud_id(project_name)
        items_data = [item.model_dump(mode="json") for item in mapping.items]
        self._client._request(
            "POST",
            f"/sync/projects/{cloud_id}/concept-mappings/bulk",
            json={"items": items_data},
        )
        logger.info(
            "cloud_storage.concept_mapping_saved",
            project=project_name,
            items_count=len(mapping.items),
        )

    def load_concept_mapping(self, project_name: str) -> ConceptMapping:
        from portiere.models.concept_mapping import ConceptMapping, ConceptMappingItem

        cloud_id = self._get_cloud_id(project_name)
        response = self._client._request(
            "GET",
            f"/sync/projects/{cloud_id}/concept-mappings",
        )
        items = [ConceptMappingItem(**item) for item in response.get("items", [])]
        return ConceptMapping(items=items)

    # --- Cross Mappings (Cloud API) ---

    def save_cross_mapping(self, project_name: str, mapping: CrossMapping) -> None:
        cloud_id = self._get_cloud_id(project_name)
        items_data = [run.model_dump(mode="json") for run in mapping.runs]
        self._client._request(
            "POST",
            f"/sync/projects/{cloud_id}/cross-mappings/bulk",
            json={"items": items_data},
        )
        logger.info(
            "cloud_storage.cross_mapping_saved",
            project=project_name,
            runs_count=len(mapping.runs),
        )

    def load_cross_mapping(self, project_name: str) -> CrossMapping:
        from portiere.models.cross_mapping import CrossMapping, CrossMappingRun

        cloud_id = self._get_cloud_id(project_name)
        response = self._client._request(
            "GET",
            f"/sync/projects/{cloud_id}/cross-mappings",
        )
        runs = [CrossMappingRun(**item) for item in response.get("items", [])]
        return CrossMapping(runs=runs)

    # --- ETL Scripts (local cache) ---

    def save_etl_script(self, project_name: str, script_name: str, content: str) -> None:
        scripts_dir = self._local_project_dir(project_name) / "etl_scripts"
        scripts_dir.mkdir(exist_ok=True)
        with open(scripts_dir / script_name, "w") as f:
            f.write(content)

    def list_etl_scripts(self, project_name: str) -> list[str]:
        scripts_dir = self._local_project_dir(project_name) / "etl_scripts"
        if not scripts_dir.exists():
            return []
        return [f.name for f in scripts_dir.glob("*") if f.is_file()]

    # --- Validation Reports (local cache) ---

    def save_validation_report(self, project_name: str, report_name: str, report: dict) -> None:
        import yaml

        reports_dir = self._local_project_dir(project_name) / "validation_reports"
        reports_dir.mkdir(exist_ok=True)
        with open(reports_dir / f"{report_name}.yaml", "w") as f:
            yaml.dump(report, f, default_flow_style=False, sort_keys=False)

    def list_validation_reports(self, project_name: str) -> list[str]:
        reports_dir = self._local_project_dir(project_name) / "validation_reports"
        if not reports_dir.exists():
            return []
        return [f.stem for f in reports_dir.glob("*.yaml")]

    # --- Profiles (local cache) ---

    def save_profile(self, project_name: str, source_name: str, profile: dict) -> None:
        profiles_dir = self._local_project_dir(project_name) / "profiles"
        profiles_dir.mkdir(exist_ok=True)
        with open(profiles_dir / f"{source_name}.json", "w") as f:
            json.dump(profile, f, indent=2, default=str)

    def load_profile(self, project_name: str, source_name: str) -> dict | None:
        profile_path = self._local_project_dir(project_name) / "profiles" / f"{source_name}.json"
        if not profile_path.exists():
            return None
        with open(profile_path) as f:
            return json.load(f)

    # --- Quality Reports (local cache) ---

    def save_quality_report(self, project_name: str, report: dict) -> None:
        reports_dir = self._local_project_dir(project_name) / "quality_reports"
        reports_dir.mkdir(exist_ok=True)
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        report_name = report.get("table_name", "report")
        with open(reports_dir / f"{report_name}_{timestamp}.json", "w") as f:
            json.dump(report, f, indent=2, default=str)

    def load_quality_reports(self, project_name: str) -> list[dict]:
        reports_dir = self._local_project_dir(project_name) / "quality_reports"
        if not reports_dir.exists():
            return []
        reports = []
        for report_file in sorted(reports_dir.glob("*.json")):
            with open(report_file) as f:
                reports.append(json.load(f))
        return reports

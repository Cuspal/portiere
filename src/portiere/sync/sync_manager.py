"""
Sync Manager — Bidirectional sync between local projects and Portiere Cloud.

Enables local projects to push mappings to cloud for collaboration,
and pull cloud changes back to local artifacts.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import structlog
import yaml

if TYPE_CHECKING:
    from portiere.client import Client
    from portiere.project import Project as LocalProject

logger = structlog.get_logger(__name__)


class SyncManager:
    """
    Manages bidirectional sync between local projects and Portiere Cloud.

    Usage:
        from portiere import Client
        from portiere.sync import SyncManager

        client = Client(api_key="pt_sk_...")
        sync = SyncManager(client=client, local_project=project)

        # Push local changes to cloud
        cloud_project_id = sync.push()

        # Pull cloud changes to local
        sync.pull()

        # Check sync status
        status = sync.status()
    """

    _CLOUD_MSG = (
        "Cloud sync is not available in the open-source SDK. "
        "For cloud sync and collaboration, see https://portiere.io"
    )

    client: Client
    local_project: Any
    cloud_project_id: str | None

    def __init__(self, client: Client, local_project: LocalProject) -> None:
        """
        Raises:
            NotImplementedError: Always. Cloud sync requires Portiere Cloud.
        """
        raise NotImplementedError(self._CLOUD_MSG)

    def push(self) -> str:
        """
        Push local project to cloud.

        Creates a new cloud project if not yet linked, then syncs
        schema mappings and concept mappings.

        Returns:
            Cloud project ID
        """
        # 1. Create or get cloud project
        if self.cloud_project_id:
            logger.info(
                "sync.push.existing_project",
                cloud_project_id=self.cloud_project_id,
            )
        else:
            response = self.client._request(
                "POST",
                "/projects",
                json={
                    "name": self.local_project.name,
                    "target_model": self.local_project.target_model,
                    "vocabularies": self.local_project.vocabularies,
                },
            )
            self.cloud_project_id = response["id"]
            self._save_sync_metadata()
            logger.info(
                "sync.push.created_project",
                cloud_project_id=self.cloud_project_id,
            )

        # 2. Push schema mappings
        schema_mapping = self.local_project.load_schema_mapping()
        if schema_mapping.items:
            items_data = [item.model_dump(mode="json") for item in schema_mapping.items]
            self.client._request(
                "POST",
                f"/sync/projects/{self.cloud_project_id}/schema-mappings/bulk",
                json={"items": items_data},
            )
            logger.info(
                "sync.push.schema_mappings",
                count=len(schema_mapping.items),
            )

        # 3. Push concept mappings
        concept_mapping = self.local_project.load_concept_mapping()
        if concept_mapping.items:
            items_data = [item.model_dump(mode="json") for item in concept_mapping.items]
            self.client._request(
                "POST",
                f"/sync/projects/{self.cloud_project_id}/concept-mappings/bulk",
                json={"items": items_data},
            )
            logger.info(
                "sync.push.concept_mappings",
                count=len(concept_mapping.items),
            )

        # 4. Update sync timestamp
        self._save_sync_metadata()

        logger.info(
            "sync.push.complete",
            cloud_project_id=self.cloud_project_id,
        )
        return self.cloud_project_id

    def pull(self) -> None:
        """
        Pull changes from cloud to local project.

        Downloads schema mappings and concept mappings from cloud
        and saves them to local artifact files.

        Raises:
            ValueError: If project not linked to cloud
        """
        if not self.cloud_project_id:
            raise ValueError("Project not linked to cloud. Run push() first.")

        # 1. Pull schema mappings
        from portiere.models.schema_mapping import SchemaMapping, SchemaMappingItem

        response = self.client._request(
            "GET",
            f"/sync/projects/{self.cloud_project_id}/schema-mappings",
        )
        schema_items = [SchemaMappingItem(**item) for item in response.get("items", [])]
        schema_mapping = SchemaMapping(items=schema_items)
        self.local_project.save_schema_mapping(schema_mapping)
        logger.info(
            "sync.pull.schema_mappings",
            count=len(schema_items),
        )

        # 2. Pull concept mappings
        from portiere.models.concept_mapping import ConceptMapping, ConceptMappingItem

        response = self.client._request(
            "GET",
            f"/sync/projects/{self.cloud_project_id}/concept-mappings",
        )
        concept_items = [ConceptMappingItem(**item) for item in response.get("items", [])]
        concept_mapping = ConceptMapping(items=concept_items)
        self.local_project.save_concept_mapping(concept_mapping)
        logger.info(
            "sync.pull.concept_mappings",
            count=len(concept_items),
        )

        # 3. Update sync timestamp
        self._save_sync_metadata()

        logger.info("sync.pull.complete")

    def status(self) -> dict:
        """
        Check sync status between local and cloud.

        Returns:
            Dict with status, message, and details
        """
        if not self.cloud_project_id:
            return {
                "status": "not_synced",
                "message": "Project not linked to cloud. Run push() first.",
            }

        # Load local metadata for last sync time
        project_yaml = self.local_project.directory / "project.yaml"
        with open(project_yaml) as f:
            metadata = yaml.safe_load(f)

        last_synced = metadata.get("last_synced")
        local_updated = self.local_project.updated_at

        return {
            "status": "synced" if last_synced else "not_synced",
            "cloud_project_id": self.cloud_project_id,
            "last_synced": last_synced,
            "local_updated_at": local_updated,
            "message": (f"Last synced: {last_synced}" if last_synced else "Not yet synced"),
        }

    def unlink(self) -> None:
        """
        Unlink local project from cloud (remove cloud_project_id).
        """
        self.cloud_project_id = None

        project_yaml = self.local_project.directory / "project.yaml"
        with open(project_yaml) as f:
            metadata = yaml.safe_load(f)

        metadata.pop("cloud_project_id", None)
        metadata.pop("last_synced", None)

        with open(project_yaml, "w") as f:
            yaml.dump(metadata, f, default_flow_style=False, sort_keys=False)

        logger.info("sync.unlinked", project=self.local_project.name)

    def _save_sync_metadata(self) -> None:
        """Save cloud project ID and sync timestamp to project.yaml."""
        project_yaml = self.local_project.directory / "project.yaml"
        with open(project_yaml) as f:
            metadata = yaml.safe_load(f)

        metadata["cloud_project_id"] = self.cloud_project_id
        metadata["last_synced"] = datetime.now(tz=timezone.utc).isoformat()

        with open(project_yaml, "w") as f:
            yaml.dump(metadata, f, default_flow_style=False, sort_keys=False)

        # Update local project instance
        self.local_project.cloud_project_id = self.cloud_project_id

"""One source-aware, revision-checked mapping store for the SDK and review UI.

The current JSON snapshot is authoritative. Legacy YAML remains readable and
is never rewritten. A source ID is hashed for filesystem-safe directory names.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import yaml

from portiere.models.concept_mapping import ConceptMapping
from portiere.models.schema_mapping import SchemaMapping

MappingKind = Literal["schema", "concept"]


class MappingConflictError(ValueError):
    """The mapping changed since it was loaded, or another writer is active."""


@contextmanager
def project_write_lock(project_dir: Path):
    """Serialize mapping migration and source registration within a project."""
    project_dir.mkdir(parents=True, exist_ok=True)
    lock = project_dir / ".mapping.write.lock"
    try:
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise MappingConflictError(f"Another project write is active; reload and retry ({lock}).")
    os.close(descriptor)
    try:
        yield
    finally:
        lock.unlink(missing_ok=True)


def atomic_write(path: Path, raw: bytes):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent, prefix=".mapping-", delete=False
        ) as stream:
            temporary = Path(stream.name)
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def touch_project(project_dir: Path) -> None:
    """Update metadata under the caller's project write lock."""
    path = project_dir / "project.yaml"
    if path.exists():
        metadata = yaml.safe_load(path.read_text(encoding="utf-8"))
        metadata["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
        atomic_write(path, yaml.safe_dump(metadata, sort_keys=False).encode("utf-8"))


class MappingStore:
    def __init__(self, project_dir: Path):
        self.project_dir = Path(project_dir)

    def source_ids(self, kind: MappingKind) -> list[str]:
        ids = set()
        directory = self.project_dir / f"{kind}_mappings"
        for path in directory.glob(f"sources/*/{kind}_mapping_reviewed.json"):
            data = json.loads(path.read_text(encoding="utf-8"))
            if data.get("source_id"):
                ids.add(data["source_id"])
        return sorted(ids)

    def _registered_ids(self) -> set[str]:
        ids = set()
        for path in (self.project_dir / "sources").glob("*.yaml"):
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            if data.get("id"):
                ids.add(data["id"])
        return ids

    def _registration_count(self) -> int:
        return len(list((self.project_dir / "sources").glob("*.yaml")))

    def _source_id(self, kind: MappingKind, source_id: str | None) -> str | None:
        if source_id is not None:
            if not source_id.strip():
                raise ValueError("source_id must not be empty")
            return source_id
        ids = set(self.source_ids(kind)) | self._registered_ids()
        if len(ids) > 1 or self._registration_count() > 1:
            raise ValueError(
                "Multiple sources exist; select a source_id when loading or saving mappings."
            )
        # An existing unscoped legacy mapping remains unscoped until bound.
        if self._existing(kind, None, fallback=False) is not None:
            if self.source_ids(kind):
                raise ValueError(
                    "Both legacy and source-scoped mappings exist; select a source_id."
                )
            return None
        return next(iter(ids), None)

    def _directory(self, kind: MappingKind, source_id: str | None) -> Path:
        directory = self.project_dir / f"{kind}_mappings"
        if source_id is not None:
            key = hashlib.sha256(source_id.encode("utf-8")).hexdigest()
            directory = directory / "sources" / key
        return directory

    def _existing(self, kind: MappingKind, source_id: str | None, *, fallback=True) -> Path | None:
        directory = self._directory(kind, source_id)
        reviewed = directory / f"{kind}_mapping_reviewed.json"
        original = directory / f"{kind}_mapping.yaml"
        if reviewed.exists():
            # Unversioned reviews cannot prove they apply to a newer draft.
            data = json.loads(reviewed.read_text(encoding="utf-8"))
            if not data.get("format_version") and original.exists():
                if original.stat().st_mtime_ns > reviewed.stat().st_mtime_ns:
                    raise MappingConflictError(
                        "Legacy draft changed after review; reload and reconcile the mapping files."
                    )
            return reviewed
        if original.exists():
            return original
        if fallback and source_id is not None:
            ids = self._registered_ids() | set(self.source_ids(kind))
            if ids == {source_id} and self._registration_count() == 1:
                return self._existing(kind, None, fallback=False)
        return None

    def load(self, kind: MappingKind, source_id: str | None = None):
        source_id = self._source_id(kind, source_id)
        model = SchemaMapping if kind == "schema" else ConceptMapping
        path = self._existing(kind, source_id)
        if path is None:
            return model(source_id=source_id)
        raw = path.read_bytes()
        data = json.loads(raw) if path.suffix == ".json" else yaml.safe_load(raw)
        payload = {"items": data or []} if not isinstance(data, dict) else data
        if payload.get("format_version", 1) != 1:
            raise ValueError(f"Unsupported mapping format in {path}")
        stored_source = payload.get("source_id")
        if stored_source is not None and stored_source != source_id:
            raise ValueError(f"Mapping source_id does not match its location: {path}")
        return model.model_validate(
            {
                **payload,
                "source_id": source_id,
                "revision": hashlib.sha256(raw).hexdigest(),
            }
        )

    def save(self, kind: MappingKind, mapping) -> Path:
        with project_write_lock(self.project_dir):
            source_id = self._source_id(kind, mapping.source_id)
            directory = self._directory(kind, source_id)
            directory.mkdir(parents=True, exist_ok=True)
            target = directory / f"{kind}_mapping_reviewed.json"
            current = self.load(kind, source_id)
            if mapping.revision is not None and mapping.revision != current.revision:
                raise MappingConflictError(
                    "Mapping revision changed; reload before saving your review."
                )
            if mapping.revision is None and current.revision is not None:
                raise MappingConflictError(
                    "An existing mapping revision must be loaded before replacement."
                )
            payload = mapping.model_dump(mode="json", exclude={"project", "source", "revision"})
            payload.update(format_version=1, source_id=source_id)
            raw = (json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode(
                "utf-8"
            )
            atomic_write(target, raw)
            mapping.source_id = source_id
            mapping.revision = hashlib.sha256(raw).hexdigest()
            touch_project(self.project_dir)
            return target

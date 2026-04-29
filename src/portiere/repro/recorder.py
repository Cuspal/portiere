"""Stateful capture of pipeline-run state into a manifest.

The recorder is created at the start of a Portiere run and updated as
the pipeline progresses (embedding chosen, vocabularies loaded, stages
executed). At ``finalize()`` it writes ``manifest.lock.json`` to the
run directory.

Credentials are redacted at the boundary: the recorder accepts a raw
connection string but stores only its credential-stripped form, so a
later ``model_dump()`` cannot leak secrets even if the calling code
mishandles the recorder.
"""

from __future__ import annotations

import json
import platform
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import structlog

import portiere
from portiere.repro.hashing import sha256_file, sha256_text
from portiere.repro.manifest import (
    EmbeddingFingerprint,
    KnowledgeBackendFingerprint,
    Manifest,
    PromptTemplateFingerprint,
    RunInfo,
    SourceDataFingerprint,
    StageEntry,
    VocabularyFingerprint,
)

logger = structlog.get_logger(__name__)


# ── Helpers ───────────────────────────────────────────────────────


def _git_state(repo_dir: Path) -> tuple[str | None, bool | None]:
    """Return ``(git_sha, git_dirty)`` for ``repo_dir``.

    Returns ``(None, None)`` when:

    * ``repo_dir`` is not a git repo
    * git is not installed on PATH
    * any git invocation errors

    A dirty tree is recorded but never blocks the run (per Slice 4
    decision 2: warn, never block).
    """
    try:
        sha = (
            subprocess.check_output(
                ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
        status = (
            subprocess.check_output(
                ["git", "-C", str(repo_dir), "status", "--porcelain"],
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
        return sha, bool(status)
    except (subprocess.CalledProcessError, FileNotFoundError, NotADirectoryError):
        return None, None


# Match `://<credentials>@` greedy-backtracking to the last `@` before any `/`.
# Handles `@` characters within the password (e.g., user:p@ssword@host).
_REDACT_CRED_RE = re.compile(r"(://)([^/]*)(@)")


def _redact_connection_string(conn: str | None) -> str | None:
    """Replace credentials in a connection URI with ``***``.

    ``postgresql://user:pass@db.example.com/clinical`` becomes
    ``postgresql://***@db.example.com/clinical``. Hosts and database
    names are preserved (they're operationally useful for audits and
    not credentials themselves).
    """
    if conn is None:
        return None
    return _REDACT_CRED_RE.sub(r"\1***\3", conn)


# ── Recorder ──────────────────────────────────────────────────────


class ManifestRecorder:
    """Build up a :class:`Manifest` over the lifetime of a pipeline run.

    The recorder owns its target run directory and a mutable in-progress
    manifest. Each setter mutates the manifest; :meth:`finalize` writes
    it to disk and returns the path.
    """

    def __init__(
        self,
        run_dir: Path,
        *,
        project_name: str,
        target_model: str,
        task: str = "standardize",
        source_standard: str | None = None,
        vocabularies_requested: list[str] | None = None,
        project_root: Path | None = None,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.project_root = project_root or Path.cwd()
        self._stages: list[StageEntry] = []
        self._start = time.time()
        self.run_id = uuid.uuid4().hex[:12]

        git_sha, git_dirty = _git_state(self.project_root)
        if git_dirty:
            logger.warning(
                "manifest.git_dirty",
                project_root=str(self.project_root),
                hint="Working tree has uncommitted changes; manifest records git_dirty=true",
            )

        self._manifest = Manifest(
            run=RunInfo(
                run_id=self.run_id,
                started_at=datetime.now(timezone.utc).isoformat(),
            ),
            portiere_version=portiere.__version__,
            python_version=sys.version.split()[0],
            os_string=platform.platform(),
            git_sha=git_sha,
            git_dirty=git_dirty,
            project_name=project_name,
            target_model=target_model,
            task=task,
            source_standard=source_standard,
            vocabularies_requested=list(vocabularies_requested or []),
            embedding=EmbeddingFingerprint(name="unknown", dimension=0),  # filled later
        )

    # ── Setters ─────────────────────────────────────────────────────

    def set_embedding(
        self,
        name: str,
        *,
        dimension: int,
        hf_revision: str | None = None,
        sha256_of_config: str | None = None,
    ) -> None:
        """Record the embedding model identity (name + revision + dimension)."""
        self._manifest.embedding = EmbeddingFingerprint(
            name=name,
            dimension=dimension,
            hf_revision=hf_revision,
            sha256_of_config=sha256_of_config,
        )

    def set_knowledge_backend(self, type_: str, *, index_hash: str | None = None) -> None:
        """Record the knowledge-layer backend type and optional index hash."""
        self._manifest.knowledge_backend = KnowledgeBackendFingerprint(
            type=type_, index_hash=index_hash
        )

    def add_vocabulary(
        self,
        *,
        name: str,
        version_date: str | None = None,
        path: str | None = None,
    ) -> None:
        """Append a vocabulary fingerprint. ``path`` is hashed when present."""
        sha = sha256_file(path) if path and Path(path).exists() else None
        self._manifest.vocabularies.append(
            VocabularyFingerprint(
                name=name,
                version_date=version_date,
                sha256_of_source_file=sha,
                path=path,
            )
        )

    def add_prompt(self, name: str, template: str) -> None:
        """Record a prompt template by hash. The template TEXT is never stored."""
        self._manifest.prompt_templates.append(
            PromptTemplateFingerprint(name=name, sha256=sha256_text(template))
        )

    def set_thresholds(self, thresholds: dict) -> None:
        """Snapshot the thresholds config used by the run."""
        self._manifest.thresholds = dict(thresholds)

    def set_source_data(
        self,
        *,
        path: str | None = None,
        connection_string: str | None = None,
        table_or_query: str | None = None,
    ) -> None:
        """Record the source data fingerprint.

        For files, ``path`` + sha256 are stored. For databases,
        credentials are stripped before storage — the raw connection
        string never reaches the manifest schema.
        """
        sha = sha256_file(path) if path and Path(path).exists() else None
        self._manifest.source_data = SourceDataFingerprint(
            path=path,
            sha256=sha,
            connection_string_redacted=_redact_connection_string(connection_string),
            table_or_query=table_or_query,
        )

    def record_stage(
        self,
        stage: str,
        *,
        inputs: dict | None = None,
        outputs: dict | None = None,
        metrics: dict | None = None,
    ) -> None:
        """Append a stage entry to the manifest.

        Stages run quickly enough that we record only a single
        timestamp (now) for both ``started_at`` and ``finished_at``;
        per-stage durations are not currently tracked.
        """
        now = datetime.now(timezone.utc).isoformat()
        self._stages.append(
            StageEntry(
                stage=stage,
                started_at=now,
                finished_at=now,
                inputs=inputs or {},
                outputs=outputs or {},
                metrics=metrics or {},
            )
        )

    # ── Finalize ────────────────────────────────────────────────────

    def finalize(self) -> Path:
        """Write the manifest to ``<run_dir>/manifest.lock.json``.

        Returns the path of the written file. Callable more than once;
        subsequent calls overwrite with the latest state (run timestamps
        update each time).
        """
        self._manifest.stages = list(self._stages)
        self._manifest.run.finished_at = datetime.now(timezone.utc).isoformat()
        self._manifest.run.duration_seconds = round(time.time() - self._start, 3)

        out = self.run_dir / "manifest.lock.json"
        with out.open("w") as f:
            json.dump(
                self._manifest.model_dump(mode="json"),
                f,
                indent=2,
                sort_keys=True,
            )
        logger.info(
            "manifest.finalized",
            run_id=self.run_id,
            path=str(out),
            duration_seconds=self._manifest.run.duration_seconds,
            n_stages=len(self._stages),
        )
        return out

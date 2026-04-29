"""Replay a Portiere pipeline run from its manifest.

``replay()`` validates that all referenced artifacts (source data,
vocabulary files) still exist and match their recorded sha256, then
reconstructs the project with the manifest's recorded configuration.
For v0.2.0 this stops at project reconstruction + source re-attach;
the caller can re-invoke pipeline ops as needed. Future versions may
auto-replay the recorded ``stages`` list.

Output bytes are not promised to be identical to the original run —
clinical pipelines have legitimate nondeterminism sources (LLM
sampling, BM25 ties, FAISS thread effects).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import structlog

from portiere.exceptions import PortiereError
from portiere.repro.hashing import sha256_file
from portiere.repro.manifest import Manifest

logger = structlog.get_logger(__name__)


class ManifestReplayError(PortiereError):
    """Raised when a manifest cannot be replayed.

    Common causes: a referenced artifact (source CSV, vocabulary file)
    is missing, or its sha256 has changed since the manifest was written.
    """


def _load_manifest(path: str | Path) -> Manifest:
    p = Path(path)
    return Manifest(**json.loads(p.read_text()))


def _verify_artifacts(m: Manifest) -> None:
    """Raise :class:`ManifestReplayError` if any referenced artifact
    is missing or has changed sha256.
    """
    if m.source_data and m.source_data.path:
        src = Path(m.source_data.path)
        if not src.exists():
            raise ManifestReplayError(f"source data missing: {src}")
        if m.source_data.sha256 and sha256_file(src) != m.source_data.sha256:
            raise ManifestReplayError(
                f"source data sha256 mismatch: {src} (manifest says "
                f"{m.source_data.sha256!r}; current file differs)"
            )

    for v in m.vocabularies:
        if v.path is None:
            continue
        vp = Path(v.path)
        if not vp.exists():
            raise ManifestReplayError(f"vocabulary missing: {vp}")
        recorded = v.sha256_of_source_file
        if recorded and not recorded.startswith("meta:"):
            actual = sha256_file(vp)
            if actual != recorded:
                raise ManifestReplayError(
                    f"vocabulary sha256 mismatch: {vp} (manifest says "
                    f"{recorded!r}; current file differs)"
                )


def replay(
    manifest_path: str | Path,
    *,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Verify artifacts and reconstruct the project from a manifest.

    Args:
        manifest_path: Path to a ``manifest.lock.json``.
        output_dir: Optional output directory hint. The replayed run
            is recorded as a fresh manifest in the same project's
            ``runs/<replay_run_id>/`` directory; ``output_dir`` is
            included in the result for downstream tooling.

    Returns:
        dict with: ``manifest_path``, ``project_name``, ``target_model``,
        ``replay_run_id``, ``output_dir`` (str | None),
        ``source_path`` (the re-attached source path, if any).

    Raises:
        ManifestReplayError: A referenced artifact is missing or its
            sha256 has changed since the manifest was written.
    """
    manifest_path = Path(manifest_path)
    m = _load_manifest(manifest_path)
    _verify_artifacts(m)

    # Reconstruct the project under a *replay* name to avoid clobbering
    # the original project's storage. Caller-supplied output_dir is
    # surfaced unchanged in the result.
    import portiere

    replay_name = f"replay-{m.project_name}-{m.run.run_id}"
    project = portiere.init(
        name=replay_name,
        target_model=m.target_model,
        task=m.task,
        source_standard=m.source_standard,
        vocabularies=m.vocabularies_requested or None,
    )

    source_path: str | None = None
    if m.source_data and m.source_data.path and Path(m.source_data.path).exists():
        try:
            source = project.add_source(m.source_data.path)
            source_path = source.get("path")
        except Exception as exc:
            logger.warning("replay.add_source_failed", error=str(exc))

    logger.info(
        "replay.reconstructed",
        manifest=str(manifest_path),
        original_run_id=m.run.run_id,
        replay_project=replay_name,
    )

    return {
        "manifest_path": str(manifest_path),
        "project_name": m.project_name,
        "target_model": m.target_model,
        "replay_run_id": getattr(project._recorder, "run_id", None)
        if project._recorder is not None
        else None,
        "output_dir": str(output_dir) if output_dir is not None else None,
        "source_path": source_path,
    }

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
from pydantic import BaseModel, computed_field

from portiere.exceptions import PortiereError
from portiere.repro.hashing import sha256_file
from portiere.repro.manifest import Manifest
from portiere.repro.replay_comparator import COMPARATORS, StageReplayResult

logger = structlog.get_logger(__name__)


class ReplayReport(BaseModel):
    """Aggregated per-stage outcome from ``auto_replay()``.

    ``passed`` is True iff every stage that was attempted (passed is not
    None) succeeded. Stages with ``passed=None`` (e.g., dependency
    unavailable) do NOT fail the overall report.
    """

    manifest_path: str
    per_stage: list[StageReplayResult] = []

    @computed_field  # type: ignore[prop-decorator]
    @property
    def passed(self) -> bool:
        return all(s.passed is not False for s in self.per_stage)


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


def _replay_stage(project: Any, stage_entry: dict, source_path: str | None) -> dict | None:
    """Best-effort re-invocation of a single stage on the reconstructed project.

    Returns the replayed output dict, or ``None`` if the stage cannot be
    replayed in the current environment (LLM missing, output dir gone,
    knowledge layer not configured, etc.). Callers treat ``None`` as
    ``passed=None`` (unavailable), not a failure.
    """
    stage = stage_entry.get("stage")

    if stage == "ingest":
        # Source-data sha256 has already been verified by replay(); for the
        # comparator we just surface the format the file currently has.
        if source_path:
            fmt = Path(source_path).suffix.lstrip(".") or None
            return {"format": fmt}
        return None

    if stage == "validate":
        # Re-run validate if the recorded output_path still exists. Comparing
        # the binary all_passed flag is cheap and gives a strong reproducibility
        # signal when the data is still on disk.
        recorded_path = stage_entry.get("inputs", {}).get("output_path")
        if not recorded_path or not Path(recorded_path).exists():
            return None
        try:
            from portiere.engines.polars_engine import PolarsEngine

            report = project.validate(engine=PolarsEngine(), output_path=recorded_path)
            return {
                "total_tables": report.get("total_tables", 0),
                "all_passed": bool(report.get("all_passed", False)),
            }
        except Exception as exc:
            logger.debug("auto_replay.validate_unavailable", error=str(exc))
            return None

    # Stages that need an LLM + knowledge layer (schema, concept) and ETL
    # require the same engine + maps that the original run had. Fully
    # replaying them is out of v0.3.1 scope; record as unavailable.
    if stage in ("schema", "concept", "etl", "profile"):
        return None

    return None


def auto_replay(manifest_path: str | Path) -> ReplayReport:
    """Verify artifacts, reconstruct the project, and re-run each recorded stage.

    For each stage in ``manifest.stages``, attempts a best-effort re-execution
    and compares the replayed output to the recorded output via per-stage
    comparators. Stops at the first stage that fails comparison (fail-fast).
    Stages whose dependencies are unavailable in the current environment
    record ``passed=None`` and do not fail the overall report.

    Args:
        manifest_path: Path to a ``manifest.lock.json``.

    Returns:
        :class:`ReplayReport` with per-stage results and an aggregate
        ``passed`` flag.

    Raises:
        ManifestReplayError: A referenced artifact is missing or its
            sha256 has changed (same conditions as :func:`replay`).
    """
    manifest_path = Path(manifest_path)
    m = _load_manifest(manifest_path)
    _verify_artifacts(m)

    # Reconstruct the project (re-using replay()'s logic is overkill since we
    # need the project object back; build it inline).
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
            logger.warning("auto_replay.add_source_failed", error=str(exc))

    per_stage: list[StageReplayResult] = []
    for stage_entry in m.model_dump()["stages"]:
        stage_name = stage_entry.get("stage", "")
        comparator = COMPARATORS.get(stage_name)
        if comparator is None:
            # Unrecognized stage name (e.g., "profile"): record as unavailable.
            per_stage.append(
                StageReplayResult(
                    stage=stage_name,
                    passed=None,
                    reason=f"no comparator registered for stage={stage_name!r}",
                )
            )
            continue

        replayed = _replay_stage(project, stage_entry, source_path)
        if replayed is None:
            per_stage.append(
                StageReplayResult(
                    stage=stage_name,
                    passed=None,
                    reason="stage replay unavailable in this environment",
                )
            )
            continue

        result = comparator(stage_entry, replayed)
        per_stage.append(result)
        if result.passed is False:
            # Fail-fast: later stages depend on earlier outputs.
            break

    logger.info(
        "auto_replay.complete",
        manifest=str(manifest_path),
        n_stages=len(per_stage),
        passed=all(s.passed is not False for s in per_stage),
    )

    return ReplayReport(manifest_path=str(manifest_path), per_stage=per_stage)

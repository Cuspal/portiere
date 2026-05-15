"""Per-stage output comparators for ``portiere replay --auto-replay``.

Each comparator takes a recorded ``StageEntry`` dict (as serialized in
``manifest.lock.json``) and the replayed stage's output dict, and returns
a :class:`StageReplayResult`. Comparators are pure — they don't touch the
filesystem or execute any pipeline code.

Tolerance bands (locked in the v0.3.1 implementation plan):

* ``ingest``: byte-identical (source data hash is verified upstream by
  ``replay()`` before any comparator runs; comparator only checks the
  recorded ``format`` matches).
* ``schema``: identical mapping decisions (``n_mappings`` equal).
* ``concept``: ±1% drift on ``auto_rate``; ``n_mappings`` must not drop.
* ``etl``: identical row count + column set (paths can differ).
* ``validate``: ``all_passed`` flag identical (binary).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class StageReplayResult(BaseModel):
    """Per-stage outcome from auto-replay.

    Three states:

    * ``passed=True`` — replayed output matches recorded within tolerance.
    * ``passed=False`` — drift exceeds tolerance; the report's overall
      ``passed`` becomes False.
    * ``passed=None`` — stage could not be replayed (e.g., LLM not
      configured); does NOT fail the overall report.
    """

    stage: str
    passed: bool | None = None
    drift_pct: float | None = None
    reason: str = ""


def _outputs(recorded: dict) -> dict:
    return recorded.get("outputs", {}) or {}


def _metrics(recorded: dict) -> dict:
    return recorded.get("metrics", {}) or {}


def _inputs(recorded: dict) -> dict:
    return recorded.get("inputs", {}) or {}


def compare_ingest(recorded: dict, replayed: dict) -> StageReplayResult:
    """Ingest is byte-verified upstream — only the recorded format must match."""
    recorded_fmt = _inputs(recorded).get("format")
    replayed_fmt = replayed.get("format")
    if recorded_fmt is None and replayed_fmt is None:
        return StageReplayResult(stage="ingest", passed=True)
    if recorded_fmt == replayed_fmt:
        return StageReplayResult(stage="ingest", passed=True)
    return StageReplayResult(
        stage="ingest",
        passed=False,
        reason=f"format mismatch: recorded={recorded_fmt!r}, replayed={replayed_fmt!r}",
    )


def compare_schema(recorded: dict, replayed: dict) -> StageReplayResult:
    """Schema mapping decisions must be identical."""
    recorded_n = _outputs(recorded).get("n_mappings")
    replayed_n = replayed.get("n_mappings")
    if recorded_n is None or replayed_n is None:
        return StageReplayResult(
            stage="schema",
            passed=None,
            reason="n_mappings missing from recorded or replayed output",
        )
    if recorded_n == replayed_n:
        return StageReplayResult(stage="schema", passed=True)
    return StageReplayResult(
        stage="schema",
        passed=False,
        reason=f"n_mappings drift: recorded={recorded_n}, replayed={replayed_n}",
    )


def compare_concept(recorded: dict, replayed: dict) -> StageReplayResult:
    """Concept mapping accepts ±1% drift on auto_rate; n_mappings must not drop."""
    recorded_n = _outputs(recorded).get("n_mappings")
    replayed_n = replayed.get("n_mappings")
    if recorded_n is not None and replayed_n is not None and replayed_n < recorded_n:
        return StageReplayResult(
            stage="concept",
            passed=False,
            reason=f"n_mappings dropped: recorded={recorded_n}, replayed={replayed_n}",
        )

    recorded_rate = _metrics(recorded).get("auto_rate")
    replayed_rate = replayed.get("auto_rate")
    if recorded_rate is None or replayed_rate is None:
        return StageReplayResult(stage="concept", passed=True, drift_pct=0.0)

    drift = abs(replayed_rate - recorded_rate) * 100.0
    passed = drift <= 1.0
    return StageReplayResult(
        stage="concept",
        passed=passed,
        drift_pct=drift,
        reason="" if passed else f"auto_rate drift {drift:.2f}% > 1%",
    )


def compare_etl(recorded: dict, replayed: dict) -> StageReplayResult:
    """ETL: row count + column set identical; paths can differ."""
    rec_rows: Any = _outputs(recorded).get("row_count")
    rep_rows: Any = replayed.get("row_count")
    if rec_rows is not None and rep_rows is not None and rec_rows != rep_rows:
        return StageReplayResult(
            stage="etl",
            passed=False,
            reason=f"row_count drift: recorded={rec_rows}, replayed={rep_rows}",
        )
    rec_cols = _outputs(recorded).get("columns")
    rep_cols = replayed.get("columns")
    if rec_cols is not None and rep_cols is not None and set(rec_cols) != set(rep_cols):
        return StageReplayResult(
            stage="etl",
            passed=False,
            reason=f"column set mismatch: recorded={sorted(rec_cols)}, replayed={sorted(rep_cols)}",
        )
    return StageReplayResult(stage="etl", passed=True)


def compare_validate(recorded: dict, replayed: dict) -> StageReplayResult:
    """Validate: all_passed must match (binary)."""
    recorded_pass = _metrics(recorded).get("all_passed")
    replayed_pass = replayed.get("all_passed")
    if recorded_pass is None or replayed_pass is None:
        return StageReplayResult(
            stage="validate",
            passed=None,
            reason="all_passed missing from recorded or replayed output",
        )
    if bool(recorded_pass) == bool(replayed_pass):
        return StageReplayResult(stage="validate", passed=True)
    return StageReplayResult(
        stage="validate",
        passed=False,
        reason=f"all_passed flipped: recorded={recorded_pass}, replayed={replayed_pass}",
    )


# Stage name -> comparator. The recorder uses these stage names verbatim
# (see project.py record_stage() calls): "ingest", "profile", "schema",
# "concept", "etl", "validate". "profile" is internal-only and skipped.
COMPARATORS = {
    "ingest": compare_ingest,
    "schema": compare_schema,
    "concept": compare_concept,
    "etl": compare_etl,
    "validate": compare_validate,
}

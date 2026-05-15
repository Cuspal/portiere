"""USAGI baseline backend — subprocess wrapper around OHDSI's USAGI JAR.

USAGI (https://github.com/OHDSI/Usagi) is the OHDSI community's official
mapping tool: TF-IDF over Athena concept names, manual review UI for
ambiguous matches. It is the canonical baseline for any ML-driven
concept-mapping system.

This module provides three plumbing pieces — input materialization, output
parsing, and a subprocess driver — that together let the benchmark runner
route ``--backend usagi`` through USAGI instead of Portiere's own
knowledge layer.

USAGI itself is NOT bundled. The wrapper expects:

* ``java`` on the user's ``$PATH`` (Java 17+).
* A USAGI JAR pinned via ``USAGI_JAR`` env var or the ``usagi_jar``
  parameter.

The CI workflow ``.github/workflows/usagi-baseline.yml`` runs only on tag
pushes and downloads a SHA-pinned JAR. Default CI excludes USAGI tests via
the ``@pytest.mark.slow`` marker.
"""

from __future__ import annotations

import csv
import shutil
import subprocess
from collections.abc import Iterable
from pathlib import Path

from portiere.exceptions import PortiereError


class UsagiUnavailableError(PortiereError):
    """Raised when Java or the USAGI JAR is not available in the environment."""


# ── Input materialization ─────────────────────────────────────────


def write_usagi_input_csv(rows: Iterable[dict], out_path: Path) -> None:
    """Write a USAGI-compatible input CSV.

    USAGI expects at minimum a ``source_code`` column and a ``source_name``
    column (human-readable label). Additional metadata (frequency, etc.)
    can be passed via the ``count`` column but is not required.

    Args:
        rows: Iterable of dicts with keys ``concept_id``, ``concept_code``,
            ``concept_name`` (the Athena CONCEPT.csv shape).
        out_path: Where to write the CSV (tab-delimited, USAGI's default).
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["source_code", "source_name", "source_concept_id"]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "source_code": row["concept_code"],
                    "source_name": row["concept_name"],
                    "source_concept_id": row["concept_id"],
                }
            )


# ── Output parsing ────────────────────────────────────────────────


def parse_usagi_output(
    output_csv: Path,
    code_to_concept: dict[str, int],
) -> dict[int, list[int]]:
    """Parse USAGI's batch-export CSV into the runner's predictions shape.

    Args:
        output_csv: USAGI's exported mappings (tab-delimited). Expected
            columns: ``source_code``, ``target_concept_id``, ``match_score``.
        code_to_concept: Map from ``source_code`` (e.g. "E11.9") to the
            original ICD concept_id (e.g. 100). Rows whose source_code is
            not in this map are silently dropped.

    Returns:
        ``{source_concept_id: [target_concept_id, ...]}`` ordered by USAGI's
        own match_score descending.
    """
    rows_by_source: dict[str, list[tuple[float, int]]] = {}
    with output_csv.open(encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            src = row.get("source_code") or ""
            try:
                tgt = int(row.get("target_concept_id") or 0)
                score = float(row.get("match_score") or 0.0)
            except (ValueError, TypeError):
                continue
            rows_by_source.setdefault(src, []).append((score, tgt))

    predictions: dict[int, list[int]] = {}
    for src, scored in rows_by_source.items():
        cid = code_to_concept.get(src)
        if cid is None:
            continue
        scored.sort(reverse=True, key=lambda x: x[0])
        predictions[cid] = [tgt for _score, tgt in scored]
    return predictions


# ── Subprocess driver ─────────────────────────────────────────────


def run_usagi(
    *,
    input_rows: list[dict],
    athena_concept_csv: Path,
    usagi_jar: Path,
    work_dir: Path | None = None,
    java_bin: str | None = None,
) -> dict[int, list[int]]:
    """Run USAGI in batch mode and return predictions in the runner's shape.

    Raises:
        UsagiUnavailableError: ``java`` not on ``$PATH`` or ``usagi_jar``
            does not exist. The benchmark runner catches this and records
            the backend as unavailable.
    """
    java = java_bin or shutil.which("java")
    if java is None:
        raise UsagiUnavailableError("Java 17+ not found on PATH")
    if not usagi_jar.exists():
        raise UsagiUnavailableError(f"USAGI JAR missing: {usagi_jar}")

    work = work_dir or Path("/tmp/usagi_run")
    work.mkdir(parents=True, exist_ok=True)
    input_csv = work / "input.csv"
    output_csv = work / "output.csv"

    write_usagi_input_csv(input_rows, input_csv)

    # USAGI batch CLI invocation. The exact arg layout depends on the JAR
    # build; this is the standard "batch" mode pattern documented in
    # https://github.com/OHDSI/Usagi/wiki/Batch-Mode .
    cmd = [
        java,
        "-jar",
        str(usagi_jar),
        "--batch",
        "--input",
        str(input_csv),
        "--vocabulary",
        str(athena_concept_csv),
        "--output",
        str(output_csv),
    ]
    subprocess.run(cmd, check=True, capture_output=True)

    code_to_concept: dict[str, int] = {
        str(row["concept_code"]): int(row["concept_id"]) for row in input_rows
    }
    return parse_usagi_output(output_csv, code_to_concept)

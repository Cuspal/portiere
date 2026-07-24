"""Tests for the USAGI baseline backend (Slice 3, v0.3.1).

USAGI is OHDSI's official Java-based mapping tool. We wrap it as a
subprocess so it can serve as a baseline row in the ICD→SNOMED benchmark.

All tests requiring an actual JAR/JVM are marked ``@pytest.mark.slow`` and
are excluded from CI by default. The non-slow tests verify the wrapper's
input/output plumbing using pure-Python fakes.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

# ── Input materialization ──────────────────────────────────────────


class TestUsagiInputCsv:
    def test_writes_required_columns(self, tmp_path):
        from portiere.benchmarks.athena_icd_snomed.usagi_baseline import (
            write_usagi_input_csv,
        )

        rows = [
            {"concept_id": 100, "concept_code": "E11.9", "concept_name": "Type 2 diabetes"},
            {"concept_id": 101, "concept_code": "I10", "concept_name": "Hypertension"},
        ]
        out = tmp_path / "usagi_input.csv"
        write_usagi_input_csv(rows, out)
        assert out.exists()

        text = out.read_text()
        # USAGI input expects at minimum a source_code and source_name column.
        assert "source_code" in text
        assert "source_name" in text
        assert "E11.9" in text
        assert "Hypertension" in text

    def test_empty_rows_writes_header_only(self, tmp_path):
        from portiere.benchmarks.athena_icd_snomed.usagi_baseline import (
            write_usagi_input_csv,
        )

        out = tmp_path / "usagi_input.csv"
        write_usagi_input_csv([], out)
        lines = out.read_text().strip().splitlines()
        assert len(lines) == 1  # header only


# ── Output parsing ────────────────────────────────────────────────


class TestUsagiOutputParse:
    def test_parses_usagi_match_output(self, tmp_path):
        from portiere.benchmarks.athena_icd_snomed.usagi_baseline import (
            parse_usagi_output,
        )

        # Synthetic USAGI output: per-source-code ranked candidates.
        # Columns mirror USAGI's "Approved Mappings" export.
        output_csv = tmp_path / "usagi_output.csv"
        output_csv.write_text(
            "source_code\ttarget_concept_id\tmatch_score\n"
            "E11.9\t200\t0.95\n"
            "E11.9\t201\t0.80\n"
            "I10\t250\t0.99\n"
        )
        # Map back from source_code -> source concept_id
        code_to_concept = {"E11.9": 100, "I10": 101}
        predictions = parse_usagi_output(output_csv, code_to_concept)
        assert predictions == {100: [200, 201], 101: [250]}

    def test_missing_concept_id_is_skipped(self, tmp_path):
        from portiere.benchmarks.athena_icd_snomed.usagi_baseline import (
            parse_usagi_output,
        )

        output_csv = tmp_path / "usagi_output.csv"
        output_csv.write_text(
            "source_code\ttarget_concept_id\tmatch_score\nUNKNOWN\t999\t0.5\nE11.9\t200\t0.9\n"
        )
        predictions = parse_usagi_output(output_csv, {"E11.9": 100})
        assert predictions == {100: [200]}
        assert 999 not in predictions  # UNKNOWN had no concept_id mapping


# ── Subprocess invocation (slow) ──────────────────────────────────


@pytest.mark.slow
class TestUsagiSubprocess:
    def test_run_usagi_skips_when_java_missing(self, tmp_path):
        if shutil.which("java") is not None and os.environ.get("USAGI_JAR"):
            pytest.skip("Java + USAGI_JAR available — not a missing-deps test")
        from portiere.benchmarks.athena_icd_snomed.usagi_baseline import (
            UsagiUnavailableError,
            run_usagi,
        )

        with pytest.raises(UsagiUnavailableError):
            run_usagi(
                input_rows=[],
                athena_concept_csv=tmp_path / "CONCEPT.csv",
                usagi_jar=Path("/nonexistent/usagi.jar"),
            )

    def test_run_usagi_batch_unsupported_raises_unavailable(self, tmp_path, monkeypatch):
        """No published USAGI JAR (<= v1.4.3) supports headless batch mode —
        the JAR opens the review UI and exits non-zero when headless. The
        wrapper must surface that as the designed UsagiUnavailableError (with
        a pointer to the UI-export bridge), never as a raw
        CalledProcessError. The JAR is deliberately NOT spawned here: on a
        machine with a display it would open a UI window."""
        import subprocess as _subprocess

        from portiere.benchmarks.athena_icd_snomed import usagi_baseline as ub

        jar = tmp_path / "usagi.jar"
        jar.write_bytes(b"not a real jar")

        def _fail(cmd, check, capture_output):
            raise _subprocess.CalledProcessError(1, cmd)

        monkeypatch.setattr(ub.subprocess, "run", _fail)
        monkeypatch.setattr(ub.shutil, "which", lambda _name: "/usr/bin/java")

        with pytest.raises(ub.UsagiUnavailableError, match="usagi_ui_bridge"):
            ub.run_usagi(
                input_rows=[
                    {"concept_id": 100, "concept_code": "E11.9", "concept_name": "Type 2 diabetes"},
                ],
                athena_concept_csv=tmp_path / "CONCEPT.csv",
                usagi_jar=jar,
                work_dir=tmp_path / "work",
            )

    def test_ui_bridge_fixture_roundtrip(self, tmp_path):
        """The UI-export bridge is the supported USAGI scoring path. Verify
        its plumbing against the committed gold input fixture (the exact
        seed=42 set the published USAGI row was scored on): the fixture is
        well-formed, and a synthetic UI export scores through
        parse_usagi_output correctly."""
        import csv

        from portiere.benchmarks.athena_icd_snomed.usagi_baseline import (
            parse_usagi_output,
        )

        fixture = Path(__file__).parent / "fixtures" / "usagi_input_gold1000.csv"
        with fixture.open() as f:
            rows = list(csv.DictReader(f, delimiter="\t"))
        assert len(rows) == 1000
        assert {"source_code", "source_name", "source_concept_id"} <= set(rows[0])

        # Simulate a UI export for the first two codes and score it.
        code_to_concept = {r["source_code"]: int(r["source_concept_id"]) for r in rows[:2]}
        export = tmp_path / "usagi_export.csv"
        export.write_text(
            "source_code\ttarget_concept_id\tmatch_score\n"
            f"{rows[0]['source_code']}\t44054006\t0.91\n"
            f"{rows[1]['source_code']}\t123456\t0.42\n"
        )
        predictions = parse_usagi_output(export, code_to_concept)
        assert predictions[int(rows[0]["source_concept_id"])] == [44054006]
        assert predictions[int(rows[1]["source_concept_id"])] == [123456]


# ── Runner + CLI dispatch ─────────────────────────────────────────


class TestRunnerUsagiDispatch:
    def test_runner_accepts_usagi_backend(self, tmp_path, monkeypatch):
        """run_benchmark(backend='usagi', ...) routes through usagi_baseline."""
        from portiere.benchmarks.athena_icd_snomed import runner as bench_runner
        from portiere.benchmarks.athena_icd_snomed.runner import (
            BenchmarkResult,
            run_benchmark,
        )

        # Stub run_usagi to return canned predictions without invoking Java.
        def _stub_run_usagi(*, input_rows, athena_concept_csv, usagi_jar, **kw):
            return {row["concept_id"]: [row["concept_id"] + 100] for row in input_rows}

        monkeypatch.setattr(bench_runner, "_run_usagi_or_raise", _stub_run_usagi)

        # Build a synthetic Athena fixture (5 ICD codes mapped to 5 SNOMED).
        import pandas as pd

        athena = tmp_path / "athena"
        athena.mkdir()
        concept_rows = []
        for i, code in enumerate(["E11.9", "I10", "J45.909", "E66.9", "F32.A"]):
            concept_rows.append(
                {
                    "concept_id": 100 + i,
                    "concept_name": f"icd-{i}",
                    "domain_id": "Condition",
                    "vocabulary_id": "ICD10CM",
                    "concept_class_id": "x",
                    "standard_concept": "",
                    "concept_code": code,
                    "valid_start_date": "1970-01-01",
                    "valid_end_date": "2099-12-31",
                    "invalid_reason": "",
                }
            )
        for i in range(5):
            concept_rows.append(
                {
                    "concept_id": 200 + i,
                    "concept_name": f"snomed-{i}",
                    "domain_id": "Condition",
                    "vocabulary_id": "SNOMED",
                    "concept_class_id": "Clinical Finding",
                    "standard_concept": "S",
                    "concept_code": f"sno-{i}",
                    "valid_start_date": "1970-01-01",
                    "valid_end_date": "2099-12-31",
                    "invalid_reason": "",
                }
            )
        pd.DataFrame(concept_rows).to_csv(athena / "CONCEPT.csv", sep="\t", index=False)
        cr_rows = [
            {
                "concept_id_1": 100 + i,
                "concept_id_2": 200 + i,
                "relationship_id": "Maps to",
                "valid_start_date": "1970-01-01",
                "valid_end_date": "2099-12-31",
                "invalid_reason": "",
            }
            for i in range(5)
        ]
        pd.DataFrame(cr_rows).to_csv(athena / "CONCEPT_RELATIONSHIP.csv", sep="\t", index=False)

        result = run_benchmark(athena, backend="usagi")
        assert isinstance(result, BenchmarkResult)
        assert result.n >= 0

    def test_cli_accepts_usagi_backend(self):
        from click.testing import CliRunner

        from portiere.cli import cli

        # --help should mention usagi as a valid backend choice
        result = CliRunner().invoke(cli, ["benchmark", "athena-icd-snomed", "--help"])
        assert result.exit_code == 0
        assert "usagi" in result.output

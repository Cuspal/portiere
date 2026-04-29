"""Tests for the ICD→SNOMED benchmark runner (Slice 6).

The runner is exercised against a synthetic Athena-shaped fixture so
the test suite stays self-contained — running on the user's real
Athena export is a separate, manual ``portiere benchmark`` invocation.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

# ── Synthetic Athena fixture ─────────────────────────────────────


def _make_synthetic_athena(tmp_path: Path) -> Path:
    """Build a tiny tab-delimited Athena-shape directory."""
    base = tmp_path / "athena"
    base.mkdir()

    # 5 ICD-10-CM source concepts + 5 SNOMED standard concepts
    concept_rows = [
        # ICD-10-CM (non-standard, source concepts)
        {
            "concept_id": 100,
            "concept_name": "Type 2 diabetes mellitus",
            "domain_id": "Condition",
            "vocabulary_id": "ICD10CM",
            "concept_class_id": "4-char billing code",
            "standard_concept": "",
            "concept_code": "E11.9",
        },
        {
            "concept_id": 101,
            "concept_name": "Essential hypertension",
            "domain_id": "Condition",
            "vocabulary_id": "ICD10CM",
            "concept_class_id": "3-char nonbill code",
            "standard_concept": "",
            "concept_code": "I10",
        },
        {
            "concept_id": 102,
            "concept_name": "Asthma, uncomplicated",
            "domain_id": "Condition",
            "vocabulary_id": "ICD10CM",
            "concept_class_id": "6-char billing code",
            "standard_concept": "",
            "concept_code": "J45.909",
        },
        {
            "concept_id": 103,
            "concept_name": "Obesity, unspecified",
            "domain_id": "Condition",
            "vocabulary_id": "ICD10CM",
            "concept_class_id": "4-char billing code",
            "standard_concept": "",
            "concept_code": "E66.9",
        },
        {
            "concept_id": 104,
            "concept_name": "Major depression",
            "domain_id": "Condition",
            "vocabulary_id": "ICD10CM",
            "concept_class_id": "4-char billing code",
            "standard_concept": "",
            "concept_code": "F32.A",
        },
        # SNOMED (standard concepts)
        {
            "concept_id": 200,
            "concept_name": "Type 2 diabetes mellitus",
            "domain_id": "Condition",
            "vocabulary_id": "SNOMED",
            "concept_class_id": "Clinical Finding",
            "standard_concept": "S",
            "concept_code": "44054006",
        },
        {
            "concept_id": 201,
            "concept_name": "Essential hypertension",
            "domain_id": "Condition",
            "vocabulary_id": "SNOMED",
            "concept_class_id": "Clinical Finding",
            "standard_concept": "S",
            "concept_code": "59621000",
        },
        {
            "concept_id": 202,
            "concept_name": "Asthma",
            "domain_id": "Condition",
            "vocabulary_id": "SNOMED",
            "concept_class_id": "Clinical Finding",
            "standard_concept": "S",
            "concept_code": "195967001",
        },
        {
            "concept_id": 203,
            "concept_name": "Obesity",
            "domain_id": "Condition",
            "vocabulary_id": "SNOMED",
            "concept_class_id": "Clinical Finding",
            "standard_concept": "S",
            "concept_code": "414916001",
        },
        {
            "concept_id": 204,
            "concept_name": "Major depressive disorder",
            "domain_id": "Condition",
            "vocabulary_id": "SNOMED",
            "concept_class_id": "Clinical Finding",
            "standard_concept": "S",
            "concept_code": "370143000",
        },
    ]
    for row in concept_rows:
        row.setdefault("valid_start_date", "1970-01-01")
        row.setdefault("valid_end_date", "2099-12-31")
        row.setdefault("invalid_reason", "")

    pd.DataFrame(concept_rows).to_csv(base / "CONCEPT.csv", sep="\t", index=False)

    # Maps-to: each ICD code → its SNOMED counterpart
    cr_rows = [
        {"concept_id_1": 100, "concept_id_2": 200, "relationship_id": "Maps to"},
        {"concept_id_1": 101, "concept_id_2": 201, "relationship_id": "Maps to"},
        {"concept_id_1": 102, "concept_id_2": 202, "relationship_id": "Maps to"},
        {"concept_id_1": 103, "concept_id_2": 203, "relationship_id": "Maps to"},
        {"concept_id_1": 104, "concept_id_2": 204, "relationship_id": "Maps to"},
    ]
    for row in cr_rows:
        row.update(
            {"valid_start_date": "1970-01-01", "valid_end_date": "2099-12-31", "invalid_reason": ""}
        )
    pd.DataFrame(cr_rows).to_csv(base / "CONCEPT_RELATIONSHIP.csv", sep="\t", index=False)

    # Test set: hold out 3 of the 5 ICD codes
    test_set = pd.DataFrame({"icd10cm_concept_id": [100, 101, 102]})
    test_set_path = tmp_path / "gold_test_set.csv"
    test_set.to_csv(test_set_path, index=False)

    return base


# ── compute_metrics() ─────────────────────────────────────────────


class TestComputeMetrics:
    def test_perfect_top1(self):
        from benchmarks.athena_icd_snomed.runner import compute_metrics

        # Every prediction puts the gold answer at rank 0
        predictions = {
            100: [200, 999, 998],
            101: [201, 999, 998],
            102: [202, 999, 998],
        }
        gold = {100: {200}, 101: {201}, 102: {202}}
        m = compute_metrics(predictions, gold)
        assert m.top_1 == 1.0
        assert m.top_5 == 1.0
        assert m.mrr == 1.0

    def test_top5_better_than_top1(self):
        from benchmarks.athena_icd_snomed.runner import compute_metrics

        # Gold is at rank 2 for all → top_1 = 0, top_5 = 1.0
        predictions = {
            100: [999, 998, 200, 997, 996],
            101: [999, 998, 201, 997, 996],
        }
        gold = {100: {200}, 101: {201}}
        m = compute_metrics(predictions, gold)
        assert m.top_1 == 0.0
        assert m.top_5 == 1.0
        # MRR = mean(1/3, 1/3) = 1/3
        assert m.mrr == pytest.approx(1 / 3, rel=1e-3)

    def test_missing_gold_excluded(self):
        from benchmarks.athena_icd_snomed.runner import compute_metrics

        # Test concept 999 has no gold mapping → excluded from N
        predictions = {100: [200], 999: [777]}
        gold = {100: {200}}
        m = compute_metrics(predictions, gold)
        assert m.n == 1
        assert m.top_1 == 1.0

    def test_no_predictions_returns_zero(self):
        from benchmarks.athena_icd_snomed.runner import compute_metrics

        m = compute_metrics({}, {100: {200}})
        assert m.n == 0
        assert m.top_1 == 0.0
        assert m.mrr == 0.0


# ── run_benchmark() ───────────────────────────────────────────────


class TestRunBenchmark:
    def test_runs_against_synthetic_athena(self, tmp_path):
        from benchmarks.athena_icd_snomed.runner import run_benchmark

        athena = _make_synthetic_athena(tmp_path)
        test_set = tmp_path / "gold_test_set.csv"
        result = run_benchmark(athena, test_set_path=test_set)
        assert result.n == 3
        # Shape only — metric values depend on Portiere config and
        # the BM25-based fallback retrieval; we don't assert specific
        # numbers here (those go in expected_results.json).
        assert 0.0 <= result.top_1 <= 1.0
        assert 0.0 <= result.top_5 <= 1.0
        assert result.top_5 >= result.top_1
        assert 0.0 <= result.mrr <= 1.0


# ── write_expected_results() ──────────────────────────────────────


class TestWriteExpectedResults:
    def test_round_trip(self, tmp_path):
        from benchmarks.athena_icd_snomed.runner import (
            BenchmarkResult,
            write_expected_results,
        )

        result = BenchmarkResult(n=1000, top_1=0.62, top_5=0.81, top_10=0.85, mrr=0.71)
        out = tmp_path / "expected_results.json"
        write_expected_results(result, athena_release_date="2024-09-01", out=out)
        loaded = json.loads(out.read_text())
        assert loaded["n"] == 1000
        assert loaded["top_1"] == 0.62
        assert loaded["athena_release_date"] == "2024-09-01"


# ── CLI ──────────────────────────────────────────────────────────


class TestBenchmarkCLI:
    def test_command_registered(self):
        from portiere.cli import cli

        assert "benchmark" in cli.commands

    def test_benchmark_command_exists(self):
        from portiere.cli.benchmark import benchmark_group

        assert "athena-icd-snomed" in benchmark_group.commands

    def test_benchmark_runs_via_cli(self, tmp_path):
        from click.testing import CliRunner

        from portiere.cli import cli

        athena = _make_synthetic_athena(tmp_path)
        test_set = tmp_path / "gold_test_set.csv"
        out_json = tmp_path / "bench_run.json"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "benchmark",
                "athena-icd-snomed",
                "--athena-dir",
                str(athena),
                "--test-set",
                str(test_set),
                "--out",
                str(out_json),
            ],
        )
        assert result.exit_code == 0, result.output
        assert out_json.exists()
        loaded = json.loads(out_json.read_text())
        assert loaded["n"] == 3

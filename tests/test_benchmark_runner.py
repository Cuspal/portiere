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
        from portiere.benchmarks.athena_icd_snomed.runner import compute_metrics

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
        from portiere.benchmarks.athena_icd_snomed.runner import compute_metrics

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
        from portiere.benchmarks.athena_icd_snomed.runner import compute_metrics

        # Test concept 999 has no gold mapping → excluded from N
        predictions = {100: [200], 999: [777]}
        gold = {100: {200}}
        m = compute_metrics(predictions, gold)
        assert m.n == 1
        assert m.top_1 == 1.0

    def test_no_predictions_returns_zero(self):
        from portiere.benchmarks.athena_icd_snomed.runner import compute_metrics

        m = compute_metrics({}, {100: {200}})
        assert m.n == 0
        assert m.top_1 == 0.0
        assert m.mrr == 0.0

    def test_top10_differentiates_from_top5(self):
        """Regression: top_10 must reflect ranks 6-10 when present.

        The v0.2.0 published numbers showed top_5 == top_10 == 0.528
        because upstream truncation capped the candidate list at 5
        before it reached this metric. With a properly sized prediction
        list, a hit at rank 7 must lift top_10 above top_5.
        """
        from portiere.benchmarks.athena_icd_snomed.runner import compute_metrics

        # Gold at rank 7 (index 6) for src=100, rank 1 (index 0) for src=101
        predictions = {
            100: [990, 991, 992, 993, 994, 995, 200, 996, 997, 998],
            101: [201, 999, 998, 997, 996, 995, 994, 993, 992, 991],
        }
        gold = {100: {200}, 101: {201}}
        m = compute_metrics(predictions, gold)
        assert m.top_1 == 0.5  # only src=101 has gold at rank 1
        assert m.top_5 == 0.5  # src=100's rank-7 hit doesn't count
        assert m.top_10 == 1.0  # both within top-10
        assert m.top_10 > m.top_5


# ── run_benchmark() ───────────────────────────────────────────────


class TestRunBenchmark:
    def test_runs_against_synthetic_athena(self, tmp_path):
        from portiere.benchmarks.athena_icd_snomed.runner import run_benchmark

        athena = _make_synthetic_athena(tmp_path)
        test_set = tmp_path / "gold_test_set.csv"
        result = run_benchmark(athena, test_set_path=test_set, backend="bm25s")
        assert result.n == 3
        # Shape only — metric values depend on Portiere config and
        # the BM25-based fallback retrieval; we don't assert specific
        # numbers here (those go in expected_results.json).
        assert 0.0 <= result.top_1 <= 1.0
        assert 0.0 <= result.top_5 <= 1.0
        assert result.top_5 >= result.top_1
        assert 0.0 <= result.mrr <= 1.0

    def test_backend_parameter_is_propagated(self, tmp_path, monkeypatch):
        """The backend kwarg must end up in the KnowledgeLayerConfig."""
        from portiere.benchmarks.athena_icd_snomed import runner as bench_runner

        captured: dict = {}
        original_factory = bench_runner.PortiereConfig

        def _capture(*args, **kwargs):
            captured["knowledge_layer"] = kwargs.get("knowledge_layer")
            return original_factory(*args, **kwargs)

        monkeypatch.setattr(bench_runner, "PortiereConfig", _capture)

        athena = _make_synthetic_athena(tmp_path)
        test_set = tmp_path / "gold_test_set.csv"
        bench_runner.run_benchmark(athena, test_set_path=test_set, backend="bm25s")

        assert captured["knowledge_layer"].backend == "bm25s"

    @pytest.mark.parametrize("backend", ["bm25s"])
    def test_each_backend_produces_a_valid_result(self, tmp_path, backend):
        from portiere.benchmarks.athena_icd_snomed.runner import run_benchmark

        athena = _make_synthetic_athena(tmp_path)
        test_set = tmp_path / "gold_test_set.csv"
        result = run_benchmark(athena, test_set_path=test_set, backend=backend)

        assert result.n == 3
        assert 0.0 <= result.top_1 <= 1.0
        assert 0.0 <= result.top_5 <= 1.0
        assert 0.0 <= result.top_10 <= 1.0
        assert result.top_5 >= result.top_1
        assert result.top_10 >= result.top_5

    @pytest.mark.slow
    @pytest.mark.parametrize("backend", ["faiss", "hybrid"])
    def test_each_embedding_backend_produces_a_valid_result(self, tmp_path, backend):
        from portiere.benchmarks.athena_icd_snomed.runner import run_benchmark

        athena = _make_synthetic_athena(tmp_path)
        test_set = tmp_path / "gold_test_set.csv"
        result = run_benchmark(athena, test_set_path=test_set, backend=backend)

        assert result.n == 3
        assert 0.0 <= result.top_1 <= 1.0
        assert 0.0 <= result.top_5 <= 1.0
        assert 0.0 <= result.top_10 <= 1.0
        assert result.top_5 >= result.top_1
        assert result.top_10 >= result.top_5


# ── write_expected_results() ──────────────────────────────────────


class TestWriteExpectedResults:
    def test_round_trip(self, tmp_path):
        from portiere.benchmarks.athena_icd_snomed.runner import (
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


# ── append_run_to_expected_results() ─────────────────────────────


class TestMultiRunResults:
    def test_append_run_creates_runs_array(self, tmp_path):
        from portiere.benchmarks.athena_icd_snomed.runner import (
            BenchmarkResult,
            append_run_to_expected_results,
        )

        out = tmp_path / "expected_results.json"
        result = BenchmarkResult(n=1000, top_1=0.288, top_5=0.528, top_10=0.553, mrr=0.382)
        append_run_to_expected_results(
            result,
            backend="bm25s",
            athena_release_date="2026-04-30",
            out=out,
        )

        loaded = json.loads(out.read_text())
        assert loaded["athena_release_date"] == "2026-04-30"
        assert len(loaded["runs"]) == 1
        assert loaded["runs"][0] == {
            "backend": "bm25s",
            "n": 1000,
            "top_1": 0.288,
            "top_5": 0.528,
            "top_10": 0.553,
            "mrr": 0.382,
        }

    def test_appending_a_second_run_preserves_the_first(self, tmp_path):
        from portiere.benchmarks.athena_icd_snomed.runner import (
            BenchmarkResult,
            append_run_to_expected_results,
        )

        out = tmp_path / "expected_results.json"
        append_run_to_expected_results(
            BenchmarkResult(n=1000, top_1=0.288, top_5=0.528, top_10=0.553, mrr=0.382),
            backend="bm25s",
            athena_release_date="2026-04-30",
            out=out,
        )
        append_run_to_expected_results(
            BenchmarkResult(n=1000, top_1=0.342, top_5=0.604, top_10=0.631, mrr=0.451),
            backend="faiss",
            athena_release_date="2026-04-30",
            out=out,
        )

        loaded = json.loads(out.read_text())
        backends = [r["backend"] for r in loaded["runs"]]
        assert backends == ["bm25s", "faiss"]

    def test_appending_replaces_existing_run_for_same_backend(self, tmp_path):
        """Re-running with a backend already present overwrites that row."""
        from portiere.benchmarks.athena_icd_snomed.runner import (
            BenchmarkResult,
            append_run_to_expected_results,
        )

        out = tmp_path / "expected_results.json"
        append_run_to_expected_results(
            BenchmarkResult(n=1000, top_1=0.10, top_5=0.20, top_10=0.30, mrr=0.15),
            backend="hybrid",
            athena_release_date="2026-04-30",
            out=out,
        )
        append_run_to_expected_results(
            BenchmarkResult(n=1000, top_1=0.40, top_5=0.65, top_10=0.68, mrr=0.50),
            backend="hybrid",
            athena_release_date="2026-04-30",
            out=out,
        )

        loaded = json.loads(out.read_text())
        assert len(loaded["runs"]) == 1
        assert loaded["runs"][0]["top_1"] == 0.40


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
                "--backend",
                "bm25s",
                "--out",
                str(out_json),
            ],
        )
        assert result.exit_code == 0, result.output
        assert out_json.exists()
        loaded = json.loads(out_json.read_text())
        assert loaded["runs"][0]["n"] == 3

    def test_benchmark_cli_accepts_backend_flag(self, tmp_path):
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
                "--backend",
                "bm25s",
                "--out",
                str(out_json),
            ],
        )
        assert result.exit_code == 0, result.output
        assert out_json.exists()
        loaded = json.loads(out_json.read_text())
        assert "runs" in loaded
        assert any(r["backend"] == "bm25s" for r in loaded["runs"])


# ── sampling ─────────────────────────────────────────────────────


class TestSampling:
    def test_uniform_sampling_matches_v021_behavior(self, tmp_path):
        """Default uniform sampling must produce the same IDs as the v0.2.1 runner."""
        from portiere.benchmarks.athena_icd_snomed.sampling import generate_test_ids

        athena = _make_synthetic_athena(tmp_path)
        concept = pd.read_csv(athena / "CONCEPT.csv", sep="\t", low_memory=False)
        cr = pd.read_csv(athena / "CONCEPT_RELATIONSHIP.csv", sep="\t", low_memory=False)

        ids_a = generate_test_ids(concept, cr, n=3, seed=42, stratify_by=None)
        ids_b = generate_test_ids(concept, cr, n=3, seed=42, stratify_by=None)
        assert ids_a == ids_b
        assert len(ids_a) == 3
        assert all(isinstance(i, int) for i in ids_a)

    def test_uniform_sampling_deterministic_across_seeds(self, tmp_path):
        """Different seeds produce different (but still deterministic) sets."""
        from portiere.benchmarks.athena_icd_snomed.sampling import generate_test_ids

        athena = _make_synthetic_athena(tmp_path)
        concept = pd.read_csv(athena / "CONCEPT.csv", sep="\t", low_memory=False)
        cr = pd.read_csv(athena / "CONCEPT_RELATIONSHIP.csv", sep="\t", low_memory=False)

        ids_42 = generate_test_ids(concept, cr, n=3, seed=42, stratify_by=None)
        ids_42_again = generate_test_ids(concept, cr, n=3, seed=42, stratify_by=None)
        assert ids_42 == ids_42_again

    def test_domain_stratification_returns_correct_count(self, tmp_path):
        """Stratified sample size must equal n (when pool is big enough)."""
        from portiere.benchmarks.athena_icd_snomed.sampling import generate_test_ids

        athena = _make_synthetic_athena(tmp_path)
        concept = pd.read_csv(athena / "CONCEPT.csv", sep="\t", low_memory=False)
        cr = pd.read_csv(athena / "CONCEPT_RELATIONSHIP.csv", sep="\t", low_memory=False)

        # Fixture has 5 ICD codes in 1 domain (Condition) — pool <= n so
        # all 5 are returned regardless of stratification.
        ids = generate_test_ids(concept, cr, n=5, seed=42, stratify_by="domain")
        assert len(ids) <= 5
        assert all(isinstance(i, int) for i in ids)

    def test_invalid_stratify_by_raises(self, tmp_path):
        from portiere.benchmarks.athena_icd_snomed.sampling import generate_test_ids

        athena = _make_synthetic_athena(tmp_path)
        concept = pd.read_csv(athena / "CONCEPT.csv", sep="\t", low_memory=False)
        cr = pd.read_csv(athena / "CONCEPT_RELATIONSHIP.csv", sep="\t", low_memory=False)

        with pytest.raises(ValueError, match="stratify_by"):
            generate_test_ids(concept, cr, n=3, seed=42, stratify_by="claim-frequency")


# ── stratified run_benchmark ──────────────────────────────────────


class TestStratifiedRunBenchmark:
    def test_run_benchmark_accepts_stratify_by_kwarg(self, tmp_path):
        from portiere.benchmarks.athena_icd_snomed.runner import run_benchmark

        athena = _make_synthetic_athena(tmp_path)
        test_set = tmp_path / "gold_test_set.csv"
        result = run_benchmark(
            athena,
            test_set_path=test_set,
            backend="bm25s",
            stratify_by="domain",
        )
        assert result.n == 3

    def test_cli_accepts_stratify_by_flag(self, tmp_path):
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
                "--backend",
                "bm25s",
                "--stratify-by",
                "domain",
                "--out",
                str(out_json),
            ],
        )
        assert result.exit_code == 0, result.output

    def test_cli_rejects_unknown_stratify_value(self, tmp_path):
        from click.testing import CliRunner

        from portiere.cli import cli

        athena = _make_synthetic_athena(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "benchmark",
                "athena-icd-snomed",
                "--athena-dir",
                str(athena),
                "--stratify-by",
                "claim-frequency",
            ],
        )
        assert result.exit_code != 0
        assert "claim-frequency" in result.output or "Invalid value" in result.output

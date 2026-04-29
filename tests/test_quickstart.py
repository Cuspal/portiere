"""End-to-end smoke test for ``portiere quickstart`` (Slice 5 Task 5.7)."""

from __future__ import annotations

import json
from pathlib import Path


def _find_manifest(out_dir: Path) -> Path | None:
    runs = out_dir / "portiere-quickstart" / "runs"
    if not runs.exists():
        return None
    found = list(runs.glob("*/manifest.lock.json"))
    return found[-1] if found else None


class TestQuickstartCommand:
    def test_command_registered(self):
        from portiere.cli import cli

        assert "quickstart" in cli.commands

    def test_quickstart_is_quickstart_command(self):
        from portiere.cli.quickstart import quickstart_command

        assert quickstart_command.name == "quickstart"


class TestQuickstartEndToEnd:
    def test_runs_to_completion_offline(self, tmp_path, monkeypatch):
        """End-to-end: portiere quickstart against tmp output dir."""
        from click.testing import CliRunner

        from portiere.cli import cli

        monkeypatch.setenv("PORTIERE_QUICKSTART_DIR", str(tmp_path))
        runner = CliRunner()
        result = runner.invoke(cli, ["quickstart"])
        assert result.exit_code == 0, f"\nSTDOUT:\n{result.output}\n"
        # All 5 stages report in the summary
        assert "Pipeline summary" in result.output
        for stage in ("knowledge_layer", "ingest", "schema", "concept", "etl"):
            assert stage in result.output, f"missing stage in summary: {stage}"

    def test_emits_manifest_with_all_stages(self, tmp_path, monkeypatch):
        from click.testing import CliRunner

        from portiere.cli import cli

        monkeypatch.setenv("PORTIERE_QUICKSTART_DIR", str(tmp_path))
        runner = CliRunner()
        result = runner.invoke(cli, ["quickstart"])
        assert result.exit_code == 0

        manifest = _find_manifest(tmp_path)
        assert manifest is not None and manifest.exists()
        m = json.loads(manifest.read_text())
        recorded_stages = {s["stage"] for s in m["stages"]}
        # Spec acceptance: manifest covers all 5 stages
        for stage in ("ingest", "schema", "concept", "etl", "validate"):
            assert stage in recorded_stages, f"manifest missing stage: {stage}"

    def test_manifest_records_target_model_and_vocabularies(self, tmp_path, monkeypatch):
        from click.testing import CliRunner

        from portiere.cli import cli

        monkeypatch.setenv("PORTIERE_QUICKSTART_DIR", str(tmp_path))
        runner = CliRunner()
        runner.invoke(cli, ["quickstart"])

        manifest = _find_manifest(tmp_path)
        assert manifest is not None
        m = json.loads(manifest.read_text())
        assert m["target_model"] == "omop_cdm_v5.4"
        assert set(m["vocabularies_requested"]) == {"ICD10CM", "LOINC", "RxNorm"}
        # Embedding identity is recorded; provider is "none" (no SapBERT download)
        assert m["embedding"]["name"] == "none"

    def test_explicit_output_dir_flag(self, tmp_path):
        from click.testing import CliRunner

        from portiere.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["quickstart", "--output-dir", str(tmp_path)])
        assert result.exit_code == 0
        assert _find_manifest(tmp_path) is not None

    def test_prints_snomed_note(self, tmp_path, monkeypatch):
        """Per Slice 5 decision 3 — quickstart output points users at vocab docs
        for SNOMED rather than offering a confusing flag."""
        from click.testing import CliRunner

        from portiere.cli import cli

        monkeypatch.setenv("PORTIERE_QUICKSTART_DIR", str(tmp_path))
        runner = CliRunner()
        result = runner.invoke(cli, ["quickstart"])
        assert result.exit_code == 0
        assert "SNOMED" in result.output
        assert "vocabulary-setup" in result.output or "vocabulary_setup" in result.output

    def test_prints_replay_hint(self, tmp_path, monkeypatch):
        from click.testing import CliRunner

        from portiere.cli import cli

        monkeypatch.setenv("PORTIERE_QUICKSTART_DIR", str(tmp_path))
        runner = CliRunner()
        result = runner.invoke(cli, ["quickstart"])
        assert result.exit_code == 0
        assert "portiere replay" in result.output
        assert "manifest.lock.json" in result.output


class TestQuickstartReplayRoundtrip:
    """End-to-end: quickstart -> replay should both succeed."""

    def test_replay_after_quickstart(self, tmp_path, monkeypatch):
        from click.testing import CliRunner

        from portiere.cli import cli

        monkeypatch.setenv("PORTIERE_QUICKSTART_DIR", str(tmp_path))
        runner = CliRunner()

        # 1. Run quickstart
        result1 = runner.invoke(cli, ["quickstart"])
        assert result1.exit_code == 0

        # 2. Replay the emitted manifest
        manifest = _find_manifest(tmp_path)
        assert manifest is not None
        result2 = runner.invoke(cli, ["replay", str(manifest)])
        # Replay reconstructs the project — exit 0 is success even if
        # later pipeline ops aren't auto-rerun (v0.2.0 scope).
        assert result2.exit_code == 0, f"replay failed:\n{result2.output}\n"
        assert "portiere-quickstart" in result2.output


class TestNoNetworkCalls:
    """The whole demo runs entirely against bundled data — no socket."""

    def test_no_outbound_network(self, tmp_path, monkeypatch):
        # Patch socket.getaddrinfo to raise on any DNS lookup. Loopback
        # connections (e.g., embedded HTTP within a library) would still
        # work via loopback IP, but DNS-lookup-driven outbound calls fail.
        import socket

        from click.testing import CliRunner

        from portiere.cli import cli

        def _no_dns(*args, **kwargs):
            raise RuntimeError("DNS lookup attempted during quickstart")

        monkeypatch.setattr(socket, "getaddrinfo", _no_dns)
        monkeypatch.setenv("PORTIERE_QUICKSTART_DIR", str(tmp_path))

        runner = CliRunner()
        result = runner.invoke(cli, ["quickstart"])
        # Quickstart should run end-to-end without DNS
        assert result.exit_code == 0, (
            f"quickstart triggered DNS — likely no longer offline.\n"
            f"output:\n{result.output}\n"
            f"exception: {result.exception}"
        )


class TestQuickstartUnder60Seconds:
    """Spec acceptance: <60s on a 2020-or-newer laptop."""

    def test_under_120_seconds(self, tmp_path, monkeypatch):
        import time

        from click.testing import CliRunner

        from portiere.cli import cli

        monkeypatch.setenv("PORTIERE_QUICKSTART_DIR", str(tmp_path))
        runner = CliRunner()
        start = time.time()
        result = runner.invoke(cli, ["quickstart"])
        elapsed = time.time() - start
        assert result.exit_code == 0
        # CI has a 120s budget; spec target is 60s on a real laptop.
        assert elapsed < 120, f"quickstart took {elapsed:.1f}s (CI budget 120s)"

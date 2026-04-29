"""Tests for portiere.repro.replay (Slice 4 Task 4.5)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


def _make_manifest_for_file_source(
    tmp_path: Path,
    source_path: Path,
    *,
    bad_sha: bool = False,
    target_model: str = "omop_cdm_v5.4",
    vocabularies: list[Path] | None = None,
) -> Path:
    """Build a minimal manifest pointing at a file source.

    If ``source_path`` doesn't exist, we record a placeholder sha so the
    fixture itself doesn't error (the missing-source case is what the
    test wants to exercise inside ``replay()``).
    """
    from portiere.repro.hashing import sha256_file

    if bad_sha or not source_path.exists():
        sha = "deadbeef"
    else:
        sha = sha256_file(source_path)
    vocab_entries = []
    for v in vocabularies or []:
        vocab_entries.append(
            {
                "name": v.stem,
                "version_date": None,
                "sha256_of_source_file": sha256_file(v),
                "path": str(v),
            }
        )

    manifest = {
        "manifest_version": "1",
        "run": {
            "run_id": "abc123",
            "started_at": "2026-04-29T00:00:00+00:00",
            "finished_at": "2026-04-29T00:01:00+00:00",
            "duration_seconds": 60.0,
        },
        "portiere_version": "0.2.0",
        "python_version": "3.12.1",
        "os_string": "TestOS",
        "git_sha": None,
        "git_dirty": None,
        "project_name": "replay-target",
        "target_model": target_model,
        "task": "standardize",
        "source_standard": None,
        "vocabularies_requested": ["SNOMED"],
        "embedding": {
            "name": "sapbert",
            "hf_revision": None,
            "sha256_of_config": None,
            "dimension": 768,
        },
        "knowledge_backend": None,
        "vocabularies": vocab_entries,
        "prompt_templates": [],
        "thresholds": {},
        "source_data": {
            "path": str(source_path),
            "sha256": sha,
            "connection_string_redacted": None,
            "table_or_query": None,
        },
        "stages": [],
    }
    out = tmp_path / "manifest.lock.json"
    out.write_text(json.dumps(manifest, indent=2))
    return out


class TestReplayValidation:
    def test_raises_when_source_missing(self, tmp_path):
        from portiere.repro.replay import ManifestReplayError, replay

        # Fake a source path that doesn't exist
        manifest_path = _make_manifest_for_file_source(
            tmp_path, source_path=tmp_path / "nonexistent.csv"
        )
        with pytest.raises(ManifestReplayError, match="source data missing"):
            replay(manifest_path)

    def test_raises_when_source_sha_mismatch(self, tmp_path):
        from portiere.repro.replay import ManifestReplayError, replay

        src = tmp_path / "patients.csv"
        src.write_text("a,b\n1,2\n")
        manifest_path = _make_manifest_for_file_source(tmp_path, src, bad_sha=True)
        with pytest.raises(ManifestReplayError, match="sha256 mismatch"):
            replay(manifest_path)

    def test_raises_when_vocab_missing(self, tmp_path):
        from portiere.repro.replay import ManifestReplayError, replay

        src = tmp_path / "patients.csv"
        src.write_text("a,b\n1,2\n")
        # vocab path that doesn't exist
        bad_vocab = tmp_path / "no_such_vocab.csv"
        # build a manifest with a vocab entry that doesn't exist
        from portiere.repro.hashing import sha256_file

        manifest = {
            "manifest_version": "1",
            "run": {
                "run_id": "x",
                "started_at": "2026-04-29T00:00:00+00:00",
                "finished_at": None,
                "duration_seconds": None,
            },
            "portiere_version": "0.2.0",
            "python_version": "3.12.1",
            "os_string": "TestOS",
            "git_sha": None,
            "git_dirty": None,
            "project_name": "p",
            "target_model": "omop_cdm_v5.4",
            "task": "standardize",
            "source_standard": None,
            "vocabularies_requested": [],
            "embedding": {"name": "x", "dimension": 1},
            "knowledge_backend": None,
            "vocabularies": [
                {
                    "name": "SNOMED",
                    "version_date": None,
                    "sha256_of_source_file": "abc",
                    "path": str(bad_vocab),
                }
            ],
            "prompt_templates": [],
            "thresholds": {},
            "source_data": {
                "path": str(src),
                "sha256": sha256_file(src),
                "connection_string_redacted": None,
                "table_or_query": None,
            },
            "stages": [],
        }
        out = tmp_path / "manifest.lock.json"
        out.write_text(json.dumps(manifest))
        with pytest.raises(ManifestReplayError, match="vocabulary missing"):
            replay(out)


class TestReplaySuccess:
    def test_returns_summary_for_valid_manifest(self, tmp_path):
        from portiere.repro.replay import replay

        src = tmp_path / "patients.csv"
        src.write_text("a,b\n1,2\n")
        manifest_path = _make_manifest_for_file_source(tmp_path, src)
        result = replay(manifest_path)
        assert "replay_run_id" in result
        assert "manifest_path" in result
        assert "project_name" in result
        assert result["project_name"] == "replay-target"

    def test_explicit_output_dir_is_used(self, tmp_path):
        from portiere.repro.replay import replay

        src = tmp_path / "patients.csv"
        src.write_text("a,b\n1,2\n")
        manifest_path = _make_manifest_for_file_source(tmp_path, src)
        out_dir = tmp_path / "replay_out"
        result = replay(manifest_path, output_dir=out_dir)
        assert result["output_dir"] == str(out_dir)


class TestReplayCLI:
    def test_replay_command_exists(self):
        from portiere.cli.replay import replay_command

        assert replay_command.name == "replay"

    def test_replay_command_runs(self, tmp_path):
        from click.testing import CliRunner

        from portiere.cli.replay import replay_command

        src = tmp_path / "patients.csv"
        src.write_text("a,b\n1,2\n")
        manifest_path = _make_manifest_for_file_source(tmp_path, src)

        runner = CliRunner()
        result = runner.invoke(replay_command, [str(manifest_path)])
        assert result.exit_code == 0, result.output

    def test_replay_command_reports_missing_artifact(self, tmp_path):
        from click.testing import CliRunner

        from portiere.cli.replay import replay_command

        manifest_path = _make_manifest_for_file_source(
            tmp_path, source_path=tmp_path / "nonexistent.csv"
        )
        runner = CliRunner()
        result = runner.invoke(replay_command, [str(manifest_path)])
        # exit code is non-zero (Click exits 1 by default when raising)
        assert result.exit_code != 0

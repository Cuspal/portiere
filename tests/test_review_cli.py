"""Tests for `portiere review <project-dir>` CLI command (Slice 4, v0.3.1).

The CLI launches Streamlit as a subprocess — we don't actually start the
server in tests. We assert the command is registered, parses its args, and
constructs the right subprocess invocation (verified by monkeypatching
``subprocess.Popen``).
"""

from __future__ import annotations

import pytest
from click.testing import CliRunner


class TestReviewCommandRegistered:
    def test_review_command_in_cli_group(self):
        from portiere.cli import cli

        assert "review" in cli.commands

    def test_review_help_mentions_project_dir(self):
        from portiere.cli import cli

        result = CliRunner().invoke(cli, ["review", "--help"])
        assert result.exit_code == 0
        assert "PROJECT_DIR" in result.output or "project" in result.output.lower()


class TestReviewCommandInvocation:
    def test_review_launches_streamlit_subprocess(self, tmp_path, monkeypatch):
        """Verify the CLI builds a streamlit run command and Popens it."""
        from portiere.cli import cli

        captured: dict = {}

        class _FakePopen:
            def __init__(self, cmd, **kw):
                captured["cmd"] = cmd
                captured["kw"] = kw

            def wait(self):
                return 0

            def terminate(self):
                pass

        monkeypatch.setattr("subprocess.Popen", _FakePopen)

        # Use --no-wait so the test doesn't block; if --no-wait isn't a flag,
        # the test still works because _FakePopen.wait() returns 0 immediately.
        result = CliRunner().invoke(cli, ["review", str(tmp_path)])
        assert result.exit_code == 0, result.output

        cmd = captured.get("cmd")
        assert cmd is not None
        # The command should contain "streamlit" and "run"
        joined = " ".join(cmd)
        assert "streamlit" in joined
        assert "run" in joined
        # And the project dir should be in the argv (via -- arg pass-through)
        assert str(tmp_path) in joined

    def test_review_rejects_missing_project_dir(self, tmp_path):
        from portiere.cli import cli

        missing = tmp_path / "does_not_exist"
        result = CliRunner().invoke(cli, ["review", str(missing)])
        assert result.exit_code != 0

    def test_review_default_host_is_localhost(self, tmp_path, monkeypatch):
        """Default binds to 127.0.0.1 (local-only — no auth)."""
        from portiere.cli import cli

        captured: dict = {}

        class _FakePopen:
            def __init__(self, cmd, **kw):
                captured["cmd"] = cmd

            def wait(self):
                return 0

            def terminate(self):
                pass

        monkeypatch.setattr("subprocess.Popen", _FakePopen)

        result = CliRunner().invoke(cli, ["review", str(tmp_path)])
        assert result.exit_code == 0, result.output
        joined = " ".join(captured["cmd"])
        # Streamlit's --server.address flag enforces local-only by default.
        assert "127.0.0.1" in joined or "localhost" in joined

    def test_review_host_override_propagates(self, tmp_path, monkeypatch):
        from portiere.cli import cli

        captured: dict = {}

        class _FakePopen:
            def __init__(self, cmd, **kw):
                captured["cmd"] = cmd

            def wait(self):
                return 0

            def terminate(self):
                pass

        monkeypatch.setattr("subprocess.Popen", _FakePopen)

        result = CliRunner().invoke(cli, ["review", "--host", "0.0.0.0", str(tmp_path)])
        assert result.exit_code == 0, result.output
        joined = " ".join(captured["cmd"])
        assert "0.0.0.0" in joined


class TestStreamlitImport:
    def test_streamlit_is_importable(self):
        """The [review] extra must pull streamlit into the dev env for this slice."""
        pytest.importorskip("streamlit")

"""Tests for the `portiere doctor` egress-posture preflight."""

from click.testing import CliRunner

from portiere.cli import cli


def test_doctor_reports_local_stack(tmp_path):
    cfg = tmp_path / "portiere.yaml"
    cfg.write_text("llm:\n  provider: none\n")
    res = CliRunner().invoke(cli, ["doctor", "--config", str(cfg)])
    assert res.exit_code == 0, res.output
    assert "egress" in res.output.lower()
    assert "LOCAL" in res.output


def test_doctor_flags_remote_provider(tmp_path):
    cfg = tmp_path / "portiere.yaml"
    cfg.write_text("llm:\n  provider: openai\n  api_key: sk-test\n")
    res = CliRunner().invoke(cli, ["doctor", "--config", str(cfg)])
    assert res.exit_code == 0  # reporting alone never fails
    assert "REMOTE" in res.output
    assert "openai" in res.output


def test_doctor_assert_no_egress_fails_on_remote(tmp_path):
    cfg = tmp_path / "portiere.yaml"
    cfg.write_text("llm:\n  provider: anthropic\n  api_key: sk-test\n")
    res = CliRunner().invoke(cli, ["doctor", "--config", str(cfg), "--assert-no-egress"])
    assert res.exit_code == 1
    assert "anthropic" in res.output


def test_doctor_assert_no_egress_passes_on_local(tmp_path):
    cfg = tmp_path / "portiere.yaml"
    cfg.write_text("llm:\n  provider: ollama\n  endpoint: http://localhost:11434\n")
    res = CliRunner().invoke(cli, ["doctor", "--config", str(cfg), "--assert-no-egress"])
    assert res.exit_code == 0, res.output


def test_doctor_no_config_uses_defaults():
    res = CliRunner().invoke(cli, ["doctor"])
    assert res.exit_code == 0, res.output
    assert "LOCAL" in res.output


def test_doctor_reports_remote_knowledge_backend(tmp_path):
    cfg = tmp_path / "portiere.yaml"
    cfg.write_text(
        "knowledge_layer:\n  backend: qdrant\n  qdrant_url: https://xyz.cloud.qdrant.io\n"
    )
    res = CliRunner().invoke(cli, ["doctor", "--config", str(cfg), "--assert-no-egress"])
    assert res.exit_code == 1
    assert "qdrant" in res.output

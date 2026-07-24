"""Tests for the offline (no-egress) mode guarantee."""

import pytest

from portiere.config import (
    EmbeddingConfig,
    LLMConfig,
    PortiereConfig,
    RerankerConfig,
)
from portiere.exceptions import ConfigurationError


class TestOfflineConstruction:
    def test_offline_defaults_false(self):
        cfg = PortiereConfig()
        assert cfg.offline is False

    def test_offline_with_default_local_stack_ok(self):
        cfg = PortiereConfig(offline=True)
        assert cfg.offline is True

    def test_offline_rejects_remote_llm(self):
        with pytest.raises(ConfigurationError) as exc:
            PortiereConfig(offline=True, llm=LLMConfig(provider="openai"))
        assert "offline" in str(exc.value).lower()
        assert "openai" in str(exc.value)

    def test_offline_rejects_remote_embedding(self):
        with pytest.raises(ConfigurationError):
            PortiereConfig(offline=True, embedding=EmbeddingConfig(provider="bedrock"))

    def test_offline_rejects_remote_reranker_endpoint(self):
        with pytest.raises(ConfigurationError):
            PortiereConfig(
                offline=True,
                reranker=RerankerConfig(endpoint="https://remote.example.com"),
            )

    def test_offline_allows_local_ollama(self):
        cfg = PortiereConfig(
            offline=True,
            llm=LLMConfig(provider="ollama", endpoint="http://localhost:11434"),
        )
        assert cfg.offline is True

    def test_offline_error_lists_all_violations(self):
        """A reviewer wants the full list, not just the first violation."""
        with pytest.raises(ConfigurationError) as exc:
            PortiereConfig(
                offline=True,
                llm=LLMConfig(provider="anthropic"),
                embedding=EmbeddingConfig(provider="openai"),
            )
        msg = str(exc.value)
        assert "anthropic" in msg
        assert "openai" in msg

    def test_offline_env_var(self, monkeypatch):
        monkeypatch.setenv("PORTIERE_OFFLINE", "true")
        cfg = PortiereConfig()
        assert cfg.offline is True


class TestOfflineRuntimeEnforcement:
    def test_assert_no_egress_passes_on_local_stack(self):
        cfg = PortiereConfig(offline=True)
        cfg.assert_no_egress()  # must not raise

    def test_assert_no_egress_catches_post_construction_mutation(self):
        """A config mutated after construction must still be caught at runtime."""
        cfg = PortiereConfig(offline=True)
        cfg.llm = LLMConfig(provider="openai")
        with pytest.raises(ConfigurationError):
            cfg.assert_no_egress()

    def test_assert_no_egress_noop_when_not_offline(self):
        cfg = PortiereConfig(llm=LLMConfig(provider="openai"))
        cfg.assert_no_egress()  # offline not asserted -> no-op


class TestOfflineProjectWiring:
    def test_project_init_enforces_offline(self, tmp_path):
        """Project construction re-checks the guarantee at runtime."""
        from portiere.project import Project

        cfg = PortiereConfig(offline=True, local_project_dir=tmp_path)
        cfg.llm = LLMConfig(provider="openai")  # post-construction mutation

        class _Stub:
            pass

        with pytest.raises(ConfigurationError):
            Project(
                name="p",
                target_model="omop_cdm_v5.4",
                vocabularies=[],
                config=cfg,
                storage=_Stub(),
                project_id="x",
                engine=_Stub(),
            )

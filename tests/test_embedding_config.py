"""
Tests for EmbeddingConfig, RerankerConfig, and smart defaults in PortiereConfig.
"""

import pytest

from portiere.config import EmbeddingConfig, PortiereConfig, RerankerConfig


class TestEmbeddingConfig:
    """Tests for EmbeddingConfig model."""

    def test_defaults(self):
        config = EmbeddingConfig()
        assert config.provider == "huggingface"
        assert config.model == "cambridgeltl/SapBERT-from-PubMedBERT-fulltext"
        assert config.endpoint is None
        assert config.api_key is None
        assert config.batch_size == 64

    def test_custom_values(self):
        config = EmbeddingConfig(
            provider="openai",
            model="text-embedding-3-small",
            api_key="sk-test",
        )
        assert config.provider == "openai"
        assert config.model == "text-embedding-3-small"
        assert config.api_key == "sk-test"

    def test_extra_fields_allowed(self):
        config = EmbeddingConfig(provider="openai", model="test", custom_field="value")
        assert config.custom_field == "value"

    def test_portiere_provider_rejected(self):
        """'portiere' is not a valid embedding provider."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            EmbeddingConfig(provider="portiere")


class TestRerankerConfig:
    """Tests for RerankerConfig model."""

    def test_defaults(self):
        config = RerankerConfig()
        assert config.provider == "huggingface"
        assert config.model == "cross-encoder/ms-marco-MiniLM-L-6-v2"
        assert config.endpoint is None
        assert config.api_key is None

    def test_none_provider(self):
        config = RerankerConfig(provider="none")
        assert config.provider == "none"

    def test_portiere_provider_rejected(self):
        """'portiere' is not a valid reranker provider."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            RerankerConfig(provider="portiere")


class TestSmartDefaults:
    """Tests for PortiereConfig smart defaults via model_validator."""

    def test_no_config_defaults_to_huggingface(self):
        """Default config → huggingface with SapBERT."""
        config = PortiereConfig()
        assert config.embedding.provider == "huggingface"
        assert config.embedding.model == "cambridgeltl/SapBERT-from-PubMedBERT-fulltext"
        assert config.reranker.provider == "huggingface"
        assert config.pipeline == "local"

    def test_api_key_defaults_to_cloud_pipeline(self):
        """API key present → pipeline defaults to 'cloud', providers stay local defaults."""
        config = PortiereConfig(api_key="pt_sk_test123")
        assert config.pipeline == "cloud"
        assert config.embedding.provider == "huggingface"
        assert config.reranker.provider == "huggingface"

    def test_api_key_with_endpoint_sets_cloud_pipeline(self):
        """API key + endpoint sets cloud pipeline."""
        config = PortiereConfig(api_key="pt_sk_test", endpoint="https://custom.portiere.dev")
        assert config.pipeline == "cloud"
        assert config.endpoint == "https://custom.portiere.dev"

    def test_explicit_pipeline_not_overridden(self):
        """Explicitly set pipeline is not overridden by api_key."""
        config = PortiereConfig(api_key="pt_sk_test", pipeline="local")
        assert config.pipeline == "local"


class TestBackwardCompat:
    """Tests for backward compatibility with legacy string fields."""

    def test_legacy_embedding_model_string(self):
        """Legacy embedding_model string → coerced to EmbeddingConfig."""
        config = PortiereConfig(embedding_model="custom/model")
        assert config.embedding.provider == "huggingface"
        assert config.embedding.model == "custom/model"
        # Legacy field still readable
        assert config.embedding_model == "custom/model"

    def test_legacy_reranker_model_string(self):
        """Legacy reranker_model string → coerced to RerankerConfig."""
        config = PortiereConfig(reranker_model="cross-encoder/test")
        assert config.reranker.provider == "huggingface"
        assert config.reranker.model == "cross-encoder/test"

    def test_legacy_reranker_model_none(self):
        """Legacy reranker_model=None → provider='none'."""
        config = PortiereConfig(reranker_model=None)
        assert config.reranker.provider == "none"

    def test_legacy_embedding_model_syncs_back(self):
        """When using explicit EmbeddingConfig, legacy field stays in sync."""
        config = PortiereConfig(
            embedding=EmbeddingConfig(provider="ollama", model="nomic-embed-text")
        )
        assert config.embedding_model == "nomic-embed-text"


class TestExplicitConfig:
    """Tests for explicit EmbeddingConfig/RerankerConfig overriding smart defaults."""

    def test_explicit_embedding_not_overridden_by_api_key(self):
        """Explicit embedding config is NOT overridden by api_key presence."""
        config = PortiereConfig(
            api_key="pt_sk_test",
            embedding=EmbeddingConfig(provider="ollama", model="nomic-embed-text"),
        )
        assert config.embedding.provider == "ollama"
        assert config.embedding.model == "nomic-embed-text"

    def test_explicit_reranker_not_overridden(self):
        """Explicit reranker config is NOT overridden by api_key presence."""
        config = PortiereConfig(
            api_key="pt_sk_test",
            reranker=RerankerConfig(provider="none"),
        )
        assert config.reranker.provider == "none"

    def test_mixed_explicit_and_default(self):
        """Explicit embedding + default reranker (with api_key)."""
        config = PortiereConfig(
            api_key="pt_sk_test",
            embedding=EmbeddingConfig(
                provider="openai", model="text-embedding-3-small", api_key="sk-xxx"
            ),
        )
        assert config.embedding.provider == "openai"
        assert config.embedding.api_key == "sk-xxx"
        # Reranker stays at default (huggingface) — api_key no longer changes provider
        assert config.reranker.provider == "huggingface"

    def test_openai_compatible_endpoint(self):
        """OpenAI-compatible endpoint with custom base_url."""
        config = PortiereConfig(
            embedding=EmbeddingConfig(
                provider="openai",
                model="BAAI/bge-large-en-v1.5",
                endpoint="http://localhost:8000/v1",
            ),
            reranker=RerankerConfig(provider="none"),
        )
        assert config.embedding.provider == "openai"
        assert config.embedding.endpoint == "http://localhost:8000/v1"
        assert config.reranker.provider == "none"

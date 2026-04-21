"""
Tests for EmbeddingGateway and provider implementations.
"""

import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from portiere.config import EmbeddingConfig
from portiere.embedding.gateway import EmbeddingGateway
from portiere.embedding.providers.base import BaseEmbeddingProvider


class TestEmbeddingGatewayRouting:
    """Test that EmbeddingGateway routes to the correct provider."""

    def test_huggingface_provider(self):
        """HuggingFace config creates HuggingFaceEmbeddingProvider."""
        mock_model = MagicMock()
        mock_model.get_sentence_embedding_dimension.return_value = 768
        mock_st = MagicMock()
        mock_st.SentenceTransformer.return_value = mock_model

        with patch.dict(sys.modules, {"sentence_transformers": mock_st}):
            config = EmbeddingConfig(provider="huggingface", model="test-model")
            gateway = EmbeddingGateway(config)

            from portiere.embedding.providers.huggingface_provider import (
                HuggingFaceEmbeddingProvider,
            )

            assert isinstance(gateway._provider, HuggingFaceEmbeddingProvider)

    def test_ollama_provider(self):
        """Ollama config creates OllamaEmbeddingProvider."""
        config = EmbeddingConfig(
            provider="ollama",
            model="nomic-embed-text",
            endpoint="http://localhost:11434",
        )
        gateway = EmbeddingGateway(config)

        from portiere.embedding.providers.ollama_provider import (
            OllamaEmbeddingProvider,
        )

        assert isinstance(gateway._provider, OllamaEmbeddingProvider)

    def test_openai_provider(self):
        """OpenAI config creates OpenAIEmbeddingProvider."""
        config = EmbeddingConfig(
            provider="openai",
            model="text-embedding-3-small",
            api_key="sk-test",
        )
        gateway = EmbeddingGateway(config)

        from portiere.embedding.providers.openai_provider import (
            OpenAIEmbeddingProvider,
        )

        assert isinstance(gateway._provider, OpenAIEmbeddingProvider)

    def test_unknown_provider_raises(self):
        """Unknown provider raises ValidationError at config level."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            EmbeddingConfig(provider="nonexistent", model="test")


class TestEmbeddingGatewayEncode:
    """Test EmbeddingGateway.encode() delegation."""

    def test_encode_delegates_to_provider(self):
        """encode() delegates to the underlying provider."""
        config = EmbeddingConfig(provider="huggingface", model="test")
        gateway = EmbeddingGateway(config)

        mock_provider = MagicMock(spec=BaseEmbeddingProvider)
        expected = np.random.randn(2, 768).astype("float32")
        mock_provider.encode.return_value = expected
        gateway._provider = mock_provider

        result = gateway.encode(["hello", "world"])
        np.testing.assert_array_equal(result, expected)
        mock_provider.encode.assert_called_once()

    def test_dimension_delegates(self):
        """dimension property delegates to provider."""
        config = EmbeddingConfig(provider="huggingface", model="test")
        gateway = EmbeddingGateway(config)

        mock_provider = MagicMock(spec=BaseEmbeddingProvider)
        mock_provider.dimension = 384
        gateway._provider = mock_provider

        assert gateway.dimension == 384

    def test_get_sentence_embedding_dimension_alias(self):
        """get_sentence_embedding_dimension() is backward-compat alias."""
        config = EmbeddingConfig(provider="huggingface", model="test")
        gateway = EmbeddingGateway(config)

        mock_provider = MagicMock(spec=BaseEmbeddingProvider)
        mock_provider.dimension = 768
        gateway._provider = mock_provider

        assert gateway.get_sentence_embedding_dimension() == 768


class TestHuggingFaceProvider:
    """Test HuggingFaceEmbeddingProvider."""

    def test_lazy_loading(self):
        """Model is loaded lazily on first encode()."""
        from portiere.embedding.providers.huggingface_provider import (
            HuggingFaceEmbeddingProvider,
        )

        config = EmbeddingConfig(provider="huggingface", model="test-model")
        provider = HuggingFaceEmbeddingProvider(config)
        assert provider._model is None

        mock_model = MagicMock()
        mock_model.get_sentence_embedding_dimension.return_value = 768
        expected = np.random.randn(1, 768).astype("float32")
        mock_model.encode.return_value = expected

        mock_st = MagicMock()
        mock_st.SentenceTransformer.return_value = mock_model

        with patch.dict(sys.modules, {"sentence_transformers": mock_st}):
            result = provider.encode(["test"])
            np.testing.assert_array_equal(result, expected)
            assert provider._model is mock_model

    def test_dimension_property(self):
        """dimension property returns model dimension."""
        from portiere.embedding.providers.huggingface_provider import (
            HuggingFaceEmbeddingProvider,
        )

        config = EmbeddingConfig(provider="huggingface", model="test-model")
        provider = HuggingFaceEmbeddingProvider(config)

        mock_model = MagicMock()
        mock_model.get_sentence_embedding_dimension.return_value = 384

        mock_st = MagicMock()
        mock_st.SentenceTransformer.return_value = mock_model

        with patch.dict(sys.modules, {"sentence_transformers": mock_st}):
            assert provider.dimension == 384


class TestOllamaProvider:
    """Test OllamaEmbeddingProvider."""

    def test_default_endpoint(self):
        """Default endpoint is localhost:11434."""
        from portiere.embedding.providers.ollama_provider import (
            OllamaEmbeddingProvider,
        )

        config = EmbeddingConfig(provider="ollama", model="nomic-embed-text")
        provider = OllamaEmbeddingProvider(config)
        assert provider._endpoint == "http://localhost:11434"

    def test_encode_posts_per_text(self):
        """encode() posts to /api/embeddings for each text."""
        from portiere.embedding.providers.ollama_provider import (
            OllamaEmbeddingProvider,
        )

        config = EmbeddingConfig(
            provider="ollama",
            model="nomic-embed-text",
            endpoint="http://localhost:11434",
        )
        provider = OllamaEmbeddingProvider(config)

        mock_response = MagicMock()
        mock_response.json.return_value = {"embedding": [0.1, 0.2, 0.3]}
        mock_response.raise_for_status = MagicMock()

        with patch.object(provider._client, "post", return_value=mock_response) as mock_post:
            result = provider.encode(["hello", "world"])

            assert isinstance(result, np.ndarray)
            assert result.shape == (2, 3)
            assert mock_post.call_count == 2  # Once per text


class TestOpenAIProvider:
    """Test OpenAIEmbeddingProvider."""

    def test_custom_base_url(self):
        """Custom endpoint sets base_url for OpenAI-compatible servers."""
        from portiere.embedding.providers.openai_provider import (
            OpenAIEmbeddingProvider,
        )

        config = EmbeddingConfig(
            provider="openai",
            model="BAAI/bge-large-en-v1.5",
            endpoint="http://localhost:8000/v1",
            api_key="not-needed",
        )
        provider = OpenAIEmbeddingProvider(config)
        assert provider._client.base_url is not None

    def test_encode_calls_embeddings_create(self):
        """encode() calls client.embeddings.create()."""
        from portiere.embedding.providers.openai_provider import (
            OpenAIEmbeddingProvider,
        )

        config = EmbeddingConfig(
            provider="openai",
            model="text-embedding-3-small",
            api_key="sk-test",
        )
        provider = OpenAIEmbeddingProvider(config)

        # Mock the OpenAI embeddings response
        mock_embedding = MagicMock()
        mock_embedding.embedding = [0.1, 0.2, 0.3]
        mock_response = MagicMock()
        mock_response.data = [mock_embedding, mock_embedding]

        with patch.object(
            provider._client.embeddings, "create", return_value=mock_response
        ) as mock_create:
            result = provider.encode(["hello", "world"])

            assert isinstance(result, np.ndarray)
            assert result.shape == (2, 3)
            mock_create.assert_called_once()


class TestLocalRerankerMultiProvider:
    """Test LocalReranker with RerankerConfig."""

    def test_none_provider_not_available(self):
        """provider='none' → available is False."""
        from portiere.config import RerankerConfig
        from portiere.local.reranker import LocalReranker

        config = RerankerConfig(provider="none")
        reranker = LocalReranker(reranker_config=config)
        assert not reranker.available

    def test_none_provider_passthrough(self):
        """provider='none' → rerank returns candidates unchanged."""
        from portiere.config import RerankerConfig
        from portiere.local.reranker import LocalReranker

        config = RerankerConfig(provider="none")
        reranker = LocalReranker(reranker_config=config)

        candidates = [
            {"concept_name": "A", "score": 0.9},
            {"concept_name": "B", "score": 0.8},
        ]
        # available is False, so rerank_with_blending should not be called
        # directly test rerank fallback
        result = reranker.rerank("test", candidates, top_k=2)
        assert len(result) == 2

    def test_legacy_model_name_still_works(self):
        """Old-style model_name= kwarg still works."""
        from portiere.local.reranker import LocalReranker

        reranker = LocalReranker(model_name="cross-encoder/test")
        assert reranker.model_name == "cross-encoder/test"
        assert reranker._provider == "huggingface"

    def test_score_pair_none_provider_returns_default(self):
        """provider='none' → score_pair returns 0.5."""
        from portiere.config import RerankerConfig
        from portiere.local.reranker import LocalReranker

        config = RerankerConfig(provider="none")
        reranker = LocalReranker(reranker_config=config)
        score = reranker.score_pair("test", "target")
        assert score == 0.5

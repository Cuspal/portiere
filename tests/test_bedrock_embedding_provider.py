"""
Tests for BedrockEmbeddingProvider.
"""

import io
import json
import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from portiere.config import EmbeddingConfig


class TestBedrockEmbeddingProviderInit:
    """Test BedrockEmbeddingProvider initialization."""

    def test_default_region(self):
        """Default region is us-east-1 when no endpoint/aws_region."""
        mock_boto3 = MagicMock()
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            from portiere.embedding.providers.bedrock_provider import (
                BedrockEmbeddingProvider,
            )

            config = EmbeddingConfig(
                provider="bedrock",
                model="amazon.titan-embed-text-v2:0",
            )
            provider = BedrockEmbeddingProvider(config)
            assert provider.region == "us-east-1"
            mock_boto3.client.assert_called_once_with("bedrock-runtime", region_name="us-east-1")

    def test_custom_region_via_endpoint(self):
        """endpoint field is reused as AWS region."""
        mock_boto3 = MagicMock()
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            from portiere.embedding.providers.bedrock_provider import (
                BedrockEmbeddingProvider,
            )

            config = EmbeddingConfig(
                provider="bedrock",
                model="amazon.titan-embed-text-v2:0",
                endpoint="eu-west-1",
            )
            provider = BedrockEmbeddingProvider(config)
            assert provider.region == "eu-west-1"

    def test_custom_region_via_extra_field(self):
        """aws_region extra field works as region source."""
        mock_boto3 = MagicMock()
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            from portiere.embedding.providers.bedrock_provider import (
                BedrockEmbeddingProvider,
            )

            config = EmbeddingConfig(
                provider="bedrock",
                model="amazon.titan-embed-text-v2:0",
                aws_region="ap-southeast-1",
            )
            provider = BedrockEmbeddingProvider(config)
            assert provider.region == "ap-southeast-1"

    def test_import_error_without_boto3(self):
        """Raises ImportError with helpful message when boto3 missing."""
        from portiere.embedding.providers.bedrock_provider import (
            BedrockEmbeddingProvider,
        )

        config = EmbeddingConfig(
            provider="bedrock",
            model="amazon.titan-embed-text-v2:0",
        )
        with patch.dict(sys.modules, {"boto3": None}):
            with pytest.raises(ImportError, match="portiere\\[bedrock\\]"):
                BedrockEmbeddingProvider(config)


def _make_bedrock_response(body_dict: dict) -> dict:
    """Helper to create a mock Bedrock invoke_model response."""
    body_bytes = json.dumps(body_dict).encode()
    return {"body": io.BytesIO(body_bytes)}


class TestBedrockTitanEncode:
    """Test encoding with Amazon Titan Embeddings models."""

    def _make_provider(self, model="amazon.titan-embed-text-v2:0"):
        mock_boto3 = MagicMock()
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            from portiere.embedding.providers.bedrock_provider import (
                BedrockEmbeddingProvider,
            )

            config = EmbeddingConfig(provider="bedrock", model=model)
            provider = BedrockEmbeddingProvider(config)
        return provider

    def test_encode_titan_single_text(self):
        """Titan model encodes single text correctly."""
        provider = self._make_provider()
        embedding = [0.1, 0.2, 0.3, 0.4]
        provider._client.invoke_model.return_value = _make_bedrock_response(
            {"embedding": embedding, "inputTextTokenCount": 5}
        )

        result = provider.encode(["hello"], normalize_embeddings=False)

        assert isinstance(result, np.ndarray)
        assert result.shape == (1, 4)
        np.testing.assert_array_almost_equal(result[0], embedding)

    def test_encode_titan_multiple_texts(self):
        """Titan model calls invoke_model per text."""
        provider = self._make_provider()

        emb1 = [0.1, 0.2, 0.3]
        emb2 = [0.4, 0.5, 0.6]
        provider._client.invoke_model.side_effect = [
            _make_bedrock_response({"embedding": emb1, "inputTextTokenCount": 3}),
            _make_bedrock_response({"embedding": emb2, "inputTextTokenCount": 3}),
        ]

        result = provider.encode(["hello", "world"], normalize_embeddings=False)

        assert result.shape == (2, 3)
        assert provider._client.invoke_model.call_count == 2

    def test_encode_titan_v2_includes_dimensions(self):
        """Titan v2 request body includes dimensions and normalize."""
        provider = self._make_provider("amazon.titan-embed-text-v2:0")
        provider._client.invoke_model.return_value = _make_bedrock_response(
            {"embedding": [0.1, 0.2], "inputTextTokenCount": 1}
        )

        provider.encode(["test"], normalize_embeddings=False)

        call_args = provider._client.invoke_model.call_args
        body = json.loads(call_args[1]["body"])
        assert body["dimensions"] == 1024
        assert body["normalize"] is True
        assert body["inputText"] == "test"

    def test_encode_titan_v1_no_dimensions(self):
        """Titan v1 request body does NOT include dimensions."""
        provider = self._make_provider("amazon.titan-embed-text-v1")
        provider._client.invoke_model.return_value = _make_bedrock_response(
            {"embedding": [0.1, 0.2], "inputTextTokenCount": 1}
        )

        provider.encode(["test"], normalize_embeddings=False)

        call_args = provider._client.invoke_model.call_args
        body = json.loads(call_args[1]["body"])
        assert "dimensions" not in body
        assert body["inputText"] == "test"


class TestBedrockCohereEncode:
    """Test encoding with Cohere Embed models on Bedrock."""

    def _make_provider(self, model="cohere.embed-english-v3"):
        mock_boto3 = MagicMock()
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            from portiere.embedding.providers.bedrock_provider import (
                BedrockEmbeddingProvider,
            )

            config = EmbeddingConfig(provider="bedrock", model=model)
            provider = BedrockEmbeddingProvider(config)
        return provider

    def test_encode_cohere_batch(self):
        """Cohere model sends all texts in single batch call."""
        provider = self._make_provider()
        embeddings = [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]
        provider._client.invoke_model.return_value = _make_bedrock_response(
            {"embeddings": embeddings}
        )

        result = provider.encode(["hello", "world"], normalize_embeddings=False)

        assert result.shape == (2, 3)
        # Cohere sends all texts in one call
        assert provider._client.invoke_model.call_count == 1

    def test_cohere_request_body(self):
        """Cohere request includes texts and input_type."""
        provider = self._make_provider()
        provider._client.invoke_model.return_value = _make_bedrock_response(
            {"embeddings": [[0.1, 0.2]]}
        )

        provider.encode(["test"], normalize_embeddings=False)

        call_args = provider._client.invoke_model.call_args
        body = json.loads(call_args[1]["body"])
        assert body["texts"] == ["test"]
        assert body["input_type"] == "search_document"


class TestBedrockNormalization:
    """Test L2 normalization behavior."""

    def _make_provider(self):
        mock_boto3 = MagicMock()
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            from portiere.embedding.providers.bedrock_provider import (
                BedrockEmbeddingProvider,
            )

            config = EmbeddingConfig(
                provider="bedrock",
                model="amazon.titan-embed-text-v2:0",
            )
            provider = BedrockEmbeddingProvider(config)
        return provider

    def test_l2_normalization(self):
        """normalize_embeddings=True produces unit vectors."""
        provider = self._make_provider()
        provider._client.invoke_model.return_value = _make_bedrock_response(
            {"embedding": [3.0, 4.0], "inputTextTokenCount": 1}
        )

        result = provider.encode(["test"], normalize_embeddings=True)

        norm = np.linalg.norm(result[0])
        assert abs(norm - 1.0) < 1e-6

    def test_no_normalization(self):
        """normalize_embeddings=False preserves original values."""
        provider = self._make_provider()
        provider._client.invoke_model.return_value = _make_bedrock_response(
            {"embedding": [3.0, 4.0], "inputTextTokenCount": 1}
        )

        result = provider.encode(["test"], normalize_embeddings=False)

        np.testing.assert_array_almost_equal(result[0], [3.0, 4.0])


class TestBedrockDimension:
    """Test dimension property."""

    def test_dimension_cached_after_encode(self):
        """dimension is cached after first encode call."""
        mock_boto3 = MagicMock()
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            from portiere.embedding.providers.bedrock_provider import (
                BedrockEmbeddingProvider,
            )

            config = EmbeddingConfig(
                provider="bedrock",
                model="amazon.titan-embed-text-v2:0",
            )
            provider = BedrockEmbeddingProvider(config)

        assert provider._dimension is None

        provider._client.invoke_model.return_value = _make_bedrock_response(
            {"embedding": [0.1, 0.2, 0.3], "inputTextTokenCount": 1}
        )

        provider.encode(["test"], normalize_embeddings=False)
        assert provider._dimension == 3
        assert provider.dimension == 3

    def test_dimension_probes_if_unknown(self):
        """dimension property triggers probe encode if not yet known."""
        mock_boto3 = MagicMock()
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            from portiere.embedding.providers.bedrock_provider import (
                BedrockEmbeddingProvider,
            )

            config = EmbeddingConfig(
                provider="bedrock",
                model="amazon.titan-embed-text-v2:0",
            )
            provider = BedrockEmbeddingProvider(config)

        provider._client.invoke_model.return_value = _make_bedrock_response(
            {"embedding": [0.1] * 1024, "inputTextTokenCount": 1}
        )

        assert provider.dimension == 1024
        provider._client.invoke_model.assert_called_once()


class TestBedrockGatewayRouting:
    """Test that EmbeddingGateway routes to BedrockEmbeddingProvider."""

    def test_bedrock_provider_routing(self):
        """Bedrock config creates BedrockEmbeddingProvider."""
        mock_boto3 = MagicMock()
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            from portiere.embedding.gateway import EmbeddingGateway
            from portiere.embedding.providers.bedrock_provider import (
                BedrockEmbeddingProvider,
            )

            config = EmbeddingConfig(
                provider="bedrock",
                model="amazon.titan-embed-text-v2:0",
            )
            gateway = EmbeddingGateway(config)
            assert isinstance(gateway._provider, BedrockEmbeddingProvider)

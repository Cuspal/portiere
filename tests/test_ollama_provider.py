"""Tests for Ollama provider."""

from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest

from portiere.config import LLMConfig
from portiere.llm.providers.ollama_provider import OllamaProvider


class TestOllamaProvider:
    """Tests for Ollama provider."""

    def test_init_sets_default_endpoint(self):
        """Ollama uses localhost:11434 as default."""
        config = LLMConfig(provider="ollama", model="llama3:70b")
        provider = OllamaProvider(config)

        assert provider.endpoint == "http://localhost:11434"

    def test_init_uses_custom_endpoint(self):
        """Ollama uses custom endpoint from config."""
        config = LLMConfig(
            provider="ollama",
            endpoint="http://remote-ollama:8080",
            model="llama3:70b",
        )
        provider = OllamaProvider(config)

        assert provider.endpoint == "http://remote-ollama:8080"

    @pytest.mark.asyncio
    async def test_complete_basic(self):
        """Complete returns text from Ollama."""
        config = LLMConfig(provider="ollama", model="llama3:70b")
        provider = OllamaProvider(config)

        mock_response = Mock()
        mock_response.json.return_value = {"response": "Test response from Ollama"}

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            result = await provider.complete("What is SNOMED CT?")

        assert result == "Test response from Ollama"
        mock_client.post.assert_called_once()

        # Verify request structure
        call_args = mock_client.post.call_args
        assert call_args.args[0] == "http://localhost:11434/api/generate"
        request_body = call_args.kwargs["json"]
        assert request_body["model"] == "llama3:70b"
        assert request_body["stream"] is False

    @pytest.mark.asyncio
    async def test_complete_with_system_prompt(self):
        """System prompt is combined with user prompt."""
        config = LLMConfig(provider="ollama", model="llama3:70b")
        provider = OllamaProvider(config)

        mock_response = Mock()
        mock_response.json.return_value = {"response": "Response with system"}

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            result = await provider.complete(
                "What is SNOMED CT?",
                system="You are a medical expert.",
            )

        # Verify system prompt is prepended
        call_args = mock_client.post.call_args
        request_body = call_args.kwargs["json"]
        prompt = request_body["prompt"]
        assert "medical expert" in prompt
        assert "SNOMED CT" in prompt

    @pytest.mark.asyncio
    async def test_complete_with_json_mode(self):
        """JSON mode adds instruction to prompt."""
        config = LLMConfig(provider="ollama", model="llama3:70b")
        provider = OllamaProvider(config)

        mock_response = Mock()
        mock_response.json.return_value = {"response": '{"result": "json"}'}

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            result = await provider.complete("Test", json_mode=True)

        # Verify JSON instruction is added
        call_args = mock_client.post.call_args
        request_body = call_args.kwargs["json"]
        prompt = request_body["prompt"]
        assert "JSON" in prompt

    @pytest.mark.asyncio
    async def test_complete_with_custom_parameters(self):
        """Custom max_tokens and temperature are passed to API."""
        config = LLMConfig(provider="ollama", model="llama3:70b")
        provider = OllamaProvider(config)

        mock_response = Mock()
        mock_response.json.return_value = {"response": "Response"}

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            result = await provider.complete(
                "Test",
                max_tokens=2000,
                temperature=0.7,
            )

        # Verify options
        call_args = mock_client.post.call_args
        request_body = call_args.kwargs["json"]
        options = request_body["options"]
        assert options["num_predict"] == 2000
        assert options["temperature"] == 0.7

    @pytest.mark.asyncio
    async def test_complete_connection_error(self):
        """Connection error raises helpful message."""
        config = LLMConfig(provider="ollama", model="llama3:70b")
        provider = OllamaProvider(config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post.side_effect = httpx.ConnectError("Connection refused")
            mock_client_class.return_value.__aenter__.return_value = mock_client

            with pytest.raises(ConnectionError, match="ollama serve"):
                await provider.complete("Test")

    @pytest.mark.asyncio
    async def test_complete_http_error(self):
        """HTTP errors are wrapped with context."""
        config = LLMConfig(provider="ollama", model="llama3:70b")
        provider = OllamaProvider(config)

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Model not found",
            request=Mock(),
            response=Mock(json=lambda: {"error": "model 'llama3:70b' not found"}),
        )

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            with pytest.raises(RuntimeError, match="model 'llama3:70b' not found"):
                await provider.complete("Test")

    @pytest.mark.asyncio
    async def test_complete_uses_long_timeout(self):
        """Ollama uses 120s timeout for local inference."""
        config = LLMConfig(provider="ollama", model="llama3:70b")
        provider = OllamaProvider(config)

        mock_response = Mock()
        mock_response.json.return_value = {"response": "Response"}

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            result = await provider.complete("Test")

        # Verify timeout
        call_args = mock_client.post.call_args
        assert call_args.kwargs["timeout"] == 120.0

    @pytest.mark.asyncio
    async def test_embed_returns_vectors(self):
        """Embed returns embedding vectors."""
        config = LLMConfig(provider="ollama", model="llama3:70b")
        provider = OllamaProvider(config)

        mock_response_1 = Mock()
        mock_response_1.json.return_value = {"embedding": [0.1, 0.2, 0.3]}
        mock_response_2 = Mock()
        mock_response_2.json.return_value = {"embedding": [0.4, 0.5, 0.6]}

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post.side_effect = [mock_response_1, mock_response_2]
            mock_client_class.return_value.__aenter__.return_value = mock_client

            result = await provider.embed(["text1", "text2"])

        assert len(result) == 2
        assert result[0] == [0.1, 0.2, 0.3]
        assert result[1] == [0.4, 0.5, 0.6]

        # Verify endpoint
        calls = mock_client.post.call_args_list
        assert len(calls) == 2
        assert "/api/embeddings" in calls[0].args[0]
        assert "/api/embeddings" in calls[1].args[0]

    @pytest.mark.asyncio
    async def test_embed_connection_error(self):
        """Embed connection error raises helpful message."""
        config = LLMConfig(provider="ollama", model="llama3:70b")
        provider = OllamaProvider(config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post.side_effect = httpx.ConnectError("Connection refused")
            mock_client_class.return_value.__aenter__.return_value = mock_client

            with pytest.raises(ConnectionError, match="ollama serve"):
                await provider.embed(["test"])

    @pytest.mark.asyncio
    async def test_embed_http_error(self):
        """Embed HTTP errors are wrapped with context."""
        config = LLMConfig(provider="ollama", model="llama3:70b")
        provider = OllamaProvider(config)

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Model error",
            request=Mock(),
            response=Mock(json=lambda: {"error": "embedding failed"}),
        )

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            with pytest.raises(RuntimeError, match="embedding failed"):
                await provider.embed(["test"])

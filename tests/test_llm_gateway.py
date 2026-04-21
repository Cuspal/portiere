"""
Tests for LLM Gateway.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from portiere.config import LLMConfig
from portiere.llm.gateway import LLMGateway


class TestLLMGatewayInit:
    """Tests for LLMGateway initialization."""

    def test_init_with_none_provider_raises(self):
        """Gateway raises ValueError for 'none' provider."""
        config = LLMConfig(provider="none")
        with pytest.raises(ValueError, match="LLM provider is 'none'"):
            LLMGateway(config)

    def test_init_with_openai_provider(self):
        """Gateway initializes with OpenAI provider."""
        config = LLMConfig(provider="openai", api_key="sk-test")

        with patch("portiere.llm.gateway.LLMGateway._create_provider") as mock_create:
            mock_create.return_value = Mock()
            gateway = LLMGateway(config)

        assert gateway.config.provider == "openai"

    def test_init_with_bedrock_provider(self):
        """Gateway initializes with Bedrock provider."""
        config = LLMConfig(
            provider="bedrock",
            model="anthropic.claude-3-5-sonnet-20241022-v2:0",
            aws_region="us-east-1",
        )

        with patch("portiere.llm.gateway.LLMGateway._create_provider") as mock_create:
            mock_create.return_value = Mock()
            gateway = LLMGateway(config)

        assert gateway.config.provider == "bedrock"

    def test_init_with_ollama_provider(self):
        """Gateway initializes with Ollama provider."""
        config = LLMConfig(
            provider="ollama",
            endpoint="http://localhost:11434",
            model="llama3:70b",
        )

        with patch("portiere.llm.gateway.LLMGateway._create_provider") as mock_create:
            mock_create.return_value = Mock()
            gateway = LLMGateway(config)

        assert gateway.config.provider == "ollama"

    def test_unsupported_provider_raises(self):
        """Unsupported provider raises ValidationError or ValueError."""
        from pydantic import ValidationError

        with pytest.raises((ValueError, ValidationError)):
            LLMConfig(provider="unsupported", api_key="test")


class TestLLMGatewayComplete:
    """Tests for completion methods."""

    @pytest.mark.asyncio
    async def test_complete_basic(self):
        """Complete returns text response."""
        config = LLMConfig(provider="openai", api_key="sk-test")
        mock_provider = AsyncMock()
        mock_provider.complete.return_value = "Test response"

        with patch("portiere.llm.gateway.LLMGateway._create_provider") as mock_create:
            mock_create.return_value = mock_provider
            gateway = LLMGateway(config)

            result = await gateway.complete("Test prompt")

        assert result == "Test response"
        mock_provider.complete.assert_called_once()

    @pytest.mark.asyncio
    async def test_complete_with_system_prompt(self):
        """Complete passes system prompt to provider."""
        config = LLMConfig(provider="openai", api_key="sk-test")
        mock_provider = AsyncMock()
        mock_provider.complete.return_value = "Response"

        with patch("portiere.llm.gateway.LLMGateway._create_provider") as mock_create:
            mock_create.return_value = mock_provider
            gateway = LLMGateway(config)

            await gateway.complete(
                "User prompt",
                system="You are a clinical expert.",
            )

        call_kwargs = mock_provider.complete.call_args.kwargs
        assert call_kwargs["system"] == "You are a clinical expert."

    @pytest.mark.asyncio
    async def test_complete_structured_returns_dict(self):
        """Complete structured returns parsed JSON."""
        config = LLMConfig(provider="openai", api_key="sk-test")
        mock_provider = AsyncMock()
        mock_provider.complete.return_value = '{"concept_id": 12345, "confidence": 0.95}'

        with patch("portiere.llm.gateway.LLMGateway._create_provider") as mock_create:
            mock_create.return_value = mock_provider
            gateway = LLMGateway(config)

            result = await gateway.complete_structured(
                "Map this term",
                schema={"type": "object"},
            )

        assert isinstance(result, dict)
        assert result["concept_id"] == 12345
        assert result["confidence"] == 0.95


class TestLLMGatewayEmbed:
    """Tests for embedding methods."""

    @pytest.mark.asyncio
    async def test_embed_returns_vectors(self):
        """Embed returns list of vectors."""
        config = LLMConfig(provider="openai", api_key="sk-test")
        mock_provider = AsyncMock()
        mock_provider.embed.return_value = [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]

        with patch("portiere.llm.gateway.LLMGateway._create_provider") as mock_create:
            mock_create.return_value = mock_provider
            gateway = LLMGateway(config)

            result = await gateway.embed(["text1", "text2"])

        assert len(result) == 2
        assert len(result[0]) == 3
        mock_provider.embed.assert_called_once_with(["text1", "text2"])

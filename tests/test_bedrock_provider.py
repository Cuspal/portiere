"""Tests for AWS Bedrock provider."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from portiere.config import LLMConfig
from portiere.llm.providers.bedrock_provider import BedrockProvider


class TestBedrockProvider:
    """Tests for Bedrock provider."""

    def test_init_requires_aioboto3(self):
        """Bedrock provider requires aioboto3."""
        config = LLMConfig(
            provider="bedrock",
            model="anthropic.claude-3-5-sonnet-20241022-v2:0",
        )

        with patch.dict("sys.modules", {"aioboto3": None}):
            with pytest.raises(ImportError, match="aioboto3 is required"):
                BedrockProvider(config)

    def test_init_sets_default_region(self):
        """Bedrock uses us-east-1 as default region."""
        config = LLMConfig(
            provider="bedrock",
            model="anthropic.claude-3-5-sonnet-20241022-v2:0",
        )

        mock_aioboto3 = MagicMock()
        with patch.dict("sys.modules", {"aioboto3": mock_aioboto3}):
            provider = BedrockProvider(config)
            assert provider.region == "us-east-1"

    def test_init_uses_custom_region(self):
        """Bedrock uses custom aws_region from config."""
        config = LLMConfig(
            provider="bedrock",
            model="anthropic.claude-3-5-sonnet-20241022-v2:0",
            aws_region="us-west-2",
        )

        mock_aioboto3 = MagicMock()
        with patch.dict("sys.modules", {"aioboto3": mock_aioboto3}):
            provider = BedrockProvider(config)
            assert provider.region == "us-west-2"

    @pytest.mark.asyncio
    async def test_complete_basic(self):
        """Complete returns text from Bedrock."""
        config = LLMConfig(
            provider="bedrock",
            model="anthropic.claude-3-5-sonnet-20241022-v2:0",
        )

        # Mock aioboto3 session and client
        mock_bedrock_client = AsyncMock()
        mock_bedrock_client.converse.return_value = {
            "output": {"message": {"content": [{"text": "Test response from Bedrock"}]}}
        }

        mock_session = MagicMock()
        mock_session.client.return_value.__aenter__.return_value = mock_bedrock_client

        mock_aioboto3 = MagicMock()
        mock_aioboto3.Session.return_value = mock_session

        with patch.dict("sys.modules", {"aioboto3": mock_aioboto3}):
            provider = BedrockProvider(config)
            result = await provider.complete("What is SNOMED CT?")

        assert result == "Test response from Bedrock"
        mock_bedrock_client.converse.assert_called_once()

        # Verify request structure
        call_kwargs = mock_bedrock_client.converse.call_args.kwargs
        assert call_kwargs["modelId"] == "anthropic.claude-3-5-sonnet-20241022-v2:0"
        assert len(call_kwargs["messages"]) == 1
        assert call_kwargs["messages"][0]["role"] == "user"

    @pytest.mark.asyncio
    async def test_complete_with_system_prompt(self):
        """System prompt is included in request."""
        config = LLMConfig(
            provider="bedrock",
            model="anthropic.claude-3-5-sonnet-20241022-v2:0",
        )

        mock_bedrock_client = AsyncMock()
        mock_bedrock_client.converse.return_value = {
            "output": {"message": {"content": [{"text": "Response with system"}]}}
        }

        mock_session = MagicMock()
        mock_session.client.return_value.__aenter__.return_value = mock_bedrock_client

        mock_aioboto3 = MagicMock()
        mock_aioboto3.Session.return_value = mock_session

        with patch.dict("sys.modules", {"aioboto3": mock_aioboto3}):
            provider = BedrockProvider(config)
            result = await provider.complete(
                "Test",
                system="You are a medical expert.",
            )

        # Check that system prompt is included
        call_kwargs = mock_bedrock_client.converse.call_args.kwargs
        assert "system" in call_kwargs
        system_prompts = call_kwargs["system"]
        assert len(system_prompts) == 1
        assert "medical expert" in system_prompts[0]["text"]

    @pytest.mark.asyncio
    async def test_complete_with_json_mode(self):
        """JSON mode adds instruction to system prompt."""
        config = LLMConfig(
            provider="bedrock",
            model="anthropic.claude-3-5-sonnet-20241022-v2:0",
        )

        mock_bedrock_client = AsyncMock()
        mock_bedrock_client.converse.return_value = {
            "output": {"message": {"content": [{"text": '{"result": "json"}'}]}}
        }

        mock_session = MagicMock()
        mock_session.client.return_value.__aenter__.return_value = mock_bedrock_client

        mock_aioboto3 = MagicMock()
        mock_aioboto3.Session.return_value = mock_session

        with patch.dict("sys.modules", {"aioboto3": mock_aioboto3}):
            provider = BedrockProvider(config)
            result = await provider.complete("Test", json_mode=True)

        # Check that system prompt includes JSON instruction
        call_kwargs = mock_bedrock_client.converse.call_args.kwargs
        assert "system" in call_kwargs
        system_prompts = call_kwargs["system"]
        assert any("JSON" in prompt["text"] for prompt in system_prompts)

    @pytest.mark.asyncio
    async def test_complete_with_custom_parameters(self):
        """Custom max_tokens and temperature are passed to API."""
        config = LLMConfig(
            provider="bedrock",
            model="anthropic.claude-3-5-sonnet-20241022-v2:0",
        )

        mock_bedrock_client = AsyncMock()
        mock_bedrock_client.converse.return_value = {
            "output": {"message": {"content": [{"text": "Response"}]}}
        }

        mock_session = MagicMock()
        mock_session.client.return_value.__aenter__.return_value = mock_bedrock_client

        mock_aioboto3 = MagicMock()
        mock_aioboto3.Session.return_value = mock_session

        with patch.dict("sys.modules", {"aioboto3": mock_aioboto3}):
            provider = BedrockProvider(config)
            result = await provider.complete(
                "Test",
                max_tokens=2000,
                temperature=0.7,
            )

        # Verify inference config
        call_kwargs = mock_bedrock_client.converse.call_args.kwargs
        inference_config = call_kwargs["inferenceConfig"]
        assert inference_config["maxTokens"] == 2000
        assert inference_config["temperature"] == 0.7

    @pytest.mark.asyncio
    async def test_complete_error_handling(self):
        """Bedrock errors are wrapped with context."""
        config = LLMConfig(
            provider="bedrock",
            model="anthropic.claude-3-5-sonnet-20241022-v2:0",
        )

        mock_bedrock_client = AsyncMock()
        mock_bedrock_client.converse.side_effect = Exception("Bedrock API error")

        mock_session = MagicMock()
        mock_session.client.return_value.__aenter__.return_value = mock_bedrock_client

        mock_aioboto3 = MagicMock()
        mock_aioboto3.Session.return_value = mock_session

        with patch.dict("sys.modules", {"aioboto3": mock_aioboto3}):
            provider = BedrockProvider(config)

            with pytest.raises(RuntimeError, match="Bedrock API error"):
                await provider.complete("Test")

    @pytest.mark.asyncio
    async def test_embed_not_implemented(self):
        """Embeddings raise NotImplementedError."""
        config = LLMConfig(
            provider="bedrock",
            model="anthropic.claude-3-5-sonnet-20241022-v2:0",
        )

        mock_aioboto3 = MagicMock()
        with patch.dict("sys.modules", {"aioboto3": mock_aioboto3}):
            provider = BedrockProvider(config)

            with pytest.raises(NotImplementedError, match="Bedrock embeddings"):
                await provider.embed(["test"])

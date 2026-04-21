"""
Portiere LLM Gateway — Routes to appropriate LLM provider.

Supports BYO-LLM: customer brings their own LLM endpoint
(Azure OpenAI, Anthropic, Bedrock, Ollama).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from portiere.config import LLMConfig

logger = structlog.get_logger(__name__)


class LLMGateway:
    """
    LLM Gateway for routing calls to different providers.

    Example:
        from portiere import LLMConfig
        from portiere.llm import LLMGateway

        config = LLMConfig(provider="openai", api_key="sk-...")
        gateway = LLMGateway(config)

        response = await gateway.complete("What is SNOMED CT?")
    """

    def __init__(self, config: LLMConfig) -> None:
        """
        Initialize LLM gateway.

        Args:
            config: LLM configuration
        """
        self.config = config
        self._provider = self._create_provider()
        logger.info(
            "LLM gateway initialized",
            provider=config.provider,
            model=config.model,
        )

    def _create_provider(self):
        """Create the appropriate provider based on config."""
        if self.config.provider == "none":
            raise ValueError(
                "LLM provider is 'none'. Set pipeline='cloud' to use Portiere API, "
                "or configure a local LLM provider (openai, anthropic, ollama, etc.)."
            )
        elif self.config.provider == "openai":
            from portiere.llm.providers.openai_provider import OpenAIProvider

            return OpenAIProvider(self.config)
        elif self.config.provider == "azure_openai":
            from portiere.llm.providers.openai_provider import AzureOpenAIProvider

            return AzureOpenAIProvider(self.config)
        elif self.config.provider == "anthropic":
            from portiere.llm.providers.anthropic_provider import AnthropicProvider

            return AnthropicProvider(self.config)
        elif self.config.provider == "bedrock":
            from portiere.llm.providers.bedrock_provider import BedrockProvider

            return BedrockProvider(self.config)
        elif self.config.provider == "ollama":
            from portiere.llm.providers.ollama_provider import OllamaProvider

            return OllamaProvider(self.config)
        else:
            raise ValueError(f"Unsupported LLM provider: {self.config.provider}")

    async def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
        json_mode: bool = False,
    ) -> str:
        """
        Generate a completion.

        Args:
            prompt: User prompt
            system: Optional system prompt
            max_tokens: Maximum tokens in response
            temperature: Sampling temperature
            json_mode: If True, request JSON output

        Returns:
            Generated text
        """
        return await self._provider.complete(
            prompt=prompt,
            system=system,
            max_tokens=max_tokens or self.config.max_tokens,
            temperature=temperature or self.config.temperature,
            json_mode=json_mode,
        )

    async def complete_structured(
        self,
        prompt: str,
        schema: dict[str, Any],
        *,
        system: str | None = None,
    ) -> dict[str, Any]:
        """
        Generate structured output matching a schema.

        Args:
            prompt: User prompt
            schema: JSON schema for response
            system: Optional system prompt

        Returns:
            Parsed JSON matching schema
        """
        import json

        response = await self.complete(
            prompt=prompt,
            system=system,
            json_mode=True,
        )
        return json.loads(response)

    async def embed(self, texts: list[str]) -> list[list[float]]:
        """
        Generate embeddings for texts.

        Args:
            texts: List of texts to embed

        Returns:
            List of embedding vectors
        """
        return await self._provider.embed(texts)

"""Anthropic Claude provider."""

from portiere.llm.providers.base import BaseLLMProvider


class AnthropicProvider(BaseLLMProvider):
    """Anthropic Claude API provider."""

    def __init__(self, config):
        super().__init__(config)
        try:
            import anthropic

            self._client = anthropic.AsyncAnthropic(api_key=config.api_key)
        except ImportError:
            raise ImportError(
                "Anthropic is required for this provider. "
                "Install with: pip install portiere[anthropic]"
            )

    async def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        max_tokens: int = 1000,
        temperature: float = 0.0,
        json_mode: bool = False,
    ) -> str:
        """Generate completion via Anthropic API."""
        kwargs = {
            "model": self.config.model or "claude-3-5-sonnet-20241022",
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }

        if system:
            kwargs["system"] = system

        if json_mode:
            # Claude doesn't have native JSON mode, so we add to system prompt
            json_instruction = "\n\nRespond only with valid JSON, no additional text."
            kwargs["system"] = (system or "") + json_instruction

        response = await self._client.messages.create(**kwargs)  # type: ignore[call-overload]
        return response.content[0].text

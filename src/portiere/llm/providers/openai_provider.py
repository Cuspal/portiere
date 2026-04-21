"""OpenAI and Azure OpenAI providers."""

from portiere.llm.providers.base import BaseLLMProvider


class OpenAIProvider(BaseLLMProvider):
    """OpenAI API provider."""

    def __init__(self, config):
        super().__init__(config)
        try:
            import openai

            self._client = openai.AsyncOpenAI(api_key=config.api_key)
        except ImportError:
            raise ImportError(
                "OpenAI is required for this provider. Install with: pip install portiere-health[openai]"
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
        """Generate completion via OpenAI API."""
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        kwargs = {
            "model": self.config.model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        response = await self._client.chat.completions.create(**kwargs)  # type: ignore[call-overload]
        return response.choices[0].message.content

    async def embed(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings via OpenAI API."""
        response = await self._client.embeddings.create(
            model="text-embedding-3-small",
            input=texts,
        )
        return [item.embedding for item in response.data]


class AzureOpenAIProvider(BaseLLMProvider):
    """Azure OpenAI Service provider."""

    def __init__(self, config):
        super().__init__(config)
        try:
            import openai

            self._client = openai.AsyncAzureOpenAI(
                api_key=config.api_key,
                api_version="2024-02-01",
                azure_endpoint=config.endpoint,
            )
        except ImportError:
            raise ImportError(
                "OpenAI is required for this provider. Install with: pip install portiere-health[openai]"
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
        """Generate completion via Azure OpenAI."""
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        kwargs = {
            "model": self.config.model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        response = await self._client.chat.completions.create(**kwargs)  # type: ignore[call-overload]
        return response.choices[0].message.content

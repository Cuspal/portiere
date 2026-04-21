"""AWS Bedrock provider using Claude models via Converse API."""

from typing import TYPE_CHECKING

from portiere.llm.providers.base import BaseLLMProvider

if TYPE_CHECKING:
    from portiere.config import LLMConfig


class BedrockProvider(BaseLLMProvider):
    """
    AWS Bedrock provider for Claude models.

    Uses the Converse API for a unified interface across Bedrock models.
    Supports IAM authentication via standard AWS credential chain.

    Configuration:
        provider: "bedrock"
        model: "anthropic.claude-3-5-sonnet-20241022-v2:0"
        aws_region: "us-east-1"  # Optional, defaults to AWS_DEFAULT_REGION

    Credentials are loaded from:
    1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    2. ~/.aws/credentials profile
    3. IAM role (if running on EC2/ECS/Lambda)

    Example:
        from portiere import LLMConfig
        from portiere.llm.providers.bedrock_provider import BedrockProvider

        config = LLMConfig(
            provider="bedrock",
            model="anthropic.claude-3-5-sonnet-20241022-v2:0",
            aws_region="us-west-2",
        )
        provider = BedrockProvider(config)
        response = await provider.complete("What is SNOMED CT?")
    """

    def __init__(self, config: "LLMConfig") -> None:
        super().__init__(config)
        try:
            import aioboto3

            self._aioboto3 = aioboto3
        except ImportError:
            raise ImportError(
                "aioboto3 is required for Bedrock provider. "
                "Install with: pip install portiere[bedrock]"
            )

        # Get region from config or fallback to AWS environment
        self.region = getattr(config, "aws_region", None) or "us-east-1"

    async def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        max_tokens: int = 1000,
        temperature: float = 0.0,
        json_mode: bool = False,
    ) -> str:
        """
        Generate completion via AWS Bedrock Converse API.

        Args:
            prompt: User prompt
            system: Optional system prompt
            max_tokens: Maximum tokens in response
            temperature: Sampling temperature
            json_mode: If True, request JSON output

        Returns:
            Generated text

        Raises:
            RuntimeError: If Bedrock API call fails
        """
        # Build messages
        messages = [{"role": "user", "content": [{"text": prompt}]}]

        # Build system prompt (with JSON instruction if needed)
        system_prompts = []
        if system:
            system_prompts.append({"text": system})
        if json_mode:
            system_prompts.append({"text": "\n\nRespond only with valid JSON, no additional text."})

        # Build request payload
        request_body = {
            "modelId": self.config.model,
            "messages": messages,
            "inferenceConfig": {
                "maxTokens": max_tokens,
                "temperature": temperature,
            },
        }

        if system_prompts:
            request_body["system"] = system_prompts

        # Call Bedrock Converse API
        session = self._aioboto3.Session()
        async with session.client(
            "bedrock-runtime",
            region_name=self.region,
        ) as bedrock:
            try:
                response = await bedrock.converse(**request_body)

                # Extract text from response
                output = response["output"]["message"]["content"][0]["text"]
                return output

            except Exception as e:
                # Wrap Bedrock errors with helpful context
                raise RuntimeError(
                    f"Bedrock API error (model={self.config.model}, region={self.region}): {e!s}"
                ) from e

    async def embed(self, texts: list[str]) -> list[list[float]]:
        """
        Not implemented on the LLM provider.

        Use ``EmbeddingConfig(provider='bedrock')`` with the embedding
        module instead — see ``portiere.embedding.providers.bedrock_provider``.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "Use EmbeddingConfig(provider='bedrock') for Bedrock embeddings. "
            "See portiere.embedding.providers.bedrock_provider."
        )

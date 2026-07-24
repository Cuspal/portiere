"""Tests for egress classification — which providers would send data off-machine."""

from portiere.egress import (
    egress_violations,
    endpoint_is_remote,
    provider_is_remote,
)


class TestProviderClassification:
    def test_remote_llm_providers(self):
        for p in ("openai", "azure_openai", "anthropic", "bedrock"):
            assert provider_is_remote("llm", p) is True

    def test_local_llm_providers(self):
        for p in ("ollama", "none"):
            assert provider_is_remote("llm", p) is False

    def test_remote_embedding_providers(self):
        for p in ("openai", "bedrock"):
            assert provider_is_remote("embedding", p) is True

    def test_local_embedding_providers(self):
        for p in ("huggingface", "ollama", "none"):
            assert provider_is_remote("embedding", p) is False

    def test_reranker_providers_are_local(self):
        for p in ("huggingface", "none"):
            assert provider_is_remote("reranker", p) is False


class TestEndpointClassification:
    def test_none_and_localhost_are_local(self):
        assert endpoint_is_remote(None) is False
        assert endpoint_is_remote("http://localhost:11434") is False
        assert endpoint_is_remote("http://127.0.0.1:8080/v1") is False
        assert endpoint_is_remote("http://[::1]:9000") is False

    def test_external_host_is_remote(self):
        assert endpoint_is_remote("https://api.openai.com/v1") is True
        assert endpoint_is_remote("https://my-hf-endpoint.aws.cloud") is True


class TestEgressViolations:
    def test_default_local_stack_has_no_violations(self):
        from portiere.config import EmbeddingConfig, LLMConfig, RerankerConfig

        class Cfg:
            llm = LLMConfig()  # provider="none"
            embedding = EmbeddingConfig()  # huggingface
            reranker = RerankerConfig()  # huggingface

        assert egress_violations(Cfg()) == []

    def test_remote_llm_flagged(self):
        from portiere.config import EmbeddingConfig, LLMConfig, RerankerConfig

        class Cfg:
            llm = LLMConfig(provider="anthropic")
            embedding = EmbeddingConfig()
            reranker = RerankerConfig()

        violations = egress_violations(Cfg())
        assert len(violations) == 1
        assert "llm" in violations[0] and "anthropic" in violations[0]

    def test_multiple_violations_all_listed(self):
        from portiere.config import EmbeddingConfig, LLMConfig, RerankerConfig

        class Cfg:
            llm = LLMConfig(provider="openai")
            embedding = EmbeddingConfig(provider="bedrock")
            reranker = RerankerConfig(endpoint="https://remote.example.com")

        violations = egress_violations(Cfg())
        assert len(violations) == 3

    def test_local_ollama_endpoint_not_flagged(self):
        from portiere.config import EmbeddingConfig, LLMConfig, RerankerConfig

        class Cfg:
            llm = LLMConfig(provider="ollama", endpoint="http://localhost:11434")
            embedding = EmbeddingConfig()
            reranker = RerankerConfig()

        assert egress_violations(Cfg()) == []


class TestEndpointSpoofing:
    """Regression: substring matching was spoofable (v0.4.0 review finding)."""

    def test_localhost_subdomain_is_remote(self):
        assert endpoint_is_remote("https://localhost.evil.example.com/v1") is True

    def test_marker_in_path_or_query_is_remote(self):
        assert endpoint_is_remote("https://evil.example.com/v1?x=localhost") is True
        assert endpoint_is_remote("https://evil.example.com/localhost/api") is True
        assert endpoint_is_remote("https://127.0.0.1.evil.com/") is True

    def test_dot_localhost_tld_is_local(self):
        # RFC 6761: *.localhost resolves to loopback
        assert endpoint_is_remote("http://myserver.localhost:8080") is False

    def test_bare_host_port(self):
        assert endpoint_is_remote("localhost:11434") is False
        assert endpoint_is_remote("evil.example.com:11434") is True

    def test_mongodb_srv_always_remote(self):
        assert endpoint_is_remote("mongodb+srv://cluster0.abc.mongodb.net/db") is True

    def test_unparseable_is_remote(self):
        assert endpoint_is_remote("http://[malformed") is True


class TestKnowledgeLayerEgress:
    """Regression: knowledge backends could egress while doctor said LOCAL."""

    def _cfg(self, **kl_kwargs):
        from portiere.config import (
            EmbeddingConfig,
            KnowledgeLayerConfig,
            LLMConfig,
            RerankerConfig,
        )

        class Cfg:
            llm = LLMConfig()
            embedding = EmbeddingConfig()
            reranker = RerankerConfig()
            knowledge_layer = KnowledgeLayerConfig(**kl_kwargs)

        return Cfg()

    def test_remote_qdrant_flagged(self):
        v = egress_violations(self._cfg(backend="qdrant", qdrant_url="https://xyz.cloud.qdrant.io"))
        assert len(v) == 1 and "qdrant_url" in v[0]

    def test_remote_elasticsearch_flagged(self):
        v = egress_violations(
            self._cfg(backend="elasticsearch", elasticsearch_url="https://es.internal.corp:9200")
        )
        assert len(v) == 1

    def test_mongodb_atlas_flagged(self):
        v = egress_violations(
            self._cfg(
                backend="mongodb",
                mongodb_connection_string="mongodb+srv://u:p@c0.mongodb.net/db",
            )
        )
        assert len(v) == 1

    def test_local_bm25s_clean(self):
        assert egress_violations(self._cfg(backend="bm25s")) == []

    def test_local_pgvector_clean(self):
        v = egress_violations(
            self._cfg(
                backend="pgvector",
                pgvector_connection_string="postgresql://u:p@localhost:5432/db",
            )
        )
        assert v == []


class TestLocalEndpointOverride:
    """embedding provider=openai + loopback base_url is a local vLLM-style server."""

    def test_embedding_openai_localhost_is_local(self):
        from portiere.config import EmbeddingConfig, LLMConfig, RerankerConfig

        class Cfg:
            llm = LLMConfig()
            embedding = EmbeddingConfig(provider="openai", endpoint="http://localhost:8000/v1")
            reranker = RerankerConfig()

        assert egress_violations(Cfg()) == []

    def test_llm_openai_localhost_still_remote(self):
        """The LLM openai provider ignores `endpoint` — always talks to
        api.openai.com — so a loopback endpoint must NOT excuse it."""
        from portiere.config import EmbeddingConfig, LLMConfig, RerankerConfig

        class Cfg:
            llm = LLMConfig(provider="openai", endpoint="http://localhost:8000/v1")
            embedding = EmbeddingConfig()
            reranker = RerankerConfig()

        v = egress_violations(Cfg())
        assert len(v) == 1 and "llm" in v[0]

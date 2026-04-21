"""
Tests for Portiere configuration.

Tests the configuration loading and validation for:
- PortiereConfig
- LLMConfig
- Environment variable parsing
"""

import os
from unittest.mock import patch


class TestLLMConfig:
    """Tests for LLMConfig."""

    def test_openai_config(self):
        """Test OpenAI LLM configuration."""
        from portiere.config import LLMConfig

        config = LLMConfig(
            provider="openai",
            model="gpt-4-turbo",
            api_key="sk-test-key",
        )

        assert config.provider == "openai"
        assert config.model == "gpt-4-turbo"
        assert config.api_key == "sk-test-key"

    def test_anthropic_config(self):
        """Test Anthropic LLM configuration."""
        from portiere.config import LLMConfig

        config = LLMConfig(
            provider="anthropic",
            model="claude-3-5-sonnet-latest",
            api_key="anthropic-key",
        )

        assert config.provider == "anthropic"
        assert config.model == "claude-3-5-sonnet-latest"

    def test_bedrock_config(self):
        """Test AWS Bedrock configuration."""
        from portiere.config import LLMConfig

        config = LLMConfig(
            provider="bedrock",
            model="anthropic.claude-3-sonnet-20240229-v1:0",
            aws_region="us-east-1",
        )

        assert config.provider == "bedrock"
        assert config.aws_region == "us-east-1"

    def test_config_with_custom_options(self):
        """Test LLM config with custom model options."""
        from portiere.config import LLMConfig

        config = LLMConfig(
            provider="openai",
            model="gpt-4",
            api_key="test-key",
            temperature=0.1,
            max_tokens=4096,
        )

        assert config.temperature == 0.1
        assert config.max_tokens == 4096


class TestPortiereConfig:
    """Tests for PortiereConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        from portiere.config import PortiereConfig

        config = PortiereConfig()

        # PortiereConfig has llm, engine, thresholds, artifacts
        assert config.llm is not None
        assert config.engine is not None
        assert config.thresholds is not None
        assert config.artifacts is not None

    def test_config_with_custom_endpoint(self):
        """Test configuration with custom endpoint."""
        from portiere.config import PortiereConfig

        config = PortiereConfig(endpoint="https://custom.api.portiere.io")

        assert config.endpoint == "https://custom.api.portiere.io"

    def test_config_with_llm(self):
        """Test configuration with LLM settings."""
        from portiere.config import LLMConfig, PortiereConfig

        llm = LLMConfig(
            provider="openai",
            model="gpt-4",
            api_key="test-key",
        )

        config = PortiereConfig(llm=llm)

        assert config.llm is not None
        assert config.llm.provider == "openai"

    def test_config_thresholds(self):
        """Test threshold configuration defaults."""
        from portiere.config import PortiereConfig

        config = PortiereConfig()

        assert config.thresholds.schema_mapping.auto_accept == 0.95
        assert config.thresholds.concept_mapping.auto_accept == 0.95
        assert config.thresholds.validation.min_completeness == 0.95

    def test_config_artifacts(self):
        """Test artifact configuration defaults."""
        from portiere.config import PortiereConfig

        config = PortiereConfig()

        assert config.artifacts.output_dir == "./artifacts"
        assert config.artifacts.engine_type == "spark"
        assert config.artifacts.include_tests is True


class TestEnvironmentConfig:
    """Tests for environment-based configuration."""

    @patch.dict(os.environ, {"PORTIERE_API_KEY": "env-api-key"})
    def test_api_key_from_env(self):
        """Test loading API key from environment."""
        from portiere.config import PortiereConfig

        config = PortiereConfig()
        # Note: Actual behavior depends on implementation
        # This test documents the expected interface

    @patch.dict(
        os.environ,
        {
            "PORTIERE_LLM_PROVIDER": "openai",
            "PORTIERE_LLM_MODEL": "gpt-4",
            "OPENAI_API_KEY": "env-openai-key",
        },
    )
    def test_llm_config_from_env(self):
        """Test loading LLM config from environment variables."""
        from portiere.config import PortiereConfig

        # Environment-based LLM configuration
        config = PortiereConfig()
        # This documents the expected environment variable pattern


class TestLocalModeConfig:
    """Tests for local mode configuration."""

    def test_default_mode_is_local(self):
        """Test that default mode is local."""
        from portiere.config import PortiereConfig

        config = PortiereConfig()
        assert config.mode == "local"

    def test_default_pipeline_is_local(self):
        """Test that default pipeline is local."""
        from portiere.config import PortiereConfig

        config = PortiereConfig()
        assert config.pipeline == "local"

    def test_pipeline_cloud_config(self):
        """Test cloud pipeline configuration."""
        from portiere.config import PortiereConfig

        config = PortiereConfig(pipeline="cloud", api_key="ptk_test")
        assert config.pipeline == "cloud"

    def test_quality_config_defaults(self):
        """Test quality config defaults."""
        from portiere.config import PortiereConfig

        config = PortiereConfig()
        assert config.quality is not None
        assert config.quality.enabled is True
        assert config.quality.profile_on_ingest is True
        assert config.quality.output_format == "json"

    def test_local_mode_config(self):
        """Test local mode configuration."""
        from portiere.config import PortiereConfig

        config = PortiereConfig(mode="local")
        assert config.mode == "local"
        assert config.local_project_dir is not None

    def test_hybrid_mode_config(self):
        """Test hybrid mode configuration."""
        from portiere.config import PortiereConfig

        config = PortiereConfig(mode="hybrid")
        assert config.mode == "hybrid"

    def test_custom_local_project_dir(self):
        """Test custom local project directory."""
        from pathlib import Path

        from portiere.config import PortiereConfig

        custom_dir = Path("/tmp/my_projects")
        config = PortiereConfig(mode="local", local_project_dir=custom_dir)
        assert config.local_project_dir == custom_dir

    def test_custom_embedding_model(self):
        """Test custom embedding model configuration."""
        from portiere.config import PortiereConfig

        config = PortiereConfig(
            mode="local", embedding_model="sentence-transformers/all-MiniLM-L6-v2"
        )
        assert config.embedding_model == "sentence-transformers/all-MiniLM-L6-v2"

    def test_default_embedding_model(self):
        """Test default embedding model."""
        from portiere.config import PortiereConfig

        config = PortiereConfig()
        assert config.embedding_model == "cambridgeltl/SapBERT-from-PubMedBERT-fulltext"

    def test_model_cache_dir(self):
        """Test model cache directory configuration."""
        from pathlib import Path

        from portiere.config import PortiereConfig

        config = PortiereConfig()
        assert config.model_cache_dir == Path.home() / ".portiere" / "models"


class TestKnowledgeLayerConfig:
    """Tests for knowledge layer configuration."""

    def test_default_backend_is_bm25s(self):
        """Test that default backend is BM25s."""
        from portiere.config import KnowledgeLayerConfig

        config = KnowledgeLayerConfig()
        assert config.backend == "bm25s"

    def test_bm25s_backend_config(self):
        """Test BM25s backend configuration."""
        from pathlib import Path

        from portiere.config import KnowledgeLayerConfig

        config = KnowledgeLayerConfig(
            backend="bm25s", bm25s_corpus_path=Path("./vocab/omop_concepts.json")
        )
        assert config.backend == "bm25s"
        assert config.bm25s_corpus_path == Path("./vocab/omop_concepts.json")

    def test_faiss_backend_config(self):
        """Test FAISS backend configuration."""
        from pathlib import Path

        from portiere.config import KnowledgeLayerConfig

        config = KnowledgeLayerConfig(
            backend="faiss",
            faiss_index_path=Path("./vocab/omop_faiss.index"),
            faiss_metadata_path=Path("./vocab/concept_metadata.json"),
        )
        assert config.backend == "faiss"
        assert config.faiss_index_path == Path("./vocab/omop_faiss.index")
        assert config.faiss_metadata_path == Path("./vocab/concept_metadata.json")

    def test_elasticsearch_backend_config(self):
        """Test Elasticsearch backend configuration."""
        from portiere.config import KnowledgeLayerConfig

        config = KnowledgeLayerConfig(
            backend="elasticsearch",
            elasticsearch_url="http://localhost:9200",
            elasticsearch_index="portiere_concepts",
        )
        assert config.backend == "elasticsearch"
        assert config.elasticsearch_url == "http://localhost:9200"
        assert config.elasticsearch_index == "portiere_concepts"

    def test_hybrid_backend_config(self):
        """Test hybrid backend configuration."""
        from pathlib import Path

        from portiere.config import KnowledgeLayerConfig

        config = KnowledgeLayerConfig(
            backend="hybrid",
            faiss_index_path=Path("./vocab/omop_faiss.index"),
            faiss_metadata_path=Path("./vocab/concept_metadata.json"),
            elasticsearch_url="http://localhost:9200",
            fusion_method="rrf",
            rrf_k=60,
        )
        assert config.backend == "hybrid"
        assert config.fusion_method == "rrf"
        assert config.rrf_k == 60

    def test_default_fusion_method(self):
        """Test default fusion method for hybrid backend."""
        from portiere.config import KnowledgeLayerConfig

        config = KnowledgeLayerConfig(backend="hybrid")
        assert config.fusion_method == "rrf"
        assert config.rrf_k == 60


class TestIntegratedLocalConfig:
    """Tests for integrated local mode configuration."""

    def test_full_local_mode_setup(self):
        """Test complete local mode configuration."""
        from pathlib import Path

        from portiere.config import KnowledgeLayerConfig, PortiereConfig

        config = PortiereConfig(
            mode="local",
            local_project_dir=Path("~/my_projects"),
            embedding_model="sentence-transformers/all-MiniLM-L6-v2",
            knowledge_layer=KnowledgeLayerConfig(
                backend="bm25s", bm25s_corpus_path=Path("./vocab/omop_concepts.json")
            ),
        )

        assert config.mode == "local"
        assert config.knowledge_layer is not None
        assert config.knowledge_layer.backend == "bm25s"
        assert config.embedding_model == "sentence-transformers/all-MiniLM-L6-v2"

    def test_hybrid_mode_with_faiss_backend(self):
        """Test hybrid mode with FAISS backend."""
        from pathlib import Path

        from portiere.config import KnowledgeLayerConfig, PortiereConfig

        config = PortiereConfig(
            mode="hybrid",
            api_key="pt_sk_test_key",
            knowledge_layer=KnowledgeLayerConfig(
                backend="faiss",
                faiss_index_path=Path("./vocab/custom_faiss.index"),
                faiss_metadata_path=Path("./vocab/metadata.json"),
            ),
        )

        assert config.mode == "hybrid"
        assert config.api_key == "pt_sk_test_key"
        assert config.knowledge_layer.backend == "faiss"

    def test_cloud_mode_without_knowledge_layer(self):
        """Test cloud mode doesn't require knowledge layer."""
        from portiere.config import PortiereConfig

        config = PortiereConfig(mode="cloud", api_key="pt_sk_test_key")

        assert config.mode == "cloud"
        assert config.knowledge_layer is None

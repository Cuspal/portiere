"""Tests for portiere CLI commands."""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from portiere.cli.models import models


@pytest.fixture
def runner():
    return CliRunner()


class TestModelsInfo:
    def test_info_shows_config(self, runner):
        result = runner.invoke(models, ["info"])
        assert result.exit_code == 0
        assert "Embedding provider:" in result.output
        assert "Reranker provider:" in result.output
        assert "Cache directory:" in result.output
        assert "Operating mode:" in result.output

    def test_info_shows_model_names(self, runner):
        result = runner.invoke(models, ["info"])
        assert result.exit_code == 0
        assert "huggingface" in result.output or "none" in result.output


class TestModelsList:
    def test_list_no_cache_dir(self, runner, tmp_path):
        nonexistent = tmp_path / "no_models"
        with patch("portiere.cli.models.PortiereConfig") as mock_config_cls:
            mock_config = MagicMock()
            mock_config.model_cache_dir = nonexistent
            mock_config_cls.return_value = mock_config
            result = runner.invoke(models, ["list"])
        assert result.exit_code == 0
        assert "No model cache directory found" in result.output

    def test_list_empty_cache(self, runner, tmp_path):
        cache_dir = tmp_path / "models"
        cache_dir.mkdir()
        with patch("portiere.cli.models.PortiereConfig") as mock_config_cls:
            mock_config = MagicMock()
            mock_config.model_cache_dir = cache_dir
            mock_config_cls.return_value = mock_config
            result = runner.invoke(models, ["list"])
        assert result.exit_code == 0
        assert "No cached models found" in result.output

    def test_list_with_cached_model(self, runner, tmp_path):
        cache_dir = tmp_path / "models"
        model_dir = cache_dir / "SapBERT"
        model_dir.mkdir(parents=True)
        (model_dir / "config.json").write_text("{}")
        with patch("portiere.cli.models.PortiereConfig") as mock_config_cls:
            mock_config = MagicMock()
            mock_config.model_cache_dir = cache_dir
            mock_config_cls.return_value = mock_config
            result = runner.invoke(models, ["list"])
        assert result.exit_code == 0
        assert "SapBERT" in result.output


class TestModelsDownload:
    def test_download_success(self, runner):
        mock_gateway = MagicMock()
        mock_gateway.dimension = 768
        with patch("portiere.embedding.EmbeddingGateway", return_value=mock_gateway):
            result = runner.invoke(models, ["download", "some/model"])
        assert result.exit_code == 0
        assert "Downloading some/model" in result.output
        assert "Model loaded: some/model" in result.output
        assert "768" in result.output

    def test_download_failure(self, runner):
        with patch(
            "portiere.embedding.EmbeddingGateway",
            side_effect=RuntimeError("model not found"),
        ):
            result = runner.invoke(models, ["download", "bad/model"])
        assert result.exit_code != 0
        assert "Error downloading model" in result.output


class TestModelsHelp:
    def test_models_group_help(self, runner):
        result = runner.invoke(models, ["--help"])
        assert result.exit_code == 0
        assert "download" in result.output
        assert "list" in result.output
        assert "info" in result.output

    def test_download_help(self, runner):
        result = runner.invoke(models, ["download", "--help"])
        assert result.exit_code == 0
        assert "MODEL_NAME" in result.output

"""
Model Management CLI — Download and manage sentence-transformer models.

Provides commands to download, list, and manage embedding models
used by the Portiere SDK for concept search and mapping.
"""

from __future__ import annotations

import click

from portiere.config import PortiereConfig


@click.group()
def models():
    """Manage embedding and reranker models."""
    pass


@models.command()
@click.argument("model_name")
def download(model_name: str):
    """Download and cache a sentence-transformer model.

    MODEL_NAME can be a HuggingFace model identifier or a local path.

    Examples:
        portiere models download sentence-transformers/all-MiniLM-L6-v2
        portiere models download cambridgeltl/SapBERT-from-PubMedBERT-fulltext
    """
    from portiere.config import EmbeddingConfig
    from portiere.embedding import EmbeddingGateway

    click.echo(f"Downloading {model_name}...")
    try:
        gateway = EmbeddingGateway(EmbeddingConfig(provider="huggingface", model=model_name))
        click.echo(f"Model loaded: {model_name}")
        click.echo(f"Embedding dimension: {gateway.dimension}")
    except Exception as e:
        click.echo(f"Error downloading model: {e}", err=True)
        raise SystemExit(1)


@models.command("list")
def list_models():
    """List cached models in the model cache directory."""
    config = PortiereConfig()
    cache_dir = config.model_cache_dir

    if not cache_dir.exists():
        click.echo(f"No model cache directory found at {cache_dir}")
        return

    click.echo(f"Model cache directory: {cache_dir}")

    found = False
    for model_dir in cache_dir.iterdir():
        if model_dir.is_dir():
            size = sum(f.stat().st_size for f in model_dir.rglob("*") if f.is_file())
            size_mb = size / (1024 * 1024)
            click.echo(f"  {model_dir.name} ({size_mb:.1f} MB)")
            found = True

    if not found:
        click.echo("  No cached models found.")


@models.command()
def info():
    """Show current model configuration."""
    config = PortiereConfig()

    click.echo(f"Embedding provider: {config.embedding.provider}")
    click.echo(f"Embedding model:    {config.embedding.model}")
    if config.embedding.endpoint:
        click.echo(f"Embedding endpoint: {config.embedding.endpoint}")
    click.echo(f"Reranker provider:  {config.reranker.provider}")
    click.echo(
        f"Reranker model:     {config.reranker.model if config.reranker.provider != 'none' else 'disabled'}"
    )
    click.echo(f"Cache directory:    {config.model_cache_dir}")
    click.echo(
        f"Operating mode:     {config.effective_mode} (pipeline: {config.effective_pipeline})"
    )

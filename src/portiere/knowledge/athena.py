"""
Athena Vocabulary Loader — Build knowledge layer indexes from OHDSI Athena downloads.

Provides ``load_athena_concepts()`` to parse Athena CSV files and
``build_knowledge_layer()`` to create backend-specific indexes for local
concept mapping.

Example:
    from portiere.knowledge import build_knowledge_layer

    paths = build_knowledge_layer(
        athena_path="./data/athena/",
        output_path="./data/vocab/",
        backend="bm25s",
        vocabularies=["SNOMED", "LOINC", "RxNorm", "ICD10CM"],
    )
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path

import structlog

logger = structlog.get_logger(__name__)


def load_athena_concepts(
    athena_path: str | Path,
    vocabularies: list[str] | None = None,
) -> list[dict]:
    """
    Load concepts from an OHDSI Athena download directory.

    Reads ``CONCEPT.csv`` and optionally ``CONCEPT_SYNONYM.csv`` to produce
    structured concept records for knowledge layer indexing.

    Args:
        athena_path: Path to extracted Athena download directory.
            Must contain ``CONCEPT.csv``. Optionally contains
            ``CONCEPT_SYNONYM.csv`` for synonym data.
        vocabularies: Filter to specific vocabulary IDs (e.g.,
            ``["SNOMED", "LOINC"]``). If ``None``, loads all standard
            concepts in the download.

    Returns:
        List of concept dicts with keys: ``concept_id``, ``concept_name``,
        ``vocabulary_id``, ``domain_id``, ``concept_class_id``,
        ``standard_concept``, ``synonyms``.

    Raises:
        FileNotFoundError: If ``athena_path`` or ``CONCEPT.csv`` does not exist.
    """
    athena_dir = Path(athena_path)

    concept_file = athena_dir / "CONCEPT.csv"
    if not concept_file.exists():
        raise FileNotFoundError(
            f"CONCEPT.csv not found in {athena_dir}. "
            "Download vocabularies from athena.ohdsi.org and extract the zip."
        )

    # Load synonyms if available
    synonyms_map: dict[int, list[str]] = defaultdict(list)
    synonym_file = athena_dir / "CONCEPT_SYNONYM.csv"
    if synonym_file.exists():
        logger.info("athena.loading_synonyms", path=str(synonym_file))
        with open(synonym_file, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                try:
                    cid = int(row["concept_id"])
                    syn = row.get("concept_synonym_name", "").strip()
                    if syn:
                        synonyms_map[cid].append(syn)
                except (ValueError, KeyError):
                    continue

    # Load concepts
    logger.info("athena.loading_concepts", path=str(concept_file))
    concepts: list[dict] = []

    with open(concept_file, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            # Only include standard concepts
            if row.get("standard_concept") != "S":
                continue

            vocab_id = row.get("vocabulary_id", "")
            if vocabularies and vocab_id not in vocabularies:
                continue

            try:
                concept_id = int(row["concept_id"])
            except (ValueError, KeyError):
                continue

            concept_name = row.get("concept_name", "").strip()
            if not concept_name:
                continue

            concept = {
                "concept_id": concept_id,
                "concept_name": concept_name,
                "vocabulary_id": vocab_id,
                "domain_id": row.get("domain_id", ""),
                "concept_class_id": row.get("concept_class_id", ""),
                "standard_concept": "S",
            }

            # Attach synonyms (exclude duplicates of the concept name)
            syns = [
                s for s in synonyms_map.get(concept_id, []) if s.lower() != concept_name.lower()
            ]
            if syns:
                concept["synonyms"] = syns

            concepts.append(concept)

    logger.info(
        "athena.concepts_loaded",
        count=len(concepts),
        vocabularies=vocabularies or "all",
    )
    return concepts


_ALL_BACKENDS = {
    "bm25s",
    "faiss",
    "elasticsearch",
    "hybrid",
    "chromadb",
    "pgvector",
    "mongodb",
    "qdrant",
    "milvus",
}

_VECTOR_BACKENDS = {"faiss", "chromadb", "pgvector", "mongodb", "qdrant", "milvus"}


def build_knowledge_layer(
    athena_path: str | Path,
    output_path: str | Path,
    backend: str = "bm25s",
    vocabularies: list[str] | None = None,
    embedding_model: str = "cambridgeltl/SapBERT-from-PubMedBERT-fulltext",
    *,
    embedding_gateway=None,
    hybrid_backends: list[str] | None = None,
    **backend_kwargs,
) -> dict:
    """
    Build a knowledge layer index from an OHDSI Athena vocabulary download.

    This is the main entry point for local vocabulary setup. Parses Athena
    CSV files and creates backend-specific indexes.

    Args:
        athena_path: Path to extracted Athena download directory.
        output_path: Directory to save index files.
        backend: Backend type. One of: ``"bm25s"``, ``"faiss"``,
            ``"elasticsearch"``, ``"chromadb"``, ``"pgvector"``,
            ``"mongodb"``, ``"qdrant"``, ``"milvus"``, or ``"hybrid"``.
        vocabularies: Filter to specific vocabulary IDs. If ``None``,
            indexes all standard concepts.
        embedding_model: Sentence-transformer model for embeddings.
            Used when ``embedding_gateway`` is not provided.
        embedding_gateway: Pre-configured ``EmbeddingGateway`` instance.
            If provided, takes precedence over ``embedding_model``.
        hybrid_backends: List of sub-backends when ``backend="hybrid"``.
            e.g. ``["bm25s", "chromadb"]``. Each sub-backend's config
            is derived from ``backend_kwargs``.
        **backend_kwargs: Backend-specific connection parameters, e.g.
            ``pgvector_connection_string``, ``mongodb_connection_string``,
            ``qdrant_url``, ``milvus_uri``, ``chroma_collection``.

    Returns:
        Dict with index/config paths suitable for ``KnowledgeLayerConfig``:

        - ``"bm25s"``: ``{"bm25s_corpus_path": "..."}``
        - ``"faiss"``: ``{"faiss_index_path": "...", "faiss_metadata_path": "..."}``
        - ``"chromadb"``: ``{"chroma_persist_path": "..."}``
        - ``"pgvector"``: ``{"pgvector_connection_string": "..."}``
        - ``"mongodb"``: ``{"mongodb_connection_string": "..."}``
        - ``"qdrant"``: ``{"qdrant_url": "..."}``
        - ``"milvus"``: ``{"milvus_uri": "..."}``
        - ``"hybrid"``: merged paths from each sub-backend

    Example:
        from portiere.knowledge import build_knowledge_layer

        # Single backend
        paths = build_knowledge_layer(
            athena_path="./data/athena/",
            output_path="./data/vocab/",
            backend="bm25s",
            vocabularies=["SNOMED", "LOINC", "RxNorm", "ICD10CM"],
        )

        # Hybrid: BM25s + ChromaDB
        paths = build_knowledge_layer(
            athena_path="./data/athena/",
            output_path="./data/vocab/",
            backend="hybrid",
            hybrid_backends=["bm25s", "chromadb"],
            vocabularies=["SNOMED", "LOINC"],
        )
    """
    if backend not in _ALL_BACKENDS:
        raise ValueError(
            f"Unsupported backend: {backend!r}. Supported: {', '.join(sorted(_ALL_BACKENDS))}"
        )

    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Parse Athena CSVs
    concepts = load_athena_concepts(athena_path, vocabularies=vocabularies)
    if not concepts:
        raise ValueError(
            f"No standard concepts found in {athena_path}. "
            "Verify the Athena download contains CONCEPT.csv with standard concepts."
        )

    # Create embedding gateway if needed for vector backends
    needs_embeddings = backend in _VECTOR_BACKENDS or (
        backend == "hybrid"
        and hybrid_backends
        and any(hb in _VECTOR_BACKENDS for hb in hybrid_backends)
    )
    if needs_embeddings and embedding_gateway is None:
        from portiere.config import EmbeddingConfig
        from portiere.embedding import EmbeddingGateway

        embedding_gateway = EmbeddingGateway(
            EmbeddingConfig(provider="huggingface", model=embedding_model)
        )

    # Build backend-specific builder map
    builder_map = {
        "bm25s": lambda: _build_bm25s(concepts, output_dir),
        "faiss": lambda: _build_faiss(concepts, output_dir, embedding_model),
        "chromadb": lambda: _build_chromadb(
            concepts, output_dir, embedding_gateway, **backend_kwargs
        ),
        "pgvector": lambda: _build_pgvector(concepts, embedding_gateway, **backend_kwargs),
        "mongodb": lambda: _build_mongodb(concepts, embedding_gateway, **backend_kwargs),
        "qdrant": lambda: _build_qdrant(concepts, embedding_gateway, **backend_kwargs),
        "milvus": lambda: _build_milvus(concepts, output_dir, embedding_gateway, **backend_kwargs),
    }

    paths: dict[str, str] = {}

    if backend == "hybrid":
        if not hybrid_backends:
            hybrid_backends = ["bm25s", "faiss"]
        for sub_backend in hybrid_backends:
            if sub_backend not in builder_map:
                raise ValueError(
                    f"Unknown hybrid sub-backend: {sub_backend!r}. "
                    f"Supported: {', '.join(sorted(builder_map.keys()))}"
                )
            paths.update(builder_map[sub_backend]())
        paths["hybrid_backends"] = ",".join(hybrid_backends)
    elif backend in builder_map:
        paths.update(builder_map[backend]())
    else:
        raise ValueError(f"No builder for backend: {backend!r}")

    logger.info(
        "athena.knowledge_layer_built",
        backend=backend,
        concepts=len(concepts),
        paths=paths,
    )
    return paths


def _build_bm25s(concepts: list[dict], output_dir: Path) -> dict[str, str]:
    """Save concepts as JSON corpus for BM25s backend."""
    corpus_path = output_dir / "concepts.json"

    with open(corpus_path, "w") as f:
        json.dump(concepts, f, indent=2)

    logger.info(
        "athena.bm25s_built",
        count=len(concepts),
        path=str(corpus_path),
    )
    return {"bm25s_corpus_path": str(corpus_path)}


def _build_faiss(concepts: list[dict], output_dir: Path, embedding_model: str) -> dict[str, str]:
    """Build FAISS vector index from concepts."""
    from portiere.knowledge.local_faiss_backend import LocalFAISSBackend

    index_path = output_dir / "concepts.index"
    metadata_path = output_dir / "concepts.meta.json"

    backend = LocalFAISSBackend(
        index_path=index_path,
        metadata_path=metadata_path,
        embedding_model=embedding_model,
    )
    backend.index_concepts(concepts)

    return {
        "faiss_index_path": str(index_path),
        "faiss_metadata_path": str(metadata_path),
    }


def _build_chromadb(
    concepts: list[dict],
    output_dir: Path,
    embedding_gateway,
    **kwargs,
) -> dict[str, str]:
    """Build ChromaDB collection from concepts."""
    from portiere.knowledge.chroma_backend import ChromaDBBackend

    persist_path = Path(kwargs.get("chroma_persist_path", output_dir / "chroma"))
    collection = kwargs.get("chroma_collection", "portiere_concepts")

    backend = ChromaDBBackend(
        collection_name=collection,
        persist_path=persist_path,
        embedding_gateway=embedding_gateway,
    )
    backend.index_concepts(concepts)

    return {"chroma_persist_path": str(persist_path)}


def _build_pgvector(
    concepts: list[dict],
    embedding_gateway,
    **kwargs,
) -> dict[str, str]:
    """Index concepts into PostgreSQL + pgvector."""
    from portiere.knowledge.pgvector_backend import PGVectorBackend

    connection_string = kwargs.get("pgvector_connection_string")
    if not connection_string:
        raise ValueError(
            "pgvector_connection_string is required to build PGVector index. "
            "Pass it as a keyword argument."
        )
    table = kwargs.get("pgvector_table", "portiere_concepts")

    backend = PGVectorBackend(
        connection_string=connection_string,
        table_name=table,
        embedding_gateway=embedding_gateway,
    )
    backend.index_concepts(concepts)

    return {"pgvector_connection_string": connection_string}


def _build_mongodb(
    concepts: list[dict],
    embedding_gateway,
    **kwargs,
) -> dict[str, str]:
    """Index concepts into MongoDB with Atlas Vector Search."""
    from portiere.knowledge.mongodb_backend import MongoDBBackend

    connection_string = kwargs.get("mongodb_connection_string")
    if not connection_string:
        raise ValueError(
            "mongodb_connection_string is required to build MongoDB index. "
            "Pass it as a keyword argument."
        )
    database = kwargs.get("mongodb_database", "portiere")
    collection = kwargs.get("mongodb_collection", "concepts")

    backend = MongoDBBackend(
        connection_string=connection_string,
        database=database,
        collection=collection,
        embedding_gateway=embedding_gateway,
    )
    backend.index_concepts(concepts)

    return {"mongodb_connection_string": connection_string}


def _build_qdrant(
    concepts: list[dict],
    embedding_gateway,
    **kwargs,
) -> dict[str, str]:
    """Index concepts into Qdrant."""
    from portiere.knowledge.qdrant_backend import QdrantBackend

    url = kwargs.get("qdrant_url")
    collection = kwargs.get("qdrant_collection", "portiere_concepts")
    api_key = kwargs.get("qdrant_api_key")

    backend = QdrantBackend(
        url=url,
        collection_name=collection,
        api_key=api_key,
        embedding_gateway=embedding_gateway,
    )
    backend.index_concepts(concepts)

    return {"qdrant_url": url or ":memory:"}


def _build_milvus(
    concepts: list[dict],
    output_dir: Path,
    embedding_gateway,
    **kwargs,
) -> dict[str, str]:
    """Index concepts into Milvus."""
    from portiere.knowledge.milvus_backend import MilvusBackend

    uri = kwargs.get("milvus_uri", str(output_dir / "milvus_portiere.db"))
    collection = kwargs.get("milvus_collection", "portiere_concepts")

    backend = MilvusBackend(
        uri=uri,
        collection_name=collection,
        embedding_gateway=embedding_gateway,
    )
    backend.index_concepts(concepts)

    return {"milvus_uri": uri}

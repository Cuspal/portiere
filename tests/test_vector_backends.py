"""
Tests for vector-database knowledge layer backends.

Tests the pluggable vector-search backends with fully mocked drivers:
- ChromaDBBackend (chromadb)
- PGVectorBackend (psycopg + pgvector)
- MongoDBBackend (pymongo)
- QdrantBackend (qdrant-client)
- MilvusBackend (pymilvus)
"""

import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

SAMPLE_CONCEPTS = [
    {
        "concept_id": 201826,
        "concept_name": "Type 2 diabetes mellitus",
        "vocabulary_id": "SNOMED",
        "domain_id": "Condition",
        "concept_class_id": "Clinical Finding",
        "standard_concept": "S",
    },
    {
        "concept_id": 320128,
        "concept_name": "Essential hypertension",
        "vocabulary_id": "SNOMED",
        "domain_id": "Condition",
        "concept_class_id": "Clinical Finding",
        "standard_concept": "S",
    },
    {
        "concept_id": 4329847,
        "concept_name": "Myocardial infarction",
        "vocabulary_id": "SNOMED",
        "domain_id": "Condition",
        "concept_class_id": "Clinical Finding",
        "standard_concept": "S",
    },
    {
        "concept_id": 36714559,
        "concept_name": "Hemoglobin A1c measurement",
        "vocabulary_id": "LOINC",
        "domain_id": "Measurement",
        "concept_class_id": "Lab Test",
        "standard_concept": "S",
    },
    {
        "concept_id": 1503297,
        "concept_name": "Metformin",
        "vocabulary_id": "RxNorm",
        "domain_id": "Drug",
        "concept_class_id": "Ingredient",
        "standard_concept": "S",
    },
]

EXPECTED_KEYS = {
    "concept_id",
    "concept_name",
    "vocabulary_id",
    "domain_id",
    "concept_class_id",
    "standard_concept",
}
EXPECTED_SEARCH_KEYS = EXPECTED_KEYS | {"score"}


def _make_embedding_gateway(dim: int = 384):
    """Create a mock embedding_gateway that returns random normalised vectors."""
    gw = MagicMock()

    def _encode(texts, convert_to_numpy=True):
        n = len(texts)
        vecs = np.random.randn(n, dim).astype("float32")
        norms = np.linalg.norm(vecs, axis=1, keepdims=True)
        norms[norms == 0] = 1
        return vecs / norms

    gw.encode = MagicMock(side_effect=_encode)
    return gw


# ---------------------------------------------------------------------------
# ChromaDB
# ---------------------------------------------------------------------------


class TestChromaDBBackend:
    """Tests for ChromaDBBackend (chromadb mocked)."""

    def _make_mock_chromadb(self):
        """Return a mock chromadb module wired up enough for __init__."""
        mod = MagicMock()
        mock_collection = MagicMock()
        mock_collection.count.return_value = 0
        mock_collection.get.return_value = {"metadatas": []}
        mod.Client.return_value.get_or_create_collection.return_value = mock_collection
        return mod, mock_collection

    def test_import_error_when_driver_missing(self):
        with patch.dict(sys.modules, {"chromadb": None}):
            # Force re-import
            from importlib import reload

            import portiere.knowledge.chroma_backend as cb_mod

            with pytest.raises(ImportError, match="chromadb"):
                reload(cb_mod)
                cb_mod.ChromaDBBackend()

    def test_init_succeeds_with_mocked_driver(self):
        mock_chromadb, _ = self._make_mock_chromadb()
        with patch.dict(sys.modules, {"chromadb": mock_chromadb}):
            from portiere.knowledge.chroma_backend import ChromaDBBackend

            backend = ChromaDBBackend.__new__(ChromaDBBackend)
            backend._chromadb = mock_chromadb
            backend._embedding_gateway = None
            backend._collection_name = "test"
            backend._persist_path = None
            backend._client = mock_chromadb.Client()
            backend._collection = backend._client.get_or_create_collection(name="test")
            backend._concept_id_index = {}
            # No error means success
            assert backend._collection is not None

    def test_embed_raises_without_gateway(self):
        mock_chromadb, _ = self._make_mock_chromadb()
        with patch.dict(sys.modules, {"chromadb": mock_chromadb}):
            from portiere.knowledge.chroma_backend import ChromaDBBackend

            backend = ChromaDBBackend.__new__(ChromaDBBackend)
            backend._embedding_gateway = None

            with pytest.raises(RuntimeError, match="embedding_gateway"):
                backend._embed(["test"])

    def test_search_returns_correct_format(self):
        mock_chromadb, mock_collection = self._make_mock_chromadb()
        mock_collection.count.return_value = 5
        mock_collection.query.return_value = {
            "metadatas": [
                [
                    {
                        "concept_id": 201826,
                        "concept_name": "Type 2 diabetes mellitus",
                        "vocabulary_id": "SNOMED",
                        "domain_id": "Condition",
                        "concept_class_id": "Clinical Finding",
                        "standard_concept": "S",
                    },
                ]
            ],
            "distances": [[0.2]],
        }

        with patch.dict(sys.modules, {"chromadb": mock_chromadb}):
            from portiere.knowledge.chroma_backend import ChromaDBBackend

            backend = ChromaDBBackend.__new__(ChromaDBBackend)
            backend._embedding_gateway = _make_embedding_gateway()
            backend._collection = mock_collection
            backend._concept_id_index = {}

            results = backend.search("diabetes", limit=5)

            assert isinstance(results, list)
            assert len(results) == 1
            assert set(results[0].keys()) == EXPECTED_SEARCH_KEYS
            assert results[0]["concept_id"] == 201826
            assert results[0]["score"] > 0

    def test_get_concept_returns_correct_format(self):
        mock_chromadb, _mock_collection = self._make_mock_chromadb()

        with patch.dict(sys.modules, {"chromadb": mock_chromadb}):
            from portiere.knowledge.chroma_backend import ChromaDBBackend

            backend = ChromaDBBackend.__new__(ChromaDBBackend)
            backend._concept_id_index = {
                201826: {
                    "concept_id": 201826,
                    "concept_name": "Type 2 diabetes mellitus",
                    "vocabulary_id": "SNOMED",
                    "domain_id": "Condition",
                    "concept_class_id": "Clinical Finding",
                    "standard_concept": "S",
                }
            }

            result = backend.get_concept(201826)
            assert result["concept_id"] == 201826
            assert "concept_name" in result

    def test_index_concepts_calls_driver(self):
        mock_chromadb, mock_collection = self._make_mock_chromadb()

        with patch.dict(sys.modules, {"chromadb": mock_chromadb}):
            from portiere.knowledge.chroma_backend import ChromaDBBackend

            backend = ChromaDBBackend.__new__(ChromaDBBackend)
            backend._embedding_gateway = _make_embedding_gateway()
            backend._collection = mock_collection
            backend._collection_name = "test"
            backend._concept_id_index = {}

            backend.index_concepts(SAMPLE_CONCEPTS)

            mock_collection.upsert.assert_called_once()
            call_kwargs = mock_collection.upsert.call_args
            assert len(call_kwargs[1]["ids"]) == len(SAMPLE_CONCEPTS)


# ---------------------------------------------------------------------------
# PGVector
# ---------------------------------------------------------------------------


class TestPGVectorBackend:
    """Tests for PGVectorBackend (psycopg + pgvector mocked)."""

    def _make_mock_modules(self):
        """Return mock psycopg and pgvector modules."""
        mock_psycopg = MagicMock()
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value = mock_conn

        mock_pgvector = MagicMock()
        mock_pgvector_psycopg = MagicMock()

        return mock_psycopg, mock_pgvector, mock_pgvector_psycopg, mock_conn

    def test_import_error_when_driver_missing(self):
        with patch.dict(sys.modules, {"psycopg": None, "pgvector": None, "pgvector.psycopg": None}):
            from portiere.knowledge.pgvector_backend import PGVectorBackend

            with pytest.raises(ImportError, match="psycopg"):
                PGVectorBackend(connection_string="postgresql://localhost/test")

    def test_init_succeeds_with_mocked_driver(self):
        mock_psycopg, mock_pgvector, mock_pgvector_psycopg, _mock_conn = self._make_mock_modules()
        with patch.dict(
            sys.modules,
            {
                "psycopg": mock_psycopg,
                "pgvector": mock_pgvector,
                "pgvector.psycopg": mock_pgvector_psycopg,
            },
        ):
            from portiere.knowledge.pgvector_backend import PGVectorBackend

            backend = PGVectorBackend(connection_string="postgresql://localhost/test")
            assert backend._conn is not None

    def test_embed_raises_without_gateway(self):
        from portiere.knowledge.pgvector_backend import PGVectorBackend

        backend = PGVectorBackend.__new__(PGVectorBackend)
        backend._embedding_gateway = None

        with pytest.raises(RuntimeError, match="embedding_gateway"):
            backend._embed(["test"])

    def test_search_returns_correct_format(self):
        from portiere.knowledge.pgvector_backend import PGVectorBackend

        backend = PGVectorBackend.__new__(PGVectorBackend)
        backend._embedding_gateway = _make_embedding_gateway()
        backend._table_name = "test_concepts"
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = [
            (
                201826,
                "Type 2 diabetes mellitus",
                "SNOMED",
                "Condition",
                "Clinical Finding",
                "S",
                0.95,
            ),
        ]
        mock_conn.cursor.return_value = mock_cursor
        backend._conn = mock_conn

        results = backend.search("diabetes", limit=5)

        assert isinstance(results, list)
        assert len(results) == 1
        assert set(results[0].keys()) == EXPECTED_SEARCH_KEYS
        assert results[0]["concept_id"] == 201826
        assert results[0]["score"] == 0.95

    def test_get_concept_returns_correct_format(self):
        from portiere.knowledge.pgvector_backend import PGVectorBackend

        backend = PGVectorBackend.__new__(PGVectorBackend)
        backend._table_name = "test_concepts"
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone.return_value = (
            201826,
            "Type 2 diabetes mellitus",
            "SNOMED",
            "Condition",
            "Clinical Finding",
            "S",
        )
        mock_conn.cursor.return_value = mock_cursor
        backend._conn = mock_conn

        result = backend.get_concept(201826)
        assert set(result.keys()) >= {"concept_id", "concept_name"}
        assert result["concept_id"] == 201826

    def test_index_concepts_calls_driver(self):
        mock_psycopg, mock_pgvector, mock_pgvector_psycopg, mock_conn = self._make_mock_modules()

        # Mock psycopg.rows module for the import inside index_concepts
        mock_psycopg_rows = MagicMock()
        with patch.dict(
            sys.modules,
            {
                "psycopg": mock_psycopg,
                "psycopg.rows": mock_psycopg_rows,
                "pgvector": mock_pgvector,
                "pgvector.psycopg": mock_pgvector_psycopg,
            },
        ):
            from portiere.knowledge.pgvector_backend import PGVectorBackend

            backend = PGVectorBackend.__new__(PGVectorBackend)
            backend._embedding_gateway = _make_embedding_gateway()
            backend._table_name = "test_concepts"
            backend._conn = mock_conn
            backend._dimension = None

            mock_cursor = MagicMock()
            mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
            mock_cursor.__exit__ = MagicMock(return_value=False)
            mock_conn.cursor.return_value = mock_cursor

            backend.index_concepts(SAMPLE_CONCEPTS)

            # _ensure_table + insert calls go through cursor.execute
            assert mock_cursor.execute.call_count >= len(SAMPLE_CONCEPTS)
            mock_conn.commit.assert_called()


# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------


class TestMongoDBBackend:
    """Tests for MongoDBBackend (pymongo mocked)."""

    def _make_mock_pymongo(self):
        """Return a mock pymongo module."""
        mod = MagicMock()
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mod.MongoClient.return_value = mock_client
        return mod, mock_client, mock_collection

    def test_import_error_when_driver_missing(self):
        with patch.dict(sys.modules, {"pymongo": None}):
            from portiere.knowledge.mongodb_backend import MongoDBBackend

            with pytest.raises(ImportError, match="pymongo"):
                MongoDBBackend(connection_string="mongodb://localhost:27017")

    def test_init_succeeds_with_mocked_driver(self):
        mock_pymongo, _, _mock_collection = self._make_mock_pymongo()
        with patch.dict(sys.modules, {"pymongo": mock_pymongo}):
            from portiere.knowledge.mongodb_backend import MongoDBBackend

            backend = MongoDBBackend(connection_string="mongodb://localhost:27017")
            assert backend._collection is not None

    def test_embed_raises_without_gateway(self):
        from portiere.knowledge.mongodb_backend import MongoDBBackend

        backend = MongoDBBackend.__new__(MongoDBBackend)
        backend._embedding_gateway = None

        with pytest.raises(RuntimeError, match="embedding_gateway"):
            backend._embed(["test"])

    def test_search_returns_correct_format(self):
        from portiere.knowledge.mongodb_backend import MongoDBBackend

        backend = MongoDBBackend.__new__(MongoDBBackend)
        backend._embedding_gateway = _make_embedding_gateway()

        mock_collection = MagicMock()
        mock_collection.aggregate.return_value = [
            {
                "concept_id": 201826,
                "concept_name": "Type 2 diabetes mellitus",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
                "concept_class_id": "Clinical Finding",
                "standard_concept": "S",
                "score": 0.92,
            },
        ]
        backend._collection = mock_collection

        results = backend.search("diabetes", limit=5)

        assert isinstance(results, list)
        assert len(results) == 1
        assert set(results[0].keys()) == EXPECTED_SEARCH_KEYS
        assert results[0]["concept_id"] == 201826
        assert results[0]["score"] > 0

    def test_get_concept_returns_correct_format(self):
        from portiere.knowledge.mongodb_backend import MongoDBBackend

        backend = MongoDBBackend.__new__(MongoDBBackend)
        mock_collection = MagicMock()
        mock_collection.find_one.return_value = {
            "concept_id": 201826,
            "concept_name": "Type 2 diabetes mellitus",
            "vocabulary_id": "SNOMED",
            "domain_id": "Condition",
            "concept_class_id": "Clinical Finding",
            "standard_concept": "S",
        }
        backend._collection = mock_collection

        result = backend.get_concept(201826)
        assert result["concept_id"] == 201826
        assert "concept_name" in result

    def test_index_concepts_calls_driver(self):
        mock_pymongo, _, mock_collection = self._make_mock_pymongo()
        mock_pymongo.UpdateOne = MagicMock()

        with patch.dict(sys.modules, {"pymongo": mock_pymongo}):
            from portiere.knowledge.mongodb_backend import MongoDBBackend

            backend = MongoDBBackend.__new__(MongoDBBackend)
            backend._embedding_gateway = _make_embedding_gateway()
            backend._collection = mock_collection
            backend._pymongo = mock_pymongo

            backend.index_concepts(SAMPLE_CONCEPTS)

            mock_collection.bulk_write.assert_called_once()
            mock_collection.create_index.assert_called()


# ---------------------------------------------------------------------------
# Qdrant
# ---------------------------------------------------------------------------


class TestQdrantBackend:
    """Tests for QdrantBackend (qdrant-client mocked)."""

    def _make_mock_qdrant(self):
        """Return mock qdrant_client module and its sub-objects."""
        mod = MagicMock()
        mock_client = MagicMock()
        mod.QdrantClient.return_value = mock_client
        mock_models = MagicMock()
        mod.models = mock_models
        return mod, mock_client, mock_models

    def test_import_error_when_driver_missing(self):
        with patch.dict(sys.modules, {"qdrant_client": None}):
            from portiere.knowledge.qdrant_backend import QdrantBackend

            with pytest.raises(ImportError, match="qdrant-client"):
                QdrantBackend()

    def test_init_succeeds_with_mocked_driver(self):
        mock_mod, _mock_client, _mock_models = self._make_mock_qdrant()
        with patch.dict(sys.modules, {"qdrant_client": mock_mod}):
            from portiere.knowledge.qdrant_backend import QdrantBackend

            backend = QdrantBackend()
            assert backend._client is not None

    def test_embed_raises_without_gateway(self):
        from portiere.knowledge.qdrant_backend import QdrantBackend

        backend = QdrantBackend.__new__(QdrantBackend)
        backend._embedding_gateway = None

        with pytest.raises(RuntimeError, match="embedding_gateway"):
            backend._embed(["test"])

    def test_search_returns_correct_format(self):
        from portiere.knowledge.qdrant_backend import QdrantBackend

        backend = QdrantBackend.__new__(QdrantBackend)
        backend._embedding_gateway = _make_embedding_gateway()
        backend._collection_name = "test"
        backend._models = MagicMock()

        # Mock search results as ScoredPoint-like objects
        mock_hit = MagicMock()
        mock_hit.payload = {
            "concept_id": 201826,
            "concept_name": "Type 2 diabetes mellitus",
            "vocabulary_id": "SNOMED",
            "domain_id": "Condition",
            "concept_class_id": "Clinical Finding",
            "standard_concept": "S",
        }
        mock_hit.score = 0.93

        mock_client = MagicMock()
        mock_client.search.return_value = [mock_hit]
        backend._client = mock_client

        results = backend.search("diabetes", limit=5)

        assert isinstance(results, list)
        assert len(results) == 1
        assert set(results[0].keys()) == EXPECTED_SEARCH_KEYS
        assert results[0]["concept_id"] == 201826
        assert results[0]["score"] == 0.93

    def test_get_concept_returns_correct_format(self):
        from portiere.knowledge.qdrant_backend import QdrantBackend

        backend = QdrantBackend.__new__(QdrantBackend)
        backend._collection_name = "test"
        backend._models = MagicMock()

        mock_point = MagicMock()
        mock_point.payload = {
            "concept_id": 201826,
            "concept_name": "Type 2 diabetes mellitus",
            "vocabulary_id": "SNOMED",
            "domain_id": "Condition",
            "concept_class_id": "Clinical Finding",
            "standard_concept": "S",
        }

        mock_client = MagicMock()
        mock_client.scroll.return_value = ([mock_point], None)
        backend._client = mock_client

        result = backend.get_concept(201826)
        assert result["concept_id"] == 201826
        assert "concept_name" in result

    def test_index_concepts_calls_driver(self):
        from portiere.knowledge.qdrant_backend import QdrantBackend

        backend = QdrantBackend.__new__(QdrantBackend)
        backend._embedding_gateway = _make_embedding_gateway()
        backend._collection_name = "test"
        backend._models = MagicMock()

        mock_client = MagicMock()
        # _ensure_collection checks get_collections
        mock_collections_resp = MagicMock()
        mock_collections_resp.collections = []
        mock_client.get_collections.return_value = mock_collections_resp
        backend._client = mock_client

        backend.index_concepts(SAMPLE_CONCEPTS)

        # Should call create_collection (since collection list is empty)
        mock_client.create_collection.assert_called_once()
        # Should call upsert with points
        mock_client.upsert.assert_called_once()
        call_kwargs = mock_client.upsert.call_args
        assert call_kwargs[1]["collection_name"] == "test"


# ---------------------------------------------------------------------------
# Milvus
# ---------------------------------------------------------------------------


class TestMilvusBackend:
    """Tests for MilvusBackend (pymilvus mocked)."""

    def _make_mock_pymilvus(self):
        """Return a mock pymilvus module."""
        mod = MagicMock()
        mock_client = MagicMock()
        mod.MilvusClient.return_value = mock_client
        return mod, mock_client

    def test_import_error_when_driver_missing(self):
        with patch.dict(sys.modules, {"pymilvus": None}):
            from portiere.knowledge.milvus_backend import MilvusBackend

            with pytest.raises(ImportError, match="pymilvus"):
                MilvusBackend()

    def test_init_succeeds_with_mocked_driver(self):
        mock_pymilvus, _mock_client = self._make_mock_pymilvus()
        with patch.dict(sys.modules, {"pymilvus": mock_pymilvus}):
            from portiere.knowledge.milvus_backend import MilvusBackend

            backend = MilvusBackend(uri="./test_milvus.db")
            assert backend._client is not None

    def test_embed_raises_without_gateway(self):
        from portiere.knowledge.milvus_backend import MilvusBackend

        backend = MilvusBackend.__new__(MilvusBackend)
        backend._embedding_gateway = None

        with pytest.raises(RuntimeError, match="embedding_gateway"):
            backend._embed(["test"])

    def test_search_returns_correct_format(self):
        from portiere.knowledge.milvus_backend import MilvusBackend

        backend = MilvusBackend.__new__(MilvusBackend)
        backend._embedding_gateway = _make_embedding_gateway()
        backend._collection_name = "test"

        mock_client = MagicMock()
        # Milvus search returns list of lists of hits
        mock_client.search.return_value = [
            [
                {
                    "entity": {
                        "concept_id": 201826,
                        "concept_name": "Type 2 diabetes mellitus",
                        "vocabulary_id": "SNOMED",
                        "domain_id": "Condition",
                        "concept_class_id": "Clinical Finding",
                        "standard_concept": "S",
                    },
                    "distance": 0.91,
                },
            ]
        ]
        backend._client = mock_client

        results = backend.search("diabetes", limit=5)

        assert isinstance(results, list)
        assert len(results) == 1
        assert set(results[0].keys()) == EXPECTED_SEARCH_KEYS
        assert results[0]["concept_id"] == 201826
        assert results[0]["score"] == 0.91

    def test_get_concept_returns_correct_format(self):
        from portiere.knowledge.milvus_backend import MilvusBackend

        backend = MilvusBackend.__new__(MilvusBackend)
        backend._collection_name = "test"

        mock_client = MagicMock()
        mock_client.query.return_value = [
            {
                "concept_id": 201826,
                "concept_name": "Type 2 diabetes mellitus",
                "vocabulary_id": "SNOMED",
                "domain_id": "Condition",
                "concept_class_id": "Clinical Finding",
                "standard_concept": "S",
            },
        ]
        backend._client = mock_client

        result = backend.get_concept(201826)
        assert result["concept_id"] == 201826
        assert "concept_name" in result

    def test_index_concepts_calls_driver(self):
        from portiere.knowledge.milvus_backend import MilvusBackend

        backend = MilvusBackend.__new__(MilvusBackend)
        backend._embedding_gateway = _make_embedding_gateway()
        backend._collection_name = "test"
        backend._dimension = None
        backend._pymilvus_imports = {
            "Collection": MagicMock(),
            "CollectionSchema": MagicMock(),
            "DataType": MagicMock(),
            "FieldSchema": MagicMock(),
            "MilvusClient": MagicMock(),
            "connections": MagicMock(),
            "utility": MagicMock(),
        }

        mock_client = MagicMock()
        mock_client.has_collection.return_value = False
        mock_schema = MagicMock()
        mock_client.create_schema.return_value = mock_schema
        backend._client = mock_client

        backend.index_concepts(SAMPLE_CONCEPTS)

        # Should call create_collection (has_collection returned False)
        mock_client.create_collection.assert_called_once()
        # Should call insert with data
        mock_client.insert.assert_called_once()
        call_kwargs = mock_client.insert.call_args
        assert call_kwargs[1]["collection_name"] == "test"
        assert len(call_kwargs[1]["data"]) == len(SAMPLE_CONCEPTS)

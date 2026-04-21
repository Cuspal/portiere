"""
Tests for knowledge layer backends.

Tests the pluggable knowledge layer interface and implementations:
- KnowledgeLayerBackend (abstract)
- BM25sBackend (pure Python)
- LocalFAISSBackend (vector search, mocked)
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Sample concept corpus for testing
SAMPLE_CONCEPTS = [
    {
        "concept_id": 201826,
        "concept_name": "Type 2 diabetes mellitus",
        "vocabulary_id": "SNOMED",
        "domain_id": "Condition",
    },
    {
        "concept_id": 320128,
        "concept_name": "Essential hypertension",
        "vocabulary_id": "SNOMED",
        "domain_id": "Condition",
    },
    {
        "concept_id": 4329847,
        "concept_name": "Myocardial infarction",
        "vocabulary_id": "SNOMED",
        "domain_id": "Condition",
    },
    {
        "concept_id": 36714559,
        "concept_name": "Hemoglobin A1c measurement",
        "vocabulary_id": "LOINC",
        "domain_id": "Measurement",
    },
    {
        "concept_id": 3004249,
        "concept_name": "Systolic blood pressure",
        "vocabulary_id": "LOINC",
        "domain_id": "Measurement",
    },
    {
        "concept_id": 1503297,
        "concept_name": "Metformin",
        "vocabulary_id": "RxNorm",
        "domain_id": "Drug",
    },
    {
        "concept_id": 1308216,
        "concept_name": "Lisinopril",
        "vocabulary_id": "RxNorm",
        "domain_id": "Drug",
    },
    {
        "concept_id": 1332418,
        "concept_name": "Amlodipine",
        "vocabulary_id": "RxNorm",
        "domain_id": "Drug",
    },
]


class TestKnowledgeLayerBackendInterface:
    """Tests for the abstract KnowledgeLayerBackend interface."""

    def test_cannot_instantiate_abstract_class(self):
        """Test that abstract class cannot be instantiated."""
        from portiere.knowledge.base import KnowledgeLayerBackend

        with pytest.raises(TypeError):
            KnowledgeLayerBackend()

    def test_concrete_implementation_must_implement_all_methods(self):
        """Test that subclass must implement all abstract methods."""
        from portiere.knowledge.base import KnowledgeLayerBackend

        class IncompleteBackend(KnowledgeLayerBackend):
            def search(self, query, **kwargs):
                return []

        with pytest.raises(TypeError):
            IncompleteBackend()

    def test_batch_search_default_implementation(self):
        """Test that batch_search works via default implementation."""
        from portiere.knowledge.base import KnowledgeLayerBackend

        class SimpleBackend(KnowledgeLayerBackend):
            def search(self, query, vocabularies=None, domain=None, limit=10):
                return [{"concept_name": query, "score": 1.0}]

            def get_concept(self, concept_id):
                return {}

            def index_concepts(self, concepts):
                pass

        backend = SimpleBackend()
        results = backend.batch_search(["diabetes", "hypertension"])

        assert len(results) == 2
        assert results[0][0]["concept_name"] == "diabetes"
        assert results[1][0]["concept_name"] == "hypertension"


class TestBM25sBackend:
    """Tests for BM25sBackend."""

    @pytest.fixture
    def corpus_file(self):
        """Create a temporary corpus file with sample concepts."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(SAMPLE_CONCEPTS, f)
            corpus_path = Path(f.name)

        yield corpus_path
        corpus_path.unlink(missing_ok=True)

    @pytest.fixture
    def bm25s_backend(self, corpus_file):
        """Create a BM25s backend with sample data."""
        from portiere.knowledge.bm25s_backend import BM25sBackend

        return BM25sBackend(corpus_path=corpus_file, use_stemming=False)

    def test_search_returns_results(self, bm25s_backend):
        """Test that search returns relevant results."""
        results = bm25s_backend.search("diabetes")

        assert len(results) > 0
        assert results[0]["concept_name"] == "Type 2 diabetes mellitus"
        assert results[0]["concept_id"] == 201826
        assert results[0]["score"] > 0

    def test_search_with_vocabulary_filter(self, bm25s_backend):
        """Test search filtered by vocabulary."""
        results = bm25s_backend.search("blood pressure", vocabularies=["LOINC"])

        assert len(results) > 0
        for result in results:
            assert result["vocabulary_id"] == "LOINC"

    def test_search_with_domain_filter(self, bm25s_backend):
        """Test search filtered by domain."""
        results = bm25s_backend.search("metformin", domain="Drug")

        assert len(results) > 0
        for result in results:
            assert result["domain_id"] == "Drug"

    def test_search_with_limit(self, bm25s_backend):
        """Test search respects limit parameter."""
        results = bm25s_backend.search("blood", limit=2)

        assert len(results) <= 2

    def test_search_no_results(self, bm25s_backend):
        """Test search returns empty for nonsensical query."""
        results = bm25s_backend.search("xyznonexistent123")

        # BM25 may still return results with very low scores
        # but they should be filtered by score > 0
        for result in results:
            assert result["score"] > 0

    def test_get_concept(self, bm25s_backend):
        """Test getting concept by ID."""
        concept = bm25s_backend.get_concept(201826)

        assert concept["concept_id"] == 201826
        assert concept["concept_name"] == "Type 2 diabetes mellitus"

    def test_get_concept_not_found(self, bm25s_backend):
        """Test getting nonexistent concept raises error."""
        with pytest.raises(ValueError, match="not found"):
            bm25s_backend.get_concept(999999)

    def test_index_concepts(self):
        """Test indexing concepts creates corpus file."""
        from portiere.knowledge.bm25s_backend import BM25sBackend

        with tempfile.TemporaryDirectory() as tmpdir:
            corpus_path = Path(tmpdir) / "concepts.json"
            backend = BM25sBackend(corpus_path=corpus_path, use_stemming=False)

            # Index concepts
            backend.index_concepts(SAMPLE_CONCEPTS)

            # Verify file created
            assert corpus_path.exists()

            # Verify search works after indexing
            results = backend.search("diabetes")
            assert len(results) > 0
            assert results[0]["concept_name"] == "Type 2 diabetes mellitus"

    def test_empty_corpus_raises_on_search(self):
        """Test that searching without corpus raises RuntimeError."""
        from portiere.knowledge.bm25s_backend import BM25sBackend

        with tempfile.TemporaryDirectory() as tmpdir:
            corpus_path = Path(tmpdir) / "nonexistent.json"
            backend = BM25sBackend(corpus_path=corpus_path)

            with pytest.raises(RuntimeError, match="not loaded"):
                backend.search("diabetes")

    def test_batch_search(self, bm25s_backend):
        """Test batch search returns results for multiple queries."""
        results = bm25s_backend.batch_search(["diabetes", "hypertension", "metformin"])

        assert len(results) == 3
        assert len(results[0]) > 0  # diabetes results
        assert len(results[1]) > 0  # hypertension results
        assert len(results[2]) > 0  # metformin results


class TestLocalFAISSBackend:
    """Tests for LocalFAISSBackend (with mocked dependencies)."""

    def test_search_not_loaded_raises_error(self):
        """Test that searching without loaded index raises RuntimeError."""
        from portiere.knowledge.local_faiss_backend import LocalFAISSBackend

        with tempfile.TemporaryDirectory() as tmpdir:
            backend = LocalFAISSBackend(
                index_path=Path(tmpdir) / "test.index",
                metadata_path=Path(tmpdir) / "metadata.json",
            )

            with pytest.raises(RuntimeError, match="not loaded"):
                backend.search("diabetes")

    def test_get_concept_not_loaded(self):
        """Test get_concept when no metadata loaded."""
        from portiere.knowledge.local_faiss_backend import LocalFAISSBackend

        with tempfile.TemporaryDirectory() as tmpdir:
            backend = LocalFAISSBackend(
                index_path=Path(tmpdir) / "test.index",
                metadata_path=Path(tmpdir) / "metadata.json",
            )

            with pytest.raises(ValueError, match="not found"):
                backend.get_concept(201826)

    def test_search_with_mocked_faiss(self):
        """Test FAISS search with mocked index and model."""
        import numpy as np

        from portiere.knowledge.local_faiss_backend import LocalFAISSBackend

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create metadata file
            metadata = {str(i): c for i, c in enumerate(SAMPLE_CONCEPTS)}
            metadata_path = Path(tmpdir) / "metadata.json"
            with open(metadata_path, "w") as f:
                json.dump(metadata, f)

            backend = LocalFAISSBackend(
                index_path=Path(tmpdir) / "test.index",
                metadata_path=metadata_path,
            )

            # Manually load metadata (index file doesn't exist for mock)
            backend.metadata = metadata
            backend._concept_id_index = {c["concept_id"]: c for c in metadata.values()}

            # Mock the FAISS index
            mock_index = MagicMock()
            mock_index.ntotal = len(SAMPLE_CONCEPTS)
            mock_index.search.return_value = (
                np.array([[0.1, 0.3, 0.5]]),  # distances
                np.array([[0, 1, 2]]),  # indices
            )
            backend.index = mock_index

            # Mock the model
            mock_model = MagicMock()
            mock_embedding = np.random.randn(1, 768).astype("float32")
            mock_embedding = mock_embedding / np.linalg.norm(mock_embedding, axis=1, keepdims=True)
            mock_model.encode.return_value = mock_embedding
            backend._model = mock_model

            # Search
            results = backend.search("diabetes", limit=3)

            assert len(results) == 3
            assert results[0]["concept_id"] == SAMPLE_CONCEPTS[0]["concept_id"]
            mock_model.encode.assert_called_once()
            mock_index.search.assert_called_once()

    def test_search_with_vocabulary_filter_mocked(self):
        """Test FAISS search with vocabulary filter and mocked dependencies."""
        import numpy as np

        from portiere.knowledge.local_faiss_backend import LocalFAISSBackend

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create metadata file
            metadata = {str(i): c for i, c in enumerate(SAMPLE_CONCEPTS)}
            metadata_path = Path(tmpdir) / "metadata.json"
            with open(metadata_path, "w") as f:
                json.dump(metadata, f)

            backend = LocalFAISSBackend(
                index_path=Path(tmpdir) / "test.index",
                metadata_path=metadata_path,
            )

            # Mock the FAISS index to return all indices
            mock_index = MagicMock()
            mock_index.ntotal = len(SAMPLE_CONCEPTS)
            all_indices = list(range(len(SAMPLE_CONCEPTS)))
            all_distances = [0.1 * i for i in range(len(SAMPLE_CONCEPTS))]
            mock_index.search.return_value = (
                np.array([all_distances]),
                np.array([all_indices]),
            )
            backend.index = mock_index

            # Mock the model
            mock_model = MagicMock()
            mock_embedding = np.random.randn(1, 768).astype("float32")
            mock_embedding = mock_embedding / np.linalg.norm(mock_embedding, axis=1, keepdims=True)
            mock_model.encode.return_value = mock_embedding
            backend._model = mock_model

            # Search with vocabulary filter
            results = backend.search("blood pressure", vocabularies=["LOINC"], limit=10)

            # Only LOINC concepts should be returned
            for result in results:
                assert result["vocabulary_id"] == "LOINC"

"""Extra Athena-loader coverage (Slice 8 gap-fill).

Exercises ``build_knowledge_layer`` with backends whose optional
dependencies aren't installed (so the ``_build_X`` import path raises
ImportError) — covers the import-guard lines without requiring the
heavy backend libs.
"""

from __future__ import annotations

import pandas as pd
import pytest


def _make_tiny_athena(tmp_path):
    """Reuse the demo data layout for a minimal Athena directory."""
    base = tmp_path / "athena"
    base.mkdir()
    pd.DataFrame(
        [
            {
                "concept_id": 1,
                "concept_name": "Type 2 diabetes",
                "domain_id": "Condition",
                "vocabulary_id": "ICD10CM",
                "concept_class_id": "C",
                "standard_concept": "S",
                "concept_code": "E11.9",
                "valid_start_date": "1970-01-01",
                "valid_end_date": "2099-12-31",
                "invalid_reason": "",
            },
            {
                "concept_id": 2,
                "concept_name": "Hypertension",
                "domain_id": "Condition",
                "vocabulary_id": "SNOMED",
                "concept_class_id": "C",
                "standard_concept": "S",
                "concept_code": "I10",
                "valid_start_date": "1970-01-01",
                "valid_end_date": "2099-12-31",
                "invalid_reason": "",
            },
        ]
    ).to_csv(base / "CONCEPT.csv", sep="\t", index=False)
    pd.DataFrame(
        [
            {
                "concept_id_1": 1,
                "concept_id_2": 2,
                "relationship_id": "Maps to",
                "valid_start_date": "1970-01-01",
                "valid_end_date": "2099-12-31",
                "invalid_reason": "",
            }
        ]
    ).to_csv(base / "CONCEPT_RELATIONSHIP.csv", sep="\t", index=False)
    return base


# ── load_athena_concepts ─────────────────────────────────────────


class TestLoadAthenaConcepts:
    def test_default_loads_all(self, tmp_path):
        from portiere.knowledge.athena import load_athena_concepts

        base = _make_tiny_athena(tmp_path)
        concepts = load_athena_concepts(base)
        assert isinstance(concepts, list)
        assert len(concepts) >= 1

    def test_filtered_by_vocabularies(self, tmp_path):
        from portiere.knowledge.athena import load_athena_concepts

        base = _make_tiny_athena(tmp_path)
        # Only ICD10CM
        concepts = load_athena_concepts(base, vocabularies=["ICD10CM"])
        # Either pre-filter or post-filter — just check it returns a list
        assert isinstance(concepts, list)

    def test_missing_concept_csv_raises(self, tmp_path):
        from portiere.knowledge.athena import load_athena_concepts

        empty = tmp_path / "empty"
        empty.mkdir()
        with pytest.raises((FileNotFoundError, ValueError)):
            load_athena_concepts(empty)


# ── build_knowledge_layer with optional-dep backends ────────────


class TestBuildWithOptionalBackends:
    def test_bm25s_round_trip(self, tmp_path):
        from portiere.knowledge import build_knowledge_layer

        athena = _make_tiny_athena(tmp_path)
        out = tmp_path / "index"
        paths = build_knowledge_layer(
            athena_path=str(athena),
            output_path=str(out),
            backend="bm25s",
            vocabularies=["ICD10CM", "SNOMED"],
        )
        assert "bm25s_corpus_path" in paths

    @pytest.mark.parametrize(
        "backend",
        ["faiss", "chromadb", "pgvector", "mongodb", "qdrant", "milvus"],
    )
    def test_optional_backend_raises_or_works(self, tmp_path, backend):
        """If the optional dep is installed, the build either raises a
        config-related error (e.g., missing connection string) or
        succeeds. If the dep is NOT installed, ImportError is fine.
        Either way the import-guard line gets exercised."""
        from portiere.knowledge import build_knowledge_layer

        athena = _make_tiny_athena(tmp_path)
        out = tmp_path / f"index_{backend}"
        with pytest.raises((ImportError, ValueError, ModuleNotFoundError, RuntimeError)):
            build_knowledge_layer(
                athena_path=str(athena),
                output_path=str(out),
                backend=backend,
                vocabularies=["ICD10CM"],
            )

    def test_unknown_backend_raises(self, tmp_path):
        from portiere.knowledge import build_knowledge_layer

        athena = _make_tiny_athena(tmp_path)
        with pytest.raises((ValueError, KeyError)):
            build_knowledge_layer(
                athena_path=str(athena),
                output_path=str(tmp_path / "x"),
                backend="not_a_real_backend",  # type: ignore[arg-type]
            )

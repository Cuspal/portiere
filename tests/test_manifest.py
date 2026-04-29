"""Tests for the reproducibility manifest schema (Slice 4 Task 4.1)."""

from __future__ import annotations

import json

import pytest


class TestManifestSchema:
    """Manifest is a Pydantic model with strict (extra-forbid) field set."""

    def test_required_fields(self):
        from portiere.repro.manifest import EmbeddingFingerprint, Manifest, RunInfo

        m = Manifest(
            run=RunInfo(run_id="abc123", started_at="2026-04-29T12:00:00+00:00"),
            portiere_version="0.2.0",
            python_version="3.12.1",
            os_string="Darwin-25.3.0-arm64",
            git_sha=None,
            git_dirty=None,
            project_name="test",
            target_model="omop_cdm_v5.4",
            embedding=EmbeddingFingerprint(name="sapbert", hf_revision=None, dimension=768),
        )
        assert m.manifest_version == "1"
        assert m.run.run_id == "abc123"
        assert m.target_model == "omop_cdm_v5.4"
        assert m.task == "standardize"  # default
        assert m.source_standard is None  # default

    def test_extra_fields_forbidden(self):
        import pydantic

        from portiere.repro.manifest import Manifest

        with pytest.raises(pydantic.ValidationError):
            Manifest(unknown_field="should not be allowed")

    def test_json_roundtrip(self):
        from portiere.repro.manifest import (
            EmbeddingFingerprint,
            Manifest,
            RunInfo,
            StageEntry,
        )

        original = Manifest(
            run=RunInfo(run_id="r1", started_at="2026-04-29T12:00:00+00:00"),
            portiere_version="0.2.0",
            python_version="3.12.1",
            os_string="Darwin",
            git_sha="abc123def",
            git_dirty=True,
            project_name="proj",
            target_model="omop_cdm_v5.4",
            embedding=EmbeddingFingerprint(name="sapbert", hf_revision="rev1", dimension=768),
            stages=[
                StageEntry(
                    stage="ingest",
                    started_at="2026-04-29T12:00:00+00:00",
                    finished_at="2026-04-29T12:00:01+00:00",
                    inputs={"path": "patients.csv"},
                ),
            ],
        )
        # round-trip via JSON
        as_dict = original.model_dump(mode="json")
        as_json = json.dumps(as_dict)
        roundtripped = Manifest(**json.loads(as_json))
        assert roundtripped == original

    def test_cross_map_fields(self):
        """Cross-map projects record source_standard."""
        from portiere.repro.manifest import EmbeddingFingerprint, Manifest, RunInfo

        m = Manifest(
            run=RunInfo(run_id="x", started_at="2026-04-29T12:00:00+00:00"),
            portiere_version="0.2.0",
            python_version="3.12.1",
            os_string="Linux",
            git_sha=None,
            git_dirty=None,
            project_name="fhir-export",
            target_model="fhir_r4",
            task="cross_map",
            source_standard="omop_cdm_v5.4",
            embedding=EmbeddingFingerprint(name="sapbert", hf_revision=None, dimension=768),
        )
        assert m.task == "cross_map"
        assert m.source_standard == "omop_cdm_v5.4"


class TestStageEntry:
    def test_minimal(self):
        from portiere.repro.manifest import StageEntry

        s = StageEntry(
            stage="schema",
            started_at="2026-04-29T12:00:00+00:00",
            finished_at="2026-04-29T12:00:05+00:00",
        )
        assert s.stage == "schema"
        assert s.inputs == {}
        assert s.outputs == {}
        assert s.metrics == {}


class TestEmbeddingFingerprint:
    def test_required(self):
        from portiere.repro.manifest import EmbeddingFingerprint

        e = EmbeddingFingerprint(name="sapbert", dimension=768)
        assert e.name == "sapbert"
        assert e.dimension == 768
        assert e.hf_revision is None
        assert e.sha256_of_config is None

    def test_with_revision(self):
        from portiere.repro.manifest import EmbeddingFingerprint

        e = EmbeddingFingerprint(
            name="cambridgeltl/SapBERT-from-PubMedBERT-fulltext",
            hf_revision="abc123",
            sha256_of_config="def456",
            dimension=768,
        )
        assert e.hf_revision == "abc123"


class TestVocabularyFingerprint:
    def test_round_trip(self):
        from portiere.repro.manifest import VocabularyFingerprint

        v = VocabularyFingerprint(
            name="SNOMED",
            version_date="2024-09-01",
            sha256_of_source_file="abc",
            path="/data/athena/CONCEPT.csv",
        )
        assert v.name == "SNOMED"
        assert v.path == "/data/athena/CONCEPT.csv"


class TestSourceDataFingerprint:
    def test_file_source(self):
        from portiere.repro.manifest import SourceDataFingerprint

        s = SourceDataFingerprint(path="/data/x.csv", sha256="abc")
        assert s.path == "/data/x.csv"
        assert s.connection_string_redacted is None

    def test_db_source_redacted(self):
        from portiere.repro.manifest import SourceDataFingerprint

        s = SourceDataFingerprint(
            connection_string_redacted="postgresql://***@host/db",
            table_or_query="patients",
        )
        assert s.connection_string_redacted == "postgresql://***@host/db"
        # raw connection string field doesn't even exist in the schema
        assert not hasattr(s, "connection_string")

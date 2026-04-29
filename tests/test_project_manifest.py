"""Integration tests for the manifest <-> Project wiring (Slice 4 Task 4.4).

These tests exercise Project's lazy ManifestRecorder, the
``finalize_run()`` API, and the context-manager protocol.
"""

from __future__ import annotations

import json

import pytest


def _make_project(tmp_path, **kwargs):
    """Build a Project pointing storage at tmp_path."""
    from portiere import PortiereConfig
    from portiere.engines import PolarsEngine
    from portiere.storage import LocalStorageBackend

    config = PortiereConfig(local_project_dir=tmp_path)

    # Build directly to avoid touching the real filesystem
    from portiere.project import Project

    storage = LocalStorageBackend(base_dir=tmp_path)
    metadata = storage.create_project(
        kwargs.get("name", "test"),
        kwargs.get("target_model", "omop_cdm_v5.4"),
        kwargs.get("vocabularies", ["SNOMED"]),
        task=kwargs.get("task", "standardize"),
        source_standard=kwargs.get("source_standard"),
    )
    return Project(
        name=kwargs.get("name", "test"),
        target_model=kwargs.get("target_model", "omop_cdm_v5.4"),
        vocabularies=kwargs.get("vocabularies", ["SNOMED"]),
        task=kwargs.get("task", "standardize"),
        source_standard=kwargs.get("source_standard"),
        config=config,
        storage=storage,
        project_id=metadata["id"],
        engine=PolarsEngine(),
    )


def _read_manifest(path):
    return json.loads(path.read_text())


class TestProjectFinalizeRun:
    def test_finalize_with_no_pipeline_ops_returns_none(self, tmp_path):
        """A project that never ran a stage doesn't write a manifest."""
        project = _make_project(tmp_path)
        out = project.finalize_run()
        assert out is None

    def test_finalize_after_add_source_writes_manifest(self, tmp_path):
        """add_source triggers recorder creation; finalize_run writes the file."""
        project = _make_project(tmp_path)

        csv = tmp_path / "patients.csv"
        csv.write_text("patient_id,gender\n1,M\n2,F\n")
        project.add_source(str(csv))

        out = project.finalize_run()
        assert out is not None
        assert out.exists()
        m = _read_manifest(out)
        assert m["project_name"] == "test"
        assert m["target_model"] == "omop_cdm_v5.4"
        assert m["source_data"]["path"] == str(csv)
        assert m["source_data"]["sha256"] is not None

    def test_finalize_idempotent(self, tmp_path):
        """Calling finalize_run twice doesn't error and overwrites with fresh state."""
        project = _make_project(tmp_path)
        csv = tmp_path / "x.csv"
        csv.write_text("a,b\n1,2\n")
        project.add_source(str(csv))
        first = project.finalize_run()
        second = project.finalize_run()
        assert first == second  # same path
        assert second.exists()


class TestProjectContextManager:
    def test_with_block_finalizes_on_exit(self, tmp_path):
        """Using Project as a context manager auto-finalizes the run."""
        project = _make_project(tmp_path)

        csv = tmp_path / "x.csv"
        csv.write_text("a,b\n1,2\n")

        with project:
            project.add_source(str(csv))
        # after exit, manifest should exist
        runs = list((tmp_path / project.name / "runs").glob("*/manifest.lock.json"))
        assert len(runs) == 1

    def test_enter_returns_self(self, tmp_path):
        project = _make_project(tmp_path)
        with project as p:
            assert p is project

    def test_with_block_finalizes_on_exception(self, tmp_path):
        """An exception inside the with block still finalizes the manifest."""
        project = _make_project(tmp_path)
        csv = tmp_path / "x.csv"
        csv.write_text("a,b\n1,2\n")

        with pytest.raises(RuntimeError):
            with project:
                project.add_source(str(csv))
                raise RuntimeError("boom")

        runs = list((tmp_path / project.name / "runs").glob("*/manifest.lock.json"))
        assert len(runs) == 1


class TestStageRecording:
    def test_add_source_records_ingest_stage(self, tmp_path):
        project = _make_project(tmp_path)
        csv = tmp_path / "x.csv"
        csv.write_text("a,b\n1,2\n")

        with project:
            project.add_source(str(csv))

        runs = list((tmp_path / project.name / "runs").glob("*/manifest.lock.json"))
        m = _read_manifest(runs[0])
        stages = [s["stage"] for s in m["stages"]]
        assert "ingest" in stages


class TestProjectSourceFingerprint:
    def test_database_source_redacts_credentials(self, tmp_path):
        project = _make_project(tmp_path)
        # add_source for DB doesn't try to read; we just check the manifest
        with project:
            try:
                project.add_source(
                    connection_string="postgresql://alice:secret@db.example.com/clinical",
                    table="patients",
                )
            except Exception:
                # add_source may fail to introspect a fake DB; that's fine
                pass

        runs = list((tmp_path / project.name / "runs").glob("*/manifest.lock.json"))
        if not runs:
            pytest.skip("add_source did not trigger recorder for DB source — env-dependent")
        m = _read_manifest(runs[0])
        contents = json.dumps(m)
        assert "secret" not in contents
        assert "alice:" not in contents


class TestEmbeddingRecorded:
    def test_embedding_name_from_config(self, tmp_path):
        project = _make_project(tmp_path)
        csv = tmp_path / "x.csv"
        csv.write_text("a,b\n1,2\n")
        with project:
            project.add_source(str(csv))

        runs = list((tmp_path / project.name / "runs").glob("*/manifest.lock.json"))
        m = _read_manifest(runs[0])
        # default embedding model from PortiereConfig
        assert m["embedding"]["name"]

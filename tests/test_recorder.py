"""Tests for ManifestRecorder (Slice 4 Task 4.3)."""

from __future__ import annotations

import json
import subprocess

# ── Helpers ───────────────────────────────────────────────────────


def _make_recorder(tmp_path, **kwargs):
    """Standard recorder factory for tests; tmp_path is its run_dir AND
    project_root (a non-git directory unless the test inits one)."""
    from portiere.repro.recorder import ManifestRecorder

    return ManifestRecorder(
        run_dir=tmp_path / "runs" / "r1",
        project_name=kwargs.pop("project_name", "test"),
        target_model=kwargs.pop("target_model", "omop_cdm_v5.4"),
        project_root=kwargs.pop("project_root", tmp_path),
        **kwargs,
    )


def _read_manifest(path) -> dict:
    return json.loads(path.read_text())


# ── Tests ─────────────────────────────────────────────────────────


class TestRecorderInit:
    def test_creates_run_dir(self, tmp_path):
        r = _make_recorder(tmp_path)
        assert r.run_dir.exists()
        assert r.run_dir.is_dir()

    def test_run_id_is_short_hex(self, tmp_path):
        r = _make_recorder(tmp_path)
        # uuid4 hex truncated to 12 chars
        assert len(r.run_id) == 12
        assert all(c in "0123456789abcdef" for c in r.run_id)

    def test_environment_captured(self, tmp_path):
        import portiere

        r = _make_recorder(tmp_path)
        out = r.finalize()
        m = _read_manifest(out)
        assert m["portiere_version"] == portiere.__version__
        assert m["python_version"]
        assert m["os_string"]


class TestRecorderGitState:
    def test_no_git_repo_records_nulls(self, tmp_path):
        # tmp_path is not a git repo
        r = _make_recorder(tmp_path)
        out = r.finalize()
        m = _read_manifest(out)
        assert m["git_sha"] is None
        assert m["git_dirty"] is None

    def test_clean_git_repo(self, tmp_path):
        # init an empty repo, commit one file, make sure dirty=False
        subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
        subprocess.run(
            ["git", "config", "user.email", "test@example.com"], cwd=tmp_path, check=True
        )
        subprocess.run(["git", "config", "user.name", "Test"], cwd=tmp_path, check=True)
        (tmp_path / "f.txt").write_text("hi")
        subprocess.run(["git", "add", "f.txt"], cwd=tmp_path, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "init"], cwd=tmp_path, check=True)

        r = _make_recorder(tmp_path)
        out = r.finalize()
        m = _read_manifest(out)
        assert m["git_sha"] is not None
        assert len(m["git_sha"]) >= 7  # hex SHA
        assert m["git_dirty"] is False

    def test_dirty_repo_records_dirty(self, tmp_path):
        subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
        subprocess.run(
            ["git", "config", "user.email", "test@example.com"], cwd=tmp_path, check=True
        )
        subprocess.run(["git", "config", "user.name", "Test"], cwd=tmp_path, check=True)
        (tmp_path / "f.txt").write_text("hi")
        subprocess.run(["git", "add", "f.txt"], cwd=tmp_path, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "init"], cwd=tmp_path, check=True)
        # introduce uncommitted change
        (tmp_path / "f.txt").write_text("dirty")

        r = _make_recorder(tmp_path)
        out = r.finalize()
        m = _read_manifest(out)
        assert m["git_dirty"] is True


class TestRecorderEmbedding:
    def test_set_embedding(self, tmp_path):
        r = _make_recorder(tmp_path)
        r.set_embedding(
            name="cambridgeltl/SapBERT-from-PubMedBERT-fulltext",
            dimension=768,
            hf_revision="abc123",
        )
        out = r.finalize()
        m = _read_manifest(out)
        assert m["embedding"]["name"] == "cambridgeltl/SapBERT-from-PubMedBERT-fulltext"
        assert m["embedding"]["dimension"] == 768
        assert m["embedding"]["hf_revision"] == "abc123"


class TestRecorderVocabulary:
    def test_add_vocabulary_with_existing_file(self, tmp_path):
        r = _make_recorder(tmp_path)
        vocab_file = tmp_path / "concept.csv"
        vocab_file.write_text("concept_id,concept_name\n1,test\n")
        r.add_vocabulary(name="SNOMED", version_date="2024-09-01", path=str(vocab_file))
        out = r.finalize()
        m = _read_manifest(out)
        assert len(m["vocabularies"]) == 1
        v = m["vocabularies"][0]
        assert v["name"] == "SNOMED"
        assert v["version_date"] == "2024-09-01"
        assert v["sha256_of_source_file"] is not None

    def test_add_vocabulary_missing_file_skips_hash(self, tmp_path):
        r = _make_recorder(tmp_path)
        r.add_vocabulary(name="LOINC", path="/nonexistent/path.csv")
        out = r.finalize()
        m = _read_manifest(out)
        v = m["vocabularies"][0]
        assert v["sha256_of_source_file"] is None


class TestRecorderPrompts:
    def test_add_prompt_hashes_template(self, tmp_path):
        from portiere.repro.hashing import sha256_text

        r = _make_recorder(tmp_path)
        template = "You are a clinical mapping assistant..."
        r.add_prompt(name="llm_verifier", template=template)
        out = r.finalize()
        m = _read_manifest(out)
        assert len(m["prompt_templates"]) == 1
        p = m["prompt_templates"][0]
        assert p["name"] == "llm_verifier"
        assert p["sha256"] == sha256_text(template)
        # the template TEXT is never stored — only its hash
        assert template not in json.dumps(m)


class TestRecorderSourceData:
    def test_file_source_records_path_and_hash(self, tmp_path):
        r = _make_recorder(tmp_path)
        f = tmp_path / "patients.csv"
        f.write_text("id,name\n1,Alice\n")
        r.set_source_data(path=str(f))
        out = r.finalize()
        m = _read_manifest(out)
        assert m["source_data"]["path"] == str(f)
        assert m["source_data"]["sha256"] is not None

    def test_db_source_redacts_credentials(self, tmp_path):
        r = _make_recorder(tmp_path)
        r.set_source_data(
            connection_string="postgresql://alice:p@ssword@db.example.com/clinical",
            table_or_query="patients",
        )
        out = r.finalize()
        m = _read_manifest(out)
        # credentials must NOT appear in the manifest
        contents = json.dumps(m)
        assert "p@ssword" not in contents
        assert "alice:" not in contents
        # but the redacted form should be there
        assert m["source_data"]["connection_string_redacted"] is not None
        assert m["source_data"]["table_or_query"] == "patients"
        assert m["source_data"]["path"] is None

    def test_db_source_keeps_host_and_db(self, tmp_path):
        r = _make_recorder(tmp_path)
        r.set_source_data(
            connection_string="postgresql://user:pw@db.example.com/clinical",
            table_or_query="patients",
        )
        out = r.finalize()
        m = _read_manifest(out)
        assert "db.example.com" in m["source_data"]["connection_string_redacted"]


class TestRecorderStages:
    def test_record_stage_appends(self, tmp_path):
        r = _make_recorder(tmp_path)
        r.record_stage("ingest", inputs={"path": "x.csv"}, outputs={"row_count": 100})
        r.record_stage("schema", metrics={"auto_accepted": 7})
        out = r.finalize()
        m = _read_manifest(out)
        assert len(m["stages"]) == 2
        assert m["stages"][0]["stage"] == "ingest"
        assert m["stages"][0]["inputs"] == {"path": "x.csv"}
        assert m["stages"][1]["stage"] == "schema"
        assert m["stages"][1]["metrics"] == {"auto_accepted": 7}


class TestRecorderFinalize:
    def test_writes_to_run_dir(self, tmp_path):
        r = _make_recorder(tmp_path)
        out = r.finalize()
        assert out.name == "manifest.lock.json"
        assert out.parent == r.run_dir
        assert out.exists()

    def test_finalized_manifest_has_finished_at_and_duration(self, tmp_path):
        r = _make_recorder(tmp_path)
        out = r.finalize()
        m = _read_manifest(out)
        assert m["run"]["finished_at"] is not None
        assert m["run"]["duration_seconds"] is not None
        assert m["run"]["duration_seconds"] >= 0


class TestRecorderNoSecretLeak:
    def test_api_key_in_env_does_not_appear(self, tmp_path, monkeypatch):
        """Belt-and-suspenders: API keys in env must NEVER end up in the manifest."""
        monkeypatch.setenv("OPENAI_API_KEY", "sk-FAKEKEY-9999999999")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-FAKE-1111111111")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "AWSFAKE/1234567890")

        r = _make_recorder(tmp_path)
        out = r.finalize()
        contents = out.read_text()
        assert "sk-FAKEKEY-9999999999" not in contents
        assert "sk-ant-FAKE-1111111111" not in contents
        assert "AWSFAKE/1234567890" not in contents


class TestRecorderThresholds:
    def test_set_thresholds(self, tmp_path):
        r = _make_recorder(tmp_path)
        r.set_thresholds({"auto_accept": 0.95, "needs_review": 0.70})
        out = r.finalize()
        m = _read_manifest(out)
        assert m["thresholds"] == {"auto_accept": 0.95, "needs_review": 0.70}


class TestRecorderProjectFields:
    def test_cross_map_fields_recorded(self, tmp_path):
        r = _make_recorder(
            tmp_path,
            target_model="fhir_r4",
            task="cross_map",
            source_standard="omop_cdm_v5.4",
            vocabularies_requested=["SNOMED", "LOINC"],
        )
        out = r.finalize()
        m = _read_manifest(out)
        assert m["target_model"] == "fhir_r4"
        assert m["task"] == "cross_map"
        assert m["source_standard"] == "omop_cdm_v5.4"
        assert m["vocabularies_requested"] == ["SNOMED", "LOINC"]

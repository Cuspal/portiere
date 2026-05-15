"""Targeted coverage fill for v0.3.1 modules (Slice 7).

These tests exercise specific lines/branches that the feature-slice tests
left uncovered. They keep behavior assertions tight — coverage incidentally
follows.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# ── mcode.py ──────────────────────────────────────────────────────


class TestMcodeRequiredFields:
    def test_check_required_fields_flags_missing_top_level_required(self):
        from portiere.quality.fhir_profile.mcode import _check_required_fields

        # Synthetic SD with one required top-level field
        sd = {
            "snapshot": {
                "element": [
                    {"id": "Patient", "min": 0},
                    {"id": "Patient.identifier", "min": 1, "max": "*"},
                    {"id": "Patient.identifier.system", "min": 1},  # nested, skipped
                    {"id": "Patient.name", "min": 0},  # not required, skipped
                ]
            }
        }
        resource = {"resourceType": "Patient", "id": "p1"}  # no identifier
        failures = _check_required_fields(resource, sd, resource_index=0)
        assert len(failures) == 1
        assert "identifier" in failures[0].invariant_id

    def test_check_required_fields_passes_when_present(self):
        from portiere.quality.fhir_profile.mcode import _check_required_fields

        sd = {
            "snapshot": {
                "element": [
                    {"id": "Patient.identifier", "min": 1},
                ]
            }
        }
        resource = {"resourceType": "Patient", "identifier": [{"value": "x"}]}
        assert _check_required_fields(resource, sd, resource_index=0) == []


class TestMcodeInvariantCollection:
    def test_collect_mcode_invariants_picks_only_mcode_keys(self):
        from portiere.quality.fhir_profile.mcode import _collect_mcode_invariants

        sd = {
            "snapshot": {
                "element": [
                    {
                        "id": "Patient",
                        "constraint": [
                            {"key": "dom-1", "expression": "x.exists()", "severity": "error"},
                            {
                                "key": "mcode-1",
                                "expression": "active.exists()",
                                "severity": "error",
                                "human": "Patient must have active",
                            },
                        ],
                    },
                    {
                        "id": "Patient.name",
                        "constraint": [
                            {
                                "key": "mcode-2",
                                "expression": "family.exists()",
                                "severity": "warning",
                                "human": "Family name recommended",
                            }
                        ],
                    },
                ]
            }
        }
        invariants = _collect_mcode_invariants(sd)
        assert len(invariants) == 2
        assert {i["id"] for i in invariants} == {"mcode-1", "mcode-2"}
        # Element id is preserved for context-aware evaluation
        nested = next(i for i in invariants if i["id"] == "mcode-2")
        assert nested["elem_id"] == "Patient.name"


class TestMcodeOrchestratorInvariantPath:
    def test_orchestrator_runs_synthetic_mcode_invariant(self, monkeypatch):
        """Exercise the invariant evaluation path in validate_against_mcode."""
        pytest.importorskip("fhirpathpy")
        from portiere.quality.fhir_profile import mcode as mcode_mod

        # Fake SD with one mcode-* invariant on a list field
        fake_sd = {
            "snapshot": {
                "element": [
                    {"id": "Patient.identifier", "min": 1},
                    {
                        "id": "Patient.name",
                        "constraint": [
                            {
                                "key": "mcode-name-1",
                                "expression": "family.exists()",
                                "severity": "error",
                                "human": "Family name required",
                            }
                        ],
                    },
                ]
            }
        }
        monkeypatch.setattr(mcode_mod, "_load_sd", lambda stem: fake_sd)

        # Resource without family on any name → invariant fails on each name
        resources = [
            {
                "resourceType": "Patient",
                "id": "p1",
                "meta": {
                    "profile": [
                        "http://hl7.org/fhir/us/mcode/StructureDefinition/mcode-cancer-patient"
                    ]
                },
                "identifier": [{"value": "x"}],
                "name": [{"given": ["Jane"]}],  # no family
            }
        ]
        report = mcode_mod.validate_against_mcode(resources)
        # Schema validation may also flag; we just need the invariant path to run.
        assert report.total_resources == 1
        # The invariant should have been collected and evaluated.
        ids = {f.invariant_id for f in report.failures}
        assert "mcode-name-1" in ids

    def test_orchestrator_evaluates_invariant_on_dict_field(self, monkeypatch):
        """Single-value (non-list) child field is wrapped into a single-element context."""
        pytest.importorskip("fhirpathpy")
        from portiere.quality.fhir_profile import mcode as mcode_mod

        fake_sd = {
            "snapshot": {
                "element": [
                    {
                        "id": "Observation.subject",
                        "constraint": [
                            {
                                "key": "mcode-subj-1",
                                "expression": "reference.exists()",
                                "severity": "error",
                            }
                        ],
                    }
                ]
            }
        }
        monkeypatch.setattr(mcode_mod, "_load_sd", lambda stem: fake_sd)

        resources = [
            {
                "resourceType": "Observation",
                "id": "o1",
                "meta": {
                    "profile": [
                        "http://hl7.org/fhir/us/mcode/StructureDefinition/mcode-cancer-disease-status"
                    ]
                },
                "subject": {"reference": "Patient/p1"},  # single dict, not a list
            }
        ]
        report = mcode_mod.validate_against_mcode(resources)
        # subject.reference exists → no failure for this invariant
        ids = {f.invariant_id for f in report.failures}
        assert "mcode-subj-1" not in ids


# ── usagi_baseline.py ─────────────────────────────────────────────


class TestUsagiUnavailablePaths:
    def test_run_usagi_raises_when_java_missing(self, tmp_path, monkeypatch):
        from portiere.benchmarks.athena_icd_snomed.usagi_baseline import (
            UsagiUnavailableError,
            run_usagi,
        )

        monkeypatch.setattr("shutil.which", lambda name: None)
        with pytest.raises(UsagiUnavailableError, match=r"Java"):
            run_usagi(
                input_rows=[],
                athena_concept_csv=tmp_path / "CONCEPT.csv",
                usagi_jar=tmp_path / "usagi.jar",
            )

    def test_run_usagi_raises_when_jar_missing(self, tmp_path, monkeypatch):
        from portiere.benchmarks.athena_icd_snomed.usagi_baseline import (
            UsagiUnavailableError,
            run_usagi,
        )

        # Java available, but JAR doesn't exist
        monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/java")
        missing_jar = tmp_path / "nope.jar"
        with pytest.raises(UsagiUnavailableError, match=r"JAR missing"):
            run_usagi(
                input_rows=[],
                athena_concept_csv=tmp_path / "CONCEPT.csv",
                usagi_jar=missing_jar,
            )

    def test_run_usagi_invokes_subprocess_when_deps_present(self, tmp_path, monkeypatch):
        """Exercise the subprocess.run path without actually launching Java."""
        import subprocess as sp

        from portiere.benchmarks.athena_icd_snomed import usagi_baseline as ub

        monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/java")

        # Synthetic JAR file + Athena CSV
        jar = tmp_path / "usagi.jar"
        jar.write_bytes(b"fake jar bytes")
        concept_csv = tmp_path / "CONCEPT.csv"
        concept_csv.write_text("concept_id\tconcept_name\n")

        # Capture the subprocess call and write a fake output CSV the parser can read
        captured = {}

        def _fake_run(cmd, check, capture_output):
            captured["cmd"] = cmd
            # write a synthetic USAGI output that the parser will consume
            out_path = Path(cmd[cmd.index("--output") + 1])
            out_path.write_text("source_code\ttarget_concept_id\tmatch_score\nE11.9\t200\t0.9\n")
            return sp.CompletedProcess(cmd, 0, b"", b"")

        monkeypatch.setattr(sp, "run", _fake_run)

        predictions = ub.run_usagi(
            input_rows=[{"concept_id": 100, "concept_code": "E11.9", "concept_name": "T2DM"}],
            athena_concept_csv=concept_csv,
            usagi_jar=jar,
            work_dir=tmp_path / "work",
        )
        assert predictions == {100: [200]}
        # The cmd should include the JAR path and --batch
        assert "--batch" in captured["cmd"]
        assert str(jar) in captured["cmd"]


class TestUsagiOutputParseEdgeCases:
    def test_parse_skips_non_numeric_rows(self, tmp_path):
        from portiere.benchmarks.athena_icd_snomed.usagi_baseline import parse_usagi_output

        bad = tmp_path / "out.csv"
        bad.write_text(
            "source_code\ttarget_concept_id\tmatch_score\n"
            "E11.9\tNOT_AN_INT\t0.9\n"
            "I10\t250\tNOT_A_FLOAT\n"
            "J45.909\t300\t0.8\n"
        )
        predictions = parse_usagi_output(bad, {"E11.9": 100, "I10": 101, "J45.909": 102})
        # Only the well-formed J45.909 row should land
        assert predictions == {102: [300]}


# ── replay.py auto_replay paths ───────────────────────────────────


def _write_manifest(
    tmp_path: Path,
    src: Path,
    *,
    stages: list[dict] | None = None,
) -> Path:
    import json

    from portiere.repro.hashing import sha256_file

    manifest = {
        "manifest_version": "1",
        "run": {
            "run_id": "cov-7",
            "started_at": "2026-05-12T00:00:00+00:00",
            "finished_at": "2026-05-12T00:01:00+00:00",
            "duration_seconds": 60.0,
        },
        "portiere_version": "0.3.1",
        "python_version": "3.12.1",
        "os_string": "TestOS",
        "git_sha": None,
        "git_dirty": None,
        "project_name": "cov-target",
        "target_model": "omop_cdm_v5.4",
        "task": "standardize",
        "source_standard": None,
        "vocabularies_requested": [],
        "embedding": {
            "name": "sapbert",
            "hf_revision": None,
            "sha256_of_config": None,
            "dimension": 768,
        },
        "knowledge_backend": None,
        "vocabularies": [],
        "prompt_templates": [],
        "thresholds": {},
        "source_data": {
            "path": str(src),
            "sha256": sha256_file(src) if src.exists() else "deadbeef",
            "connection_string_redacted": None,
            "table_or_query": None,
        },
        "stages": stages or [],
    }
    out = tmp_path / "manifest.lock.json"
    out.write_text(json.dumps(manifest, indent=2))
    return out


def _stage(name: str, **kw) -> dict:
    return {
        "stage": name,
        "started_at": "2026-05-12T00:00:00+00:00",
        "finished_at": "2026-05-12T00:00:01+00:00",
        "inputs": kw.get("inputs", {}),
        "outputs": kw.get("outputs", {}),
        "metrics": kw.get("metrics", {}),
    }


class TestAutoReplayValidateRerun:
    def test_validate_stage_unavailable_when_output_path_missing(self, tmp_path):
        from portiere.repro.replay import auto_replay

        src = tmp_path / "src.csv"
        src.write_text("a,b\n1,2\n")
        manifest_path = _write_manifest(
            tmp_path,
            src,
            stages=[
                _stage(
                    "validate",
                    inputs={"output_path": "/nonexistent/dir"},
                    metrics={"all_passed": True},
                )
            ],
        )
        report = auto_replay(manifest_path)
        assert len(report.per_stage) == 1
        assert report.per_stage[0].passed is None
        assert "unavailable" in report.per_stage[0].reason.lower()

    def test_validate_stage_no_output_path_unavailable(self, tmp_path):
        from portiere.repro.replay import auto_replay

        src = tmp_path / "src.csv"
        src.write_text("a,b\n1,2\n")
        manifest_path = _write_manifest(
            tmp_path,
            src,
            stages=[_stage("validate", metrics={"all_passed": True})],
        )
        report = auto_replay(manifest_path)
        assert report.per_stage[0].passed is None

    def test_unknown_stage_name_marked_unavailable(self, tmp_path):
        from portiere.repro.replay import auto_replay

        src = tmp_path / "src.csv"
        src.write_text("a,b\n1,2\n")
        manifest_path = _write_manifest(
            tmp_path,
            src,
            stages=[_stage("zoinks", outputs={"x": 1})],
        )
        report = auto_replay(manifest_path)
        assert len(report.per_stage) == 1
        assert report.per_stage[0].passed is None
        assert "no comparator" in report.per_stage[0].reason.lower()

    def test_ingest_passes_then_validate_unavailable_does_not_stop_loop(self, tmp_path):
        """UNAVAILABLE is not a hard fail — later stages are still attempted."""
        from portiere.repro.replay import auto_replay

        src = tmp_path / "src.csv"
        src.write_text("a,b\n1,2\n")
        manifest_path = _write_manifest(
            tmp_path,
            src,
            stages=[
                _stage("ingest", inputs={"format": "csv"}, outputs={"row_count": 1}),
                _stage(
                    "validate",
                    inputs={"output_path": "/nope"},
                    metrics={"all_passed": True},
                ),
            ],
        )
        report = auto_replay(manifest_path)
        # Both attempts are recorded; report.passed True because neither is False
        assert len(report.per_stage) == 2
        assert report.passed is True

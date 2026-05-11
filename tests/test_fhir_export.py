"""Tests for FHIR Bundle / NDJSON export (Slice 4, v0.3.0)."""

from __future__ import annotations

import json

import pytest


class TestBundleExport:
    def test_empty_input_produces_empty_transaction_bundle(self):
        from portiere.export.fhir.bundle import to_transaction_bundle

        bundle = to_transaction_bundle([])
        assert bundle["resourceType"] == "Bundle"
        assert bundle["type"] == "transaction"
        assert bundle["entry"] == []

    def test_single_patient_produces_one_entry(self):
        from portiere.export.fhir.bundle import to_transaction_bundle

        patient = {"resourceType": "Patient", "id": "p1", "gender": "female"}
        bundle = to_transaction_bundle([patient])

        assert len(bundle["entry"]) == 1
        entry = bundle["entry"][0]
        assert entry["resource"] == patient
        assert entry["fullUrl"].startswith("urn:uuid:")
        assert entry["request"]["method"] == "POST"
        assert entry["request"]["url"] == "Patient"

    def test_each_entry_has_unique_full_url(self):
        from portiere.export.fhir.bundle import to_transaction_bundle

        resources = [
            {"resourceType": "Patient", "id": "p1"},
            {"resourceType": "Observation", "id": "o1", "status": "final"},
        ]
        bundle = to_transaction_bundle(resources)
        full_urls = [e["fullUrl"] for e in bundle["entry"]]
        assert len(set(full_urls)) == 2

    def test_resource_type_required(self):
        from portiere.export.fhir.bundle import to_transaction_bundle

        with pytest.raises(ValueError, match="resourceType"):
            to_transaction_bundle([{"id": "x"}])


class TestNdjsonExport:
    def test_resources_grouped_by_type(self, tmp_path):
        from portiere.export.fhir.ndjson import to_ndjson_files

        resources = [
            {"resourceType": "Patient", "id": "p1"},
            {"resourceType": "Patient", "id": "p2"},
            {"resourceType": "Observation", "id": "o1", "status": "final"},
        ]
        to_ndjson_files(resources, out_dir=tmp_path)

        assert (tmp_path / "Patient.ndjson").exists()
        assert (tmp_path / "Observation.ndjson").exists()
        patient_lines = (tmp_path / "Patient.ndjson").read_text().strip().splitlines()
        assert len(patient_lines) == 2
        assert {json.loads(line)["id"] for line in patient_lines} == {"p1", "p2"}
        obs_lines = (tmp_path / "Observation.ndjson").read_text().strip().splitlines()
        assert len(obs_lines) == 1

    def test_returns_list_of_paths(self, tmp_path):
        from portiere.export.fhir.ndjson import to_ndjson_files

        resources = [
            {"resourceType": "Patient", "id": "p1"},
            {"resourceType": "Observation", "id": "o1", "status": "final"},
        ]
        files = to_ndjson_files(resources, out_dir=tmp_path)
        assert sorted(p.name for p in files) == ["Observation.ndjson", "Patient.ndjson"]

    def test_empty_input_writes_nothing(self, tmp_path):
        from portiere.export.fhir.ndjson import to_ndjson_files

        files = to_ndjson_files([], out_dir=tmp_path)
        assert files == []
        assert list(tmp_path.iterdir()) == []


class TestExportCli:
    def test_export_command_registered(self):
        from portiere.cli import cli

        assert "export" in cli.commands

    def test_bundle_format_writes_single_json(self, tmp_path):
        from click.testing import CliRunner

        from portiere.cli import cli

        resources_file = tmp_path / "resources.json"
        resources_file.write_text(
            json.dumps(
                [
                    {"resourceType": "Patient", "id": "p1"},
                    {"resourceType": "Observation", "id": "o1", "status": "final"},
                ]
            )
        )
        out_file = tmp_path / "bundle.json"

        result = CliRunner().invoke(
            cli,
            [
                "export",
                "--input",
                str(resources_file),
                "--format",
                "bundle",
                "--out",
                str(out_file),
            ],
        )
        assert result.exit_code == 0, result.output
        assert out_file.exists()
        bundle = json.loads(out_file.read_text())
        assert bundle["resourceType"] == "Bundle"
        assert len(bundle["entry"]) == 2

    def test_ndjson_format_writes_directory(self, tmp_path):
        from click.testing import CliRunner

        from portiere.cli import cli

        resources_file = tmp_path / "resources.json"
        resources_file.write_text(
            json.dumps(
                [
                    {"resourceType": "Patient", "id": "p1"},
                    {"resourceType": "Observation", "id": "o1", "status": "final"},
                ]
            )
        )
        out_dir = tmp_path / "ndjson_out"

        result = CliRunner().invoke(
            cli,
            [
                "export",
                "--input",
                str(resources_file),
                "--format",
                "ndjson",
                "--out",
                str(out_dir),
            ],
        )
        assert result.exit_code == 0, result.output
        assert (out_dir / "Patient.ndjson").exists()
        assert (out_dir / "Observation.ndjson").exists()


class TestFhirExportRoundTrip:
    def test_bundle_round_trips_through_fhir_resources(self):
        """Exported Bundle parses back as a fhir.resources Bundle model."""
        pytest.importorskip("fhir.resources")
        from fhir.resources.bundle import Bundle

        from portiere.export.fhir.bundle import to_transaction_bundle

        resources = [
            {
                "resourceType": "Patient",
                "id": "p1",
                "identifier": [{"system": "urn:oid:1", "value": "x"}],
                "name": [{"family": "Doe"}],
                "gender": "female",
            }
        ]
        bundle_dict = to_transaction_bundle(resources)
        bundle_model = Bundle.model_validate(bundle_dict)
        assert bundle_model.type == "transaction"
        assert len(bundle_model.entry) == 1
        assert bundle_model.entry[0].request.method == "POST"

    def test_ndjson_round_trips(self, tmp_path):
        """NDJSON output parses back, one resource per line."""
        pytest.importorskip("fhir.resources")
        from fhir.resources import get_fhir_model_class

        from portiere.export.fhir.ndjson import to_ndjson_files

        resources = [
            {
                "resourceType": "Patient",
                "id": "p1",
                "identifier": [{"system": "urn:oid:1", "value": "x"}],
                "name": [{"family": "Doe"}],
                "gender": "female",
            }
        ]
        files = to_ndjson_files(resources, out_dir=tmp_path)
        for f in files:
            for line in f.read_text().strip().splitlines():
                resource = json.loads(line)
                model_cls = get_fhir_model_class(resource["resourceType"])
                model_cls.model_validate(resource)

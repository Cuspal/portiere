"""Tests for mCODE 2.0.0 profile validator."""

from __future__ import annotations

import pytest


class TestMcodeOrchestrator:
    def test_validate_passes_for_compliant_cancer_patient(self):
        pytest.importorskip("fhir.resources")
        pytest.importorskip("fhirpathpy")
        from portiere.quality.fhir_profile.mcode import validate_against_mcode

        resources = [
            {
                "resourceType": "Patient",
                "id": "p1",
                "meta": {
                    "profile": [
                        "http://hl7.org/fhir/us/mcode/StructureDefinition/mcode-cancer-patient"
                    ]
                },
                "identifier": [{"system": "urn:oid:1", "value": "x"}],
                "name": [{"family": "Doe", "given": ["Jane"]}],
                "gender": "female",
                "birthDate": "1960-01-15",
            }
        ]
        report = validate_against_mcode(resources)
        assert report.profile == "mcode-2.0.0"
        assert report.total_resources == 1

    def test_non_mcode_resource_is_skipped(self):
        pytest.importorskip("fhir.resources")
        pytest.importorskip("fhirpathpy")
        from portiere.quality.fhir_profile.mcode import validate_against_mcode

        # A plain Patient without meta.profile claiming mCODE -> skipped
        resources = [
            {"resourceType": "Patient", "id": "p1", "gender": "female"},
        ]
        report = validate_against_mcode(resources)
        assert report.passed is True
        assert "Patient" in report.skipped

    def test_bundle_is_skipped(self):
        pytest.importorskip("fhir.resources")
        pytest.importorskip("fhirpathpy")
        from portiere.quality.fhir_profile.mcode import validate_against_mcode

        resources = [{"resourceType": "Bundle", "id": "b1", "type": "collection"}]
        report = validate_against_mcode(resources)
        assert report.passed is True
        assert "Bundle" in report.skipped

    def test_primary_cancer_condition_recognized(self):
        pytest.importorskip("fhir.resources")
        pytest.importorskip("fhirpathpy")
        from portiere.quality.fhir_profile.mcode import validate_against_mcode

        resources = [
            {
                "resourceType": "Condition",
                "id": "c1",
                "meta": {
                    "profile": [
                        "http://hl7.org/fhir/us/mcode/StructureDefinition/mcode-primary-cancer-condition"
                    ]
                },
                "clinicalStatus": {"coding": [{"system": "x", "code": "active"}]},
                "verificationStatus": {"coding": [{"system": "x", "code": "confirmed"}]},
                "category": [{"coding": [{"system": "x", "code": "problem-list-item"}]}],
                "code": {"coding": [{"system": "x", "code": "C50.0"}]},
                "subject": {"reference": "Patient/p1"},
            }
        ]
        report = validate_against_mcode(resources)
        # Resource is recognized as mCODE — not in skipped
        assert "Condition" not in report.skipped
        assert report.total_resources == 1


class TestMcodeRegistration:
    def test_mcode_exported_from_quality_fhir_profile_package(self):
        from portiere.quality.fhir_profile import validate_against_mcode

        assert callable(validate_against_mcode)

    def test_project_validate_accepts_mcode_profile(self):
        pytest.importorskip("fhir.resources")
        pytest.importorskip("fhirpathpy")
        from portiere.models.project import Project

        project = Project(id="p1", name="test")
        report = project.validate(fhir_profile="mcode-2.0.0", resources=[])
        assert report.profile == "mcode-2.0.0"

    def test_project_validate_rejects_unknown_profile(self):
        from portiere.models.project import Project

        project = Project(id="p1", name="test")
        with pytest.raises(ValueError, match=r"[Uu]nsupported"):
            project.validate(fhir_profile="bogus-1.0", resources=[])


class TestValidateCliAcceptsMcode:
    def test_validate_cli_accepts_mcode_profile(self, tmp_path):
        pytest.importorskip("fhir.resources")
        pytest.importorskip("fhirpathpy")
        import json

        from click.testing import CliRunner

        from portiere.cli import cli

        resources_file = tmp_path / "resources.json"
        resources_file.write_text(
            json.dumps([{"resourceType": "Bundle", "id": "b1", "type": "collection"}])
        )

        result = CliRunner().invoke(
            cli,
            [
                "validate",
                "--fhir-profile",
                "mcode-2.0.0",
                "--input",
                str(resources_file),
            ],
        )
        assert result.exit_code == 0
        assert "mcode-2.0.0" in result.output


class TestExportCliAcceptsMcode:
    def test_export_cli_accepts_mcode_profile(self, tmp_path):
        pytest.importorskip("fhir.resources")
        pytest.importorskip("fhirpathpy")
        import json

        from click.testing import CliRunner

        from portiere.cli import cli

        resources_file = tmp_path / "resources.json"
        resources_file.write_text(
            json.dumps([{"resourceType": "Bundle", "id": "b1", "type": "collection"}])
        )

        result = CliRunner().invoke(
            cli,
            [
                "export",
                "--input",
                str(resources_file),
                "--format",
                "bundle",
                "--out",
                str(tmp_path / "bundle.json"),
                "--fhir-profile",
                "mcode-2.0.0",
            ],
        )
        assert result.exit_code == 0, result.output

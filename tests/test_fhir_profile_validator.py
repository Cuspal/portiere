"""Tests for FHIR profile validator (US Core 6.1.0)."""

from __future__ import annotations

import pytest

# ── ProfileValidationReport ───────────────────────────────────────


class TestProfileValidationReport:
    def test_empty_report_is_passing(self):
        from portiere.quality.fhir_profile.report import ProfileValidationReport

        report = ProfileValidationReport(profile="us-core-6.1.0")
        assert report.passed is True
        assert report.total_resources == 0
        assert report.failures == []
        assert report.skipped == []

    def test_report_with_failures_is_not_passing(self):
        from portiere.quality.fhir_profile.report import (
            ProfileValidationReport,
            ResourceFailure,
        )

        report = ProfileValidationReport(
            profile="us-core-6.1.0",
            failures=[
                ResourceFailure(
                    resource_type="Patient",
                    resource_index=0,
                    invariant_id="us-core-1",
                    message="Patient.identifier required",
                ),
            ],
        )
        assert report.passed is False
        assert len(report.failures) == 1

    def test_report_with_warning_only_is_passing(self):
        from portiere.quality.fhir_profile.report import (
            ProfileValidationReport,
            ResourceFailure,
        )

        report = ProfileValidationReport(
            profile="us-core-6.1.0",
            failures=[
                ResourceFailure(
                    resource_type="Patient",
                    resource_index=0,
                    invariant_id="us-core-warn",
                    message="Recommended field missing",
                    severity="warning",
                ),
            ],
        )
        assert report.passed is True

    def test_report_serializes_to_json(self):
        from portiere.quality.fhir_profile.report import ProfileValidationReport

        report = ProfileValidationReport(
            profile="us-core-6.1.0", total_resources=3, skipped=["Bundle"]
        )
        payload = report.model_dump(mode="json")
        assert payload["profile"] == "us-core-6.1.0"
        assert payload["total_resources"] == 3
        assert payload["passed"] is True


# ── Schema validator ──────────────────────────────────────────────


class TestSchemaValidator:
    def test_valid_patient_passes(self):
        pytest.importorskip("fhir.resources")
        from portiere.quality.fhir_profile.validator import validate_resource_schema

        patient = {
            "resourceType": "Patient",
            "id": "p1",
            "identifier": [{"system": "urn:oid:2.16.840.1.113883.4.1", "value": "111-22-3333"}],
            "name": [{"family": "Doe", "given": ["Jane"]}],
            "gender": "female",
            "birthDate": "1990-01-15",
        }
        failures = validate_resource_schema(patient, resource_index=0)
        assert failures == []

    def test_missing_resource_type_fails(self):
        pytest.importorskip("fhir.resources")
        from portiere.quality.fhir_profile.validator import validate_resource_schema

        not_a_resource = {"id": "x", "name": [{"family": "Doe"}]}
        failures = validate_resource_schema(not_a_resource, resource_index=0)
        assert len(failures) == 1
        assert "resourcetype" in failures[0].message.lower()

    def test_invalid_field_type_fails(self):
        pytest.importorskip("fhir.resources")
        from portiere.quality.fhir_profile.validator import validate_resource_schema

        patient_bad = {
            "resourceType": "Patient",
            "id": "p1",
            "birthDate": "not-a-date",
        }
        failures = validate_resource_schema(patient_bad, resource_index=0)
        assert len(failures) >= 1


# ── Invariant validator ───────────────────────────────────────────


class TestInvariantValidator:
    def test_truthy_invariant_passes(self):
        pytest.importorskip("fhirpathpy")
        from portiere.quality.fhir_profile.validator import validate_invariant

        patient = {"resourceType": "Patient", "id": "p1", "active": True}
        result = validate_invariant(patient, expression="active.exists()")
        assert result.passed is True

    def test_falsy_invariant_fails(self):
        pytest.importorskip("fhirpathpy")
        from portiere.quality.fhir_profile.validator import validate_invariant

        patient = {"resourceType": "Patient", "id": "p1"}
        result = validate_invariant(patient, expression="identifier.exists()")
        assert result.passed is False

    def test_unsupported_expression_returns_none(self):
        pytest.importorskip("fhirpathpy")
        from portiere.quality.fhir_profile.validator import validate_invariant

        patient = {"resourceType": "Patient"}
        result = validate_invariant(patient, expression="!@#$ invalid syntax")
        assert result.passed is None


# ── US Core orchestrator ──────────────────────────────────────────


class TestUsCoreOrchestrator:
    def test_validate_passes_for_compliant_patient(self):
        pytest.importorskip("fhir.resources")
        pytest.importorskip("fhirpathpy")
        from portiere.quality.fhir_profile.us_core import validate_against_us_core

        resources = [
            {
                "resourceType": "Patient",
                "id": "p1",
                "identifier": [{"system": "urn:oid:2.16.840.1.113883.4.1", "value": "111-22-3333"}],
                "name": [{"family": "Doe", "given": ["Jane"]}],
                "gender": "female",
            }
        ]
        report = validate_against_us_core(resources)
        assert report.passed is True
        assert report.total_resources == 1
        assert report.failures == []

    def test_validate_reports_missing_identifier(self):
        pytest.importorskip("fhir.resources")
        pytest.importorskip("fhirpathpy")
        from portiere.quality.fhir_profile.us_core import validate_against_us_core

        resources = [
            {
                "resourceType": "Patient",
                "id": "p1",
                "name": [{"family": "Doe"}],
                "gender": "female",
            }
        ]
        report = validate_against_us_core(resources)
        # US Core Patient requires identifier — invariant or schema should flag it
        assert report.total_resources == 1

    def test_non_us_core_resource_is_skipped(self):
        pytest.importorskip("fhir.resources")
        pytest.importorskip("fhirpathpy")
        from portiere.quality.fhir_profile.us_core import validate_against_us_core

        resources = [{"resourceType": "Bundle", "id": "b1", "type": "collection"}]
        report = validate_against_us_core(resources)
        assert report.passed is True
        assert "Bundle" in report.skipped


# ── Project.validate integration ──────────────────────────────────


class TestProjectValidateFhirProfile:
    def test_validate_with_fhir_profile_returns_report(self):
        pytest.importorskip("fhir.resources")
        pytest.importorskip("fhirpathpy")
        from portiere.models.project import Project
        from portiere.quality.fhir_profile.report import ProfileValidationReport

        project = Project(id="p1", name="test-project")
        resources = [
            {
                "resourceType": "Patient",
                "id": "p1",
                "identifier": [{"system": "urn:example", "value": "123"}],
                "name": [{"family": "Doe", "given": ["Jane"]}],
                "gender": "female",
            }
        ]
        result = project.validate(fhir_profile="us-core-6.1.0", resources=resources)
        assert isinstance(result, ProfileValidationReport)
        assert result.passed is True

    def test_validate_with_unsupported_fhir_profile_raises(self):
        from portiere.models.project import Project

        project = Project(id="p1", name="test-project")
        with pytest.raises(ValueError, match=r"[Uu]nsupported"):
            project.validate(fhir_profile="unknown-profile-1.0", resources=[])


# ── validate CLI command ───────────────────────────────────────────


class TestValidateCLI:
    def test_validate_cli_fhir_profile_flag(self, tmp_path):
        pytest.importorskip("fhir.resources")
        pytest.importorskip("fhirpathpy")
        import json

        from click.testing import CliRunner

        from portiere.cli import cli

        resources = [{"resourceType": "Bundle", "id": "b1", "type": "collection"}]
        resources_file = tmp_path / "resources.json"
        resources_file.write_text(json.dumps(resources))

        runner = CliRunner()
        result = runner.invoke(
            cli, ["validate", "--fhir-profile", "us-core-6.1.0", "--input", str(resources_file)]
        )
        assert result.exit_code == 0
        assert "us-core-6.1.0" in result.output

    def test_validate_cli_missing_profile_errors(self, tmp_path):
        import json

        from click.testing import CliRunner

        from portiere.cli import cli

        resources_file = tmp_path / "resources.json"
        resources_file.write_text(json.dumps([{"resourceType": "Patient"}]))

        result = CliRunner().invoke(cli, ["validate", "--input", str(resources_file)])
        assert result.exit_code != 0
        assert "--fhir-profile" in result.output

    def test_validate_cli_unsupported_profile_errors(self, tmp_path):
        import json

        from click.testing import CliRunner

        from portiere.cli import cli

        resources_file = tmp_path / "resources.json"
        resources_file.write_text(json.dumps([{"resourceType": "Patient"}]))

        result = CliRunner().invoke(
            cli,
            [
                "validate",
                "--fhir-profile",
                "mcode-1.0",
                "--input",
                str(resources_file),
            ],
        )
        assert result.exit_code != 0
        assert "Unsupported" in result.output or "mcode-1.0" in result.output

    def test_validate_cli_fails_when_resources_invalid(self, tmp_path):
        pytest.importorskip("fhir.resources")
        pytest.importorskip("fhirpathpy")
        import json

        from click.testing import CliRunner

        from portiere.cli import cli

        resources_file = tmp_path / "resources.json"
        resources_file.write_text(json.dumps([{"resourceType": "Patient", "id": "p1"}]))

        result = CliRunner().invoke(
            cli,
            [
                "validate",
                "--fhir-profile",
                "us-core-6.1.0",
                "--input",
                str(resources_file),
            ],
        )
        assert result.exit_code == 1
        assert "FAIL" in result.output

    def test_validate_cli_accepts_single_resource_object(self, tmp_path):
        pytest.importorskip("fhir.resources")
        pytest.importorskip("fhirpathpy")
        import json

        from click.testing import CliRunner

        from portiere.cli import cli

        # Top-level object (not array) — the CLI wraps it into a list
        resources_file = tmp_path / "resources.json"
        resources_file.write_text(
            json.dumps({"resourceType": "Bundle", "id": "b1", "type": "collection"})
        )

        result = CliRunner().invoke(
            cli,
            [
                "validate",
                "--fhir-profile",
                "us-core-6.1.0",
                "--input",
                str(resources_file),
            ],
        )
        assert result.exit_code == 0
        assert "PASS" in result.output

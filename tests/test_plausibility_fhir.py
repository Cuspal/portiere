"""Tests for FHIR-specific plausibility Python rules (Slice 3 Task 3.6)."""

from __future__ import annotations

import pandas as pd


class TestPatientBirthDateNotFuture:
    def test_passes(self):
        from portiere.quality.plausibility.fhir import patient_birthdate_not_future

        df = pd.DataFrame({"birthDate": ["1980-01-01", "2020-01-01"]})
        r = patient_birthdate_not_future(df, reference_date="2026-01-01")
        assert r.passed is True

    def test_fails_for_future_birthdate(self):
        from portiere.quality.plausibility.fhir import patient_birthdate_not_future

        df = pd.DataFrame({"birthDate": ["1980-01-01", "2099-01-01"]})
        r = patient_birthdate_not_future(df, reference_date="2026-01-01")
        assert r.passed is False
        assert r.failed_count == 1

    def test_passes_when_column_absent(self):
        from portiere.quality.plausibility.fhir import patient_birthdate_not_future

        df = pd.DataFrame({"other": [1]})
        r = patient_birthdate_not_future(df, reference_date="2026-01-01")
        assert r.passed is True
        assert "not present" in r.detail


class TestObservationStatusInValueSet:
    def test_passes_for_valid_statuses(self):
        from portiere.quality.plausibility.fhir import observation_status_in_valueset

        df = pd.DataFrame({"status": ["final", "preliminary", "amended"]})
        r = observation_status_in_valueset(df)
        assert r.passed is True

    def test_fails_for_unknown_status(self):
        from portiere.quality.plausibility.fhir import observation_status_in_valueset

        df = pd.DataFrame({"status": ["final", "weird-status"]})
        r = observation_status_in_valueset(df)
        assert r.passed is False
        assert r.failed_count == 1


class TestMedicationRequestIntentInValueSet:
    def test_passes(self):
        from portiere.quality.plausibility.fhir import medication_request_intent_in_valueset

        df = pd.DataFrame({"intent": ["order", "plan", "proposal"]})
        r = medication_request_intent_in_valueset(df)
        assert r.passed is True

    def test_fails(self):
        from portiere.quality.plausibility.fhir import medication_request_intent_in_valueset

        df = pd.DataFrame({"intent": ["order", "free-form"]})
        r = medication_request_intent_in_valueset(df)
        assert r.passed is False
        assert r.failed_count == 1

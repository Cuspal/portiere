"""Sanity check: US Core 6.1.0 profile JSONs are present and reasonably sized."""

from __future__ import annotations

import json
from pathlib import Path

PROFILES_DIR = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "portiere"
    / "standards"
    / "fhir_profiles"
    / "us_core_6_1_0"
)

EXPECTED_RESOURCES = {
    "Patient",
    "Practitioner",
    "Organization",
    "Encounter",
    "Condition",
    "Observation",
    "MedicationRequest",
    "AllergyIntolerance",
    "Procedure",
    "DocumentReference",
}


def test_all_profiles_present():
    files = {p.stem for p in PROFILES_DIR.glob("*.json")}
    assert files == EXPECTED_RESOURCES, f"Missing or extra: {EXPECTED_RESOURCES ^ files}"


def test_each_profile_is_a_snapshot_structure_definition():
    for p in PROFILES_DIR.glob("*.json"):
        sd = json.loads(p.read_text())
        assert sd.get("resourceType") == "StructureDefinition", p.name
        assert sd.get("kind") == "resource", p.name
        assert "snapshot" in sd, f"{p.name} is differential-only — re-fetch snapshot"


def test_total_size_under_5_mb():
    # Full snapshot SDs are ~150-250 KB each; 10 files are roughly 1.7 MB unminified.
    total = sum(p.stat().st_size for p in PROFILES_DIR.glob("*.json"))
    assert total < 5 * 1024 * 1024, f"US Core profiles total {total // 1024} KB — over budget"

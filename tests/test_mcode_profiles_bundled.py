"""Sanity check: mCODE STU3 2.0.0 profile JSONs are present and reasonably sized."""

from __future__ import annotations

import json
from pathlib import Path

PROFILES_DIR = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "portiere"
    / "standards"
    / "fhir_profiles"
    / "mcode_2_0_0"
)

EXPECTED_RESOURCES = {
    "CancerPatient",
    "PrimaryCancerCondition",
    "CancerDiseaseStatus",
    "CancerStage",
    "TNMStageGroup",
}


def test_all_profiles_present():
    files = {p.stem for p in PROFILES_DIR.glob("*.json")}
    assert files == EXPECTED_RESOURCES, f"Missing or extra: {EXPECTED_RESOURCES ^ files}"


def test_each_profile_is_a_snapshot_structure_definition():
    for p in PROFILES_DIR.glob("*.json"):
        sd = json.loads(p.read_text())
        assert sd.get("resourceType") == "StructureDefinition", p.name
        assert "snapshot" in sd, f"{p.name} is differential-only — re-fetch snapshot"


def test_total_size_under_2_mb():
    # mCODE snapshots are ~130-170 KB each; 5 files ≈ 720 KB.
    total = sum(p.stat().st_size for p in PROFILES_DIR.glob("*.json"))
    assert total < 2 * 1024 * 1024, f"mCODE profiles total {total // 1024} KB — over budget"

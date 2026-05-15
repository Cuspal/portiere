"""Download FHIR profile snapshot StructureDefinitions bundled with Portiere.

Covers:
  - US Core 6.1.0 (10 resource types)
  - mCODE STU3 2.0.0 (5 core oncology profiles)

This script is NOT shipped in the wheel. It is the reproducible recipe for
the JSONs under ``src/portiere/standards/fhir_profiles/``.

Usage:
    python scripts/build_fhir_profiles.py
    python scripts/build_fhir_profiles.py us_core   # subset
    python scripts/build_fhir_profiles.py mcode     # subset

Sources:
    US Core 6.1.0  — https://hl7.org/fhir/us/core/STU6.1/
    mCODE STU3 2.0.0 — https://hl7.org/fhir/us/mcode/STU3/
"""

from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

# Snapshot StructureDefinition canonical URLs in US Core 6.1.0.
US_CORE_PROFILES = {
    "Patient": "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-patient.json",
    "Practitioner": "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-practitioner.json",
    "Organization": "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-organization.json",
    "Encounter": "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-encounter.json",
    "Condition": "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-condition-problems-health-concerns.json",
    "Observation": "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-observation-lab.json",
    "MedicationRequest": "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-medicationrequest.json",
    "AllergyIntolerance": "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-allergyintolerance.json",
    "Procedure": "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-procedure.json",
    "DocumentReference": "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-documentreference.json",
}

# mCODE STU3 2.0.0 core profiles.
# Stems intentionally describe the profile (e.g. CancerPatient), not the base
# resource type, because mCODE uses meta.profile rather than resourceType to
# claim the profile (one Patient resource can be a CancerPatient).
MCODE_PROFILES = {
    "CancerPatient": "https://hl7.org/fhir/us/mcode/STU3/StructureDefinition-mcode-cancer-patient.json",
    "PrimaryCancerCondition": "https://hl7.org/fhir/us/mcode/STU3/StructureDefinition-mcode-primary-cancer-condition.json",
    "CancerDiseaseStatus": "https://hl7.org/fhir/us/mcode/STU3/StructureDefinition-mcode-cancer-disease-status.json",
    "CancerStage": "https://hl7.org/fhir/us/mcode/STU3/StructureDefinition-mcode-cancer-stage.json",
    "TNMStageGroup": "https://hl7.org/fhir/us/mcode/STU3/StructureDefinition-mcode-tnm-stage-group.json",
}

PROFILE_SETS: dict[str, tuple[dict[str, str], str]] = {
    "us_core": (US_CORE_PROFILES, "us_core_6_1_0"),
    "mcode": (MCODE_PROFILES, "mcode_2_0_0"),
}


def fetch_set(profiles: dict[str, str], out_dir: Path) -> list[str]:
    """Download each profile JSON. Return list of failed stems."""
    out_dir.mkdir(parents=True, exist_ok=True)
    failed: list[str] = []
    for stem, url in profiles.items():
        print(f"  {stem:<28} ...", end=" ", flush=True)
        try:
            with urllib.request.urlopen(url, timeout=30) as f:
                sd = json.load(f)
        except Exception as exc:
            print(f"FAILED: {exc}")
            failed.append(stem)
            continue

        if not sd.get("snapshot"):
            print("WARN: differential only, skipping")
            failed.append(stem)
            continue

        out_file = out_dir / f"{stem}.json"
        out_file.write_text(json.dumps(sd, indent=2, sort_keys=True))
        print(f"ok ({out_file.stat().st_size // 1024} KB)")
    return failed


def main(only: str | None = None) -> None:
    base = Path(__file__).resolve().parents[1] / "src" / "portiere" / "standards" / "fhir_profiles"
    sets_to_fetch = {only: PROFILE_SETS[only]} if only in PROFILE_SETS else PROFILE_SETS
    if only and only not in PROFILE_SETS:
        print(f"Unknown profile set: {only!r}. Choose from {sorted(PROFILE_SETS)}", file=sys.stderr)
        sys.exit(2)

    all_failed: list[str] = []
    for set_name, (profiles, subdir) in sets_to_fetch.items():
        out_dir = base / subdir
        print(
            f"\n[{set_name}] -> {out_dir.relative_to(Path.cwd()) if out_dir.is_relative_to(Path.cwd()) else out_dir}"
        )
        all_failed.extend(f"{set_name}/{f}" for f in fetch_set(profiles, out_dir))

    if all_failed:
        print(f"\nFailed: {all_failed}", file=sys.stderr)
        sys.exit(1)
    print("\nAll profiles fetched.")


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    main(arg)

"""Download US Core 6.1.0 snapshot StructureDefinitions for the 10 resource
types Portiere generates that US Core covers.

This script is NOT shipped in the wheel. It is the reproducible recipe for
how the JSONs in src/portiere/standards/fhir_profiles/us_core_6_1_0/ were
created.

Usage:
    python scripts/build_us_core_profiles.py

Source: https://hl7.org/fhir/us/core/STU6.1/ (US Core IG 6.1.0).
"""

from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

# Snapshot StructureDefinition canonical URLs in US Core 6.1.0.
US_CORE_PROFILES = {
    "Patient":            "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-patient.json",
    "Practitioner":       "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-practitioner.json",
    "Organization":       "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-organization.json",
    "Encounter":          "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-encounter.json",
    "Condition":          "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-condition-problems-health-concerns.json",
    "Observation":        "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-observation-lab.json",
    "MedicationRequest":  "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-medicationrequest.json",
    "AllergyIntolerance": "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-allergyintolerance.json",
    "Procedure":          "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-procedure.json",
    "DocumentReference":  "https://hl7.org/fhir/us/core/STU6.1/StructureDefinition-us-core-documentreference.json",
}


def main(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    failed = []
    for resource_type, url in US_CORE_PROFILES.items():
        print(f"Fetching {resource_type} ...", end=" ", flush=True)
        try:
            with urllib.request.urlopen(url, timeout=30) as f:
                sd = json.load(f)
        except Exception as e:
            print(f"FAILED: {e}")
            failed.append(resource_type)
            continue

        if not sd.get("snapshot"):
            print(f"WARN: no snapshot section — differential only, skipping")
            failed.append(resource_type)
            continue

        out_file = out_dir / f"{resource_type}.json"
        out_file.write_text(json.dumps(sd, indent=2, sort_keys=True))
        print(f"ok ({out_file.stat().st_size // 1024} KB)")

    if failed:
        print(f"\nFailed resources: {failed}", file=sys.stderr)
        sys.exit(1)
    print(f"\nAll {len(US_CORE_PROFILES)} profiles written to {out_dir}")


if __name__ == "__main__":
    target = (
        Path(__file__).resolve().parents[1]
        / "src" / "portiere" / "standards" / "fhir_profiles" / "us_core_6_1_0"
    )
    main(target)

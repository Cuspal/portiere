"""Reproducibly regenerate ``src/portiere/_demo_data/`` from upstream sources.

**Status: documented stub for v0.3.0.** For v0.2.0 the bundle is
hand-curated and committed directly. This file describes how the bundle
*could* be regenerated reproducibly so the curation step is auditable.

Inputs (download manually before running)::

    --synthea-output <dir>     Output dir from a Synthea run
                               (java -jar synthea-with-dependencies.jar
                                -p 50 -s 42 ...)
    --icd10cm <path>           CMS ICD-10-CM tabular CSV/XML
    --loinc <path>             LOINC.csv from loinc.org
    --rxnorm <path>            RxNorm RXNCONSO.RRF from NLM

Output::

    Overwrites src/portiere/_demo_data/ with curated CSVs (sources +
    vocabulary/ in Athena format).

Curation invariant (asserted at end of run AND in CI)::

    every code in synthetic_*.csv must exist in vocabulary/CONCEPT.csv
"""

from __future__ import annotations

import argparse
import sys

SEED = 42
N_PATIENTS = 20


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--synthea-output", help="Synthea output directory")
    parser.add_argument("--icd10cm", help="CMS ICD-10-CM file")
    parser.add_argument("--loinc", help="LOINC.csv from loinc.org")
    parser.add_argument("--rxnorm", help="RxNorm RXNCONSO.RRF from NLM")
    parser.add_argument(
        "--out",
        default="src/portiere/_demo_data",
        help="Output directory (default: src/portiere/_demo_data)",
    )
    args = parser.parse_args()

    print(
        "build_demo_data.py is a documented stub for v0.3.0.\n"
        "For v0.2.0 the bundle is hand-curated and committed directly.\n"
        "Args received: " + repr(vars(args))
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""One-time generator for ``benchmarks/athena_icd_snomed/gold_test_set.csv``.

Holds out a stratified-or-random sample of ICD-10-CM concept_ids from
the user's Athena vocabulary export. Output is **license-clean** —
just integer IDs, no concept names, descriptions, or other Athena
content. Anyone with their own Athena export can reproduce the
benchmark by running the runner against this committed test set.

Run this once during release prep to lock the test set; commit the
output. Re-running with the same seed produces the same sample.

Usage::

    python scripts/build_benchmark_test_set.py \\
        --athena-dir /path/to/extracted/athena \\
        --n 1000 \\
        --seed 42

Output::

    benchmarks/athena_icd_snomed/gold_test_set.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--athena-dir",
        required=True,
        help="Extracted Athena vocabulary directory (CONCEPT.csv etc.)",
    )
    parser.add_argument("--n", type=int, default=1000, help="Test-set size (default 1000)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default 42)")
    parser.add_argument(
        "--out",
        default="benchmarks/athena_icd_snomed/gold_test_set.csv",
        help="Output CSV path",
    )
    args = parser.parse_args()

    import pandas as pd

    athena = Path(args.athena_dir)
    concept = pd.read_csv(athena / "CONCEPT.csv", sep="\t", low_memory=False)
    cr = pd.read_csv(athena / "CONCEPT_RELATIONSHIP.csv", sep="\t", low_memory=False)

    # ICD-10-CM concepts that have at least one Maps-to relationship in Athena
    icd = concept[concept["vocabulary_id"] == "ICD10CM"]
    has_gold = cr[
        (cr["relationship_id"] == "Maps to")
        & cr["concept_id_1"].isin(icd["concept_id"])
    ]["concept_id_1"].drop_duplicates()
    pool = icd[icd["concept_id"].isin(has_gold)]

    if len(pool) < args.n:
        print(
            f"warning: only {len(pool)} ICD-10-CM concepts have a Maps-to "
            f"in Athena; sampling all of them instead of {args.n}.",
            file=sys.stderr,
        )
        sampled_ids = pool["concept_id"]
    else:
        sampled_ids = pool["concept_id"].sample(n=args.n, random_state=args.seed)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"icd10cm_concept_id": sampled_ids.astype(int)}).to_csv(
        out_path, index=False
    )
    print(f"Wrote {len(sampled_ids)} test concept IDs -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

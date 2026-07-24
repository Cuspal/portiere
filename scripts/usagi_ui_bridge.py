#!/usr/bin/env python3
"""USAGI UI-export bridge — the supported way to score the USAGI baseline.

No published USAGI release (<= v1.4.3) has a headless batch mode: the JAR
opens the review UI. This bridge makes the UI run reproducible and scoreable:

1. Generate the deterministic gold-set input for USAGI's "Import codes":

       python3 scripts/usagi_ui_bridge.py gen-input \\
           --athena-dir /path/to/athena --out usagi_input.csv

2. In the USAGI UI: build the vocabulary index from the same Athena dir →
   File > Import codes (map source_code / source_name; filter standard
   concepts + SNOMED) → let it auto-map (no hand corrections) →
   File > Export source_to_concept_map → usagi_export.csv.

3. Score the export and append the benchmark row:

       python3 scripts/usagi_ui_bridge.py score \\
           --athena-dir /path/to/athena \\
           --export usagi_export.csv \\
           --out src/portiere/benchmarks/athena_icd_snomed/expected_results.json

Note: the UI export carries one best match per code, so USAGI's top-5/top-10
equal its top-1 — footnote this wherever the row is published.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def _gold(athena_dir: Path, n: int = 1000, seed: int = 42):
    from portiere.benchmarks.athena_icd_snomed.runner import (
        _generate_test_ids,
        _load_athena_concept,
        _load_athena_relationships,
    )

    concept = _load_athena_concept(athena_dir)
    cr = _load_athena_relationships(athena_dir)
    test_ids = _generate_test_ids(concept, cr, n=n, seed=seed)
    return concept, cr, test_ids


def cmd_gen_input(args: argparse.Namespace) -> None:
    from portiere.benchmarks.athena_icd_snomed.usagi_baseline import (
        write_usagi_input_csv,
    )

    concept, _cr, test_ids = _gold(Path(args.athena_dir), args.n, args.seed)
    rows = concept[concept["concept_id"].isin(test_ids)]
    input_rows = [
        {
            "concept_id": int(r["concept_id"]),
            "concept_code": str(r["concept_code"]),
            "concept_name": str(r["concept_name"]),
        }
        for _i, r in rows.iterrows()
    ]
    write_usagi_input_csv(input_rows, Path(args.out))
    print(f"wrote {args.out} with {len(input_rows)} codes (seed={args.seed})")


def cmd_score(args: argparse.Namespace) -> None:
    from portiere.benchmarks.athena_icd_snomed.runner import (
        append_run_to_expected_results,
        compute_metrics,
    )

    concept, cr, test_ids = _gold(Path(args.athena_dir), args.n, args.seed)
    maps_to = cr[(cr["relationship_id"] == "Maps to") & cr["concept_id_1"].isin(test_ids)]
    gold = {
        int(k): set(map(int, v))
        for k, v in maps_to.groupby("concept_id_1")["concept_id_2"].apply(set).to_dict().items()
    }
    code_to_id = {
        str(r["concept_code"]): int(r["concept_id"])
        for _i, r in concept[concept["concept_id"].isin(test_ids)].iterrows()
    }

    export_csv = Path(args.export)
    sample = export_csv.read_text(encoding="utf-8-sig", errors="replace")[:4096]
    delim = "\t" if sample.count("\t") > sample.count(",") else ","
    predictions: dict[int, list[int]] = {}
    with export_csv.open(encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f, delimiter=delim):
            src = str(row.get("source_code") or row.get("sourceCode") or "").strip()
            try:
                tgt = int(float(row.get("target_concept_id") or row.get("targetConceptId") or "0"))
            except (ValueError, TypeError):
                continue
            cid = code_to_id.get(src)
            if cid is None or tgt <= 0:
                continue
            predictions.setdefault(cid, []).append(tgt)

    print(f"parsed {len(predictions)} mapped codes (delim={delim!r})")
    result = compute_metrics(predictions, gold)
    print(
        f"n={result.n}  top-1={result.top_1:.3f}  top-5={result.top_5:.3f}  "
        f"top-10={result.top_10:.3f}  MRR={result.mrr:.3f}"
    )
    append_run_to_expected_results(
        result,
        backend="usagi",
        athena_release_date=args.athena_release_date,
        out=args.out,
    )
    print(f"usagi row appended to {args.out}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    common = {
        "--athena-dir": dict(required=True),
        "--n": dict(type=int, default=1000),
        "--seed": dict(type=int, default=42),
    }

    g = sub.add_parser("gen-input", help="write the USAGI Import-codes CSV")
    for flag, kw in common.items():
        g.add_argument(flag, **kw)
    g.add_argument("--out", default="usagi_input.csv")
    g.set_defaults(func=cmd_gen_input)

    s = sub.add_parser("score", help="score a UI export, append the benchmark row")
    for flag, kw in common.items():
        s.add_argument(flag, **kw)
    s.add_argument("--export", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--athena-release-date", default="2026-04-30")
    s.set_defaults(func=cmd_score)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

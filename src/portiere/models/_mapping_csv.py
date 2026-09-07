"""Read mapping artifacts without converting identifiers into numbers/nulls."""

from __future__ import annotations

import bz2
import csv
import gzip
import lzma
from pathlib import Path
from typing import Any


def read_mapping_records(path: str, *, engine=None) -> list[dict]:
    """Preserve literal strings and read the whole artifact, without a row cap.

    Local CSV and common compression formats need no dataframe dependency.
    An explicit engine owns its transport (including Spark output directories).
    Other pandas-supported formats/URLs retain the existing lazy pandas path.
    """
    if engine is not None:
        options: dict[str, Any] = {}
        if engine.engine_name == "pandas":
            options = {"dtype": str, "keep_default_na": False}
        elif engine.engine_name == "polars":
            options = {"infer_schema_length": 0}
        elif engine.engine_name == "spark":
            options = {"inferSchema": False}
        frame = engine.read_csv(path, **options)
        if engine.engine_name == "polars":
            return frame.to_dicts()
        if engine.engine_name == "spark":
            return [row.asDict() for row in frame.toLocalIterator()]
        return engine.to_pandas(frame).to_dict("records")

    suffix = Path(path).suffix.lower()
    if "://" not in str(path) and suffix in {".csv", ".gz", ".bz2", ".xz", ""}:
        opener: Any = {".gz": gzip.open, ".bz2": bz2.open, ".xz": lzma.open}.get(suffix, open)
        with opener(path, "rt", encoding="utf-8-sig", newline="") as stream:
            return list(csv.DictReader(stream))

    import pandas as pd

    return pd.read_csv(path, dtype=str, keep_default_na=False).to_dict("records")

"""Read mapping artifacts without converting identifiers into numbers/nulls."""

from __future__ import annotations

import bz2
import csv
import gzip
import io
import lzma
import zipfile
from pathlib import Path
from typing import Any


def write_mapping_records(path: str, records: list[dict], fieldnames: list[str]) -> None:
    """Write literal review rows using the compression advertised by the path."""
    suffix = Path(path).suffix.lower()
    if "://" in str(path) or suffix not in {".csv", ".gz", ".bz2", ".xz", ".zip", ""}:
        import pandas as pd

        pd.DataFrame.from_records(records, columns=fieldnames).to_csv(path, index=False)
        return

    def write(stream):
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    if suffix == ".zip":
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            with archive.open(Path(path).stem, "w") as binary:
                with io.TextIOWrapper(binary, encoding="utf-8", newline="") as stream:
                    write(stream)
        return
    opener: Any = {".gz": gzip.open, ".bz2": bz2.open, ".xz": lzma.open}.get(suffix, open)
    with opener(path, "wt", encoding="utf-8", newline="") as stream:
        write(stream)


def read_mapping_records(path: str, *, engine=None) -> list[dict]:
    """Preserve literal strings and read the whole artifact, without a row cap.

    Local CSV and common compression formats need no dataframe dependency.
    An explicit engine owns its transport (including Spark output directories).
    Other pandas-supported formats/URLs retain the existing lazy pandas path.
    """
    suffix = Path(path).suffix.lower()
    if "://" not in str(path) and suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            members = [member for member in archive.infolist() if not member.is_dir()]
            if len(members) != 1:
                raise ValueError("Mapping ZIP must contain exactly one CSV file.")
            with archive.open(members[0]) as binary:
                with io.TextIOWrapper(binary, encoding="utf-8-sig", newline="") as stream:
                    return list(csv.DictReader(stream))

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

    if "://" not in str(path) and suffix in {".csv", ".gz", ".bz2", ".xz", ""}:
        opener: Any = {".gz": gzip.open, ".bz2": bz2.open, ".xz": lzma.open}.get(suffix, open)
        with opener(path, "rt", encoding="utf-8-sig", newline="") as stream:
            return list(csv.DictReader(stream))

    import pandas as pd

    return pd.read_csv(path, dtype=str, keep_default_na=False).to_dict("records")

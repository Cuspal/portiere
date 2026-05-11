"""FHIR NDJSON Bulk Data ($export-shape) serializer."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def to_ndjson_files(resources: list[dict[str, Any]], *, out_dir: Path) -> list[Path]:
    """Write resources as one NDJSON file per resourceType.

    Returns the list of files written (empty list if no resources).
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for idx, resource in enumerate(resources):
        rt = resource.get("resourceType")
        if not rt:
            raise ValueError(f"Resource at index {idx} missing required 'resourceType'")
        by_type[rt].append(resource)

    written: list[Path] = []
    for rt, group in by_type.items():
        path = out_dir / f"{rt}.ndjson"
        with path.open("w", encoding="utf-8") as f:
            for r in group:
                f.write(json.dumps(r, separators=(",", ":")))
                f.write("\n")
        written.append(path)
    return written

"""Bundled demo data — source CSVs + Athena-format vocabulary subset.

Internal package (underscore-prefixed). The data files ship inside
the wheel via ``[tool.hatch.build.targets.wheel] force-include``.

Public access:

    from portiere._demo_data import demo_data_dir, vocabulary_dir

    demo_data_dir() / "synthetic_patients.csv"
    vocabulary_dir() / "CONCEPT.csv"

See the directory's ``README.md`` for what's bundled and the
"every source code has a vocab match" curation invariant.
"""

from __future__ import annotations

from pathlib import Path


def demo_data_dir() -> Path:
    """Return the absolute path to the bundled demo-data directory."""
    return Path(__file__).parent


def vocabulary_dir() -> Path:
    """Return the absolute path to the bundled Athena-format vocabulary subset."""
    return demo_data_dir() / "vocabulary"


def synthetic_source_files() -> dict[str, Path]:
    """Return a mapping of logical source name -> bundled CSV path.

    Useful for iterating over all the demo source CSVs without
    hardcoding filenames in the quickstart command.
    """
    base = demo_data_dir()
    return {
        "patients": base / "synthetic_patients.csv",
        "conditions": base / "synthetic_conditions.csv",
        "observations": base / "synthetic_observations.csv",
        "medications": base / "synthetic_medications.csv",
    }


__all__ = ["demo_data_dir", "synthetic_source_files", "vocabulary_dir"]

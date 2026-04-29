"""Curation invariant for bundled demo data (Slice 5 Task 5.3).

**Release-blocking test.** Every code referenced by a source CSV must
exist as a ``concept_code`` in the bundled ``vocabulary/CONCEPT.csv``.
A violation means the demo will fail at concept-mapping time — fix the
source CSVs or expand the vocabulary subset before merging.
"""

from __future__ import annotations

import csv

import pytest


def _load_vocab_codes() -> set[str]:
    """Return the set of concept_code values from the bundled CONCEPT.csv."""
    from portiere._demo_data import vocabulary_dir

    concept_csv = vocabulary_dir() / "CONCEPT.csv"
    with concept_csv.open() as f:
        reader = csv.DictReader(f, delimiter="\t")
        return {row["concept_code"] for row in reader}


def _load_codes_from_source(filename: str, code_column: str) -> set[str]:
    """Load distinct codes from a source CSV column."""
    from portiere._demo_data import demo_data_dir

    p = demo_data_dir() / filename
    with p.open() as f:
        reader = csv.DictReader(f)
        return {row[code_column] for row in reader if row[code_column]}


class TestSourceFilesPresent:
    def test_patients_csv_exists(self):
        from portiere._demo_data import synthetic_source_files

        assert synthetic_source_files()["patients"].exists()

    def test_all_source_csvs_exist(self):
        from portiere._demo_data import synthetic_source_files

        for name, path in synthetic_source_files().items():
            assert path.exists(), f"missing demo source: {name} at {path}"


class TestVocabularyFilesPresent:
    @pytest.mark.parametrize(
        "filename",
        [
            "CONCEPT.csv",
            "VOCABULARY.csv",
            "DOMAIN.csv",
            "CONCEPT_CLASS.csv",
            "CONCEPT_RELATIONSHIP.csv",
        ],
    )
    def test_vocab_file_exists(self, filename):
        from portiere._demo_data import vocabulary_dir

        assert (vocabulary_dir() / filename).exists()


class TestCurationInvariant:
    """Every code in a source CSV must exist in the bundled vocabulary."""

    def test_dx_codes_in_vocab(self):
        codes = _load_codes_from_source("synthetic_conditions.csv", "dx_code")
        vocab = _load_vocab_codes()
        missing = codes - vocab
        assert not missing, (
            f"synthetic_conditions.csv references codes not in the bundled "
            f"CONCEPT.csv: {sorted(missing)}"
        )

    def test_lab_codes_in_vocab(self):
        codes = _load_codes_from_source("synthetic_observations.csv", "lab_code")
        vocab = _load_vocab_codes()
        missing = codes - vocab
        assert not missing, (
            f"synthetic_observations.csv references codes not in the bundled "
            f"CONCEPT.csv: {sorted(missing)}"
        )

    def test_med_codes_in_vocab(self):
        codes = _load_codes_from_source("synthetic_medications.csv", "med_code")
        vocab = _load_vocab_codes()
        missing = codes - vocab
        assert not missing, (
            f"synthetic_medications.csv references codes not in the bundled "
            f"CONCEPT.csv: {sorted(missing)}"
        )

    def test_patient_ids_consistent_across_sources(self):
        """Every patient_id used in conditions/observations/medications must
        appear in synthetic_patients.csv (referential integrity check)."""
        patients = _load_codes_from_source("synthetic_patients.csv", "patient_id")
        for fname in (
            "synthetic_conditions.csv",
            "synthetic_observations.csv",
            "synthetic_medications.csv",
        ):
            referenced = _load_codes_from_source(fname, "patient_id")
            unknown = referenced - patients
            assert not unknown, f"{fname} references unknown patient_id values: {sorted(unknown)}"


class TestVocabularySize:
    """Sanity check: bundle stays small (wheel-size budget is ≤2 MB)."""

    def test_concept_csv_is_small(self):
        from portiere._demo_data import vocabulary_dir

        size = (vocabulary_dir() / "CONCEPT.csv").stat().st_size
        assert size < 100_000, (
            f"CONCEPT.csv is {size} bytes — the demo subset should stay tight "
            f"(<100 KB). If you intentionally grew it, raise this threshold."
        )

    def test_total_demo_data_dir_size(self):
        from portiere._demo_data import demo_data_dir

        total = sum(p.stat().st_size for p in demo_data_dir().rglob("*") if p.is_file())
        # Slice 5 budget: ≤2 MB total demo data
        assert total < 2_000_000, f"_demo_data/ is {total} bytes — Slice 5 budget is ≤2 MB total."

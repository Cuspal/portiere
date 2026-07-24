"""Tests for the Excel deliverable workbooks (portiere.reports)."""

import pytest

openpyxl = pytest.importorskip("openpyxl")

from portiere.models.concept_mapping import ConceptMapping, ConceptMappingItem  # noqa: E402
from portiere.models.schema_mapping import SchemaMapping, SchemaMappingItem  # noqa: E402
from portiere.quality.profile_report import build_source_profile  # noqa: E402
from portiere.reports import (  # noqa: E402
    build_concept_workbook,
    build_profile_workbook,
    build_schema_workbook,
)


def _profile():
    return build_source_profile(
        {
            "row_count": 100,
            "column_count": 2,
            "columns": [
                {
                    "name": "gender",
                    "type": "Utf8",
                    "n_unique": 2,
                    "present_count": 90,
                    "min_len": 1,
                    "max_len": 1,
                    "example": "M",
                    "top_values": [{"gender": "M", "len": 50}, {"gender": "F", "len": 40}],
                },
                {
                    "name": "age",
                    "type": "Int64",
                    "n_unique": 60,
                    "present_count": 100,
                    "min_len": 0,
                    "max_len": 0,
                    "example": "42",
                    "top_values": [],
                    "num_min": 0.0,
                    "num_max": 99.0,
                    "num_mean": 41.5,
                    "num_std": 18.2,
                },
            ],
        },
        source="HIS_patient",
        system="HIS",
    )


class TestProfileWorkbook:
    def test_sheets_and_content(self, tmp_path):
        out = tmp_path / "profile.xlsx"
        path = build_profile_workbook([_profile()], out, title="HIS scan")
        wb = openpyxl.load_workbook(path)
        names = wb.sheetnames
        assert "TOC" in names
        assert "Table Overview" in names
        assert "Field Overview" in names
        assert any("HIS_patient" in n for n in names)  # value-frequency sheet

        fo = wb["Field Overview"]
        header = [c.value for c in fo[1]]
        for col in ("Table", "Field", "Type", "Present %", "N distinct", "Mean", "Std"):
            assert col in header
        # age row carries numeric stats
        rows = list(fo.iter_rows(min_row=2, values_only=True))
        age = next(r for r in rows if r[header.index("Field")] == "age")
        assert age[header.index("Mean")] == 41.5

    def test_value_frequency_sheet(self, tmp_path):
        out = tmp_path / "profile.xlsx"
        build_profile_workbook([_profile()], out)
        wb = openpyxl.load_workbook(out)
        vs = wb[next(n for n in wb.sheetnames if "HIS_patient" in n)]
        vals = [v for row in vs.iter_rows(values_only=True) for v in row if v is not None]
        assert "M" in vals and 50 in vals  # top value with count
        assert any("top 10" in str(v).lower() for v in vals)  # truncation labeled


class TestSchemaWorkbook:
    def _mapping(self):
        return SchemaMapping(
            items=[
                SchemaMappingItem(
                    source_table="HIS_patient",
                    source_column="sex",
                    target_table="person",
                    target_column="gender_concept_id",
                    confidence=0.91,
                    status="approved",
                ),
                SchemaMappingItem(
                    source_table="HIS_patient",
                    source_column="dob",
                    target_table="person",
                    target_column="year_of_birth",
                    confidence=0.72,
                    status="needs_review",
                ),
            ]
        )

    def test_person_sheet_lists_all_standard_fields(self, tmp_path):
        out = tmp_path / "schema.xlsx"
        build_schema_workbook(self._mapping(), out, standard="omop_cdm_v5.4")
        wb = openpyxl.load_workbook(out)
        assert "person" in [n.lower() for n in wb.sheetnames]
        ws = wb[next(n for n in wb.sheetnames if n.lower() == "person")]
        header = [c.value for c in ws[2]]  # row 1 is the sheet title
        for col in (
            "OMOP Field",
            "Spec",
            "Data Type",
            "Description",
            "Source Table",
            "Source Column",
            "Confidence",
            "Status",
        ):
            assert col in header
        fields = [r[header.index("OMOP Field")] for r in ws.iter_rows(min_row=3, values_only=True)]
        # mapped AND unmapped standard fields both listed
        assert "gender_concept_id" in fields
        assert "person_id" in fields  # unmapped — still a row (the work queue)
        gender = next(
            r
            for r in ws.iter_rows(min_row=3, values_only=True)
            if r[header.index("OMOP Field")] == "gender_concept_id"
        )
        assert gender[header.index("Source Column")] == "sex"
        assert gender[header.index("Status")] == "approved"
        person_id = next(
            r
            for r in ws.iter_rows(min_row=3, values_only=True)
            if r[header.index("OMOP Field")] == "person_id"
        )
        assert person_id[header.index("Status")] == "UNMAPPED"

    def test_summary_sheet(self, tmp_path):
        out = tmp_path / "schema.xlsx"
        build_schema_workbook(self._mapping(), out, standard="omop_cdm_v5.4")
        wb = openpyxl.load_workbook(out)
        assert "Summary" in wb.sheetnames


class TestConceptWorkbook:
    def _mapping(self):
        return ConceptMapping(
            items=[
                ConceptMappingItem(
                    source_code="0",
                    source_description="male",
                    source_column="sex",
                    target_concept_id=8507,
                    target_concept_name="MALE",
                    target_vocabulary_id="Gender",
                    target_domain_id="Gender",
                    confidence=0.99,
                    method="auto",
                ),
                ConceptMappingItem(
                    source_code="X99",
                    source_description="mystery",
                    source_column="dx",
                    confidence=0.2,
                    method="manual",
                ),
            ]
        )

    def test_sheets_and_s2cm_columns(self, tmp_path):
        out = tmp_path / "concepts.xlsx"
        build_concept_workbook(self._mapping(), out)
        wb = openpyxl.load_workbook(out)
        for sheet in ("Source_to_Concept_Map", "vocabulary", "status", "readme"):
            assert sheet in wb.sheetnames
        ws = wb["Source_to_Concept_Map"]
        header = [c.value for c in ws[1]]
        for col in (
            "source_code",
            "source_concept_id",
            "source_vocabulary_id",
            "source_code_description",
            "target_concept_id",
            "target_vocabulary_id",
            "valid_start_date",
            "valid_end_date",
            "invalid_reason",
            "target_concept_name",
            "confidence",
            "method",
        ):
            assert col in header
        rows = list(ws.iter_rows(min_row=2, values_only=True))
        male = next(r for r in rows if r[header.index("source_code")] == "0")
        assert male[header.index("target_concept_id")] == 8507

    def test_status_sheet_generated_counts(self, tmp_path):
        out = tmp_path / "concepts.xlsx"
        build_concept_workbook(self._mapping(), out)
        wb = openpyxl.load_workbook(out)
        ws = wb["status"]
        vals = [v for row in ws.iter_rows(values_only=True) for v in row if v is not None]
        assert "sex" in vals and "dx" in vals  # per source column/vocabulary rows
        assert "auto" in [str(v) for v in [c.value for c in ws[1]]] or any(
            "auto" in str(v) for v in vals
        )

    def test_readme_has_provenance(self, tmp_path):
        out = tmp_path / "concepts.xlsx"
        build_concept_workbook(self._mapping(), out)
        wb = openpyxl.load_workbook(out)
        vals = [str(v) for row in wb["readme"].iter_rows(values_only=True) for v in row if v]
        joined = " ".join(vals)
        assert "portiere" in joined.lower()
        assert any(ch.isdigit() for ch in joined)  # version/date present


class TestWorkbookCLI:
    def test_profile_workbook_cli(self, tmp_path):
        import polars as pl
        from click.testing import CliRunner

        from portiere.cli import cli

        src = tmp_path / "HIS_patient.csv"
        pl.DataFrame({"sex": ["M", "F", ""], "age": [30, 40, 50]}).write_csv(src)
        out = tmp_path / "scan.xlsx"
        res = CliRunner().invoke(
            cli, ["workbook", "profile", str(src), "-o", str(out), "--system", "HIS"]
        )
        assert res.exit_code == 0, res.output
        wb = openpyxl.load_workbook(out)
        assert "Field Overview" in wb.sheetnames

    def test_schema_workbook_cli(self, tmp_path):
        import json

        from click.testing import CliRunner

        from portiere.cli import cli

        mapping = tmp_path / "schema_mapping_reviewed.json"
        mapping.write_text(
            json.dumps(
                {
                    "items": [
                        {
                            "source_table": "HIS_patient",
                            "source_column": "sex",
                            "target_table": "person",
                            "target_column": "gender_concept_id",
                            "confidence": 0.9,
                            "status": "approved",
                        }
                    ]
                }
            )
        )
        out = tmp_path / "schema.xlsx"
        res = CliRunner().invoke(
            cli, ["workbook", "schema", "--mapping", str(mapping), "-o", str(out)]
        )
        assert res.exit_code == 0, res.output
        assert out.exists()

    def test_concepts_workbook_cli(self, tmp_path):
        import json

        from click.testing import CliRunner

        from portiere.cli import cli

        mapping = tmp_path / "concept_mapping.json"
        mapping.write_text(
            json.dumps(
                {
                    "items": [
                        {
                            "source_code": "0",
                            "source_description": "male",
                            "source_column": "sex",
                            "target_concept_id": 8507,
                            "target_vocabulary_id": "Gender",
                            "confidence": 0.99,
                            "method": "auto",
                        }
                    ]
                }
            )
        )
        out = tmp_path / "concepts.xlsx"
        res = CliRunner().invoke(
            cli, ["workbook", "concepts", "--mapping", str(mapping), "-o", str(out)]
        )
        assert res.exit_code == 0, res.output
        wb = openpyxl.load_workbook(out)
        assert "Source_to_Concept_Map" in wb.sheetnames

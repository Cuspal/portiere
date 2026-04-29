"""Tests for portiere.local.cross_mapper (Slice 8 coverage gap-fill).

CrossStandardMapper is exercised against the bundled OMOP→FHIR
crossmap and a custom YAML, plus error / fallback paths.
"""

from __future__ import annotations

import pandas as pd


def _make_mapper(source="omop_cdm_v5.4", target="fhir_r4", custom=None):
    from portiere.local.cross_mapper import CrossStandardMapper

    return CrossStandardMapper(source, target, custom_crossmap=custom)


# ── list_crossmaps ───────────────────────────────────────────────


class TestListCrossmaps:
    def test_lists_built_in(self):
        from portiere.local.cross_mapper import list_crossmaps

        result = list_crossmaps()
        assert isinstance(result, list)
        assert len(result) >= 1
        # Each entry has a sensible shape
        for entry in result:
            assert "source" in entry or "target" in entry or "name" in entry


# ── Constructor + crossmap loading ───────────────────────────────


class TestConstructor:
    def test_loads_built_in_omop_to_fhir(self):
        m = _make_mapper("omop_cdm_v5.4", "fhir_r4")
        assert m.source_model.name == "omop_cdm_v5.4"
        assert m.target_model.name == "fhir_r4"
        assert "person" in m.get_entity_map()

    def test_loads_built_in_fhir_to_omop(self):
        m = _make_mapper("fhir_r4", "omop_cdm_v5.4")
        assert m.target_model.name == "omop_cdm_v5.4"

    def test_custom_crossmap_yaml(self, tmp_path):
        custom = tmp_path / "custom.yaml"
        custom.write_text(
            "source: omop_cdm_v5.4\n"
            "target: fhir_r4\n"
            "entity_map:\n"
            "  person: Patient\n"
            "field_map:\n"
            "  person.person_id: Patient.id\n"
        )
        m = _make_mapper(custom=custom)
        assert m.get_entity_map() == {"person": "Patient"}

    def test_no_built_in_crossmap_falls_through_to_empty(self, tmp_path):
        # Two real standards but no crossmap between them
        m = _make_mapper("openehr_1.0.4", "hl7v2_2.5.1")
        assert m.get_entity_map() == {}


# ── map_record ───────────────────────────────────────────────────


class TestMapRecord:
    def test_omop_person_to_fhir_patient(self):
        m = _make_mapper()
        record = {
            "person_id": 12345,
            "gender_concept_id": 8507,
            "year_of_birth": 1985,
            "month_of_birth": 3,
            "day_of_birth": 15,
        }
        result = m.map_record("person", record)
        # The crossmap maps person_id -> Patient.id
        assert "id" in result
        assert str(result["id"]) == "12345"

    def test_unknown_source_entity_passthrough(self):
        m = _make_mapper()
        # No mapping for "unknown_entity" → returns original record
        result = m.map_record("unknown_entity", {"x": 1})
        assert result == {"x": 1}

    def test_map_record_handles_missing_field(self):
        m = _make_mapper()
        # Record with one of the expected fields missing
        result = m.map_record("person", {"person_id": 1})
        # Map_record shouldn't crash
        assert isinstance(result, dict)


# ── map_records ──────────────────────────────────────────────────


class TestMapRecords:
    def test_basic_list(self):
        m = _make_mapper()
        records = [
            {"person_id": 1, "gender_concept_id": 8507, "year_of_birth": 1980},
            {"person_id": 2, "gender_concept_id": 8532, "year_of_birth": 1990},
        ]
        result = m.map_records("person", records)
        assert len(result) == 2
        assert isinstance(result[0], dict)


# ── map_dataframe ────────────────────────────────────────────────


class TestMapDataFrame:
    def test_pandas_dataframe(self):
        m = _make_mapper()
        df = pd.DataFrame(
            [
                {"person_id": 1, "gender_concept_id": 8507, "year_of_birth": 1980},
                {"person_id": 2, "gender_concept_id": 8532, "year_of_birth": 1990},
            ]
        )
        result = m.map_dataframe("person", df)
        # Either pandas or list of dicts depending on impl
        assert hasattr(result, "__len__")
        assert len(result) == 2


# ── Introspection ────────────────────────────────────────────────


class TestIntrospection:
    def test_get_entity_map(self):
        m = _make_mapper()
        em = m.get_entity_map()
        assert em["person"] == "Patient"

    def test_get_field_map_for_entity(self):
        m = _make_mapper()
        fm = m.get_field_map("person")
        assert isinstance(fm, dict)

    def test_get_field_map_unknown_entity(self):
        m = _make_mapper()
        fm = m.get_field_map("definitely_not_an_entity")
        # Empty dict for an unknown entity
        assert fm == {}

    def test_get_mapping_report(self):
        m = _make_mapper()
        report = m.get_mapping_report()
        assert isinstance(report, dict)
        assert report["source_standard"] == "omop_cdm_v5.4"
        assert report["target_standard"] == "fhir_r4"
        assert "entity_mappings" in report
        assert isinstance(report["field_mappings"], int)
        assert isinstance(report["unmapped_source_fields"], list)
        assert isinstance(report["unmapped_target_fields"], list)


# ── _set_nested helper (private API but worth covering) ─────────


class TestSetNested:
    def test_top_level_field(self):
        from portiere.local.cross_mapper import CrossStandardMapper

        d: dict = {}
        CrossStandardMapper._set_nested(d, "id", 42)
        assert d == {"id": 42}

    def test_nested_path(self):
        from portiere.local.cross_mapper import CrossStandardMapper

        d: dict = {}
        CrossStandardMapper._set_nested(d, "code.system", "http://snomed.info/sct")
        assert d == {"code": {"system": "http://snomed.info/sct"}}

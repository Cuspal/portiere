"""Tests for YAMLTargetModel.generate_ddl() and helpers (Slice 8 coverage gap-fill).

Covers the four standard-type-specific DDL generators (SQL, FHIR
StructureDefinition, HL7v2 segment summary, OpenEHR archetype summary)
plus the generic fallback and the SQL-type helper.
"""

from __future__ import annotations

import json

# ── generate_ddl() dispatch ──────────────────────────────────────


class TestDispatch:
    def test_relational_dispatches_to_sql(self):
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("omop_cdm_v5.4")
        ddl = m.generate_ddl()
        assert "CREATE TABLE" in ddl

    def test_resource_dispatches_to_fhir(self):
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("fhir_r4")
        out = m.generate_ddl()
        # FHIR generator returns JSON
        parsed = json.loads(out)
        assert isinstance(parsed, dict)
        assert "Patient" in parsed
        assert parsed["Patient"]["resourceType"] == "Patient"

    def test_segment_dispatches_to_hl7v2(self):
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("hl7v2_2.5.1")
        out = m.generate_ddl()
        assert "Segment Definitions" in out

    def test_archetype_dispatches_to_openehr(self):
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("openehr_1.0.4")
        out = m.generate_ddl()
        assert "Archetype Definitions" in out


# ── _generate_sql_ddl details ────────────────────────────────────


class TestSqlDdl:
    def test_includes_all_omop_entities(self):
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("omop_cdm_v5.4")
        sql = m._generate_sql_ddl()

        # Sanity: every entity gets a CREATE TABLE
        for entity in m.get_schema():
            assert f"CREATE TABLE {entity}" in sql

    def test_emits_foreign_keys(self):
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("omop_cdm_v5.4")
        sql = m._generate_sql_ddl()
        # condition_occurrence.person_id is annotated with fk: person.person_id
        assert "FOREIGN KEY" in sql
        assert "REFERENCES person(person_id)" in sql

    def test_uses_ddl_field_when_present(self):
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("omop_cdm_v5.4")
        sql = m._generate_sql_ddl()
        # Person.person_id has explicit ddl: "INTEGER PRIMARY KEY"
        assert "INTEGER PRIMARY KEY" in sql


# ── _generate_fhir_structure details ─────────────────────────────


class TestFhirStructure:
    def test_includes_required_flags(self):
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("fhir_r4")
        out = json.loads(m._generate_fhir_structure())
        # Patient.id is required
        assert out["Patient"]["fields"]["id"]["required"] is True

    def test_descriptions_present(self):
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("fhir_r4")
        out = json.loads(m._generate_fhir_structure())
        # Some fields have descriptions
        any_with_desc = any(f.get("description", "") for f in out["Patient"]["fields"].values())
        assert any_with_desc


# ── _generate_segment_summary / _generate_archetype_summary ─────


class TestSegmentSummary:
    def test_lists_entities(self):
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("hl7v2_2.5.1")
        out = m._generate_segment_summary()
        # At least one segment
        for entity in list(m.get_schema())[:2]:
            assert entity in out


class TestArchetypeSummary:
    def test_lists_archetypes(self):
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("openehr_1.0.4")
        out = m._generate_archetype_summary()
        for entity in list(m.get_schema())[:2]:
            assert entity in out


# ── Generic / fallback summary ───────────────────────────────────


class TestGenericSummary:
    def test_unknown_type_uses_generic(self, tmp_path):
        from portiere.standards import YAMLTargetModel

        # Custom YAML with a non-recognized standard_type
        yaml = """
name: my_custom
version: "1.0"
standard_type: custom_unknown_type
entities:
  thing:
    fields:
      x: {type: integer, required: true, description: an x}
"""
        p = tmp_path / "std.yaml"
        p.write_text(yaml)
        m = YAMLTargetModel(p)
        out = m.generate_ddl()
        assert isinstance(out, str)
        assert "thing" in out


# ── _type_to_sql helper ──────────────────────────────────────────


class TestTypeToSql:
    def test_known_types(self):
        from portiere.standards import YAMLTargetModel

        # Static method-ish helper; instantiate any model to call it
        m = YAMLTargetModel.from_name("omop_cdm_v5.4")
        # Basic mapping
        assert m._type_to_sql("integer") in ("INTEGER", "BIGINT", "INT")
        # Date / datetime
        sql_date = m._type_to_sql("date")
        assert "DATE" in sql_date.upper() or sql_date.upper() == "DATE"

    def test_unknown_type_falls_back(self):
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("omop_cdm_v5.4")
        out = m._type_to_sql("__not_a_real_type__")
        # Fallback should still be a string (e.g., VARCHAR or TEXT)
        assert isinstance(out, str)
        assert len(out) > 0


# ── __repr__ ─────────────────────────────────────────────────────


class TestRepr:
    def test_repr_contains_name_and_version(self):
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("omop_cdm_v5.4")
        r = repr(m)
        assert "omop_cdm_v5.4" in r
        assert "v5.4" in r

"""Tests for portiere.local.transforms (Slice 8 coverage gap-fill).

Each built-in transform is independently testable with simple inputs.
TransformRegistry is the dispatcher; the transform_* functions are the
worker functions called per-field during cross-standard mapping.
"""

from __future__ import annotations

from datetime import datetime

# ── TransformRegistry ────────────────────────────────────────────


class TestTransformRegistry:
    def test_builtins_registered(self):
        from portiere.local.transforms import TransformRegistry

        r = TransformRegistry()
        names = r.list_transforms()
        for required in (
            "passthrough",
            "str",
            "int",
            "float",
            "bool",
            "value_map",
            "format",
            "codeable_concept",
            "fhir_reference",
            "fhir_date",
            "fhir_period",
            "hl7v2_field",
            "dv_quantity",
            "dv_coded_text",
            "vocabulary_lookup",
        ):
            assert required in names

    def test_get_returns_callable(self):
        from portiere.local.transforms import TransformRegistry

        r = TransformRegistry()
        assert callable(r.get("passthrough"))

    def test_get_unknown_returns_none(self):
        from portiere.local.transforms import TransformRegistry

        assert TransformRegistry().get("nope") is None

    def test_register_custom(self):
        from portiere.local.transforms import TransformRegistry

        r = TransformRegistry()
        r.register("upper", lambda v, **kw: str(v).upper())
        assert r.execute("upper", "hi") == "HI"

    def test_execute_unknown_falls_back_to_passthrough(self):
        from portiere.local.transforms import TransformRegistry

        r = TransformRegistry()
        assert r.execute("nonexistent_transform", "abc") == "abc"

    def test_execute_failing_transform_returns_original(self):
        from portiere.local.transforms import TransformRegistry

        def boom(value, **_):
            raise RuntimeError("intentional failure")

        r = TransformRegistry()
        r.register("boom", boom)
        assert r.execute("boom", "fallback_value") == "fallback_value"


# ── Type casts ───────────────────────────────────────────────────


class TestTypeCasts:
    def test_passthrough(self):
        from portiere.local.transforms import transform_passthrough

        assert transform_passthrough(42) == 42
        assert transform_passthrough("x") == "x"

    def test_str_none_to_empty(self):
        from portiere.local.transforms import transform_str

        assert transform_str(None) == ""
        assert transform_str(42) == "42"
        assert transform_str("hi") == "hi"

    def test_int_normal(self):
        from portiere.local.transforms import transform_int

        assert transform_int("42") == 42
        assert transform_int(3.7) == 3
        assert transform_int(None) is None

    def test_int_invalid_returns_none(self):
        from portiere.local.transforms import transform_int

        assert transform_int("not a number") is None
        assert transform_int([1, 2]) is None

    def test_float_normal(self):
        from portiere.local.transforms import transform_float

        assert transform_float("3.14") == 3.14
        assert transform_float(2) == 2.0
        assert transform_float(None) is None

    def test_float_invalid_returns_none(self):
        from portiere.local.transforms import transform_float

        assert transform_float("xyz") is None

    def test_bool_truthy_strings(self):
        from portiere.local.transforms import transform_bool

        for s in ("true", "TRUE", "yes", "1", "y", "t"):
            assert transform_bool(s) is True
        for s in ("false", "no", "0", "n", "", "other"):
            assert transform_bool(s) is False

    def test_bool_passthrough_for_bool(self):
        from portiere.local.transforms import transform_bool

        assert transform_bool(True) is True
        assert transform_bool(False) is False

    def test_bool_other_uses_python_bool(self):
        from portiere.local.transforms import transform_bool

        assert transform_bool(1) is True
        assert transform_bool(0) is False
        assert transform_bool([]) is False
        assert transform_bool([1]) is True

    def test_bool_none(self):
        from portiere.local.transforms import transform_bool

        assert transform_bool(None) is None


# ── value_map ────────────────────────────────────────────────────


class TestValueMap:
    def test_no_config_passthrough(self):
        from portiere.local.transforms import transform_value_map

        assert transform_value_map("x") == "x"

    def test_exact_match(self):
        from portiere.local.transforms import transform_value_map

        config = {"mapping": {"M": "MALE", "F": "FEMALE"}}
        assert transform_value_map("M", config=config) == "MALE"
        assert transform_value_map("F", config=config) == "FEMALE"

    def test_string_match_for_numeric_keys(self):
        from portiere.local.transforms import transform_value_map

        # YAML may parse numeric keys as ints; we should still match
        # when the input is a string version of the same number.
        config = {"mapping": {8507: "MALE", 8532: "FEMALE"}}
        assert transform_value_map("8507", config=config) == "MALE"

    def test_default_used_when_unknown(self):
        from portiere.local.transforms import transform_value_map

        config = {"mapping": {"M": "MALE"}, "default": "UNKNOWN"}
        assert transform_value_map("Z", config=config) == "UNKNOWN"

    def test_default_falls_back_to_value(self):
        from portiere.local.transforms import transform_value_map

        config = {"mapping": {"M": "MALE"}}
        # No default → returns original value
        assert transform_value_map("Z", config=config) == "Z"


# ── format ───────────────────────────────────────────────────────


class TestFormat:
    def test_none_returns_empty(self):
        from portiere.local.transforms import transform_format

        assert transform_format(None) == ""

    def test_no_config_returns_str(self):
        from portiere.local.transforms import transform_format

        assert transform_format(42) == "42"

    def test_datetime_formatting(self):
        from portiere.local.transforms import transform_format

        dt = datetime(2025, 6, 15)
        assert transform_format(dt, config={"pattern": "%Y/%m/%d"}) == "2025/06/15"

    def test_string_with_input_format(self):
        from portiere.local.transforms import transform_format

        config = {"pattern": "%Y-%m-%d", "input_format": "%m/%d/%Y"}
        assert transform_format("06/15/2025", config=config) == "2025-06-15"

    def test_invalid_input_format_falls_through(self):
        from portiere.local.transforms import transform_format

        config = {"pattern": "%Y-%m-%d", "input_format": "%m/%d/%Y"}
        # Doesn't match input_format → falls through to pattern.format()
        # which is a no-op on patterns without {}, so we just get the
        # pattern back. Not the most useful behavior but stable.
        result = transform_format("not-a-date", config=config)
        assert isinstance(result, str)


# ── codeable_concept ─────────────────────────────────────────────


class TestCodeableConcept:
    def test_none_returns_empty_dict(self):
        from portiere.local.transforms import transform_codeable_concept

        assert transform_codeable_concept(None) == {}

    def test_basic_wrap(self):
        from portiere.local.transforms import transform_codeable_concept

        config = {"system": "http://snomed.info/sct"}
        result = transform_codeable_concept("44054006", config=config)
        assert result["coding"][0]["system"] == "http://snomed.info/sct"
        assert result["coding"][0]["code"] == "44054006"
        assert result["text"] == "44054006"

    def test_with_display_field(self):
        from portiere.local.transforms import transform_codeable_concept

        config = {"system": "http://snomed.info/sct", "display_field": "name"}
        record = {"name": "Type 2 diabetes"}
        result = transform_codeable_concept("44054006", config=config, record=record)
        assert result["coding"][0]["display"] == "Type 2 diabetes"
        assert result["text"] == "Type 2 diabetes"


# ── fhir_reference ───────────────────────────────────────────────


class TestFhirReference:
    def test_none_returns_empty(self):
        from portiere.local.transforms import transform_fhir_reference

        assert transform_fhir_reference(None) == {}

    def test_default_resource_type(self):
        from portiere.local.transforms import transform_fhir_reference

        assert transform_fhir_reference(42) == {"reference": "Patient/42"}

    def test_explicit_resource_type(self):
        from portiere.local.transforms import transform_fhir_reference

        assert transform_fhir_reference("abc", config={"resource_type": "Encounter"}) == {
            "reference": "Encounter/abc"
        }


# ── fhir_date ────────────────────────────────────────────────────


class TestFhirDate:
    def test_none(self):
        from portiere.local.transforms import transform_fhir_date

        assert transform_fhir_date(None) is None

    def test_datetime(self):
        from portiere.local.transforms import transform_fhir_date

        assert transform_fhir_date(datetime(2025, 6, 15, 10, 30)) == "2025-06-15"

    def test_string_iso(self):
        from portiere.local.transforms import transform_fhir_date

        assert transform_fhir_date("2025-06-15") == "2025-06-15"
        assert transform_fhir_date("2025-06-15 10:30:00") == "2025-06-15"

    def test_string_us_format(self):
        from portiere.local.transforms import transform_fhir_date

        assert transform_fhir_date("06/15/2025") == "2025-06-15"

    def test_unparseable_returns_string(self):
        from portiere.local.transforms import transform_fhir_date

        assert transform_fhir_date("not a date") == "not a date"


# ── fhir_period ──────────────────────────────────────────────────


class TestFhirPeriod:
    def test_no_config(self):
        from portiere.local.transforms import transform_fhir_period

        assert transform_fhir_period("anything") == {}

    def test_no_record(self):
        from portiere.local.transforms import transform_fhir_period

        assert transform_fhir_period("anything", config={"start_field": "s"}) == {}

    def test_both_fields_present(self):
        from portiere.local.transforms import transform_fhir_period

        config = {"start_field": "start", "end_field": "end"}
        record = {"start": "2025-01-01", "end": "2025-06-30"}
        result = transform_fhir_period(None, config=config, record=record)
        assert result == {"start": "2025-01-01", "end": "2025-06-30"}

    def test_only_start_present(self):
        from portiere.local.transforms import transform_fhir_period

        config = {"start_field": "start", "end_field": "end"}
        record = {"start": "2025-01-01"}
        result = transform_fhir_period(None, config=config, record=record)
        assert "start" in result
        assert "end" not in result


# ── hl7v2_field ──────────────────────────────────────────────────


class TestHl7v2Field:
    def test_none(self):
        from portiere.local.transforms import transform_hl7v2_field

        assert transform_hl7v2_field(None) == ""

    def test_value(self):
        from portiere.local.transforms import transform_hl7v2_field

        assert transform_hl7v2_field(42) == "42"


# ── dv_quantity ──────────────────────────────────────────────────


class TestDvQuantity:
    def test_none(self):
        from portiere.local.transforms import transform_dv_quantity

        assert transform_dv_quantity(None) == {}

    def test_with_static_units(self):
        from portiere.local.transforms import transform_dv_quantity

        result = transform_dv_quantity(98.6, config={"units": "F"})
        assert result["_type"] == "DV_QUANTITY"
        assert result["magnitude"] == 98.6
        assert result["units"] == "F"

    def test_with_unit_field(self):
        from portiere.local.transforms import transform_dv_quantity

        config = {"unit_field": "uom"}
        record = {"uom": "mg/dL"}
        result = transform_dv_quantity("180", config=config, record=record)
        assert result["units"] == "mg/dL"
        assert result["magnitude"] == 180.0

    def test_unit_field_falls_back_to_static_units(self):
        from portiere.local.transforms import transform_dv_quantity

        config = {"unit_field": "missing", "units": "static_unit"}
        record = {"other": "x"}
        result = transform_dv_quantity("1", config=config, record=record)
        assert result["units"] == "static_unit"


# ── dv_coded_text ────────────────────────────────────────────────


class TestDvCodedText:
    def test_none(self):
        from portiere.local.transforms import transform_dv_coded_text

        assert transform_dv_coded_text(None) == {}

    def test_basic(self):
        from portiere.local.transforms import transform_dv_coded_text

        config = {"terminology_id": "SNOMED-CT", "display_field": "name"}
        record = {"name": "Hypertension"}
        result = transform_dv_coded_text("38341003", config=config, record=record)
        assert result["_type"] == "DV_CODED_TEXT"
        assert result["value"] == "Hypertension"
        assert result["defining_code"]["terminology_id"]["value"] == "SNOMED-CT"
        assert result["defining_code"]["code_string"] == "38341003"


# ── vocabulary_lookup ────────────────────────────────────────────


class TestVocabularyLookup:
    def test_no_bridge_returns_value(self):
        from portiere.local.transforms import transform_vocabulary_lookup

        assert transform_vocabulary_lookup("E11.9") == "E11.9"

    def test_none_returns_none(self):
        from portiere.local.transforms import transform_vocabulary_lookup

        assert transform_vocabulary_lookup(None) is None

    def test_non_int_value_passes_through(self):
        from portiere.local.transforms import transform_vocabulary_lookup

        class FakeBridge:
            def map_concept(self, *args, **kw):
                return [{"concept_id": 999, "concept_name": "x", "concept_code": "X1"}]

        # value can't be int-coerced → passthrough
        assert (
            transform_vocabulary_lookup("not_an_int", vocabulary_bridge=FakeBridge())
            == "not_an_int"
        )

    def test_no_results_returns_original(self):
        from portiere.local.transforms import transform_vocabulary_lookup

        class FakeBridge:
            def map_concept(self, *args, **kw):
                return []

        assert transform_vocabulary_lookup(42, vocabulary_bridge=FakeBridge()) == 42

    def test_output_concept_id(self):
        from portiere.local.transforms import transform_vocabulary_lookup

        class FakeBridge:
            def map_concept(self, *args, **kw):
                return [{"concept_id": 999, "concept_name": "x", "concept_code": "X1"}]

        config = {"target_vocabulary": "SNOMED", "output": "concept_id"}
        result = transform_vocabulary_lookup(42, config=config, vocabulary_bridge=FakeBridge())
        assert result == 999

    def test_output_concept_name(self):
        from portiere.local.transforms import transform_vocabulary_lookup

        class FakeBridge:
            def map_concept(self, *args, **kw):
                return [{"concept_id": 999, "concept_name": "Diabetes", "concept_code": "X1"}]

        config = {"target_vocabulary": "SNOMED", "output": "concept_name"}
        result = transform_vocabulary_lookup(42, config=config, vocabulary_bridge=FakeBridge())
        assert result == "Diabetes"

    def test_output_concept_code_default(self):
        from portiere.local.transforms import transform_vocabulary_lookup

        class FakeBridge:
            def map_concept(self, *args, **kw):
                return [{"concept_id": 999, "concept_name": "x", "concept_code": "ABC"}]

        result = transform_vocabulary_lookup(
            42, config={"target_vocabulary": "SNOMED"}, vocabulary_bridge=FakeBridge()
        )
        assert result == "ABC"

    def test_output_codeable_concept(self):
        from portiere.local.transforms import transform_vocabulary_lookup

        class FakeBridge:
            def map_concept(self, *args, **kw):
                return [{"concept_id": 999, "concept_name": "x", "concept_code": "X1"}]

            def concept_to_codeable_concept(self, cid):
                return {"coding": [{"system": "x", "code": str(cid)}]}

        config = {"target_vocabulary": "SNOMED", "output": "codeable_concept"}
        result = transform_vocabulary_lookup(42, config=config, vocabulary_bridge=FakeBridge())
        assert "coding" in result

    def test_output_dv_coded_text(self):
        from portiere.local.transforms import transform_vocabulary_lookup

        class FakeBridge:
            def map_concept(self, *args, **kw):
                return [{"concept_id": 999, "concept_name": "x", "concept_code": "X1"}]

            def concept_to_dv_coded_text(self, cid):
                return {"_type": "DV_CODED_TEXT", "value": "x"}

        config = {"target_vocabulary": "SNOMED", "output": "dv_coded_text"}
        result = transform_vocabulary_lookup(42, config=config, vocabulary_bridge=FakeBridge())
        assert result["_type"] == "DV_CODED_TEXT"

    def test_unknown_output_falls_back_to_concept_code(self):
        from portiere.local.transforms import transform_vocabulary_lookup

        class FakeBridge:
            def map_concept(self, *args, **kw):
                return [{"concept_id": 999, "concept_name": "x", "concept_code": "ABC"}]

        config = {"target_vocabulary": "SNOMED", "output": "weird_unknown_output"}
        result = transform_vocabulary_lookup(42, config=config, vocabulary_bridge=FakeBridge())
        assert result == "ABC"

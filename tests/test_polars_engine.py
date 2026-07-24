"""Tests for PolarsEngine compute engine."""

from pathlib import Path

import pytest

from portiere.engines.polars_engine import PolarsEngine


@pytest.fixture
def engine():
    return PolarsEngine()


@pytest.fixture
def sample_csv(tmp_path):
    path = tmp_path / "patients.csv"
    path.write_text("patient_id,age,gender,diagnosis\n1,45,M,E11.9\n2,62,F,I10\n3,38,M,J06.9\n")
    return str(path)


class TestPolarsEngineBasics:
    def test_engine_name(self, engine):
        assert engine.engine_name == "polars"

    def test_read_csv(self, engine, sample_csv):
        df = engine.read_source(sample_csv, format="csv")
        assert engine.count(df) == 3
        assert "patient_id" in df.columns

    def test_read_csv_via_read_csv_method(self, engine, sample_csv):
        df = engine.read_csv(sample_csv)
        assert engine.count(df) == 3

    def test_profile_returns_row_and_column_counts(self, engine, sample_csv):
        df = engine.read_source(sample_csv)
        profile = engine.profile(df)
        assert profile["row_count"] == 3
        assert profile["column_count"] == 4
        assert len(profile["columns"]) == 4

    def test_profile_column_has_required_keys(self, engine, sample_csv):
        df = engine.read_source(sample_csv)
        profile = engine.profile(df)
        col = profile["columns"][0]
        assert "name" in col
        assert "type" in col
        assert "null_count" in col
        assert "n_unique" in col

    def test_schema_returns_name_type_pairs(self, engine, sample_csv):
        df = engine.read_source(sample_csv)
        schema = engine.schema(df)
        assert isinstance(schema, list)
        assert all("name" in s and "type" in s for s in schema)
        names = [s["name"] for s in schema]
        assert "patient_id" in names

    def test_count(self, engine, sample_csv):
        df = engine.read_source(sample_csv)
        assert engine.count(df) == 3

    def test_sample(self, engine, sample_csv):
        df = engine.read_source(sample_csv)
        sampled = engine.sample(df, 2)
        assert engine.count(sampled) == 2

    def test_sample_larger_than_df_returns_all(self, engine, sample_csv):
        df = engine.read_source(sample_csv)
        sampled = engine.sample(df, 100)
        assert engine.count(sampled) == 3

    def test_from_records(self, engine):
        records = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
        df = engine.from_records(records)
        assert engine.count(df) == 2
        assert "a" in df.columns


class TestPolarsEngineWrite:
    def test_write_csv(self, engine, sample_csv, tmp_path):
        df = engine.read_source(sample_csv)
        out = str(tmp_path / "out.csv")
        engine.write_csv(df, out)
        df2 = engine.read_csv(out)
        assert engine.count(df2) == 3

    def test_write_parquet(self, engine, sample_csv, tmp_path):
        df = engine.read_source(sample_csv)
        out = str(tmp_path / "out.parquet")
        engine.write(df, out, format="parquet")
        df2 = engine.read_source(out, format="parquet")
        assert engine.count(df2) == 3

    def test_write_json(self, engine, sample_csv, tmp_path):
        df = engine.read_source(sample_csv)
        out = str(tmp_path / "out.json")
        engine.write(df, out, format="json")
        assert Path(out).exists()

    def test_write_unsupported_format_raises(self, engine, sample_csv, tmp_path):
        df = engine.read_source(sample_csv)
        with pytest.raises(ValueError, match="Unsupported format"):
            engine.write(df, str(tmp_path / "out.xyz"), format="xyz")


class TestPolarsEngineTransform:
    def test_transform_rename(self, engine, sample_csv):
        df = engine.read_source(sample_csv)
        result = engine.transform(df, {"renames": {"patient_id": "id"}})
        assert "id" in result.columns
        assert "patient_id" not in result.columns

    def test_transform_select(self, engine, sample_csv):
        df = engine.read_source(sample_csv)
        result = engine.transform(df, {"select": ["patient_id", "age"]})
        assert result.columns == ["patient_id", "age"]

    def test_transform_rename_nonexistent_column(self, engine, sample_csv):
        df = engine.read_source(sample_csv)
        # Should not raise — just skip columns that don't exist
        result = engine.transform(df, {"renames": {"nonexistent": "new_name"}})
        assert "nonexistent" not in result.columns
        assert "new_name" not in result.columns

    def test_transform_empty_spec(self, engine, sample_csv):
        df = engine.read_source(sample_csv)
        result = engine.transform(df, {})
        assert engine.count(result) == 3


class TestPolarsEngineDistinct:
    def test_get_distinct_values(self, engine, sample_csv):
        df = engine.read_source(sample_csv)
        distinct = engine.get_distinct_values(df, "gender")
        assert len(distinct) == 2
        assert all("value" in d and "count" in d for d in distinct)

    def test_get_distinct_values_with_limit(self, engine, sample_csv):
        df = engine.read_source(sample_csv)
        distinct = engine.get_distinct_values(df, "patient_id", limit=2)
        assert len(distinct) == 2


class TestPolarsEngineMisc:
    def test_to_pandas(self, engine, sample_csv):
        import pandas as pd

        df = engine.read_source(sample_csv)
        pdf = engine.to_pandas(df)
        assert isinstance(pdf, pd.DataFrame)
        assert len(pdf) == 3

    def test_read_source_unsupported_format_raises(self, engine, sample_csv):
        with pytest.raises(ValueError, match="Unsupported format"):
            engine.read_source(sample_csv, format="xlsx")

    def test_map_column(self, engine, sample_csv):
        df = engine.read_source(sample_csv)
        mapping = {"M": 8507, "F": 8532}
        result = engine.map_column(df, "gender", mapping, "gender_concept_id", default=0)
        assert "gender_concept_id" in result.columns


class TestPolarsEngineEnrichment:
    def test_profile_enrichment(self, engine):
        import polars as pl

        df = pl.DataFrame({"code": ["A", " ", "", None, "BB"], "n": [1, 2, 3, 4, 5]})
        cols = {c["name"]: c for c in engine.profile(df)["columns"]}
        # "A","BB" present; " ","" whitespace/empty and None are missing
        assert cols["code"]["present_count"] == 2
        assert cols["code"]["min_len"] == 1
        assert cols["code"]["max_len"] == 2
        assert cols["code"]["example"] == "A"
        # numeric column: only nulls are missing
        assert cols["n"]["present_count"] == 5
        # existing keys unchanged
        assert cols["n"]["null_pct"] == 0.0

    def test_profile_empty_as_missing_off(self, engine):
        import polars as pl

        df = pl.DataFrame({"code": ["A", " ", "", None]})
        cols = {c["name"]: c for c in engine.profile(df, empty_as_missing=False)["columns"]}
        # only None missing when empty_as_missing is off
        assert cols["code"]["present_count"] == 3


class TestPolarsEnginePresentTop:
    def test_top_value_over_present_excludes_empty(self, engine):
        import polars as pl

        # "" is the single most common raw value, but the present top must be a
        # real value with share <= 100%.
        df = pl.DataFrame({"race": ["", "", "", "", "White", "White", "Asian"]})
        col = engine.profile(df)["columns"][0]
        assert col["present_top_value"] == "White"
        assert col["present_top_count"] == 2
        assert col["present_n_distinct"] == 2  # White, Asian (empty excluded)
        # share stays within bounds: 2 present-top / 3 present = 66.7%
        assert col["present_top_count"] <= col["present_count"]


class TestPolarsEngineNumericStats:
    def test_numeric_stats_present(self, engine):
        import polars as pl

        df = pl.DataFrame({"age": [10, 20, 30, 40], "name": ["a", "b", "c", "d"]})
        cols = {c["name"]: c for c in engine.profile(df)["columns"]}
        assert cols["age"]["num_min"] == 10.0
        assert cols["age"]["num_max"] == 40.0
        assert cols["age"]["num_mean"] == 25.0
        assert cols["age"]["num_std"] is not None
        # string columns carry no numeric stats
        assert cols["name"]["num_mean"] is None

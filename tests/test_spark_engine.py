"""Tests for SparkEngine — skipped when pyspark is not installed."""

import pytest

pytest.importorskip("pyspark")

from portiere.engines.spark_engine import SparkEngine


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[1]").appName("portiere-tests").getOrCreate()
    yield session
    session.stop()


class TestSparkEngineEnrichment:
    def test_profile_enrichment(self, spark):
        df = spark.createDataFrame([("A",), (" ",), ("",), (None,), ("BB",)], ["code"])
        cols = {c["name"]: c for c in SparkEngine().profile(df)["columns"]}
        assert cols["code"]["present_count"] == 2
        assert cols["code"]["min_len"] == 1
        assert cols["code"]["max_len"] == 2
        assert cols["code"]["example"] in ("A", "BB")

    def test_profile_empty_as_missing_off(self, spark):
        df = spark.createDataFrame([("A",), (" ",), ("",), (None,)], ["code"])
        cols = {c["name"]: c for c in SparkEngine().profile(df, empty_as_missing=False)["columns"]}
        assert cols["code"]["present_count"] == 3

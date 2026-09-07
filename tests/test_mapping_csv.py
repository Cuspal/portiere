"""Mapping artifacts preserve identifiers across supported CSV transports."""

import pytest

from portiere.models.concept_mapping import ConceptMapping
from portiere.models.schema_mapping import SchemaMapping


@pytest.mark.parametrize("suffix", [".csv.gz", ".csv.bz2", ".csv.xz"])
@pytest.mark.parametrize(
    "model,column", [(ConceptMapping, "source_code"), (SchemaMapping, "source_column")]
)
def test_compressed_export_import(tmp_path, suffix, model, column):
    original = model(items=[{column: "00123"}, {column: "NA"}])
    path = str(tmp_path / ("mapping" + suffix))
    original.to_csv(path)

    restored = model.from_csv(path)

    assert [getattr(item, column) for item in restored.items] == ["00123", "NA"]


@pytest.mark.parametrize("engine_name", ["pandas", "polars"])
def test_engine_keeps_numeric_only_codes(tmp_path, engine_name):
    from portiere.engines import get_engine

    path = tmp_path / "mapping.csv"
    path.write_text("source_code,target_concept_id\n00123,123\n00456,456\n")
    mapping = ConceptMapping.from_csv(str(path), engine=get_engine(engine_name))

    assert [item.source_code for item in mapping.items] == ["00123", "00456"]


@pytest.mark.parametrize("engine_name", ["pandas", "polars"])
def test_schema_export_without_source_table_can_be_imported(tmp_path, engine_name):
    from portiere.engines import get_engine

    original = SchemaMapping(items=[{"source_column": "00123"}])
    path = str(tmp_path / "mapping.csv")
    original.to_csv(path)

    restored = SchemaMapping.from_csv(path, engine=get_engine(engine_name))

    assert restored.items[0].source_column == "00123"
    assert restored.items[0].source_table == ""

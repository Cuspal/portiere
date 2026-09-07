"""External reviews preserve source ownership and optimistic concurrency."""

import pytest

import portiere
from portiere.config import PortiereConfig
from portiere.engines import get_engine
from portiere.models.concept_mapping import ConceptMapping
from portiere.models.schema_mapping import SchemaMapping
from portiere.runner.etl_runner import ETLRunner


@pytest.fixture
def project(tmp_path):
    return portiere.init("hospital", config=PortiereConfig(local_project_dir=tmp_path / "projects"))


def concept(source_id):
    return ConceptMapping(
        source_id=source_id,
        items=[
            {
                "source_code": "00123",
                "source_column": "code",
                "target_concept_id": 100,
                "method": "auto",
            }
        ],
    )


@pytest.mark.parametrize("extension", ["csv", "json"])
def test_external_review_round_trip_and_stale_import(project, tmp_path, extension):
    project.save_concept_mapping(concept("a"))
    project.save_concept_mapping(concept("b"))
    path = str(tmp_path / f"review.{extension}")
    project.export_concept_mapping(path, source_id="a")
    external = getattr(ConceptMapping, f"from_{extension}")(path)
    assert external.source_id == "a"
    assert external.revision == project.load_concept_mapping(source_id="a").revision
    external.items[0].reject()
    getattr(external, f"to_{extension}")(path)
    imported = project.import_concept_mapping(path, source_id="a")
    assert imported.items[0].rejected
    assert project.load_concept_mapping(source_id="b").items[0].approved
    with pytest.raises(ValueError, match=r"revision|changed"):
        project.import_concept_mapping(path, source_id="a")


def test_external_review_cannot_be_rebound_to_another_source(project, tmp_path):
    project.save_concept_mapping(concept("a"))
    path = str(tmp_path / "review.json")
    project.export_concept_mapping(path, source_id="a")
    with pytest.raises(ValueError, match=r"source"):
        project.import_concept_mapping(path, source_id="b")


def test_legacy_import_replacement_requires_explicit_revision(project):
    records = [{"source_code": "00123", "target_concept_id": 100, "method": "auto"}]
    first = project.import_concept_mapping(records=records, source_id="a")
    with pytest.raises(ValueError, match=r"revision"):
        project.import_concept_mapping(records=records, source_id="a")
    replaced = project.import_concept_mapping(
        records=records, source_id="a", expected_revision=first.revision
    )
    assert replaced.source_id == "a"


def test_mixed_csv_context_is_rejected():
    with pytest.raises(ValueError, match=r"source|metadata"):
        ConceptMapping.from_records(
            [
                {"source_code": "a", "mapping_source_id": "a", "mapping_revision": "r"},
                {"source_code": "b", "mapping_source_id": "b", "mapping_revision": "r"},
            ]
        )


def test_empty_csv_review_requires_json(tmp_path):
    mapping = ConceptMapping(source_id="a", revision="r")
    with pytest.raises(ValueError, match="JSON"):
        mapping.to_csv(str(tmp_path / "empty.csv"))
    assert not (tmp_path / "empty.csv").exists()
    mapping.to_json(str(tmp_path / "empty.json"))
    assert ConceptMapping.from_json(str(tmp_path / "empty.json")).source_id == "a"


def test_csv_exchange_does_not_require_pandas(tmp_path, monkeypatch):
    import sys

    monkeypatch.setitem(sys.modules, "pandas", None)
    path = str(tmp_path / "review.csv")
    original = concept("a")
    original.to_csv(path)
    assert ConceptMapping.from_csv(path).items[0].source_code == "00123"


def test_polars_project_exports_without_pandas(project, tmp_path, monkeypatch):
    import sys

    project.save_concept_mapping(concept("a"))
    monkeypatch.setitem(sys.modules, "pandas", None)
    project.export_concept_mapping(str(tmp_path / "review.csv"), source_id="a")
    project.export_concept_mapping(str(tmp_path / "approved.csv"), source_id="a", omop_format=True)
    assert "00123" in (tmp_path / "approved.csv").read_text()


@pytest.mark.parametrize("suffix", [".csv.gz", ".csv.zip"])
def test_compressed_review_and_approved_exports_round_trip(project, tmp_path, suffix):
    project.save_concept_mapping(concept("a"))
    review = str(tmp_path / f"review{suffix}")
    approved = str(tmp_path / f"approved{suffix}")
    project.export_concept_mapping(review, source_id="a")
    project.export_concept_mapping(approved, source_id="a", omop_format=True)
    assert ConceptMapping.from_csv(review).items[0].source_code == "00123"
    assert ConceptMapping.from_csv(approved).items[0].source_code == "00123"
    assert project.import_concept_mapping(review, source_id="a").items[0].source_code == "00123"


def test_legacy_mapping_replacement_still_requires_revision(project, tmp_path):
    import yaml

    path = tmp_path / "projects" / "hospital" / "concept_mappings" / "concept_mapping.yaml"
    path.write_text(yaml.safe_dump([{"source_code": "old", "method": "review"}]))
    with pytest.raises(ValueError, match="revision"):
        project.import_concept_mapping(records=[{"source_code": "new", "method": "auto"}])
    assert project.load_concept_mapping().items[0].source_code == "old"


def test_runner_rejects_mappings_from_different_sources():
    with pytest.raises(ValueError, match=r"source"):
        ETLRunner.from_mappings(get_engine("polars"), SchemaMapping(source_id="a"), concept("b"))


def test_no_approved_routes_cannot_report_success(tmp_path):
    from portiere.exceptions import ETLExecutionError

    source = tmp_path / "source.csv"
    source.write_text("id\n1\n")
    runner = ETLRunner.from_mappings(get_engine("polars"), SchemaMapping(), ConceptMapping())
    with pytest.raises(ETLExecutionError, match=r"approved|actionable"):
        runner.run(str(source), str(tmp_path / "output"))
    assert not (tmp_path / "output").exists()

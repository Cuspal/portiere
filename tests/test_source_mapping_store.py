"""SDK and review UI share source-bound revisions without lost updates."""

import pytest

from portiere.models.concept_mapping import ConceptMapping
from portiere.models.schema_mapping import SchemaMapping
from portiere.review_ui import state
from portiere.storage.local_backend import LocalStorageBackend


@pytest.fixture
def storage(tmp_path):
    backend = LocalStorageBackend(tmp_path)
    backend.create_project("hospital", "omop_cdm_v5.4", [])
    return backend


def schema(source_id, column):
    return SchemaMapping(
        source_id=source_id,
        items=[
            {
                "source_column": column,
                "target_table": "person",
                "target_column": "person_id",
                "status": "auto_accepted",
            }
        ],
    )


def test_two_sources_survive_save_and_reload(storage):
    storage.save_schema_mapping("hospital", schema("clinic-a", "patient_id"))
    storage.save_schema_mapping("hospital", schema("clinic-b", "local_id"))

    a = storage.load_schema_mapping("hospital", source_id="clinic-a")
    b = storage.load_schema_mapping("hospital", source_id="clinic-b")

    assert a.source_id == "clinic-a"
    assert a.items[0].source_column == "patient_id"
    assert b.items[0].source_column == "local_id"
    with pytest.raises(ValueError, match=r"source"):
        storage.load_schema_mapping("hospital")


def test_schema_review_reaches_sdk_for_only_selected_source(storage, tmp_path):
    storage.save_schema_mapping("hospital", schema("clinic-a", "patient_id"))
    storage.save_schema_mapping("hospital", schema("clinic-b", "local_id"))
    loaded = state.load_schema_mapping(tmp_path / "hospital", source_id="clinic-a")
    reviewed = state.apply_user_decision(loaded, index=0, decision="reject")
    state.save_reviewed_schema_mapping(reviewed, tmp_path / "hospital")

    assert (
        storage.load_schema_mapping("hospital", source_id="clinic-a").items[0].status == "rejected"
    )
    assert (
        storage.load_schema_mapping("hospital", source_id="clinic-b").items[0].status
        == "auto_accepted"
    )


def test_concept_review_reaches_sdk_and_preserves_source(storage, tmp_path):
    storage.save_concept_mapping(
        "hospital",
        ConceptMapping(
            source_id="clinic-a",
            items=[
                {
                    "source_code": "00123",
                    "source_column": "code",
                    "target_concept_id": 100,
                    "method": "auto",
                }
            ],
        ),
    )
    loaded = state.load_concept_mapping(tmp_path / "hospital", source_id="clinic-a")
    reviewed = state.apply_concept_decision(loaded, index=0, decision="reject")
    state.save_reviewed_concept_mapping(reviewed, tmp_path / "hospital")

    restored = storage.load_concept_mapping("hospital", source_id="clinic-a")
    assert restored.source_id == "clinic-a"
    assert restored.items[0].rejected is True
    assert restored.to_source_to_concept_map() == []


def test_stale_review_cannot_overwrite_newer_decision(storage):
    storage.save_schema_mapping("hospital", schema("clinic-a", "patient_id"))
    first = storage.load_schema_mapping("hospital", source_id="clinic-a")
    stale = storage.load_schema_mapping("hospital", source_id="clinic-a")
    first.items[0].reject()
    storage.save_schema_mapping("hospital", first)
    stale.items[0].approve()

    with pytest.raises(ValueError, match=r"changed|revision|reload"):
        storage.save_schema_mapping("hospital", stale)
    assert (
        storage.load_schema_mapping("hospital", source_id="clinic-a").items[0].status == "rejected"
    )


def test_review_session_keeps_displayed_revision_until_explicit_reload(storage, tmp_path):
    storage.save_schema_mapping("hospital", schema("clinic-a", "patient_id"))
    session = {}
    displayed = state.load_review_session(session, tmp_path / "hospital", "schema", "clinic-a")
    concurrent = storage.load_schema_mapping("hospital", source_id="clinic-a")
    concurrent.items[0].reject()
    storage.save_schema_mapping("hospital", concurrent)

    rerendered = state.load_review_session(session, tmp_path / "hospital", "schema", "clinic-a")
    assert rerendered.revision == displayed.revision
    edit = state.apply_user_decision(rerendered, index=0, decision="approve")
    with pytest.raises(ValueError, match=r"revision"):
        state.save_review_session(session, tmp_path / "hospital", "schema", "clinic-a", edit)
    refreshed = state.load_review_session(
        session, tmp_path / "hospital", "schema", "clinic-a", reload=True
    )
    assert refreshed.items[0].status == "rejected"


def test_review_session_cannot_save_under_another_source(storage, tmp_path):
    storage.save_schema_mapping("hospital", schema("clinic-a", "patient_id"))
    storage.save_schema_mapping("hospital", schema("clinic-b", "local_id"))
    a = storage.load_schema_mapping("hospital", source_id="clinic-a")
    a.items[0].reject()

    with pytest.raises(ValueError, match=r"source"):
        state.save_review_session({}, tmp_path / "hospital", "schema", "clinic-b", a)
    assert (
        storage.load_schema_mapping("hospital", source_id="clinic-a").items[0].status
        == "auto_accepted"
    )


def test_legacy_mapping_is_not_bound_while_another_source_has_no_id(storage, tmp_path):
    import yaml

    storage.save_source("hospital", "clinic-a", {"id": "a", "name": "clinic-a"})
    storage.save_source("hospital", "clinic-b", {"name": "clinic-b"})
    legacy = tmp_path / "hospital" / "schema_mappings" / "schema_mapping.yaml"
    legacy.write_text(yaml.safe_dump([{"source_table": "clinic-b", "source_column": "b_id"}]))

    assert storage.load_schema_mapping("hospital", source_id="a").items == []


def test_legacy_alias_and_scoped_write_cannot_both_win(storage, tmp_path, monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    import yaml

    from portiere.storage.mapping_store import MappingStore

    storage.save_source("hospital", "clinic-a", {"id": "a", "name": "clinic-a"})
    legacy = tmp_path / "hospital" / "schema_mappings" / "schema_mapping.yaml"
    legacy.write_text(yaml.safe_dump([{"source_column": "patient_id", "status": "needs_review"}]))
    unscoped = storage.load_schema_mapping("hospital")
    scoped = storage.load_schema_mapping("hospital", source_id="a")
    unscoped.items[0].reject()
    scoped.items[0].approve()
    barrier = threading.Barrier(2)
    original = MappingStore.load

    def simultaneous_read(self, *args, **kwargs):
        value = original(self, *args, **kwargs)
        try:
            barrier.wait(timeout=0.5)
        except threading.BrokenBarrierError:
            pass
        return value

    monkeypatch.setattr(MappingStore, "load", simultaneous_read)

    def save(mapping):
        try:
            storage.save_schema_mapping("hospital", mapping)
            return True
        except ValueError:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(save, [unscoped, scoped]))
    assert sum(outcomes) == 1


def test_two_source_sdk_review_reload_and_etl(tmp_path):
    import polars as pl

    import portiere
    from portiere.config import EmbeddingConfig, PortiereConfig, RerankerConfig

    project = portiere.init(
        "hospital",
        config=PortiereConfig(
            local_project_dir=tmp_path / "projects",
            embedding=EmbeddingConfig(provider="none"),
            reranker=RerankerConfig(provider="none"),
            offline=True,
        ),
    )
    first_file = tmp_path / "first.csv"
    second_file = tmp_path / "second.csv"
    first_file.write_text("patient_id\n1\n")
    second_file.write_text("patient_id\n2\n")
    first = project.add_source(str(first_file), name="clinic-a")
    second = project.add_source(str(second_file), name="clinic-b")
    a = project.map_schema(first)
    b = project.map_schema(second)
    assert a.source_id == first["id"]
    assert b.source_id == second["id"]
    b.items[0].approve("person", "person_source_value")
    project.save_schema_mapping(b)

    for source, column in [(first, "person_id"), (second, "person_source_value")]:
        result = project.run_etl(source, str(tmp_path / source["name"]))
        assert result.success
        output = pl.read_csv(tmp_path / source["name"] / "person.csv")
        assert output.columns == [column]

    reopened = portiere.init("hospital", config=project.config)
    assert (
        reopened.load_schema_mapping(source=first).items[0].effective_target_column == "person_id"
    )
    assert (
        reopened.load_schema_mapping(source=second).items[0].effective_target_column
        == "person_source_value"
    )


def test_implicit_same_filename_collision_requires_source_name(tmp_path):
    import portiere
    from portiere.config import PortiereConfig

    project = portiere.init(
        "hospital", config=PortiereConfig(local_project_dir=tmp_path / "projects")
    )
    for folder in ["clinic-a", "clinic-b"]:
        (tmp_path / folder).mkdir()
        (tmp_path / folder / "patients.csv").write_text("patient_id\n1\n")
    project.add_source(str(tmp_path / "clinic-a" / "patients.csv"))

    with pytest.raises(ValueError, match=r"name"):
        project.add_source(str(tmp_path / "clinic-b" / "patients.csv"))


def test_etl_rejects_an_explicit_mapping_from_an_old_revision(tmp_path):
    import portiere
    from portiere.config import PortiereConfig

    project = portiere.init(
        "hospital", config=PortiereConfig(local_project_dir=tmp_path / "projects")
    )
    file = tmp_path / "patients.csv"
    file.write_text("patient_id\n1\n")
    source = project.add_source(str(file))
    mapping = schema(source["id"], "patient_id")
    project.save_schema_mapping(mapping)
    stale = project.load_schema_mapping(source)
    current = project.load_schema_mapping(source)
    current.items[0].reject()
    project.save_schema_mapping(current)

    with pytest.raises(ValueError, match=r"revision|changed|stale"):
        project.run_etl(source, str(tmp_path / "output"), schema_mapping=stale)
    assert not (tmp_path / "output").exists()


def test_registration_reuses_source_identity(storage):
    first = storage.register_source(
        "hospital", "clinic-a", {"name": "clinic-a", "path": "first.csv"}
    )
    repeated = storage.register_source(
        "hospital", "clinic-a", {"name": "clinic-a", "path": "first.csv"}
    )
    rebound = storage.register_source(
        "hospital", "clinic-a", {"name": "clinic-a", "path": "next.csv"}, replace_binding=True
    )
    assert first["id"] == repeated["id"] == rebound["id"]
    assert storage.list_sources("hospital")[0]["path"] == "next.csv"


def test_simultaneous_registration_never_returns_competing_ids(storage):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from portiere.storage.mapping_store import MappingConflictError

    start = threading.Barrier(2)

    def register(_):
        start.wait()
        try:
            return storage.register_source("hospital", "clinic-a", {"path": "source.csv"})["id"]
        except MappingConflictError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        returned = {value for value in pool.map(register, range(2)) if value is not None}
    assert returned == {storage.list_sources("hospital")[0]["id"]}


def test_failed_atomic_replace_keeps_previous_revision(storage, tmp_path, monkeypatch):
    from portiere.storage import mapping_store

    storage.save_schema_mapping("hospital", schema("a", "id"))
    mapping = storage.load_schema_mapping("hospital", source_id="a")
    revision = mapping.revision
    mapping.items[0].reject()

    def failed_replace(*args):
        raise OSError("simulated write failure")

    monkeypatch.setattr(mapping_store.os, "replace", failed_replace)
    with pytest.raises(OSError, match=r"write failure"):
        storage.save_schema_mapping("hospital", mapping)
    restored = storage.load_schema_mapping("hospital", source_id="a")
    assert restored.revision == revision == mapping.revision
    assert restored.items[0].status == "auto_accepted"
    assert not list((tmp_path / "hospital").rglob(".mapping-*"))
    assert not (tmp_path / "hospital" / ".mapping.write.lock").exists()


def test_registration_cannot_interleave_with_mapping_timestamp(storage, tmp_path, monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from pathlib import Path

    import yaml

    from portiere.storage import mapping_store

    reached = threading.Event()
    release = threading.Event()
    original = mapping_store.os.replace

    def pause_metadata(source, destination):
        if Path(destination).name == "project.yaml" and not reached.is_set():
            reached.set()
            assert release.wait(5)
        return original(source, destination)

    monkeypatch.setattr(mapping_store.os, "replace", pause_metadata)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(storage.save_schema_mapping, "hospital", schema("a", "id"))
        assert reached.wait(5)
        try:
            # Readers see the complete previous metadata while replacement waits.
            assert (
                yaml.safe_load((tmp_path / "hospital" / "project.yaml").read_text())["name"]
                == "hospital"
            )
            with pytest.raises(mapping_store.MappingConflictError):
                storage.register_source("hospital", "clinic-a", {"path": "source.csv"})
        finally:
            release.set()
            future.result(timeout=5)


@pytest.mark.parametrize(
    "kwargs", [{"candidate_index": -1}, {"candidate_index": 10}, {}, {"target_concept_id": 0}]
)
def test_review_override_requires_a_valid_selection(kwargs):
    mapping = ConceptMapping(
        items=[{"source_code": "a", "target_concept_id": 123, "method": "review"}]
    )
    with pytest.raises(ValueError, match=r"candidate|concept|select"):
        state.apply_concept_decision(mapping, index=0, decision="override", **kwargs)
    assert mapping.items[0].method == "review"

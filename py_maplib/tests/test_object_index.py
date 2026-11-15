"""Object indexes created with Model.create_index for triples that are already in the model."""

import polars as pl
import pytest
from maplib import IRI, IndexingOptions, Model

# Hybrid and disk storage need maplib Enterprise Edition.
STORAGES = ["memory", "hybrid", "disk"]

P = "urn:maplib:test:p"
Q = "urn:maplib:test:q"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def model(storage, tmp_path) -> Model:
    if storage == "memory":
        return Model()
    # Probe on a model of its own: a probe triple in the tested model would add a predicate
    # (and an object index) to the counts below.
    probe = Model(storage=storage, storage_folder=str(tmp_path / "probe"))
    try:
        probe.reads("<urn:a> <urn:b> <urn:c> .", format="ntriples")
    except BaseException as e:  # The open source build panics: hybrid and disk storage are EE.
        if "Contact Data Treehouse" in str(e):
            pytest.skip(f"{storage} storage needs maplib Enterprise Edition")
        raise
    return Model(storage=storage, storage_folder=str(tmp_path / "storage"))


def add(m: Model, start: int, n: int):
    lines = []
    for i in range(start, start + n):
        lines.append(f"<urn:s{i}> <{P}> <urn:o{i % 50}> .")
        lines.append(f'<urn:s{i}> <{Q}> "value {i % 50}" .')
        lines.append(f"<urn:s{i}> <{RDF_TYPE}> <urn:C> .")
    m.reads("\n".join(lines), format="ntriples")


def object_sorted_tables(m: Model, folder) -> int:
    """The number of triple tables with an object index, from the files of the serialized model."""
    m.serialize(str(folder), single_file=False)
    return len(list(folder.rglob("os_triples.parquet")))


def by_object(m: Model) -> list:
    df = m.query(f"SELECT ?s WHERE {{ ?s <{P}> <urn:o7> . ?s <{Q}> \"value 7\" }}")
    return sorted(df.get_column("s").to_list())


@pytest.mark.parametrize("storage", STORAGES)
def test_create_index_sorts_existing_triples_by_object(storage, tmp_path):
    m = model(storage, tmp_path)
    add(m, 0, 1000)
    # Several segments per table.
    add(m, 1000, 300)
    add(m, 1300, 100)
    expected = sorted(f"<urn:s{i}>" for i in range(1400) if i % 50 == 7)
    assert by_object(m) == expected
    # The default object indexes: rdf:type only, as there is no rdfs:label.
    assert object_sorted_tables(m, tmp_path / "default") == 1

    m.create_index(IndexingOptions(object_sort_all=True))
    assert object_sorted_tables(m, tmp_path / "all") == 3
    assert by_object(m) == expected

    m.create_index(IndexingOptions(object_sort_some=[IRI(P)]))
    assert object_sorted_tables(m, tmp_path / "some") == 1
    assert by_object(m) == expected

    # Triples added later are indexed by the same options.
    add(m, 1400, 100)
    expected = sorted(f"<urn:s{i}>" for i in range(1500) if i % 50 == 7)
    assert by_object(m) == expected
    assert object_sorted_tables(m, tmp_path / "some_after_add") == 1

    m.create_index(IndexingOptions())
    assert object_sorted_tables(m, tmp_path / "back_to_default") == 1
    assert by_object(m) == expected


def test_object_index_lookup_after_create_index():
    m = Model(indexing_options=IndexingOptions(object_sort_some=[IRI(Q)]))
    add(m, 0, 500)
    m.create_index(IndexingOptions(object_sort_some=[IRI(P), IRI(Q)]))
    df = m.query(f"SELECT ?s WHERE {{ ?s <{P}> <urn:o3> }}")
    assert df.height == 10
    df = m.query(f"SELECT (COUNT(*) AS ?n) WHERE {{ ?s <{P}> ?o . ?s <{Q}> ?v }}")
    assert df.get_column("n").to_list() == [500]
    assert isinstance(df, pl.DataFrame)

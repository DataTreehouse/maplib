"""Tests for the storage modes of Model: "memory", "hybrid" (cat maps on disk) and "disk"
(cat maps on disk and triple tables as parquet files on disk)."""
import gc
import pathlib

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from maplib import Model

from .disk import disk_params, storage_kwargs, STORAGE_FOLDER

TRIPLES_FOLDER = pathlib.Path(STORAGE_FOLDER) / "maplib_storage" / "triples"

TURTLE = """
@prefix ex: <http://example.net/ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:a a ex:Thing ; rdfs:label "A"@en, "A"@no ; ex:value 1 ; ex:when "2024-01-01T00:00:00Z"^^xsd:dateTime ; ex:next ex:b .
ex:b a ex:Thing ; rdfs:label "B" ; ex:value 2 ; ex:weight 2.5 ; ex:next ex:c .
ex:c a ex:Other ; rdfs:label "C" ; ex:value 3 ; ex:next _:blank .
_:blank ex:value 4 .
"""

ALL_QUERY = """
SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o
"""


def triples_df(m: Model) -> pl.DataFrame:
    # Blank node labels differ between models, so they are normalized away.
    return m.query(
        """
        SELECT ?s ?p ?o WHERE {
            ?s_ ?p ?o_ .
            BIND(IF(isBLANK(?s_), "blank", STR(?s_)) AS ?s)
            BIND(IF(isBLANK(?o_), "blank", STR(?o_)) AS ?o)
        } ORDER BY ?s ?p ?o
        """
    )


def memory_model() -> Model:
    m = Model()
    m.reads(TURTLE, format="turtle")
    return m


def parquet_files():
    if not TRIPLES_FOLDER.exists():
        return []
    return sorted(TRIPLES_FOLDER.glob("*.parquet"))


def test_storage_argument_validation():
    # Only argument validation is checked here, it works without maplib_ee.
    with pytest.raises(ValueError, match="requires a storage folder"):
        Model(storage="disk")
    with pytest.raises(ValueError, match="requires a storage folder"):
        Model(storage="hybrid")
    with pytest.raises(ValueError, match="cannot be used"):
        Model(storage="memory", storage_folder=STORAGE_FOLDER)
    with pytest.raises(ValueError, match="Unknown storage mode"):
        Model(storage="cloud")


def test_storage_memory_explicit():
    m = Model(storage="memory")
    m.reads(TURTLE, format="turtle")
    assert_frame_equal(triples_df(m), triples_df(memory_model()))


@pytest.mark.parametrize("storage", disk_params())
def test_storage_read_query(storage):
    m = Model(**storage_kwargs(storage))
    m.reads(TURTLE, format="turtle")
    assert_frame_equal(triples_df(m), triples_df(memory_model()))

    # Constant subject and object lookups go through the sparse indices.
    df = m.query(
        """
        PREFIX ex: <http://example.net/ns#>
        SELECT ?s WHERE { ?s a ex:Thing } ORDER BY ?s
        """
    )
    assert df["s"].to_list() == ["<http://example.net/ns#a>", "<http://example.net/ns#b>"]
    df = m.query(
        """
        PREFIX ex: <http://example.net/ns#>
        SELECT ?v WHERE { ex:b ex:value ?v }
        """
    )
    assert df["v"].to_list() == [2]
    df = m.query(
        """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?s WHERE { ?s rdfs:label "C" }
        """
    )
    assert df["s"].to_list() == ["<http://example.net/ns#c>"]

    if storage == "disk":
        assert len(parquet_files()) > 0
    else:
        assert len(parquet_files()) == 0


@pytest.mark.parametrize("storage", disk_params())
def test_storage_update(storage):
    m = Model(**storage_kwargs(storage))
    m.reads(TURTLE, format="turtle")
    expected = memory_model()
    for model in [m, expected]:
        model.update(
            """
            PREFIX ex: <http://example.net/ns#>
            DELETE { ?s ex:value ?v } INSERT { ?s ex:doubled ?v2 }
            WHERE { ?s ex:value ?v . BIND(?v * 2 AS ?v2) FILTER(?v > 1) }
            """
        )
        model.update(
            """
            PREFIX ex: <http://example.net/ns#>
            INSERT DATA { ex:d a ex:Thing ; ex:value 10 . }
            """
        )
    assert_frame_equal(triples_df(m), triples_df(expected))


@pytest.mark.parametrize("storage", disk_params())
def test_storage_many_segments_and_compact(storage):
    # Many small inserts into the same triple table create segments, and are compacted
    # automatically when there are more than 10 segments.
    m = Model(**storage_kwargs(storage))
    expected = Model()
    n_batches = 25
    for i in range(n_batches):
        df = pl.DataFrame(
            {
                "subject": [f"urn:maplib:s{i}_{j}" for j in range(200)],
                "object": [f"urn:maplib:o{(i * 7 + j) % 300}" for j in range(200)],
            }
        )
        m.map_triples(df, predicate="urn:maplib:p")
        expected.map_triples(df, predicate="urn:maplib:p")
    assert_frame_equal(triples_df(m), triples_df(expected))
    count = m.query("SELECT (COUNT(*) AS ?c) WHERE { ?s <urn:maplib:p> ?o }")["c"][0]
    assert count == n_batches * 200

    files_before = len(parquet_files())
    m.compact()
    expected.compact()
    assert_frame_equal(triples_df(m), triples_df(expected))
    if storage == "disk":
        # Compaction replaces the segment files with a single file.
        assert 0 < len(parquet_files()) <= files_before

    df = m.query("SELECT ?o WHERE { <urn:maplib:s3_5> <urn:maplib:p> ?o }")
    assert df["o"].to_list() == [f"<urn:maplib:o{(3 * 7 + 5) % 300}>"]


@pytest.mark.parametrize("storage", disk_params())
def test_storage_named_graphs(storage):
    m = Model(**storage_kwargs(storage))
    m.reads(TURTLE, format="turtle", graph="urn:maplib:g")
    m.reads(TURTLE, format="turtle")
    assert_frame_equal(triples_df(m), triples_df(memory_model()))
    df = m.query(ALL_QUERY, graph="urn:maplib:g")
    assert df.height == triples_df(memory_model()).height


@pytest.mark.parametrize("storage", disk_params())
def test_storage_deserialize(storage, tmp_path):
    original = memory_model()
    serialized = str(tmp_path / "serialized")
    original.serialize(serialized)
    m = Model.deserialize(serialized, **storage_kwargs(storage))
    assert_frame_equal(triples_df(m), triples_df(original))
    if storage == "disk":
        assert len(parquet_files()) > 0

    # The deserialized triples can be added to.
    m.reads(
        """
        @prefix ex: <http://example.net/ns#> .
        ex:e a ex:Thing .
        """,
        format="turtle",
    )
    df = m.query(
        """
        PREFIX ex: <http://example.net/ns#>
        SELECT ?s WHERE { ?s a ex:Thing } ORDER BY ?s
        """
    )
    assert df.height == 3


@pytest.mark.parametrize("storage", [s for s in disk_params() if s == "disk"])
def test_storage_disk_files_removed_with_model(storage):
    m = Model(**storage_kwargs(storage))
    m.reads(TURTLE, format="turtle")
    assert len(parquet_files()) > 0
    del m
    gc.collect()
    assert len(parquet_files()) == 0


GRAPH_MANAGEMENT_UPDATES = [
    "CLEAR GRAPH <urn:maplib:g>",
    "DROP GRAPH <urn:maplib:g>",
    "CLEAR DEFAULT",
    "DROP NAMED",
    "CLEAR ALL",
    "COPY <urn:maplib:g> TO <urn:maplib:h>",
    "MOVE <urn:maplib:g> TO DEFAULT",
    "ADD DEFAULT TO <urn:maplib:g>",
]


def graph_df(m: Model, graph) -> pl.DataFrame:
    return m.query(
        """
        SELECT ?s ?p ?o WHERE {
            ?s_ ?p ?o_ .
            BIND(IF(isBLANK(?s_), "blank", STR(?s_)) AS ?s)
            BIND(IF(isBLANK(?o_), "blank", STR(?o_)) AS ?o)
        } ORDER BY ?s ?p ?o
        """,
        graph=graph,
    )


def three_graphs(m: Model):
    m.reads(TURTLE, format="turtle")
    m.reads(TURTLE, format="turtle", graph="urn:maplib:g")
    m.update(
        """
        PREFIX ex: <http://example.net/ns#>
        INSERT DATA { GRAPH <urn:maplib:h> { ex:h ex:value 7 . ex:a ex:value 1 . } }
        """
    )


@pytest.mark.parametrize("update", GRAPH_MANAGEMENT_UPDATES)
@pytest.mark.parametrize("storage", disk_params())
def test_storage_graph_management(storage, update):
    # CLEAR, DROP, COPY, MOVE and ADD give the same graphs and report the same changes in every
    # storage mode, and the graphs can be written to and queried afterwards.
    m = Model(**storage_kwargs(storage))
    expected = Model()
    for model in [m, expected]:
        three_graphs(model)
    changes = m.update(update)
    assert changes == expected.update(update)
    for model in [m, expected]:
        model.update(
            """
            PREFIX ex: <http://example.net/ns#>
            INSERT DATA { ex:new ex:value 11 . GRAPH <urn:maplib:g> { ex:new ex:value 12 } }
            """
        )
    for graph in [None, "urn:maplib:g", "urn:maplib:h"]:
        assert_frame_equal(graph_df(m, graph), graph_df(expected, graph))


@pytest.mark.parametrize("storage", [s for s in disk_params() if s == "disk"])
def test_storage_drop_removes_table_files(storage):
    # Dropping a graph in disk storage removes the parquet files of its tables, and leaves those
    # of the other graphs.
    m = Model(**storage_kwargs(storage))
    m.reads(TURTLE, format="turtle")
    files_default = set(parquet_files())
    m.reads(TURTLE, format="turtle", graph="urn:maplib:g")
    assert set(parquet_files()) > files_default
    m.update("DROP GRAPH <urn:maplib:g>")
    gc.collect()
    assert set(parquet_files()) == files_default
    assert_frame_equal(triples_df(m), triples_df(memory_model()))
    m.update("CLEAR DEFAULT")
    gc.collect()
    assert parquet_files() == []
    assert triples_df(m).height == 0


@pytest.mark.parametrize("storage", disk_params())
def test_storage_clear_deserialized(storage, tmp_path):
    # Clearing a deserialized model does not touch the files it was deserialized from.
    serialized = str(tmp_path / "serialized")
    memory_model().serialize(serialized, single_file=False)
    files = sorted(p for p in pathlib.Path(serialized).rglob("*") if p.is_file())
    assert files
    m = Model.deserialize(serialized, **storage_kwargs(storage))
    m.update("CLEAR ALL")
    gc.collect()
    assert triples_df(m).height == 0
    assert sorted(p for p in pathlib.Path(serialized).rglob("*") if p.is_file()) == files
    del m
    gc.collect()
    again = Model.deserialize(serialized, **storage_kwargs(storage))
    assert_frame_equal(triples_df(again), triples_df(memory_model()))


@pytest.mark.parametrize("storage", disk_params())
def test_storage_truncate_graph(storage):
    m = Model(**storage_kwargs(storage))
    m.reads(TURTLE, format="turtle")
    m.reads(TURTLE, format="turtle", graph="urn:maplib:g")
    m.truncate_graph("urn:maplib:g")
    assert graph_df(m, "urn:maplib:g").height == 0
    assert_frame_equal(triples_df(m), triples_df(memory_model()))
    m.truncate_graph()
    assert triples_df(m).height == 0
    m.reads(TURTLE, format="turtle")
    assert_frame_equal(triples_df(m), triples_df(memory_model()))

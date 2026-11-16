"""Model.update reports what it actually changed (w3c-cg/sparql-dev#63): the number of triples
really inserted and deleted per graph, and optionally the triples themselves."""
import shutil

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from maplib import Model, DEFAULT_GRAPH

from .disk import disk_params, storage_kwargs, STORAGE_FOLDER

EX = "http://example.net/ns#"
G1 = "http://example.net/g1"

DATA = f"""
<{EX}a> <{EX}knows> <{EX}b> .
<{EX}a> <{EX}knows> <{EX}c> .
<{EX}b> <{EX}knows> <{EX}c> .
<{EX}a> <{EX}age> 30 .
<{EX}b> <{EX}age> 40 .
"""


@pytest.fixture(params=disk_params())
def model(request):
    storage = request.param
    if storage != "memory":
        shutil.rmtree(STORAGE_FOLDER, ignore_errors=True)
    m = Model(**storage_kwargs(storage))
    m.reads(DATA, format="turtle")
    yield m
    del m
    if storage != "memory":
        shutil.rmtree(STORAGE_FOLDER, ignore_errors=True)


def counts(changes):
    return {g: (c["inserted"], c["deleted"]) for g, c in changes.items()}


def count_triples(m: Model, graph=None) -> int:
    return m.query("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }", graph=graph)["n"][0]


def test_insert_data_counts_only_new_triples(model):
    changes = model.update(
        f"""INSERT DATA {{
            <{EX}a> <{EX}knows> <{EX}b> .
            <{EX}a> <{EX}knows> <{EX}d> .
            <{EX}d> <{EX}age> 50 .
            <{EX}d> <{EX}age> 50 .
        }}"""
    )
    assert counts(changes) == {DEFAULT_GRAPH: (2, 0)}
    assert "inserted_triples" not in changes[DEFAULT_GRAPH]
    assert count_triples(model) == 7


def test_insert_data_only_duplicates(model):
    changes = model.update(f"INSERT DATA {{ <{EX}a> <{EX}knows> <{EX}b> }}")
    assert counts(changes) == {DEFAULT_GRAPH: (0, 0)}


def test_delete_data_counts_only_existing_triples(model):
    changes = model.update(
        f"""DELETE DATA {{
            <{EX}a> <{EX}knows> <{EX}b> .
            <{EX}a> <{EX}knows> <{EX}nobody> .
            <{EX}zzz> <{EX}age> 30 .
        }}"""
    )
    assert counts(changes) == {DEFAULT_GRAPH: (0, 1)}
    assert count_triples(model) == 4


def test_delete_absent_leaves_graph_unchanged(model):
    before = model.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o")
    changes = model.update(f"DELETE DATA {{ <{EX}a> <{EX}knows> <{EX}nobody> }}")
    assert counts(changes) == {DEFAULT_GRAPH: (0, 0)}
    after = model.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o")
    assert_frame_equal(before, after)


def test_delete_insert_where(model):
    changes = model.update(
        f"""DELETE {{ ?s <{EX}age> ?a }} INSERT {{ ?s <{EX}age> ?b }}
        WHERE {{ ?s <{EX}age> ?a BIND(?a + 1 AS ?b) }}"""
    )
    assert counts(changes) == {DEFAULT_GRAPH: (2, 2)}


def test_delete_insert_where_return_triples(model):
    changes = model.update(
        f"""DELETE {{ ?s <{EX}knows> ?o }} INSERT {{ ?o <{EX}knownBy> ?s }}
        WHERE {{ ?s <{EX}knows> ?o FILTER(?o = <{EX}c>) }}""",
        return_triples=True,
    )
    c = changes[DEFAULT_GRAPH]
    assert (c["inserted"], c["deleted"]) == (2, 2)
    assert list(c["inserted_triples"].keys()) == [f"{EX}knownBy"]
    assert list(c["deleted_triples"].keys()) == [f"{EX}knows"]
    deleted = c["deleted_triples"][f"{EX}knows"].sort("subject")
    assert deleted.columns == ["subject", "object"]
    assert deleted["subject"].to_list() == [f"<{EX}a>", f"<{EX}b>"]
    assert deleted["object"].to_list() == [f"<{EX}c>", f"<{EX}c>"]
    inserted = c["inserted_triples"][f"{EX}knownBy"].sort("object")
    assert inserted["object"].to_list() == [f"<{EX}a>", f"<{EX}b>"]


def test_delete_where_fast_path(model):
    changes = model.update(f"DELETE WHERE {{ ?s <{EX}knows> ?o }}", return_triples=True)
    c = changes[DEFAULT_GRAPH]
    assert (c["inserted"], c["deleted"]) == (0, 3)
    assert c["deleted_triples"][f"{EX}knows"].height == 3
    changes = model.update("DELETE WHERE { ?s ?p ?o }")
    assert counts(changes) == {DEFAULT_GRAPH: (0, 2)}
    assert count_triples(model) == 0


def test_named_graphs(model):
    changes = model.update(
        f"""INSERT DATA {{
            GRAPH <{G1}> {{ <{EX}x> <{EX}knows> <{EX}y> . <{EX}x> <{EX}age> 1 }}
            <{EX}a> <{EX}knows> <{EX}b> .
        }}"""
    )
    assert counts(changes) == {DEFAULT_GRAPH: (0, 0), G1: (2, 0)}
    changes = model.update(
        f"""DELETE {{ GRAPH <{G1}> {{ ?s ?p ?o }} }}
        INSERT {{ ?s ?p ?o }}
        WHERE {{ GRAPH <{G1}> {{ ?s ?p ?o }} }}"""
    )
    assert counts(changes) == {DEFAULT_GRAPH: (2, 0), G1: (0, 2)}


def test_mixed_types_per_predicate(model):
    changes = model.update(
        f"""INSERT DATA {{
            <{EX}a> <{EX}val> 1 .
            <{EX}a> <{EX}val> "one" .
            <{EX}a> <{EX}val> <{EX}one> .
        }}""",
        return_triples=True,
    )
    c = changes[DEFAULT_GRAPH]
    assert c["inserted"] == 3
    # One DataFrame per predicate, even with several object types.
    assert c["inserted_triples"][f"{EX}val"].height == 3


def test_operations_are_summed(model):
    changes = model.update(
        f"""INSERT DATA {{ <{EX}a> <{EX}knows> <{EX}z> }} ;
        DELETE DATA {{ <{EX}a> <{EX}knows> <{EX}z> }}"""
    )
    assert counts(changes) == {DEFAULT_GRAPH: (1, 1)}


def test_solution_mappings(model):
    changes = model.update(
        f"INSERT DATA {{ <{EX}q> <{EX}age> 9 }}",
        return_triples=True,
        solution_mappings=True,
    )
    sm = changes[DEFAULT_GRAPH]["inserted_triples"][f"{EX}age"]
    assert sm.mappings.height == 1
    assert set(sm.rdf_types.keys()) == {"subject", "object"}


def test_clear_and_drop():
    # Clearing graphs is not supported with a storage folder, so only in memory.
    m = Model()
    m.reads(DATA, format="turtle")
    m.update(f"INSERT DATA {{ GRAPH <{G1}> {{ <{EX}x> <{EX}knows> <{EX}y> }} }}")
    changes = m.update(f"CLEAR GRAPH <{G1}>", return_triples=True)
    c = changes[G1]
    assert (c["inserted"], c["deleted"]) == (0, 1)
    assert c["deleted_triples"][f"{EX}knows"].height == 1
    changes = m.update("DROP DEFAULT")
    assert counts(changes) == {DEFAULT_GRAPH: (0, 5)}



def test_insert_template_with_several_graphs(model):
    # One INSERT template writing to the default graph, a named graph and a graph from a variable:
    # the inserted triples are counted in the graph they went to.
    g2 = "http://example.net/g2"
    changes = model.update(
        f"""INSERT {{
            ?s <{EX}copy> ?o .
            GRAPH <{G1}> {{ ?s <{EX}knows> ?o }}
            GRAPH ?g {{ ?s <{EX}inG> ?o }}
        }}
        WHERE {{ ?s <{EX}knows> ?o BIND(IRI(IF(?o = <{EX}c>, "{G1}", "{g2}")) AS ?g) }}""",
        return_triples=True,
    )
    assert counts(changes) == {DEFAULT_GRAPH: (3, 0), G1: (5, 0), g2: (1, 0)}
    assert changes[G1]["inserted_triples"][f"{EX}inG"].height == 2
    assert changes[g2]["inserted_triples"][f"{EX}inG"].height == 1
    # Running it again inserts nothing new.
    changes = model.update(
        f"""INSERT {{
            ?s <{EX}copy> ?o .
            GRAPH <{G1}> {{ ?s <{EX}knows> ?o }}
            GRAPH ?g {{ ?s <{EX}inG> ?o }}
        }}
        WHERE {{ ?s <{EX}knows> ?o BIND(IRI(IF(?o = <{EX}c>, "{G1}", "{g2}")) AS ?g) }}"""
    )
    assert {g: c for g, c in counts(changes).items() if c != (0, 0)} == {}


def test_load(model, tmp_path):
    # LOAD reports the triples it really inserted, in the graph they went to.
    nt = tmp_path / "load.nt"
    nt.write_text(f"<{EX}a> <{EX}knows> <{EX}b> .\n<{EX}l> <{EX}knows> <{EX}m> .\n")
    trig = tmp_path / "load.trig"
    trig.write_text(f"<{EX}t> <{EX}knows> <{EX}u> .\n<{G1}> {{ <{EX}t> <{EX}knows> <{EX}v> }}\n")
    urls = [nt.as_uri(), trig.as_uri()]
    model.set_federation(allow_load=True, allowed_endpoints=urls)
    changes = model.update(f"LOAD <{urls[0]}> INTO GRAPH <{G1}>", return_triples=True)
    assert counts(changes) == {G1: (2, 0)}
    assert changes[G1]["inserted_triples"][f"{EX}knows"].height == 2
    changes = model.update(f"LOAD <{urls[0]}>")
    # One of the two triples is already in the default graph.
    assert counts(changes) == {DEFAULT_GRAPH: (1, 0)}
    changes = model.update(f"LOAD <{urls[0]}> INTO GRAPH <{G1}>")
    assert counts(changes) == {G1: (0, 0)}
    # Quads of a dataset loaded into the default graph go to their own graphs.
    changes = model.update(f"LOAD <{urls[1]}>")
    assert counts(changes) == {DEFAULT_GRAPH: (1, 0), G1: (1, 0)}


def test_add_copy_move():
    # COPY and MOVE clear the target first, which is only supported in memory.
    g2, g3 = "http://example.net/g2", "http://example.net/g3"
    m = Model()
    m.reads(DATA, format="turtle")
    m.update(f"INSERT DATA {{ GRAPH <{G1}> {{ <{EX}x> <{EX}knows> <{EX}y> . <{EX}a> <{EX}knows> <{EX}b> }} }}")
    # ADD inserts what is not in the target yet.
    assert counts(m.update(f"ADD <{G1}> TO DEFAULT")) == {DEFAULT_GRAPH: (1, 0)}
    assert counts(m.update(f"ADD <{G1}> TO <{g2}>")) == {g2: (2, 0)}
    # COPY replaces the target: its triples that are not in the source are deleted, and those
    # that are are deleted and inserted again.
    m.update(f"INSERT DATA {{ GRAPH <{g3}> {{ <{EX}z> <{EX}knows> <{EX}z> }} }}")
    c = counts(m.update(f"COPY <{G1}> TO <{g3}>"))
    assert c[g3] == (2, 1)
    assert c.get(G1, (0, 0)) == (0, 0)
    # MOVE also empties the source.
    c = counts(m.update(f"MOVE <{g2}> TO <{g3}>"))
    assert c[g3] == (2, 2)
    assert c[g2] == (0, 2)
    assert count_triples(m, graph=g2) == 0
    assert count_triples(m, graph=g3) == 2
    # Copying a graph to itself changes nothing.
    assert {k: v for k, v in counts(m.update(f"COPY <{g3}> TO <{g3}>")).items() if v != (0, 0)} == {}

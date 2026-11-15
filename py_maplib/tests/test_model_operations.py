"""Coverage for Model-level operations that were not exercised elsewhere:
add_graph, add_triples, create_index / IndexingOptions, compact, size, get_predicate(_iris),
serialize/deserialize variants and query return formats."""

import json

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from maplib import (
    IRI,
    IndexingOptions,
    Literal,
    Model,
    RDFType,
    SolutionMappings,
    add_triples,
    xsd,
)

EX = "http://example.net/ns#"
G1 = "urn:graph:one"
G2 = "urn:graph:two"

DATA = f"""
<{EX}a> <{EX}p> <{EX}b> .
<{EX}b> <{EX}p> <{EX}c> .
<{EX}a> <{EX}name> "Alice" .
<{EX}b> <{EX}name> "Bob"@en .
<{EX}a> <{EX}age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
"""

SPO = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o"


def _model(graph=None):
    m = Model()
    m.reads(DATA, format="ntriples", graph=graph)
    return m


def _spo(m, graph=None):
    return m.query(SPO, graph=graph)


# --- size / predicates ---------------------------------------------------------------


def test_size_default_and_named_graphs():
    m = _model()
    m.reads(f"<{EX}x> <{EX}q> <{EX}y> .", format="ntriples", graph=G1)
    assert m.size() == 5
    assert m.size(G1) == 1
    assert m.size(G2) == 0


def test_size_counts_duplicates_once():
    m = _model()
    m.reads(DATA, format="ntriples")
    assert m.size() == 5


def test_get_predicate_iris_and_get_predicate():
    m = _model()
    preds = sorted(p.iri for p in m.get_predicate_iris())
    assert preds == sorted([EX + "p", EX + "name", EX + "age"])

    sms = m.get_predicate(IRI(EX + "name"))
    assert len(sms) >= 1
    assert all(isinstance(sm, SolutionMappings) for sm in sms)
    total = sum(sm.mappings.height for sm in sms)
    assert total == 2
    for sm in sms:
        assert set(sm.mappings.columns) >= {"subject", "object"}
        assert "subject" in sm.rdf_types


def test_get_predicate_iris_named_graph():
    m = _model()
    m.reads(f"<{EX}x> <{EX}q> <{EX}y> .", format="ntriples", graph=G1)
    assert [p.iri for p in m.get_predicate_iris(G1)] == [EX + "q"]


# --- add_graph / add_triples ---------------------------------------------------------


def test_add_graph_default_to_default():
    src = _model()
    dst = Model()
    dst.add_graph(src)
    assert_frame_equal(_spo(src), _spo(dst))
    # Source is unchanged
    assert src.size() == 5


def test_add_graph_between_named_graphs():
    src = _model(graph=G1)
    dst = Model()
    dst.reads(f"<{EX}x> <{EX}q> <{EX}y> .", format="ntriples", graph=G2)
    dst.add_graph(src, source_graph=G1, target_graph=G2)
    assert dst.size(G2) == 6
    assert dst.size() == 0
    assert_frame_equal(
        _spo(src, graph=G1),
        dst.query(
            f"SELECT ?s ?p ?o WHERE {{ ?s ?p ?o FILTER(?p != <{EX}q>) }} ORDER BY ?s ?p ?o",
            graph=G2,
        ),
    )


def test_add_graph_named_to_default():
    src = _model(graph=G1)
    dst = Model()
    dst.add_graph(src, source_graph=G1)
    assert dst.size() == 5
    assert_frame_equal(_spo(src, graph=G1), _spo(dst))


def test_add_graph_is_idempotent_for_duplicates():
    src = _model()
    dst = _model()
    dst.add_graph(src)
    assert dst.size() == 5


def test_add_triples_helper():
    src = _model(graph=G1)
    dst = Model()
    add_triples(src, dst, source_graph=G1, target_graph=G2)
    assert dst.size(G2) == 5
    assert_frame_equal(_spo(src, graph=G1), _spo(dst, graph=G2))


# --- indexing / compaction -----------------------------------------------------------


@pytest.mark.parametrize(
    "options",
    [
        None,
        IndexingOptions(),
        IndexingOptions(object_sort_all=True),
        IndexingOptions(object_sort_some=[IRI(EX + "name")]),
    ],
)
def test_create_index_preserves_results(options):
    m = _model()
    before = _spo(m)
    m.create_index(options)
    assert_frame_equal(before, _spo(m))
    df = m.query(f'SELECT ?s WHERE {{ ?s <{EX}name> "Alice" }}')
    assert df["s"].to_list() == [f"<{EX}a>"]


def test_model_with_indexing_options_object_sort_all():
    m = Model(indexing_options=IndexingOptions(object_sort_all=True))
    m.reads(DATA, format="ntriples")
    df = m.query(f"SELECT ?s WHERE {{ ?s <{EX}p> <{EX}c> }}")
    assert df["s"].to_list() == [f"<{EX}b>"]


def test_compact_after_many_additions():
    m = Model()
    for i in range(20):
        m.reads(f'<{EX}s{i}> <{EX}v> "{i}"^^<{xsd.integer.iri}> .', format="ntriples")
    before = m.query(f"SELECT ?s ?v WHERE {{ ?s <{EX}v> ?v }} ORDER BY ?v")
    m.compact()
    after = m.query(f"SELECT ?s ?v WHERE {{ ?s <{EX}v> ?v }} ORDER BY ?v")
    assert_frame_equal(before, after)
    assert m.size() == 20
    total = sum(sm.mappings.height for sm in m.get_predicate(IRI(EX + "v")))
    assert total == 20


# --- serialization -------------------------------------------------------------------


@pytest.mark.parametrize("single_file", [True, False])
def test_serialize_deserialize_round_trip(tmp_path, single_file):
    m = _model()
    m.reads(f"<{EX}x> <{EX}q> <{EX}y> .", format="ntriples", graph=G1)
    path = tmp_path / "model.maplib"
    m.serialize(path, single_file=single_file)
    if single_file:
        assert path.is_file()
    else:
        assert path.is_dir()
    m2 = Model.deserialize(path)
    assert_frame_equal(_spo(m), _spo(m2))
    assert_frame_equal(_spo(m, graph=G1), _spo(m2, graph=G1))
    assert m2.size() == 5
    assert m2.size(G1) == 1


def test_deserialized_model_can_be_extended(tmp_path):
    m = _model()
    path = tmp_path / "m.maplib"
    m.serialize(str(path))
    m2 = Model.deserialize(str(path))
    m2.reads(f'<{EX}c> <{EX}name> "Carol" .', format="ntriples")
    assert m2.size() == 6
    df = m2.query(
        f"SELECT (STR(?n) AS ?name) WHERE {{ ?s <{EX}name> ?n }} ORDER BY ?name"
    )
    assert df["name"].to_list() == ["Alice", "Bob", "Carol"]


# --- query return formats ------------------------------------------------------------


def test_query_return_format_json_select():
    m = _model()
    s = m.query(
        f"SELECT ?s ?n WHERE {{ ?s <{EX}name> ?n }} ORDER BY ?s", return_format="json"
    )
    res = json.loads(s)
    assert sorted(res["head"]["vars"]) == ["n", "s"]
    bindings = res["results"]["bindings"]
    assert len(bindings) == 2
    assert bindings[0]["s"] == {"type": "uri", "value": EX + "a"}
    assert bindings[0]["n"]["value"] == "Alice"
    assert bindings[1]["n"]["value"] == "Bob"
    assert bindings[1]["n"].get("xml:lang") == "en"


def test_query_return_format_json_vars_in_projection_order():
    m = _model()
    for _ in range(20):
        s = m.query(
            f"SELECT ?s ?n ?x WHERE {{ ?s <{EX}name> ?n BIND(1 AS ?x) }}",
            return_format="json",
        )
        assert json.loads(s)["head"]["vars"] == ["s", "n", "x"]


def test_query_return_json_flag_matches_format():
    m = _model()
    q = f"SELECT ?s WHERE {{ ?s <{EX}p> ?o }} ORDER BY ?s"
    assert json.loads(m.query(q, return_json=True)) == json.loads(
        m.query(q, return_format="json")
    )


def test_query_return_format_json_ask():
    m = _model()
    res = json.loads(
        m.query(f"ASK {{ <{EX}a> <{EX}p> <{EX}b> }}", return_format="json")
    )
    assert res["boolean"] is True
    res = json.loads(
        m.query(f"ASK {{ <{EX}a> <{EX}p> <{EX}c> }}", return_format="json")
    )
    assert res["boolean"] is False


def test_query_return_format_turtle_construct():
    m = _model()
    ttl = m.query(
        f"CONSTRUCT {{ ?s <{EX}knows> ?o }} WHERE {{ ?s <{EX}p> ?o }}",
        return_format="turtle",
    )
    assert isinstance(ttl, str)
    m2 = Model()
    m2.reads(ttl, format="turtle")
    assert m2.size() == 2
    df = m2.query(f"SELECT ?s ?o WHERE {{ ?s <{EX}knows> ?o }} ORDER BY ?s")
    assert df["o"].to_list() == [f"<{EX}b>", f"<{EX}c>"]


def test_query_construct_returns_list_of_dataframes():
    m = _model()
    res = m.query(
        f"CONSTRUCT {{ ?s <{EX}knows> ?o . ?s a <{EX}Person> }} WHERE {{ ?s <{EX}p> ?o }}"
    )
    assert isinstance(res, list)
    assert len(res) == 2
    assert sorted(df.height for df in res) == [2, 2]


def test_query_solution_mappings_types_and_round_trip():
    m = _model()
    sm = m.query(
        f"SELECT ?s ?n WHERE {{ ?s <{EX}name> ?n }}", solution_mappings=True
    )
    assert isinstance(sm, SolutionMappings)
    assert set(sm.rdf_types) == {"s", "n"}
    assert sm.mappings.height == 2

    # Map the solution mappings back in using map_triples
    sm2 = m.query(
        f"SELECT ?subject ?object WHERE {{ ?subject <{EX}name> ?object }}",
        solution_mappings=True,
    )
    m2 = Model()
    m2.map_triples(sm2, predicate=EX + "label")
    df = m2.query(
        f"SELECT ?s ?o WHERE {{ ?s <{EX}label> ?o }} ORDER BY ?s"
    )
    assert df.height == 2
    # The language tag of "Bob"@en must survive the round trip
    assert m2.query(
        f'ASK {{ <{EX}b> <{EX}label> "Bob"@en }}'
    ) is True


def test_solution_mappings_constructor():
    df = pl.DataFrame({"s": [EX + "a"], "o": ["x"]})
    sm = SolutionMappings(df, {"s": RDFType.IRI, "o": RDFType.Literal(xsd.string)})
    assert sm.mappings.height == 1
    assert set(sm.rdf_types) == {"s", "o"}


@pytest.mark.parametrize(
    "value",
    [IRI(EX + "a"), f"<{EX}a>"],
)
def test_query_bindings_iri(value):
    m = _model()
    df = m.query(f"SELECT ?o WHERE {{ ?s <{EX}p> ?o }}", bindings={"s": value})
    assert df["o"].to_list() == [f"<{EX}b>"]


def test_query_bindings_literal():
    m = _model()
    df = m.query(
        f"SELECT ?s WHERE {{ ?s <{EX}age> ?age }}",
        bindings={"age": Literal("30", data_type=xsd.integer)},
    )
    assert df["s"].to_list() == [f"<{EX}a>"]
    df = m.query(
        f"SELECT ?s WHERE {{ ?s <{EX}age> ?age }}",
        bindings={"age": Literal("31", data_type=xsd.integer)},
    )
    assert df.height == 0


def test_query_named_graph_isolated_from_default():
    m = _model()
    m.reads(f"<{EX}x> <{EX}p> <{EX}y> .", format="ntriples", graph=G1)
    assert m.query(f"SELECT ?s WHERE {{ ?s <{EX}p> ?o }}").height == 2
    assert m.query(f"SELECT ?s WHERE {{ ?s <{EX}p> ?o }}", graph=G1).height == 1


def test_query_graph_keyword_over_named_graphs():
    m = _model()
    m.reads(f"<{EX}x> <{EX}p> <{EX}y> .", format="ntriples", graph=G1)
    df = m.query(f"SELECT ?s WHERE {{ GRAPH <{G1}> {{ ?s <{EX}p> ?o }} }}")
    assert df["s"].to_list() == [f"<{EX}x>"]


# --- updates -------------------------------------------------------------------------


def test_update_insert_data_and_delete_data():
    m = _model()
    m.update(f'INSERT DATA {{ <{EX}c> <{EX}name> "Carol" }}')
    assert m.size() == 6
    m.update(f'DELETE DATA {{ <{EX}a> <{EX}name> "Alice" }}')
    assert m.size() == 5
    df = m.query(
        f"SELECT (STR(?n) AS ?name) WHERE {{ ?s <{EX}name> ?n }} ORDER BY ?name"
    )
    assert df["name"].to_list() == ["Bob", "Carol"]


def test_update_delete_insert_where():
    m = _model()
    m.update(
        f"""
        DELETE {{ ?s <{EX}p> ?o }}
        INSERT {{ ?o <{EX}inverseP> ?s }}
        WHERE {{ ?s <{EX}p> ?o }}
        """
    )
    assert m.query(f"SELECT ?s WHERE {{ ?s <{EX}p> ?o }}").height == 0
    df = m.query(f"SELECT ?s ?o WHERE {{ ?s <{EX}inverseP> ?o }} ORDER BY ?s")
    assert df["s"].to_list() == [f"<{EX}b>", f"<{EX}c>"]
    assert df["o"].to_list() == [f"<{EX}a>", f"<{EX}b>"]


def test_update_into_named_graph():
    m = _model()
    m.update(
        f"INSERT {{ GRAPH <{G1}> {{ ?s <{EX}copy> ?o }} }} WHERE {{ ?s <{EX}p> ?o }}"
    )
    assert m.size(G1) == 2
    assert m.query(f"SELECT ?s WHERE {{ ?s <{EX}copy> ?o }}").height == 0


def test_insert_returns_inserted_triples_and_target_graph():
    m = _model()
    res = m.insert(
        f"CONSTRUCT {{ ?s a <{EX}Node> }} WHERE {{ ?s <{EX}p> ?o }}",
        target_graph=G1,
    )
    assert isinstance(res, dict)
    assert len(res) == 1
    (df,) = res.values()
    assert df.height == 2
    assert m.size(G1) == 2
    assert m.size() == 5


def test_insert_solution_mappings():
    m = _model()
    res = m.insert(
        f"CONSTRUCT {{ ?s a <{EX}Node> }} WHERE {{ ?s <{EX}p> ?o }}",
        solution_mappings=True,
    )
    (sm,) = res.values()
    assert isinstance(sm, SolutionMappings)
    assert sm.mappings.height == 2



def test_insert_returns_all_types_of_a_predicate():
    # The objects of <tag> are both IRIs and a literal, which are stored in two tables.
    m = _model()
    res = m.insert(
        f"""CONSTRUCT {{ ?s <{EX}tag> ?o . ?s <{EX}tag> "t" }}
        WHERE {{ ?s <{EX}p> ?o }}"""
    )
    assert list(res.keys()) == [f"{EX}tag"]
    df = res[f"{EX}tag"]
    assert df.height == 4
    assert m.query(f"SELECT ?s ?o WHERE {{ ?s <{EX}tag> ?o }}").height == 4
    res = m.insert(
        f"""CONSTRUCT {{ ?s <{EX}tag> ?o . ?s <{EX}tag> "t" }}
        WHERE {{ ?s <{EX}p> ?o }}""",
        solution_mappings=True,
    )
    assert all(sm.mappings.height == 0 for sm in res.values())

def test_insert_from_source_graph():
    m = _model(graph=G1)
    m.insert(
        f"CONSTRUCT {{ ?s a <{EX}Node> }} WHERE {{ ?s <{EX}p> ?o }}",
        source_graph=G1,
        target_graph=G2,
    )
    assert m.size(G2) == 2
    assert m.size() == 0

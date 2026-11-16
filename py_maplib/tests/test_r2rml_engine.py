"""Model.map_r2rml beyond the W3C test cases: the fast paths of the column-wise engine (IRI
templates checked once instead of per value, relative templates resolved with the base IRI,
values that need no percent encoding), joins on columns of different numeric types, time zone
offsets, blank nodes and graph columns, from DuckDB and from polars DataFrames."""

import polars as pl
import pytest

from maplib import Model

duckdb = pytest.importorskip("duckdb")

PREFIXES = """@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
"""


def _database():
    con = duckdb.connect()
    con.execute(
        """CREATE TABLE person AS SELECT * FROM (VALUES
            (1, 'Ann B', 'Bjørn', 10::BIGINT, '2020-01-01T10:00:00Z', 'g1'),
            (2, 'Cy', 'a/b?c#d', 20::BIGINT, '2020-01-01T10:00:00+02:00', 'g2'),
            (3, NULL, 'x', NULL, '2020-01-01T10:00:00', NULL),
            (-4, 'Dee', '~._-', 10::BIGINT, 'not a date', 'g1')
          ) t(id, first, last, dept, born, g)"""
    )
    con.execute(
        """CREATE TABLE dept AS SELECT * FROM (VALUES (10::INTEGER, 'Ten'), (20::INTEGER, 'Twenty'))
          t(id, name)"""
    )
    return con


def _sources():
    con = _database()
    tables = {t: con.sql(f"SELECT * FROM {t}").pl() for t in ("person", "dept")}
    return [{"database": con}, {"tables": tables}]


def _map(mapping: str, **kwargs) -> Model:
    m = Model()
    m.map_r2rml(PREFIXES + mapping, base_iri="http://example.org/base/", **kwargs)
    return m


def _strings(m: Model, query: str, var: str) -> list:
    return sorted(m.query("PREFIX ex: <http://example.org/>\n" + query)[var].to_list())


@pytest.mark.parametrize("source", _sources(), ids=["duckdb", "polars"])
def test_iri_templates_and_percent_encoding(source):
    m = _map(
        """<#P> rr:logicalTable [ rr:tableName "person" ] ;
          rr:subjectMap [ rr:template "http://example.org/person/{last}/{id}" ] ;
          rr:predicateObjectMap [ rr:predicate ex:rel ; rr:objectMap [ rr:template "rel/{first}" ] ] .""",
        **source,
    )
    assert _strings(m, "SELECT ?s WHERE { ?s ex:rel ?o }", "s") == [
        "<http://example.org/person/Bj%C3%B8rn/1>",
        "<http://example.org/person/a%2Fb%3Fc%23d/2>",
        "<http://example.org/person/~._-/-4>",
    ]
    # A relative template is resolved against the base IRI.
    assert _strings(m, "SELECT ?o WHERE { ?s ex:rel ?o }", "o") == [
        "<http://example.org/base/rel/Ann%20B>",
        "<http://example.org/base/rel/Cy>",
        "<http://example.org/base/rel/Dee>",
    ]


@pytest.mark.parametrize("source", _sources(), ids=["duckdb", "polars"])
def test_join_on_different_integer_types(source):
    m = _map(
        """<#D> rr:logicalTable [ rr:tableName "dept" ] ;
          rr:subjectMap [ rr:template "http://example.org/dept/{id}" ] .
        <#P> rr:logicalTable [ rr:tableName "person" ] ;
          rr:subjectMap [ rr:template "http://example.org/person/{id}" ] ;
          rr:predicateObjectMap [ rr:predicate ex:dept ; rr:objectMap [ rr:parentTriplesMap <#D> ;
            rr:joinCondition [ rr:child "dept" ; rr:parent "id" ] ] ] .""",
        **source,
    )
    res = m.query(
        "PREFIX ex: <http://example.org/> SELECT ?s ?o WHERE { ?s ex:dept ?o } ORDER BY ?s"
    )
    assert res["s"].to_list() == [
        "<http://example.org/person/-4>",
        "<http://example.org/person/1>",
        "<http://example.org/person/2>",
    ]
    assert res["o"].to_list() == [
        "<http://example.org/dept/10>",
        "<http://example.org/dept/10>",
        "<http://example.org/dept/20>",
    ]


@pytest.mark.parametrize("source", _sources(), ids=["duckdb", "polars"])
def test_datetimes_with_time_zones_and_data_errors(source):
    with pytest.warns(UserWarning, match="not a date"):
        m = _map(
            """<#P> rr:logicalTable [ rr:tableName "person" ] ;
              rr:subjectMap [ rr:template "http://example.org/person/{id}" ] ;
              rr:predicateObjectMap [ rr:predicate ex:born ;
                rr:objectMap [ rr:column "born" ; rr:datatype xsd:dateTime ] ] .""",
            ignore_data_errors=True,
            **source,
        )
    res = m.query(
        """PREFIX ex: <http://example.org/>
        SELECT ?s (STR(?b) AS ?v) (TZ(?b) AS ?tz) WHERE { ?s ex:born ?b } ORDER BY ?s"""
    )
    assert res["s"].to_list() == [
        "<http://example.org/person/1>",
        "<http://example.org/person/2>",
        "<http://example.org/person/3>",
    ]
    assert res["tz"].to_list() == ["Z", "+02:00", ""]


@pytest.mark.parametrize("source", _sources(), ids=["duckdb", "polars"])
def test_blank_nodes_and_graph_columns(source):
    m = _map(
        """<#P> rr:logicalTable [ rr:tableName "person" ] ;
          rr:subjectMap [ rr:template "{last}" ; rr:termType rr:BlankNode ;
            rr:graphMap [ rr:template "http://example.org/graph/{g}" ] ] ;
          rr:predicateObjectMap [ rr:predicate ex:first ; rr:objectMap [ rr:column "first" ] ] .""",
        **source,
    )
    res = m.query(
        """PREFIX ex: <http://example.org/>
        SELECT ?g ?f (ISBLANK(?s) AS ?b) WHERE { GRAPH ?g { ?s ex:first ?f } } ORDER BY ?f"""
    )
    assert res["g"].to_list() == [
        "<http://example.org/graph/g1>",
        "<http://example.org/graph/g2>",
        "<http://example.org/graph/g1>",
    ]
    assert res["f"].to_list() == ["Ann B", "Cy", "Dee"]
    assert res["b"].to_list() == [True, True, True]
    # Distinct values give distinct blank nodes, also when they differ only in escaped characters.
    assert m.query(
        "SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { GRAPH ?g { ?s ?p ?o } }"
    )["c"].to_list() == [3]

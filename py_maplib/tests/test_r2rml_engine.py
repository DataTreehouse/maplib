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
        # Letters outside ASCII are unreserved in IRIs and are kept; reserved characters are encoded.
        "<http://example.org/person/Bjørn/1>",
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


MAPPING = """<#P> rr:logicalTable [ rr:tableName "person" ] ;
  rr:subjectMap [ rr:template "http://example.org/person/{id}" ; rr:class ex:Person ] ;
  rr:predicateObjectMap [ rr:predicate ex:first ; rr:objectMap [ rr:column "first" ] ] ;
  rr:predicateObjectMap [ rr:predicate ex:dept ; rr:objectMap [ rr:parentTriplesMap <#D> ;
    rr:joinCondition [ rr:child "dept" ; rr:parent "id" ] ] ] .
<#D> rr:logicalTable [ rr:sqlQuery "SELECT id, name FROM dept" ] ;
  rr:subjectMap [ rr:template "dept/{id}" ] ;
  rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ."""

@pytest.mark.parametrize("source", ["duckdb", "polars"])
def test_decimal_columns_are_xsd_decimal(source):
    """DECIMAL columns with a scale give xsd:decimal literals (the natural mapping), from Arrow
    record batches and from polars DataFrames; a scale of 0 gives xsd:integer."""
    con = duckdb.connect()
    con.execute(
        """CREATE TABLE price AS SELECT * FROM (VALUES
            (1, 7.5, 1234567.125::DECIMAL(18,3), 42::DECIMAL(9,0)),
            (2, -0.25, NULL, 7::DECIMAL(9,0)),
            (3, 10.0, 0.001::DECIMAL(18,3), NULL)
          ) t(id, small, big, whole)"""
    )
    mapping = """<#P> rr:logicalTable [ rr:tableName "price" ] ;
      rr:subjectMap [ rr:template "http://example.org/price/{id}" ] ;
      rr:predicateObjectMap [ rr:predicate ex:small ; rr:objectMap [ rr:column "small" ] ] ;
      rr:predicateObjectMap [ rr:predicate ex:big ; rr:objectMap [ rr:column "big" ] ] ;
      rr:predicateObjectMap [ rr:predicate ex:whole ; rr:objectMap [ rr:column "whole" ] ] ."""
    kwargs = {"database": con} if source == "duckdb" else {"tables": {"price": con.sql("SELECT * FROM price").pl()}}
    m = _map(mapping, **kwargs)
    res = m.query(
        """PREFIX ex: <http://example.org/>
        SELECT ?p (STR(?o) AS ?v) (STR(DATATYPE(?o)) AS ?dt) WHERE { ?s ?p ?o } ORDER BY ?p ?o"""
    )
    xsd = "http://www.w3.org/2001/XMLSchema#"
    assert res.rows() == [
        ("<http://example.org/big>", "0.001", xsd + "decimal"),
        ("<http://example.org/big>", "1234567.125", xsd + "decimal"),
        ("<http://example.org/small>", "-0.25", xsd + "decimal"),
        ("<http://example.org/small>", "7.5", xsd + "decimal"),
        ("<http://example.org/small>", "10", xsd + "decimal"),
        ("<http://example.org/whole>", "7", xsd + "integer"),
        ("<http://example.org/whole>", "42", xsd + "integer"),
    ]
    # The values compare as numbers.
    above = m.query("PREFIX ex: <http://example.org/> SELECT ?s WHERE { ?s ex:small ?v FILTER(?v > 7.0) }")
    assert above.height == 2


MAPPING_GRAPH = "urn:graph:r2rml"
DATA_GRAPH = "urn:graph:data"


@pytest.mark.parametrize("source", _sources(), ids=["duckdb", "polars"])
def test_mapping_graph_gives_the_same_triples_as_the_mapping(source):
    """A mapping read into a named graph (blank node term maps, a join and a relative IRI
    template) maps as the same mapping given as Turtle, and stays in its graph."""
    base = "http://example.org/base/"
    if "tables" in source:
        # Keyed by the table name and by the query.
        tables = source["tables"]
        source = {"tables": {"person": tables["person"], "SELECT id, name FROM dept": tables["dept"]}}
    expected = _map(MAPPING, **source)

    m = Model()
    m.reads(PREFIXES + MAPPING, format="turtle", graph=MAPPING_GRAPH, base_iri=base)
    mapping_size = m.query("SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }", graph=MAPPING_GRAPH)["c"].to_list()
    m.map_r2rml(mapping_graph=MAPPING_GRAPH, base_iri=base, graph=DATA_GRAPH, **source)

    query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o"
    actual = m.query(query, graph=DATA_GRAPH)
    # 4 types, 3 first names, 3 departments of persons and 2 department names.
    assert actual.height == 12
    assert actual.rows() == expected.query(query).rows()
    depts = m.query("PREFIX ex: <http://example.org/> SELECT ?d WHERE { ?p ex:dept ?d }", graph=DATA_GRAPH)
    assert sorted(depts["d"].to_list()) == [
        "<http://example.org/base/dept/10>",
        "<http://example.org/base/dept/10>",
        "<http://example.org/base/dept/20>",
    ]
    # The mapping is untouched, and no data went to the default graph.
    assert m.query("SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }", graph=MAPPING_GRAPH)["c"].to_list() == mapping_size
    assert m.query("SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }")["c"].to_list() == [0]


def test_mapping_graph_follows_updates_of_the_mapping():
    """The mapping is read from the graph at each call, so a SPARQL update of it takes effect."""
    con = _database()
    m = Model()
    m.reads(PREFIXES + MAPPING, format="turtle", graph=MAPPING_GRAPH, base_iri="http://example.org/base/")
    m.update(
        """PREFIX rr: <http://www.w3.org/ns/r2rml#>
        PREFIX ex: <http://example.org/>
        DELETE { GRAPH <urn:graph:r2rml> { ?pom rr:predicate ex:first } }
        INSERT { GRAPH <urn:graph:r2rml> { ?pom rr:predicate ex:givenName } }
        WHERE { GRAPH <urn:graph:r2rml> { ?pom rr:predicate ex:first } }"""
    )
    m.map_r2rml(mapping_graph=MAPPING_GRAPH, database=con, graph=DATA_GRAPH)
    names = m.query("PREFIX ex: <http://example.org/> SELECT ?n WHERE { ?s ex:givenName ?n }", graph=DATA_GRAPH)
    assert sorted(names["n"].to_list()) == ["Ann B", "Cy", "Dee"]
    assert m.query("PREFIX ex: <http://example.org/> SELECT ?n WHERE { ?s ex:first ?n }", graph=DATA_GRAPH).height == 0


def test_mapping_graph_argument_errors():
    con = _database()
    m = Model()
    with pytest.raises(Exception, match="mapping_graph"):
        m.map_r2rml(database=con)
    with pytest.raises(Exception, match="not both"):
        m.map_r2rml(PREFIXES + MAPPING, mapping_graph=MAPPING_GRAPH, database=con)
    # A graph without triples maps, here one that does not exist.
    with pytest.raises(Exception, match="no R2RML triples maps"):
        m.map_r2rml(mapping_graph="urn:graph:empty", database=con)

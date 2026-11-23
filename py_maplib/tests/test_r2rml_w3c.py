"""The W3C R2RML test cases (testdata/r2rml, see its README), run with DuckDB as the database.

Each test case creates its database from the SQL script, maps it with Model.map_r2rml using the
base IRI http://example.com/base/, and compares the result with the expected N-Quads graph by
graph (blank nodes up to isomorphism, literals by value). Test cases without expected output must
raise an error.
"""

import re
from pathlib import Path

import pytest
import rdflib
from rdflib import Literal, URIRef
from rdflib.compare import isomorphic

from maplib import Model

duckdb = pytest.importorskip("duckdb")

TESTDATA = Path(__file__).parent / "testdata" / "r2rml"
BASE = "http://example.com/base/"
MAPPING_GRAPH = "urn:maplib:test:r2rml-mapping"
TEST = "http://www.w3.org/2006/03/test-description#"
RDB2RDF = "http://purl.org/NET/rdb2rdf-test#"
DCTERMS = "http://purl.org/dc/terms/"

# Test cases where maplib knowingly deviates from the test suite.
KNOWN_DEVIATIONS = {
    # A regular identifier (Name) matches the column "Name" case insensitively, as in DuckDB,
    # Snowflake and SQL Server, instead of being folded to NAME or name and not found.
    "R2RMLTC0002f": "regular identifiers match columns case insensitively",
    # DuckDB renames the second "ID" of the query result, so the duplicate column is not seen.
    "R2RMLTC0002h": "DuckDB renames duplicate result columns",
    # DuckDB has no blank padded CHAR(n): it is VARCHAR.
    "R2RMLTC0018a": "DuckDB does not pad CHAR(n)",
}


def _cases():
    g = rdflib.Graph()
    g.parse(TESTDATA / "manifest.ttl", format="turtle")
    cases = []
    for tc in g.subjects(rdflib.RDF.type, URIRef(RDB2RDF + "R2RML")):
        identifier = str(g.value(tc, URIRef(DCTERMS + "identifier")))
        database = g.value(tc, URIRef(RDB2RDF + "database"))
        script = str(g.value(database, URIRef(RDB2RDF + "sqlScriptFile")))
        mapping = str(g.value(tc, URIRef(RDB2RDF + "mappingDocument")))
        expected = g.value(tc, URIRef(RDB2RDF + "hasExpectedOutput"))
        output = g.value(tc, URIRef(RDB2RDF + "output"))
        cases.append(
            pytest.param(
                identifier,
                script,
                mapping,
                bool(expected.toPython()) if expected is not None else True,
                str(output) if output is not None else None,
                id=identifier,
            )
        )
    return sorted(cases, key=lambda p: p.id)


def _database(script: str):
    path = TESTDATA / "databases" / script
    postgres = path.with_name(path.stem + "-postgresql.sql")
    if postgres.exists():
        path = postgres
    sql = path.read_text()
    sql = re.sub(r"\bcascade\b", "", sql, flags=re.IGNORECASE)
    # PostgreSQL bytea literals '\x...' are hex blobs.
    sql = re.sub(r"'\\x([0-9A-Fa-f]+)'", lambda m: "unhex('" + m.group(1) + "')", sql)
    sql = sql.replace("BYTEA", "BLOB").replace("bytea", "BLOB")
    con = duckdb.connect()
    for statement in sql.split(";"):
        if statement.strip():
            con.execute(statement)
    return con


class DuckDB:
    def __init__(self, con):
        self.con = con

    def query(self, sql: str):
        return self.con.sql(sql).pl()


def _normalize(term):
    if isinstance(term, Literal) and term.datatype is not None and term.language is None:
        try:
            value = term.toPython()
        except Exception:
            return term
        if value is not None and not isinstance(value, Literal):
            if isinstance(value, float):
                return Literal(repr(value), datatype=term.datatype)
            normalized = Literal(value, datatype=term.datatype)
            if normalized.toPython() == value:
                return normalized
    return term


def _graphs(ds: rdflib.Dataset):
    out = {}
    for s, p, o, g in ds.quads((None, None, None, None)):
        name = None if g is None or isinstance(g, rdflib.Graph) and g.identifier == rdflib.graph.DATASET_DEFAULT_GRAPH_ID else g
        if isinstance(name, rdflib.Graph):
            name = name.identifier
        if name == rdflib.graph.DATASET_DEFAULT_GRAPH_ID:
            name = None
        out.setdefault(name, rdflib.Graph()).add((s, p, _normalize(o)))
    return out


# The query results come as polars DataFrames (DuckDB above), or as Arrow record batches from
# the duckdb connection itself. With "graph", the mapping is first read into a graph of the model
# and mapped with mapping_graph.
@pytest.mark.parametrize("source", ["polars", "arrow", "graph"])
@pytest.mark.parametrize("identifier,script,mapping,has_expected,output", _cases())
def test_r2rml_w3c(identifier, script, mapping, has_expected, output, source, tmp_path):
    if identifier in KNOWN_DEVIATIONS:
        pytest.skip(KNOWN_DEVIATIONS[identifier])
    con = _database(script)
    database = DuckDB(con) if source == "polars" else con
    m = Model()
    mapping_path = TESTDATA / identifier / mapping

    def run():
        if source == "graph":
            m.read(mapping_path, format="turtle", graph=MAPPING_GRAPH, base_iri=BASE)
            m.map_r2rml(mapping_graph=MAPPING_GRAPH, database=database, base_iri=BASE)
            m.update(f"DROP GRAPH <{MAPPING_GRAPH}>")
        else:
            m.map_r2rml(mapping_path, database=database, base_iri=BASE)

    if not has_expected:
        with pytest.raises(Exception):
            run()
        return
    run()
    out = tmp_path / "out.nq"
    m.write(out, format="nquads")
    actual = rdflib.Dataset()
    actual.parse(out, format="nquads")
    expected = rdflib.Dataset()
    expected.parse(TESTDATA / identifier / output, format="nquads")
    a = _graphs(actual)
    e = _graphs(expected)
    assert set(a) == set(e), f"graphs differ: {sorted(map(str, a))} != {sorted(map(str, e))}"
    for name in e:
        assert isomorphic(a[name], e[name]), (
            f"graph {name} differs:\nactual:\n{a[name].serialize(format='nt')}\n"
            f"expected:\n{e[name].serialize(format='nt')}"
        )


def test_r2rml_tables_and_duckdb_connection():
    """Logical tables given as DataFrames by table name, and a duckdb connection as database."""
    import polars as pl

    mapping = """
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.com/> .
    <#Pumps> rr:logicalTable [ rr:tableName "pumps" ] ;
        rr:subjectMap [ rr:template "http://example.com/pump/{id}" ; rr:class ex:Pump ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ; rr:language "en" ] ] ,
            [ rr:predicate ex:site ; rr:objectMap [ rr:parentTriplesMap <#Sites> ;
                rr:joinCondition [ rr:child "site" ; rr:parent "code" ] ] ] .
    <#Sites> rr:logicalTable [ rr:sqlQuery "SELECT code, label FROM sites" ] ;
        rr:subjectMap [ rr:template "site/{code}" ; rr:graph ex:sites ] ;
        rr:predicateObjectMap [ rr:predicate ex:label ; rr:objectMap [ rr:column "label" ] ] .
    """
    con = duckdb.connect()
    con.execute("CREATE TABLE sites AS SELECT * FROM (VALUES ('A b', 'Alpha'), ('B', 'Beta')) t(code, label)")
    pumps = pl.DataFrame({"id": [1, 2, 3], "name": ["P1", "P2", None], "site": ["A b", "B", "C"]})
    m = Model()
    m.map_r2rml(mapping, database=con, tables={"pumps": pumps}, base_iri="http://example.com/")
    res = m.query(
        """PREFIX ex:<http://example.com/>
        SELECT ?p (STR(?nl) AS ?n) (LANG(?nl) AS ?l) ?s WHERE {
            ?p a ex:Pump . OPTIONAL { ?p ex:name ?nl } OPTIONAL { ?p ex:site ?s } }
        ORDER BY ?p"""
    )
    assert res.height == 3
    assert res["s"].to_list()[:2] == ["<http://example.com/site/A%20b>", "<http://example.com/site/B>"]
    assert res["n"].to_list()[:2] == ["P1", "P2"]
    assert res["l"].to_list()[0] == "en"
    labels = m.query(
        """PREFIX ex:<http://example.com/>
        SELECT ?l WHERE { GRAPH ex:sites { ?s ex:label ?l } } ORDER BY ?l"""
    )
    assert labels["l"].to_list() == ["Alpha", "Beta"]

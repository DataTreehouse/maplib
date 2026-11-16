"""
SPARQL 1.1 Federated Query (SERVICE) and SPARQL Update LOAD, against local mock endpoints
(sparql_mock_endpoint.py). The W3C service tests are run by test_w3c_sparql11.py.
"""

import re

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from maplib import Model, MaplibException
from .sparql_mock_endpoint import MockEndpoints

EX = "http://example.org/"

REMOTE_TTL = """
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:a ex:name "Alan"@en ; ex:age 42 ; ex:born "1980-01-02T03:04:05Z"^^xsd:dateTime ; ex:ok true .
ex:b ex:name "Bob" ; ex:age "forty"@en .
ex:c ex:name "Cecil" .
ex:d ex:knows ex:a .
"""

LOCAL_TTL = """
@prefix ex: <http://example.org/> .
ex:a a ex:Person .
ex:b a ex:Person .
ex:c a ex:Person .
ex:x a ex:Person .
"""


@pytest.fixture
def mock():
    with MockEndpoints() as m:
        yield m


def model(ttl: str) -> Model:
    m = Model()
    m.reads(ttl, format="turtle")
    return m


def sorted_df(df: pl.DataFrame) -> pl.DataFrame:
    return df.sort(df.columns, nulls_last=True)


@pytest.mark.parametrize("fmt", ["json", "xml", "tsv"])
def test_service_join_types_and_unbound(mock, fmt):
    url = mock.add("r", model(REMOTE_TTL), fmt)
    m = model(LOCAL_TTL)
    res = m.query(
        f"""
        PREFIX ex: <{EX}>
        SELECT ?p (DATATYPE(?age) AS ?dt) (LANG(?name) AS ?lang) (BOUND(?born) AS ?hasborn) ?ok WHERE {{
            ?p a ex:Person .
            SERVICE <{url}> {{
                ?p ex:name ?name .
                OPTIONAL {{ ?p ex:age ?age }}
                OPTIONAL {{ ?p ex:born ?born }}
                OPTIONAL {{ ?p ex:ok ?ok }}
            }}
        }} ORDER BY ?p"""
    )
    assert res["p"].to_list() == [f"<{EX}a>", f"<{EX}b>", f"<{EX}c>"]
    assert res["dt"].to_list() == [
        "<http://www.w3.org/2001/XMLSchema#integer>",
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>",
        None,
    ]
    assert res["lang"].to_list() == ["en", "", ""]
    assert res["hasborn"].to_list() == [True, False, False]
    assert res["ok"].to_list() == [True, None, None]


def test_service_alone_and_same_as_local(mock):
    url = mock.add("r", model(REMOTE_TTL))
    m = Model()
    q = f"PREFIX ex: <{EX}> SELECT ?s ?n WHERE {{ SERVICE <{url}> {{ ?s ex:name ?n }} }}"
    remote = sorted_df(m.query(q))
    local = sorted_df(
        model(REMOTE_TTL).query(f"PREFIX ex: <{EX}> SELECT ?s ?n WHERE {{ ?s ex:name ?n }}")
    )
    assert_frame_equal(remote, local)
    # Sent without VALUES, since nothing is bound before it.
    assert "VALUES" not in mock.endpoint(url).queries[-1]


def bind_join_data(n: int):
    local = "\n".join(f"<{EX}s{i}> a <{EX}T> ." for i in range(n))
    remote = "\n".join(f'<{EX}s{i}> <{EX}v> {i} .' for i in range(0, 2 * n, 2))
    return model(local), model(remote)


def test_bind_join_batches(mock):
    local, remote = bind_join_data(2500)
    url = mock.add("r", remote)
    local.set_federation(batch_size=1000)
    q = f"""SELECT ?s ?v WHERE {{ ?s a <{EX}T> . SERVICE <{url}> {{ ?s <{EX}v> ?v }} }}"""
    df = local.query(q)
    assert df.height == 1250
    queries = mock.endpoint(url).queries
    assert len(queries) == 3
    assert all("VALUES" in x for x in queries)
    # Without the bind join, the pattern is sent once and joined locally: the same results.
    local.set_federation(bind_join=False)
    df2 = local.query(q)
    assert len(queries) == 4 and "VALUES" not in queries[-1]
    assert_frame_equal(sorted_df(df), sorted_df(df2))


def test_bind_join_concurrency_and_get(mock):
    local, remote = bind_join_data(300)
    url = mock.add("r", remote)
    local.set_federation(batch_size=10, concurrency=8, method="GET")
    df = local.query(
        f"SELECT ?s ?v WHERE {{ ?s a <{EX}T> . SERVICE <{url}> {{ ?s <{EX}v> ?v }} }}"
    )
    assert df.height == 150
    assert len(mock.endpoint(url).queries) == 30


def test_unbound_incoming_variable_is_not_sent(mock):
    url = mock.add("r", model(REMOTE_TTL))
    m = model(LOCAL_TTL + f"<{EX}a> <{EX}nick> \"Al\" .")
    df = m.query(
        f"""PREFIX ex: <{EX}>
        SELECT ?p ?nick ?name WHERE {{
            ?p a ex:Person . OPTIONAL {{ ?p ex:nick ?nick }}
            SERVICE <{url}> {{ ?p ex:name ?name OPTIONAL {{ ?p ex:nick ?nick }} }}
        }}"""
    )
    # ?nick is unbound in some local rows, so only ?p is sent.
    q = mock.endpoint(url).queries[-1]
    values = re.search(r"VALUES\s*(\([^)]*\)|\?\w+)", q)
    assert values is not None, q
    assert "?p" in values.group(1) and "?nick" not in values.group(1), q
    assert df.height == 3


def test_optional_service_sends_left_bindings(mock):
    url = mock.add("r", model(REMOTE_TTL))
    m = model(LOCAL_TTL)
    df = m.query(
        f"""PREFIX ex: <{EX}>
        SELECT ?p ?name WHERE {{
            ?p a ex:Person .
            OPTIONAL {{ SERVICE <{url}> {{ ?p ex:name ?name }} }}
        }}"""
    )
    assert df.height == 4
    assert df.filter(pl.col("name").is_null())["p"].to_list() == [f"<{EX}x>"]
    assert "VALUES" in mock.endpoint(url).queries[-1]


def test_union_minus_filter_around_and_inside(mock):
    url = mock.add("r", model(REMOTE_TTL))
    m = model(LOCAL_TTL)
    df = m.query(
        f"""PREFIX ex: <{EX}>
        SELECT ?p WHERE {{
            ?p a ex:Person .
            MINUS {{ SERVICE <{url}> {{ ?p ex:age ?age FILTER(isLiteral(?age)) }} }}
        }}"""
    )
    assert sorted(df["p"].to_list()) == [f"<{EX}c>", f"<{EX}x>"]
    df = m.query(
        f"""PREFIX ex: <{EX}>
        SELECT ?p ?n WHERE {{
            {{ ?p a ex:Person . SERVICE <{url}> {{ ?p ex:name ?n }} }}
            UNION
            {{ SERVICE <{url}> {{ ?p ex:knows ?n }} }}
            FILTER(?p != ex:b)
        }}"""
    )
    assert sorted(df["p"].to_list()) == [f"<{EX}a>", f"<{EX}c>", f"<{EX}d>"]


def test_variable_endpoint(mock):
    url1 = mock.add("one", model(f'<{EX}a> <{EX}name> "one" .'))
    url2 = mock.add("two", model(f'<{EX}a> <{EX}name> "two" .'))
    m = Model()
    df = m.query(
        f"""SELECT ?e ?n WHERE {{
            VALUES ?e {{ <{url1}> <{url2}> }}
            SERVICE ?e {{ ?s <{EX}name> ?n }}
        }}"""
    )
    assert sorted(df["n"].to_list()) == ["one", "two"]
    with pytest.raises(MaplibException, match="needs \\?e to be bound"):
        m.query(f"SELECT * WHERE {{ SERVICE ?e {{ ?s ?p ?o }} }}")


def test_silent(mock):
    url = mock.add("r", model(REMOTE_TTL))
    mock.endpoint(url).fail_status = 500
    m = model(LOCAL_TTL)
    with pytest.raises(MaplibException, match="answered 500"):
        m.query(f"SELECT * WHERE {{ ?p a ?t . SERVICE <{url}> {{ ?p ?q ?o }} }}")
    df = m.query(f"SELECT * WHERE {{ ?p a ?t . SERVICE SILENT <{url}> {{ ?p ?q ?o }} }}")
    assert df.height == 4
    # An endpoint that can not be reached.
    df = m.query("SELECT * WHERE { SERVICE SILENT <http://127.0.0.1:9/sparql> { ?s ?p ?o } }")
    assert df.height == 1
    with pytest.raises(MaplibException, match="Could not reach"):
        m.query("SELECT * WHERE { SERVICE <http://127.0.0.1:9/sparql> { ?s ?p ?o } }")


def test_allow_list(mock):
    url = mock.add("r", model(REMOTE_TTL))
    m = model(LOCAL_TTL)
    m.set_federation(allowed_endpoints=["http://127.0.0.1:1"])
    for silent in ["", "SILENT"]:
        with pytest.raises(MaplibException, match="is not allowed"):
            m.query(f"SELECT * WHERE {{ SERVICE {silent} <{url}> {{ ?s ?p ?o }} }}")
    assert mock.endpoint(url).queries == []
    m.set_federation(allowed_endpoints=[mock.base])
    assert m.query(f"SELECT * WHERE {{ SERVICE <{url}> {{ ?s ?p ?o }} }}").height > 0
    m.set_federation(allowed_endpoints=[])
    with pytest.raises(MaplibException, match="is not allowed"):
        m.query(f"SELECT * WHERE {{ SERVICE <{url}> {{ ?s ?p ?o }} }}")


def test_headers_and_max_rows(mock):
    url = mock.add("r", model(REMOTE_TTL))
    m = Model()
    m.set_federation(headers={mock.base: {"Authorization": "Bearer secret"}})
    m.query(f"SELECT * WHERE {{ SERVICE <{url}> {{ ?s ?p ?o }} }}")
    assert mock.endpoint(url).headers_seen[-1]["authorization"] == "Bearer secret"
    m.set_federation(max_rows=2)
    with pytest.raises(MaplibException, match="more than the maximum of 2 rows"):
        m.query(f"SELECT * WHERE {{ SERVICE SILENT <{url}> {{ ?s ?p ?o }} }}")


def test_timeout(mock):
    url = mock.add("r", model(REMOTE_TTL))
    mock.endpoint(url).delay = 3
    m = Model()
    m.set_federation(timeout=0.5)
    with pytest.raises(MaplibException, match="timeout"):
        m.query(f"SELECT * WHERE {{ SERVICE <{url}> {{ ?s ?p ?o }} }}")


def test_service_in_update(mock):
    url = mock.add("r", model(REMOTE_TTL))
    m = model(LOCAL_TTL)
    m.update(
        f"""PREFIX ex: <{EX}>
        INSERT {{ ?p ex:localName ?n }} WHERE {{ ?p a ex:Person . SERVICE <{url}> {{ ?p ex:name ?n }} }}"""
    )
    assert m.query(f"SELECT * WHERE {{ ?p <{EX}localName> ?n }}").height == 3


TTL_FILE = f'<{EX}l1> <{EX}p> "loaded" .\n'.encode()


def test_load_refused_by_default(mock):
    url = mock.add_file("/data.ttl", TTL_FILE, "text/turtle")
    m = Model()
    with pytest.raises(MaplibException, match="LOAD is not allowed"):
        m.update(f"LOAD <{url}>")
    with pytest.raises(MaplibException, match="LOAD is not allowed"):
        m.update(f"LOAD SILENT <{url}>")


def test_load(mock):
    m = Model()
    m.set_federation(allow_load=True)
    # By media type, without an extension.
    url = mock.add_file("/data", TTL_FILE, "text/turtle")
    m.update(f"LOAD <{url}>")
    assert m.query(f"SELECT ?o WHERE {{ <{EX}l1> <{EX}p> ?o }}")["o"].to_list() == ["loaded"]
    # By extension, without a media type, into a graph.
    url = mock.add_file("/x/data.nt", TTL_FILE, None)
    m.update(f"LOAD <{url}> INTO GRAPH <{EX}g>")
    assert m.query(f"SELECT ?o WHERE {{ <{EX}l1> <{EX}p> ?o }}", graph=f"{EX}g").height == 1
    # Missing: an error, SILENT ignores it.
    with pytest.raises(MaplibException, match="404"):
        m.update(f"LOAD <{mock.base}/missing.ttl>")
    m.update(f"LOAD SILENT <{mock.base}/missing.ttl>")
    # The allow list applies to LOAD as well, also with SILENT.
    m.set_federation(allow_load=True, allowed_endpoints=["http://127.0.0.1:1"])
    with pytest.raises(MaplibException, match="is not allowed"):
        m.update(f"LOAD SILENT <{url}>")


def test_load_file_url_needs_explicit_allow(tmp_path):
    f = tmp_path / "x.ttl"
    f.write_bytes(TTL_FILE)
    m = Model()
    m.set_federation(allow_load=True)
    with pytest.raises(MaplibException, match="is not allowed"):
        m.update(f"LOAD <{f.as_uri()}>")
    m.set_federation(allow_load=True, allowed_endpoints=[tmp_path.as_uri()])
    m.update(f"LOAD <{f.as_uri()}>")
    assert m.query(f"SELECT ?o WHERE {{ <{EX}l1> <{EX}p> ?o }}").height == 1


def test_set_federation_validates_arguments():
    m = Model()
    with pytest.raises(Exception):
        m.set_federation(batch_size=0)
    with pytest.raises(Exception):
        m.set_federation(method="PUT")

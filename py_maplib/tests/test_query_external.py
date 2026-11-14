import multiprocessing
import socket
import time

import pytest
from rdflib import Graph
from rdflib_endpoint import SparqlEndpoint

from maplib import Model


def run_server():
    """Target function to run the web server in a separate process."""
    g = Graph()
    g.parse(data='<http://a> <http://b> <http://c> .', format='turtle')
    app = SparqlEndpoint(graph=g)
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")


@pytest.fixture(scope="module")
def live_endpoint():
    # Start the server in the background
    proc = multiprocessing.Process(target=run_server, daemon=True)
    proc.start()
    # Wait until uvicorn accepts connections (startup time varies between Python versions)
    deadline = time.time() + 30
    while True:
        try:
            with socket.create_connection(("127.0.0.1", 8000), timeout=0.5):
                break
        except OSError:
            if not proc.is_alive():
                pytest.fail("SPARQL endpoint process exited before accepting connections")
            if time.time() > deadline:
                proc.terminate()
                pytest.fail("SPARQL endpoint did not start within 30 seconds")
            time.sleep(0.05)
    yield "http://127.0.0.1:8000"
    proc.terminate()


def test_query_external_get(live_endpoint):
    m = Model()
    df = m.query_external(
        """
        SELECT * WHERE {?a ?b ?c}
        """,
        live_endpoint,
        "GET",
    )
    assert df.height == 1


def test_query_external_post(live_endpoint):
    m = Model()
    df = m.query_external(
        """
        SELECT * WHERE {?a ?b ?c}
        """,
        live_endpoint,
        "POST"
    )
    assert df.height == 1


OPTIONAL_DATA = """
@prefix ex: <http://example.org/> .
ex:a a ex:T ; ex:name "A" ; ex:value 1 .
ex:b a ex:T ; ex:value ex:other .
ex:c a ex:T .
"""


def run_optional_server():
    g = Graph()
    g.parse(data=OPTIONAL_DATA, format="turtle")
    app = SparqlEndpoint(graph=g)
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8001, log_level="warning")


@pytest.fixture(scope="module")
def optional_endpoint():
    proc = multiprocessing.Process(target=run_optional_server, daemon=True)
    proc.start()
    deadline = time.time() + 30
    while True:
        try:
            with socket.create_connection(("127.0.0.1", 8001), timeout=0.5):
                break
        except OSError:
            if not proc.is_alive():
                pytest.fail("SPARQL endpoint process exited before accepting connections")
            if time.time() > deadline:
                proc.terminate()
                pytest.fail("SPARQL endpoint did not start within 30 seconds")
            time.sleep(0.05)
    yield "http://127.0.0.1:8001"
    proc.terminate()


@pytest.mark.parametrize("method", ["GET", "POST"])
def test_query_external_partially_bound_optional(optional_endpoint, method):
    # unbound OPTIONAL values used to panic ("entered unreachable code") while building the columns
    df = Model().query_external(
        """PREFIX ex: <http://example.org/>
        SELECT ?s ?name WHERE { ?s a ex:T OPTIONAL { ?s ex:name ?name } } ORDER BY DESC(?s)""",
        optional_endpoint,
        method,
    )
    assert df.height == 3
    assert df["s"].to_list() == ["<http://example.org/c>", "<http://example.org/b>", "<http://example.org/a>"]
    # the first rows are unbound, the bound value comes last
    assert df["name"].to_list() == [None, None, "A"]


def test_query_external_never_bound_optional(optional_endpoint):
    df = Model().query_external(
        """PREFIX ex: <http://example.org/>
        SELECT ?s ?missing WHERE { ?s a ex:T OPTIONAL { ?s ex:missing ?missing } }""",
        optional_endpoint,
    )
    assert df.height == 3
    assert df["missing"].null_count() == 3


def test_query_external_unbound_with_mixed_types(optional_endpoint):
    # ?value is a literal, an IRI and unbound: a struct column with nulls where unbound
    df = Model().query_external(
        """PREFIX ex: <http://example.org/>
        SELECT ?s ?value WHERE { ?s a ex:T OPTIONAL { ?s ex:value ?value } } ORDER BY ?s""",
        optional_endpoint,
    )
    assert df.height == 3
    assert df["value"].is_null().to_list() == [False, False, True]

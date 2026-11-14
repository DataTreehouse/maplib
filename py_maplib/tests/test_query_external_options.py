"""Coverage for the options of Model.query_external: return formats, solution mappings, bindings
and headers, against a local rdflib-endpoint server."""

import json
import multiprocessing
import socket
import time

import pytest

from maplib import IRI, Literal, Model, SolutionMappings, xsd

EX = "http://example.net/ns#"

DATA = f"""
<{EX}a> <{EX}name> "Alice" .
<{EX}b> <{EX}name> "Bob" .
<{EX}a> <{EX}age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
"""


def _run_server(port):
    from rdflib import Graph
    from rdflib_endpoint import SparqlEndpoint
    import uvicorn

    g = Graph()
    g.parse(data=DATA, format="turtle")
    uvicorn.run(SparqlEndpoint(graph=g), host="127.0.0.1", port=port, log_level="warning")


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def endpoint():
    port = _free_port()
    proc = multiprocessing.Process(target=_run_server, args=(port,), daemon=True)
    proc.start()
    deadline = time.time() + 30
    while True:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            if not proc.is_alive() or time.time() > deadline:
                proc.terminate()
                pytest.fail("SPARQL endpoint did not start")
            time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"
    proc.terminate()


NAMES = f"SELECT ?s ?n WHERE {{ ?s <{EX}name> ?n }} ORDER BY ?n"


@pytest.mark.parametrize("method", ["GET", "POST"])
def test_query_external_dataframe(endpoint, method):
    df = Model().query_external(NAMES, endpoint, method=method)
    assert df["n"].to_list() == ["Alice", "Bob"]
    assert df["s"].to_list() == [f"<{EX}a>", f"<{EX}b>"]


def test_query_external_solution_mappings(endpoint):
    sm = Model().query_external(NAMES, endpoint, solution_mappings=True)
    assert isinstance(sm, SolutionMappings)
    assert sm.mappings.height == 2
    assert set(sm.rdf_types) == {"s", "n"}


def test_query_external_typed_literal(endpoint):
    df = Model().query_external(
        f"SELECT ?age WHERE {{ <{EX}a> <{EX}age> ?age }}", endpoint
    )
    assert df["age"].to_list() == [30]


@pytest.mark.parametrize("flag", ["return_json", "return_format"])
def test_query_external_json(endpoint, flag):
    kwargs = {"return_json": True} if flag == "return_json" else {"return_format": "json"}
    s = Model().query_external(NAMES, endpoint, **kwargs)
    res = json.loads(s)
    assert sorted(res["head"]["vars"]) == ["n", "s"]
    assert sorted(b["n"]["value"] for b in res["results"]["bindings"]) == ["Alice", "Bob"]


@pytest.mark.parametrize(
    "value",
    [IRI(EX + "a"), f"<{EX}a>"],
)
def test_query_external_bindings_iri(endpoint, value):
    df = Model().query_external(
        f"SELECT ?n WHERE {{ ?s <{EX}name> ?n }}", endpoint, bindings={"s": value}
    )
    assert df["n"].to_list() == ["Alice"]


def test_query_external_bindings_literal(endpoint):
    df = Model().query_external(
        f"SELECT ?s WHERE {{ ?s <{EX}name> ?n }}",
        endpoint,
        bindings={"n": Literal("Bob", data_type=xsd.string)},
    )
    assert df["s"].to_list() == [f"<{EX}b>"]


@pytest.mark.parametrize("method", ["GET", "POST"])
def test_query_external_headers(endpoint, method):
    df = Model().query_external(
        NAMES,
        endpoint,
        method=method,
        headers=[("X-Test-Header", "maplib"), ("User-Agent", "maplib-tests")],
    )
    assert df.height == 2


def test_query_external_unreachable_endpoint_raises():
    port = _free_port()
    with pytest.raises(Exception):
        Model().query_external(NAMES, f"http://127.0.0.1:{port}")


def test_query_external_bad_query_raises(endpoint):
    with pytest.raises(Exception):
        Model().query_external("SELECT ?x WHERE { not sparql", endpoint)

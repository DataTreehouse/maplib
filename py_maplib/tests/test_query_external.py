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

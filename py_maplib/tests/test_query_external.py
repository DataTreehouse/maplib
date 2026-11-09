import multiprocessing
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
    time.sleep(1)  # Give uvicorn a brief second to bind to the port
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

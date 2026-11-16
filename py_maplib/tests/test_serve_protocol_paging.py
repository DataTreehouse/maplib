"""The W3C SPARQL 1.1 Protocol query tests against Model.serve(), with and without the paging
and slicing parameters of the server.

The test data in ``testdata/w3c_sparql11_protocol`` is an unmodified copy of
``sparql/sparql11/protocol`` of https://github.com/w3c/rdf-tests (commit 7b5afb91), see its
``LICENSE.md``. Each query test (``query_*`` and ``bad_query_*``) sends its HTTP requests as
described in the manifest and checks the status class, the result format and the boolean
result. Every test is also run with ``limit=1000&offset=0`` added, which must not change its
outcome, and the SELECT tests with ``page_size=1``, which gives the only solution and no next
page token. The update tests are not run (the server is read only here).

Model.serve() is part of maplib Enterprise Edition, so these tests skip on the open-source
build.
"""

import http.client
import json
import re
import urllib.parse
from pathlib import Path

import pytest
import rdflib

from maplib import Model

SUITE = Path(__file__).parent / "testdata" / "w3c_sparql11_protocol"
BASE = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/"
MF = rdflib.Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
HT = rdflib.Namespace("http://www.w3.org/2011/http#")
HTS = rdflib.Namespace("http://www.w3.org/2011/http-statusCodes#")
CNT = rdflib.Namespace("http://www.w3.org/2011/content#")
UT = rdflib.Namespace("http://www.w3.org/2009/sparql/tests/test-update#")

RESULT_TYPES = {
    "boolean": ("application/sparql-results+xml", "application/sparql-results+json"),
    "tabular": (
        "application/sparql-results+xml",
        "application/sparql-results+json",
        "text/csv",
        "text/tab-separated-values",
    ),
    "RDF": ("application/rdf+xml", "text/turtle", "application/n-triples", "text/html",
              "application/xhtml+xml"),
}


def load_tests():
    g = rdflib.Graph()
    g.parse(SUITE / "manifest.ttl", format="turtle", publicID=BASE + "manifest.ttl")
    tests = []
    for entry in g.subjects(rdflib.RDF.type, MF.ProtocolTest):
        name = str(entry).split("#")[-1]
        if not (name.startswith("query_") or name.startswith("bad_query")):
            continue
        graphs = []
        for gd in g.objects(entry, UT.graphData):
            path = SUITE / str(g.value(gd, UT.graph)).split("/")[-1]
            graphs.append((str(g.value(gd, rdflib.RDFS.label)), path))
        requests = []
        conn = g.value(entry, MF.action)
        for req in rdflib.collection.Collection(g, g.value(conn, HT.requests)):
            headers = {}
            hs = g.value(req, HT.headers)
            if hs is not None:
                for h in rdflib.collection.Collection(g, hs):
                    headers[str(g.value(h, HT.fieldName)).lower()] = str(g.value(h, HT.fieldValue))
            body = g.value(req, HT.body)
            if body is not None:
                encoding = str(g.value(body, CNT.characterEncoding) or "UTF-8")
                body = str(g.value(body, CNT.chars)).encode(encoding.lower())
            resp = g.value(req, HT.resp)
            statuses = {str(s).split("StatusCode")[-1][0] for s in g.objects(resp, MF.expectedStatus)}
            boolean = g.value(resp, MF.expectedBoolean)
            fmt = g.value(resp, MF.expectedFormat)
            requests.append(
                {
                    "method": str(g.value(req, HT.methodName)),
                    "path": str(g.value(req, HT.absolutePath)),
                    "headers": headers,
                    "body": body,
                    "statuses": statuses,
                    "boolean": None if boolean is None else bool(boolean.toPython()),
                    "format": None if fmt is None else str(fmt),
                }
            )
        tests.append((name, graphs, requests))
    return sorted(tests)


TESTS = load_tests()


def serve(graphs):
    m = Model()
    for iri, path in graphs:
        m.reads(path.read_text(), format="ntriples", graph=iri)
    try:
        return m.serve(port=0, flight_port=None)
    except Exception as e:
        if "Enterprise Edition" in str(e):
            pytest.skip("Model.serve() needs maplib Enterprise Edition")
        raise


def is_select(r) -> bool:
    text = urllib.parse.unquote_plus(r["path"]) + (r["body"] or b"").decode("utf-8", "replace")
    return re.search(r"\bSELECT\b", text, re.I) is not None


def with_params(r, params):
    """The request with extra protocol parameters, in the form body of a form POST and in the
    URL otherwise."""
    r = dict(r)
    extra = urllib.parse.urlencode(params)
    if r["method"] == "POST" and r["headers"].get("content-type", "").startswith(
        "application/x-www-form-urlencoded"
    ):
        r["body"] = (r["body"] or b"") + b"&" + extra.encode()
    else:
        r["path"] += ("&" if "?" in r["path"] else "?") + extra
    return r


def send(server, r):
    """Sends the request with exactly the headers of the test (urllib would add a form
    Content-Type to a body without one)."""
    path = r["path"]
    query = path.split("?", 1)[1] if "?" in path else ""
    url = urllib.parse.urlsplit(server.url)
    conn = http.client.HTTPConnection(url.hostname, url.port, timeout=30)
    try:
        conn.request(
            r["method"],
            url.path + ("?" + query if query else ""),
            body=r["body"],
            headers=r["headers"],
        )
        resp = conn.getresponse()
        return resp.status, dict(resp.getheaders()), resp.read()
    finally:
        conn.close()


def header(headers, name):
    for k, v in headers.items():
        if k.lower() == name:
            return v
    return None


def check(r, status, headers, body, label):
    assert str(status)[0] in r["statuses"], f"{label}: status {status}: {body[:300]!r}"
    if str(status)[0] != "2":
        return
    media = (header(headers, "content-type") or "").split(";")[0].strip()
    if r["format"] is not None:
        assert media in RESULT_TYPES[r["format"]], f"{label}: media type {media}"
    if r["boolean"] is not None:
        if media.endswith("json"):
            value = json.loads(body)["boolean"]
        else:
            value = re.search(rb"<boolean>\s*(true|false)\s*</boolean>", body).group(1) == b"true"
        assert value == r["boolean"], f"{label}: boolean {value}"


@pytest.mark.parametrize("name,graphs,requests", TESTS, ids=[t[0] for t in TESTS])
def test_w3c_protocol_query(name, graphs, requests):
    with serve(graphs) as server:
        for r in requests:
            status, headers, body = send(server, r)
            check(r, status, headers, body, "as given")
            status, headers, body = send(server, with_params(r, {"limit": 1000, "offset": 0}))
            check(r, status, headers, body, "with limit and offset")
            if is_select(r):
                status, headers, body = send(server, with_params(r, {"page_size": 1}))
                check(r, status, headers, body, "with page_size")
                if str(status)[0] == "2":
                    assert header(headers, "next-page-token") is None


def test_query_tests_are_all_found():
    # 12 query tests and 7 bad query tests.
    assert len(TESTS) == 19

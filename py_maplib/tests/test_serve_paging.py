"""Model.serve(): paging (page_size, page_token) and the limit and offset protocol parameters.

Model.serve() is part of maplib Enterprise Edition, so these tests skip on the open-source build.
"""

import json
import urllib.error
import urllib.parse
import urllib.request

import pytest

from maplib import Model

EX = "http://example.org/"
QUERY = f"SELECT ?s ?v WHERE {{ ?s <{EX}v> ?v }} ORDER BY DESC(?v) ?s"
N = 95


@pytest.fixture(scope="module")
def server():
    m = Model()
    # Every value occurs twice, so pages end in ties.
    m.reads(
        "\n".join(f"<{EX}s{i:03d}> <{EX}v> {i // 2} ." for i in range(N)) + "\n",
        format="turtle",
    )
    try:
        s = m.serve(port=0, flight_port=None, cors_origins=["*"])
    except Exception as e:
        if "Enterprise Edition" in str(e):
            pytest.skip("Model.serve() needs maplib Enterprise Edition")
        raise
    with s:
        yield s


def get(server, method="GET", **params):
    q = urllib.parse.urlencode(params)
    headers = {"Accept": "application/sparql-results+json"}
    if method == "GET":
        req = urllib.request.Request(server.url + "?" + q, headers=headers)
    else:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        req = urllib.request.Request(server.url, data=q.encode(), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def solutions(body):
    return [(b["s"]["value"], b["v"]["value"]) for b in json.loads(body)["results"]["bindings"]]


def header(headers, name):
    for k, v in headers.items():
        if k.lower() == name:
            return v
    return None


def full(server):
    status, _, body = get(server, query=QUERY)
    assert status == 200
    return solutions(body)


def test_full_result(server):
    assert len(full(server)) == N


@pytest.mark.parametrize("method", ["GET", "POST"])
@pytest.mark.parametrize("page_size", [1, 10, 94, 95, 200])
def test_pages_with_tokens(server, method, page_size):
    expected = full(server)
    got = []
    token = None
    pages = 0
    while True:
        params = {"query": QUERY, "page_size": page_size}
        if token is not None:
            params["page_token"] = token
        status, headers, body = get(server, method, **params)
        assert status == 200, body
        page = solutions(body)
        pages += 1
        got.extend(page)
        token = header(headers, "next-page-token")
        if token is None:
            break
        assert len(page) == page_size
    assert got == expected
    assert pages == -(-N // page_size)


def test_token_without_page_size_uses_default(server):
    _, headers, _ = get(server, query=QUERY, page_size=5)
    status, headers, body = get(server, query=QUERY, page_token=header(headers, "next-page-token"))
    assert status == 200
    assert len(solutions(body)) == N - 5
    assert header(headers, "next-page-token") is None


def test_limit_and_offset(server):
    f = full(server)
    _, _, body = get(server, query=QUERY, limit=7, offset=3)
    assert solutions(body) == f[3:10]
    # They apply after the LIMIT and OFFSET of the query.
    _, _, body = get(server, query=QUERY + " LIMIT 20 OFFSET 10", limit=100, offset=5)
    assert solutions(body) == f[15:30]
    _, _, body = get(server, query=QUERY + " LIMIT 20", limit=5)
    assert solutions(body) == f[0:5]
    # With paging, they bound the paged result as a whole.
    _, headers, body = get(server, query=QUERY, limit=12, offset=4, page_size=10)
    assert solutions(body) == f[4:14]
    _, headers, body = get(
        server, query=QUERY, limit=12, offset=4, page_size=10,
        page_token=header(headers, "next-page-token"),
    )
    assert solutions(body) == f[14:16]
    assert header(headers, "next-page-token") is None


def test_construct_limit(server):
    q = f"CONSTRUCT {{ ?s <{EX}v> ?v }} WHERE {{ ?s <{EX}v> ?v }}"
    req = urllib.request.Request(
        server.url + "?" + urllib.parse.urlencode({"query": q, "limit": 3}),
        headers={"Accept": "application/n-triples"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        assert len([l for l in r.read().decode().splitlines() if l.strip()]) == 3


@pytest.mark.parametrize(
    "params",
    [{"page_size": "0"}, {"page_size": "-1"}, {"limit": "x"}, {"page_token": "garbage"}],
)
def test_bad_parameters(server, params):
    status, _, body = get(server, query=QUERY, **params)
    assert status == 400, body


def test_token_of_another_query(server):
    _, headers, _ = get(server, query=QUERY, page_size=5)
    status, _, body = get(
        server, query=QUERY + " LIMIT 50", page_token=header(headers, "next-page-token")
    )
    assert status == 400
    assert b"another query" in body


def test_paging_ask_is_refused(server):
    status, _, _ = get(server, query="ASK {}", page_size=5)
    assert status == 400


def test_cors_exposes_the_token_header(server):
    req = urllib.request.Request(
        server.url + "?" + urllib.parse.urlencode({"query": QUERY, "page_size": 5}),
        headers={"Accept": "application/sparql-results+json", "Origin": "http://example.com"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        exposed = (r.headers.get("Access-Control-Expose-Headers") or "").lower()
        assert "next-page-token" in exposed


def test_ask_with_limit_and_offset(server):
    # ASK has solution modifiers: it is true when a solution is left after OFFSET.
    ask = f"ASK {{ ?s <{EX}v> ?v }}"
    for offset, expected in [(0, True), (N - 1, True), (N, False)]:
        status, _, body = get(server, query=ask, offset=offset, limit=1)
        assert status == 200, body
        assert json.loads(body)["boolean"] is expected, offset

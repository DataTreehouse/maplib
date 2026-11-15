"""
Local SPARQL endpoints for the federated query (SERVICE) and LOAD tests.

``MockEndpoints`` runs one HTTP server on 127.0.0.1 with any number of endpoints, each a
maplib ``Model`` at its own path, e.g. ``http://127.0.0.1:<port>/example1.org/sparql``.
Endpoints answer the SPARQL 1.1 Protocol (GET ``?query=``, POST ``application/sparql-query`` and
form POST) with SPARQL JSON, XML or TSV results, chosen per endpoint, and record the queries
they receive, so that tests can check how the bind join batched them. Files added with
``add_file`` are served at their path (for LOAD), with a media type, or none.

Nothing is sent over the network beyond the loopback interface.
"""

import json
import threading
import xml.sax.saxutils as saxutils
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from maplib import Model


def _json_to_xml(results: dict) -> str:
    esc = saxutils.escape
    out = ['<?xml version="1.0"?>', '<sparql xmlns="http://www.w3.org/2005/sparql-results#">']
    out.append("<head>")
    for v in results["head"].get("vars", []):
        out.append(f'<variable name="{esc(v)}"/>')
    out.append("</head>")
    if "boolean" in results:
        out.append(f"<boolean>{'true' if results['boolean'] else 'false'}</boolean>")
    else:
        out.append("<results>")
        for b in results["results"]["bindings"]:
            out.append("<result>")
            for k, t in b.items():
                out.append(f'<binding name="{esc(k)}">')
                if t["type"] == "uri":
                    out.append(f"<uri>{esc(t['value'])}</uri>")
                elif t["type"] == "bnode":
                    out.append(f"<bnode>{esc(t['value'])}</bnode>")
                else:
                    attrs = ""
                    if "xml:lang" in t:
                        attrs = f' xml:lang="{esc(t["xml:lang"])}"'
                    elif "datatype" in t:
                        attrs = f' datatype="{esc(t["datatype"])}"'
                    out.append(f"<literal{attrs}>{esc(t['value'])}</literal>")
                out.append("</binding>")
            out.append("</result>")
        out.append("</results>")
    out.append("</sparql>")
    return "\n".join(out)


def _tsv_term(t: Optional[dict]) -> str:
    if t is None:
        return ""
    if t["type"] == "uri":
        return f"<{t['value']}>"
    if t["type"] == "bnode":
        return f"_:{t['value']}"
    v = (
        t["value"]
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    if "xml:lang" in t:
        return f'"{v}"@{t["xml:lang"]}'
    if "datatype" in t:
        return f'"{v}"^^<{t["datatype"]}>'
    return f'"{v}"'


def _json_to_tsv(results: dict) -> str:
    variables = results["head"].get("vars", [])
    lines = ["\t".join("?" + v for v in variables)]
    for b in results["results"]["bindings"]:
        lines.append("\t".join(_tsv_term(b.get(v)) for v in variables))
    return "\n".join(lines) + "\n"


MEDIA_TYPES = {
    "json": "application/sparql-results+json",
    "xml": "application/sparql-results+xml",
    "tsv": "text/tab-separated-values",
}


class Endpoint:
    def __init__(self, model: Model, fmt: str):
        self.model = model
        self.format = fmt
        self.queries: List[str] = []
        self.fail_status: Optional[int] = None
        self.delay: float = 0.0
        self.headers_seen: List[Dict[str, str]] = []


class MockEndpoints:
    def __init__(self):
        self.endpoints: Dict[str, Endpoint] = {}
        self.files: Dict[str, tuple] = {}
        self.lock = threading.Lock()
        mock = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _respond(self, status: int, body: bytes, media_type: Optional[str]):
                self.send_response(status)
                if media_type is not None:
                    self.send_header("Content-Type", media_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _query(self, path: str, query: Optional[str]):
                ep = mock.endpoints.get(path)
                if ep is None:
                    return self._respond(404, b"No such endpoint", "text/plain")
                with mock.lock:
                    ep.queries.append(query or "")
                    ep.headers_seen.append(dict(self.headers))
                if ep.delay:
                    import time

                    time.sleep(ep.delay)
                if ep.fail_status is not None:
                    return self._respond(ep.fail_status, b"Failing on purpose", "text/plain")
                if query is None:
                    return self._respond(400, b"No query", "text/plain")
                try:
                    res = json.loads(ep.model.query(query, return_format="json"))
                except Exception as e:
                    return self._respond(400, str(e).encode(), "text/plain")
                if ep.format == "json":
                    body = json.dumps(res)
                elif ep.format == "xml":
                    body = _json_to_xml(res)
                else:
                    body = _json_to_tsv(res)
                self._respond(200, body.encode("utf-8"), MEDIA_TYPES[ep.format])

            def do_GET(self):
                u = urlparse(self.path)
                if u.path in mock.files:
                    body, media_type = mock.files[u.path]
                    return self._respond(200, body, media_type)
                q = parse_qs(u.query).get("query", [None])[0]
                self._query(u.path, q)

            def do_POST(self):
                u = urlparse(self.path)
                n = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(n).decode("utf-8")
                ct = (self.headers.get("Content-Type") or "").split(";")[0].strip()
                if ct == "application/x-www-form-urlencoded":
                    q = parse_qs(body).get("query", [None])[0]
                else:
                    q = body
                self._query(u.path, q)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def add(self, name: str, model: Model, fmt: str = "json") -> str:
        """Adds an endpoint at /<name>/sparql, returns its URL."""
        path = f"/{name}/sparql"
        self.endpoints[path] = Endpoint(model, fmt)
        return self.base + path

    def endpoint(self, url: str) -> Endpoint:
        return self.endpoints[urlparse(url).path]

    def add_file(self, path: str, body: bytes, media_type: Optional[str]) -> str:
        self.files[path] = (body, media_type)
        return self.base + path

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

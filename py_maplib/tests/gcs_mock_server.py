"""A minimal in-process stand-in for the Google Cloud Storage XML API, as object_store uses it:
PUT, GET, HEAD and DELETE of objects with the generation preconditions
(``x-goog-if-generation-match``, 0 meaning "does not exist"), ``x-goog-generation`` and ETag
headers, and ``list-type=2`` listings. fake-gcs-server does not route the percent-encoded object
paths object_store sends to the XML API, see object_storage_emulators.py, so the conditional
puts of maplib.storage are tested against this server instead. Real GCS is tested with
MAPLIB_TEST_GCS_URL.

Use the storage options of ``GcsMock.storage_options``: ``google_base_url`` points
object_store at the server, and a static ``google_bearer_token`` replaces credentials.
"""

import hashlib
import threading
from email.utils import formatdate
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit
from xml.sax.saxutils import escape


class _Object:
    def __init__(self, data: bytes, generation: int):
        self.data = data
        self.generation = generation
        self.etag = '"' + hashlib.md5(data).hexdigest() + '"'
        self.modified = datetime.now(timezone.utc)


class GcsMock:
    def __init__(self):
        self.objects = {}  # (bucket, key) -> _Object
        self.lock = threading.Lock()
        self.generation = 1_700_000_000_000_000
        self.requests = []  # (method, path, if-generation-match header)
        mock = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _target(self):
                parts = urlsplit(self.path)
                bucket, _, key = parts.path.lstrip("/").partition("/")
                return unquote(bucket), unquote(key), parse_qs(parts.query)

            def _send(self, status, body=b"", headers=None):
                self.send_response(status)
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)

            def _meta(self, o):
                return {
                    "ETag": o.etag,
                    "x-goog-generation": str(o.generation),
                    "Last-Modified": formatdate(o.modified.timestamp(), usegmt=True),
                }

            def do_PUT(self):
                bucket, key, _ = self._target()
                data = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                match = self.headers.get("x-goog-if-generation-match")
                mock.requests.append(("PUT", key, match))
                with mock.lock:
                    current = mock.objects.get((bucket, key))
                    if match is not None:
                        ok = current is None if match == "0" else (
                            current is not None and str(current.generation) == match)
                        if not ok:
                            return self._send(412, b"<Error><Code>PreconditionFailed</Code></Error>")
                    mock.generation += 1
                    o = _Object(data, mock.generation)
                    mock.objects[(bucket, key)] = o
                self._send(200, headers=self._meta(o))

            def do_GET(self):
                bucket, key, query = self._target()
                if not key and "list-type" in query:
                    return self._list(bucket, query.get("prefix", [""])[0])
                with mock.lock:
                    o = mock.objects.get((bucket, key))
                if o is None:
                    return self._send(404, b"<Error><Code>NoSuchKey</Code></Error>")
                self._send(200, o.data, self._meta(o))

            do_HEAD = do_GET

            def do_DELETE(self):
                bucket, key, _ = self._target()
                with mock.lock:
                    found = mock.objects.pop((bucket, key), None)
                self._send(204 if found else 404)

            def _list(self, bucket, prefix):
                with mock.lock:
                    items = sorted((k, o) for (b, k), o in mock.objects.items()
                                   if b == bucket and k.startswith(prefix))
                contents = "".join(
                    f"<Contents><Key>{escape(k)}</Key><Size>{len(o.data)}</Size>"
                    f"<LastModified>{o.modified.strftime('%Y-%m-%dT%H:%M:%S.000Z')}</LastModified>"
                    f"<ETag>{escape(o.etag)}</ETag></Contents>"
                    for k, o in items
                )
                body = ('<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>'
                        f"{contents}</ListBucketResult>").encode()
                self._send(200, body, {"Content-Type": "application/xml"})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def storage_options(self):
        return {
            "google_base_url": f"http://127.0.0.1:{self.server.server_port}",
            "google_bearer_token": "test-token",
            "allow_http": "true",
        }

    def stop(self):
        self.server.shutdown()
        self.server.server_close()

"""Object storage backends for tests:

- "file": file:// URLs in a temporary folder.
- "azure": Azure Blob Storage, by default the Azurite emulator run in Docker
  (mcr.microsoft.com/azure-storage/azurite), or a real container when MAPLIB_TEST_AZURE_URL
  is set (e.g. az://container/prefix, credentials from the environment, options from
  MAPLIB_TEST_AZURE_OPTIONS as a JSON object).
- "gcs": Google Cloud Storage, only when MAPLIB_TEST_GCS_URL is set (e.g. gs://bucket/prefix,
  credentials from the environment or the metadata server, options from
  MAPLIB_TEST_GCS_OPTIONS). There is no usable emulator: fake-gcs-server does not route the
  percent-encoded object paths that object_store sends to its XML API, nor implement XML
  multipart uploads.

Azurite is started once per test session on a free port. Tests of a backend that is not
available (no Docker, or MAPLIB_TEST_NO_EMULATORS is set) are skipped.
"""

import base64
import datetime
import hashlib
import hmac
import json
import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

import pytest

AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite"
AZURITE_ACCOUNT = "devstoreaccount1"
AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)
CONTAINER = "maplib-test"
BACKENDS = ["file", "azure", "gcs"]


@dataclass
class Backend:
    name: str
    root: str
    storage_options: Dict[str, str] = field(default_factory=dict)
    local_root: Optional[Path] = None

    def url(self, *parts: str) -> str:
        return "/".join([self.root.rstrip("/"), *parts])

    def unique(self, name: str) -> str:
        """A URL under a fresh prefix, so that tests do not see each other's objects."""
        return self.url(uuid.uuid4().hex[:12], name)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _docker(args, timeout, check=False) -> subprocess.CompletedProcess:
    # In a session of its own, so that the docker CLI touching a terminal can not stop the
    # process group of the tests.
    return subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        check=check,
        timeout=timeout,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )


def _docker_available() -> bool:
    if os.environ.get("MAPLIB_TEST_NO_EMULATORS") or shutil.which("docker") is None:
        return False
    try:
        return _docker(["info"], timeout=30).returncode == 0
    except subprocess.TimeoutExpired:
        return False


def _wait_for(url: str, timeout: float = 60) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=2)
            return
        except urllib.error.HTTPError:
            return  # The server answers.
        except Exception:
            time.sleep(0.3)
    raise RuntimeError(f"{url} did not start")


def _azure_create_container(endpoint: str) -> None:
    """Creates the test container in Azurite with a Shared Key signed request."""
    date = datetime.datetime.now(datetime.timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    version = "2021-08-06"
    canonical_headers = f"x-ms-date:{date}\nx-ms-version:{version}\n"
    canonical_resource = f"/{AZURITE_ACCOUNT}/{AZURITE_ACCOUNT}/{CONTAINER}\nrestype:container"
    string_to_sign = "PUT\n" + "\n" * 11 + canonical_headers + canonical_resource
    signature = base64.b64encode(
        hmac.new(base64.b64decode(AZURITE_KEY), string_to_sign.encode(), hashlib.sha256).digest()
    ).decode()
    req = urllib.request.Request(
        f"{endpoint}/{AZURITE_ACCOUNT}/{CONTAINER}?restype=container",
        method="PUT",
        headers={
            "x-ms-date": date,
            "x-ms-version": version,
            "Authorization": f"SharedKey {AZURITE_ACCOUNT}:{signature}",
            "Content-Length": "0",
        },
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as e:
        if e.code != 409:  # Already exists
            raise


def _from_env(name: str, url_var: str, options_var: str) -> Optional[Backend]:
    url = os.environ.get(url_var)
    if not url:
        return None
    return Backend(name, url, json.loads(os.environ.get(options_var, "{}")))


class Emulators:
    def __init__(self, tmp_root: Path):
        self.tmp_root = tmp_root
        self.containers = []
        self.backends: Dict[str, Optional[Backend]] = {}
        self.errors: Dict[str, str] = {}

    def get(self, name: str) -> Backend:
        if name not in self.backends:
            try:
                self.backends[name] = getattr(self, f"_start_{name}")()
            except Exception as e:  # pragma: no cover - depends on the machine
                self.backends[name] = None
                self.errors[name] = str(e)
        backend = self.backends[name]
        if backend is None:
            pytest.skip(f"{name} object storage is not available: {self.errors.get(name)}")
        return backend

    def _start_file(self) -> Backend:
        root = self.tmp_root / "file_store"
        root.mkdir(parents=True, exist_ok=True)
        return Backend("file", f"file://{root.as_posix()}", {}, local_root=root)

    def _start_azure(self) -> Backend:
        real = _from_env("azure", "MAPLIB_TEST_AZURE_URL", "MAPLIB_TEST_AZURE_OPTIONS")
        if real is not None:
            return real
        if not _docker_available():
            raise RuntimeError("Docker is not available and MAPLIB_TEST_AZURE_URL is not set")
        port = _free_port()
        out = _docker(
            ["run", "-d", "--rm", "-p", f"127.0.0.1:{port}:10000", AZURITE_IMAGE,
             "azurite-blob", "--blobHost", "0.0.0.0", "--skipApiVersionCheck", "--loose"],
            timeout=120,
            check=True,
        )
        self.containers.append(out.stdout.strip())
        endpoint = f"http://127.0.0.1:{port}"
        _wait_for(endpoint)
        _azure_create_container(endpoint)
        # The emulator mode of object_store finds Azurite through this variable.
        os.environ["AZURITE_BLOB_STORAGE_URL"] = endpoint
        return Backend("azure", f"az://{CONTAINER}", {"azure_storage_use_emulator": "true"})

    def _start_gcs(self) -> Backend:
        real = _from_env("gcs", "MAPLIB_TEST_GCS_URL", "MAPLIB_TEST_GCS_OPTIONS")
        if real is None:
            raise RuntimeError("MAPLIB_TEST_GCS_URL is not set (there is no usable GCS emulator)")
        return real

    def stop(self) -> None:
        for c in self.containers:
            try:
                _docker(["stop", "-t", "2", c], timeout=60)
            except subprocess.TimeoutExpired:
                pass


@pytest.fixture(scope="session")
def emulators(tmp_path_factory):
    e = Emulators(tmp_path_factory.mktemp("object_storage"))
    yield e
    e.stop()


@pytest.fixture(params=BACKENDS)
def backend(request, emulators) -> Backend:
    return emulators.get(request.param)

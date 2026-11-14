"""Download the reference HDT files used by ``test_hdt_compliance.py``.

They were written by other HDT implementations and are fetched at pinned commits (and
checked against pinned SHA-256 hashes) rather than vendored, as their licences differ from
maplib's. They go to ``testdata/hdt_reference/`` (git ignored).

    python fetch_hdt_reference.py
"""

import hashlib
import sys
import urllib.request
from pathlib import Path

TARGET = Path(__file__).parent / "testdata" / "hdt_reference"

# name: (repository, commit, path, sha256, licence)
FILES = {
    # Written by hdt-java (rdfhdt/hdt-java), 320,771 triples from DBpedia.
    "dbpedia.hdt": (
        "rdfhdt/hdt-java",
        "43a55a6ec1fb9c549253f51cb761f13f7f3a5bbb",
        "hdt-java-core/src/test/resources/dbpedia.hdt",
        "3d8d686ccaa05734b757adbeff38db4d87a5f768b3b61081edf53fb0940eaa97",
        "LGPL-3.0",
    ),
    # Written by hdt-cpp (rdfhdt/hdt-cpp), with the optional literal dictionary.
    "literals.hdt": (
        "rdfhdt/hdt-cpp",
        "b80a9243a3663af53658d149d1067a908036a3ba",
        "libhdt/data/literals.hdt",
        "bcbec8f5d00a3cdd1027acbfcdd4c7a6a88ae3b9890be96e39266079bfd71eb0",
        "LGPL-2.1",
    ),
    # Written by hdt-cpp, from the test resources of the hdt crate (KonradHoeffner/hdt).
    # Has blank nodes.
    "snikmeta.hdt": (
        "KonradHoeffner/hdt",
        "a06fd3ce107e8803a42ff4f5be530b5cd5a86769",
        "tests/resources/snikmeta.hdt",
        "c15905ba0e2e3dde5e656a72c7c04ae851c63f0acff3ed8059efbd7906afa054",
        "MIT",
    ),
}


def fetch(target: Path = TARGET) -> Path:
    target.mkdir(parents=True, exist_ok=True)
    for name, (repository, commit, path, sha256, _licence) in FILES.items():
        out = target / name
        if out.exists() and hashlib.sha256(out.read_bytes()).hexdigest() == sha256:
            continue
        url = f"https://raw.githubusercontent.com/{repository}/{commit}/{path}"
        with urllib.request.urlopen(url, timeout=120) as response:
            data = response.read()
        digest = hashlib.sha256(data).hexdigest()
        if digest != sha256:
            raise ValueError(f"{url}: SHA-256 {digest}, expected {sha256}")
        out.write_bytes(data)
    return target


if __name__ == "__main__":
    path = fetch(Path(sys.argv[1]) if len(sys.argv) > 1 else TARGET)
    print(f"Reference HDT files in {path}")

"""maplib.storage: raw bytes and conditional puts in object storage and local files. Runs
against file:// URLs, Azure Blob Storage (Azurite by default) and Google Cloud Storage (when
MAPLIB_TEST_GCS_URL is set), see object_storage_emulators.py, and against plain local paths."""

import threading
from datetime import datetime, timedelta, timezone

import pytest
from maplib import MaplibException, Model, storage

from .object_storage_emulators import Backend, backend, emulators  # noqa: F401


def _publish_cycle(url, opts):
    t1 = storage.put(url, b'{"v": 1}', if_none_match=True, storage_options=opts)
    assert t1
    with pytest.raises(storage.AlreadyExists):
        storage.put(url, b"x", if_none_match=True, storage_options=opts)

    data, t = storage.get(url, storage_options=opts)
    assert data == b'{"v": 1}'
    assert t == t1
    info = storage.head(url, storage_options=opts)
    assert info.size == len(data)
    assert info.version == t1
    assert info.name == "manifest.json"
    assert abs(info.last_modified - datetime.now(timezone.utc)) < timedelta(minutes=10)

    t2 = storage.put(url, b'{"v": 2}', if_match=t1, storage_options=opts)
    assert t2 and t2 != t1
    # A writer that read version 1 loses, and the object stays at version 2.
    with pytest.raises(storage.PreconditionFailed):
        storage.put(url, b'{"v": 3}', if_match=t1, storage_options=opts)
    assert storage.get(url, storage_options=opts) == (b'{"v": 2}', t2)

    # Unconditional puts replace the object.
    t3 = storage.put(url, b'{"v": 4}', storage_options=opts)
    assert storage.get(url, storage_options=opts) == (b'{"v": 4}', t3)

    storage.delete(url, storage_options=opts)
    storage.delete(url, storage_options=opts)  # deleting a missing object is fine
    assert not storage.exists(url, storage_options=opts)
    assert storage.head(url, storage_options=opts) is None
    with pytest.raises(storage.NotFound):
        storage.get(url, storage_options=opts)
    # An update of a deleted object fails.
    with pytest.raises(storage.PreconditionFailed):
        storage.put(url, b"x", if_match=t3, storage_options=opts)


def _listing(prefix, opts):
    storage.put(f"{prefix}/manifest.json", b"{}", storage_options=opts)
    storage.put(f"{prefix}/graphs/a.hdt", b"aa", storage_options=opts)
    storage.put(f"{prefix}/uploads/m1/nodesets.zip", b"zip", storage_options=opts)
    storage.put(f"{prefix}2/other", b"no", storage_options=opts)
    listed = storage.list(prefix, storage_options=opts)
    assert [(o.name, o.size) for o in listed] == [
        ("graphs/a.hdt", 2),
        ("manifest.json", 2),
        ("uploads/m1/nodesets.zip", 3),
    ]
    assert listed[1].url == f"{prefix}/manifest.json"
    assert listed[1].version == storage.head(listed[1].url, storage_options=opts).version
    assert storage.list(prefix + "/", storage_options=opts) == listed
    for o in listed:
        storage.delete(o.url, storage_options=opts)
    assert storage.list(prefix, storage_options=opts) == []
    assert storage.list(f"{prefix}/nothing/here", storage_options=opts) == []


def test_conditional_puts(backend: Backend):
    _publish_cycle(backend.unique("manifest.json"), backend.storage_options)


def test_list(backend: Backend):
    _listing(backend.unique("model"), backend.storage_options)


def test_hdt_snapshot_next_to_manifest(backend: Backend):
    """The layout of a writable portal: an HDT file per graph and a manifest naming them."""
    m = Model()
    m.reads("<http://ex/a> <http://ex/p> <http://ex/b> .", format="ntriples")
    prefix = backend.unique("model")
    m.write(f"{prefix}/graphs/default.hdt", format="hdt", storage_options=backend.storage_options)
    token = storage.put(f"{prefix}/manifest.json", b'["graphs/default.hdt"]', if_none_match=True,
                        storage_options=backend.storage_options)
    names = [o.name for o in storage.list(prefix, storage_options=backend.storage_options)]
    assert names == ["graphs/default.hdt", "manifest.json"]
    assert storage.head(f"{prefix}/manifest.json", storage_options=backend.storage_options).version == token


def test_local_paths(tmp_path):
    _publish_cycle(str(tmp_path / "new_folder" / "manifest.json"), None)
    _listing(str(tmp_path / "model"), None)
    # Relative paths, pathlike folders made as needed.
    p = tmp_path / "rel"
    p.mkdir()
    import os

    cwd = os.getcwd()
    os.chdir(p)
    try:
        storage.put("sub/x.bin", bytearray(b"\x00\x01"))
        assert storage.get("sub/x.bin")[0] == b"\x00\x01"
        storage.put("sub/y.bin", memoryview(b"mv"))
        assert storage.get(str(p / "sub" / "y.bin"))[0] == b"mv"
        assert storage.get((p / "sub" / "y.bin").as_uri())[0] == b"mv"
    finally:
        os.chdir(cwd)


def test_concurrent_local_writers(tmp_path):
    """Only one of the writers that read the same version publishes; the rest must merge."""
    url = str(tmp_path / "manifest.json")
    token = storage.put(url, b"0")
    results = []

    def writer(i):
        try:
            storage.put(url, str(i).encode(), if_match=token)
            results.append(i)
        except storage.PreconditionFailed:
            pass

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(1, 17)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(results) == 1
    assert storage.get(url)[0] == str(results[0]).encode()


def test_errors(tmp_path):
    with pytest.raises(storage.NotFound) as e:
        storage.get(str(tmp_path / "missing"))
    assert isinstance(e.value, FileNotFoundError)
    assert isinstance(e.value, storage.StorageError)
    assert isinstance(e.value, MaplibException)
    assert issubclass(storage.AlreadyExists, storage.PreconditionFailed)
    assert issubclass(storage.AlreadyExists, FileExistsError)
    with pytest.raises(ValueError):
        storage.put(str(tmp_path / "x"), b"", if_match="t", if_none_match=True)
    with pytest.raises(storage.StorageError, match="unknown storage option"):
        storage.get("gs://bucket/x", storage_options={"no_such_option": "1"})
    # Plain HTTP stores have no conditional puts. Fails before any request is made.
    with pytest.raises(storage.NotSupported):
        storage.put("http://127.0.0.1:9/x", b"", if_none_match=True)

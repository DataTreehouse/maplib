"""Dropped models free their threads.

Each open on-disk dictionary (a DuckDB database) has its own threads, one per core. Before
2026-11-19 the database of a dropped hybrid or disk model stayed open, so each model left about
14 threads behind, and a process that created a few hundred models (a server, a test suite) failed
with "failed to spawn thread".
"""
import gc
import os
import subprocess

import pytest

from maplib import Model

from .disk import disk_params

pytestmark = pytest.mark.skipif(
    os.uname().sysname not in ("Darwin", "Linux"), reason="counts the threads of the process"
)


def process_threads() -> int:
    if os.uname().sysname == "Linux":
        return len(os.listdir("/proc/self/task"))
    out = subprocess.check_output(["ps", "-M", "-p", str(os.getpid())]).decode()
    return out.strip().count("\n")


def use_model(storage: str, folder) -> None:
    kwargs = {"storage": storage}
    if storage != "memory":
        folder.parent.mkdir(parents=True, exist_ok=True)
        kwargs["storage_folder"] = str(folder)
    m = Model(**kwargs)
    m.reads('<http://a> <http://b> "c" .\n<http://a> <http://d> <http://e> .', format="ntriples")
    assert m.query("SELECT * WHERE { ?s ?p ?o }").height == 2
    m.update('INSERT DATA { <http://f> <http://b> "g" }')
    assert m.query('SELECT ?s WHERE { ?s <http://b> "g" }').height == 1


def threads_after(storage: str, tmp_path, n: int) -> int:
    for i in range(n):
        use_model(storage, tmp_path / f"s{i}")
    gc.collect()
    return process_threads()


@pytest.mark.parametrize("storage", disk_params())
def test_dropped_models_free_their_threads(storage, tmp_path):
    # The first model starts the threads that are shared by all models (Polars, rayon).
    before = threads_after(storage, tmp_path / "first", 1)
    after = threads_after(storage, tmp_path / "more", 20)
    assert after - before < 10, (before, after)


@pytest.mark.parametrize("storage", [s for s in disk_params() if s != "memory"])
def test_many_dropped_models_stay_under_a_thread_ceiling(storage, tmp_path):
    before = threads_after(storage, tmp_path / "first", 1)
    after = threads_after(storage, tmp_path / "more", 300)
    assert after - before < 20, (before, after)

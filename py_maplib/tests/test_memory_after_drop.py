"""Dropping a model gives its memory back to the operating system, also when nothing is allocated
afterwards: the allocator would otherwise keep the freed pages, and loading and dropping models
again and again would grow the process."""
import gc
import os
import subprocess

from maplib import Model


def rss_mb() -> float:
    try:
        import psutil

        return psutil.Process().memory_info().rss / 2**20
    except ImportError:
        pass
    if os.path.exists("/proc/self/statm"):
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 2**20
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(os.getpid())], capture_output=True, text=True)
    return int(out.stdout.strip()) / 1024


def test_dropped_model_gives_memory_back(tmp_path):
    path = tmp_path / "triples.nt"
    with open(path, "w") as f:
        for i in range(300_000):
            f.write(f'<http://example.net/s{i}> <http://example.net/p{i % 7}> "value {i}" .\n')
    # Reading a file this large purges the allocator, which also gives back what earlier tests in
    # the same process freed; read and drop once first so the baseline is measured after that purge.
    warm_up = Model()
    warm_up.read(path)
    del warm_up
    gc.collect()
    base = rss_mb()
    m = Model()
    m.read(path)
    loaded = rss_mb() - base
    del m
    gc.collect()
    dropped = rss_mb() - base
    assert loaded > 20, f"the model should take memory ({loaded:.0f} MB)"
    assert dropped < 0.5 * loaded, (
        f"{dropped:.0f} MB of the {loaded:.0f} MB the model took are still held after dropping it"
    )

"""Model.query(max_memory_mb=) and maplib.set_memory_limit(max_query_memory_mb=): a query that
produces far more rows than fit in memory is stopped before it exhausts it.

The runaway queries run in a child process, watched from here: its memory is sampled every few
milliseconds and it is killed above a hard ceiling, so a regression fails the test instead of
taking the machine's memory.
"""

import ctypes
import os
import subprocess
import sys
import textwrap
import time

import pytest

from maplib import MaplibException, Model, QueryMemoryLimitError

# The child is killed when its memory goes above this. Without the limit, the join below makes
# 300 million rows (a few GB within a second) and passes it.
HARD_CEILING_MB = 1500

MODEL = """
import sys, time
import polars as pl
import maplib
from maplib import Model

n = {n}
m = Model()
m.map_triples(
    pl.DataFrame({{
        "subject": [f"http://ex.org/s{{i}}" for i in range(n)],
        "object": [f"http://ex.org/k{{i % 3}}" for i in range(n)],
    }}),
    predicate="http://ex.org/kind",
)
# Every subject with every subject of the same kind: n * n / 3 rows.
JOIN = "PREFIX ex:<http://ex.org/> SELECT ?s ?t WHERE {{ ?s ex:kind ?k . ?t ex:kind ?k }}"
print("ready", flush=True)
"""


def _footprint_mb(pid):
    """The memory of a process in MB: its physical footprint on macOS (counting compressed
    memory, which RSS misses), its resident and swapped memory on Linux."""
    if sys.platform == "darwin":

        class RUsage(ctypes.Structure):
            _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [
                (f, ctypes.c_uint64)
                for f in (
                    "user_time system_time pkg_idle_wkups interrupt_wkups pageins "
                    "wired_size resident_size phys_footprint"
                ).split()
            ] + [("rest", ctypes.c_uint64 * 40)]

        r = RUsage()
        libproc = ctypes.CDLL("libproc.dylib")
        if libproc.proc_pid_rusage(pid, 2, ctypes.byref(r)) != 0:
            return 0
        return r.phys_footprint / 1e6
    try:
        kb = 0
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith(("VmRSS:", "VmSwap:")):
                    kb += int(line.split()[1])
        return kb * 1024 / 1e6
    except OSError:
        return 0


def _run_capped(script, timeout_s=60):
    """Runs `script` in a child process, killing it above HARD_CEILING_MB. Returns its stdout and
    the peak memory seen in MB, measured from when the child printed "ready"."""
    p = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        # Without RUST_LOG, which CI sets to debug, so the child's first line is its own.
        env={k: v for k, v in os.environ.items() if k != "RUST_LOG"},
    )
    before = []
    for line in p.stdout:
        if line.strip() == "ready":
            break
        before.append(line)
    else:
        p.wait()
        raise AssertionError(f"the child exited ({p.returncode}) before it was ready:\n" + "".join(before))
    peak = 0.0
    started = time.monotonic()
    killed = False
    while p.poll() is None:
        mb = _footprint_mb(p.pid)
        peak = max(peak, mb)
        if mb > HARD_CEILING_MB or time.monotonic() - started > timeout_s:
            p.kill()
            killed = True
            break
        time.sleep(0.002)
    out = p.stdout.read()
    p.wait()
    assert not killed, f"killed with {peak:.0f} MB after {time.monotonic() - started:.1f} s\n{out}"
    assert p.returncode == 0, out
    return out, peak


@pytest.mark.skipif(
    sys.platform not in ("darwin", "linux"), reason="reads the child's memory from the OS"
)
@pytest.mark.parametrize(
    "call",
    [
        "m.query(JOIN, max_memory_mb=200)",
        "m.query(JOIN, max_memory_mb=200, timeout=60)",
        "m.query(JOIN, max_memory_mb=200, max_rows=10**12)",
        # The default of every query, as for a served model.
        "maplib.set_memory_limit(max_query_memory_mb=200); m.query(JOIN)",
        "m.update('PREFIX ex:<http://ex.org/> INSERT { ?s ex:same ?t } WHERE { ?s ex:kind ?k . ?t ex:kind ?k }', max_memory_mb=200)",
    ],
)
def test_runaway_join_stops_at_its_memory_limit(call):
    script = MODEL.format(n=30_000) + textwrap.dedent(
        f"""
        try:
            {call}
            print("no error")
        except maplib.QueryMemoryLimitError as e:
            print("stopped:", e)
        # The stopped query does not go on in the background: the model answers at once.
        t = time.monotonic()
        assert m.query("SELECT (COUNT(*) AS ?c) WHERE {{ ?a ?b ?c }}")["c"][0] == n
        print("answered in", round(time.monotonic() - t, 3))
        """
    )
    out, peak = _run_capped(script)
    assert "stopped: The query was stopped as it used more than its memory limit of 200 MB" in out, out
    # The limit counts growth from the start of the query; the batches in flight when it is
    # noticed, the model and the Python process come on top.
    assert peak < 1000, f"peak {peak:.0f} MB\n{out}"


@pytest.mark.skipif(
    sys.platform not in ("darwin", "linux"), reason="reads the child's memory from the OS"
)
def test_runaway_join_stops_at_its_timeout_without_crashing():
    # A join that is estimated to be small (its inputs have 6000 rows) and gives 12 million rows:
    # it runs on the streaming engine when the query has a timeout, so it stops on time. The
    # process used to abort when a cancelled in-memory collect finished after its query had
    # given up on it.
    script = MODEL.format(n=6_000) + textwrap.dedent(
        """
        t = time.monotonic()
        try:
            m.query(JOIN + " ORDER BY ?s ?t", timeout=0.05)
            print("no error")
        except maplib.QueryTimeoutError as e:
            print("stopped after", round(time.monotonic() - t, 3))
        time.sleep(0.5)
        assert m.query("SELECT (COUNT(*) AS ?c) WHERE { ?a ?b ?c }")["c"][0] == n
        print("answered")
        """
    )
    out, _ = _run_capped(script)
    assert "stopped after" in out, out
    assert float(out.split("stopped after")[1].split()[0]) < 1.0, out
    assert "answered" in out, out


def test_queries_within_their_memory_limit_answer():
    m = Model()
    m.update("INSERT DATA { <http://example.org/a> <http://example.org/p> <http://example.org/b> }")
    df = m.query("SELECT ?a WHERE { ?a ?p ?b }", max_memory_mb=1000)
    assert df["a"].to_list() == ["<http://example.org/a>"]
    changes = m.update(
        "INSERT { ?b <http://example.org/inv> ?a } WHERE { ?a <http://example.org/p> ?b }",
        max_memory_mb=1000,
    )
    assert sum(c["inserted"] for c in changes.values()) == 1


def test_memory_limit_error_is_a_maplib_exception():
    assert issubclass(QueryMemoryLimitError, MaplibException)


@pytest.mark.parametrize("bad", [0, -1, float("nan"), float("inf")])
def test_invalid_memory_limits(bad):
    with pytest.raises(Exception, match="max_memory_mb"):
        Model().query("SELECT ?a WHERE { ?a ?b ?c }", max_memory_mb=bad)

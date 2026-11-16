"""A query with a join far larger than its height estimate must stop cleanly on a timeout or a
memory limit.

    ?s ex:kind ?k . ?t ex:kind ?k

?k has 3 values, so the join has n * n / 3 rows, while it was estimated at 1.4 * n. With a small
estimate the join ran on Polars' in-memory engine, which can not stop a running join: a timed out
query returned, but the join went on, and when it ended Polars sent its result to the receiver
that maplib had dropped, which aborted the process. With ``abort_above_mb`` and no timeout, the
process was aborted instead of the query raising a MemoryError. Each case runs in a child
process, as memory limits apply to the whole process.
"""

import subprocess
import sys
import textwrap

N = 15_000

CHILD = textwrap.dedent(
    """
    import sys
    import time
    import polars as pl
    import maplib

    n, case = int(sys.argv[1]), sys.argv[2]
    ex = "http://ex.org/"
    m = maplib.Model()
    m.map_triples(
        pl.DataFrame(
            {
                "subject": [f"{ex}s{i}" for i in range(n)],
                "object": [f"{ex}k{i % 3}" for i in range(n)],
            }
        ),
        predicate=ex + "kind",
    )
    q = f"PREFIX ex: <{ex}> SELECT ?s ?t WHERE {{ ?s ex:kind ?k . ?t ex:kind ?k }}"
    if case == "timeout":
        try:
            m.query(q, timeout=0.005)
            print("finished", flush=True)
        except Exception as e:
            print("error", type(e).__name__, flush=True)
        # The join must not go on after the query returned.
        time.sleep(1.0)
        print("memory_mb", maplib.memory_usage() // 1_000_000, flush=True)
    else:
        maplib.set_memory_limit(abort_above_mb=int(case))
        try:
            m.query(q)
            print("finished", flush=True)
        except Exception as e:
            print("error", type(e).__name__, flush=True)
    small = m.query(f"PREFIX ex: <{ex}> SELECT ?s WHERE {{ ?s ex:kind ex:k1 }}")
    print("rows", small.height, flush=True)
    """
)


def run(case):
    p = subprocess.run(
        [sys.executable, "-c", CHILD, str(N), case],
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert p.returncode == 0, f"exit {p.returncode}\n{p.stdout}\n{p.stderr[-2000:]}"
    return dict(line.split(" ", 1) for line in p.stdout.splitlines())


def test_timed_out_large_join_stops():
    lines = run("timeout")
    assert lines["error"] == "QueryTimeoutError"
    assert int(lines["memory_mb"]) < 200
    assert lines["rows"] == str(N // 3)


def test_memory_limit_stops_a_large_join_without_aborting():
    # The join's result needs over 0.6 GB. The query is stopped at 90% of the limit, checked between
    # batches of the join, so the limit leaves room for the memory used between two checks.
    lines = run("500")
    assert lines["error"] == "MemoryError"
    assert lines["rows"] == str(N // 3)

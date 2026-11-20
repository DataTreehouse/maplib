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

import os
import subprocess
import sys
import textwrap

# Without RUST_LOG, which CI sets to debug: logging every step of the join slows it enough between
# memory checks for Polars' own abort threshold to fire first.
_CHILD_ENV = {k: v for k, v in os.environ.items() if k != "RUST_LOG"}

N = 15_000
# Times the memory case runs the large join, each followed by a small query.
ROUNDS = 4
# The memory limit of a query of its own, far below the process limit of the memory case.
OWN_LIMIT_MB = 100

CHILD = textwrap.dedent(
    """
    import sys
    import time
    import polars as pl
    import maplib

    n, case, rounds = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])
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
    small = f"PREFIX ex: <{ex}> SELECT ?s WHERE {{ ?s ex:kind ex:k1 }}"

    def big(**kwargs):
        try:
            m.query(q, **kwargs)
            print("finished", flush=True)
        except Exception as e:
            print("error", type(e).__name__, flush=True)

    if case.startswith("own:"):
        # The large join is stopped by the process limit, and then run again right away with a
        # limit of its own, which must stop it, though the first join may still be freeing its
        # memory when the second starts.
        _, process_mb, own_mb = case.split(":")
        maplib.set_memory_limit(abort_above_mb=int(process_mb))
        for _ in range(rounds):
            big()
            big(max_memory_mb=float(own_mb))
            print("rows", m.query(small).height, flush=True)
    elif case == "timeout":
        big(timeout=0.005)
        # The join must not go on after the query returned.
        time.sleep(1.0)
        print("memory_mb", maplib.memory_usage() // 1_000_000, flush=True)
        print("rows", m.query(small).height, flush=True)
    else:
        maplib.set_memory_limit(abort_above_mb=int(case))
        for _ in range(rounds):
            big()
            # The stopped join may still be freeing its memory, which the next query waits for
            # instead of being stopped at once.
            try:
                print("rows", m.query(small).height, flush=True)
            except Exception as e:
                print("small_error", type(e).__name__, e, flush=True)
    """
)


def run(case):
    p = subprocess.run(
        [sys.executable, "-c", CHILD, str(N), case, str(ROUNDS)],
        capture_output=True,
        text=True,
        timeout=600,
        env=_CHILD_ENV,
    )
    assert p.returncode == 0, f"exit {p.returncode}\n{p.stdout}\n{p.stderr[-2000:]}"
    lines = {}
    for line in p.stdout.splitlines():
        key, value = line.split(" ", 1)
        lines.setdefault(key, []).append(value)
    return lines


def test_timed_out_large_join_stops():
    lines = run("timeout")
    assert lines["error"] == ["QueryTimeoutError"]
    assert int(lines["memory_mb"][0]) < 200
    assert lines["rows"] == [str(N // 3)]


def test_memory_limit_stops_a_large_join_without_aborting():
    # The join's result needs over 0.6 GB. The query is stopped at 90% of the limit, checked between
    # batches of the join, so the limit leaves room for the memory used between two checks. The
    # small query after each stopped join must succeed, though the join may still be freeing its
    # memory when that query starts.
    lines = run("500")
    assert "small_error" not in lines, lines["small_error"]
    assert lines["error"] == ["MemoryError"] * ROUNDS
    assert lines["rows"] == [str(N // 3)] * ROUNDS


def test_own_limit_of_a_query_right_after_a_stopped_one():
    # A query's own limit counts its growth from the least memory seen since it started. It
    # counted from when the query was made, which could be while the stopped join was still
    # freeing over 400 MB, so the second join was not stopped by its own limit of 100 MB but by
    # the process limit, with a MemoryError.
    lines = run(f"own:500:{OWN_LIMIT_MB}")
    assert lines["error"] == ["MemoryError", "QueryMemoryLimitError"] * ROUNDS
    assert lines["rows"] == [str(N // 3)] * ROUNDS

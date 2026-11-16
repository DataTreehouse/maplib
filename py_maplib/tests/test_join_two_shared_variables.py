"""Joining two stars of a basic graph pattern that share two variables must join on both at once.

    ?s ex:kind ?k ; ex:region ?r . ?t ex:kind ?k ; ex:region ?r . FILTER(?s != ?t)

?k has 3 values and ?r 1000, so the result has about n * n / 3000 rows. The patterns were joined
one at a time, so `?t ex:kind ?k` was joined on ?k alone: n * n / 3 rows before ?r was checked
(2 GB for n = 15 000, over 100 GB for n = 200 000). The query runs in a child process with a hard
memory ceiling (``abort_above_mb``) that the quadratic join goes far over.
"""

import subprocess
import sys
import textwrap

import pytest

N = 9_000
# The quadratic join of 9 000 subjects has 27 million rows and needs about 0.7 GB.
CEILING_MB = 300

CHILD = textwrap.dedent(
    """
    import sys
    import polars as pl
    import maplib

    n, ceiling_mb, variant = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
    ex = "http://ex.org/"
    m = maplib.Model()
    subj = [f"{ex}s{i}" for i in range(n)]
    m.map_triples(
        pl.DataFrame({"subject": subj, "object": [f"{ex}r{i % 1000}" for i in range(n)]}),
        predicate=ex + "region",
    )
    m.map_triples(
        pl.DataFrame({"subject": subj, "object": [f"{ex}k{i % 3}" for i in range(n)]}),
        predicate=ex + "kind",
    )
    maplib.set_memory_limit(abort_above_mb=ceiling_mb)
    star = "ex:kind ?k ; ex:region ?r"
    if variant == "grouped":
        where = f"{{ ?s {star} }} {{ ?t {star} }}"
    else:
        where = f"?s {star} . ?t {star} ."
    q = f"PREFIX ex: <{ex}> SELECT ?s ?t WHERE {{ {where} FILTER(?s != ?t) }}"
    if variant == "limit":
        q += " LIMIT 10000"
    df = m.query(q)
    print("rows", df.height, flush=True)
    pairs = df.select(
        pl.col("s").str.extract(r"s(\\d+)>?$").cast(pl.Int64).alias("s"),
        pl.col("t").str.extract(r"s(\\d+)>?$").cast(pl.Int64).alias("t"),
    )
    print("wrong", pairs.filter((pl.col("s") % 3000 != pl.col("t") % 3000) | (pl.col("s") == pl.col("t"))).height, flush=True)
    print("distinct", pairs.unique().height, flush=True)
    """
)


def expected_rows(n):
    # Subjects i and j match when i % 3000 == j % 3000 (as i % 3 and i % 1000 then both agree).
    return sum(c * (c - 1) for c in (n // 3000 + (1 if j < n % 3000 else 0) for j in range(3000)))


@pytest.mark.parametrize("variant", ["bgp", "grouped", "limit"])
def test_join_on_two_shared_variables_stays_near_the_result_size(variant):
    p = subprocess.run(
        [sys.executable, "-c", CHILD, str(N), str(CEILING_MB), variant],
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert p.returncode == 0, f"exit {p.returncode}\n{p.stdout}\n{p.stderr[-2000:]}"
    lines = dict(line.split(" ", 1) for line in p.stdout.splitlines())
    expected = 10_000 if variant == "limit" else expected_rows(N)
    assert lines["rows"] == str(expected)
    assert lines["wrong"] == "0"
    assert lines["distinct"] == str(expected)

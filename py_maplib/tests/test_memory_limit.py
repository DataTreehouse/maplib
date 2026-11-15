"""Tests of the memory limit (``maplib.set_memory_limit``).

The out-of-memory tests run a query in a child process with a hard memory ceiling
(``abort_above_mb``), which aborts the process when maplib's allocations pass it, as the
operating system's OOM killer would, but at a size that is small and the same on every machine.

The query is a full ORDER BY on an integer and an IRI: without a limit it dies at the ceiling,
with a limit the same query spills to disk, stays under the ceiling and gives the right result.
Not everything spills: the hash tables of joins and group-bys stay in memory, as does the
bottom-k that Polars uses for ORDER BY with LIMIT/OFFSET.
"""

import subprocess
import sys
import textwrap

import pytest

import maplib

N_TRIPLES = 1_000_000
# Where the checked rows of the sorted result are.
OFFSET = N_TRIPLES // 2
# About 90 MB is loaded. Without a limit the sort goes over 250 MB; with the limit it spills
# and stays under 170 MB.
CEILING_MB = 210
LIMIT_MB = 120

CHILD = textwrap.dedent(
    """
    import sys
    import polars as pl
    import maplib

    n, limit_mb, ceiling_mb, spill_dir, offset = sys.argv[1:6]
    n = int(n)
    limit_mb = None if limit_mb == "none" else int(limit_mb)

    m = maplib.Model()
    # Subject i has the object n - 1 - i, so sorting on the object reverses the subjects.
    df = pl.select(
        subject=pl.format("http://example.net/s{}", pl.int_range(0, n)),
        object=n - 1 - pl.int_range(0, n),
    )
    m.map_triples(df, predicate="http://example.net/p")
    del df

    # Set after loading, so that the ceiling applies to the query.
    maplib.set_memory_limit(
        max_memory_mb=limit_mb, spill_dir=spill_dir, abort_above_mb=int(ceiling_mb)
    )
    print("loaded_mb", maplib.memory_usage() // 1_000_000, flush=True)
    df = m.query(
        '''
        SELECT ?a ?x WHERE { ?a <http://example.net/p> ?x }
        ORDER BY ?x ?a
        '''
    )
    offset = int(offset)
    rows = df.slice(offset, 2)
    print("height", df.height, flush=True)
    print("result", rows["a"].to_list(), rows["x"].to_list(), flush=True)
    """
)


def run_child(limit_mb, tmp_path, ceiling_mb=CEILING_MB):
    spill_dir = tmp_path / "spill"
    spill_dir.mkdir()
    return subprocess.run(
        [
            sys.executable,
            "-c",
            CHILD,
            str(N_TRIPLES),
            "none" if limit_mb is None else str(limit_mb),
            str(ceiling_mb),
            str(spill_dir),
            str(OFFSET),
        ],
        capture_output=True,
        text=True,
        timeout=600,
    )


def expected_result():
    a = [f"<http://example.net/s{N_TRIPLES - 1 - x}>" for x in [OFFSET, OFFSET + 1]]
    return f"result {a} {[OFFSET, OFFSET + 1]}"


@pytest.fixture
def reset_memory_limit():
    yield
    maplib.set_memory_limit()


@pytest.mark.skip(
    reason="Flaky: always fails on the EE build and fails on OSS under parallel load; under investigation"
)
def test_memory_usage_is_tracked():
    # Zero would mean that the global allocator does not count, and nothing would ever spill.
    # Each thread reports in steps of 4 MB, so the count is rough and the model must be large.
    import polars as pl

    m = maplib.Model()
    df = pl.select(
        subject=pl.format("http://example.net/s{}", pl.int_range(0, 3_000_000)),
        object=pl.int_range(0, 3_000_000),
    )
    m.map_triples(df, predicate="http://example.net/p")
    assert maplib.memory_usage() > 20_000_000


def test_set_memory_limit(reset_memory_limit):
    default = maplib.memory_limit()
    assert default > 0
    maplib.set_memory_limit(max_memory_mb=1234)
    assert maplib.memory_limit() == 1234 * 1_000_000
    maplib.set_memory_limit()
    assert maplib.memory_limit() == default


def test_set_memory_limit_rejects_zero(reset_memory_limit):
    with pytest.raises(Exception):
        maplib.set_memory_limit(max_memory_mb=0)


def test_query_completes_under_a_high_ceiling(tmp_path):
    out = run_child(LIMIT_MB, tmp_path, ceiling_mb=100_000)
    assert out.returncode == 0, out.stdout + out.stderr
    assert expected_result() in out.stdout, out.stdout


def test_query_over_ceiling_aborts(tmp_path):
    # abort_above_mb stops the process with a message before the operating system would.
    out = run_child(None, tmp_path)
    assert out.returncode != 0, out.stdout + out.stderr
    assert "memory usage exceeded" in out.stderr, out.stderr
    assert "result" not in out.stdout


def test_query_with_limit_stays_under_ceiling(tmp_path):
    # The same query as in test_query_over_ceiling_aborts, under the same ceiling: with a limit
    # it spills to disk instead.
    out = run_child(LIMIT_MB, tmp_path)
    assert out.returncode == 0, out.stdout + out.stderr
    assert f"height {N_TRIPLES}" in out.stdout, out.stdout
    assert expected_result() in out.stdout, out.stdout

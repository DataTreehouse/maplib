"""Per-segment Bloom filters on subject and object dictionary codes: lookups must give the same
results with the filters on (MAPLIB_SEGMENT_FILTER_BITS, default 10) and off (0), for tables
of many segments, before and after compact() and deletes, after serialization (with and
without filter files), and in disk storage with tables in object storage, where the filters
avoid downloads."""

import contextlib
import os
import pathlib
import shutil

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from maplib import IndexingOptions, Model

from .disk import disk_params, is_maplib_ee, storage_kwargs, STORAGE_FOLDER

ENV = "MAPLIB_SEGMENT_FILTER_BITS"
EX = "http://example.net/ns#"
# Decreasing batch sizes, so that every batch becomes a segment of its own.
BATCHES = [(0, 4000), (4000, 5500), (5500, 6000), (6000, 6150), (6150, 6190)]
N_Q = 30


@contextlib.contextmanager
def filter_bits(bits):
    old = os.environ.get(ENV)
    os.environ[ENV] = str(bits)
    try:
        yield
    finally:
        if old is None:
            del os.environ[ENV]
        else:
            os.environ[ENV] = old


def batch_nt(start, end):
    lines = []
    for i in range(start, end):
        s = f"<{EX}s{i}>"
        lines.append(f'{s} <{EX}p> <{EX}o{i % 50}> .')
        lines.append(f'{s} <{EX}name> "n{i}" .')
        if i % 10 != 9 and i + 1 < end:
            lines.append(f"{s} <{EX}next> <{EX}s{i + 1}> .")
        # Many predicates, each holding a few subjects of each batch.
        lines.append(f"{s} <{EX}q{i % N_Q}> <{EX}o{i % 7}> .")
        if i % 13 == 0:
            lines.append(f"_:b{i} <{EX}p> {s} .")
    return "\n".join(lines) + "\n"


def build(storage, object_index, bits):
    with filter_bits(bits):
        kwargs = storage_kwargs(storage)
        if object_index:
            kwargs["indexing_options"] = IndexingOptions(object_sort_all=True)
        m = Model(**kwargs)
        for start, end in BATCHES:
            m.reads(batch_nt(start, end), format="ntriples")
    return m


SUBJECTS = [
    "s0", "s3999", "s4001", "s5999", "s6100", "s6189",  # present, in each segment
    "o3",  # known IRI, never a subject
    "absent_iri",  # not in the dictionary
]

QUERIES = []
for s in SUBJECTS:
    QUERIES.append(f"SELECT ?o WHERE {{ <{EX}{s}> <{EX}p> ?o }} ORDER BY ?o")
    QUERIES.append(f"SELECT ?p ?o WHERE {{ <{EX}{s}> ?p ?o }} ORDER BY ?p ?o")
    QUERIES.append(f"SELECT ?x WHERE {{ <{EX}{s}> <{EX}next>+ ?x }} ORDER BY ?x")
    QUERIES.append(f"SELECT ?x WHERE {{ ?x <{EX}next>* <{EX}{s}> }} ORDER BY ?x")
    QUERIES.append(f"SELECT ?s WHERE {{ ?s <{EX}p> <{EX}{s}> }} ORDER BY ?s")
QUERIES += [
    f"""SELECT ?s ?p ?o WHERE {{
        VALUES ?s {{ {' '.join(f'<{EX}{s}>' for s in SUBJECTS)} }}
        ?s ?p ?o }} ORDER BY ?s ?p ?o""",
    f"""SELECT ?s ?o WHERE {{
        VALUES ?s {{ <{EX}s1> <{EX}s4100> <{EX}o1> <{EX}s6170> }}
        ?s <{EX}name> ?o }} ORDER BY ?s ?o""",
    f'SELECT ?s WHERE {{ ?s <{EX}name> "n4321" }} ORDER BY ?s',
    f'SELECT ?s WHERE {{ ?s <{EX}name> "absent" }} ORDER BY ?s',
    f"SELECT ?s WHERE {{ ?s <{EX}p> <{EX}o7> }} ORDER BY ?s",
    f"SELECT ?s WHERE {{ ?s <{EX}q3> <{EX}o3> }} ORDER BY ?s",
    f"""SELECT ?s ?x WHERE {{
        VALUES ?s {{ <{EX}s10> <{EX}s4010> <{EX}s6150> <{EX}o2> }}
        ?s <{EX}next>/<{EX}next> ?x }} ORDER BY ?s ?x""",
    "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }",
]


def results(m):
    out = []
    for q in QUERIES:
        df = m.query(q)
        out.append(df.sort(df.columns) if df.height else df)
    return out


def assert_same(a, b):
    assert len(a) == len(b)
    for q, x, y in zip(QUERIES, a, b):
        try:
            assert_frame_equal(x, y)
        except AssertionError as e:
            raise AssertionError(f"Different results for {q}") from e


def delete_some(m):
    m.update(
        f"""DELETE DATA {{
            <{EX}s0> <{EX}p> <{EX}o0> .
            <{EX}s4001> <{EX}name> "n4001" .
            <{EX}s6170> <{EX}next> <{EX}s6171> .
            <{EX}o3> <{EX}p> <{EX}o3> .
            <{EX}s1> <{EX}p> <{EX}o9> .
        }}"""
    )


@pytest.fixture(autouse=True)
def clean_storage():
    yield
    shutil.rmtree(STORAGE_FOLDER, ignore_errors=True)


@pytest.mark.parametrize("object_index", [False, True])
@pytest.mark.parametrize("storage", disk_params())
def test_filters_on_and_off_agree(storage, object_index):
    on = build(storage, object_index, 10)
    expected = results(on)
    assert expected[-1]["n"][0] > 20_000
    with filter_bits(0):
        off = build("memory", object_index, 0)
        assert_same(expected, results(off))
        # Many false positives must not change anything either.
        assert_same(expected, results(build("memory", object_index, 1)))

        delete_some(on)
        delete_some(off)
        after_delete = results(on)
        assert_same(after_delete, results(off))
    # compact() renumbers the codes, so the filters are rebuilt.
    with filter_bits(10):
        on.compact()
    with filter_bits(0):
        off.compact()
    assert_same(after_delete, results(on))
    assert_same(after_delete, results(off))
    # Inserts after compaction add segments with new codes.
    with filter_bits(10):
        on.reads(batch_nt(7000, 7100) + f"<{EX}s0> <{EX}p> <{EX}o0> .\n", format="ntriples")
    with filter_bits(0):
        off.reads(batch_nt(7000, 7100) + f"<{EX}s0> <{EX}p> <{EX}o0> .\n", format="ntriples")
    assert_same(results(on), results(off))


@pytest.mark.parametrize("storage", disk_params())
def test_serialized_filters_and_old_stores(storage, tmp_path):
    m = build("memory", True, 10)
    expected = results(m)
    folder = tmp_path / "model"
    m.serialize(str(folder), single_file=False)
    filter_files = list(folder.rglob("*_filter.bin"))
    assert len(filter_files) > N_Q
    assert_same(expected, results(Model.deserialize(str(folder), **storage_kwargs(storage))))
    shutil.rmtree(STORAGE_FOLDER, ignore_errors=True)
    with filter_bits(0):
        assert_same(expected, results(Model.deserialize(str(folder), **storage_kwargs(storage))))
    shutil.rmtree(STORAGE_FOLDER, ignore_errors=True)
    # A store written before the filters existed has no filter files.
    for f in filter_files:
        f.unlink()
    old = Model.deserialize(str(folder), **storage_kwargs(storage))
    assert_same(expected, results(old))
    with filter_bits(10):
        old.reads(batch_nt(7000, 7100), format="ntriples")
    assert results(old)[-1]["n"][0] > expected[-1]["n"][0]


def downloaded(storage_folder: pathlib.Path):
    return len(list((storage_folder / "maplib_storage" / "triples").glob("*.parquet")))


@pytest.mark.skipif(not is_maplib_ee(), reason="disk storage needs maplib_ee")
def test_remote_lookups_of_absent_subjects_avoid_downloads(tmp_path):
    m = build("memory", False, 10)
    expected = results(m)
    remote = tmp_path / "remote"
    remote.mkdir()
    url = f"file://{remote}/model"
    m.serialize(url, single_file=False)

    counts = {}
    for bits in [10, 0]:
        folder = tmp_path / f"disk{bits}"
        with filter_bits(bits):
            d = Model.deserialize(url, storage="disk", storage_folder=str(folder))
            assert downloaded(folder) == 0
            # Known IRIs that are subjects of no table, and a subject of a few tables.
            for s in ["o3", "o4", "s4001"]:
                d.query(f"SELECT ?p ?o WHERE {{ <{EX}{s}> ?p ?o }}")
            counts[bits] = downloaded(folder)
            assert_same(expected, results(d))
    # Without filters every table with IRI subjects is downloaded; with them only the few
    # tables that hold s4001 (and rare false positives).
    assert counts[0] > N_Q
    assert counts[10] <= 6, counts


@pytest.mark.parametrize("object_index", [False, True])
@pytest.mark.parametrize("storage", disk_params())
def test_small_inserts_are_found_although_no_segment_filter_holds_them(storage, object_index):
    # Small inserts may be kept outside the segments (in a write buffer, with no filter): their
    # subjects are in no segment's filter, and lookups, paths, deduplication and deletes must
    # still find them.
    on = build(storage, object_index, 10)
    with filter_bits(0):
        off = build("memory", object_index, 0)
    small = (
        f"<{EX}new1> <{EX}p> <{EX}o1> .\n"
        f'<{EX}new1> <{EX}name> "new one" .\n'
        f"<{EX}new1> <{EX}next> <{EX}new2> .\n"
        f"<{EX}new2> <{EX}next> <{EX}s0> .\n"
        f"<{EX}o3> <{EX}p> <{EX}new1> .\n"
    )
    queries = [
        f"SELECT ?p ?o WHERE {{ <{EX}new1> ?p ?o }} ORDER BY ?p ?o",
        f"SELECT ?o WHERE {{ <{EX}o3> <{EX}p> ?o }} ORDER BY ?o",
        f"SELECT ?s WHERE {{ ?s <{EX}p> <{EX}new1> }} ORDER BY ?s",
        f'SELECT ?s WHERE {{ ?s <{EX}name> "new one" }}',
        f"SELECT ?x WHERE {{ <{EX}new1> <{EX}next>+ ?x }} ORDER BY ?x",
        f"SELECT ?x WHERE {{ ?x <{EX}next>+ <{EX}s1> }} ORDER BY ?x",
        "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }",
    ]

    def run(m):
        return [m.query(q) for q in queries]

    for m, bits in [(on, 10), (off, 0)]:
        with filter_bits(bits):
            m.reads(small, format="ntriples")
            # Inserting the same triples again must not duplicate them.
            m.reads(small, format="ntriples")
    got, expected = run(on), run(off)
    for q, x, y in zip(queries, got, expected):
        assert_frame_equal(x, y, check_row_order=False)
    assert got[0].height == 3 and got[1].height == 1 and got[4].height > 2
    for m, bits in [(on, 10), (off, 0)]:
        with filter_bits(bits):
            m.update(f"DELETE DATA {{ <{EX}new1> <{EX}p> <{EX}o1> . <{EX}o3> <{EX}p> <{EX}new1> . }}")
    got, expected = run(on), run(off)
    for q, x, y in zip(queries, got, expected):
        assert_frame_equal(x, y, check_row_order=False)
    assert got[0].height == 2 and got[1].height == 0

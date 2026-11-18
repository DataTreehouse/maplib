"""DELETE DATA removes triples from the stored tables in place, without rebuilding them. These
tests compare the model with the expected triples after many small and some large deletions, in
tables with several segments, with and without the object index, and check that lookups by
subject and by object, inserts after deletes, change reports and serialization stay correct."""
import random
import shutil

import pytest

from maplib import Model, IndexingOptions, DEFAULT_GRAPH

from .disk import disk_params, storage_kwargs, STORAGE_FOLDER

EX = "http://example.net/ns#"
# More rows than a row group of a table on disk (16,384), so that tables span several groups.
N = 20_000


def subject(i):
    return f"<{EX}s{i}>"


def triple_lines(i):
    return {
        "v": f"{subject(i)} <{EX}v> {i * 7 % N} .",
        "link": f"{subject(i)} <{EX}link> {subject(i * 7919 % N)} .",
        "label": f'{subject(i)} <{EX}label> "label {i}" .',
        "name": f'{subject(i)} <{EX}name> "name {i % 100}"@en .',
    }


def data():
    return "\n".join(line for i in range(N) for line in triple_lines(i).values())


@pytest.fixture(params=[(s, o) for s in disk_params() for o in (False, True)],
                ids=lambda p: f"{p[0]}-{'object_sort' if p[1] else 'default'}")
def model(request):
    storage, object_sort = request.param
    if storage != "memory":
        shutil.rmtree(STORAGE_FOLDER, ignore_errors=True)
    kwargs = storage_kwargs(storage)
    if object_sort:
        kwargs["indexing_options"] = IndexingOptions(object_sort_all=True)
    m = Model(**kwargs)
    m.reads(data(), format="turtle")
    yield m
    del m
    if storage != "memory":
        shutil.rmtree(STORAGE_FOLDER, ignore_errors=True)


def stored(m):
    """All triples, as N-Triples lines."""
    return set(line for line in m.writes(format="ntriples").splitlines() if line)


def expected(lines):
    """The triples of N-Triples lines in the form of `stored`."""
    m = Model()
    m.reads("\n".join(lines), format="turtle")
    return stored(m)


def delete(m, lines):
    return m.update("DELETE DATA { " + " ".join(lines) + " }")


def check_lookups(m, present, absent):
    for i in present:
        df = m.query(f"SELECT ?o WHERE {{ {subject(i)} <{EX}link> ?o }}")
        assert df["o"].to_list() == [subject(i * 7919 % N)], i
        df = m.query(f"SELECT ?s WHERE {{ ?s <{EX}link> {subject(i * 7919 % N)} }}")
        assert df["s"].to_list() == [subject(i)], i
        df = m.query(f'SELECT ?s WHERE {{ ?s <{EX}label> "label {i}" }}')
        assert df["s"].to_list() == [subject(i)], i
    for i in absent:
        assert m.query(f"SELECT ?o WHERE {{ {subject(i)} <{EX}link> ?o }}").height == 0, i
        df = m.query(f"SELECT ?s WHERE {{ ?s <{EX}link> {subject(i * 7919 % N)} }}")
        assert df.height == 0, i
        assert m.query(f'SELECT ?s WHERE {{ ?s <{EX}label> "label {i}" }}').height == 0, i


def test_many_small_and_some_large_deletes(model):
    rng = random.Random(7)
    lines = {i: triple_lines(i) for i in range(N)}
    # Triples added after the read are in segments of their own.
    extra = [f"<{EX}new{i}> <{EX}v> {i} ." for i in range(300)]
    model.update("INSERT DATA { " + " ".join(extra) + " }")
    remaining = {(i, k) for i in range(N) for k in lines[i]}
    remaining_extra = set(extra)
    deleted_subjects = []

    # One triple at a time, from the read table and from the added segments, more times than a
    # table is cut into slices or ranges before it is rewritten.
    order = rng.sample(range(N), 150)
    for n, i in enumerate(order):
        changes = delete(model, [lines[i]["link"], lines[i]["label"]])
        assert changes[DEFAULT_GRAPH]["deleted"] == 2
        remaining -= {(i, "link"), (i, "label")}
        deleted_subjects.append(i)
        if n % 3 == 0:
            x = extra[n]
            assert delete(model, [x])[DEFAULT_GRAPH]["deleted"] == 1
            remaining_extra.discard(x)
    # Large deletes, of many rows in many places of the tables.
    for _ in range(2):
        batch = rng.sample(sorted(remaining), 2_000)
        changes = delete(model, [lines[i][k] for i, k in batch])
        assert changes[DEFAULT_GRAPH]["deleted"] == 2_000
        remaining -= set(batch)
    # Absent triples, alone and with present ones.
    gone = deleted_subjects[0]
    changes = delete(model, [lines[gone]["link"], f"<{EX}nope> <{EX}v> 1 .", f"<{EX}s1> <{EX}v> -5 ."])
    assert changes[DEFAULT_GRAPH]["deleted"] == 0

    want = expected([lines[i][k] for i, k in remaining] + sorted(remaining_extra))
    assert stored(model) == want
    present = [i for i in range(0, N, 997) if (i, "link") in remaining and (i, "label") in remaining]
    check_lookups(model, present, deleted_subjects[:40])

    # A deleted triple can be added again, and a stored one is not added twice.
    kept = present[0]
    changes = model.update(
        f"INSERT DATA {{ {lines[gone]['link']} {lines[gone]['label']} {lines[kept]['link']} }}"
    )
    assert changes[DEFAULT_GRAPH]["inserted"] == 2
    check_lookups(model, [gone, kept], [])

    # The model reads back the same after compaction and serialization.
    before = stored(model)
    model.compact()
    assert stored(model) == before


def test_delete_reports_the_deleted_triples(model):
    lines = [triple_lines(5)["link"], triple_lines(6)["v"], f"<{EX}nope> <{EX}v> 1 ."]
    changes = model.update("DELETE DATA { " + " ".join(lines) + " }", return_triples=True)
    c = changes[DEFAULT_GRAPH]
    assert c["deleted"] == 2
    assert sorted(c["deleted_triples"].keys()) == [f"{EX}link", f"{EX}v"]
    link = c["deleted_triples"][f"{EX}link"]
    assert link.height == 1


def test_delete_whole_table(model):
    lines = [triple_lines(i)["name"] for i in range(N)]
    for chunk in range(0, N, 5_000):
        delete(model, lines[chunk:chunk + 5_000])
    assert model.query(f"SELECT ?s WHERE {{ ?s <{EX}name> ?o }}").height == 0
    model.update(f"INSERT DATA {{ {lines[3]} }}")
    assert model.query(f"SELECT ?s WHERE {{ ?s <{EX}name> ?o }}")["s"].to_list() == [subject(3)]


def test_serialize_after_deletes(model, tmp_path):
    rng = random.Random(3)
    for i in rng.sample(range(N), 80):
        delete(model, [triple_lines(i)["link"]])
    before = stored(model)
    path = tmp_path / "m.maplib"
    model.serialize(str(path))
    assert stored(Model.deserialize(str(path))) == before

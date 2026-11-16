"""Tests for streams that iterate on a persistent Model: in_place=True on the map calls, the
drop_graphs step, Model.drop_graphs and Model.scratch_graphs. See docs/model_stream_iterations.md.
"""

import gc
import io
import pathlib
import threading

import polars as pl
import pytest

from maplib import Model, TRANSIENT_GRAPH

from .disk import disk_params, storage_kwargs, STORAGE_FOLDER

EX = "http://example.net/ns#"
BATCH = "urn:iter:batch"
SCRATCH = "urn:iter:scratch"
TEMPLATE_IRI = f"{EX}ExampleTemplate"
TRIPLES_FOLDER = pathlib.Path(STORAGE_FOLDER) / "maplib_storage" / "triples"

TEMPLATE = """
@prefix ex:<http://example.net/ns#>.
ex:ExampleTemplate [ ottr:IRI ?s, xsd:string ?v ] :: {
    ottr:Triple(?s, ex:hasValue, ?v)
} .
"""

BLANK_TEMPLATE = """
@prefix ex:<http://example.net/ns#>.
ex:BlankTemplate [ xsd:string ?v ] :: {
    ottr:Triple(_:b, ex:hasValue, ?v)
} .
"""

# Joins the batch with the reference data in the default graph.
LINK = f"""
PREFIX ex:<{EX}>
INSERT {{ GRAPH <{SCRATCH}> {{ ?s ex:inCat ?c }} }}
WHERE {{ ?s ex:hasValue ?v . ?s ex:category ?c }}
"""

SUMMARY = f"""
PREFIX ex:<{EX}>
CONSTRUCT {{ ?c ex:hasMember ?s }} WHERE {{ ?s ex:inCat ?c }}
"""

# Records the size of the batch graph as seen by a step, in the default graph.
COUNT_BATCH = f"""
PREFIX ex:<{EX}>
INSERT {{ ex:counter ex:seen ?n }}
WHERE {{ {{ SELECT (COUNT(*) AS ?n) WHERE {{ GRAPH <{BATCH}> {{ ?s ?p ?o }} }} }} }}
"""

FAILING_UPDATE = f"PREFIX ex:<{EX}> INSERT {{ ?s ex:p ?o }} WHERE {{ ?s ex:nope }}"

N = 60


def _df(n=N, start=0):
    return pl.DataFrame(
        {
            "s": [f"{EX}s{i}" for i in range(start, start + n)],
            "v": [f"v{i}" for i in range(start, start + n)],
        }
    )


def _batches(n=N, size=10):
    df = _df(n)
    return [df.slice(i, size) for i in range(0, n, size)]


def _reference_turtle(n=N):
    # Every third subject has no category, so not every batch row joins.
    lines = [f"@prefix ex:<{EX}> ."]
    for i in range(n):
        if i % 3 != 2:
            lines.append(f"ex:s{i} ex:category ex:c{i % 3} .")
    return "\n".join(lines)


def _model(reference=True, **kwargs):
    m = Model(**kwargs)
    m.add_template(TEMPLATE)
    if reference:
        m.reads(_reference_turtle(), format="turtle")
    return m


def _lines(data: bytes):
    return sorted(line for line in data.decode().splitlines() if line.strip())


def _graph_size(m, graph):
    return m.query(
        f"SELECT ?s ?p ?o WHERE {{ GRAPH <{graph}> {{ ?s ?p ?o }} }}", include_graphs=[]
    ).height


def _named_graphs(m):
    df = m.query(
        "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }", include_graphs=[]
    )
    return {g.strip("<>") for g in df["g"].to_list()} if df.height else set()


def _default_lines(m):
    return _lines(m.write_bytes(format="ntriples"))


def _members(m):
    return m.query(f"SELECT ?c ?s WHERE {{ ?c <{EX}hasMember> ?s }}").height


def _in_place_stream(m, data, links):
    return (
        m.map(TEMPLATE_IRI, data, graph=BATCH, stream=True, in_place=True)
        .update(LINK, include_graphs=[BATCH])
        .insert(SUMMARY, source_graph=SCRATCH, target_graph=None)
        .write(links, graph=SCRATCH)
        .drop_graphs([BATCH, SCRATCH])
    )


# Equivalence and joins


def test_in_place_equals_python_loop():
    m = _model()
    links = io.BytesIO()
    _in_place_stream(m, _batches(), links).collect()

    loop = _model()
    loop_links = []
    for df in _batches():
        with loop.scratch_graphs(BATCH, SCRATCH):
            loop.map(TEMPLATE_IRI, df, graph=BATCH)
            loop.update(LINK, include_graphs=[BATCH])
            loop.insert(SUMMARY, source_graph=SCRATCH, target_graph=None)
            loop_links.append(loop.write_bytes(format="ntriples", graph=SCRATCH))

    assert _default_lines(m) == _default_lines(loop)
    assert _lines(links.getvalue()) == _lines(b"".join(loop_links))
    # 2 of 3 subjects have a category
    assert _members(m) == 2 * N // 3
    assert len(_lines(links.getvalue())) == 2 * N // 3
    assert _named_graphs(m) == set() == _named_graphs(loop)


def test_in_place_join_with_persistent_data_finds_matches_fresh_batches_do_not():
    m = _model()
    _in_place_stream(m, _batches(), io.BytesIO()).collect()
    assert _members(m) == 2 * N // 3

    fresh = _model()
    out = io.BytesIO()
    fresh.map(TEMPLATE_IRI, _batches(), graph=BATCH, stream=True).update(
        LINK, include_graphs=[BATCH]
    ).write(out, graph=SCRATCH).collect()
    assert out.getvalue() == b""
    assert _members(fresh) == 0


def test_in_place_steps_see_what_earlier_batches_kept():
    # Each batch counts the members kept by earlier batches.
    m = _model()
    count_members = f"""
PREFIX ex:<{EX}>
INSERT {{ GRAPH <{SCRATCH}> {{ ex:counter ex:before ?n }} }}
WHERE {{ {{ SELECT (COUNT(*) AS ?n) WHERE {{ ?c ex:hasMember ?s }} }} }}
"""
    out = io.BytesIO()
    (
        m.map(TEMPLATE_IRI, _batches(size=15), graph=BATCH, stream=True, in_place=True)
        .update(count_members)
        .write(out, graph=SCRATCH)
        .update(LINK, include_graphs=[BATCH])
        .insert(SUMMARY, source_graph=SCRATCH, target_graph=None)
        .drop_graphs([BATCH, SCRATCH])
        .collect()
    )
    seen = sorted(int(line.split('"')[1]) for line in _lines(out.getvalue()))
    assert seen == [0, 10, 20, 30]


def test_map_default_in_place_adds_template_to_model():
    m = Model()
    df = pl.DataFrame({"k": [f"{EX}k{i}" for i in range(10)], "x": [f"x{i}" for i in range(10)]})
    m.map_default([df.slice(0, 5), df.slice(5, 5)], "k", stream=True, in_place=True).collect()
    assert m.size() == 10
    templates = m.get_templates()
    assert len(templates) == 1
    m.map(templates[0].iri, pl.DataFrame({"k": [f"{EX}other"], "x": ["y"]}))
    assert m.size() == 11


def test_map_triples_and_map_json_in_place():
    m = Model()
    triples = pl.DataFrame({"subject": [f"{EX}a", f"{EX}b"], "object": [f"{EX}c", f"{EX}d"]})
    m.map_triples(
        [triples], predicate=f"{EX}p", stream=True, in_place=True, graph=BATCH
    ).insert(
        f"CONSTRUCT {{ ?s <{EX}q> ?o }} WHERE {{ ?s <{EX}p> ?o }}", source_graph=BATCH
    ).drop_graphs([BATCH]).collect()
    assert m.size() == 2
    assert _named_graphs(m) == set()

    j = Model()
    docs = ['{"a": 1}', '{"a": 2}', '{"a": 3}']
    j.map_json(docs, stream=True, in_place=True, documents_per_batch=2).collect()
    expected = Model()
    expected.map_json(docs)
    assert j.size() == expected.size() > 0


def test_in_place_without_drop_graphs_accumulates_in_model():
    m = _model(reference=False)
    m.map(TEMPLATE_IRI, _batches(), stream=True, in_place=True).collect()
    assert m.size() == N


def test_in_place_ignored_without_stream():
    m = _model(reference=False)
    assert m.map(TEMPLATE_IRI, _df(), in_place=True) is None
    assert m.size() == N


# Graphs named in drop_graphs are empty after every iteration


def test_drop_graphs_empty_after_every_iteration():
    sizes = [3, 5, 7, 11]
    df = _df(sum(sizes))
    batches, start = [], 0
    for s in sizes:
        batches.append(df.slice(start, s))
        start += s
    m = _model(reference=False)
    m.map(TEMPLATE_IRI, batches, graph=BATCH, stream=True, in_place=True).update(
        COUNT_BATCH
    ).drop_graphs([BATCH]).collect()
    seen = sorted(
        m.query(f"SELECT ?n WHERE {{ <{EX}counter> <{EX}seen> ?n }}")["n"].to_list()
    )
    # Not cumulative: each batch sees only its own triples
    assert seen == sizes
    assert _graph_size(m, BATCH) == 0
    assert _named_graphs(m) == set()


def test_drop_graphs_still_run_after_failing_step():
    m = _model()
    with pytest.raises(Exception):
        (
            m.map(TEMPLATE_IRI, _batches(), graph=BATCH, stream=True, in_place=True)
            .update(LINK, include_graphs=[BATCH])
            .insert(SUMMARY, source_graph=SCRATCH, target_graph=None)
            .update(FAILING_UPDATE)
            .drop_graphs([BATCH])
            .update(COUNT_BATCH)
            .drop_graphs([SCRATCH])
            .collect()
        )
    assert _named_graphs(m) == set()
    # No rollback: the increment of the failing (first) batch stays
    assert _members(m) == len([i for i in range(10) if i % 3 != 2])
    # The steps after the failing one, other than drop_graphs, did not run
    assert m.query(f"SELECT ?n WHERE {{ <{EX}counter> <{EX}seen> ?n }}").height == 0


def test_drop_graphs_run_after_failing_mapping():
    m = _model()
    bad = pl.DataFrame({"s": ["not an iri"], "v": ["x"]})
    with pytest.raises(Exception):
        (
            m.map(TEMPLATE_IRI, [_df(5), bad], graph=BATCH, stream=True, in_place=True, validate_iris=True)
            .update(LINK, include_graphs=[BATCH])
            .drop_graphs([BATCH, SCRATCH])
            .collect()
        )
    assert _named_graphs(m) == set()
    assert m.size() > 0


def test_drop_graphs_empty_after_input_iterator_error():
    class Boom(Exception):
        pass

    def gen():
        yield _df(5)
        yield _df(5, start=5)
        raise Boom("input failed")

    m = _model()
    with pytest.raises(Exception, match="input failed"):
        _in_place_stream(m, gen(), io.BytesIO()).collect()
    assert _named_graphs(m) == set()
    assert _members(m) == len([i for i in range(10) if i % 3 != 2])


def test_drop_graphs_empty_after_write_target_error():
    class FailingWriter(io.RawIOBase):
        def writable(self):
            return True

        def write(self, b):
            raise OSError("disk full")

    m = _model()
    # Small batches so the buffered writer flushes inside the batch.
    with pytest.raises(Exception, match="disk full"):
        (
            m.map(TEMPLATE_IRI, _batches(n=30000, size=10000), graph=BATCH, stream=True, in_place=True)
            .write(FailingWriter(), graph=BATCH)
            .drop_graphs([BATCH])
            .collect()
        )
    assert _named_graphs(m) == set()


# Checks when collected


def test_start_check_refuses_non_empty_dropped_graph():
    m = _model()
    m.map(TEMPLATE_IRI, _df(4), graph=SCRATCH)
    before = _lines(m.write_bytes(format="ntriples", graph=SCRATCH))
    links = io.BytesIO()
    with pytest.raises(Exception, match="already has 4 triples"):
        _in_place_stream(m, _batches(), links).collect()
    assert _lines(m.write_bytes(format="ntriples", graph=SCRATCH)) == before
    assert _graph_size(m, BATCH) == 0
    assert _members(m) == 0
    # Clearing the leftovers explicitly lets it run
    m.drop_graphs([SCRATCH])
    _in_place_stream(m, _batches(), links).collect()
    assert _members(m) == 2 * N // 3


def test_start_check_on_default_graph():
    m = _model()
    with pytest.raises(Exception, match="already has"):
        m.map(TEMPLATE_IRI, _batches(), graph=BATCH, stream=True, in_place=True).drop_graphs(
            [None]
        ).collect()
    assert m.size() == len(_reference_turtle().splitlines()) - 1


def test_write_of_persistent_graph_refused_with_in_place(tmp_path):
    m = _model()
    with pytest.raises(Exception, match="no later"):
        m.map(TEMPLATE_IRI, _batches(), graph=BATCH, stream=True, in_place=True).write(
            tmp_path / "out.nt"
        ).drop_graphs([BATCH]).collect()
    # A drop_graphs step before the write does not count
    with pytest.raises(Exception, match="no later"):
        m.map(TEMPLATE_IRI, _batches(), graph=BATCH, stream=True, in_place=True).drop_graphs(
            [BATCH]
        ).write(tmp_path / "out.nt", graph=BATCH).collect()
    with pytest.raises(Exception, match="must name the graph"):
        m.map(TEMPLATE_IRI, _batches(), graph=BATCH, stream=True, in_place=True).write(
            tmp_path / "out.nq", format="nquads"
        ).drop_graphs([BATCH]).collect()
    # Nothing ran
    assert _graph_size(m, BATCH) == 0
    # Allowed without in_place
    m.map(TEMPLATE_IRI, _batches(), graph=BATCH, stream=True).write(
        tmp_path / "out.nt"
    ).collect()


def test_write_of_transient_graph_allowed_with_in_place():
    m = _model(reference=False)
    out = io.BytesIO()
    m.map(TEMPLATE_IRI, _batches(), graph=TRANSIENT_GRAPH, stream=True, in_place=True).write(
        out, graph=TRANSIENT_GRAPH
    ).drop_graphs([TRANSIENT_GRAPH]).collect()
    assert len(_lines(out.getvalue())) == N


def test_compact_every_requires_in_place_and_positive():
    m = _model()
    with pytest.raises(Exception, match="in_place"):
        m.map(TEMPLATE_IRI, _batches(), stream=True, compact_every=2)
    with pytest.raises(Exception, match="at least 1"):
        m.map(TEMPLATE_IRI, _batches(), stream=True, in_place=True, compact_every=0)


# drop_graphs without in_place


def test_drop_graphs_without_in_place_frees_graph_for_later_steps():
    m = _model(reference=False)
    before, after = io.BytesIO(), io.BytesIO()
    m.map(TEMPLATE_IRI, _batches(), graph=BATCH, stream=True).write(
        before, graph=BATCH
    ).drop_graphs([BATCH]).write(after, graph=BATCH).collect()
    assert len(_lines(before.getvalue())) == N
    assert after.getvalue() == b""
    assert m.size() == 0


def test_drop_graphs_without_in_place_after_failing_step_raises():
    m = _model(reference=False)
    with pytest.raises(Exception):
        m.map(TEMPLATE_IRI, _batches(), graph=BATCH, stream=True).update(
            FAILING_UPDATE
        ).drop_graphs([BATCH]).collect()
    assert m.size() == 0


# Blank nodes


def test_blank_nodes_unique_across_iterations_and_streams():
    m = Model()
    m.add_template(BLANK_TEMPLATE)
    df = _df(10).select("v")
    a, b = io.BytesIO(), io.BytesIO()
    for out in (a, b):
        m.map(
            f"{EX}BlankTemplate", [df.slice(0, 5), df.slice(5, 5)], graph=BATCH, stream=True,
            in_place=True,
        ).write(out, graph=BATCH).drop_graphs([BATCH]).collect()
    labels = [{x.split()[0] for x in _lines(buf.getvalue())} for buf in (a, b)]
    assert len(labels[0]) == 10 and len(labels[1]) == 10
    assert not labels[0] & labels[1]
    # A fresh-batch stream from the same model does not reuse them either
    c = io.BytesIO()
    m.map(f"{EX}BlankTemplate", [df], stream=True).write(c).collect()
    labels_c = {x.split()[0] for x in _lines(c.getvalue())}
    assert not labels_c & (labels[0] | labels[1])


# Concurrency


def test_queries_between_batches_only_see_whole_iterations():
    m = _model()
    observed = []

    def gen():
        for df in _batches(size=10):
            # Runs between batches, without the model's lock.
            observed.append((_members(m), _named_graphs(m)))
            yield df

    _in_place_stream(m, gen(), io.BytesIO()).collect()
    members_per_batch = [len([i for i in range(k, k + 10) if i % 3 != 2]) for k in range(0, N, 10)]
    cumulative = [sum(members_per_batch[:k]) for k in range(len(members_per_batch))]
    assert [o[0] for o in observed] == cumulative
    assert all(o[1] == set() for o in observed)


def test_query_from_other_thread_during_collect_sees_whole_iterations():
    batches = _batches(n=3000, size=500)
    # Every subject of every batch has a category in this test.
    m2 = Model()
    m2.add_template(TEMPLATE)
    m2.reads(
        "\n".join([f"@prefix ex:<{EX}> ."] + [f"ex:s{i} ex:category ex:c{i % 3} ." for i in range(3000)]),
        format="turtle",
    )
    stop = threading.Event()
    seen = []
    errors = []

    def poll():
        try:
            while not stop.is_set():
                seen.append((_members(m2), _graph_size(m2, BATCH), _graph_size(m2, SCRATCH)))
        except Exception as e:  # pragma: no cover
            errors.append(e)

    t = threading.Thread(target=poll)
    t.start()
    try:
        _in_place_stream(m2, batches, io.BytesIO()).collect()
    finally:
        stop.set()
        t.join()
    assert not errors
    assert seen
    for members, batch, scratch in seen:
        assert members % 500 == 0
        assert batch == 0 and scratch == 0
    assert _members(m2) == 3000


# Model.drop_graphs and scratch_graphs


def test_model_drop_graphs_and_truncate_graph():
    m = _model(reference=False)
    m.map(TEMPLATE_IRI, _df(5), graph=BATCH)
    m.map(TEMPLATE_IRI, _df(6), graph=SCRATCH)
    m.map(TEMPLATE_IRI, _df(7))
    m.drop_graphs([BATCH, "urn:does:not:exist"])
    assert _named_graphs(m) == {SCRATCH}
    m.truncate_graph(SCRATCH)
    assert _named_graphs(m) == set()
    m.drop_graphs([None])
    assert m.size() == 0
    # The graphs can be filled again
    m.map(TEMPLATE_IRI, _df(3), graph=BATCH)
    assert _graph_size(m, BATCH) == 3


def test_scratch_graphs_drops_on_exit_and_on_error():
    m = _model(reference=False)
    with m.scratch_graphs(BATCH, SCRATCH) as mm:
        assert mm is m
        m.map(TEMPLATE_IRI, _df(5), graph=BATCH)
        m.insert(
            f"CONSTRUCT {{ ?s <{EX}q> ?v }} WHERE {{ GRAPH <{BATCH}> {{ ?s <{EX}hasValue> ?v }} }}",
            target_graph=SCRATCH,
        )
        assert _named_graphs(m) == {BATCH, SCRATCH}
    assert _named_graphs(m) == set()

    with pytest.raises(RuntimeError, match="boom"):
        with m.scratch_graphs(BATCH):
            m.map(TEMPLATE_IRI, _df(5), graph=BATCH)
            raise RuntimeError("boom")
    assert _named_graphs(m) == set()


def test_drop_graphs_clears_full_text_index_on_dropped_graph():
    m = _model(reference=False)
    try:
        m.create_index(fts=True)
    except Exception:
        pytest.skip("full text search index not available")
    m.map(TEMPLATE_IRI, _df(5), graph=BATCH)
    m.drop_graphs([BATCH])
    m.map(TEMPLATE_IRI, _df(2), graph=BATCH)
    assert _graph_size(m, BATCH) == 2


# Memory and disk


def _parquet_files():
    if not TRIPLES_FOLDER.exists():
        return []
    return sorted(TRIPLES_FOLDER.glob("*.parquet"))


@pytest.mark.parametrize("storage", disk_params())
def test_storage_drop_graphs_and_many_iterations(storage):
    m = _model(**storage_kwargs(storage))
    m.map(TEMPLATE_IRI, _df(5), graph=BATCH)
    m.drop_graphs([BATCH])
    assert _named_graphs(m) == set()
    m.map(TEMPLATE_IRI, _df(5), graph=SCRATCH)
    m.truncate_graph(SCRATCH)
    assert _named_graphs(m) == set()

    gc.collect()
    files_before = len(_parquet_files())
    for k in range(3):
        _in_place_stream(m, _batches(size=10), io.BytesIO()).collect()
        if k == 0:
            gc.collect()
            files_after_one = len(_parquet_files())
    gc.collect()
    assert _named_graphs(m) == set()
    assert _members(m) == 2 * N // 3
    if storage == "disk":
        # The dropped graphs' files are deleted, so the folder does not grow with the iterations
        # beyond the persistent graph's increments.
        assert files_after_one >= files_before
        files_end = len(_parquet_files())
        assert files_end - files_after_one <= 2 * (files_after_one - files_before) + 4
    del m
    gc.collect()


def test_compact_every_keeps_results():
    m = _model()
    m.map(TEMPLATE_IRI, _batches(size=5), graph=BATCH, stream=True, in_place=True, compact_every=3).update(
        LINK, include_graphs=[BATCH]
    ).insert(SUMMARY, source_graph=SCRATCH, target_graph=None).drop_graphs(
        [BATCH, SCRATCH]
    ).collect()
    assert _members(m) == 2 * N // 3
    assert _named_graphs(m) == set()
    # Fresh IRIs every batch, with garbage collection of the dictionary: the model still answers
    # queries over the kept graph after many compactions.
    fresh = _model(reference=False)
    batches = [_df(5, start=1000 * k) for k in range(12)]
    fresh.map(TEMPLATE_IRI, batches, graph=BATCH, stream=True, in_place=True, compact_every=2).insert(
        f"CONSTRUCT {{ <{EX}root> <{EX}count> ?n }} WHERE {{ {{ SELECT (COUNT(*) AS ?n) WHERE {{ ?s <{EX}hasValue> ?v }} }} }}",
        source_graph=BATCH,
    ).drop_graphs([BATCH]).collect()
    assert fresh.query(f"SELECT ?n WHERE {{ <{EX}root> <{EX}count> ?n }}")["n"].to_list() == [5]
    assert _named_graphs(fresh) == set()

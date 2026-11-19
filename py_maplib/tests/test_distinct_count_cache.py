"""Whole-table COUNT(DISTINCT ?s) and COUNT(DISTINCT ?o) are cached by the generations of the
tables counted, and a table with one segment and an empty write buffer answers from the exact
distinct counts of the segment. Every kind of write must change the answer exactly as it changes
the data: each count is checked against rdflib after INSERT DATA (into the write buffer and
merged into segments), DELETE DATA (from the buffer and from segments), DELETE WHERE,
INSERT ... WHERE, compact(), and serialization, in a named graph too, in every storage mode,
with and without the object index. Each query is run twice, so the second answer comes from the
cache."""

import pytest
import rdflib

from maplib import IndexingOptions, Model

from .disk import disk_params, storage_kwargs

EX = "http://example.net/ns#"
GRAPH = "http://example.net/graph"


def nt(start, end):
    lines = []
    for i in range(start, end):
        s = f"<{EX}s{i}>"
        lines.append(f"{s} <{EX}p> <{EX}o{i % 50}> .")
        lines.append(f'{s} <{EX}name> "n{i % 300}" .')
        if i % 3 == 0:
            lines.append(f"{s} <{EX}p> <{EX}s{(i * 7) % 1000}> .")
        if i % 11 == 0:
            lines.append(f"_:b{i} <{EX}p> {s} .")
            lines.append(f'{s} <{EX}n> "{i % 17}"^^<http://www.w3.org/2001/XMLSchema#integer> .')
    return "\n".join(lines) + "\n"


def queries():
    out = []
    for v in ["?s", "?o"]:
        out.append(f"SELECT (COUNT(DISTINCT {v}) AS ?c) WHERE {{ ?s ?p ?o }}")
        for p in ["p", "name", "n"]:
            out.append(f"SELECT (COUNT(DISTINCT {v}) AS ?c) WHERE {{ ?s <{EX}{p}> ?o }}")
        out.append(
            f"SELECT (COUNT(DISTINCT {v}) AS ?c) WHERE {{ ?s ?p ?o FILTER(ISIRI(?o)) }}"
        )
    return out


QUERIES = queries()


class Pair:
    """A maplib model and an rdflib dataset with the same triples, in the default graph or in
    GRAPH."""

    def __init__(self, storage, object_index, graph):
        kwargs = storage_kwargs(storage)
        if object_index:
            kwargs["indexing_options"] = IndexingOptions(object_sort_all=True)
        self.m = Model(**kwargs)
        self.graph = graph
        self.ds = rdflib.Dataset()
        self.g = self.ds.graph(rdflib.URIRef(graph)) if graph else self.ds.default_graph

    def reads(self, data):
        self.m.reads(data, format="ntriples", graph=self.graph)
        self.g.parse(data=data, format="nt")

    def update(self, u):
        self.m.update(u, graph=self.graph)
        self.g.update(u)

    def check(self, when):
        for q in QUERIES:
            expected = int(list(self.g.query(q))[0][0])
            for attempt in range(2):
                df = self.m.query(q, graph=self.graph)
                got = df["c"][0]
                assert got == expected, f"{when}, attempt {attempt}: {q}: {got} != {expected}"


@pytest.mark.parametrize("storage", disk_params())
@pytest.mark.parametrize("object_index", [False, True])
@pytest.mark.parametrize("graph", [None, GRAPH])
def test_distinct_counts_follow_every_write(storage, object_index, graph, tmp_path):
    p = Pair(storage, object_index, graph)
    p.reads(nt(0, 1000))
    p.check("loaded")

    # Into the write buffer: new subjects and objects, and new triples of known terms.
    p.update(
        f"""INSERT DATA {{
        <{EX}s5000> <{EX}p> <{EX}o_new> .
        <{EX}s5001> <{EX}name> "brand new" .
        <{EX}s1> <{EX}p> <{EX}o7> .
        <{EX}s2> <{EX}n> "99"^^<http://www.w3.org/2001/XMLSchema#integer> .
    }}"""
    )
    p.check("insert data into the buffer")

    # Deletes from the buffer (s5000 and o_new disappear) and from the segment (s0 stops being
    # an object of p, and s10 loses its name).
    p.update(
        f"""DELETE DATA {{
        <{EX}s5000> <{EX}p> <{EX}o_new> .
        <{EX}s10> <{EX}name> "n10" .
        <{EX}s0> <{EX}p> <{EX}s0> .
    }}"""
    )
    p.check("delete data from buffer and segment")

    # Every triple of some subjects: subjects and objects disappear from several tables.
    p.update(f"DELETE WHERE {{ <{EX}s3> ?p ?o }}")
    p.update(f"DELETE WHERE {{ ?s <{EX}p> <{EX}o9> }}")
    p.check("delete where")

    p.update(f"INSERT {{ ?s <{EX}name> ?o }} WHERE {{ ?s <{EX}p> ?o FILTER(?o = <{EX}o1>) }}")
    p.check("insert where")

    # A large insert merges the buffer into the segments.
    p.reads(nt(1000, 6000))
    p.check("large insert")

    p.update(f"DELETE WHERE {{ ?s <{EX}name> \"n7\" }}")
    p.check("delete after merge")

    p.m.compact()
    p.check("compact")

    p.update(f"DELETE DATA {{ <{EX}s12> <{EX}p> <{EX}o12> . <{EX}s20> <{EX}p> <{EX}o20> . }}")
    p.check("delete after compact")

    folder = tmp_path / "model"
    p.m.serialize(str(folder), single_file=False)
    p.m = Model.deserialize(str(folder), **storage_kwargs(storage))
    p.check("deserialized")
    p.update(f"INSERT DATA {{ <{EX}s9000> <{EX}p> <{EX}o9000> . }}")
    p.update(f"DELETE WHERE {{ <{EX}s6> ?p ?o }}")
    p.check("written after deserialization")


@pytest.mark.parametrize("storage", disk_params())
def test_distinct_counts_of_one_segment_deletes(storage):
    """Deletes from a single compacted segment update its exact counts: runs removed in whole,
    in part, at the start and the end of the segment, and in blocks of adjacent rows."""
    kwargs = storage_kwargs(storage)
    kwargs["indexing_options"] = IndexingOptions(object_sort_all=True)
    m = Model(**kwargs)
    g = rdflib.Graph()
    data = "".join(
        f"<{EX}s{i:04}> <{EX}p> <{EX}o{j:03}> .\n" for i in range(200) for j in range(i % 5 + 1)
    )
    m.reads(data, format="ntriples")
    g.parse(data=data, format="nt")
    m.compact()
    deletes = [
        f"<{EX}s0000> <{EX}p> <{EX}o000> .",  # the whole first run, at the start
        f"<{EX}s0199> <{EX}p> <{EX}o000> .",  # part of the last run
        f"<{EX}s0004> <{EX}p> <{EX}o000> . <{EX}s0004> <{EX}p> <{EX}o001> .",
        " ".join(f"<{EX}s{i:04}> <{EX}p> <{EX}o{j:03}> ." for i in range(10, 20) for j in range(5)),
        f"<{EX}s0199> <{EX}p> <{EX}o001> . <{EX}s0199> <{EX}p> <{EX}o002> ."
        f" <{EX}s0199> <{EX}p> <{EX}o003> . <{EX}s0199> <{EX}p> <{EX}o004> .",
    ]
    for d in deletes:
        u = f"DELETE DATA {{ {d} }}"
        m.update(u)
        g.update(u)
        for v in ["?s", "?o"]:
            q = f"SELECT (COUNT(DISTINCT {v}) AS ?c) WHERE {{ ?s <{EX}p> ?o }}"
            expected = int(list(g.query(q))[0][0])
            for _ in range(2):
                assert m.query(q)["c"][0] == expected, (d, q)


def test_distinct_count_fast_paths():
    """A single compacted segment answers from its exact count, a repeated whole-store count from
    the cache, and a write makes the next count compute again."""
    import maplib.maplib as native

    if not hasattr(native, "_fast_path_counts"):
        pytest.skip("built without fast path counters")

    def paths(m, q):
        native._reset_fast_path_counts()
        m.query(q)
        return {k for k, v in native._fast_path_counts().items() if v and k.startswith("count_distinct.")}

    m = Model()
    m.reads(nt(0, 1000), format="ntriples")
    m.compact()
    # One table: IRI subjects and string objects.
    one = f"SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE {{ ?s <{EX}name> ?o }}"
    assert paths(m, one) == {"count_distinct.segment_exact"}
    every = "SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s ?p ?o }"
    assert "count_distinct.cached" not in paths(m, every)
    assert paths(m, every) == {"count_distinct.cached"}
    m.update(f"INSERT DATA {{ <{EX}s9999> <{EX}name> \"x\" }}")
    assert "count_distinct.cached" not in paths(m, every)
    assert paths(m, every) == {"count_distinct.cached"}

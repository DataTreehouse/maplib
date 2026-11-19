"""Bound-subject lookups, `<s> ?p ?o` and property paths see triples added by a small INSERT DATA
into a table that already has sorted segments with sparse indexes (lookups seek in those
indexes, and small inserts may be held outside the segments)."""
import polars as pl
from maplib import Model

EX = "http://ex.org/"


def make_model():
    m = Model()
    n = 5_000
    m.map_triples(
        pl.DataFrame({
            "subject": [f"{EX}s{i}" for i in range(n)],
            "object": [f"{EX}s{i + 1}" for i in range(n)],
        }),
        predicate=EX + "next",
    )
    m.map_triples(
        pl.DataFrame({"subject": [f"{EX}s{i}" for i in range(n)], "object": [f"{EX}o{i}" for i in range(n)]}),
        predicate=EX + "val",
    )
    return m


def test_point_lookup_after_small_insert():
    m = make_model()
    m.update(f"INSERT DATA {{ <{EX}s100> <{EX}val> <{EX}extra> . <{EX}new> <{EX}val> <{EX}o_new> }}")
    df = m.query(f"SELECT ?o WHERE {{ <{EX}s100> <{EX}val> ?o }} ORDER BY ?o")
    assert df["o"].to_list() == [f"<{EX}extra>", f"<{EX}o100>"]
    df = m.query(f"SELECT ?o WHERE {{ <{EX}new> <{EX}val> ?o }}")
    assert df["o"].to_list() == [f"<{EX}o_new>"]


def test_describe_subject_after_small_insert():
    m = make_model()
    m.update(f"INSERT DATA {{ <{EX}s7> <{EX}other> \"x\" }}")
    df = m.query(f"SELECT ?p WHERE {{ <{EX}s7> ?p ?o }} ORDER BY ?p")
    assert df["p"].to_list() == [f"<{EX}next>", f"<{EX}other>", f"<{EX}val>"]


def test_path_after_small_insert():
    m = make_model()
    # A shortcut from s10 to a new node, and an edge out of the last node of the chain.
    m.update(
        f"INSERT DATA {{ <{EX}s10> <{EX}next> <{EX}side> . <{EX}s5000> <{EX}next> <{EX}end> }}"
    )
    df = m.query(f"SELECT (COUNT(*) AS ?c) WHERE {{ <{EX}s4990> <{EX}next>+ ?o }}")
    # s4991 .. s5000, then end.
    assert df["c"][0] == 11
    assert m.query(f"ASK {{ <{EX}s9> <{EX}next>+ <{EX}side> }}") is True

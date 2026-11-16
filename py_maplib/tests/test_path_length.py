"""Length-constrained paths: the maplib:path magic predicate and the {n,m} path syntax.

Results are checked against equivalent unions of fixed-length sequence paths."""

import random

import polars as pl
import pytest
from maplib import Model

PREFIXES = """
PREFIX ex: <http://example.org/>
PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#>
"""


def model(turtle: str, graph=None) -> Model:
    m = Model()
    m.reads("@prefix ex: <http://example.org/> .\n" + turtle, format="turtle", graph=graph)
    return m


def rows(m: Model, query: str, **kwargs) -> set:
    df = m.query(PREFIXES + query, **kwargs)
    assert isinstance(df, pl.DataFrame)
    cols = sorted(df.columns)
    return {tuple(r[c] for c in cols) for r in df.iter_rows(named=True)}


def count(m: Model, query: str, **kwargs) -> int:
    return m.query(PREFIXES + query, **kwargs).height


def union_of_sequences(path: str, lengths, with_length: bool) -> str:
    """?s ?o (and ?len) of the paths path/.../path of each length (at least 1)."""
    branches = []
    for k in lengths:
        seq = "/".join([f"({path})"] * k)
        bind = f" BIND({k} AS ?len)" if with_length else ""
        branches.append(f"{{ {{ SELECT DISTINCT ?s ?o WHERE {{ ?s {seq} ?o }} }}{bind} }}")
    return " UNION ".join(branches)


CHAIN = "ex:a ex:p ex:b . ex:b ex:p ex:c . ex:c ex:p ex:d . ex:d ex:p ex:e ."
CYCLE = "ex:a ex:p ex:b . ex:b ex:p ex:c . ex:c ex:p ex:a . ex:c ex:p ex:d ."
DIAMOND = "ex:a ex:p ex:b . ex:a ex:p ex:c . ex:b ex:p ex:d . ex:c ex:p ex:d . ex:d ex:p ex:e ."


@pytest.mark.parametrize("data", [CHAIN, CYCLE, DIAMOND])
@pytest.mark.parametrize("min_len,max_len", [(1, 1), (1, 3), (2, 4), (3, 3), (2, 6)])
def test_lengths_match_union_of_sequences(data, min_len, max_len):
    m = model(data)
    got = rows(
        m,
        f"""SELECT ?s ?o ?len WHERE {{
          ?s maplib:path [ maplib:predicate ex:p ; maplib:minLength {min_len} ;
                           maplib:maxLength {max_len} ; maplib:object ?o ; maplib:length ?len ] }}""",
    )
    expected = rows(
        m,
        f"SELECT ?s ?o ?len WHERE {{ {union_of_sequences('ex:p', range(min_len, max_len + 1), True)} }}",
    )
    assert got == expected
    # Without a length, each pair once.
    q = f"""SELECT ?s ?o WHERE {{
          ?s maplib:path [ maplib:predicate ex:p ; maplib:minLength {min_len} ;
                           maplib:maxLength {max_len} ; maplib:object ?o ] }}"""
    pairs = rows(m, q)
    assert pairs == {(o, s) for (_, o, s) in expected}
    assert count(m, q) == len(pairs)
    # The {n,m} syntax gives the same pairs.
    bounded = rows(m, f"SELECT ?s ?o WHERE {{ ?s ex:p{{{min_len},{max_len}}} ?o }}")
    assert bounded == pairs


def test_chain_lengths():
    m = model(CHAIN)
    got = rows(
        m,
        """SELECT ?o ?len WHERE {
          ex:a maplib:path [ maplib:predicate ex:p ; maplib:minLength 2 ; maplib:maxLength 3 ;
                             maplib:object ?o ; maplib:length ?len ] }""",
    )
    assert got == {(2, "<http://example.org/c>"), (3, "<http://example.org/d>")}


def test_cycle_walks_acyclic_simple():
    m = model(CYCLE)

    def q(mode):
        return f"""SELECT ?o ?len WHERE {{
          ex:a maplib:path [ maplib:predicate ex:p ; maplib:maxLength 6 ; maplib:pathMode "{mode}" ;
                             maplib:object ?o ; maplib:length ?len ] }}"""

    walk = rows(m, q("walk"))
    acyclic = rows(m, q("acyclic"))
    simple = rows(m, q("simple"))
    a = "<http://example.org/a>"
    lens_to_a = lambda rs: sorted(l for (l, o) in rs if o == a)
    # Walks go around the cycle: back at a after 3 and 6 steps.
    assert lens_to_a(walk) == [3, 6]
    assert lens_to_a(acyclic) == []
    assert lens_to_a(simple) == [3]
    # d is reached at 3 by an acyclic path, and at 6 only by a walk.
    d = "<http://example.org/d>"
    assert sorted(l for (l, o) in walk if o == d) == [3, 6]
    assert sorted(l for (l, o) in acyclic if o == d) == [3]
    # Acyclic paths without maxLength end on their own.
    unbounded = rows(
        m,
        """SELECT ?o ?len WHERE {
          ex:a maplib:path [ maplib:predicate ex:p ; maplib:pathMode "acyclic" ;
                             maplib:object ?o ; maplib:length ?len ] }""",
    )
    assert unbounded == acyclic


def test_walk_lengths_need_max_length():
    m = model(CYCLE)
    with pytest.raises(Exception, match="maxLength"):
        m.query(
            PREFIXES
            + "SELECT * WHERE { ex:a maplib:path [ maplib:predicate ex:p ; maplib:object ?o ; maplib:length ?len ] }"
        )
    # Pairs and shortest lengths need no maximum, even on a cycle.
    pairs = rows(m, "SELECT ?o WHERE { ex:a maplib:path [ maplib:predicate ex:p ; maplib:minLength 4 ; maplib:object ?o ] }")
    assert len(pairs) == 4
    shortest = rows(
        m,
        """SELECT ?o ?len WHERE { ex:a maplib:path [ maplib:predicate ex:p ; maplib:minLength 4 ;
             maplib:shortest true ; maplib:object ?o ; maplib:length ?len ] }""",
    )
    assert {l for (l, o) in shortest if o == "<http://example.org/b>"} == {4}
    assert {l for (l, o) in shortest if o == "<http://example.org/a>"} == {6}


def test_diamond_paths():
    m = model(DIAMOND)
    df = m.query(
        PREFIXES
        + """SELECT ?pi ?ni ?n WHERE {
          ex:a maplib:path [ maplib:predicate ex:p ; maplib:minLength 2 ; maplib:maxLength 2 ;
                             maplib:object ex:d ; maplib:pathIndex ?pi ; maplib:nodeIndex ?ni ;
                             maplib:pathNode ?n ] } ORDER BY ?pi ?ni"""
    )
    paths = {}
    for r in df.iter_rows(named=True):
        paths.setdefault(r["pi"], []).append(r["n"])
    got = sorted(paths.values())
    assert got == [
        ["<http://example.org/a>", "<http://example.org/b>", "<http://example.org/d>"],
        ["<http://example.org/a>", "<http://example.org/c>", "<http://example.org/d>"],
    ]
    # Without path outputs the pair is found once.
    assert m.query(PREFIXES + "ASK { ex:a maplib:path [ maplib:predicate ex:p ; maplib:minLength 2 ; maplib:maxLength 2 ; maplib:object ex:d ] }")
    # pathIndex alone: one row per path.
    assert count(m, "SELECT * WHERE { ex:a maplib:path [ maplib:predicate ex:p ; maplib:minLength 2 ; maplib:maxLength 2 ; maplib:object ex:d ; maplib:pathIndex ?pi ] }") == 2


def test_shortest_paths():
    m = model(DIAMOND + " ex:a ex:p ex:x . ex:x ex:p ex:y . ex:y ex:p ex:d .")
    df = m.query(
        PREFIXES
        + """SELECT ?pi ?len WHERE {
          ex:a maplib:path [ maplib:predicate ex:p ; maplib:shortest true ; maplib:object ex:d ;
                             maplib:length ?len ; maplib:pathIndex ?pi ] }"""
    )
    # Both shortest paths a-b-d and a-c-d, not the longer a-x-y-d.
    assert df.height == 2
    assert set(df["len"].to_list()) == {2}


def test_self_loop():
    m = model("ex:a ex:p ex:a . ex:a ex:p ex:b .")

    def q(mode):
        return rows(
            m,
            f"""SELECT ?o ?len WHERE {{ ex:a maplib:path [ maplib:predicate ex:p ; maplib:minLength 0 ;
                  maplib:maxLength 3 ; maplib:pathMode "{mode}" ; maplib:object ?o ; maplib:length ?len ] }}""",
        )

    a, b = "<http://example.org/a>", "<http://example.org/b>"
    assert sorted(l for (l, o) in q("walk") if o == a) == [0, 1, 2, 3]
    assert sorted(l for (l, o) in q("walk") if o == b) == [1, 2, 3]
    assert sorted(l for (l, o) in q("acyclic") if o == a) == [0]
    assert sorted(l for (l, o) in q("simple") if o == a) == [0, 1]
    # A variable at both ends: closed walks.
    closed = rows(m, "SELECT ?x ?len WHERE { ?x maplib:path [ maplib:predicate ex:p ; maplib:maxLength 2 ; maplib:object ?x ; maplib:length ?len ] }")
    assert {l for (l, x) in closed} == {1, 2}


def test_min_length_zero():
    m = model(CHAIN)
    got = rows(m, "SELECT ?o WHERE { ex:b maplib:path [ maplib:predicate ex:p ; maplib:minLength 0 ; maplib:maxLength 1 ; maplib:object ?o ] }")
    assert got == {("<http://example.org/b>",), ("<http://example.org/c>",)}
    # Same as ex:p{0,1} and ex:p?
    assert rows(m, "SELECT ?o WHERE { ex:b ex:p{0,1} ?o }") == got
    assert rows(m, "SELECT ?o WHERE { ex:b ex:p{,1} ?o }") == got
    assert rows(m, "SELECT ?o WHERE { ex:b ex:p? ?o }") == got
    # Unbound start: every node of the steps, at length 0.
    zero = rows(m, "SELECT ?s ?o WHERE { ?s maplib:path [ maplib:predicate ex:p ; maplib:minLength 0 ; maplib:maxLength 0 ; maplib:object ?o ] }")
    assert len(zero) == 5 and all(s == o for (o, s) in zero)


def test_errors():
    m = model(CHAIN)
    for bad in [
        "maplib:minLength 3 ; maplib:maxLength 2",
        "maplib:minLength -1",
        'maplib:minLength "two"',
        'maplib:pathMode "trail"',
        "maplib:maxLenght 3",
        "maplib:length ex:a ; maplib:maxLength 3",
    ]:
        with pytest.raises(Exception):
            m.query(PREFIXES + f"SELECT * WHERE {{ ?s maplib:path [ maplib:predicate ex:p ; {bad} ; maplib:object ?o ] }}")
    for bad in ["ex:p{3,2}", "ex:p{0}", "ex:p{,0}", "ex:p{1001}"]:
        with pytest.raises(Exception):
            m.query(PREFIXES + f"SELECT * WHERE {{ ?s {bad} ?o }}")


def test_bound_and_unbound_ends():
    m = model(CHAIN)
    opts = "maplib:predicate ex:p ; maplib:minLength 2 ; maplib:maxLength 3"
    both_unbound = rows(m, f"SELECT ?s ?o WHERE {{ ?s maplib:path [ {opts} ; maplib:object ?o ] }}")
    assert len(both_unbound) == 5
    # Object constant: searched backwards from it.
    to_e = rows(m, f"SELECT ?s WHERE {{ ?s maplib:path [ {opts} ; maplib:object ex:e ] }}")
    assert to_e == {(s,) for (o, s) in both_unbound if o == "<http://example.org/e>"}
    assert to_e == {("<http://example.org/b>",), ("<http://example.org/c>",)}
    # Both constant.
    assert m.query(PREFIXES + f"ASK {{ ex:a maplib:path [ {opts} ; maplib:object ex:d ] }}")
    assert not m.query(PREFIXES + f"ASK {{ ex:a maplib:path [ {opts} ; maplib:object ex:e ] }}")
    # No object: the starts of paths.
    starts = rows(m, f"SELECT ?s WHERE {{ ?s maplib:path [ {opts} ] }}")
    assert starts == {("<http://example.org/a>",), ("<http://example.org/b>",), ("<http://example.org/c>",)}
    # Bound by VALUES and by another pattern.
    via_values = rows(m, f"SELECT ?s ?o WHERE {{ VALUES ?s {{ ex:b }} ?s maplib:path [ {opts} ; maplib:object ?o ] }}")
    assert via_values == {("<http://example.org/d>", "<http://example.org/b>"), ("<http://example.org/e>", "<http://example.org/b>")}


def test_mixed_with_patterns_and_filters():
    m = model(CHAIN + " ex:a a ex:Start . ex:c a ex:Start . ex:e ex:name \"E\" .")
    got = rows(
        m,
        """SELECT ?s ?o ?len ?name WHERE {
          ?s a ex:Start .
          ?s maplib:path [ maplib:predicate ex:p ; maplib:maxLength 4 ; maplib:object ?o ; maplib:length ?len ] .
          ?o ex:name ?name .
          FILTER(?len > 1)
        }""",
    )
    assert {(r[0], r[3]) for r in got} == {(4, "<http://example.org/a>"), (2, "<http://example.org/c>")}
    # In OPTIONAL and UNION.
    opt = rows(
        m,
        """SELECT ?s ?o WHERE { ?s a ex:Start .
             OPTIONAL { ?s maplib:path [ maplib:predicate ex:p ; maplib:minLength 3 ; maplib:maxLength 3 ; maplib:object ?o ] } }""",
    )
    assert len(opt) == 2


def test_alternatives_and_inverse():
    data = "ex:a ex:p ex:b . ex:c ex:q ex:b . ex:c ex:p ex:d . ex:e ex:r ex:d ."
    m = model(data)
    got = rows(
        m,
        """SELECT ?s ?o ?len WHERE { ?s maplib:path [ maplib:predicate ex:p, ex:r ; maplib:inversePredicate ex:q ;
             maplib:minLength 1 ; maplib:maxLength 3 ; maplib:object ?o ; maplib:length ?len ] }""",
    )
    expected = rows(m, f"SELECT ?s ?o ?len WHERE {{ {union_of_sequences('ex:p|ex:r|^ex:q', range(1, 4), True)} }}")
    assert got == expected
    pairs = rows(m, "SELECT ?s ?o WHERE { ?s (ex:p|ex:r|^ex:q){1,3} ?o }")
    assert pairs == {(o, s) for (_, o, s) in expected}
    # Without maplib:predicate every predicate is a step.
    any_pred = rows(m, "SELECT ?s ?o WHERE { ?s maplib:path [ maplib:maxLength 2 ; maplib:object ?o ] }")
    assert any_pred == rows(m, "SELECT ?s ?o WHERE { ?s (ex:p|ex:q|ex:r){1,2} ?o }")


def test_named_graphs():
    g = "http://example.org/g"
    m = model(CHAIN, graph=g)
    m.reads("@prefix ex: <http://example.org/> . ex:a ex:p ex:z .", format="turtle")
    q = "SELECT ?o WHERE { GRAPH <%s> { ex:a maplib:path [ maplib:predicate ex:p ; maplib:minLength 2 ; maplib:maxLength 2 ; maplib:object ?o ] } }" % g
    assert rows(m, q) == {("<http://example.org/c>",)}
    q = "SELECT ?o WHERE { ex:a maplib:path [ maplib:predicate ex:p ; maplib:minLength 1 ; maplib:maxLength 2 ; maplib:object ?o ] }"
    assert rows(m, q) == {("<http://example.org/z>",)}
    assert rows(m, q, graph=g) == {("<http://example.org/b>",), ("<http://example.org/c>",)}


def test_bounded_syntax_forms():
    m = model(CYCLE)
    for path, lengths in [("ex:p{2}", [2]), ("ex:p{2,4}", [2, 3, 4]), ("ex:p{1,1}", [1])]:
        got = rows(m, f"SELECT ?s ?o WHERE {{ ?s {path} ?o }}")
        expected = rows(m, f"SELECT ?s ?o WHERE {{ {union_of_sequences('ex:p', lengths, False)} }}")
        assert got == expected, path
    # {n,} is n or more: ex:p/ex:p+ for n = 2.
    assert rows(m, "SELECT ?s ?o WHERE { ?s ex:p{2,} ?o }") == rows(m, "SELECT ?s ?o WHERE { ?s ex:p/ex:p+ ?o }")
    assert rows(m, "SELECT ?s ?o WHERE { ?s ex:p{1,} ?o }") == rows(m, "SELECT ?s ?o WHERE { ?s ex:p+ ?o }")
    assert rows(m, "SELECT ?s ?o WHERE { ?s ex:p{0,} ?o }") == rows(m, "SELECT ?s ?o WHERE { ?s ex:p* ?o }")
    # Results are distinct, as for other paths.
    assert count(m, "SELECT ?s ?o WHERE { ?s ex:p{1,6} ?o }") == len(rows(m, "SELECT ?s ?o WHERE { ?s ex:p{1,6} ?o }"))
    # Inside sequences and with inverse.
    assert rows(m, "SELECT ?o WHERE { ex:a ^ex:p{2}/ex:p ?o }") == rows(m, "SELECT ?o WHERE { ex:a ^ex:p/^ex:p/ex:p ?o }")


@pytest.mark.parametrize("seed", range(5))
def test_random_graphs(seed):
    rng = random.Random(seed)
    n = 12
    triples = set()
    for _ in range(25):
        a, b = rng.randrange(n), rng.randrange(n)
        p = rng.choice(["p", "q"])
        triples.add(f"ex:n{a} ex:{p} ex:n{b} .")
    m = model(" ".join(sorted(triples)))
    for min_len, max_len in [(1, 2), (2, 4), (3, 5)]:
        got = rows(
            m,
            f"""SELECT ?s ?o ?len WHERE {{ ?s maplib:path [ maplib:predicate ex:p, ex:q ;
               maplib:minLength {min_len} ; maplib:maxLength {max_len} ; maplib:object ?o ; maplib:length ?len ] }}""",
        )
        expected = rows(m, f"SELECT ?s ?o ?len WHERE {{ {union_of_sequences('ex:p|ex:q', range(min_len, max_len + 1), True)} }}")
        assert got == expected
        # Bound start gives the subset of rows from it.
        start = "<http://example.org/n0>"
        from_start = rows(
            m,
            f"""SELECT ?o ?len WHERE {{ ex:n0 maplib:path [ maplib:predicate ex:p, ex:q ;
               maplib:minLength {min_len} ; maplib:maxLength {max_len} ; maplib:object ?o ; maplib:length ?len ] }}""",
        )
        assert from_start == {(l, o) for (l, o, s) in expected if s == start}
        pairs = rows(m, f"SELECT ?s ?o WHERE {{ ?s (ex:p|ex:q){{{min_len},{max_len}}} ?o }}")
        assert pairs == {(o, s) for (_, o, s) in expected}


LONG_CHAIN = " ".join(f"ex:n{i} ex:p ex:n{i + 1} ." for i in range(50)) + " ex:n0 ex:start true ."


@pytest.mark.parametrize(
    "start",
    [
        "{ SELECT ?s WHERE { ?s ex:start true } }",
        "VALUES ?s { ex:n0 }",
        "{ ?s ex:start true }",
    ],
)
def test_path_starts_from_other_patterns_of_the_group(start):
    # maplib:path is joined after the patterns that bind its start, also when they are not in its
    # basic graph pattern. Evaluated first, it would enumerate the paths from all 51 nodes, more
    # than max_rows.
    m = model(LONG_CHAIN)
    for q in [
        f"SELECT ?s ?o ?i WHERE {{ {start} ?s maplib:path [ maplib:predicate ex:p ; "
        f"maplib:maxLength 3 ; maplib:object ?o ; maplib:pathIndex ?i ] }}",
        f"SELECT ?s ?o ?i WHERE {{ ?s maplib:path [ maplib:predicate ex:p ; "
        f"maplib:maxLength 3 ; maplib:object ?o ; maplib:pathIndex ?i ] {start} }}",
    ]:
        df = m.query(PREFIXES + q, max_rows=20)
        assert sorted(df["o"].to_list()) == [
            "<http://example.org/n1>",
            "<http://example.org/n2>",
            "<http://example.org/n3>",
        ]
    with pytest.raises(Exception, match="more than the configured maximum"):
        m.query(
            PREFIXES + "SELECT ?s ?o ?i WHERE { ?s maplib:path [ maplib:predicate ex:p ; "
            "maplib:maxLength 3 ; maplib:object ?o ; maplib:pathIndex ?i ] }",
            max_rows=20,
        )

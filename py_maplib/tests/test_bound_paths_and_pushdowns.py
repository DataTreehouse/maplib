"""Property paths with a bound end (constants and pushed down values), pushdowns of many values,
and deduplication of small inserts into large tables."""
import random

import pytest
from maplib import Model

EX = "http://ex.org/"
P = f"<{EX}p>"
Q = f"<{EX}q>"
N_NODES = 1500


def node(i):
    return f"<{EX}n{i}>"


@pytest.fixture(scope="module")
def path_model():
    rnd = random.Random(7)
    lines = []
    for _ in range(2500):
        a, b = rnd.randrange(N_NODES), rnd.randrange(N_NODES)
        lines.append(f"{node(a)} {P} {node(b)} .")
    for _ in range(1500):
        a, b = rnd.randrange(N_NODES), rnd.randrange(N_NODES)
        lines.append(f"{node(a)} {Q} {node(b)} .")
    # Long chain, so that closures need many steps.
    for i in range(200):
        lines.append(f"<{EX}c{i}> {P} <{EX}c{i + 1}> .")
    # Literal and blank node objects, so the path lookup has several types.
    lines.append(f'{node(1)} {P} "lit" .')
    lines.append(f"{node(2)} {P} _:b1 .")
    lines.append(f"_:b1 {P} {node(3)} .")
    m = Model()
    m.reads("\n".join(lines) + "\n", format="ntriples")
    return m


PATHS = [
    f"{P}+",
    f"{P}*",
    f"^{P}+",
    f"{P}?",
    f"{P}/{Q}*",
    f"({P}|{Q})+",
    f"({P}/^{Q})*",
    f"(!{Q})+",
    f"^({P}/{Q})+",
]


def pairs(df, s="s", o="o"):
    return set(zip(df[s].to_list(), df[o].to_list()))


def full(m, path):
    return pairs(m.query(f"SELECT ?s ?o WHERE {{ ?s {path} ?o }}"))


@pytest.mark.parametrize("path", PATHS)
def test_constant_subject(path_model, path):
    expected_all = full(path_model, path)
    for start in [node(1), node(2), f"<{EX}c0>", f"<{EX}c150>"]:
        df = path_model.query(f"SELECT ?o WHERE {{ {start} {path} ?o }}")
        assert set(df["o"].to_list()) == {o for s, o in expected_all if s == start}


@pytest.mark.parametrize("path", PATHS)
def test_constant_object(path_model, path):
    expected_all = full(path_model, path)
    for end in [node(3), f"<{EX}c200>", f"<{EX}c10>"]:
        df = path_model.query(f"SELECT ?s WHERE {{ ?s {path} {end} }}")
        assert set(df["s"].to_list()) == {s for s, o in expected_all if o == end}


@pytest.mark.parametrize("path", PATHS)
def test_both_constant(path_model, path):
    expected_all = full(path_model, path)
    for s, o in [(f"<{EX}c0>", f"<{EX}c200>"), (node(1), node(2)), (node(4), node(4))]:
        df = path_model.query(f"ASK {{ {s} {path} {o} }}")
        # A zero length path matches a term with itself, also outside the graph.
        zero_length = s == o and path in (f"{P}*", f"{P}?", f"({P}/^{Q})*")
        assert df == ((s, o) in expected_all or zero_length), (s, o)


@pytest.mark.parametrize("n_values", [20, 1200])
@pytest.mark.parametrize("path", PATHS)
def test_values_bound_subject(path_model, path, n_values):
    expected_all = full(path_model, path)
    starts = [node(i) for i in range(0, 2 * n_values, 2)] + [f"<{EX}c5>"]
    values = " ".join(starts)
    df = path_model.query(
        f"SELECT ?s ?o WHERE {{ VALUES ?s {{ {values} }} ?s {path} ?o }}"
    )
    expected = {(s, o) for s, o in expected_all if s in set(starts)}
    # A zero length path matches each bound value that is a node of the graph, also nodes
    # without triples of the path.
    if path in (f"{P}*", f"{P}?", f"({P}/^{Q})*"):
        triples = path_model.query("SELECT ?s ?o WHERE { ?s ?p ?o }")
        nodes = set(triples["s"].to_list()) | set(triples["o"].cast(str).to_list())
        expected |= {(s, s) for s in starts if s in nodes}
    assert pairs(df) == expected


@pytest.mark.parametrize("path", PATHS)
def test_values_bound_object_from_pattern(path_model, path):
    expected_all = full(path_model, path)
    # ?o is bound by the solution mappings of the preceding triple pattern.
    df = path_model.query(
        f"SELECT ?s ?o WHERE {{ <{EX}c3> {P} ?o . ?s {path} ?o }}"
    )
    assert pairs(df) == {(s, o) for s, o in expected_all if o == f"<{EX}c4>"}


def test_values_with_literal_falls_back(path_model):
    path = f"{P}*"
    expected_all = full(path_model, path)
    df = path_model.query(
        f'SELECT ?s ?o WHERE {{ VALUES ?o {{ "lit" {node(3)} }} ?s {path} ?o }}'
    )
    assert pairs(df) == {
        (s, o) for s, o in expected_all if o in {'"lit"', node(3)}
    }


def test_zero_length_from_unknown_constant(path_model):
    df = path_model.query(f"SELECT ?o WHERE {{ <{EX}unknown> {P}* ?o }}")
    assert df["o"].to_list() == [f"<{EX}unknown>"]
    df = path_model.query(f"SELECT ?o WHERE {{ <{EX}unknown> {P}+ ?o }}")
    assert df.height == 0


@pytest.fixture(scope="module")
def big_model():
    lines = []
    for i in range(20000):
        lines.append(f"<{EX}s{i}> {P} <{EX}o{i % 7000}> .")
        lines.append(f"<{EX}o{i % 7000}> {Q} <{EX}z{i % 50}> .")
    m = Model()
    m.reads("\n".join(lines) + "\n", format="ntriples")
    return m


@pytest.mark.parametrize("n_values", [100, 2000, 15000])
def test_pushdown_of_many_subject_values(big_model, n_values):
    subjects = [f"<{EX}s{i}>" for i in range(0, 20000, 20000 // n_values)]
    values = " ".join(subjects)
    df = big_model.query(
        f"SELECT ?s ?o ?z WHERE {{ VALUES ?s {{ {values} }} ?s {P} ?o . ?o {Q} ?z }}"
    )
    expected = set()
    for s in subjects:
        i = int(s[len(f"<{EX}s"):-1])
        o = i % 7000
        # o{o} has a q edge to z{i' % 50} for each i' with i' % 7000 == o
        for i2 in range(o, 20000, 7000):
            expected.add((s, f"<{EX}o{o}>", f"<{EX}z{i2 % 50}>"))
    assert set(zip(df["s"].to_list(), df["o"].to_list(), df["z"].to_list())) == expected


def test_pushdown_of_many_object_values(big_model):
    objects = [f"<{EX}o{i}>" for i in range(0, 7000, 3)]
    values = " ".join(objects)
    df = big_model.query(
        f"SELECT ?s ?o WHERE {{ VALUES ?o {{ {values} }} ?s {P} ?o }}"
    )
    expected = {
        (f"<{EX}s{i}>", f"<{EX}o{i % 7000}>") for i in range(20000) if (i % 7000) % 3 == 0
    }
    assert pairs(df) == expected


def count(m, p=P):
    return m.query(f"SELECT (COUNT(*) AS ?c) WHERE {{ ?s {p} ?o FILTER(true) }}")["c"][0]


def test_small_inserts_into_large_table():
    m = Model()
    m.reads(
        "\n".join(f"<{EX}s{i}> {P} <{EX}o{i}> ." for i in range(30000)) + "\n",
        format="ntriples",
    )
    assert count(m) == 30000
    # Small insert: duplicates, a new object for an existing subject, and new subjects.
    m.reads(
        f"<{EX}s5> {P} <{EX}o5> .\n"
        f"<{EX}s29999> {P} <{EX}o29999> .\n"
        f"<{EX}s5> {P} <{EX}o6> .\n"
        f"<{EX}new1> {P} <{EX}o1> .\n"
        f"<{EX}new1> {P} <{EX}o1> .\n",
        format="ntriples",
    )
    assert count(m) == 30002
    # Another small insert, now against several segments.
    m.reads(
        f"<{EX}s5> {P} <{EX}o6> .\n<{EX}new1> {P} <{EX}o1> .\n<{EX}new2> {P} <{EX}o2> .\n",
        format="ntriples",
    )
    assert count(m) == 30003
    # A large insert, half of it duplicates.
    m.reads(
        "\n".join(f"<{EX}s{i}> {P} <{EX}o{i}> ." for i in range(15000, 45000)) + "\n",
        format="ntriples",
    )
    assert count(m) == 45003
    # Small inserts and deletes after the large one.
    m.reads(f"<{EX}s44999> {P} <{EX}o44999> .\n<{EX}new3> {P} <{EX}o3> .\n", format="ntriples")
    assert count(m) == 45004
    m.update(
        f"DELETE DATA {{ <{EX}s5> {P} <{EX}o6> . <{EX}s7> {P} <{EX}o7> . <{EX}s7> {P} <{EX}o8> . }}"
    )
    assert count(m) == 45002
    df = m.query(f"SELECT ?o WHERE {{ <{EX}s5> {P} ?o }}")
    assert df["o"].to_list() == [f"<{EX}o5>"]


def test_small_delete_from_large_table():
    m = Model()
    m.reads(
        "\n".join(f"<{EX}s{i}> {P} <{EX}o{i}> ." for i in range(30000)) + "\n",
        format="ntriples",
    )
    m.update(f"DELETE DATA {{ <{EX}s10> {P} <{EX}o10> . <{EX}s11> {P} <{EX}o12> . }}")
    assert count(m) == 29999
    m.reads(f"<{EX}s10> {P} <{EX}o10> .\n<{EX}s12> {P} <{EX}o12> .\n", format="ntriples")
    assert count(m) == 30000

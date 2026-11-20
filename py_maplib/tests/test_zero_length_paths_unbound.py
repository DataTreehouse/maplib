"""Zero-length property paths with both ends unbound match every node of the graph: each subject
and each object, literals too, also nodes without triples of the path (SPARQL 1.1 section 18.4,
ZeroLengthPath; Oxigraph and Jena do the same). Predicates are not nodes unless they occur as a
subject or an object."""

import pytest
from maplib import Model

P = "PREFIX ex: <http://ex/> "
DATA = """@prefix ex: <http://ex/> .
ex:a ex:p ex:b .
ex:c a ex:Node .
ex:d ex:q ex:e .
ex:e ex:name "e" .
"""

A, B, C, D, E = (f"<http://ex/{x}>" for x in "abcde")
NODE = "<http://ex/Node>"
LIT = '"e"'
NODES = {A, B, C, D, E, NODE, LIT}


def model():
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def pairs(m, query):
    df = m.query(P + query)
    return {(str(r["x"]), str(r["y"])) for r in df.iter_rows(named=True)}


def identity(nodes):
    return {(n, n) for n in nodes}


@pytest.mark.parametrize("path", ["ex:p*", "ex:p?", "ex:p{0,2}", "^ex:p*", "ex:p*/ex:q*"])
def test_all_nodes(path):
    m = model()
    got = pairs(m, f"SELECT ?x ?y WHERE {{ ?x {path} ?y }}")
    forward = {(A, B)} if not path.startswith("^") else {(B, A)}
    if path == "ex:p*/ex:q*":
        forward = {(A, B), (D, E)}
    assert got == identity(NODES) | forward


def test_alternative():
    m = model()
    got = pairs(m, "SELECT ?x ?y WHERE { ?x (ex:p|ex:q)* ?y }")
    assert got == identity(NODES) | {(A, B), (D, E)}


def test_same_variable():
    m = model()
    df = m.query(P + "SELECT ?x WHERE { ?x ex:p* ?x }")
    assert {str(x) for x in df["x"].to_list()} == NODES


def test_predicate_without_triples():
    # Every node also matches a path over a predicate that is not in the graph.
    m = model()
    got = pairs(m, "SELECT ?x ?y WHERE { ?x ex:missing* ?y }")
    assert got == identity(NODES)


def test_blank_node_in_query():
    m = model()
    df = m.query(P + "SELECT ?y WHERE { _:s ex:p* ?y }")
    assert {str(y) for y in df["y"].to_list()} == NODES


def test_count():
    m = model()
    df = m.query(P + "SELECT (COUNT(*) AS ?c) WHERE { ?x ex:p* ?y }")
    assert df["c"][0] == len(NODES) + 1


def test_blank_nodes_of_the_graph():
    m = Model()
    m.reads("_:a <http://ex/q> _:b .\n<http://ex/x> <http://ex/p> <http://ex/y> .", format="ntriples")
    df = m.query(P + "SELECT ?x ?y WHERE { ?x ex:p* ?y }")
    assert df.height == 5
    df = m.query(P + "SELECT ?x WHERE { ?x ex:p* ?x FILTER(ISBLANK(?x)) }")
    assert df.height == 2


def test_one_or_more_is_unchanged():
    m = model()
    assert pairs(m, "SELECT ?x ?y WHERE { ?x ex:p+ ?y }") == {(A, B)}


def test_empty_model():
    m = Model()
    assert m.query(P + "SELECT ?x ?y WHERE { ?x ex:p* ?y }").height == 0


# The nodes are read per stored type from the subject and object columns of the triple tables,
# and joined with the followed paths without a DISTINCT: the cases below check the node set and
# that no pair occurs twice.

MIXED = """@prefix ex: <http://ex/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:a ex:p ex:b . ex:b ex:p ex:c . ex:c ex:p ex:a .
ex:loop ex:p ex:loop .
ex:a ex:q ex:b . ex:b ex:q ex:a .
ex:a ex:name "a" , "b"@en , "b"@no , "b" .
ex:b ex:name "a" .
ex:a ex:int 1 , 2 . ex:b ex:int 1 . ex:c ex:long "1"^^xsd:long .
ex:a ex:dbl 1.5e0 , 2.5e0 . ex:b ex:dbl 1.5e0 .
ex:a ex:dec 1.5 . ex:a ex:bool true . ex:b ex:bool true , false .
ex:a ex:date "2020-01-01"^^xsd:date . ex:b ex:date "2020-01-01"^^xsd:date .
ex:a ex:time "2020-01-01T00:00:00"^^xsd:dateTime , "2020-01-01T00:00:00Z"^^xsd:dateTime .
ex:b ex:time "2020-01-01T00:00:00+02:00"^^xsd:dateTime .
_:x ex:p _:y . _:y ex:r ex:a . ex:a ex:r _:x .
ex:a ex:other "x"^^ex:custom .
"""
NODES_QUERY = "SELECT DISTINCT ?x WHERE { { ?x ?p1 ?o } UNION { ?s ?p2 ?x } }"


def rows(df):
    return sorted(tuple(str(v) for v in r) for r in df.rows())


def check_against_patterns(m, path, proper, dataset=""):
    """`path` between two variables gives each node paired with itself and the `proper` pairs
    of different nodes, each once."""
    nodes = rows(m.query(P + NODES_QUERY.replace("WHERE", dataset + " WHERE")))
    got = rows(m.query(P + f"SELECT ?x ?y {dataset} WHERE {{ ?x {path} ?y }}"))
    assert len(got) == len(set(got)), "a pair occurs twice"
    assert set(got) == {(n[0], n[0]) for n in nodes} | proper
    same = rows(m.query(P + f"SELECT ?x {dataset} WHERE {{ ?x {path} ?x }}"))
    assert same == nodes
    c = m.query(P + f"SELECT (COUNT(*) AS ?c) {dataset} WHERE {{ ?x {path} ?y }}")["c"][0]
    assert c == len(got)


ABC = {(f"<http://ex/{x}>", f"<http://ex/{y}>") for x in "abc" for y in "abc" if x != y}
QAB = {(A, B), (B, A)}


def mixed_model():
    m = Model()
    m.reads(MIXED, format="turtle")
    return m


def test_mixed_types_node_set():
    m = mixed_model()
    nodes = rows(m.query(P + NODES_QUERY))
    # 4 IRIs, 2 blank nodes, 4 strings (two plain, two tagged), 2 integers, a long, 2 doubles,
    # a decimal, 2 Booleans, a date, 3 date times and a custom literal
    assert len(nodes) == 23
    df = m.query(P + "SELECT ?x ?y WHERE { ?x ex:missing* ?y }")
    assert rows(df) == [(n[0], n[0]) for n in nodes]


@pytest.mark.parametrize(
    "path,proper",
    [
        ("ex:p*", None),
        ("ex:q*", QAB),
        ("ex:missing*", set()),
        ("(ex:p|ex:q)*", None),
        ("(ex:q/ex:q)*", set()),
        ("(^ex:q)*", QAB),
        ("(ex:p|ex:missing)*", None),
    ],
)
def test_mixed_types_paths(path, proper):
    m = mixed_model()
    if proper is None:
        plus = path[:-1] + "+"
        df = m.query(P + f"SELECT ?x ?y WHERE {{ ?x {plus} ?y FILTER(!sameTerm(?x, ?y)) }}")
        proper = set(rows(df))
        assert ABC <= proper
    check_against_patterns(m, path, proper)


def test_self_loop_and_cycle_once():
    m = mixed_model()
    df = m.query(P + "SELECT ?x ?y WHERE { ?x ex:p* ?y FILTER(?x = ex:loop || ?x = ex:a) }")
    got = rows(df)
    assert got == sorted(
        [("<http://ex/loop>", "<http://ex/loop>")] + [(A, f"<http://ex/{y}>") for y in "abc"]
    )


def test_aggregates_over_all_nodes():
    m = mixed_model()
    n = len(rows(m.query(P + NODES_QUERY)))
    df = m.query(P + "SELECT (COUNT(DISTINCT ?x) AS ?c) (MIN(?y) AS ?m) WHERE { ?x ex:q* ?y }")
    assert df["c"][0] == n
    expected = m.query(P + "SELECT (MIN(?x) AS ?m) WHERE { { ?x ?p1 ?o } UNION { ?s ?p2 ?x } }")
    assert str(df["m"][0]) == str(expected["m"][0])
    df = m.query(P + "SELECT ?x (COUNT(?y) AS ?c) WHERE { ?x ex:q* ?y } GROUP BY ?x HAVING(?c > 1)")
    assert rows(df) == [(A, "2"), (B, "2")]


def test_joined_with_later_patterns():
    m = mixed_model()
    for after, expected in [
        ("?x ex:int ?i", 6),  # a: 2 integers x {a, b}, b: 1 integer x {a, b}
        ("?y ex:bool true", 4),  # a and b reach a and b, which both have true
        ("?y ex:bool false", 2),
        ("?z ex:r ?x", 3),  # _:x to itself, and a to a and b
        ("FILTER(isLiteral(?x))", 23 - 6),
        ("FILTER(isBlank(?y))", 2),
    ]:
        c = m.query(P + f"SELECT (COUNT(*) AS ?c) WHERE {{ ?x ex:q* ?y . {after} }}")["c"][0]
        assert c == expected, after


def test_named_graphs_and_datasets():
    m = Model()
    m.reads(MIXED, format="turtle")
    m.reads('<http://ex/a> <http://ex/q> <http://ex/z> .\n<http://ex/z> <http://ex/name> "a" .\n'
            '<http://ex/z> <http://ex/name> "z" .',
            format="ntriples", graph="http://ex/g1")
    m.reads('<http://ex/z> <http://ex/q> <http://ex/a> .\n_:b <http://ex/p> <http://ex/a> .',
            format="ntriples", graph="http://ex/g2")
    z = "<http://ex/z>"
    # Only the default graph
    check_against_patterns(m, "ex:q*", QAB)
    # The merge of two graphs: nodes of both graphs once each
    check_against_patterns(m, "ex:q*", {(A, z), (z, A)}, "FROM <http://ex/g1> FROM <http://ex/g2>")
    df = m.query(P + "SELECT ?x ?y WHERE { GRAPH <http://ex/g1> { ?x ex:q* ?y } }")
    assert rows(df) == sorted([(A, A), (z, z), ('"a"', '"a"'), ('"z"', '"z"'), (A, z)])
    df = m.query(P + "SELECT ?g (COUNT(*) AS ?c) WHERE { GRAPH ?g { ?x ex:q* ?y } } GROUP BY ?g")
    assert rows(df) == [("<http://ex/g1>", "5"), ("<http://ex/g2>", "4")]


def test_after_updates():
    m = mixed_model()
    m.update(P + "DELETE WHERE { ex:a ex:int ?i }")
    m.update(P + "DELETE WHERE { ?s ex:bool ?b }")
    check_against_patterns(m, "ex:q*", QAB)
    # A small insert stays in the write buffer of its table
    m.update(P + 'INSERT DATA { ex:new ex:q ex:a . ex:new ex:name "new" , "a" . ex:b ex:int 7 }')
    new = "<http://ex/new>"
    check_against_patterns(m, "ex:q*", QAB | {(new, A), (new, B)})
    m.update(P + "DELETE WHERE { ?s ?p ?o }")
    assert m.query(P + "SELECT ?x ?y WHERE { ?x ex:q* ?y }").height == 0


def test_many_nodes_sparse_and_dense_codes():
    # Thousands of nodes, and a table whose codes are few among many (sorted instead of a bitset)
    m = Model()
    n = 5000
    nt = "".join(f'<http://ex/n{i}> <http://ex/v> "{i % 700}" .\n' for i in range(n))
    nt += "".join(f"<http://ex/n{i}> <http://ex/p> <http://ex/n{i + 1}> .\n" for i in range(0, 40, 2))
    m.reads(nt, format="ntriples")
    proper = {(f"<http://ex/n{i}>", f"<http://ex/n{i + 1}>") for i in range(0, 40, 2)}
    check_against_patterns(m, "ex:p*", proper)
    m.reads("<http://ex/n0> <http://ex/p> <http://ex/n4999> .", format="ntriples",
            graph="http://ex/sparse")
    df = m.query(P + "SELECT ?x ?y FROM <http://ex/sparse> WHERE { ?x ex:p* ?y }")
    assert df.height == 3


def test_nodes_from_stored_columns_counter():
    native = pytest.importorskip("maplib.maplib")
    if not hasattr(native, "_fast_path_counts"):
        pytest.skip("no fast path counters in this build")
    m = mixed_model()

    def counts(q):
        native._reset_fast_path_counts()
        m.query(P + q)
        return {k: v for k, v in native._fast_path_counts().items() if v}

    name = "path.nodes_from_stored_columns"
    assert counts("SELECT ?x ?y WHERE { ?x ex:q* ?y }").get(name) == 1
    assert counts("SELECT ?x WHERE { ?x ex:q* ?x }").get(name) == 1
    # Not for one or more, nor when an end is bound by a constant, VALUES or another pattern
    assert name not in counts("SELECT ?x ?y WHERE { ?x ex:q+ ?y }")
    assert name not in counts("SELECT ?y WHERE { ex:a ex:q* ?y }")
    assert name not in counts("SELECT ?x ?y WHERE { VALUES ?x { ex:a } ?x ex:q* ?y }")
    assert name not in counts("SELECT ?x ?y WHERE { ?x ex:q* ?y . ?x ex:int ?i }")

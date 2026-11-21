"""Basic graph patterns with many triple patterns (stars and chains), compared with Oxigraph as
multisets of rows. The data mixes subject and object types (IRIs, blank nodes, several literal
datatypes) and has both small tables and one large enough to be pushed down into, so that
patterns are joined both within one lazy plan and with the values of the solution so far.
Chains through a literal can not match, which is found from the stored types alone; the
solutions of those patterns must still have their variables, unbound, around OPTIONAL, UNION,
VALUES and FILTER."""
import json
import random
from collections import Counter

import pyoxigraph as ox
import pytest
from maplib import Model

EX = "http://ex.org/"
PFX = f"PREFIX : <{EX}>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
N = 60
BIG = 20_000  # rows of :big, more than the small-table limit for pushdowns


def build_data(seed):
    rng = random.Random(seed)
    iris = [f"<{EX}n{i}>" for i in range(N)]
    bnodes = [f"_:b{i}" for i in range(10)]
    subjects = iris + bnodes
    lines = []

    def objects(kind):
        if kind == "iri":
            return rng.choice(iris)
        if kind == "node":
            return rng.choice(iris + bnodes)
        if kind == "int":
            return f'"{rng.randrange(5)}"^^<http://www.w3.org/2001/XMLSchema#integer>'
        if kind == "str":
            return f'"s{rng.randrange(4)}"'
        if kind == "lang":
            return f'"l{rng.randrange(3)}"@{rng.choice(["en", "no"])}'
        if kind == "mixed":
            return objects(rng.choice(["iri", "int", "str"]))
        raise ValueError(kind)

    for pred, kind, n in [
        ("a", "iri", 80),
        ("b", "iri", 70),
        ("c", "node", 60),
        ("d", "int", 50),
        ("e", "str", 50),
        ("f", "lang", 30),
        ("g", "mixed", 60),
        ("h", "iri", 90),
    ]:
        for _ in range(n):
            lines.append(f"{rng.choice(subjects)} <{EX}{pred}> {objects(kind)} .")
    # A large table, to be pushed down into from small solutions.
    for i in range(BIG):
        lines.append(f"<{EX}big{i}> <{EX}big> <{EX}n{i % N}> .")
    for i in range(N):
        lines.append(f"<{EX}n{i}> <{EX}big> <{EX}big{(i * 7) % BIG}> .")
    return "\n".join(sorted(set(lines)))


_ENGINES = {}


def engines(seed):
    if seed not in _ENGINES:
        data = build_data(seed)
        store = ox.Store()
        store.load(data.encode(), ox.RdfFormat.N_TRIPLES)
        m = Model()
        m.reads(data, format="ntriples")
        _ENGINES[seed] = (store, m)
    return _ENGINES[seed]


def ox_term(t):
    if t is None:
        return None
    if isinstance(t, ox.NamedNode):
        return ("uri", t.value)
    if isinstance(t, ox.BlankNode):
        return ("bnode",)
    if t.language:
        return ("lit", t.value, "lang", t.language)
    dt = t.datatype.value
    if dt == "http://www.w3.org/2001/XMLSchema#string":
        return ("lit", t.value, "str")
    return ("lit", t.value, dt)


def json_term(b):
    if b is None:
        return None
    if b["type"] == "uri":
        return ("uri", b["value"])
    if b["type"] == "bnode":
        return ("bnode",)
    if "xml:lang" in b:
        return ("lit", b["value"], "lang", b["xml:lang"])
    dt = b.get("datatype", "http://www.w3.org/2001/XMLSchema#string")
    if dt == "http://www.w3.org/2001/XMLSchema#string":
        return ("lit", b["value"], "str")
    return ("lit", b["value"], dt)


def rows_oxigraph(store, q):
    res = store.query(PFX + q)
    if isinstance(res, ox.QueryBoolean):
        return bool(res)
    variables = [v.value for v in res.variables]
    return variables, Counter(tuple(ox_term(r[v]) for v in variables) for r in res)


def rows_maplib(m, q):
    d = json.loads(m.query(PFX + q, return_json=True))
    if "boolean" in d:
        return d["boolean"]
    variables = d["head"]["vars"]
    return variables, Counter(
        tuple(json_term(b.get(v)) for v in variables) for b in d["results"]["bindings"]
    )


def check(seed, q):
    store, m = engines(seed)
    expected = rows_oxigraph(store, q)
    actual = rows_maplib(m, q)
    if isinstance(expected, bool):
        assert actual == expected
        return expected
    ev, er = expected
    av, ar = actual
    assert sorted(av) == sorted(ev)
    # Same column order for the comparison.
    idx = [av.index(v) for v in ev]
    ar = Counter({tuple(r[i] for i in idx): c for r, c in ar.items()})
    assert ar == er
    return er


def star(preds, s="?s"):
    return " . ".join(f"{s} :{p} ?o{i}" for i, p in enumerate(preds))


def chain(preds, v="?v"):
    return " . ".join(f"{v}{i} :{p} {v}{i + 1}" for i, p in enumerate(preds))


STARS = [
    ["a", "b"],
    ["a", "b", "c", "h"],
    ["a", "c", "d", "e", "f", "g", "h"],
    ["a", "b", "c", "d", "e", "f", "g", "h", "big"],
    ["big", "a", "h"],
    ["g", "g"],
    ["a", "b", "c", "d", "e", "f", "g", "h", "a", "b", "c", "d", "e", "f", "g", "h", "big", "a", "b", "h"],
]

CHAINS = [
    ["a", "b"],
    ["a", "b", "h", "a"],
    ["c", "c", "c"],
    ["big", "big", "a"],
    ["a", "big", "big", "h"],
    ["g", "a"],  # through a mixed object: only its IRIs continue
    ["a", "d", "a"],  # through a literal: empty from the types
    ["h", "e", "b", "c"],  # through a literal: empty from the types
    ["a", "b", "h", "a", "b", "h", "a", "b", "h", "a", "b", "h", "a", "b", "h", "a", "b", "h", "a", "b"],
    ["a", "b", "h", "a", "b", "h", "f", "a", "b", "h", "a", "b", "h", "a", "b", "h", "a", "b"],
]


@pytest.mark.parametrize("seed", [1, 2])
@pytest.mark.parametrize("preds", STARS, ids=lambda p: "-".join(p))
def test_star(seed, preds):
    check(seed, f"SELECT * WHERE {{ {star(preds)} }}")
    check(seed, f"SELECT (COUNT(*) AS ?c) WHERE {{ {star(preds)} }}")


@pytest.mark.parametrize("seed", [1, 2])
@pytest.mark.parametrize("preds", CHAINS, ids=lambda p: "-".join(p))
def test_chain(seed, preds):
    check(seed, f"SELECT * WHERE {{ {chain(preds)} }}")
    check(seed, f"SELECT (COUNT(*) AS ?c) WHERE {{ {chain(preds)} }}")


@pytest.mark.parametrize("seed", [1, 2])
@pytest.mark.parametrize("preds", [["a", "d", "a"], ["h", "e", "b"], ["a", "b", "h"]], ids=lambda p: "-".join(p))
def test_chain_in_optional(seed, preds):
    # The left side keeps its solutions; the chain's variables are unbound when it can not match.
    check(seed, f"SELECT * WHERE {{ ?v0 :b ?x OPTIONAL {{ {chain(preds)} }} }}")
    check(seed, f"SELECT ?v0 ?v3 WHERE {{ ?v0 :b ?x OPTIONAL {{ {chain(preds)} }} }}")
    check(seed, f"SELECT (COUNT(?v2) AS ?c) (COUNT(*) AS ?n) WHERE {{ ?v0 :b ?x OPTIONAL {{ {chain(preds)} }} }}")


@pytest.mark.parametrize("seed", [1, 2])
def test_empty_chain_in_union(seed):
    check(seed, f"SELECT * WHERE {{ {{ {chain(['a', 'd', 'a'])} }} UNION {{ ?v0 :a ?v1 }} }}")
    check(seed, f"SELECT * WHERE {{ {{ ?v0 :e ?v1 }} UNION {{ {chain(['a', 'e', 'b'])} }} }}")


@pytest.mark.parametrize("seed", [1, 2])
def test_values_around(seed):
    nodes = " ".join(f":n{i}" for i in range(0, N, 3))
    check(seed, f"SELECT * WHERE {{ VALUES ?s {{ {nodes} }} {star(['a', 'b', 'h'])} }}")
    check(seed, f"SELECT * WHERE {{ {star(['a', 'b', 'h'])} VALUES ?s {{ {nodes} }} }}")
    check(seed, f"SELECT * WHERE {{ VALUES ?v0 {{ {nodes} }} {chain(['a', 'big', 'big'])} }}")
    check(seed, f"SELECT * WHERE {{ VALUES ?v1 {{ {nodes} }} {chain(['a', 'd', 'a'])} }}")
    check(seed, f'SELECT * WHERE {{ VALUES ?o0 {{ 1 2 "s1" }} {star(["d", "a"])} }}')
    check(seed, f'SELECT * WHERE {{ VALUES ?o1 {{ 1 "s1" :n3 }} {star(["a", "g"])} }}')


@pytest.mark.parametrize("seed", [1, 2])
def test_filter_around(seed):
    check(seed, f"SELECT * WHERE {{ {star(['a', 'd', 'h'])} FILTER(?o1 > 2) }}")
    check(seed, f"SELECT * WHERE {{ {star(['a', 'e', 'b'])} FILTER(?o1 = \"s1\") }}")
    check(seed, f"SELECT * WHERE {{ {chain(['a', 'b', 'h'])} FILTER(?v0 != ?v3) }}")
    check(seed, f"SELECT * WHERE {{ {chain(['a', 'd', 'a'])} FILTER(!BOUND(?v2)) }}")
    check(seed, f"SELECT * WHERE {{ ?v0 :a ?x FILTER NOT EXISTS {{ {chain(['a', 'd', 'a'])} }} }}")
    check(seed, f"SELECT * WHERE {{ ?v0 :a ?x FILTER EXISTS {{ {chain(['a', 'b'])} }} }}")
    check(seed, f"SELECT * WHERE {{ ?v0 :a ?x MINUS {{ {chain(['a', 'd', 'a'])} }} }}")


@pytest.mark.parametrize("seed", [1, 2])
def test_blank_nodes_and_constants(seed):
    check(seed, "SELECT * WHERE { ?s :a [ :b ?x ; :h ?y ] }")
    check(seed, "SELECT * WHERE { ?s :a [ :d ?x ; :b ?y ] }")  # literal subject: empty
    check(seed, "SELECT * WHERE { ?s :c ?b . ?b :a ?x . ?b :h ?y }")
    check(seed, "SELECT * WHERE { :n1 :a ?x . ?x :b ?y . ?y :h ?z }")
    check(seed, "SELECT * WHERE { ?x :b :n2 . ?x :a ?y . ?y :h ?z }")
    check(seed, "SELECT * WHERE { ?x :d 3 . ?x :a ?y . ?y :b ?z }")
    check(seed, "SELECT * WHERE { ?x :a ?y . ?y :d \"s1\" }")  # wrong literal type
    check(seed, "SELECT * WHERE { ?x :e \"s1\" . ?x :a ?y }")
    check(seed, "ASK { ?x :a ?y . ?y :d ?z . ?z :a ?w }")
    check(seed, "ASK { ?x :a ?y . ?y :b ?z }")


@pytest.mark.parametrize("seed", [1, 2])
def test_variable_predicates(seed):
    check(seed, "SELECT * WHERE { ?x :a ?y . ?y ?p ?z . ?z :b ?w }")
    check(seed, "SELECT * WHERE { ?x :d ?y . ?y ?p ?z }")  # literal subject: empty
    check(seed, "SELECT * WHERE { ?x ?p ?y . ?p :a ?z }")  # no predicate is a subject of :a
    check(seed, "SELECT (COUNT(*) AS ?c) WHERE { ?x :a ?y . ?y ?p ?z . ?z :a ?w . ?w ?q ?u }")


@pytest.mark.parametrize("seed", [1, 2])
def test_missing_predicate(seed):
    check(seed, "SELECT * WHERE { ?x :a ?y . ?y :nothere ?z . ?z :b ?w }")
    check(seed, "SELECT * WHERE { ?x :a ?y OPTIONAL { ?y :nothere ?z . ?z :b ?w } }")
    check(seed, "SELECT (COUNT(*) AS ?c) (COUNT(?z) AS ?cz) WHERE { ?x :a ?y OPTIONAL { ?y :nothere ?z } }")


@pytest.mark.parametrize("seed", [1, 2])
def test_ordered_and_sliced(seed):
    # The empty chain's variables are still projected, and ORDER BY on them works.
    check(seed, f"SELECT ?v0 ?v2 WHERE {{ ?v0 :b ?x OPTIONAL {{ {chain(['a', 'd', 'a'])} }} }} ORDER BY ?v2 ?v0")
    check(seed, f"SELECT DISTINCT ?v0 WHERE {{ {chain(['a', 'b', 'h', 'a'])} }} ORDER BY ?v0 LIMIT 5")
    check(seed, f"SELECT ?v1 (COUNT(*) AS ?c) WHERE {{ {chain(['a', 'b', 'h'])} }} GROUP BY ?v1")
    check(seed, f"SELECT ?v1 (COUNT(*) AS ?c) WHERE {{ {chain(['a', 'd', 'a'])} }} GROUP BY ?v1")


@pytest.mark.parametrize("seed", [1, 2])
def test_optional_with_small_left(seed):
    # A small left side: the right side is evaluated with its values (BSBM explore Q7/Q8 shapes).
    check(seed, "SELECT * WHERE { :n1 :a ?x OPTIONAL { ?x :b ?y } OPTIONAL { ?x :d ?z } }")
    check(seed, "SELECT * WHERE { ?s :b :n2 . ?s :a ?x OPTIONAL { ?s :d ?d } OPTIONAL { ?s :e ?e } OPTIONAL { ?s :f ?f } }")
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { ?s :b ?y . ?y :h ?z OPTIONAL { ?s :d ?d } OPTIONAL { ?y :c ?c } } }")
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { ?s :big ?b . ?b :big ?c } }")
    check(seed, "SELECT * WHERE { ?b :big :n4 OPTIONAL { ?b :big ?c } }")
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { ?s :d ?d FILTER(?d > 1) } }")
    check(seed, "SELECT * WHERE { ?s :a ?x OPTIONAL { ?s :d ?d FILTER(?d > 1 && ?x != :n1) } }")
    check(seed, "SELECT * WHERE { ?s :c ?b OPTIONAL { ?b :a ?x } }")  # blank node values
    check(seed, "SELECT * WHERE { ?s :g ?o OPTIONAL { ?o :a ?x } }")  # values of several types
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { { ?s :b ?y } UNION { ?s :h ?y } } }")
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { SELECT ?s (COUNT(*) AS ?c) WHERE { ?s :b ?y } GROUP BY ?s } }")


@pytest.mark.parametrize("seed", [1, 2])
def test_optional_left_unbound_and_new_values(seed):
    # Left values that are unbound in some rows are compatible with anything on the right.
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { ?s :d ?d } OPTIONAL { ?s :e ?d } }")
    check(seed, "SELECT * WHERE { ?s :a ?x OPTIONAL { ?x :b ?y } OPTIONAL { ?y :h ?z } }")
    # Left values that are not in the model.
    check(seed, "SELECT * WHERE { VALUES ?s { :n1 :n2 :nothere } OPTIONAL { ?s :a ?x } }")
    check(seed, "SELECT * WHERE { BIND(IRI(CONCAT(STR(:), \"n\", \"5\")) AS ?s) OPTIONAL { ?s :a ?x } }")
    check(seed, "SELECT * WHERE { BIND(:nothere AS ?s) OPTIONAL { ?s :a ?x } }")
    # Right side variables unbound in some of its solutions.
    check(seed, "SELECT * WHERE { ?s :a :n3 . ?s :b ?y OPTIONAL { OPTIONAL { ?s :d ?y } ?s :h ?z } }")


@pytest.mark.parametrize("seed", [1, 2])
def test_minus_in_optional_multi_type(seed):
    # MINUS on a variable with IRIs on one side and IRIs or blank nodes on the other.
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { ?s :b ?y MINUS { ?y :a :n1 } } }")


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_optional_union_unbound_shared(seed):
    # A right solution that leaves a shared variable unbound is compatible with every left one.
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { { ?s :b ?y } UNION { ?z :h ?y } } }")
    check(seed, "SELECT * WHERE { ?s :a ?o OPTIONAL { { ?s :b ?y } UNION { ?o :h ?y } } }")
    check(seed, "SELECT * WHERE { ?s :a :n3 { { ?s :b ?y } UNION { ?z :h ?y } } }")
    check(seed, "SELECT * WHERE { ?s :a ?o OPTIONAL { ?y :h ?o OPTIONAL { ?s :b ?y } } }")
    check(seed, "SELECT * WHERE { ?s :a ?o OPTIONAL { ?o :h ?y VALUES ?s { :n3 UNDEF } } }")
    check(seed, "SELECT * WHERE { ?s :a ?o OPTIONAL { { ?s :b ?y } UNION { ?z :h ?y } FILTER(?o != ?y) } }")


@pytest.mark.parametrize("seed", [1, 2])
def test_optional_subquery_sees_no_left_values(seed):
    # A subquery is evaluated on its own: its LIMIT and aggregates are over all its solutions,
    # not those of the left side's values.
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { SELECT ?s ?y WHERE { ?s :b ?y } ORDER BY ?s ?y LIMIT 2 } }")
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { SELECT ?s (COUNT(*) AS ?c) WHERE { ?s :b ?y } GROUP BY ?s ORDER BY DESC(?c) ?s LIMIT 1 } }")
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { SELECT (COUNT(*) AS ?c) WHERE { ?s :b ?y } } }")
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { ?s :b ?y { SELECT ?y WHERE { ?y :h ?z } ORDER BY ?y LIMIT 3 } } }")


def test_after_small_inserts():
    # Small inserts go to the tables' write buffers, which the estimates must count too.
    data = build_data(3)
    store = ox.Store()
    store.load(data.encode(), ox.RdfFormat.N_TRIPLES)
    m = Model()
    m.reads(data, format="ntriples")
    ins = (PFX + "INSERT DATA { :n1 :a :n2 . :n1 :a :n3 . :n1 :a :n4 . :n2 :b :n1 . :n2 :h :n5 . "
           ":n5 :d 7 . :n4 :e \"s9\" . :big1 :big :n7 . _:x :c :n1 }")
    store.update(ins)
    m.update(ins)
    _ENGINES["ins"] = (store, m)
    for q in [
        f"SELECT * WHERE {{ {star(['a', 'b', 'h'])} }}",
        f"SELECT * WHERE {{ {chain(['a', 'b', 'h', 'a'])} }}",
        f"SELECT * WHERE {{ {chain(['a', 'h', 'd'])} }}",
        "SELECT * WHERE { :n1 :a ?x OPTIONAL { ?x :b ?y } OPTIONAL { ?x :e ?z } }",
        "SELECT * WHERE { ?s :a :n3 OPTIONAL { ?s :big ?b } }",
        f"SELECT (COUNT(*) AS ?c) WHERE {{ {star(['a', 'a', 'a'])} }}",
    ]:
        check("ins", q)


@pytest.mark.parametrize("seed", [1, 2])
def test_optional_subquery_with_left_keys(seed):
    # A sub-select grouped by (or projecting) the variable it shares with the left side is
    # evaluated with the left side's values of it, also when it projects it under another name,
    # and gives the same left join; with a LIMIT, a group without variables or a key that is not
    # grouped by, it is not.
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { SELECT ?s (COUNT(*) AS ?c) WHERE { ?s :b ?y } GROUP BY ?s } }")
    check(seed, "SELECT * WHERE { ?t :a :n3 OPTIONAL { SELECT (?s AS ?t) (COUNT(*) AS ?c) WHERE { ?s :b ?y } GROUP BY ?s } }")
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { SELECT ?s ?y WHERE { ?s :b ?y } } }")
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { SELECT DISTINCT ?s WHERE { ?s :b ?y FILTER(?y != :n1) } } }")
    check(seed, "SELECT * WHERE { ?s :a ?o OPTIONAL { SELECT ?s (SUM(?d) AS ?sum) WHERE { ?s :d ?d } GROUP BY ?s HAVING (SUM(?d) > 1) } }")
    check(seed, "SELECT * WHERE { ?y :a :n3 OPTIONAL { SELECT ?y (COUNT(*) AS ?c) WHERE { ?s :b ?y } GROUP BY ?s ?y } }")
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { SELECT ?s (COUNT(?y) AS ?c) WHERE { ?s :b ?y } GROUP BY ?s ORDER BY ?s LIMIT 2 } }")
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { SELECT ?y (COUNT(*) AS ?c) WHERE { ?s :b ?y } GROUP BY ?y } }")
    # The sub-select joined with more patterns inside the OPTIONAL, and inside a sub-select
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { { SELECT ?s (COUNT(*) AS ?c) WHERE { ?s :b ?y } GROUP BY ?s } ?s :c ?z OPTIONAL { ?z :h ?w } } }")
    check(seed, "SELECT * WHERE { ?t :a :n3 OPTIONAL { SELECT (?s AS ?t) (MAX(?c) AS ?m) WHERE { { SELECT ?s ?y (COUNT(*) AS ?c) WHERE { ?s :b ?y } GROUP BY ?s ?y } OPTIONAL { ?y :h ?w } } GROUP BY ?s } }")
    check(seed, "SELECT * WHERE { ?s :a :n3 OPTIONAL { ?s :c ?z { SELECT ?z WHERE { ?z :h ?w } ORDER BY ?z LIMIT 3 } } }")

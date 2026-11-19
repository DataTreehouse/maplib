"""COUNT(*) over small join shapes (stars, chains, EXISTS, NOT EXISTS, OPTIONAL, MINUS and UNION
with constant predicates) is answered from the stored dictionary codes without building the
joined rows. Each query is checked against rdflib, and against the general path, which an extra
FILTER(true) in the outer group makes it take."""
import itertools

import pytest
import rdflib
from maplib import Model
from maplib import maplib as native

EX = "http://ex.org/"


def data():
    lines = []

    def t(s, p, o):
        lines.append(f"{s} <{EX}{p}> {o} .")

    iri = lambda i: f"<{EX}s{i}>"
    # p: IRI to IRI, with several objects per subject, and blank node subjects and objects.
    for i in range(8):
        for j in range(i % 3 + 1):
            t(iri(i), "p", iri((i + j + 1) % 10))
    t("_:b1", "p", iri(2))
    t("_:b2", "p", "_:b1")
    t(iri(3), "p", "_:b2")
    # q: IRIs, strings, language strings, integers and doubles as objects.
    for i in range(0, 10, 2):
        t(iri(i), "q", iri(i + 1))
        t(iri(i), "q", f'"v{i}"')
        t(iri(i), "q", f'"v{i}"@en')
        t(iri(i), "q", f'"{i}"^^<http://www.w3.org/2001/XMLSchema#integer>')
    t("_:b1", "q", '"blank"')
    t(iri(5), "q", '"1.5"^^<http://www.w3.org/2001/XMLSchema#double>')
    # r: a few subjects, with dates and IRIs.
    for i in [1, 2, 3, 5, 8]:
        t(iri(i), "r", iri(i * 2 % 10))
        t(iri(i), "r", '"2020-01-01"^^<http://www.w3.org/2001/XMLSchema#date>')
    t("_:b2", "r", iri(4))
    # s: strings that are also objects of q, to join on literal objects.
    t(iri(7), "s", '"v2"')
    t(iri(9), "s", '"v4"')
    return "\n".join(lines)


PREDS = ["p", "q", "r", "missing"]

TWO = [
    # Stars and chains.
    "?s :A ?o1 . ?s :B ?o2",
    "?a :A ?b . ?b :B ?c",
    "?a :A ?b . ?c :B ?b",
    "?b :A ?a . ?b :B ?c",
    # Two patterns with no shared variable.
    "?a :A ?b . ?c :B ?d",
    # EXISTS and NOT EXISTS.
    "?s :A ?o1 FILTER EXISTS { ?s :B ?o2 }",
    "?s :A ?o1 FILTER NOT EXISTS { ?s :B ?o2 }",
    "?a :A ?b FILTER EXISTS { ?b :B ?c }",
    "?a :A ?b FILTER NOT EXISTS { ?b :B ?c }",
    "?a :A ?b FILTER EXISTS { ?x :B ?y }",
    "?a :A ?b FILTER NOT EXISTS { ?x :B ?y }",
    # OPTIONAL.
    "?s :A ?o1 OPTIONAL { ?s :B ?o2 }",
    "?a :A ?b OPTIONAL { ?b :B ?c }",
    "?a :A ?b OPTIONAL { ?c :B ?a }",
    "?a :A ?b OPTIONAL { ?x :B ?y }",
    # MINUS, also with no shared variable, which removes nothing.
    "?s :A ?o1 MINUS { ?s :B ?o2 }",
    "?a :A ?b MINUS { ?b :B ?c }",
    "?a :A ?b MINUS { ?x :B ?y }",
    # UNION.
    "{ ?s :A ?o } UNION { ?s :B ?o }",
    "{ ?s :A ?o } UNION { ?x :B ?y }",
]

THREE = [
    "?s :A ?o1 . ?s :B ?o2 . ?s :C ?o3",
    "?a :A ?b . ?b :B ?c . ?c :C ?d",
    "?a :A ?b . ?b :B ?c . ?a :C ?d",
    "?s :A ?o1 . ?s :B ?o2 FILTER EXISTS { ?s :C ?o3 }",
    "?s :A ?o1 FILTER EXISTS { ?s :B ?o2 . ?s :C ?o3 }",
    "?a :A ?b . ?b :B ?c FILTER EXISTS { ?c :C ?d }",
    "?a :A ?b FILTER NOT EXISTS { ?b :B ?c . ?c :C ?d }",
    "?s :A ?o1 . ?s :B ?o2 OPTIONAL { ?s :C ?o3 }",
    "?s :A ?o1 OPTIONAL { ?s :B ?o2 . ?s :C ?o3 }",
    "?a :A ?b . ?b :B ?c OPTIONAL { ?c :C ?d }",
    "?a :A ?b OPTIONAL { ?b :B ?c . ?c :C ?d }",
    "?a :A ?b OPTIONAL { ?b :B ?c } OPTIONAL { ?a :C ?d }",
    "?s :A ?o1 . ?s :B ?o2 MINUS { ?s :C ?o3 }",
    "?s :A ?o1 MINUS { ?s :B ?o2 . ?s :C ?o3 }",
    "?a :A ?b . ?b :B ?c MINUS { ?c :C ?d }",
    "?a :A ?b MINUS { ?b :B ?c . ?c :C ?d }",
    "{ ?s :A ?o1 } UNION { ?s :B ?o1 } ?s :C ?o2",
    "{ ?s :A ?o1 } UNION { ?s :B ?o1 } FILTER EXISTS { ?s :C ?o2 }",
    "?s :A ?o1 OPTIONAL { { ?s :B ?o2 } UNION { ?s :C ?o2 } }",
    "?s :A ?o1 MINUS { { ?s :B ?o2 } UNION { ?x :C ?y } }",
    # Blank nodes in the pattern are variables.
    "?s :A [] . ?s :B _:x . _:x :C ?o",
]

# Shapes that are not answered from the codes: they must still be right.
OTHER = [
    # A cycle.
    "?a :A ?b . ?b :B ?c . ?c :C ?a",
    "?a :A ?b . ?a :B ?b",
    # Two shared variables, and a variable an OPTIONAL binds that is used later.
    "?a :A ?b OPTIONAL { ?a :B ?b }",
    "?a :A ?b MINUS { ?a :B ?b }",
    "?a :A ?b OPTIONAL { ?b :B ?c } ?c :C ?d",
    "?a :A ?b OPTIONAL { ?b :B ?c } FILTER EXISTS { ?c :C ?d }",
    # An OPTIONAL with a filter.
    "?a :A ?b OPTIONAL { ?b :B ?c FILTER(ISIRI(?c)) }",
    # A constant, and a variable predicate.
    f"?a :A <{EX}s2> . ?a :B ?c",
    "?a :A ?b . ?b ?p ?c",
]


def query(body, general, agg="COUNT(*)"):
    f = " FILTER(true)" if general else ""
    return f"PREFIX : <{EX}> SELECT ({agg} AS ?c) WHERE {{ {body}{f} }}"


def bodies(templates, n):
    for tpl in templates:
        for preds in itertools.product(PREDS, repeat=n):
            body = tpl
            for name, p in zip("ABC", preds):
                body = body.replace(f":{name} ", f":{p} ")
            yield body


@pytest.fixture(scope="module")
def m():
    m = Model()
    m.reads(data(), format="ntriples")
    return m


@pytest.fixture(scope="module")
def g():
    g = rdflib.Graph()
    g.parse(data=data(), format="nt")
    return g


def taken(m, q):
    """The result of `q`, and whether it was counted on the codes."""
    native._reset_fast_path_counts()
    df = m.query(q)
    return df, native._fast_path_counts()["count.joins_on_codes"] > 0


def check(m, g, body, agg="COUNT(*)", on_codes=None):
    q = query(body, False, agg)
    fast, hit = taken(m, q)
    if on_codes is not None:
        assert hit == on_codes, body
    expected = [int(r[-1]) for r in g.query(q)]
    assert fast["c"].to_list() == expected, body
    general, hit = taken(m, query(body, True, agg))
    assert not hit, body
    assert fast["c"].to_list() == general["c"].to_list(), body
    assert fast["c"].dtype == general["c"].dtype, body


# Joins and attachments on a key that some surviving table stores uncoded fall back to the general
# path: the objects of :q include xsd:integer and xsd:double literals, those of :r xsd:date
# literals. An OPTIONAL keeps all the tables of its own patterns, so it falls back too.
UNCODED_KEYS = {
    "?a :q ?b . ?c :q ?b",
    "?a :r ?b . ?c :r ?b",
    "?a :p ?b OPTIONAL { ?c :r ?a }",
    "?a :q ?b OPTIONAL { ?c :q ?a }",
    "?a :q ?b OPTIONAL { ?c :r ?a }",
    "?a :r ?b OPTIONAL { ?c :r ?a }",
}


@pytest.mark.parametrize("body", list(bodies(TWO, 2)))
def test_two_patterns(m, g, body):
    check(m, g, body, on_codes=body not in UNCODED_KEYS)


def test_uncoded_keys_are_two_pattern_shapes():
    assert UNCODED_KEYS <= set(bodies(TWO, 2))


@pytest.mark.parametrize("body", list(bodies(THREE, 3)))
def test_three_patterns(m, g, body):
    check(m, g, body, on_codes=True)


@pytest.mark.parametrize("body", list(bodies(OTHER, 3)))
def test_other_shapes(m, g, body):
    check(m, g, body, on_codes=False)


def test_count_of_a_variable(m, g):
    for agg in ["COUNT(?s)", "COUNT(?o2)", "COUNT(DISTINCT ?s)", "COUNT(*) AS ?x) (COUNT(?s)"]:
        check(m, g, "?s :p ?o1 OPTIONAL { ?s :r ?o2 }", agg)
        check(m, g, "?s :p ?o1 . ?s :q ?o2", agg)


def test_known_counts(m):
    def c(body):
        return m.query(query(body, False))["c"][0]

    # s0..s7 have 1, 2, 3, 1, 2, 3, 1, 2 objects of p, s3 one more, and two blank nodes one.
    assert c("?s :p ?o") == 18
    # s0, s2, s4, s6 have 4 objects of q and 1, 3, 2, 1 of p; s5 has 1 of q and 3 of p, and
    # _:b1 one of each.
    assert c("?s :p ?o1 . ?s :q ?o2") == 4 * (1 + 3 + 2 + 1) + 3 + 1
    assert c("?s :p ?o1 FILTER EXISTS { ?s :q ?o2 }") == 1 + 3 + 2 + 1 + 3 + 1
    assert c("?s :p ?o1 MINUS { ?s :q ?o2 }") == 18 - 11
    assert c("?s :p ?o1 OPTIONAL { ?s :q ?o2 }") == 32 + 18 - 11
    # The literal objects of q join the literal objects of s.
    assert c("?a :q ?x . ?b :s ?x") == 2
    assert c("{ ?s :p ?o } UNION { ?s :missing ?o }") == 18


def test_empty_model():
    m = Model()
    for body in bodies(TWO[:6] + THREE[:3], 3):
        assert m.query(query(body, False))["c"].to_list() == [0]


def test_after_insert_and_delete():
    m = Model()
    m.reads(data(), format="ntriples")
    body = "?s :p ?o1 . ?s :r ?o2"

    def both():
        return m.query(query(body, False))["c"][0], m.query(query(body, True))["c"][0]

    before, general = both()
    assert before == general
    # s9 has no objects of p, so the count stays; s1 has two.
    m.update(f"INSERT DATA {{ <{EX}s9> <{EX}r> <{EX}s1> . <{EX}s1> <{EX}r> <{EX}s7> . }}")
    assert both() == (before + 2, before + 2)
    m.update(f"DELETE DATA {{ <{EX}s1> <{EX}r> <{EX}s7> . <{EX}s1> <{EX}r> <{EX}s2> . }}")
    assert both() == (before - 2, before - 2)


@pytest.mark.parametrize("body", [b for b in bodies(TWO + THREE[:16], 3)][::7])
def test_after_small_inserts(body):
    """Small inserts and deletes go to the tables' write buffers, which the counts read too."""
    m = Model()
    m.reads(data(), format="ntriples")
    m.update(
        f"""INSERT DATA {{ <{EX}s9> <{EX}p> <{EX}s1> . <{EX}s1> <{EX}q> <{EX}s3> .
        <{EX}s3> <{EX}r> <{EX}s5> . <{EX}s5> <{EX}r> "x" . _:n <{EX}p> <{EX}s9> . }}"""
    )
    m.update(f"DELETE DATA {{ <{EX}s2> <{EX}q> <{EX}s3> . <{EX}s0> <{EX}p> <{EX}s1> . }}")
    g = rdflib.Graph()
    g.parse(data=data(), format="nt")
    g.update(
        f"""INSERT DATA {{ <{EX}s9> <{EX}p> <{EX}s1> . <{EX}s1> <{EX}q> <{EX}s3> .
        <{EX}s3> <{EX}r> <{EX}s5> . <{EX}s5> <{EX}r> "x" . _:n <{EX}p> <{EX}s9> . }}"""
    )
    g.update(f"DELETE DATA {{ <{EX}s2> <{EX}q> <{EX}s3> . <{EX}s0> <{EX}p> <{EX}s1> . }}")
    check(m, g, body)


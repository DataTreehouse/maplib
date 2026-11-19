"""Patterns with a single IRI subject read that subject's rows eagerly from triples in memory and
leave tables without it out of the query plan. Check the answers against filtering all triples."""
import json
import pytest
from maplib import Model

EX = "http://ex.org/"
DATA = f"""
<{EX}a> <{EX}p> <{EX}b> .
<{EX}a> <{EX}p> "x" .
<{EX}a> <{EX}q> "x"@en .
<{EX}a> <{EX}q> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
<{EX}a> <{EX}r> "2020-01-01T00:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<{EX}a> <{EX}r> _:b1 .
<{EX}b> <{EX}p> <{EX}a> .
<{EX}b> <{EX}q> "y" .
_:b1 <{EX}p> <{EX}a> .
<{EX}c> <{EX}s> "only c" .
"""
# Many subjects per table, so the sparse index has several keys and lookups take a range.
BULK = "\n".join(f'<{EX}n{i:05d}> <{EX}p> "v{i}" .\n<{EX}n{i:05d}> <{EX}q> <{EX}n{(i + 1) % 3000:05d}> .' for i in range(3000))

SUBJECTS = [f"{EX}a", f"{EX}b", f"{EX}c", f"{EX}n00000", f"{EX}n01500", f"{EX}n02999", f"{EX}missing", f"{EX}p"]


def bindings(m, q):
    """The solutions as sorted tuples of canonical SPARQL JSON terms."""
    d = json.loads(m.query(q, return_json=True))
    vs = d["head"]["vars"]
    return sorted(tuple(json.dumps(b.get(v), sort_keys=True) for v in vs) for b in d["results"]["bindings"])


def expected(m, s, p=None):
    rows = bindings(m, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
    subj = json.dumps({"type": "uri", "value": s}, sort_keys=True)
    out = [(pp, o) for (ss, pp, o) in rows if ss == subj]
    if p is not None:
        pj = json.dumps({"type": "uri", "value": p}, sort_keys=True)
        out = [(o,) for (pp, o) in out if pp == pj]
    return sorted(out)


def got(m, s):
    return bindings(m, f"SELECT ?p ?o WHERE {{ <{s}> ?p ?o }}")


@pytest.fixture
def m():
    m = Model()
    m.reads(DATA + BULK, format="ntriples")
    # A second segment in some tables.
    m.reads(f'<{EX}a> <{EX}p> "later" .\n<{EX}n01500> <{EX}p> "later" .', format="ntriples")
    return m


@pytest.mark.parametrize("s", SUBJECTS)
def test_variable_predicate(m, s):
    assert got(m, s) == expected(m, s)


@pytest.mark.parametrize("s", SUBJECTS)
def test_fixed_predicate_and_object(m, s):
    for p in ["p", "q", "r", "s"]:
        assert bindings(m, f"SELECT ?o WHERE {{ <{s}> <{EX}{p}> ?o }}") == expected(m, s, f"{EX}{p}")
    # A fixed object too, for each IRI object, and one that must not match.
    for (o,) in expected(m, s, f"{EX}p"):
        o = json.loads(o)
        if o["type"] == "uri":
            assert m.query(f"ASK {{ <{s}> <{EX}p> <{o['value']}> }}")
    assert not m.query(f'ASK {{ <{s}> <{EX}p> "no such value" }}')


def test_star_and_join(m):
    q = f"SELECT ?v ?w WHERE {{ <{EX}n00010> <{EX}p> ?v ; <{EX}q> ?n . ?n <{EX}p> ?w }}"
    assert m.query(q).rows() == [("v10", "v11")]


def test_after_delete_and_insert(m):
    m.update(f'DELETE DATA {{ <{EX}a> <{EX}p> "x" . <{EX}n01500> <{EX}p> "v1500" . }}')
    m.update(f'INSERT DATA {{ <{EX}a> <{EX}t> "new" . }}')
    for s in [f"{EX}a", f"{EX}n01500"]:
        assert got(m, s) == expected(m, s)
    assert m.query(f'ASK {{ <{EX}a> <{EX}t> "new" }}')


def test_subject_only_in_named_graph():
    m = Model()
    m.reads(DATA, format="ntriples")
    m.reads(f'<{EX}g> <{EX}p> "in g" .', format="ntriples", graph="urn:g")
    assert m.query(f"SELECT ?p ?o WHERE {{ <{EX}g> ?p ?o }}").height == 0
    assert m.query(f"SELECT ?p ?o WHERE {{ <{EX}g> ?p ?o }}", graph="urn:g").height == 1


@pytest.mark.parametrize("per_row", [False, True])
@pytest.mark.parametrize(
    "minus",
    [
        f"?s <{EX}knows> ?o",  # no solutions for most ?s once ?s is substituted
        f"?s <{EX}nonexistent> ?o",  # never any solutions (failed before, too)
        "?s ?p ?o FILTER(false)",
    ],
)
def test_minus_over_empty_pattern(monkeypatch, per_row, minus):
    # A MINUS whose right side has no solutions leaves the left side as it is, also when its
    # variables come out untyped.
    if per_row:
        monkeypatch.setenv("MAPLIB_LATERAL_PER_ROW", "1")
    else:
        monkeypatch.delenv("MAPLIB_LATERAL_PER_ROW", raising=False)
    m = Model()
    m.reads(
        f"""<{EX}a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{EX}T> .
            <{EX}b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{EX}T> .
            <{EX}a> <{EX}knows> <{EX}b> .""",
        format="ntriples",
    )
    q = f"SELECT ?s ?o WHERE {{ ?s a <{EX}T> LATERAL {{ ?o a <{EX}T> MINUS {{ {minus} }} }} }}"
    got = sorted(m.query(q).rows())
    knows = minus.startswith("?s <http://ex.org/knows>")
    expected = sorted(
        (f"{EX}{s}", f"{EX}{o}") for s in "ab" for o in "ab" if not (knows and (s, o) == ("a", "b"))
    )
    assert [tuple(x.strip("<>") for x in r) for r in got] == expected

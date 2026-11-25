"""RDF 1.2 features that maplib does not support yet are refused with a clear error.

The parsers and the SPARQL parser are built with RDF 1.2, so RDF 1.2 syntax is read. Triple terms
are stored (see test_rdf12_triple_terms.py), but cannot be queried yet, and literals with a base
direction cannot be stored or queried yet: they must give an error instead of being stored wrongly
(a base direction would be lost, as language strings have no place for it) or being matched like
an unbound variable. RDF 1.2 syntax without these features (the VERSION directive) is read.

When triple terms become supported, the tests of them move to the tests of that support.
"""

import pytest

from maplib import MaplibException, Model

EX = "http://example.org/"
TRIPLE_TERMS = "triple terms are not supported yet"
BASE_DIRECTION = "base direction are not supported yet"


def model_with_data():
    m = Model()
    m.reads(f"<{EX}a> <{EX}p> <{EX}b> .", format="ntriples")
    return m


def size(m):
    return m.query("SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }")["c"][0]


@pytest.mark.parametrize(
    "fmt,doc",
    [
        ("ntriples", f'<{EX}a> <{EX}label> "abc"@en--rtl .'),
        ("turtle", f'PREFIX : <{EX}>\n:a :label "abc"@ar--ltr .'),
        # Also inside a triple term.
        ("ntriples", f'<{EX}r> <http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies> <<( <{EX}a> <{EX}label> "abc"@en--rtl )>> .'),
    ],
)
def test_reading_base_directions_is_refused(fmt, doc):
    m = model_with_data()
    with pytest.raises(MaplibException, match=BASE_DIRECTION):
        m.reads(doc, format=fmt)
    assert size(m) == 1


def test_version_directive_is_read():
    m = Model()
    m.reads(f'VERSION "1.2"\nPREFIX : <{EX}>\n:a :p "x"@en .', format="turtle")
    df = m.query(f"SELECT ?o WHERE {{ <{EX}a> <{EX}p> ?o }}")
    assert df.height == 1


@pytest.mark.parametrize(
    "query",
    [
        f"SELECT * WHERE {{ VALUES ?t {{ <<( <{EX}a> <{EX}p> <{EX}b> )>> }} }}",
        f"SELECT ?t WHERE {{ ?s ?p ?o BIND(TRIPLE(?s, ?p, ?o) AS ?t) }}",
        f"SELECT * WHERE {{ ?s ?p ?o FILTER(isTRIPLE(?o)) }}",
        f"CONSTRUCT {{ ?s <{EX}q> <<( ?s ?p ?o )>> }} WHERE {{ ?s ?p ?o }}",
    ],
)
def test_queries_with_triple_terms_are_refused(query):
    m = model_with_data()
    with pytest.raises(MaplibException, match=TRIPLE_TERMS):
        m.query(query)


@pytest.mark.parametrize(
    "query",
    [
        f'SELECT * WHERE {{ ?s ?p ?o FILTER(?o = "abc"@en--rtl) }}',
        f"SELECT (LANGDIR(?o) AS ?d) WHERE {{ ?s ?p ?o }}",
    ],
)
def test_queries_with_base_directions_are_refused(query):
    m = model_with_data()
    with pytest.raises(MaplibException, match=BASE_DIRECTION):
        m.query(query)


@pytest.mark.parametrize(
    "update",
    [
        f"INSERT DATA {{ <{EX}r> <{EX}about> <<( <{EX}a> <{EX}p> <{EX}b> )>> }}",
        f"INSERT {{ ?s <{EX}about> <<( ?s ?p ?o )>> }} WHERE {{ ?s ?p ?o }}",
        f"DELETE DATA {{ <{EX}r> <{EX}about> <<( <{EX}a> <{EX}p> <{EX}b> )>> }}",
        f'INSERT DATA {{ <{EX}a> <{EX}label> "abc"@en--rtl }}',
    ],
)
def test_updates_with_rdf12_terms_are_refused(update):
    m = model_with_data()
    with pytest.raises(MaplibException, match="not supported yet"):
        m.update(update)
    assert size(m) == 1


def test_bindings_with_rdf12_terms_are_refused():
    m = model_with_data()
    q = "SELECT * WHERE { ?s ?p ?o }"
    with pytest.raises(Exception, match=TRIPLE_TERMS):
        m.query(q, bindings={"o": f"<<( <{EX}a> <{EX}p> <{EX}b> )>>"})
    with pytest.raises(Exception, match=BASE_DIRECTION):
        m.query(q, bindings={"o": '"abc"@en--rtl'})


def test_ordinary_data_and_queries_are_unaffected():
    m = model_with_data()
    m.update(f'INSERT DATA {{ <{EX}a> <{EX}label> "abc"@en }}')
    df = m.query(f"SELECT ?o WHERE {{ <{EX}a> ?p ?o }} ORDER BY ?o")
    assert df.height == 2

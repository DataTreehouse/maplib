"""Storing RDF 1.2 triple terms.

Each distinct triple term has a u32 id in the dictionary of triple terms, and tables store the id
where the triple term is a value (the object of rdf:reifies, for example). These tests check the
ids through joins on triple terms, which compare ids: two statements about the same triple term
join, statements about different triple terms do not.

Queries that bind a triple term to a projected variable, and triple term patterns, come with the
query support of triple terms; until then these tests only join, count and filter on other
variables.
"""

import polars as pl
import pytest

from maplib import Model
from maplib.maplib import RDFType, SolutionMappings

EX = "http://example.org/"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
PREFIXES = f"PREFIX : <{EX}> PREFIX rdf: <{RDF}>\n"


def count(m, where):
    return m.query(PREFIXES + f"SELECT (COUNT(*) AS ?c) WHERE {{ {where} }}")["c"][0]


def same_triple_term(m, a, b):
    """Whether the reifiers a and b reify the same triple term (their ids are equal)."""
    return count(m, f"{a} rdf:reifies ?t . {b} rdf:reifies ?t") == 1


TURTLE = f"""PREFIX : <{EX}>
:pump1 :hasPower 37.5 {{| :source :log1 |}} .
:pump2 :hasPower 12.0 ~ :r2 {{| :source :log2 |}} .
:r3 <{RDF}reifies> <<( :pump1 :hasPower 37.5 )>> .
:r4 <{RDF}reifies> <<( :x :says <<( :pump1 :hasPower 37.5 )>> )>> .
"""


def test_turtle_annotations_reifiers_and_nested_triple_terms():
    m = Model()
    m.reads(TURTLE, format="turtle")
    # 2 asserted triples, 2 annotations of 2 triples each, 2 rdf:reifies triples.
    assert count(m, "?s ?p ?o") == 8
    assert count(m, "?r rdf:reifies ?t") == 4
    # The asserted triples stay ordinary triples.
    assert count(m, ":pump1 :hasPower ?v") == 1
    # The annotation's reifier and :r3 reify the same triple term.
    assert count(m, "?a :source :log1 . ?a rdf:reifies ?t . :r3 rdf:reifies ?t") == 1
    assert same_triple_term(m, ":r2", ":r2")
    assert not same_triple_term(m, ":r2", ":r3")
    assert not same_triple_term(m, ":r3", ":r4")


@pytest.mark.parametrize(
    "fmt,doc",
    [
        ("ntriples", f"<{EX}r1> <{RDF}reifies> <<( <{EX}a> <{EX}p> <{EX}b> )>> .\n"
                     f"<{EX}r2> <{RDF}reifies> <<( <{EX}a> <{EX}p> <{EX}b> )>> .\n"),
        ("nquads", f"<{EX}r1> <{RDF}reifies> <<( <{EX}a> <{EX}p> <{EX}b> )>> <{EX}g> .\n"
                   f"<{EX}r2> <{RDF}reifies> <<( <{EX}a> <{EX}p> <{EX}b> )>> <{EX}g> .\n"),
        ("trig", f"PREFIX : <{EX}>\n:g {{ :r1 <{RDF}reifies> <<( :a :p :b )>> . "
                 f":r2 <{RDF}reifies> <<( :a :p :b )>> }}"),
    ],
)
def test_formats(fmt, doc):
    m = Model()
    m.reads(doc, format=fmt)
    if fmt == "ntriples":
        assert same_triple_term(m, ":r1", ":r2")
    else:
        assert count(m, "GRAPH :g { :r1 rdf:reifies ?t . :r2 rdf:reifies ?t }") == 1


def test_same_triple_term_across_reads_and_graphs():
    m = Model()
    m.reads(f"<{EX}r1> <{RDF}reifies> <<( <{EX}a> <{EX}p> \"x\"@en )>> .", format="ntriples")
    m.reads(f"<{EX}r2> <{RDF}reifies> <<( <{EX}a> <{EX}p> \"x\"@en )>> .", format="ntriples",
            graph=f"{EX}g")
    assert count(m, ":r1 rdf:reifies ?t . GRAPH :g { :r2 rdf:reifies ?t }") == 1


@pytest.mark.parametrize(
    "a,b,same",
    [
        ('"x"', '"x"', True),
        ('"x"', '"y"', False),
        ('"x"@en', '"x"@de', False),
        ('"x"@en', '"x"', False),
        ('"37.5"^^<http://www.w3.org/2001/XMLSchema#double>',
         '"37.5"^^<http://www.w3.org/2001/XMLSchema#decimal>', False),
        # Numbers are stored as values, so these are the same triple term in maplib.
        ('"01"^^<http://www.w3.org/2001/XMLSchema#integer>',
         '"1"^^<http://www.w3.org/2001/XMLSchema#integer>', True),
        ('"2020-01-01T00:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>',
         '"2020-01-01T00:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>', True),
        ("<<( <http://example.org/a> <http://example.org/p> <http://example.org/b> )>>",
         "<<( <http://example.org/a> <http://example.org/p> <http://example.org/b> )>>", True),
        ("<<( <http://example.org/a> <http://example.org/p> <http://example.org/b> )>>",
         "<<( <http://example.org/a> <http://example.org/p> <http://example.org/c> )>>", False),
    ],
)
def test_triple_term_equality(a, b, same):
    m = Model()
    m.reads(
        f"<{EX}r1> <{RDF}reifies> <<( <{EX}s> <{EX}p> {a} )>> .\n"
        f"<{EX}r2> <{RDF}reifies> <<( <{EX}s> <{EX}p> {b} )>> .\n",
        format="ntriples",
    )
    assert same_triple_term(m, ":r1", ":r2") == same


def test_blank_nodes_in_triple_terms_are_scoped_to_their_document():
    m = Model()
    doc = f"<{EX}r{{}}> <{RDF}reifies> <<( _:b <{EX}p> <{EX}o> )>> .\n"
    m.reads(doc.format(1) + doc.format(2), format="ntriples")
    # The same blank node in one document: the same triple term.
    assert same_triple_term(m, ":r1", ":r2")
    m.reads(doc.format(3), format="ntriples")
    # The same label in another document is another blank node.
    assert not same_triple_term(m, ":r1", ":r3")


def test_reading_the_same_triple_terms_twice_adds_nothing():
    m = Model()
    m.reads(TURTLE, format="turtle")
    m.reads(TURTLE.replace("{| :source :log1 |}", "").replace("{| :source :log2 |}", ""),
            format="turtle")
    assert count(m, "?r rdf:reifies ?t") == 4
    assert count(m, ":r3 rdf:reifies ?t . ?r rdf:reifies ?t") == 2


def test_triple_term_ids_survive_compaction():
    # Compaction renumbers the codes of the string dictionaries; the triple terms must refer to
    # the same IRIs afterwards, so the same triple term read again gets the same id.
    m = Model()
    m.reads(f"<{EX}r1> <{RDF}reifies> <<( <{EX}m> <{EX}p> <{EX}n> )>> .", format="ntriples")
    # IRIs that sort before and between the ones of the triple term shift their ranks.
    m.reads("".join(f"<{EX}{c}> <{EX}q> <{EX}{c}{c}> .\n" for c in "abcdlz"), format="ntriples")
    m.compact()
    m.reads(f"<{EX}r2> <{RDF}reifies> <<( <{EX}m> <{EX}p> <{EX}n> )>> .", format="ntriples")
    assert same_triple_term(m, ":r1", ":r2")
    assert not same_triple_term(m, ":r1", ":a")


def test_triple_term_iris_survive_garbage_collection():
    # IRIs used only in triple terms are used: garbage collection (after a million deleted
    # triples) must not remove them, or the same triple term read again would not be the same.
    m = Model()
    m.reads(
        f"<{EX}r1> <{RDF}reifies> <<( <{EX}onlyhere> <{EX}p> <{EX}alsoonlyhere> )>> .",
        format="ntriples",
    )
    n = 1_000_000
    df = pl.DataFrame({"subject": [f"{EX}s{i}" for i in range(n)],
                       "object": [f"{EX}o{i}" for i in range(n)]})
    m.map_triples(SolutionMappings(df, {"subject": RDFType.IRI, "object": RDFType.IRI}),
                  predicate=f"{EX}tmp")
    m.update(PREFIXES + "DELETE { ?s :tmp ?o } WHERE { ?s :tmp ?o }")
    m.reads(
        f"<{EX}r2> <{RDF}reifies> <<( <{EX}onlyhere> <{EX}p> <{EX}alsoonlyhere> )>> .",
        format="ntriples",
    )
    assert same_triple_term(m, ":r1", ":r2")
    assert count(m, "?s :tmp ?o") == 0

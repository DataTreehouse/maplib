"""Storing RDF 1.2 triple terms.

Each distinct triple term has a u32 id in the dictionary of triple terms, and tables store the id
where the triple term is a value (the object of rdf:reifies, for example). These tests check the
ids through joins on triple terms, which compare ids: two statements about the same triple term
join, statements about different triple terms do not.

Triple terms are returned to Python, and written, in their N-Triples form `<<( s p o )>>`.
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


QUERY_DATA = f"""PREFIX : <{EX}>
:pump1 :hasPower 37.5 {{| :source :log1 |}} .
:pump2 :hasPower 12.0 ~ :r2 {{| :source :log2 |}} .
:pump1 :label "p1"@en {{| :source :log2 |}} .
:r4 rdf:reifies <<( :x :says <<( :pump1 :hasPower 37.5 )>> )>> .
""".replace("PREFIX : ", f"PREFIX rdf: <{RDF}>\nPREFIX : ")


@pytest.fixture(scope="module")
def qm():
    m = Model()
    m.reads(QUERY_DATA, format="turtle")
    return m


def rows(m, query):
    df = m.query(PREFIXES + query)
    return sorted(tuple(str(v) for v in r) for r in df.iter_rows())


@pytest.mark.parametrize(
    "query,expected",
    [
        ("SELECT ?s ?src WHERE { ?r rdf:reifies <<( ?s :hasPower ?o )>> . ?r :source ?src }",
         [(f"<{EX}pump1>", f"<{EX}log1>"), (f"<{EX}pump2>", f"<{EX}log2>")]),
        ("SELECT ?r WHERE { ?r rdf:reifies <<( :pump2 :hasPower ?o )>> }", [(f"<{EX}r2>",)]),
        ('SELECT ?src WHERE { ?r rdf:reifies <<( :pump1 :label "p1"@en )>> ; :source ?src }',
         [(f"<{EX}log2>",)]),
        ('SELECT ?src WHERE { ?r rdf:reifies <<( :pump1 :label "p1"@de )>> ; :source ?src }', []),
        ("SELECT ?p WHERE { ?r rdf:reifies <<( :pump1 ?p ?o )>> }",
         [(f"<{EX}hasPower>",), (f"<{EX}label>",)]),
        ("SELECT ?who ?s WHERE { ?r rdf:reifies <<( ?who :says <<( ?s :hasPower ?v )>> )>> }",
         [(f"<{EX}x>", f"<{EX}pump1>")]),
        ("SELECT ?s ?src WHERE { << ?s :hasPower ?o >> :source ?src }",
         [(f"<{EX}pump1>", f"<{EX}log1>"), (f"<{EX}pump2>", f"<{EX}log2>")]),
        ("SELECT ?s ?src WHERE { ?s :hasPower ?o {| :source ?src |} }",
         [(f"<{EX}pump1>", f"<{EX}log1>"), (f"<{EX}pump2>", f"<{EX}log2>")]),
        ("SELECT ?r WHERE { ?r rdf:reifies <<( :nobody :hasPower ?o )>> }", []),
        ("SELECT ?s WHERE { ?s :hasPower ?o FILTER EXISTS { ?r rdf:reifies <<( ?s :hasPower ?o )>> ; :source :log2 } }",
         [(f"<{EX}pump2>",)]),
        ("SELECT ?t WHERE { :r2 rdf:reifies ?t }",
         # Numbers are stored as values, so the decimal is written in its canonical form.
         [(f"<<( <{EX}pump2> <{EX}hasPower> \"12\"^^<http://www.w3.org/2001/XMLSchema#decimal> )>>",)]),
    ],
)
def test_triple_term_patterns(qm, query, expected):
    assert rows(qm, query) == expected


@pytest.mark.parametrize(
    "query,expected",
    [
        # Values of the variables of a triple term pattern (from VALUES, or from patterns
        # evaluated before it) limit the triple terms matched before the pattern it is in.
        ("SELECT ?s ?src WHERE { VALUES ?s { :pump2 :nobody } ?r rdf:reifies <<( ?s ?p ?o )>> ; :source ?src }",
         [(f"<{EX}pump2>", f"<{EX}log2>")]),
        ("SELECT ?s ?p WHERE { VALUES ?s { :pump1 } ?r rdf:reifies <<( ?s ?p ?o )>> }",
         [(f"<{EX}pump1>", f"<{EX}hasPower>"), (f"<{EX}pump1>", f"<{EX}label>")]),
        ("SELECT ?r WHERE { VALUES ?s { :nobody } ?r rdf:reifies <<( ?s ?p ?o )>> }", []),
        ("SELECT ?r WHERE { VALUES ?s { \"not an IRI\" } ?r rdf:reifies <<( ?s ?p ?o )>> }", []),
        ("SELECT ?src WHERE { :pump1 :label ?l . ?r rdf:reifies <<( :pump1 :label ?l )>> ; :source ?src }",
         [(f"<{EX}log2>",)]),
        # A literal object: its values are not codes, so the join with the solutions does it.
        ("SELECT ?s WHERE { VALUES ?o { 12.0 } ?r rdf:reifies <<( ?s :hasPower ?o )>> }",
         [(f"<{EX}pump2>",)]),
        ("SELECT ?s WHERE { ?s :hasPower ?o . ?r rdf:reifies <<( ?s :hasPower ?o )>> ; :source :log1 }",
         [(f"<{EX}pump1>",)]),
        # Nested: the values of the inner pattern's subject.
        ("SELECT ?r ?who WHERE { VALUES ?s { :pump1 } ?r rdf:reifies <<( ?who :says <<( ?s ?p ?o )>> )>> }",
         [(f"<{EX}r4>", f"<{EX}x>")]),
        ("SELECT ?r WHERE { VALUES ?s { :pump2 } ?r rdf:reifies <<( ?who :says <<( ?s ?p ?o )>> )>> }", []),
        # The reifier bound as well.
        ("SELECT ?s WHERE { VALUES (?r ?s) { (:r2 :pump2) (:r2 :pump1) } ?r rdf:reifies <<( ?s ?p ?o )>> }",
         [(f"<{EX}pump2>",)]),
    ],
)
def test_triple_term_patterns_with_bound_variables(qm, query, expected):
    assert rows(qm, query) == expected


@pytest.mark.parametrize(
    "query,expected",
    [
        # The reifiers are found first, and their triple terms by their ids.
        ("SELECT ?s ?p WHERE { :r2 rdf:reifies <<( ?s ?p ?o )>> }",
         [(f"<{EX}pump2>", f"<{EX}hasPower>")]),
        # Triple terms in two partitions (a decimal and a language string object).
        ("SELECT ?s ?p WHERE { ?r :source :log2 . ?r rdf:reifies <<( ?s ?p ?o )>> }",
         [(f"<{EX}pump1>", f"<{EX}label>"), (f"<{EX}pump2>", f"<{EX}hasPower>")]),
        ("SELECT ?s WHERE { :r2 rdf:reifies <<( ?s :label ?o )>> }", []),
        ("SELECT ?s WHERE { :r2 rdf:reifies <<( ?s :hasPower 12.0 )>> }", [(f"<{EX}pump2>",)]),
        ("SELECT ?s WHERE { :r2 rdf:reifies <<( ?s :hasPower 13.0 )>> }", []),
        ("SELECT ?s WHERE { :r2 rdf:reifies <<( :pump1 ?p ?o )>> }", []),
        # A reifier without a triple term, and a resource that reifies nothing.
        ("SELECT ?s WHERE { :pump1 rdf:reifies <<( ?s ?p ?o )>> }", []),
        # Nested: the ids of the inner triple terms are the objects of the outer ones.
        ("SELECT ?who ?s ?p WHERE { :r4 rdf:reifies <<( ?who :says <<( ?s ?p ?o )>> )>> }",
         [(f"<{EX}x>", f"<{EX}pump1>", f"<{EX}hasPower>")]),
        ("SELECT ?who WHERE { :r4 rdf:reifies <<( ?who :says <<( ?s :label ?o )>> )>> }", []),
        ("SELECT ?who WHERE { :r2 rdf:reifies <<( ?who :says <<( ?s ?p ?o )>> )>> }", []),
    ],
)
def test_triple_terms_found_by_their_ids(qm, query, expected):
    assert rows(qm, query) == expected


def test_triple_terms_found_by_ids_above_the_cap():
    # More reifiers than ids are looked up directly (100,000): the triple terms are matched
    # as before, with the same answer.
    n = 100_050
    m = Model()
    m.reads(
        "".join(
            f"<{EX}r{i}> <{RDF}reifies> <<( <{EX}s{i}> <{EX}p{i % 3}> <{EX}o{i % 7}> )>> .\n"
            f"<{EX}r{i}> <{EX}source> <{EX}{'a' if i % 2 else 'b'}> .\n"
            for i in range(n)
        ),
        format="ntriples",
    )
    assert count(m, "?r :source ?src . ?r rdf:reifies <<( ?s ?p ?o )>>") == n
    assert count(m, "?r :source :a . ?r rdf:reifies <<( ?s :p1 ?o )>>") == len(
        [i for i in range(n) if i % 2 and i % 3 == 1])
    assert count(m, "?r :source :b . ?r rdf:reifies <<( ?s ?p ?o )>>") == n // 2


def test_triple_terms_in_sparql_json(qm):
    import json

    res = json.loads(qm.query(PREFIXES + "SELECT ?t WHERE { :r4 rdf:reifies ?t }", return_format="json"))
    t = res["results"]["bindings"][0]["t"]
    assert t["type"] == "triple"
    assert t["value"]["predicate"] == {"type": "uri", "value": f"{EX}says"}
    assert t["value"]["object"]["type"] == "triple"


@pytest.mark.parametrize("fmt", ["ntriples", "nquads", "turtle", "trig"])
def test_writing_and_reading_triple_terms_back(qm, fmt):
    written = qm.writes(format=fmt)
    m = Model()
    m.reads(written, format=fmt)
    assert count(m, "?s ?p ?o") == count(qm, "?s ?p ?o")
    assert rows(m, "SELECT ?s ?src WHERE { ?s :hasPower ?o {| :source ?src |} }") == rows(
        qm, "SELECT ?s ?src WHERE { ?s :hasPower ?o {| :source ?src |} }")


def test_turtle_keeps_blank_nodes_in_triple_terms():
    m = Model()
    m.reads(f"PREFIX : <{EX}>\n_:b :p :o .\n_:r <{RDF}reifies> <<( _:b :p :o )>> .", format="turtle")
    m2 = Model()
    m2.reads(m.writes(format="turtle"), format="turtle")
    # The blank node of the triple term is the subject of :p, not a new blank node.
    assert count(m2, "?r rdf:reifies <<( ?b :p :o )>> . ?b :p :o") == 1


@pytest.mark.parametrize("fmt", ["rdf/xml", "json-ld", "hdt"])
def test_formats_without_triple_terms_refuse_them(qm, fmt):
    with pytest.raises(Exception, match="(?i)triple terms"):
        if fmt == "hdt":
            qm.write_bytes(format=fmt)
        else:
            qm.writes(format=fmt)


# --- saving and loading ----------------------------------------------------------------

ROUND_TRIP_QUERIES = [
    "SELECT ?s ?src WHERE { ?r rdf:reifies <<( ?s :hasPower ?o )>> . ?r :source ?src }",
    'SELECT ?src WHERE { ?r rdf:reifies <<( :pump1 :label "p1"@en )>> ; :source ?src }',
    "SELECT ?who ?s WHERE { ?r rdf:reifies <<( ?who :says <<( ?s :hasPower ?v )>> )>> }",
    "SELECT ?r ?t WHERE { ?r rdf:reifies ?t }",
    "SELECT ?s ?src WHERE { ?s :hasPower ?o {| :source ?src |} }",
]


@pytest.mark.parametrize("single_file", [True, False])
def test_triple_terms_survive_serialization(qm, tmp_path, single_file):
    path = tmp_path / "model.maplib"
    qm.serialize(path, single_file=single_file)
    m = Model.deserialize(path)
    assert count(m, "?s ?p ?o") == count(qm, "?s ?p ?o")
    for q in ROUND_TRIP_QUERIES:
        assert rows(m, q) == rows(qm, q), q


def test_deserialized_triple_terms_keep_their_ids(tmp_path):
    m = Model()
    m.reads(TURTLE, format="turtle")
    # Garbage collection and compaction run before serializing: delete a triple to give them
    # something to remove and renumber.
    m.update(f"DELETE DATA {{ <{EX}pump2> <{EX}hasPower> 12.0 }}")
    path = tmp_path / "model.maplib"
    m.serialize(path)
    m2 = Model.deserialize(path)
    # A triple term read again is the stored one, a new one gets a new id.
    m2.reads(
        f"PREFIX : <{EX}>\n"
        f":r9 <{RDF}reifies> <<( :pump1 :hasPower 37.5 )>> .\n"
        f":r10 <{RDF}reifies> <<( :pump3 :hasPower 1.0 )>> .\n",
        format="turtle",
    )
    assert same_triple_term(m2, ":r3", ":r9")
    assert not same_triple_term(m2, ":r3", ":r10")
    assert not same_triple_term(m2, ":r2", ":r10")
    assert count(m2, "?r rdf:reifies ?t") == count(m, "?r rdf:reifies ?t") + 2
    # And saving the extended model again keeps all of them.
    path2 = tmp_path / "model2.maplib"
    m2.serialize(path2)
    m3 = Model.deserialize(path2)
    assert rows(m3, "SELECT ?r ?t WHERE { ?r rdf:reifies ?t }") == rows(
        m2, "SELECT ?r ?t WHERE { ?r rdf:reifies ?t }")


def test_serializations_without_triple_terms_have_no_entry(tmp_path):
    import json

    m = Model()
    m.reads(f"<{EX}a> <{EX}p> <{EX}b> .", format="ntriples")
    m.serialize(tmp_path / "m", single_file=False)
    meta = json.loads((tmp_path / "m" / "cats" / "metadata.json").read_text())
    assert "triple_terms" not in meta
    assert count(Model.deserialize(tmp_path / "m"), "?s ?p ?o") == 1


# --- many partitions -------------------------------------------------------------------
# Subjects and objects looked for skip the partitions whose filters can't hold them. These
# check that no match is skipped: with 300 predicates, for subjects in one, several and no
# partitions, for string and IRI objects, and after compaction and saving, which renumber codes.


def wide_model():
    lines = []
    for i in range(3000):
        p = f"<{EX}p{i % 300}>"
        o = f'"v{i % 11}"' if i % 2 else f"<{EX}o{i % 13}>"
        lines.append(f"<{EX}r{i}> <{RDF}reifies> <<( <{EX}s{i % 1000}> {p} {o} )>> .")
    lines.append(f"<{EX}rb> <{RDF}reifies> <<( _:b <{EX}p7> <{EX}o1> )>> .")
    m = Model()
    m.reads("\n".join(lines) + "\n", format="ntriples")
    return m


WIDE_QUERIES = [
    "SELECT ?r ?p ?o WHERE { ?r rdf:reifies <<( :s5 ?p ?o )>> }",
    "SELECT ?r WHERE { ?r rdf:reifies <<( :nobody ?p ?o )>> }",
    "SELECT ?r ?s WHERE { ?r rdf:reifies <<( ?s ?p \"v3\" )>> }",
    "SELECT ?r ?s WHERE { ?r rdf:reifies <<( ?s ?p :o4 )>> }",
    "SELECT ?r ?s ?p WHERE { VALUES ?s { :s1 :s2 :s999 :nobody } ?r rdf:reifies <<( ?s ?p ?o )>> }",
    "SELECT ?r WHERE { ?r rdf:reifies <<( ?s :p7 :o1 )>> }",
]


def expected_wide(query):
    """The answer of the query computed from the generated statements, in Python."""
    rows = []
    for i in range(3000):
        s, p = f"<{EX}s{i % 1000}>", f"<{EX}p{i % 300}>"
        # Strings are returned as their values.
        o = f"v{i % 11}" if i % 2 else f"<{EX}o{i % 13}>"
        rows.append((f"<{EX}r{i}>", s, p, o))
    if ":s5 ?p" in query:
        return sorted((r, p, o) for r, s, p, o in rows if s == f"<{EX}s5>")
    if ":nobody ?p" in query:
        return []
    if '"v3"' in query:
        return sorted((r, s) for r, s, p, o in rows if o == "v3")
    if ":o4" in query:
        return sorted((r, s) for r, s, p, o in rows if o == f"<{EX}o4>")
    if "VALUES" in query:
        keep = {f"<{EX}s1>", f"<{EX}s2>", f"<{EX}s999>"}
        return sorted((r, s, p) for r, s, p, o in rows if s in keep)
    # :p7 :o1 includes the statement about the blank node.
    return sorted([(r,) for r, s, p, o in rows if p == f"<{EX}p7>" and o == f"<{EX}o1>"]
                  + [(f"<{EX}rb>",)])


@pytest.mark.parametrize("query", WIDE_QUERIES)
def test_many_partitions(query):
    m = wide_model()
    assert rows(m, query) == expected_wide(query), query


def test_many_partitions_after_compaction_and_saving(tmp_path):
    m = wide_model()
    m.compact()
    for q in WIDE_QUERIES:
        assert rows(m, q) == expected_wide(q), q
    m.serialize(tmp_path / "wide.maplib")
    m2 = Model.deserialize(tmp_path / "wide.maplib")
    for q in WIDE_QUERIES:
        assert rows(m2, q) == expected_wide(q), q


def test_many_partitions_stacked_by_type():
    # Over 16 partitions with a variable predicate, partitions of the same types are stacked
    # into one table with a column of their predicates.
    m = wide_model()
    assert count(m, "?r rdf:reifies <<( ?s ?p ?o )>>") == 3001
    df = m.query(PREFIXES + "SELECT ?p (COUNT(*) AS ?c) WHERE { ?r rdf:reifies <<( ?s ?p ?o )>> } GROUP BY ?p")
    counts = {p: c for p, c in df.iter_rows()}
    assert len(counts) == 300
    assert counts[f"<{EX}p7>"] == 11  # with the statement about the blank node
    assert all(c == 10 for p, c in counts.items() if p != f"<{EX}p7>")
    # IRI and string objects are stacked separately and unioned.
    assert count(m, "?r rdf:reifies <<( ?s ?p ?o )>> FILTER(isIRI(?o))") == 1501
    assert count(m, "?r rdf:reifies <<( ?s ?p ?o )>> FILTER(isLiteral(?o))") == 1500

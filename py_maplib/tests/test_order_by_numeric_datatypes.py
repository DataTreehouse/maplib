"""ORDER BY compares numbers of different datatypes by value (SPARQL 1.1, 15.1: the `<` operator),
not by datatype first."""

from maplib import Model

PREFIX = "PREFIX : <http://example.org/>\n"


def test_numbers_of_different_datatypes_are_ordered_by_value():
    m = Model()
    m.reads(
        """@prefix : <http://example.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        :a :v 2 . :b :v 1.5 . :c :v 1 . :d :v "0.5"^^xsd:double . :e :v "x" . :f :v 3.25 .
        :g :w 1 .""",
        format="turtle",
    )
    df = m.query(PREFIX + "SELECT ?s WHERE { ?s :v|:w [] OPTIONAL { ?s :v ?v } } ORDER BY ?v")
    # Unbound first, numbers by value, then the string
    assert [s.rsplit("/", 1)[-1][:-1] for s in df["s"]] == ["g", "d", "c", "b", "a", "f", "e"]
    df = m.query(PREFIX + "SELECT ?s WHERE { ?s :v ?v } ORDER BY DESC(?v)")
    assert [s.rsplit("/", 1)[-1][:-1] for s in df["s"]] == ["e", "f", "a", "b", "c", "d"]


def test_large_integers_keep_their_order_next_to_decimals():
    # Doubles cannot tell these integers apart, the exact values still order them.
    m = Model()
    m.reads(
        """@prefix : <http://example.org/> .
        :a :v 9007199254740993 . :b :v 9007199254740992 . :c :v 0.5 .""",
        format="turtle",
    )
    df = m.query(PREFIX + "SELECT ?s WHERE { ?s :v ?v } ORDER BY ?v")
    assert [s.rsplit("/", 1)[-1][:-1] for s in df["s"]] == ["c", "b", "a"]

"""AVG of no values is 0 (SPARQL 1.1, 18.5.1.4)."""

from maplib import Model

PREFIX = "PREFIX : <http://example.org/>\n"
DATA = """@prefix : <http://example.org/> .
:a :g :x . :b :g :x ; :v 4 . :c :g :y ."""


def test_avg_of_no_values_is_zero():
    m = Model()
    m.reads(DATA, format="turtle")
    df = m.query(
        PREFIX
        + """SELECT ?g (AVG(?v) AS ?avg) (SUM(?v) AS ?sum)
            WHERE { ?s :g ?g OPTIONAL { ?s :v ?v } } GROUP BY ?g ORDER BY ?g"""
    )
    assert [float(x) for x in df["avg"]] == [4.0, 0.0]
    assert df["sum"].to_list() == [4, 0]

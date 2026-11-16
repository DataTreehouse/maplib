"""SAMPLE skips unbound values: they are errors, not values of the aggregate (SPARQL 1.1, 18.5.1)."""

from maplib import Model

PREFIX = "PREFIX : <http://example.org/>\n"
DATA = """@prefix : <http://example.org/> .
:a :g :x . :b :g :x ; :v 4 . :c :g :y ."""


def test_sample_skips_unbound_values():
    m = Model()
    m.reads(DATA, format="turtle")
    df = m.query(
        PREFIX
        + """SELECT ?g (SAMPLE(?v) AS ?any) (SAMPLE(?v ORDER BY ?s) AS ?first)
            WHERE { ?s :g ?g OPTIONAL { ?s :v ?v } } GROUP BY ?g ORDER BY ?g"""
    )
    # :a comes first in group :x but has no value
    assert df["any"].to_list() == [4, None]
    assert df["first"].to_list() == [4, None]

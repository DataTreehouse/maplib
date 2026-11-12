import json

import pytest

from maplib import Model

XSD = "http://www.w3.org/2001/XMLSchema#"

DATA = """@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<http://example.org/a> <http://example.org/p> "2010-06-21T11:28:01.05Z"^^xsd:dateTime .
<http://example.org/b> <http://example.org/p> "2011-02-01T01:02:03.123456Z"^^xsd:dateTime .
<http://example.org/c> <http://example.org/p> "0099-12-31T23:59:00Z"^^xsd:dateTime .
<http://example.org/d> <http://example.org/date> "2020-03-04"^^xsd:date .
"""


def results(query: str):
    m = Model()
    m.reads(DATA, format="turtle")
    out = json.loads(m.query(query, return_format="json"))["results"]["bindings"]
    return [{k: (v["value"], v.get("datatype")) for k, v in row.items()} for row in out]


@pytest.mark.parametrize(
    "function,expected",
    [
        ("YEAR", ["2010", "2011", "99"]),
        ("MONTH", ["6", "2", "12"]),
        ("DAY", ["21", "1", "31"]),
        ("HOURS", ["11", "1", "23"]),
        ("MINUTES", ["28", "2", "59"]),
    ],
)
def test_date_parts_are_integers(function, expected):
    rows = results(
        f"""SELECT ?s ({function}(?o) AS ?v) WHERE {{ ?s <http://example.org/p> ?o }}
        ORDER BY ?s"""
    )
    assert [r["v"] for r in rows] == [(v, XSD + "integer") for v in expected]


def test_seconds_is_decimal_with_fraction():
    rows = results(
        """SELECT ?s (SECONDS(?o) AS ?v) WHERE { ?s <http://example.org/p> ?o } ORDER BY ?s"""
    )
    assert [(float(v), t) for v, t in (r["v"] for r in rows)] == [
        (1.05, XSD + "decimal"),
        (3.123456, XSD + "decimal"),
        (0.0, XSD + "decimal"),
    ]


def test_date_parts_of_xsd_date():
    (row,) = results(
        """SELECT (YEAR(?o) AS ?y) (MONTH(?o) AS ?m) (DAY(?o) AS ?d)
        WHERE { ?s <http://example.org/date> ?o }"""
    )
    assert row == {
        "y": ("2020", XSD + "integer"),
        "m": ("3", XSD + "integer"),
        "d": ("4", XSD + "integer"),
    }


def test_str_of_datetime_and_date_is_xsd_lexical_form():
    rows = results(
        """SELECT ?s (STR(?o) AS ?v) WHERE { ?s ?p ?o } ORDER BY ?s"""
    )
    assert [r["v"][0] for r in rows] == [
        "2010-06-21T11:28:01.050+00:00",
        "2011-02-01T01:02:03.123456+00:00",
        "0099-12-31T23:59:00+00:00",
        "2020-03-04",
    ]

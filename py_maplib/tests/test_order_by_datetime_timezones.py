"""ORDER BY over xsd:dateTime values with different time zones.

Values with different offsets are different types in a multi type column. They used to be
sorted one type after another in an arbitrary order that changed between runs. They must be
ordered by their UTC instant (XPath op:dateTime-less-than), and the order must be the same every
time. A dateTime without a time zone is ordered as if its implicit time zone was Z, and ties at
the same instant are broken by time zone: no time zone first, then by increasing offset.
"""

import json

import pytest

from maplib import Model

DATA = """
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:a ex:v "2024-02-29T12:30:00Z"^^xsd:dateTime ,
          "2024-02-29T12:30:00+02:00"^^xsd:dateTime ,
          "2024-02-29T12:30:00"^^xsd:dateTime ,
          "2024-02-29T08:00:00-05:00"^^xsd:dateTime ,
          "2024-02-29T13:00:00+02:00"^^xsd:dateTime ,
          "2024-02-29T11:00:00Z"^^xsd:dateTime .
"""

# UTC instants: 10:30, 11:00, 11:00, 12:30 (no time zone, as Z), 12:30, 13:00.
ASCENDING = [
    "2024-02-29T12:30:00+02:00",
    "2024-02-29T11:00:00Z",
    "2024-02-29T13:00:00+02:00",
    "2024-02-29T12:30:00",
    "2024-02-29T12:30:00Z",
    "2024-02-29T08:00:00-05:00",
]


@pytest.fixture(scope="module")
def model() -> Model:
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def ordered(m: Model, order: str) -> list:
    q = f"SELECT ?o WHERE {{ ?s <http://example.org/v> ?o }} ORDER BY {order}"
    res = json.loads(m.query(q, return_format="json"))
    return [row["o"]["value"] for row in res["results"]["bindings"]]


def test_order_by_datetime_time_zones_ascending(model):
    for _ in range(30):
        assert ordered(model, "?o") == ASCENDING


def test_order_by_datetime_time_zones_descending(model):
    for _ in range(30):
        assert ordered(model, "DESC(?o)") == list(reversed(ASCENDING))


def test_order_by_datetime_time_zones_three_way():
    m = Model()
    m.reads(
        """
        @prefix ex: <http://example.org/> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:a ex:v "2024-02-29T12:30:00Z"^^xsd:dateTime , "2024-02-29T12:30:00+02:00"^^xsd:dateTime ,
            "2024-02-29T12:30:00"^^xsd:dateTime .
        """,
        format="turtle",
    )
    for _ in range(30):
        assert ordered(m, "?o") == [
            "2024-02-29T12:30:00+02:00",
            "2024-02-29T12:30:00",
            "2024-02-29T12:30:00Z",
        ]

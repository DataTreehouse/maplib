"""Inputs that used to panic, hang or crash the interpreter."""

import subprocess
import sys
import threading

import pytest
from maplib import IRI, Literal, MaplibException, Model

XSD = "http://www.w3.org/2001/XMLSchema#"


def test_syntax_error_near_multibyte_chars_is_an_error_and_model_keeps_working():
    m = Model()
    # The error message context is cut 10 bytes around the error, inside the "å".
    with pytest.raises(MaplibException):
        m.query('SELECT * WHERE { ?s ?p "æøåæø" ) }')
    m.query("SELECT * WHERE { ?s ?p ?o }")


def test_add_graph_from_itself_raises_instead_of_deadlocking():
    m = Model()
    m.insert("CONSTRUCT { <http://a/s> <http://a/p> <http://a/o> } WHERE {}")
    done = []

    def run():
        try:
            m.add_graph(m, target_graph="http://g")
        except Exception as e:
            done.append(e)

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(60)
    assert done, "add_graph(self) did not return"
    assert "Panic" not in type(done[0]).__name__
    assert m.query("SELECT * WHERE { ?s ?p ?o }").height == 1


def test_construct_skips_non_iri_predicates():
    m = Model()
    m.insert("CONSTRUCT { <http://a/s> <http://a/p> <http://a/o> } WHERE {}")
    m.insert('CONSTRUCT { ?s ?q ?o } WHERE { ?s ?p ?o BIND("lit" AS ?q) }')
    m.insert("CONSTRUCT { ?s ?q ?o } WHERE { ?s ?p ?o BIND(42 AS ?q) }")
    assert m.query("SELECT * WHERE { ?s ?p ?o }").height == 1
    m.insert(
        'CONSTRUCT { ?s ?q ?o } WHERE { ?s ?p ?o VALUES ?q { <http://a/p2> "x" } }'
    )
    assert m.query("SELECT * WHERE { ?s <http://a/p2> ?o }").height == 1
    assert m.query("SELECT * WHERE { ?s ?p ?o }").height == 2


def test_invalid_duration_to_native_raises():
    with pytest.raises(Exception) as e:
        Literal("P1X", data_type=IRI(XSD + "duration")).to_native()
    assert "Panic" not in type(e.value).__name__


@pytest.mark.parametrize(
    "lexical,expected",
    [
        ("PT1.05S", (1, 50_000_000)),
        ("PT1.5S", (1, 500_000_000)),
        ("-PT1.5S", (-1, -500_000_000)),
        ("PT3S", (3, 0)),
    ],
)
def test_duration_seconds_fraction_is_nanoseconds(lexical, expected):
    d = Literal(lexical, data_type=IRI(XSD + "duration")).to_native()
    assert d.seconds == expected


def test_date_to_native():
    d = Literal("2020-01-02", data_type=IRI(XSD + "date")).to_native()
    assert str(d) == "2020-01-02"


LONG_QUERIES = {
    "union": "' UNION '.join(f'{{ ?s <http://p/{i}> ?o }}' for i in range(3000))",
    # A FILTER with 10000 || terms still overflows: the query cache parses and walks the query
    # (Model::cache_plan) before query_mutex moves to the big stack.
    "nested": "'(' * 5000 + '?o' + ')' * 5000",
}


@pytest.mark.parametrize("kind", list(LONG_QUERIES))
def test_long_and_deep_queries_do_not_overflow_the_stack(kind):
    body = LONG_QUERIES[kind]
    if kind == "union":
        q = f"'SELECT * WHERE {{ ' + {body} + ' }}'"
    else:
        q = f"'SELECT * WHERE {{ ?s ?p ?o FILTER(' + {body} + ') }}'"
    # In a subprocess, since a stack overflow kills the interpreter.
    code = f"from maplib import Model\nModel().query({q})\n"
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr[-500:]

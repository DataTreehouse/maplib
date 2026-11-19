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


def test_add_graph_from_itself_does_not_deadlock():
    m = Model()
    m.insert("CONSTRUCT { <http://a/s> <http://a/p> <http://a/o> } WHERE {}")
    done = []

    def run():
        try:
            m.add_graph(m, target_graph="http://g")
            done.append(None)
        except BaseException as e:
            done.append(e)

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(60)
    assert done, "add_graph(self) did not return"
    assert done[0] is None, done[0]
    assert m.query("SELECT * WHERE { ?s ?p ?o }").height == 1
    assert m.query("SELECT * WHERE { ?s ?p ?o }", graph="http://g").height == 1


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
    # 2000 terms overflowed Polars' 2 MB thread stacks before RUST_MIN_STACK was raised. They need
    # about 0.7 GB at peak. Memory grows quadratically with the number of terms (about 12 GB at 10000).
    "or": "' || '.join(f'?o = <http://e/{i}>' for i in range(2000))",
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


# Queries with few enough brackets and operators are evaluated on the stack of the calling
# thread (see on_query_stack in model.rs), when it has 2 MB plus 64 kB per such token left.
# These nest as deeply as that allows on the 8 MB main thread (about 90 tokens) and on a thread
# with a 4 MB stack (about 30 tokens).
def _nested_queries(tokens):
    n = tokens - 4
    return {
        "parens": "SELECT * WHERE { ?s ?p ?o FILTER(" + "(" * n + "?o" + ")" * n + ") }",
        # Nested negations take time exponential in their depth, so these are not as deep.
        "not": "SELECT * WHERE { ?s ?p ?o FILTER(" + "!(" * 12 + "BOUND(?o)" + ")" * 12 + ") }",
        "or": "SELECT * WHERE { ?s ?p ?o FILTER("
        + " || ".join(f"?o = {i}" for i in range(n // 3))
        + ") }",
        "plus": "SELECT * WHERE { ?s ?p ?o BIND(" + " + ".join(["1"] * n) + " AS ?x) }",
        "groups": "SELECT * WHERE " + "{ " * n + "?s ?p ?o" + " }" * n,
        "optionals": "SELECT * WHERE { ?s ?p ?o "
        + "OPTIONAL { ?s ?p ?o " * n
        + "}" * n
        + " }",
        "subselects": "SELECT * WHERE " + "{ SELECT * WHERE " * (n // 2) + "{ ?s ?p ?o }" + " }" * (n // 2),
        "unions": "SELECT * WHERE { " + " UNION ".join("{ ?s ?p ?o }" for _ in range(n // 2)) + " }",
        "patterns": "SELECT * WHERE { "
        + " . ".join(f"?s{i} <http://p/{i}> ?s{i + 1}" for i in range(n))
        + " }",
    }


@pytest.mark.parametrize("stack", ["main", "4mb"])
def test_queries_evaluated_on_the_calling_threads_stack_do_not_overflow_it(stack):
    tokens = 90 if stack == "main" else 30
    queries = _nested_queries(tokens)
    code = f"""
import threading
from maplib import Model
m = Model()
m.update('INSERT DATA {{ <http://s> <http://p> 1 . <http://s> <http://p/0> <http://s> }}')
def run():
    for q in {list(queries.values())!r}:
        m.query(q)
    print("ok")
if {stack == "main"!r}:
    run()
else:
    threading.stack_size(4 * 1024 * 1024)
    t = threading.Thread(target=run)
    t.start()
    t.join()
"""
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert r.returncode == 0 and r.stdout.strip() == "ok", r.stderr[-1000:]

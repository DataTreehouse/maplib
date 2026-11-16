"""
LATERAL (SEP-0006, https://github.com/w3c/sparql-dev/blob/main/SEP/SEP-0006/sep-0006.md).

The compliance tests in ``testdata/sep0006_lateral`` are unmodified copies of the LATERAL tests
of Apache Jena and Oxigraph, see ``testdata/sep0006_lateral/README.md``. The manifests are read
with maplib, and every evaluation test runs with both evaluation strategies of LATERAL: the
set-based one (the default) and the right side evaluated once per left solution
(``MAPLIB_LATERAL_PER_ROW``).
"""

import json
import random
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

import pytest

from maplib import Model

SUITE_DIR = Path(__file__).parent / "testdata" / "sep0006_lateral"
XSD = "http://www.w3.org/2001/XMLSchema#"
MF = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#"
QT = "http://www.w3.org/2001/sw/DataAccess/tests/test-query#"
MFX = "http://jena.hpl.hp.com/2005/05/test-manifest-extra#"

STRATEGIES = ["set", "per_row"]


@pytest.fixture(params=STRATEGIES)
def strategy(request, monkeypatch):
    if request.param == "per_row":
        monkeypatch.setenv("MAPLIB_LATERAL_PER_ROW", "1")
    else:
        monkeypatch.delenv("MAPLIB_LATERAL_PER_ROW", raising=False)
    return request.param


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


def literal(lex, datatype=None, lang=None):
    if lang:
        return ("literal", lex, None, lang.lower())
    if datatype == XSD + "string":
        datatype = None
    return ("literal", lex, datatype, None)


def json_term(b):
    if b["type"] == "uri":
        return ("uri", b["value"])
    if b["type"] == "bnode":
        return ("bnode", b["value"])
    return literal(b["value"], b.get("datatype"), b.get("xml:lang"))


def parse_json(s):
    j = json.loads(s)
    return [
        {v: json_term(b) for v, b in row.items()} for row in j["results"]["bindings"]
    ]


SRX = "{http://www.w3.org/2005/sparql-results#}"


def parse_srx(path):
    rows = []
    for result in ET.parse(path).getroot().iter(SRX + "result"):
        row = {}
        for binding in result.findall(SRX + "binding"):
            (el,) = list(binding)
            text = el.text or ""
            if el.tag == SRX + "uri":
                row[binding.get("name")] = ("uri", text)
            elif el.tag == SRX + "bnode":
                row[binding.get("name")] = ("bnode", text)
            else:
                row[binding.get("name")] = literal(
                    text,
                    el.get("datatype"),
                    el.get("{http://www.w3.org/XML/1998/namespace}lang"),
                )
        rows.append(row)
    return rows


def multiset(rows):
    return Counter(tuple(sorted(r.items())) for r in rows)


def select(m, query):
    return parse_json(m.query(query, return_format="json"))


# ---------------------------------------------------------------------------
# Manifests
# ---------------------------------------------------------------------------


def manifest_tests(directory):
    """(name, type, action properties, result) of the tests in a manifest."""
    path = directory / "manifest.ttl"
    base = path.resolve().as_uri()
    m = Model()
    m.reads(path.read_text(encoding="utf-8"), format="turtle", base_iri=base)
    rows = select(
        m,
        f"""
        PREFIX mf: <{MF}>
        SELECT ?t ?type ?name ?action ?query ?data ?graphData ?result WHERE {{
            ?t a ?type ; mf:name ?name ; mf:action ?action .
            OPTIONAL {{ ?t mf:result ?result }}
            OPTIONAL {{ ?action <{QT}query> ?query }}
            OPTIONAL {{ ?action <{QT}data> ?data }}
            OPTIONAL {{ ?action <{QT}graphData> ?graphData }}
        }}
        """,
    )
    tests = []
    for r in rows:
        if r["type"][1] == MF + "Manifest":
            continue
        tests.append(
            {
                "id": f"{directory.parent.name}/{directory.name}/{r['name'][1]}",
                "type": r["type"][1],
                "action": r["action"][1],
                "query": r.get("query", (None, None))[1],
                "data": r.get("data", (None, None))[1],
                "graphData": r.get("graphData", (None, None))[1],
                "result": r.get("result", (None, None))[1],
            }
        )
    return sorted(tests, key=lambda t: t["id"])


def uri_to_path(uri):
    assert uri.startswith("file://"), uri
    return Path(uri[len("file://") :])


EVALUATION_TESTS = [
    t
    for d in [SUITE_DIR / "jena" / "Lateral", SUITE_DIR / "oxigraph" / "lateral"]
    for t in manifest_tests(d)
    if t["type"] == MF + "QueryEvaluationTest"
]
SYNTAX_TESTS = manifest_tests(SUITE_DIR / "jena" / "Syntax-Lateral")


def test_manifests_found():
    assert len(EVALUATION_TESTS) == 15
    assert len(SYNTAX_TESTS) == 13


@pytest.mark.parametrize("t", EVALUATION_TESTS, ids=lambda t: t["id"])
def test_evaluation(t, strategy):
    m = Model()
    if t["data"]:
        path = uri_to_path(t["data"])
        m.reads(path.read_text(encoding="utf-8"), format="turtle", base_iri=t["data"])
    if t["graphData"]:
        path = uri_to_path(t["graphData"])
        m.reads(
            path.read_text(encoding="utf-8"),
            format="turtle",
            base_iri=t["graphData"],
            graph=t["graphData"],
        )
    query_path = uri_to_path(t["action"] if t["query"] is None else t["query"])
    # Relative IRIs in the query (GRAPH <singleton.ttl>) are resolved against the query file.
    query = f"BASE <{query_path.resolve().as_uri()}>\n" + query_path.read_text(
        encoding="utf-8"
    )
    result_path = uri_to_path(t["result"])
    if result_path.suffix == ".srj":
        expected = parse_json(result_path.read_text(encoding="utf-8"))
    else:
        expected = parse_srx(result_path)
    actual = select(m, query)
    assert multiset(actual) == multiset(expected)


@pytest.mark.parametrize("t", SYNTAX_TESTS, ids=lambda t: t["id"])
def test_syntax(t):
    query = uri_to_path(t["action"]).read_text(encoding="utf-8")
    m = Model()
    if t["type"] == MFX + "PositiveSyntaxTestARQ":
        m.query(query)
    else:
        assert t["type"] == MFX + "NegativeSyntaxTestARQ"
        with pytest.raises(Exception):
            m.query(query)


# ---------------------------------------------------------------------------
# maplib's own tests
# ---------------------------------------------------------------------------

EX = "http://example.org/"
PREFIX = f"PREFIX ex: <{EX}>\n"

DATA = """
@prefix ex: <http://example.org/> .
ex:a a ex:T ; ex:v 1, 5, 3 ; ex:label "a" ; ex:min 2 .
ex:b a ex:T ; ex:v 10, 7 ; ex:label "bb" ; ex:min 8 .
ex:c a ex:T ; ex:label "ccc" ; ex:min 0 .
ex:d a ex:U ; ex:v 4 .
ex:a ex:knows ex:b .
ex:b ex:knows ex:c , ex:a .
"""


@pytest.fixture(scope="module")
def model():
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def iri(local):
    return ("uri", EX + local)


def integer(n):
    return literal(str(n), XSD + "integer")


def check(m, query, expected):
    assert multiset(select(m, PREFIX + query)) == multiset(expected)


def test_top_n_per_group(model, strategy):
    check(
        model,
        """SELECT ?s ?v WHERE {
            ?s a ex:T
            LATERAL { SELECT ?s ?v WHERE { ?s ex:v ?v } ORDER BY DESC(?v) LIMIT 2 }
        }""",
        [
            {"s": iri("a"), "v": integer(5)},
            {"s": iri("a"), "v": integer(3)},
            {"s": iri("b"), "v": integer(10)},
            {"s": iri("b"), "v": integer(7)},
        ],
    )


def test_offset_per_group(model, strategy):
    check(
        model,
        """SELECT ?s ?v WHERE {
            ?s a ex:T
            LATERAL { SELECT ?s ?v WHERE { ?s ex:v ?v } ORDER BY ?v OFFSET 1 LIMIT 1 }
        }""",
        [
            {"s": iri("a"), "v": integer(3)},
            {"s": iri("b"), "v": integer(10)},
        ],
    )


def test_offset_without_limit_per_group(model, strategy):
    check(
        model,
        """SELECT ?s ?v WHERE {
            ?s a ex:T
            LATERAL { SELECT ?s ?v WHERE { ?s ex:v ?v } ORDER BY ?v OFFSET 1 }
        }""",
        [
            {"s": iri("a"), "v": integer(3)},
            {"s": iri("a"), "v": integer(5)},
            {"s": iri("b"), "v": integer(10)},
        ],
    )


def test_aggregate_per_group(model, strategy):
    check(
        model,
        """SELECT ?s ?n ?max WHERE {
            ?s a ex:T
            LATERAL {
                SELECT ?s (COUNT(?v) AS ?n) (MAX(?v) AS ?max) WHERE { ?s ex:v ?v } GROUP BY ?s
            }
        }""",
        [
            {"s": iri("a"), "n": integer(3), "max": integer(5)},
            {"s": iri("b"), "n": integer(2), "max": integer(10)},
        ],
    )


def test_aggregate_with_hidden_variable_is_not_correlated(model, strategy):
    # ?s is not projected by the sub-select, so it is another variable inside it.
    check(
        model,
        """SELECT ?s ?n WHERE {
            ?s a ex:T
            LATERAL { SELECT (COUNT(*) AS ?n) WHERE { ?s ex:v ?v } }
        }""",
        [{"s": iri(x), "n": integer(6)} for x in "abc"],
    )


def test_aggregate_grouped_by_correlated_variable_in_filter(model, strategy):
    # The correlated variable is only used in a FILTER of the sub-select. To be substituted it must
    # be projected, so with an aggregate it must be grouped by.
    check(
        model,
        """SELECT ?s ?n WHERE {
            ?s ex:min ?min
            LATERAL {
                SELECT ?min (COUNT(?v) AS ?n) WHERE { ?x ex:v ?v FILTER(?v > ?min) } GROUP BY ?min
            }
        }""",
        [
            {"s": iri("a"), "n": integer(5)},
            {"s": iri("b"), "n": integer(1)},
            {"s": iri("c"), "n": integer(6)},
        ],
    )


def test_count_grouped_by_correlated_variable_empty_group(model, strategy):
    # GROUP BY over no solutions gives no group, so b, with no value above its ?min + 5, is dropped.
    check(
        model,
        """SELECT ?s ?n ?sum WHERE {
            ?s ex:min ?min
            LATERAL {
                SELECT ?min (COUNT(?v) AS ?n) (SUM(?v) AS ?sum)
                WHERE { ?x ex:v ?v FILTER(?v > ?min + 5) } GROUP BY ?min
            }
        }""",
        [
            {"s": iri("a"), "n": integer(1), "sum": integer(10)},
            {"s": iri("c"), "n": integer(2), "sum": integer(17)},
        ],
    )


def test_ungrouped_projected_variable_is_rejected(model):
    with pytest.raises(Exception):
        model.query(
            """PREFIX ex: <http://example.org/>
            SELECT ?s ?n WHERE {
                ?s ex:min ?min
                LATERAL { SELECT ?min (COUNT(?v) AS ?n) WHERE { ?x ex:v ?v FILTER(?v > ?min) } }
            }"""
        )


def test_bind_uses_left_variable(model, strategy):
    check(
        model,
        """SELECT ?s ?d WHERE {
            ?s ex:min ?min
            LATERAL { BIND(?min * 2 AS ?d) }
        }""",
        [
            {"s": iri("a"), "d": integer(4)},
            {"s": iri("b"), "d": integer(16)},
            {"s": iri("c"), "d": integer(0)},
        ],
    )


def test_filter_in_subselect_uses_left_variable(model, strategy):
    # ?min is projected by the sub-select, so it is substituted.
    check(
        model,
        """SELECT ?s ?v WHERE {
            ?s ex:min ?min
            LATERAL { SELECT ?s ?min ?v WHERE { ?s ex:v ?v FILTER(?v > ?min) } }
        }""",
        [
            {"s": iri("a"), "v": integer(5)},
            {"s": iri("a"), "v": integer(3)},
            {"s": iri("b"), "v": integer(10)},
        ],
    )


def test_distinct_per_group(model, strategy):
    check(
        model,
        """SELECT ?s ?t WHERE {
            ?s ex:knows ?o
            LATERAL { SELECT DISTINCT ?s ?t WHERE { ?s ex:knows ?x . ?x a ?t } }
        }""",
        [
            {"s": iri("a"), "t": iri("T")},
            {"s": iri("b"), "t": iri("T")},
            {"s": iri("b"), "t": iri("T")},
        ],
    )


def test_distinct_limit_per_group(model, strategy):
    check(
        model,
        """SELECT ?s ?t WHERE {
            ?s a ex:T
            LATERAL { SELECT DISTINCT ?s ?t WHERE { ?s ex:knows ?x . ?x a ?t } LIMIT 1 }
        }""",
        [
            {"s": iri("a"), "t": iri("T")},
            {"s": iri("b"), "t": iri("T")},
        ],
    )


def test_minus_uses_left_variable(model, strategy):
    check(
        model,
        """SELECT ?s ?o WHERE {
            ?s a ex:T
            LATERAL { ?o a ex:T MINUS { ?s ex:knows ?o } }
        }""",
        [
            {"s": iri("a"), "o": iri("a")},
            {"s": iri("a"), "o": iri("c")},
            {"s": iri("b"), "o": iri("b")},
            {"s": iri("c"), "o": iri("a")},
            {"s": iri("c"), "o": iri("b")},
            {"s": iri("c"), "o": iri("c")},
        ],
    )


def test_nested_lateral(model, strategy):
    # For each ?s the friend with the highest value, and that friend's lowest value.
    check(
        model,
        """SELECT ?s ?f ?low WHERE {
            ?s a ex:T
            LATERAL {
                SELECT ?s ?f WHERE { ?s ex:knows ?f . ?f ex:v ?fv } ORDER BY DESC(?fv) LIMIT 1
            }
            LATERAL {
                SELECT ?f ?low WHERE { ?f ex:v ?low } ORDER BY ?low LIMIT 1
            }
        }""",
        [
            {"s": iri("a"), "f": iri("b"), "low": integer(7)},
            {"s": iri("b"), "f": iri("a"), "low": integer(1)},
        ],
    )


def test_lateral_inside_lateral(model, strategy):
    check(
        model,
        """SELECT ?s ?f ?v WHERE {
            ?s a ex:T
            LATERAL {
                ?s ex:knows ?f
                LATERAL { SELECT ?f ?v WHERE { ?f ex:v ?v } ORDER BY DESC(?v) LIMIT 1 }
            }
        }""",
        [
            {"s": iri("a"), "f": iri("b"), "v": integer(10)},
            {"s": iri("b"), "f": iri("a"), "v": integer(5)},
        ],
    )


def test_inner_lateral_uses_outer_variable(model, strategy):
    # The innermost sub-select uses ?min from the outermost left side.
    check(
        model,
        """SELECT ?s ?f ?v WHERE {
            ?s ex:min ?min
            LATERAL {
                ?s ex:knows ?f
                LATERAL {
                    SELECT ?f ?min ?v WHERE { ?f ex:v ?v FILTER(?v > ?min) } ORDER BY ?v LIMIT 1
                }
            }
        }""",
        [{"s": iri("a"), "f": iri("b"), "v": integer(7)}],
    )


def test_hidden_variable_in_filter_is_not_substituted(model, strategy):
    # ?min is not projected by the sub-select, so inside it ?min is unbound.
    check(
        model,
        """SELECT ?s ?v WHERE {
            ?s ex:min ?min
            LATERAL { SELECT ?s ?v WHERE { ?s ex:v ?v FILTER(?v > ?min) } }
        }""",
        [],
    )


def test_unbound_left_variable_is_not_substituted(model, strategy):
    # ?f is unbound for ex:c, so the right side binds it freely there.
    check(
        model,
        """SELECT ?s ?f ?l WHERE {
            ?s a ex:T
            OPTIONAL { ?s ex:knows ?f . ?f ex:label "ccc" }
            LATERAL { SELECT ?f ?l WHERE { ?f ex:label ?l } ORDER BY ?l LIMIT 1 }
        }""",
        [
            {"s": iri("a"), "f": iri("a"), "l": literal("a")},
            {"s": iri("b"), "f": iri("c"), "l": literal("ccc")},
            {"s": iri("c"), "f": iri("a"), "l": literal("a")},
        ],
    )


def test_uncorrelated_right_side(model, strategy):
    check(
        model,
        """SELECT ?s ?x WHERE {
            ?s a ex:U
            LATERAL { SELECT ?x WHERE { ?x ex:label ?l } ORDER BY ?l LIMIT 2 }
        }""",
        [
            {"s": iri("d"), "x": iri("a")},
            {"s": iri("d"), "x": iri("b")},
        ],
    )


def test_empty_left_side(model, strategy):
    check(
        model,
        """SELECT ?s ?v WHERE {
            ?s a ex:Nothing
            LATERAL { SELECT ?s ?v WHERE { ?s ex:v ?v } LIMIT 1 }
        }""",
        [],
    )


def test_no_right_solutions(model, strategy):
    check(
        model,
        """SELECT ?s ?v WHERE {
            ?s a ex:U
            LATERAL { SELECT ?s ?v WHERE { ?s ex:nothing ?v } LIMIT 1 }
        }""",
        [],
    )


def test_optional_lateral_keeps_left(model, strategy):
    check(
        model,
        """SELECT ?s ?v WHERE {
            ?s a ex:T
            OPTIONAL { ?s ex:label ?l LATERAL { SELECT ?s ?v WHERE { ?s ex:v ?v } ORDER BY ?v LIMIT 1 } }
        }""",
        [
            {"s": iri("a"), "v": integer(1)},
            {"s": iri("b"), "v": integer(7)},
            {"s": iri("c")},
        ],
    )


def test_lateral_in_union(model, strategy):
    check(
        model,
        """SELECT ?s ?v WHERE {
            ?s a ex:T
            LATERAL {
                { SELECT ?s ?v WHERE { ?s ex:v ?v } ORDER BY ?v LIMIT 1 }
                UNION
                { SELECT ?s ?v WHERE { ?s ex:v ?v } ORDER BY DESC(?v) LIMIT 1 }
            }
        }""",
        [
            {"s": iri("a"), "v": integer(1)},
            {"s": iri("a"), "v": integer(5)},
            {"s": iri("b"), "v": integer(7)},
            {"s": iri("b"), "v": integer(10)},
        ],
    )


def test_lateral_in_construct(model, strategy):
    m = model
    out = m.query(
        PREFIX
        + """CONSTRUCT { ?s ex:top ?v } WHERE {
            ?s a ex:T
            LATERAL { SELECT ?s ?v WHERE { ?s ex:v ?v } ORDER BY DESC(?v) LIMIT 1 }
        }"""
    )
    df = out[0] if isinstance(out, list) else out
    assert df.height == 2


def test_right_side_cannot_rebind_left_variable():
    m = Model()
    with pytest.raises(Exception):
        m.query("SELECT * { ?s ?p ?o LATERAL { BIND(1 AS ?o) } }")


# ---------------------------------------------------------------------------
# Strategies agree on random data
# ---------------------------------------------------------------------------

RANDOM_QUERIES = [
    """SELECT ?s ?o ?v WHERE {
        ?s ex:type ex:G
        LATERAL { SELECT ?s ?o ?v WHERE { ?s ex:edge ?o . ?o ex:val ?v } ORDER BY DESC(?v) ?o LIMIT 3 }
    }""",
    """SELECT ?s ?n ?sum WHERE {
        ?s ex:type ex:G
        LATERAL { SELECT ?s (COUNT(?o) AS ?n) (SUM(?v) AS ?sum) WHERE { ?s ex:edge ?o . ?o ex:val ?v } GROUP BY ?s }
    }""",
    """SELECT ?s ?w ?o WHERE {
        ?s ex:val ?w
        LATERAL { SELECT ?w ?o WHERE { ?o ex:val ?v FILTER(?v > ?w) } ORDER BY ?v ?o LIMIT 2 }
    }""",
    """SELECT ?s ?w ?n WHERE {
        ?s ex:val ?w
        LATERAL { SELECT ?w (COUNT(*) AS ?n) WHERE { ?o ex:val ?v FILTER(?v < ?w) } GROUP BY ?w }
    }""",
    """SELECT ?s ?o ?o2 WHERE {
        ?s ex:type ex:G
        LATERAL {
            SELECT ?s ?o WHERE { ?s ex:edge ?o . ?o ex:val ?v } ORDER BY ?v ?o LIMIT 1
        }
        LATERAL {
            SELECT ?o ?o2 WHERE { ?o ex:edge ?o2 } ORDER BY ?o2 LIMIT 2
        }
    }""",
]


@pytest.fixture(scope="module")
def random_model():
    rnd = random.Random(42)
    lines = ["@prefix ex: <http://example.org/> ."]
    n = 200
    for i in range(n):
        if rnd.random() < 0.5:
            lines.append(f"ex:n{i} ex:type ex:G .")
        lines.append(f"ex:n{i} ex:val {rnd.randint(0, 50)} .")
        for _ in range(rnd.randint(0, 6)):
            lines.append(f"ex:n{i} ex:edge ex:n{rnd.randrange(n)} .")
    m = Model()
    m.reads("\n".join(lines), format="turtle")
    return m


@pytest.mark.parametrize("i", range(len(RANDOM_QUERIES)))
def test_strategies_agree(random_model, monkeypatch, i):
    q = PREFIX + RANDOM_QUERIES[i]
    monkeypatch.delenv("MAPLIB_LATERAL_PER_ROW", raising=False)
    set_based = select(random_model, q)
    monkeypatch.setenv("MAPLIB_LATERAL_PER_ROW", "1")
    per_row = select(random_model, q)
    assert len(set_based) > 0
    assert multiset(set_based) == multiset(per_row)

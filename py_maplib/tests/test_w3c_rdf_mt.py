"""
W3C RDF 1.1 Semantics entailment tests for Model.reason(profile="rdfs").

The test data in ``testdata/w3c_rdf_mt`` is a copy of ``rdf/rdf11/rdf-mt`` of
https://github.com/w3c/rdf-tests (without the TESTS archives and reports), see
``testdata/w3c_rdf_mt/LICENSE``.

For each ``mf:PositiveEntailmentTest`` and ``mf:NegativeEntailmentTest`` the action graph is read,
reasoned over with the "rdfs" profile (no reasoning for the "simple" regime), and the result graph
must (or must not) entail it. A result of ``false`` means that the action is inconsistent
(positive tests) or not inconsistent (negative tests); maplib does not detect datatype clashes, so
only the negative ones pass. The RDF regime is checked with the RDFS rules, which entail more.

Tests that maplib currently does not pass are listed in ``w3c_rdf_mt_known_failures.txt`` as
strict xfails with the reason. Run ``pytest test_w3c_rdf_mt.py --runxfail`` to see the failures.
"""

from collections import defaultdict
from pathlib import Path

import pytest

from maplib import Model

from .test_w3c_sparql11 import MF, RDF, dump_graph, run_select
from .w3c_entailment import (
    entails,
    graph_triples,
    load_known_failures,
    missing_triples,
    reason_or_skip,
    xfail_marks,
)

TESTS_DIR = Path(__file__).parent
SUITE_DIR = TESTS_DIR / "testdata" / "w3c_rdf_mt"
KNOWN_FAILURES_FILE = TESTS_DIR / "w3c_rdf_mt_known_failures.txt"
BASE = "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-mt/"


class Manifest:
    def __init__(self):
        m = Model()
        m.reads(
            (SUITE_DIR / "manifest.ttl").read_text(encoding="utf-8"),
            format="turtle",
            base_iri=BASE + "manifest.ttl",
        )
        res = run_select(m, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
        self.index = defaultdict(lambda: defaultdict(list))
        for row in res["rows"]:
            self.index[row["s"]][row["p"]].append(row["o"])

    def value(self, s, p):
        objs = self.index.get(s, {}).get(p, [])
        return objs[0] if objs else None

    def collection(self, node) -> list:
        items = []
        while node is not None and node != RDF.nil:
            items.append(self.value(node, RDF.first))
            node = self.value(node, RDF.rest)
        return items


def read_graph(iri) -> Model:
    assert iri[1].startswith(BASE)
    path = SUITE_DIR / iri[1][len(BASE) :]
    fmt = {".nt": "ntriples", ".ttl": "turtle", ".rdf": "rdf/xml"}[path.suffix]
    m = Model()
    m.reads(path.read_text(encoding="utf-8"), format=fmt, base_iri=iri[1])
    return m


def build_params():
    g = Manifest()
    known = load_known_failures(KNOWN_FAILURES_FILE)
    params = []
    manifest = ("uri", BASE + "manifest.ttl")
    for entry in g.collection(g.value(manifest, MF.entries)):
        tid = entry[1].rsplit("#", 1)[-1]
        params.append(pytest.param(g, entry, id=tid, marks=xfail_marks(tid, known)))
    return params


@pytest.mark.parametrize("g,entry", build_params())
def test_w3c_rdf_mt(g: Manifest, entry):
    positive = g.value(entry, RDF.type) == MF.PositiveEntailmentTest
    regime = g.value(entry, MF.entailmentRegime)[1]
    m = read_graph(g.value(entry, MF.action))
    if regime != "simple":
        reason_or_skip(m, "rdfs")
    result = g.value(entry, MF.result)
    if result[0] == "literal":
        # "false": the action is inconsistent. Inconsistencies are not detected.
        assert not positive, "The inconsistency of the action is not detected"
        return
    expected = dump_graph(read_graph(result), None)
    graph = graph_triples(m)
    if positive:
        assert entails(graph, expected), (
            f"The result is not entailed. Missing:\n{missing_triples(graph, expected)}"
        )
    else:
        assert not entails(graph, expected), "The result is entailed"

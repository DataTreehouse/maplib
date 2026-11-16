"""
W3C OWL 2 test cases for Model.reason(profile="owl-rl").

The test data in ``testdata/w3c_owl2_rl`` is from the W3C OWL 2 Test Case Repository, see
``testdata/w3c_owl2_rl/README.md`` and ``LICENSE.md``:

* ``rdfbased-sem.rdf``: the ``rdfbased-sem-*`` core test suite of the OWL 2 RL/RDF rules.
* ``profile-RL.rdf``: the test cases whose ontologies are in the OWL 2 RL profile.

Only test cases with the RDF-based semantics and an RDF/XML premise are run, since the OWL 2
RL/RDF rules are sound for the RDF-based semantics (OWL 2 Profiles, section 4.3). Each test type
of a test case is a separate pytest case:

* ``<id>/positive-entailment`` - the inferred graph must entail the conclusion (simple
  entailment, with the blank nodes of the conclusion as variables).
* ``<id>/negative-entailment`` - the inferred graph must not entail the non-conclusion.
* ``<id>/consistency`` - no rule with ``false`` in the head may match.
* ``<id>/inconsistency`` - some rule with ``false`` in the head must match.

The OWL 2 RL/RDF rules are not complete for OWL 2 Full, nor for conclusions that are not
assertions (OWL 2 Profiles, Theorem PR1), so some tests are expected to fail. They are listed in
``w3c_owl2_rl_known_failures.txt`` as strict xfails with the reason, so a test that starts
passing is reported (XPASS). Run ``pytest test_w3c_owl2_rl.py --runxfail`` to see the failures.
"""

import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from maplib import Model

from .test_w3c_sparql11 import dump_graph
from .w3c_entailment import (
    entails,
    graph_triples,
    load_known_failures,
    missing_triples,
    reason_or_skip,
    xfail_marks,
)

TESTS_DIR = Path(__file__).parent
SUITE_DIR = TESTS_DIR / "testdata" / "w3c_owl2_rl"
SUITE_FILES = ["rdfbased-sem.rdf", "profile-RL.rdf"]
KNOWN_FAILURES_FILE = TESTS_DIR / "w3c_owl2_rl_known_failures.txt"

RDF = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}"
TEST = "{http://www.w3.org/2007/OWL/testOntology#}"
TEST_NS = "http://www.w3.org/2007/OWL/testOntology#"

KINDS = {
    "PositiveEntailmentTest": "positive-entailment",
    "NegativeEntailmentTest": "negative-entailment",
    "ConsistencyTest": "consistency",
    "InconsistencyTest": "inconsistency",
}


def collect_tests():
    """(identifier, kind, test case) for the RDF-based test cases with RDF/XML ontologies."""
    seen = set()
    tests = []
    for f in SUITE_FILES:
        root = ET.parse(SUITE_DIR / f).getroot()
        for case in root.iter(TEST + "TestCase"):
            ident = case.findtext(TEST + "identifier").strip()
            if ident in seen:
                continue
            seen.add(ident)
            resources = lambda tag: {
                e.get(RDF + "resource", "").removeprefix(TEST_NS)
                for e in case.findall(TEST + tag)
            }
            if "RDF-BASED" not in resources("semantics"):
                continue
            premise = case.findtext(TEST + "rdfXmlPremiseOntology")
            if premise is None:
                continue
            types = resources("type") | {
                e.get(RDF + "resource", "").removeprefix(TEST_NS)
                for e in case.findall(RDF + "type")
            }
            for t, kind in KINDS.items():
                if t in types:
                    tests.append((ident, kind, case))
    return tests


def build_params():
    known = load_known_failures(KNOWN_FAILURES_FILE)
    params = []
    for ident, kind, case in collect_tests():
        tid = f"{ident}/{kind}"
        params.append(
            pytest.param(kind, case, id=tid, marks=xfail_marks(tid, known))
        )
    return params


def read_ontology(rdf_xml: str) -> Model:
    m = Model()
    m.reads(rdf_xml, format="rdf/xml")
    return m


@pytest.mark.parametrize("kind,case", build_params())
def test_w3c_owl2_rl(kind, case):
    m = read_ontology(case.findtext(TEST + "rdfXmlPremiseOntology"))
    res = reason_or_skip(m, "owl-rl")
    if kind == "consistency":
        assert res.consistent, f"Unexpected violations: {res.violations}"
    elif kind == "inconsistency":
        assert not res.consistent, "No violations were found"
    elif kind == "positive-entailment":
        conclusion = case.findtext(TEST + "rdfXmlConclusionOntology")
        if conclusion is None:
            pytest.skip("No RDF/XML conclusion")
        expected = dump_graph(read_ontology(conclusion), None)
        graph = graph_triples(m)
        assert entails(graph, expected), (
            f"The conclusion is not entailed. Missing:\n{missing_triples(graph, expected)}"
        )
    elif kind == "negative-entailment":
        nonconclusion = case.findtext(TEST + "rdfXmlNonConclusionOntology")
        if nonconclusion is None:
            pytest.skip("No RDF/XML non-conclusion")
        expected = dump_graph(read_ontology(nonconclusion), None)
        assert not entails(graph_triples(m), expected), "The non-conclusion is entailed"

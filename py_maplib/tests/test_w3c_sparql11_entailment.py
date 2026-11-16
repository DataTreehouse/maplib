"""
W3C SPARQL 1.1 entailment regime tests for Model.reason.

The test data in ``testdata/w3c_sparql11/entailment`` is an unmodified copy of
``sparql/sparql11/entailment`` of https://github.com/w3c/rdf-tests, see
``testdata/w3c_sparql11/LICENSE.md``.

For each ``mf:QueryEvaluationTest`` the data is read and materialized with a profile, the query
is run, and the result is compared as in test_w3c_sparql11.py:

* the RDFS regime is run with the "rdfs" profile,
* the OWL 2 RDF-Based Semantics regime, for tests that list the RL profile, with "owl-rl".

Other regimes (simple, RDF and D entailment only, the OWL 2 Direct Semantics, RIF) are skipped.
Materialization does not restrict the answers to the vocabulary of the queried graph as the
entailment regimes do, and does not infer the existential consequences of OWL 2 RL or RDFS, so
some tests are expected to fail. They are listed in ``w3c_sparql11_entailment_known_failures.txt``
as strict xfails with the reason.
"""

from pathlib import Path

import pytest

from .test_w3c_sparql11 import (
    MF,
    QT,
    Manifest,
    is_ordered,
    match_rows,
    model_with_dataset,
    parse_expected_result,
    query_form,
    read_text,
    result_rows,
    run_select,
    with_base,
)
from .w3c_entailment import load_known_failures, reason_or_skip, xfail_marks

TESTS_DIR = Path(__file__).parent
KNOWN_FAILURES_FILE = TESTS_DIR / "w3c_sparql11_entailment_known_failures.txt"
SD = "http://www.w3.org/ns/sparql-service-description#"
ENT = "http://www.w3.org/ns/entailment/"
PR = "http://www.w3.org/ns/owl-profile/"


def regime_profile(g: Manifest, action):
    """The profile to run a test with, or None if its regimes are not supported."""

    def values(p):
        out = []
        for v in g.objects(action, ("uri", SD + p)):
            out.extend(g.collection(v) if v[0] == "bnode" else [v])
        return {x[1] for x in out if x is not None}

    regimes = values("entailmentRegime")
    profiles = values("EntailmentProfile")
    if ENT + "RDFS" in regimes:
        return "rdfs"
    if ENT + "OWL-RDF-Based" in regimes and (not profiles or PR + "RL" in profiles):
        return "owl-rl"
    return None


def build_params():
    known = load_known_failures(KNOWN_FAILURES_FILE)
    g = Manifest("entailment/manifest.ttl")
    params = []
    for entries in g.objects(g.iri, MF.entries):
        for entry in g.collection(entries):
            tid = "entailment/" + entry[1].rsplit("#", 1)[-1]
            action = g.value(entry, MF.action)
            profile = regime_profile(g, action)
            if profile is None:
                marks = [pytest.mark.skip(reason="unsupported entailment regime")]
            else:
                marks = xfail_marks(tid, known)
            params.append(pytest.param(g, entry, profile, id=tid, marks=marks))
    return params


@pytest.mark.parametrize("g,entry,profile", build_params())
def test_w3c_sparql11_entailment(g: Manifest, entry, profile):
    action = g.value(entry, MF.action)
    query_iri = g.value(action, QT.query)
    query = with_base(read_text(query_iri), query_iri)
    m = model_with_dataset(g, action)
    reason_or_skip(m, profile)
    expected = parse_expected_result(g.value(entry, MF.result))
    actual = run_select(m, query)
    if query_form(read_text(query_iri)) == "ASK" or expected["boolean"] is not None:
        assert actual["boolean"] == expected["boolean"]
        return
    exp_vars = expected["vars"]
    assert set(exp_vars) == set(actual["vars"]), (
        f"Variables differ: expected {exp_vars}, got {actual['vars']}"
    )
    exp_rows = result_rows(expected, exp_vars)
    act_rows = result_rows(actual, exp_vars)
    assert match_rows(exp_rows, act_rows, ordered=is_ordered(read_text(query_iri))), (
        f"Results differ.\nExpected: {exp_rows}\nActual: {act_rows}"
    )

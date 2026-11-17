"""
W3C SPARQL 1.0 (DAWG) compliance tests.

The test data in ``testdata/w3c_sparql10`` is an unmodified copy of ``sparql/sparql10`` of
https://github.com/w3c/rdf-tests (commit 7b5afb91), see ``testdata/w3c_sparql10/LICENSE.md``.
These are the query evaluation tests (``manifest-evaluation.ttl``: basic graph patterns,
OPTIONAL, FILTER, type promotion, casts, the builtin functions, ORDER BY, DISTINCT, REDUCED,
CONSTRUCT, ASK and datasets), the syntax tests (``manifest-syntax.ttl``) and the extended
evaluation tests (``extended-manifest-evaluation.ttl``: sort-11, the relative order of plain
literals and xsd:strings). ``open-world/sameTerm-manifest.ttl`` is included by no other manifest
and is collected as well, so every test of the suite is run (491 entries).

The tests are run with the machinery of ``test_w3c_sparql11.py``, which is loaded here as a
separate module with the base IRI, directory and manifests of this suite, so the two suites
compare results in the same way.

* ``mf:QueryEvaluationTest`` - load the dataset, run the query, compare with the expected
  result (SPARQL XML results, results as RDF in the result set vocabulary, or an RDF graph).
* ``mf:PositiveSyntaxTest`` / ``mf:NegativeSyntaxTest`` - must parse / must be rejected.

Tests that maplib currently does not pass are listed in ``w3c_sparql10_known_failures.txt``
and are marked as strict xfails, so a test that starts passing is reported (XPASS) and should
then be removed from that list. Run ``pytest test_w3c_sparql10.py --runxfail`` to see the
actual failures.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).parent


def _load_sparql11_machinery():
    """A separate instance of the SPARQL 1.1 runner, pointed at the SPARQL 1.0 suite."""
    name = __name__.rsplit(".", 1)[0] + "._w3c_sparql10_runner"
    spec = importlib.util.spec_from_file_location(name, TESTS_DIR / "test_w3c_sparql11.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    # Its functions read the suite settings from module globals when called, so they are
    # set on this instance below, after it has been loaded.
    spec.loader.exec_module(mod)
    return mod


s = _load_sparql11_machinery()
s.SUITE_DIR = TESTS_DIR / "testdata" / "w3c_sparql10"
s.KNOWN_FAILURES_FILE = TESTS_DIR / "w3c_sparql10_known_failures.txt"
s.BASE = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/"
s.TOP_MANIFESTS = [
    "manifest-evaluation.ttl",
    "manifest-syntax.ttl",
    "extended-manifest-evaluation.ttl",
]
# sameTerm and mf:StringSimpleLiteralCmp: a manifest of the suite that no other one includes.
s.EXTRA_MANIFESTS = ["open-world/sameTerm-manifest.ttl"]
s.SUPPORTED_TYPES = {
    s.MF.QueryEvaluationTest,
    s.MF.PositiveSyntaxTest,
    s.MF.NegativeSyntaxTest,
}


@pytest.mark.parametrize("manifest,entry", s.build_params())
def test_w3c_sparql10(manifest, entry):
    types = s.entry_types(manifest, entry)
    if s.MF.QueryEvaluationTest in types:
        s.check_query_evaluation(manifest, entry)
    elif s.MF.PositiveSyntaxTest in types:
        s.check_syntax(manifest, entry, update=False, positive=True)
    elif s.MF.NegativeSyntaxTest in types:
        s.check_syntax(manifest, entry, update=False, positive=False)

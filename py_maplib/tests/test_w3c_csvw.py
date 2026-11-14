"""
W3C CSV on the Web (CSVW) compliance tests for the conversion to RDF (Model.map_csvw).

The test data in ``testdata/w3c_csvw`` is the part of ``tests`` from
https://github.com/w3c/csvw (commit b3f461db0e86a68c019bc1f912e86f3555907e34) that is used by
``manifest-rdf.jsonld``, see ``testdata/w3c_csvw/LICENSE.md``. The file
``testdata/w3c_csvw/well-known/csvm`` is the site-wide location configuration that the test
suite expects at ``http://www.w3.org/.well-known/csvm``.

For each test the action is converted with maplib, with the test suite base IRI
``http://www.w3.org/2013/csvw/tests/`` mapped to the test data folder:

* ``csvt:ToRdfTest`` and ``csvt:ToRdfTestWithWarnings`` - the result is compared with the
  expected Turtle up to blank node renaming. For tests with warnings, maplib must also warn.
* ``csvt:NegativeRdfTest`` - the conversion must fail.

Tests that maplib currently does not pass are listed in ``w3c_csvw_known_failures.txt`` and
are marked as strict xfails, so a test that starts passing is reported (XPASS) and should be
removed from that list. Run pytest with ``--runxfail`` to see the actual failures.
"""

import json
import warnings
from pathlib import Path
from typing import Dict

import pytest
import rdflib
from rdflib.compare import graph_diff, isomorphic, to_isomorphic

from maplib import MaplibException, Model

TESTS_DIR = Path(__file__).parent
CSVW_DIR = TESTS_DIR / "testdata" / "w3c_csvw"
KNOWN_FAILURES = TESTS_DIR / "w3c_csvw_known_failures.txt"
BASE = "http://www.w3.org/2013/csvw/tests/"
# The site-wide location configuration of w3.org, see testdata/w3c_csvw/well-known/csvm
WELL_KNOWN = "http://www.w3.org/.well-known/"


def load_known_failures() -> Dict[str, str]:
    known = {}
    if KNOWN_FAILURES.exists():
        for line in KNOWN_FAILURES.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            tid, _, reason = line.partition("#")
            known[tid.strip()] = reason.strip() or "known failure"
    return known


def build_params():
    manifest = json.loads((CSVW_DIR / "manifest-rdf.jsonld").read_text(encoding="utf-8"))
    known = load_known_failures()
    params = []
    for entry in manifest["entries"]:
        tid = entry["id"].split("#")[1]
        marks = []
        if tid in known:
            marks.append(pytest.mark.xfail(reason=known[tid], strict=True))
        params.append(pytest.param(entry, id=tid, marks=marks))
    return params


def normalize(g: rdflib.Graph) -> rdflib.Graph:
    """Literals are compared by value where rdflib knows the datatype, since maplib stores
    numbers and dates as values."""
    out = rdflib.Graph()
    for s, p, o in g:
        if isinstance(o, rdflib.Literal) and o.datatype is not None and o.value is not None:
            try:
                o = rdflib.Literal(o.value, datatype=o.datatype)
            except Exception:
                pass
        out.add((s, p, o))
    return out


def run_conversion(entry, caught):
    m = Model()
    option = entry.get("option", {})
    metadata = BASE + option["metadata"] if "metadata" in option else None
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        m.map_csvw(
            BASE + entry["action"],
            metadata=metadata,
            minimal=option.get("minimal", False),
            iri_mappings={BASE: str(CSVW_DIR), WELL_KNOWN: str(CSVW_DIR / "well-known")},
        )
        caught.extend(w)
    return m


@pytest.mark.parametrize("entry", build_params())
def test_w3c_csvw(entry):
    if "httpLink" in entry:
        pytest.fail("maplib does not fetch documents over HTTP, so Link headers are not supported")
    caught = []
    if entry["type"] == "csvt:NegativeRdfTest":
        with pytest.raises(MaplibException):
            run_conversion(entry, caught)
        return
    m = run_conversion(entry, caught)
    actual = rdflib.Graph()
    actual.parse(data=m.writes(format="ntriples"), format="nt")
    expected = rdflib.Graph()
    expected.parse(
        CSVW_DIR / entry["result"], format="turtle", publicID=BASE + entry["result"]
    )
    actual = normalize(actual)
    expected = normalize(expected)
    if not isomorphic(actual, expected):
        _, only_actual, only_expected = graph_diff(to_isomorphic(actual), to_isomorphic(expected))
        msg = "Only in maplib output:\n"
        msg += "\n".join(sorted(f"  {t}" for t in only_actual.serialize(format="nt").splitlines()))
        msg += "\nOnly in expected output:\n"
        msg += "\n".join(sorted(f"  {t}" for t in only_expected.serialize(format="nt").splitlines()))
        pytest.fail(msg)
    if entry["type"] == "csvt:ToRdfTestWithWarnings":
        assert caught, "expected a warning"

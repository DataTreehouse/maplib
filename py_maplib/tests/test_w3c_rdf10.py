"""
W3C RDF 1.0 Test Cases (2004).

The test data in ``testdata/w3c_rdf10`` is an unmodified copy of the RDF Test Cases of RDF 1.0
(W3C Recommendation, 10 February 2004), see ``testdata/w3c_rdf10/LICENSE.md``. Their single
manifest, ``Manifest.rdf``, is read with rdflib, and a pytest case is generated for each test
with the status APPROVED (the others, NOT_APPROVED, OBSOLETE(D) and WITHDRAWN, are skipped):

* ``test:PositiveParserTest`` - parse the RDF/XML input document with maplib and compare the
  graph with the expected N-Triples document, up to blank node renaming. The expected document
  is parsed by the N-Quads parser of ``w3c_rdf.py``, not by maplib.
* ``test:NegativeParserTest`` - parsing the RDF/XML input document must fail.
* ``test:PositiveEntailmentTest`` / ``test:NegativeEntailmentTest`` - read the premise
  documents, reason with the "rdfs" profile when the test names entailment rules (none means
  simple entailment), and the result must (or must not) entail the conclusion document, as in
  ``test_w3c_rdf_mt.py``. A conclusion of ``test:False-Document`` means that the premises are
  inconsistent; maplib does not detect inconsistencies, so only the negative ones pass. A
  premise of ``test:False-Document`` (which entails anything) cannot be read, so such a test is
  skipped. Builds without reasoning skip the entailment tests.
* ``test:MiscellaneousTest`` (a recommendation for RDF/XML writers) is skipped.

RDF 1.1 kept most of these tests, updated for its semantics, in ``rdf/rdf11/rdf-xml`` and
``rdf/rdf11/rdf-mt`` of https://github.com/w3c/rdf-tests (``test_w3c_rdf11.py`` and
``test_w3c_rdf_mt.py``). Literals are compared as RDF 1.1 terms, in which a plain literal is
an xsd:string literal.

Tests that maplib currently does not pass are listed in ``w3c_rdf10_known_failures.txt`` as
strict xfails. Run ``pytest test_w3c_rdf10.py --runxfail`` to see the actual failures.
"""

from pathlib import Path
from typing import List

import pytest
import rdflib

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
from .w3c_rdf import dump_dataset, format_quads, isomorphic, parse_nquads

rdflib.NORMALIZE_LITERALS = False

TESTS_DIR = Path(__file__).parent
SUITE_DIR = TESTS_DIR / "testdata" / "w3c_rdf10"
KNOWN_FAILURES_FILE = TESTS_DIR / "w3c_rdf10_known_failures.txt"
BASE = "http://www.w3.org/2000/10/rdf-tests/rdfcore/"
TEST = rdflib.Namespace(BASE + "testSchema#")

PARSER_TESTS = {TEST.PositiveParserTest, TEST.NegativeParserTest}
ENTAILMENT_TESTS = {TEST.PositiveEntailmentTest, TEST.NegativeEntailmentTest}


def iri_to_path(iri) -> Path:
    s = str(iri)
    assert s.startswith(BASE), f"IRI {s} is not inside the test suite"
    return SUITE_DIR / s[len(BASE) :]


def read_file(iri) -> str:
    # Bytes are decoded directly: reading as text would turn carriage returns into line feeds.
    return iri_to_path(iri).read_bytes().decode("utf-8")


def manifest() -> rdflib.Graph:
    g = rdflib.Graph()
    g.parse(SUITE_DIR / "Manifest.rdf", format="xml", publicID=BASE + "Manifest.rdf")
    return g


def documents(g: rdflib.Graph, test, p) -> List[rdflib.term.Node]:
    return sorted(g.objects(test, p), key=str)


def make_test_id(test) -> str:
    # e.g. http://.../rdfcore/amp-in-url/Manifest.rdf#test001 -> amp-in-url/test001
    rel = str(test)[len(BASE) :]
    directory, _, name = rel.partition("/Manifest.rdf#")
    return f"{directory}/{name}"


def build_params():
    g = manifest()
    known = load_known_failures(KNOWN_FAILURES_FILE)
    params = []
    for t in sorted(PARSER_TESTS | ENTAILMENT_TESTS | {TEST.MiscellaneousTest}, key=str):
        for test in sorted(g.subjects(rdflib.RDF.type, t), key=str):
            tid = make_test_id(test)
            status = str(g.value(test, TEST.status))
            if status != "APPROVED":
                marks = [pytest.mark.skip(reason=f"status {status}")]
            elif t == TEST.MiscellaneousTest:
                marks = [pytest.mark.skip(reason="a recommendation for RDF/XML writers")]
            else:
                marks = xfail_marks(tid, known)
            params.append(pytest.param(g, test, t, id=tid, marks=marks))
    ids = {p.id for p in params}
    unknown = sorted(set(known) - ids)
    assert not unknown, f"Known failures that are not tests: {unknown}"
    return params


def read_document(m: Model, doc):
    """Read an N-Triples or RDF/XML document of the suite into the model."""
    fmt = {".nt": "ntriples", ".rdf": "rdf/xml"}[iri_to_path(doc).suffix]
    m.reads(read_file(doc), format=fmt, base_iri=str(doc))


def check_parser(g: rdflib.Graph, test, positive: bool):
    (inp,) = documents(g, test, TEST.inputDocument)
    m = Model()
    if not positive:
        with pytest.raises(Exception):
            read_document(m, inp)
        return
    read_document(m, inp)
    (out,) = documents(g, test, TEST.outputDocument)
    expected = parse_nquads(read_file(out))
    actual = dump_dataset(m)
    assert isomorphic(expected, actual), (
        f"Graphs differ.\nExpected:\n{format_quads(expected)}\nActual:\n{format_quads(actual)}"
    )


def is_false_document(g: rdflib.Graph, doc) -> bool:
    return (doc, rdflib.RDF.type, TEST["False-Document"]) in g


def check_entailment(g: rdflib.Graph, test, positive: bool):
    premises = documents(g, test, TEST.premiseDocument)
    if any(is_false_document(g, p) for p in premises):
        pytest.skip("The premise is the inconsistent graph (False), which entails anything")
    m = Model()
    for premise in premises:
        read_document(m, premise)
    if list(g.objects(test, TEST.entailmentRules)):
        reason_or_skip(m, "rdfs")
    (conclusion,) = documents(g, test, TEST.conclusionDocument)
    if is_false_document(g, conclusion):
        # The premises are inconsistent. Inconsistencies are not detected.
        assert not positive, "The inconsistency of the premises is not detected"
        return
    c = Model()
    read_document(c, conclusion)
    expected = dump_graph(c, None)
    graph = graph_triples(m)
    if positive:
        assert entails(graph, expected), (
            f"The conclusion is not entailed. Missing:\n{missing_triples(graph, expected)}"
        )
    else:
        assert not entails(graph, expected), "The conclusion is entailed"


@pytest.mark.parametrize("g,test,test_type", build_params())
def test_w3c_rdf10(g: rdflib.Graph, test, test_type):
    if test_type in PARSER_TESTS:
        check_parser(g, test, positive=test_type == TEST.PositiveParserTest)
    else:
        check_entailment(g, test, positive=test_type == TEST.PositiveEntailmentTest)

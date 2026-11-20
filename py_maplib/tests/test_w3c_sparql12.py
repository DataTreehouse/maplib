"""
W3C SPARQL 1.2 compliance tests.

The test data in ``testdata/w3c_sparql12`` is a copy of ``sparql/sparql12`` of
https://github.com/w3c/rdf-tests (commit 56076c0efec4c353efcae2971be8f6005294ac64), see
``testdata/w3c_sparql12/LICENSE.md``, without the implementation reports. It covers triple
terms and reifiers in queries and results (``eval-triple-terms``,
``syntax-triple-terms-*``), literals with a base direction (``lang-basedir``), the new
functions and grouping (``expression``, ``grouping``), codepoint escapes, the VERSION
declaration, and RDF 1.1 behaviour that SPARQL 1.2 keeps (``rdf11``).

The tests are run with the machinery of ``test_w3c_sparql11.py``, loaded here as a separate
module with the base IRI, directory and manifests of this suite (as in
``test_w3c_sparql10.py``). Its handling of RDF terms is replaced by versions that know triple
terms and base directions:

* Expected graphs and the data files (for the literals that must keep their lexical form) are
  read with pyoxigraph, which reads RDF 1.2, instead of rdflib, which does not. Manifests are
  still read with rdflib, so that a maplib parser bug cannot drop tests from the suite.
* SPARQL JSON and XML results are parsed with triple terms (``"type": "triple"`` /
  ``<triple>``) and base directions (``its:dir``).
* Blank nodes inside triple terms take part in the blank node bijection when comparing.

Terms are the tuples of the SPARQL 1.1 runner, with ("triple", s, p, o) for triple terms, and
a base direction written into the language tag ("en--rtl"), as in Turtle.

Tests that maplib currently does not pass are listed in ``w3c_sparql12_known_failures.txt``
and are marked as strict xfails, so a test that starts passing is reported (XPASS) and should
then be removed from that list.
"""

import importlib.util
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path
from typing import List

import pyoxigraph as ox
import pytest

TESTS_DIR = Path(__file__).parent


def _load_sparql11_machinery():
    """A separate instance of the SPARQL 1.1 runner, pointed at the SPARQL 1.2 suite."""
    name = __name__.rsplit(".", 1)[0] + "._w3c_sparql12_runner"
    spec = importlib.util.spec_from_file_location(name, TESTS_DIR / "test_w3c_sparql11.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    # Its functions read the suite settings and helpers from module globals when called, so
    # they are replaced on this instance below, after it has been loaded.
    spec.loader.exec_module(mod)
    return mod


s = _load_sparql11_machinery()
s.SUITE_DIR = TESTS_DIR / "testdata" / "w3c_sparql12"
s.KNOWN_FAILURES_FILE = TESTS_DIR / "w3c_sparql12_known_failures.txt"
s.BASE = "https://w3c.github.io/rdf-tests/sparql/sparql12/"
s.TOP_MANIFESTS = ["manifest.ttl"]
s.EXTRA_MANIFESTS = []
s.SUPPORTED_TYPES = {
    s.MF.QueryEvaluationTest,
    s.MF.UpdateEvaluationTest,
    s.MF.PositiveSyntaxTest,
    s.MF.NegativeSyntaxTest,
    s.MF.PositiveUpdateSyntaxTest,
    s.MF.NegativeUpdateSyntaxTest,
}


# ---------------------------------------------------------------------------
# RDF 1.2 terms
# ---------------------------------------------------------------------------


def literal(lex: str, datatype=None, lang=None, direction=None):
    """A literal, with the base direction (if any) appended to the language tag."""
    if lang and direction:
        lang = f"{lang}--{direction}"
    return s.literal(lex, datatype, lang)


def is_triple(t) -> bool:
    return t is not None and t[0] == "triple"


def has_bnode(t) -> bool:
    if t is None:
        return False
    if t[0] == "bnode":
        return True
    return is_triple(t) and any(has_bnode(x) for x in t[1:])


def n3(t) -> str:
    if is_triple(t):
        return "<<( " + " ".join(n3(x) for x in t[1:]) + " )>>"
    return SPARQL11_N3(t)


SPARQL11_N3 = s.n3
s.n3 = n3


def ox_term(t):
    if isinstance(t, ox.NamedNode):
        return ("uri", t.value)
    if isinstance(t, ox.BlankNode):
        return ("bnode", t.value)
    if isinstance(t, ox.Triple):
        return ("triple", ox_term(t.subject), ox_term(t.predicate), ox_term(t.object))
    return literal(t.value, t.datatype.value, t.language, t.direction)


OX_FORMATS = {
    ".ttl": ox.RdfFormat.TURTLE,
    ".nt": ox.RdfFormat.N_TRIPLES,
    ".nq": ox.RdfFormat.N_QUADS,
    ".trig": ox.RdfFormat.TRIG,
    ".rdf": ox.RdfFormat.RDF_XML,
}


def load_graph(iri) -> List[tuple]:
    """Reads an RDF 1.2 file with pyoxigraph (not maplib) and returns its triples, of all
    graphs."""
    path = s.iri_to_path(iri)
    quads = ox.parse(
        path=str(path),
        format=OX_FORMATS[path.suffix],
        base_iri=iri[1] if isinstance(iri, tuple) else iri,
    )
    return [(ox_term(q.subject), ox_term(q.predicate), ox_term(q.object)) for q in quads]


def dump_graph(m, graph) -> List[tuple]:
    """The triples of a graph of the model, written by maplib as N-Triples."""
    nt = m.writes(format="ntriples", graph=graph)
    return [
        (ox_term(q.subject), ox_term(q.predicate), ox_term(q.object))
        for q in ox.parse(nt.encode(), format=ox.RdfFormat.N_TRIPLES)
    ]


def rdf_format(path: Path) -> str:
    if path.suffix == ".nq":
        return "nquads"
    return SPARQL11_RDF_FORMAT(path)


SPARQL11_RDF_FORMAT = s.rdf_format
s.load_graph = load_graph
s.dump_graph = dump_graph
s.rdf_format = rdf_format


# ---------------------------------------------------------------------------
# SPARQL 1.2 results
# ---------------------------------------------------------------------------


def json_term(b: dict):
    if b["type"] == "triple":
        v = b["value"]
        return ("triple", json_term(v["subject"]), json_term(v["predicate"]), json_term(v["object"]))
    if b["type"] == "uri":
        return ("uri", b["value"])
    if b["type"] == "bnode":
        return ("bnode", b["value"])
    return literal(b["value"], b.get("datatype"), b.get("xml:lang"), b.get("its:dir"))


SRX = "{http://www.w3.org/2005/sparql-results#}"
XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"
ITS_DIR = "{http://www.w3.org/2005/11/its}dir"


def xml_term(el):
    if el.tag == SRX + "triple":
        parts = {child.tag[len(SRX):]: xml_term(list(child)[0]) for child in el}
        return ("triple", parts["subject"], parts["predicate"], parts["object"])
    text = el.text or ""
    if el.tag == SRX + "uri":
        return ("uri", text)
    if el.tag == SRX + "bnode":
        return ("bnode", text)
    return literal(text, el.get("datatype"), el.get(XML_LANG), el.get(ITS_DIR))


def parse_xml_results(path: Path) -> dict:
    root = ET.parse(path).getroot()
    variables = [v.get("name") for v in root.iter(SRX + "variable")]
    boolean = root.find(SRX + "boolean")
    if boolean is not None:
        return {"vars": variables, "boolean": boolean.text.strip() == "true", "rows": []}
    rows = []
    for result in root.iter(SRX + "result"):
        rows.append(
            {b.get("name"): xml_term(list(b)[0]) for b in result.findall(SRX + "binding")}
        )
    return {"vars": variables, "boolean": None, "rows": rows}


s.json_term = json_term
s.parse_xml_results = parse_xml_results


# ---------------------------------------------------------------------------
# Comparison with blank nodes inside triple terms
# ---------------------------------------------------------------------------


def term_key(t):
    """A hashable key that is equal for matching terms, with blank nodes collapsed also inside
    triple terms."""
    if is_triple(t):
        return ("triple",) + tuple(term_key(x) for x in t[1:])
    return SPARQL11_TERM_KEY(t)


SPARQL11_TERM_KEY = s.term_key


def match_rows(expected: List[tuple], actual: List[tuple], ordered: bool) -> bool:
    """Match rows of terms, allowing a consistent bijection between blank nodes, which may
    occur inside triple terms. As the SPARQL 1.1 runner, with the unification recursing into
    triple terms."""
    if len(expected) != len(actual):
        return False
    if not ordered and Counter(tuple(map(term_key, r)) for r in expected) != Counter(
        tuple(map(term_key, r)) for r in actual
    ):
        return False

    def unify_term(e, a, mapping, rev, new) -> bool:
        if s.is_bnode(e) or s.is_bnode(a):
            if not (s.is_bnode(e) and s.is_bnode(a)):
                return False
            if e in mapping:
                return mapping[e] == a
            if a in rev:
                return False
            mapping[e] = a
            rev[a] = e
            new.append(e)
            return True
        if e is None or a is None:
            return e is a
        if is_triple(e) or is_triple(a):
            if not (is_triple(e) and is_triple(a)):
                return False
            return all(unify_term(x, y, mapping, rev, new) for x, y in zip(e[1:], a[1:]))
        return s.terms_equal(e, a)

    def unify(er, ar, mapping, rev):
        new = []
        if all(unify_term(e, a, mapping, rev, new) for e, a in zip(er, ar)):
            return new
        undo(new, mapping, rev)
        return None

    def undo(new, mapping, rev):
        for e in new:
            del rev[mapping.pop(e)]

    if ordered:
        mapping, rev = {}, {}
        return all(unify(er, ar, mapping, rev) is not None for er, ar in zip(expected, actual))

    if not any(has_bnode(t) for r in expected for t in r):
        return True  # The Counter comparison above was sufficient.

    expected = sorted(expected, key=lambda r: sum(has_bnode(t) for t in r))
    used = [False] * len(actual)
    mapping, rev = {}, {}

    def search(i):
        if i == len(expected):
            return True
        for j, ar in enumerate(actual):
            if used[j]:
                continue
            new = unify(expected[i], ar, mapping, rev)
            if new is None:
                continue
            used[j] = True
            if search(i + 1):
                return True
            used[j] = False
            undo(new, mapping, rev)
        return False

    return search(0)


s.term_key = term_key
s.match_rows = match_rows


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("manifest,entry", s.build_params())
def test_w3c_sparql12(manifest, entry):
    types = s.entry_types(manifest, entry)
    if s.MF.QueryEvaluationTest in types:
        s.check_query_evaluation(manifest, entry)
    elif s.MF.UpdateEvaluationTest in types:
        s.check_update_evaluation(manifest, entry)
    elif s.MF.PositiveSyntaxTest in types:
        s.check_syntax(manifest, entry, update=False, positive=True)
    elif s.MF.NegativeSyntaxTest in types:
        s.check_syntax(manifest, entry, update=False, positive=False)
    elif s.MF.PositiveUpdateSyntaxTest in types:
        s.check_syntax(manifest, entry, update=True, positive=True)
    elif s.MF.NegativeUpdateSyntaxTest in types:
        s.check_syntax(manifest, entry, update=True, positive=False)

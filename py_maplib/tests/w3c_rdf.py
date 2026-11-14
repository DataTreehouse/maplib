"""
Harness for the W3C RDF compliance test suites, used by test_w3c_rdf11.py (RDF 1.1).

The manifests are read with maplib and a pytest case is generated for each of:

* ``rdft:Test{NTriples,NQuads,Turtle,Trig}{Positive,Negative}Syntax`` and
  ``rdft:TestXMLNegativeSyntax`` - parsing must succeed / fail.
* ``rdft:Test{Turtle,Trig,XML}Eval`` - parse the action with maplib and compare the dataset
  with the expected N-Triples / N-Quads, up to blank node renaming (also inside triple terms).
  The expected result is parsed by the N-Quads 1.2 parser in this file, not by maplib.
* ``rdft:Test{NTriples,NQuads}PositiveC14N`` - parse the action, write it as N-Triples /
  N-Quads with maplib and compare the lines with the expected canonical form.
* Serializer round trips: for each eval test, the parsed dataset is written with maplib in
  every format that can hold it (N-Triples, Turtle, RDF/XML and HDT only for a default graph;
  JSON-LD with the graphs of the expected result given as a list in ``graph``), read back, and
  compared with the expected result. These have the ids ``<test id>@<format>``.

Entailment tests are skipped. The dataset of a model is read back with a SPARQL query with
JSON results, which carry triple terms and base directions, so that the eval tests do not
depend on the writers.

Tests that maplib currently does not pass are listed in the known failures file of the suite
and are marked as strict xfails, so a test that starts passing is reported (XPASS) and
should be removed from that list. Run pytest with ``--runxfail`` to see the actual failures.

RDF terms are tuples: ("uri", iri), ("bnode", label),
("literal", lexical form, datatype or None, language or None, direction or None) and
("triple", subject, predicate, object).
"""

import io
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import pytest

from maplib import DEFAULT_GRAPH, Model

TESTS_DIR = Path(__file__).parent


@dataclass(frozen=True)
class Suite:
    # The canonical base IRI of the test suite, relative IRIs are resolved against it.
    base: str
    directory: Path
    known_failures: Path


RDF11 = Suite(
    base="https://w3c.github.io/rdf-tests/rdf/rdf11/",
    directory=TESTS_DIR / "testdata" / "w3c_rdf11",
    known_failures=TESTS_DIR / "w3c_rdf11_known_failures.txt",
)

SUITES = (RDF11,)


class Namespace(str):
    def __getattr__(self, name):
        return ("uri", self + name)


MF = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
RDFT = Namespace("http://www.w3.org/ns/rdftest#")
RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
XSD = "http://www.w3.org/2001/XMLSchema#"

SYNTAX_TESTS = {
    RDFT.TestNTriplesPositiveSyntax: ("ntriples", True),
    RDFT.TestNTriplesNegativeSyntax: ("ntriples", False),
    RDFT.TestNQuadsPositiveSyntax: ("nquads", True),
    RDFT.TestNQuadsNegativeSyntax: ("nquads", False),
    RDFT.TestTurtlePositiveSyntax: ("turtle", True),
    RDFT.TestTurtleNegativeSyntax: ("turtle", False),
    RDFT.TestTrigPositiveSyntax: ("trig", True),
    RDFT.TestTrigNegativeSyntax: ("trig", False),
    RDFT.TestXMLNegativeSyntax: ("rdf/xml", False),
}
EVAL_TESTS = {
    RDFT.TestTurtleEval: "turtle",
    RDFT.TestTrigEval: "trig",
    RDFT.TestXMLEval: "rdf/xml",
}
C14N_TESTS = {
    RDFT.TestNTriplesPositiveC14N: "ntriples",
    RDFT.TestNQuadsPositiveC14N: "nquads",
}
SUPPORTED_TYPES = set(SYNTAX_TESTS) | set(EVAL_TESTS) | set(C14N_TESTS)


# ---------------------------------------------------------------------------
# Terms
# ---------------------------------------------------------------------------


def literal(lex, datatype=None, lang=None, direction=None):
    if lang:
        return ("literal", lex, None, lang.lower(), direction)
    if datatype == XSD + "string":
        datatype = None
    return ("literal", lex, datatype, None, None)


def n3(t) -> str:
    if t is None:
        return ""
    if t[0] == "uri":
        return f"<{t[1]}>"
    if t[0] == "bnode":
        return f"_:{t[1]}"
    if t[0] == "triple":
        return "<<( " + " ".join(n3(x) for x in t[1:]) + " )>>"
    _, lex, dt, lang, direction = t
    s = json.dumps(lex, ensure_ascii=False)
    if lang:
        return f"{s}@{lang}" + (f"--{direction}" if direction else "")
    if dt:
        return f"{s}^^<{dt}>"
    return s


def json_term(b: dict):
    t = b["type"]
    if t == "uri":
        return ("uri", b["value"])
    if t == "bnode":
        return ("bnode", b["value"])
    if t == "triple":
        v = b["value"]
        return ("triple", json_term(v["subject"]), json_term(v["predicate"]), json_term(v["object"]))
    return literal(b["value"], b.get("datatype"), b.get("xml:lang"), b.get("its:dir"))


def select(m: Model, query: str) -> List[dict]:
    res = json.loads(m.query(query, return_format="json"))
    return [
        {k: json_term(v) for k, v in row.items()} for row in res["results"]["bindings"]
    ]


def dump_dataset(m: Model) -> List[tuple]:
    """The quads of the model, (s, p, o, g) with g None for the default graph."""
    quads = [
        (r["s"], r["p"], r["o"], None)
        for r in select(m, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
    ]
    quads += [
        (r["s"], r["p"], r["o"], r["g"])
        for r in select(m, "SELECT ?g ?s ?p ?o WHERE { GRAPH ?g { ?s ?p ?o } }")
    ]
    return quads


# ---------------------------------------------------------------------------
# An N-Quads 1.2 parser for the expected results, independent of maplib
# ---------------------------------------------------------------------------

_ESCAPES = {"t": "\t", "b": "\b", "n": "\n", "r": "\r", "f": "\f", '"': '"', "'": "'", "\\": "\\"}
_ESCAPE_RE = re.compile(r"\\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8}|.)")


def unescape(s: str) -> str:
    def repl(m):
        e = m.group(1)
        if e[0] in "uU":
            return chr(int(e[1:], 16))
        return _ESCAPES.get(e, e)

    return _ESCAPE_RE.sub(repl, s)


_TOKEN_RE = re.compile(
    r"""\s*(?:
      (?P<open><<\()
    | (?P<close>\)>>)
    | (?P<iri><[^<>\s"]*>)
    | (?P<bnode>_:[^\s<>"()]*[^\s<>"().])
    | (?P<lit>"(?:[^"\\]|\\.)*")(?:@(?P<lang>[A-Za-z][A-Za-z0-9-]*?)(?:--(?P<dir>ltr|rtl))?(?=[\s.)<])|\^\^(?P<dt><[^<>\s"]*>))?
    | (?P<dot>\.)
    )""",
    re.VERBOSE,
)


def parse_nquads(text: str) -> List[tuple]:
    quads = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        tokens = []
        pos = 0
        while pos < len(line):
            if line[pos:].lstrip().startswith("#"):
                break
            m = _TOKEN_RE.match(line, pos)
            assert m is not None and m.end() > pos, f"Cannot parse N-Quads line {line!r}"
            tokens.append(m)
            pos = m.end()
            if not line[pos:].strip():
                break
        it = iter(tokens)

        def term(tok):
            if tok.group("iri"):
                return ("uri", unescape(tok.group("iri")[1:-1]))
            if tok.group("bnode"):
                return ("bnode", tok.group("bnode")[2:])
            if tok.group("lit"):
                dt = tok.group("dt")
                return literal(
                    unescape(tok.group("lit")[1:-1]),
                    unescape(dt[1:-1]) if dt else None,
                    tok.group("lang"),
                    tok.group("dir"),
                )
            if tok.group("open"):
                s, p, o = term(next(it)), term(next(it)), term(next(it))
                assert next(it).group("close"), f"Unclosed triple term in {line!r}"
                return ("triple", s, p, o)
            raise AssertionError(f"Unexpected token in {line!r}")

        items = []
        for tok in it:
            if tok.group("dot"):
                break
            items.append(term(tok))
        assert len(items) in (3, 4), f"Cannot parse N-Quads line {line!r}"
        quads.append((items[0], items[1], items[2], items[3] if len(items) == 4 else None))
    return quads


# ---------------------------------------------------------------------------
# Isomorphism up to blank node renaming, also inside triple terms
# ---------------------------------------------------------------------------


def bnodes_of(t, out: set):
    if t is None:
        return
    if t[0] == "bnode":
        out.add(t[1])
    elif t[0] == "triple":
        for x in t[1:]:
            bnodes_of(x, out)


def rename(t, mapping: Dict[str, str]):
    if t is None:
        return None
    if t[0] == "bnode":
        return ("bnode", mapping[t[1]])
    if t[0] == "triple":
        return ("triple",) + tuple(rename(x, mapping) for x in t[1:])
    return t


def shape(t, colors: Dict[str, int]):
    """The term with blank nodes replaced by their colors."""
    if t is None:
        return None
    if t[0] == "bnode":
        return ("bnode", colors[t[1]])
    if t[0] == "triple":
        return ("triple",) + tuple(shape(x, colors) for x in t[1:])
    return t


def refine(quads: List[tuple], bnodes: set) -> Dict[str, int]:
    """Color refinement: blank nodes get colors from the quads they occur in."""
    colors = {b: 0 for b in bnodes}
    for _ in range(len(bnodes) + 1):
        sigs = defaultdict(list)
        for q in quads:
            shaped = tuple(shape(x, colors) for x in q)
            occ = set()
            for i, x in enumerate(q):
                found = set()
                bnodes_of(x, found)
                for b in found:
                    occ.add((b, i))
            for b, i in occ:
                sigs[b].append((i, repr(shaped)))
        new = {b: hash((colors[b], tuple(sorted(sigs[b])))) for b in bnodes}
        if len(set(new.values())) == len(set(colors.values())):
            return new
        colors = new
    return colors


def isomorphic(expected: List[tuple], actual: List[tuple]) -> bool:
    expected, actual = set(expected), set(actual)
    if len(expected) != len(actual):
        return False
    eb, ab = set(), set()
    for q in expected:
        for x in q:
            bnodes_of(x, eb)
    for q in actual:
        for x in q:
            bnodes_of(x, ab)
    if len(eb) != len(ab):
        return False
    if not eb:
        return expected == actual
    ec, ac = refine(list(expected), eb), refine(list(actual), ab)
    if Counter(ec.values()) != Counter(ac.values()):
        return False
    eb_sorted = sorted(eb, key=lambda b: Counter(ec.values())[ec[b]])

    def search(i, mapping, used):
        if i == len(eb_sorted):
            return {tuple(rename(x, mapping) for x in q) for q in expected} == actual
        b = eb_sorted[i]
        for cand in ab:
            if cand not in used and ac[cand] == ec[b]:
                mapping[b] = cand
                used.add(cand)
                if search(i + 1, mapping, used):
                    return True
                del mapping[b]
                used.discard(cand)
        return False

    return search(0, {}, set())


def format_quads(quads: List[tuple], limit=30) -> str:
    lines = sorted(" ".join(n3(x) for x in q if x is not None) + " ." for q in quads)
    if len(lines) > limit:
        lines = lines[:limit] + [f"... ({len(quads)} quads)"]
    return "\n".join("  " + l for l in lines)


# ---------------------------------------------------------------------------
# Manifests
# ---------------------------------------------------------------------------


def read_file(iri) -> str:
    # Bytes are decoded directly: reading as text would turn carriage returns in the
    # test files into line feeds.
    return iri_to_path(iri).read_bytes().decode("utf-8")


def iri_to_path(iri) -> Path:
    s = iri[1] if isinstance(iri, tuple) else iri
    for suite in SUITES:
        if s.startswith(suite.base):
            return suite.directory / s[len(suite.base):]
    raise AssertionError(f"IRI {s} is not inside a test suite")


class Manifest:
    def __init__(self, suite: Suite, rel_path: str):
        self.iri = ("uri", suite.base + rel_path)
        m = Model()
        m.reads(read_file(self.iri), format="turtle",
                base_iri=self.iri[1])
        self.index = defaultdict(lambda: defaultdict(list))
        for row in select(m, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"):
            self.index[row["s"]][row["p"]].append(row["o"])
        # The manifest node is not always the document IRI, e.g. trs:manifest.
        self.nodes = [s for s, ps in self.index.items() if MF.Manifest in ps.get(RDF.type, [])]

    def objects(self, s, p) -> list:
        return self.index.get(s, {}).get(p, [])

    def value(self, s, p):
        objs = self.objects(s, p)
        return objs[0] if objs else None

    def collection(self, node) -> list:
        items = []
        while node is not None and node != RDF.nil:
            items.append(self.value(node, RDF.first))
            node = self.value(node, RDF.rest)
        return items


def collect_tests(suite: Suite, rel_path="manifest.ttl"):
    g = Manifest(suite, rel_path)
    tests = []
    for node in g.nodes:
        for include in g.objects(node, MF.include):
            for sub in g.collection(include):
                sub_rel = sub[1][len(suite.base):]
                # Sub suites that are not vendored (e.g. the RDF 1.1 entailment tests).
                if not (suite.directory / sub_rel).exists():
                    continue
                tests.extend(collect_tests(suite, sub_rel))
        directory = rel_path.rsplit("/", 1)[0]
        for entries in g.objects(node, MF.entries):
            for entry in g.collection(entries):
                tests.append((directory, g, entry))
    return tests


def make_test_id(directory: str, entry) -> str:
    name = entry[1].rsplit("#", 1)[-1].rsplit("/", 1)[-1]
    return f"{directory}/{name}"


def load_known_failures(suite: Suite) -> Dict[str, str]:
    known = {}
    if suite.known_failures.exists():
        for line in suite.known_failures.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            tid, _, reason = line.partition("#")
            known[tid.strip()] = reason.strip() or "known failure"
    return known


ROUND_TRIP_FORMATS = ["ntriples", "nquads", "turtle", "trig", "rdf/xml", "json-ld", "hdt"]
TRIPLE_FORMATS = {"ntriples", "turtle", "rdf/xml", "hdt"}
BINARY_FORMATS = {"hdt"}


def dataset_graphs(quads: List[tuple]) -> List[str]:
    """The graphs of the quads (DEFAULT_GRAPH for the default graph), to write them as a dataset."""
    named = sorted({q[3][1] for q in quads if q[3] is not None and q[3][0] == "uri"})
    return ([DEFAULT_GRAPH] if any(q[3] is None for q in quads) else []) + named


def build_round_trip_params(suite: Suite):
    known = load_known_failures(suite)
    params = []
    for directory, g, entry in collect_tests(suite):
        types = set(g.objects(entry, RDF.type))
        if not types & set(EVAL_TESTS):
            continue
        base_tid = make_test_id(directory, entry)
        for fmt in ROUND_TRIP_FORMATS:
            tid = f"{base_tid}@{fmt}"
            marks = []
            if tid in known:
                marks.append(pytest.mark.xfail(reason=known[tid], strict=True))
            elif base_tid in known:
                # A dataset that is not read correctly is not written correctly either.
                marks.append(pytest.mark.xfail(reason=f"reading: {known[base_tid]}", strict=True))
            params.append(pytest.param(g, entry, fmt, id=tid, marks=marks))
    return params


def run_round_trip(g: Manifest, entry, out_format: str):
    in_format = [EVAL_TESTS[t] for t in g.objects(entry, RDF.type) if t in EVAL_TESTS][0]
    action = g.value(entry, MF.action)
    result = g.value(entry, MF.result)
    expected = parse_nquads(read_file(result))
    if out_format in TRIPLE_FORMATS and any(q[3] is not None for q in expected):
        pytest.skip(f"{out_format} cannot hold named graphs")
    m = read_action(in_format, action)
    # JSON-LD writes the graphs it is given.
    graph = {"graph": dataset_graphs(expected)} if out_format == "json-ld" else {}
    written = m.write_bytes(format=out_format, **graph)
    # Writing to a file-like object gives the same document, as does writes() for text formats
    buf = io.BytesIO()
    m.write(buf, format=out_format, **graph)
    assert buf.getvalue() == written
    if out_format not in BINARY_FORMATS:
        assert m.writes(format=out_format, **graph).encode() == written
    # Read back from bytes, from a file-like object and (for text formats) from a string
    readers = [
        lambda m2: m2.read(written, format=out_format),
        lambda m2: m2.read(io.BytesIO(written), format=out_format),
    ]
    if out_format not in BINARY_FORMATS:
        readers.append(lambda m2: m2.reads(written.decode(), format=out_format))
    shown = written if out_format in BINARY_FORMATS else written.decode()
    for read in readers:
        m2 = Model()
        read(m2)
        actual = dump_dataset(m2)
        assert isomorphic(expected, actual), (
            f"Datasets differ after writing {out_format}.\nExpected:\n{format_quads(expected)}\n"
            f"Actual:\n{format_quads(actual)}\nWritten:\n{shown}"
        )


def build_params(suite: Suite):
    known = load_known_failures(suite)
    params = []
    for directory, g, entry in collect_tests(suite):
        types = set(g.objects(entry, RDF.type))
        tid = make_test_id(directory, entry)
        marks = []
        if not types & SUPPORTED_TYPES:
            names = sorted(t[1] for t in types)
            marks.append(pytest.mark.skip(reason=f"unsupported test type {names}"))
        elif tid in known:
            marks.append(pytest.mark.xfail(reason=known[tid], strict=True))
        params.append(pytest.param(g, entry, id=tid, marks=marks))
    return params


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def read_action(fmt: str, action) -> Model:
    m = Model()
    m.reads(read_file(action), format=fmt, base_iri=action[1])
    return m


def check_syntax(g: Manifest, entry, fmt: str, positive: bool):
    action = g.value(entry, MF.action)
    if positive:
        read_action(fmt, action)
    else:
        with pytest.raises(Exception):
            read_action(fmt, action)


def check_eval(g: Manifest, entry, fmt: str):
    action = g.value(entry, MF.action)
    result = g.value(entry, MF.result)
    actual = dump_dataset(read_action(fmt, action))
    expected = parse_nquads(read_file(result))
    assert isomorphic(expected, actual), (
        f"Datasets differ.\nExpected:\n{format_quads(expected)}\nActual:\n{format_quads(actual)}"
    )


def check_c14n(g: Manifest, entry, fmt: str):
    action = g.value(entry, MF.action)
    result = g.value(entry, MF.result)
    m = read_action(fmt, action)
    actual = m.writes(format=fmt)
    expected = read_file(result)
    # The order of the lines of canonical N-Triples / N-Quads is not specified.
    assert sorted(actual.splitlines()) == sorted(expected.splitlines()), (
        f"Canonical form differs.\nExpected:\n{expected}\nActual:\n{actual}"
    )


def run_test(manifest: Manifest, entry):
    types = set(manifest.objects(entry, RDF.type))
    for t in types:
        if t in SYNTAX_TESTS:
            fmt, positive = SYNTAX_TESTS[t]
            return check_syntax(manifest, entry, fmt, positive)
        if t in EVAL_TESTS:
            return check_eval(manifest, entry, EVAL_TESTS[t])
        if t in C14N_TESTS:
            return check_c14n(manifest, entry, C14N_TESTS[t])

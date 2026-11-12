"""
W3C SPARQL 1.1 compliance tests.

The test data in ``testdata/w3c_sparql11`` is an unmodified copy of the SPARQL 1.1
query, update and result-format test suites from https://github.com/w3c/rdf-tests
(``sparql/sparql11``), see ``testdata/w3c_sparql11/LICENSE.md``.

The manifests are read with maplib and a pytest case is generated for each of:

* ``mf:QueryEvaluationTest``  - load the dataset, run the query, compare with the
  expected result (SPARQL XML/JSON/CSV/TSV results or an RDF graph).
* ``mf:UpdateEvaluationTest`` - load the dataset, run the update, compare every graph
  of the resulting dataset with the expected dataset.
* ``mf:PositiveSyntaxTest11`` / ``mf:PositiveUpdateSyntaxTest11`` - must not raise.
* ``mf:NegativeSyntaxTest11`` / ``mf:NegativeUpdateSyntaxTest11`` - must raise.
* ``mf:CSVResultFormatTest`` - run the query and compare with the expected CSV.

Tests that maplib currently does not pass are listed in
``w3c_sparql11_known_failures.txt`` and are marked as strict xfails, so a test that
starts passing will be reported (XPASS) and should then be removed from that list.
Entries tagged ``[flaky]`` fail nondeterministically and are non-strict xfails.
Run ``pytest test_w3c_sparql11.py --runxfail`` to see the actual failures.

Only maplib and the standard library are used: manifests and expected graphs are read
with maplib, expected SPARQL results (XML, JSON, TSV, CSV) are parsed here. RDF terms
are represented as tuples: ("uri", iri), ("bnode", label) and
("literal", lexical form, datatype or None, language or None).
"""

import csv
import json
import re
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pytest

from maplib import Model

TESTS_DIR = Path(__file__).parent
SUITE_DIR = TESTS_DIR / "testdata" / "w3c_sparql11"
KNOWN_FAILURES_FILE = TESTS_DIR / "w3c_sparql11_known_failures.txt"

# The canonical base IRI of the test suite. Relative IRIs in manifests, data, queries
# and results are resolved against this base (with the sub directory appended).
BASE = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/"

class Namespace(str):
    def __getattr__(self, name):
        return ("uri", self + name)


MF = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
QT = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-query#")
UT = Namespace("http://www.w3.org/2009/sparql/tests/test-update#")
RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
XSD = "http://www.w3.org/2001/XMLSchema#"

TOP_MANIFESTS = [
    "manifest-sparql11-query.ttl",
    "manifest-sparql11-update.ttl",
    "manifest-sparql11-results.ttl",
]

SUPPORTED_TYPES = {
    MF.QueryEvaluationTest,
    MF.CSVResultFormatTest,
    MF.UpdateEvaluationTest,
    MF.PositiveSyntaxTest11,
    MF.NegativeSyntaxTest11,
    MF.PositiveUpdateSyntaxTest11,
    MF.NegativeUpdateSyntaxTest11,
}

NUMERIC_DATATYPES = {
    XSD + t
    for t in (
        "integer decimal double float int long short byte nonNegativeInteger "
        "nonPositiveInteger negativeInteger positiveInteger unsignedInt "
        "unsignedLong unsignedShort unsignedByte"
    ).split()
}
FLOATING_DATATYPES = {XSD + "double", XSD + "float"}


# ---------------------------------------------------------------------------
# RDF terms
# ---------------------------------------------------------------------------


def literal(lex: str, datatype: Optional[str] = None, lang: Optional[str] = None):
    if lang:
        return ("literal", lex, None, lang.lower())
    if datatype == XSD + "string":
        datatype = None
    return ("literal", lex, datatype, None)


def is_bnode(t) -> bool:
    return t is not None and t[0] == "bnode"


def n3(t) -> str:
    if t is None:
        return "UNDEF"
    if t[0] == "uri":
        return f"<{t[1]}>"
    if t[0] == "bnode":
        return f"_:{t[1]}"
    _, lex, dt, lang = t
    s = json.dumps(lex, ensure_ascii=False)
    if lang:
        return f"{s}@{lang}"
    if dt:
        return f"{s}^^<{dt}>"
    return s


_ESCAPES = {
    "t": "\t",
    "b": "\b",
    "n": "\n",
    "r": "\r",
    "f": "\f",
    '"': '"',
    "'": "'",
    "\\": "\\",
}
_ESCAPE_RE = re.compile(r"\\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8}|.)")


def unescape(s: str) -> str:
    def repl(m):
        e = m.group(1)
        if e[0] in "uU":
            return chr(int(e[1:], 16))
        return _ESCAPES.get(e, e)

    return _ESCAPE_RE.sub(repl, s)


# One N-Triples / Turtle-like term, as used in N-Triples and SPARQL TSV results.
_TERM = (
    r'<(?P<iri>[^>]*)>'
    r'|_:(?P<bnode>\S+)'
    r'|"(?P<lex>(?:[^"\\]|\\.)*)"(?:@(?P<lang>[A-Za-z0-9-]+)|\^\^<(?P<dt>[^>]*)>)?'
)
_TERM_RE = re.compile(_TERM)
# Named groups cannot be repeated, so the triple pattern uses an unnamed copy.
_UNNAMED_TERM = re.sub(r"\?P<\w+>", "?:", _TERM)
_TRIPLE_RE = re.compile(
    rf"^\s*({_UNNAMED_TERM})\s+({_UNNAMED_TERM})\s+({_UNNAMED_TERM})\s*\.\s*$"
)


def parse_term(s: str):
    m = _TERM_RE.fullmatch(s)
    if m is None:
        raise ValueError(f"Cannot parse RDF term {s!r}")
    if m.group("iri") is not None:
        return ("uri", unescape(m.group("iri")))
    if m.group("bnode") is not None:
        return ("bnode", m.group("bnode"))
    return literal(unescape(m.group("lex")), m.group("dt"), m.group("lang"))


def parse_ntriples(nt: str) -> List[tuple]:
    triples = []
    for line in nt.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        m = _TRIPLE_RE.match(line)
        assert m is not None, f"Cannot parse N-Triples line {line!r}"
        triples.append(tuple(parse_term(m.group(i)) for i in (1, 2, 3)))
    return triples


# ---------------------------------------------------------------------------
# Manifest handling
# ---------------------------------------------------------------------------


def iri_to_path(iri) -> Path:
    s = iri[1] if isinstance(iri, tuple) else iri
    assert s.startswith(BASE), f"IRI {s} is not inside the test suite"
    return SUITE_DIR / s[len(BASE) :]


def read_text(iri) -> str:
    return iri_to_path(iri).read_text(encoding="utf-8")


class Manifest:
    """The triples of a manifest, indexed by subject and predicate."""

    def __init__(self, rel_path: str):
        self.iri = ("uri", BASE + rel_path)
        m = Model()
        load_into_model(m, self.iri)
        res = run_select(m, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
        self.index = defaultdict(lambda: defaultdict(list))
        for row in res["rows"]:
            self.index[row["s"]][row["p"]].append(row["o"])

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


def collect_tests():
    tests = []
    for top in TOP_MANIFESTS:
        g = Manifest(top)
        for include in g.objects(g.iri, MF.include):
            for sub in g.collection(include):
                rel = sub[1][len(BASE) :]
                if not (SUITE_DIR / rel).exists():
                    continue
                sg = Manifest(rel)
                directory = rel.rsplit("/", 1)[0]
                for entries in sg.objects(sg.iri, MF.entries):
                    for entry in sg.collection(entries):
                        tests.append((directory, sg, entry))
    return tests


def make_test_id(directory: str, entry) -> str:
    name = entry[1].rsplit("#", 1)[-1].rsplit("/", 1)[-1]
    return f"{directory}/{name}"


def load_known_failures() -> Dict[str, str]:
    known = {}
    if KNOWN_FAILURES_FILE.exists():
        for line in KNOWN_FAILURES_FILE.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            tid, _, reason = line.partition("#")
            known[tid.strip()] = reason.strip() or "known failure"
    return known


def build_params():
    known = load_known_failures()
    params = []
    for directory, g, entry in collect_tests():
        types = set(g.objects(entry, RDF.type))
        tid = make_test_id(directory, entry)
        marks = []
        supported = types & SUPPORTED_TYPES
        if not supported:
            names = sorted(t[1] for t in types)
            marks.append(pytest.mark.skip(reason=f"unsupported test type {names}"))
        elif tid in known:
            # Nondeterministic failures are tagged [flaky] and are not strict.
            strict = "[flaky]" not in known[tid]
            marks.append(pytest.mark.xfail(reason=known[tid], strict=strict))
        params.append(pytest.param(g, entry, id=tid, marks=marks))
    return params


# ---------------------------------------------------------------------------
# Dataset loading
# ---------------------------------------------------------------------------


def rdf_format(path: Path) -> str:
    return {
        ".ttl": "turtle",
        ".nt": "ntriples",
        ".rdf": "rdf/xml",
        ".trig": "trig",
    }[path.suffix]


def load_into_model(m: Model, iri, graph: Optional[str] = None):
    path = iri_to_path(iri)
    m.reads(
        path.read_text(encoding="utf-8"),
        format=rdf_format(path),
        base_iri=iri[1] if isinstance(iri, tuple) else iri,
        graph=graph,
    )


def load_graph(iri) -> List[tuple]:
    """Reads an expected graph with maplib and returns its triples."""
    m = Model()
    load_into_model(m, iri)
    return dump_graph(m, None)


def dump_graph(m: Model, graph: Optional[str]) -> List[tuple]:
    return parse_ntriples(m.writes(format="ntriples", graph=graph))


def with_base(query: str, iri) -> str:
    # The base IRI of a query or update request is the location of the file.
    return f"BASE <{iri[1]}>\n{query}"


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------


def numeric_value(t):
    """The numeric value of a numeric literal, or None."""
    if t is None or t[0] != "literal" or t[2] not in NUMERIC_DATATYPES:
        return None
    try:
        if t[2] in FLOATING_DATATYPES:
            return float(t[1])
        return Decimal(t[1]).normalize()
    except (ValueError, InvalidOperation):
        return None


def terms_equal(a, b) -> bool:
    if a == b:
        return True
    # SPARQL does not require canonical lexical forms of numerics.
    if a[0] == b[0] == "literal" and a[2] == b[2]:
        va = numeric_value(a)
        return va is not None and va == numeric_value(b)
    return False


def term_key(t):
    """A hashable key that is equal for terms_equal terms (blank nodes collapsed)."""
    if t is None:
        return None
    if is_bnode(t):
        return ("bnode",)
    v = numeric_value(t)
    if v is not None:
        return ("num", t[2], v)
    return t


def match_rows(expected: List[tuple], actual: List[tuple], ordered: bool) -> bool:
    """Match rows of terms, allowing a consistent bijection between blank nodes."""
    if len(expected) != len(actual):
        return False
    if not ordered and Counter(tuple(map(term_key, r)) for r in expected) != Counter(
        tuple(map(term_key, r)) for r in actual
    ):
        return False

    def unify(er, ar, mapping, rev):
        """Extends the bijection to make the rows equal. Returns the newly mapped
        blank nodes, or None (leaving the mapping unchanged) if impossible."""
        new = []
        for e, a in zip(er, ar):
            ok = True
            if is_bnode(e) or is_bnode(a):
                if not (is_bnode(e) and is_bnode(a)):
                    ok = False
                elif e in mapping:
                    ok = mapping[e] == a
                elif a in rev:
                    ok = False
                else:
                    mapping[e] = a
                    rev[a] = e
                    new.append(e)
            elif e is None or a is None:
                ok = e is a
            else:
                ok = terms_equal(e, a)
            if not ok:
                undo(new, mapping, rev)
                return None
        return new

    def undo(new, mapping, rev):
        for e in new:
            del rev[mapping.pop(e)]

    if ordered:
        mapping, rev = {}, {}
        return all(
            unify(er, ar, mapping, rev) is not None for er, ar in zip(expected, actual)
        )

    if not any(is_bnode(t) for r in expected for t in r):
        return True  # The Counter comparison above was sufficient.

    # Backtracking search for a blank node bijection, rows with few blank nodes first.
    expected = sorted(expected, key=lambda r: sum(is_bnode(t) for t in r))
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


def result_rows(res: dict, variables: List[str]) -> List[tuple]:
    return [tuple(row.get(v) for v in variables) for row in res["rows"]]


def format_rows(variables, rows, limit=30):
    lines = ["  " + " | ".join(variables)]
    for r in rows[:limit]:
        lines.append("  " + " | ".join(n3(t) for t in r))
    if len(rows) > limit:
        lines.append(f"  ... ({len(rows)} rows)")
    return "\n".join(lines)


def strip_comments(query: str) -> str:
    out = []
    for line in query.splitlines():
        in_iri = in_str = False
        for i, c in enumerate(line):
            if c == "<" and not in_str:
                in_iri = True
            elif c == ">" and in_iri:
                in_iri = False
            elif c in "\"'" and not in_iri:
                in_str = not in_str
            elif c == "#" and not in_iri and not in_str:
                line = line[:i]
                break
        out.append(line)
    return "\n".join(out)


def query_form(query: str) -> str:
    q = strip_comments(query).upper()
    positions = {
        form: q.find(form)
        for form in ("SELECT", "CONSTRUCT", "ASK", "DESCRIBE")
        if q.find(form) >= 0
    }
    return min(positions, key=positions.get) if positions else "SELECT"


def is_ordered(query: str) -> bool:
    return "ORDER BY" in " ".join(strip_comments(query).upper().split())


# Parsed SPARQL results are dicts: {"vars": [...], "boolean": bool or None,
# "rows": [{variable: term}]} with unbound variables left out of the rows.


def json_term(b: dict):
    if b["type"] == "uri":
        return ("uri", b["value"])
    if b["type"] == "bnode":
        return ("bnode", b["value"])
    return literal(b["value"], b.get("datatype"), b.get("xml:lang"))


def parse_json_results(s: str) -> dict:
    j = json.loads(s)
    if "boolean" in j:
        return {"vars": [], "boolean": j["boolean"], "rows": []}
    return {
        "vars": j["head"].get("vars", []),
        "boolean": None,
        "rows": [
            {v: json_term(b) for v, b in row.items()} for row in j["results"]["bindings"]
        ],
    }


SRX = "{http://www.w3.org/2005/sparql-results#}"
XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"


def parse_xml_results(path: Path) -> dict:
    root = ET.parse(path).getroot()
    variables = [v.get("name") for v in root.iter(SRX + "variable")]
    boolean = root.find(SRX + "boolean")
    if boolean is not None:
        return {"vars": variables, "boolean": boolean.text.strip() == "true", "rows": []}
    rows = []
    for result in root.iter(SRX + "result"):
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
                    text, el.get("datatype"), el.get(XML_LANG)
                )
        rows.append(row)
    return {"vars": variables, "boolean": None, "rows": rows}


def tsv_term(s: str):
    if s == "":
        return None
    if re.fullmatch(r"[+-]?\d+", s):
        return literal(s, XSD + "integer")
    if re.fullmatch(r"[+-]?\d*\.\d+", s):
        return literal(s, XSD + "decimal")
    if re.fullmatch(r"[+-]?(\d+\.?\d*|\.\d+)[eE][+-]?\d+", s):
        return literal(s, XSD + "double")
    if s in ("true", "false"):
        return literal(s, XSD + "boolean")
    return parse_term(s)


def parse_tsv_results(path: Path) -> dict:
    lines = path.read_text(encoding="utf-8").splitlines()
    variables = [v.lstrip("?$") for v in lines[0].split("\t")]
    rows = []
    for line in lines[1:]:
        row = {}
        for v, cell in zip(variables, line.split("\t")):
            t = tsv_term(cell)
            if t is not None:
                row[v] = t
        rows.append(row)
    return {"vars": variables, "boolean": None, "rows": rows}


def parse_csv_results(path: Path) -> dict:
    """CSV results lose all type information, terms are kept as strings."""
    with open(path, newline="", encoding="utf-8") as f:
        lines = list(csv.reader(f))
    variables = lines[0]
    rows = [{v: c for v, c in zip(variables, r) if c != ""} for r in lines[1:]]
    return {"vars": variables, "boolean": None, "rows": rows}


def parse_expected_result(iri) -> dict:
    path = iri_to_path(iri)
    if path.suffix == ".srx":
        return parse_xml_results(path)
    if path.suffix == ".srj":
        return parse_json_results(path.read_text(encoding="utf-8"))
    if path.suffix == ".tsv":
        return parse_tsv_results(path)
    if path.suffix == ".csv":
        return parse_csv_results(path)
    raise ValueError(f"Unknown result format {path}")


def graph_isomorphic(expected: List[tuple], actual: List[tuple]) -> bool:
    # Graphs are sets of triples.
    return match_rows(list(set(expected)), list(set(actual)), ordered=False)


def format_graph(triples: List[tuple], limit=30) -> str:
    lines = sorted(" ".join(n3(t) for t in tr) + " ." for tr in triples)
    if len(lines) > limit:
        lines = lines[:limit] + [f"... ({len(triples)} triples)"]
    return "\n".join("  " + l for l in lines)


# ---------------------------------------------------------------------------
# Running maplib
# ---------------------------------------------------------------------------


def model_with_dataset(g: Manifest, action) -> Model:
    m = Model()
    for data in g.objects(action, QT.data):
        load_into_model(m, data)
    for named in g.objects(action, QT.graphData):
        load_into_model(m, named, graph=named[1])
    return m


def run_select(m: Model, query: str) -> dict:
    return parse_json_results(m.query(query, return_format="json"))


def run_construct(m: Model, query: str) -> List[tuple]:
    ttl = m.query(query, return_format="turtle")
    out = Model()
    if ttl:
        out.reads(ttl, format="turtle")
    return dump_graph(out, None)


def check_query_evaluation(g: Manifest, entry):
    action = g.value(entry, MF.action)
    query_iri = g.value(action, QT.query)
    result_iri = g.value(entry, MF.result)
    query = with_base(read_text(query_iri), query_iri)
    m = model_with_dataset(g, action)
    form = query_form(read_text(query_iri))
    result_path = iri_to_path(result_iri)

    if result_path.suffix in (".ttl", ".nt", ".rdf"):
        expected = load_graph(result_iri)
        actual = run_construct(m, query)
        assert graph_isomorphic(expected, actual), (
            f"Graphs differ.\nExpected:\n{format_graph(expected)}\n"
            f"Actual:\n{format_graph(actual)}"
        )
        return

    expected = parse_expected_result(result_iri)
    actual = run_select(m, query)

    if form == "ASK" or expected["boolean"] is not None:
        assert actual["boolean"] is not None, "Expected a boolean result"
        assert actual["boolean"] == expected["boolean"]
        return

    exp_vars = expected["vars"]
    act_vars = actual["vars"]
    assert set(exp_vars) == set(act_vars), (
        f"Variables differ: expected {exp_vars}, got {act_vars}"
    )
    if result_path.suffix == ".csv":
        # CSV loses all type information, compare the textual forms.
        exp_rows = [tuple(csv_cell(r.get(v)) for v in exp_vars) for r in expected["rows"]]
        act_rows = [tuple(csv_text(r.get(v)) for v in exp_vars) for r in actual["rows"]]
        assert sorted(exp_rows) == sorted(act_rows), (
            f"Results differ.\nExpected: {exp_rows}\nActual: {act_rows}"
        )
        return

    exp_rows = result_rows(expected, exp_vars)
    act_rows = result_rows(actual, exp_vars)
    ordered = is_ordered(read_text(query_iri))
    assert match_rows(exp_rows, act_rows, ordered=ordered), (
        f"Results differ{' (ordered)' if ordered else ''}.\n"
        f"Expected:\n{format_rows(exp_vars, exp_rows)}\n"
        f"Actual:\n{format_rows(exp_vars, act_rows)}"
    )


def csv_text(t) -> str:
    """The CSV serialization of a term."""
    if t is None:
        return ""
    if is_bnode(t):
        return "_:b"  # labels are arbitrary
    return t[1]


def csv_cell(c: Optional[str]) -> str:
    """A cell of an expected CSV result, with blank node labels made comparable."""
    if c is None:
        return ""
    return "_:b" if c.startswith("_:") else c


def check_csv_result_format(g: Manifest, entry):
    action = g.value(entry, MF.action)
    query_iri = g.value(action, QT.query)
    result_iri = g.value(entry, MF.result)
    m = model_with_dataset(g, action)
    actual = run_select(m, with_base(read_text(query_iri), query_iri))
    with open(iri_to_path(result_iri), newline="", encoding="utf-8") as f:
        exp_rows = [
            ["_:b" if c.startswith("_:") else c for c in r] for r in csv.reader(f)
        ]
    # The tests use SELECT *, where the column order is not defined by the spec.
    variables = exp_rows[0]
    assert set(variables) == set(actual["vars"]), (
        f"Header differs: {variables} vs {actual['vars']}"
    )
    act_rows = [variables] + [
        [csv_text(r.get(v)) for v in variables] for r in actual["rows"]
    ]
    assert sorted(exp_rows[1:]) == sorted(act_rows[1:]), (
        f"CSV differs.\nExpected: {exp_rows}\nActual: {act_rows}"
    )


def update_dataset(g: Manifest, node) -> Tuple[List, Dict[str, object]]:
    """Returns default graph files and {graph name: file} of an ut:action/ut:result."""
    default = list(g.objects(node, UT.data))
    named = {}
    for gd in g.objects(node, UT.graphData):
        named[g.value(gd, RDFS.label)[1]] = g.value(gd, UT.graph)
    return default, named


def check_update_evaluation(g: Manifest, entry):
    action = g.value(entry, MF.action)
    request_iri = g.value(action, UT.request)
    default, named = update_dataset(g, action)
    m = Model()
    for d in default:
        load_into_model(m, d)
    for name, file in named.items():
        load_into_model(m, file, graph=name)

    m.update(with_base(read_text(request_iri), request_iri))

    exp_default, exp_named = update_dataset(g, g.value(entry, MF.result))
    expected = []
    for d in exp_default:
        expected += load_graph(d)
    actual = dump_graph(m, None)
    assert graph_isomorphic(expected, actual), (
        f"Default graph differs.\nExpected:\n{format_graph(expected)}\n"
        f"Actual:\n{format_graph(actual)}"
    )
    for name in set(exp_named) | set(named):
        expected = load_graph(exp_named[name]) if name in exp_named else []
        actual = dump_graph(m, name)
        assert graph_isomorphic(expected, actual), (
            f"Graph <{name}> differs.\nExpected:\n{format_graph(expected)}\n"
            f"Actual:\n{format_graph(actual)}"
        )


def check_syntax(g: Manifest, entry, update: bool, positive: bool):
    action = g.value(entry, MF.action)
    text = with_base(read_text(action), action)
    m = Model()

    def run():
        if update:
            m.update(text)
        else:
            m.query(text)

    if positive:
        run()
    else:
        with pytest.raises(Exception):
            run()


@pytest.mark.parametrize("manifest,entry", build_params())
def test_w3c_sparql11(manifest: Manifest, entry):
    types = set(manifest.objects(entry, RDF.type))
    if MF.QueryEvaluationTest in types:
        check_query_evaluation(manifest, entry)
    elif MF.CSVResultFormatTest in types:
        check_csv_result_format(manifest, entry)
    elif MF.UpdateEvaluationTest in types:
        check_update_evaluation(manifest, entry)
    elif MF.PositiveSyntaxTest11 in types:
        check_syntax(manifest, entry, update=False, positive=True)
    elif MF.NegativeSyntaxTest11 in types:
        check_syntax(manifest, entry, update=False, positive=False)
    elif MF.PositiveUpdateSyntaxTest11 in types:
        check_syntax(manifest, entry, update=True, positive=True)
    elif MF.NegativeUpdateSyntaxTest11 in types:
        check_syntax(manifest, entry, update=True, positive=False)

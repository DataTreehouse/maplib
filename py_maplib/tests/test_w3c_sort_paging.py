"""The W3C SPARQL `sort` tests, evaluated as a query and through keyset pagination.

The test data in ``testdata/w3c_sparql10_sort`` is an unmodified copy of ``sparql/sparql10/sort``
of https://github.com/w3c/rdf-tests (commit 7b5afb91), see its ``LICENSE.md``. These are the
reference tests of ORDER BY: ascending and descending order of plain and typed literals,
unbound values, mixed blank nodes, IRIs and literals, numbers of different datatypes, and
ordering by expressions and by variables that are not projected.

Each test is run as one query and as pages of every size from 1 to the number of solutions
(``Model.query_pages``), and the solutions must be in the order of the expected results
(``rs:index``), so paging must keep the reference order. Blank nodes match any blank node, as
long as the matching is one to one.
"""

from pathlib import Path
from typing import Dict, List, Optional

import json

import pytest
import rdflib

from maplib import Model

from .disk import disk_params, storage_kwargs

SUITE_DIR = Path(__file__).parent / "testdata" / "w3c_sparql10_sort"
BASE = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/sort/"
MF = rdflib.Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
QT = rdflib.Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-query#")
RS = rdflib.Namespace("http://www.w3.org/2001/sw/DataAccess/tests/result-set#")

rdflib.NORMALIZE_LITERALS = False


def manifest_tests():
    g = rdflib.Graph()
    g.parse(SUITE_DIR / "manifest.ttl", format="turtle", publicID=BASE + "manifest.ttl")
    tests = []
    (m,) = g.subjects(rdflib.RDF.type, MF.Manifest)
    for entry in rdflib.collection.Collection(g, g.value(m, MF.entries)):
        action = g.value(entry, MF.action)
        tests.append(
            (
                str(g.value(entry, MF.name)),
                local(g.value(action, QT.query)),
                local(g.value(action, QT.data)),
                local(g.value(entry, MF.result)),
            )
        )
    return tests


def local(iri) -> Path:
    s = str(iri)
    assert s.startswith(BASE), s
    return SUITE_DIR / s[len(BASE) :]


def term(t) -> Optional[tuple]:
    """A comparable form of an rdflib term or a SPARQL JSON binding."""
    if t is None:
        return None
    if isinstance(t, dict):
        if t["type"] == "uri":
            return ("uri", t["value"])
        if t["type"] == "bnode":
            return ("bnode", t["value"])
        dt = t.get("datatype")
        lang = t.get("xml:lang")
        if dt is None and lang is None:
            dt = "http://www.w3.org/2001/XMLSchema#string"
        return ("literal", t["value"], dt, lang)
    if isinstance(t, rdflib.URIRef):
        return ("uri", str(t))
    if isinstance(t, rdflib.BNode):
        return ("bnode", str(t))
    dt = str(t.datatype) if t.datatype else None
    if dt is None and t.language is None:
        dt = "http://www.w3.org/2001/XMLSchema#string"
    return ("literal", str(t), dt, t.language)


def expected_rows(path: Path):
    g = rdflib.Graph()
    fmt = "xml" if path.suffix == ".rdf" else "turtle"
    g.parse(path, format=fmt, publicID=BASE + path.name)
    (rs,) = g.subjects(rdflib.RDF.type, RS.ResultSet)
    variables = sorted(str(v) for v in g.objects(rs, RS.resultVariable))
    rows = []
    for sol in g.objects(rs, RS.solution):
        index = g.value(sol, RS["index"])  # RS.index is str.index
        row = {}
        for b in g.objects(sol, RS.binding):
            row[str(g.value(b, RS.variable))] = term(g.value(b, RS.value))
        rows.append((int(index) if index is not None else None, row))
    assert all(i is not None for i, _ in rows), f"{path} has solutions without rs:index"
    rows.sort(key=lambda r: r[0])
    return variables, [tuple(r.get(v) for v in variables) for _, r in rows]


def json_rows(s: str, variables: List[str]):
    res = json.loads(s)
    return [tuple(term(b.get(v)) for v in variables) for b in res["results"]["bindings"]]


def same_order(expected, actual) -> bool:
    """Equal row by row, blank nodes matched one to one."""
    if len(expected) != len(actual):
        return False
    forward: Dict = {}
    backward: Dict = {}
    for er, ar in zip(expected, actual):
        for e, a in zip(er, ar):
            if e is not None and e[0] == "bnode":
                if a is None or a[0] != "bnode":
                    return False
                if forward.setdefault(e, a) != a or backward.setdefault(a, e) != e:
                    return False
            elif e != a:
                return False
    return True


def model_for(data: Path, storage: str) -> Model:
    m = Model(**storage_kwargs(storage))
    m.reads(data.read_text(), format="turtle", base_iri=BASE + data.name)
    return m


TESTS = manifest_tests()


@pytest.fixture(params=disk_params())
def storage(request):
    return request.param


@pytest.mark.parametrize("name,query,data,result", TESTS, ids=[t[0] for t in TESTS])
def test_w3c_sort(storage, name, query, data, result):
    m = model_for(data, storage)
    q = f"BASE <{BASE}{query.name}>\n" + query.read_text()
    variables, expected = expected_rows(result)
    whole = json_rows(m.query(q, return_format="json"), variables)
    assert same_order(expected, whole), f"query:\n{whole}\nexpected:\n{expected}"
    for page_size in range(1, len(expected) + 2):
        pages = list(m.query_pages(q, page_size=page_size, return_format="json"))
        paged = [r for p in pages for r in json_rows(p, variables)]
        assert same_order(expected, paged), f"page size {page_size}:\n{paged}\nexpected:\n{expected}"
        assert len(pages) == max(1, -(-len(expected) // page_size))


def test_suite_is_complete():
    # The manifest lists 14 tests; none may be dropped silently.
    assert len(TESTS) == 14

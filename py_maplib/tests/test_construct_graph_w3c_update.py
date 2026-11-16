"""
CONSTRUCT with GRAPH blocks in the template, checked against the W3C SPARQL 1.1 Update tests.

The template of an INSERT or DELETE is a quad template with the same semantics as a CONSTRUCT
template with GRAPH blocks (https://github.com/w3c/sparql-dev/issues/31, Apache Jena). For each
W3C update evaluation test whose request is a single DELETE/INSERT ... WHERE (or DELETE WHERE)
operation, the deleted and inserted quads are computed with CONSTRUCT queries on the input dataset,
C(del) and C(ins), and

    expected dataset = (input dataset - C(del)) + C(ins)

is checked against the expected dataset of the W3C test. When the operation only inserts, the
CONSTRUCT is also inserted with Model.insert, which must give the expected dataset.

The tests and their data are the unmodified W3C suite in testdata/w3c_sparql11 (see
test_w3c_sparql11.py). Only the requests are rewritten:

* WITH <g>: the default graph of the WHERE clause (source_graph) and of the templates is <g>.
* USING <g> / USING NAMED <g>: FROM <g> / FROM NAMED <g> in the CONSTRUCT queries.
* DELETE WHERE { Q }: the CONSTRUCT WHERE { Q } short form.
"""

from typing import Dict, List, Optional, Tuple

import pytest
import rdflib

from maplib import Model

from .test_w3c_sparql11 import (
    MF,
    RDF,
    UT,
    Manifest,
    collect_tests,
    dump_graph,
    format_graph,
    graph_isomorphic,
    is_bnode,
    load_graph,
    load_into_model,
    load_known_failures,
    make_test_id,
    note_data_literals,
    rdflib_term,
    read_text,
    strip_comments,
    update_dataset,
    with_base,
)

# Tests whose rewritten form is not expected to give the W3C result, with the reason.
KNOWN_FAILURES = {
    # maplib rejects update templates with variables that are not bound by the WHERE clause, and
    # so the CONSTRUCT with that template (as in test_w3c_sparql11).
    "delete/dawg-delete-07": "Variable ?o not found",
}


def tokens(text: str):
    """The top level tokens of an update request: words, <IRIs>, ';' and {...} groups."""
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c.isspace():
            i += 1
        elif c == "{":
            depth, j, quote = 0, i, None
            while j < n:
                d = text[j]
                if quote:
                    if d == "\\":
                        j += 1
                    elif d == quote:
                        quote = None
                elif d in "\"'":
                    quote = d
                elif d == "{":
                    depth += 1
                elif d == "}":
                    depth -= 1
                    if depth == 0:
                        break
                j += 1
            yield ("group", text[i + 1 : j])
            i = j + 1
        elif c == "<":
            j = text.index(">", i)
            yield ("iri", text[i : j + 1])
            i = j + 1
        elif c == ";":
            yield ("semi", ";")
            i += 1
        else:
            j = i
            while j < n and not text[j].isspace() and text[j] not in "{<;":
                j += 1
            yield ("word", text[i:j])
            i = j


def split_update(text: str) -> Optional[dict]:
    """The parts of a single DELETE/INSERT ... WHERE or DELETE WHERE operation, or None."""
    toks = list(tokens(strip_comments(text)))
    while toks and toks[-1][0] == "semi":
        toks.pop()
    prologue, prefixes, base = [], {}, None
    i = 0

    def kw(k):
        return i < len(toks) and toks[i][0] == "word" and toks[i][1].upper() == k

    while kw("PREFIX") or kw("BASE"):
        if kw("BASE"):
            base = toks[i + 1][1]
            prologue.append(f"BASE {base}")
            i += 2
        else:
            prefixes[toks[i + 1][1]] = toks[i + 2][1]
            prologue.append(f"PREFIX {toks[i + 1][1]} {toks[i + 2][1]}")
            i += 3
    op = {"prologue": "\n".join(prologue), "with": None, "delete": None, "insert": None,
          "using": [], "using_named": [], "where": None, "short": False}

    def iri(tok):
        kind, t = tok
        if kind == "iri":
            return t[1:-1]
        prefix, _, local = t.partition(":")
        return prefixes[prefix + ":"][1:-1] + local

    if kw("WITH"):
        op["with"] = iri(toks[i + 1])
        i += 2
    if kw("DELETE") and i + 2 < len(toks) and toks[i + 1][0] == "word" and toks[i + 1][1].upper() == "WHERE":
        if op["with"] is not None:
            return None
        op["delete"] = op["where"] = toks[i + 2][1]
        op["short"] = True
        i += 3
    else:
        if kw("DELETE") and toks[i + 1][0] == "group":
            op["delete"] = toks[i + 1][1]
            i += 2
        if kw("INSERT") and toks[i + 1][0] == "group":
            op["insert"] = toks[i + 1][1]
            i += 2
        if op["delete"] is None and op["insert"] is None:
            return None
        while kw("USING"):
            if toks[i + 1][0] == "word" and toks[i + 1][1].upper() == "NAMED":
                op["using_named"].append(iri(toks[i + 2]))
                i += 3
            else:
                op["using"].append(iri(toks[i + 1]))
                i += 2
        if not (kw("WHERE") and toks[i + 1][0] == "group"):
            return None
        op["where"] = toks[i + 1][1]
        i += 2
    if i != len(toks):
        return None
    return op


def construct(op: dict, template: str) -> str:
    if op["short"]:
        return f"{op['prologue']}\nCONSTRUCT WHERE {{ {template} }}"
    dataset = " ".join(
        [f"FROM <{g}>" for g in op["using"]] + [f"FROM NAMED <{g}>" for g in op["using_named"]]
    )
    return f"{op['prologue']}\nCONSTRUCT {{ {template} }} {dataset} WHERE {{ {op['where']} }}"


def source_graph(op: dict) -> Optional[str]:
    # USING overrides WITH for the WHERE clause.
    return op["with"] if not (op["using"] or op["using_named"]) else None


Dataset = Dict[Optional[str], List[tuple]]


def constructed_quads(m: Model, op: dict, template: str, request_iri) -> Dataset:
    """The quads constructed from the template on the dataset of m, by graph (None is default)."""
    query = with_base(construct(op, template), request_iri)
    trig = m.query(query, return_format="trig", graph=source_graph(op), include_graphs=[])
    ds = rdflib.Dataset()
    ds.parse(data=trig, format="trig")
    out: Dataset = {}
    for s, p, o, g in ds.quads((None, None, None, None)):
        name = None if g is None or g == rdflib.graph.DATASET_DEFAULT_GRAPH_ID else str(g)
        if name is None:
            name = op["with"]
        out.setdefault(name, []).append(tuple(rdflib_term(x) for x in (s, p, o)))
    return out


def model_dataset(m: Model, names) -> Dataset:
    return {name: dump_graph(m, name) for name in names}


def build_params():
    known = load_known_failures()
    params = []
    for directory, g, entry in collect_tests():
        if MF.UpdateEvaluationTest not in set(g.objects(entry, RDF.type)):
            continue
        request_iri = g.value(g.value(entry, MF.action), UT.request)
        op = split_update(read_text(request_iri))
        if op is None:
            continue
        tid = make_test_id(directory, entry)
        marks = []
        reason = KNOWN_FAILURES.get(tid) or known.get(tid)
        if reason:
            marks.append(pytest.mark.xfail(reason=reason, strict=True))
        params.append(pytest.param(g, entry, op, id=tid, marks=marks))
    return params


def input_model(g: Manifest, action) -> Tuple[Model, List[str]]:
    default, named = update_dataset(g, action)
    note_data_literals(list(default) + list(named.values()))
    m = Model()
    for d in default:
        load_into_model(m, d)
    for name, file in named.items():
        load_into_model(m, file, graph=name)
    return m, list(named)


def expected_dataset(g: Manifest, entry) -> Dataset:
    exp_default, exp_named = update_dataset(g, g.value(entry, MF.result))
    out: Dataset = {None: [t for d in exp_default for t in load_graph(d)]}
    for name, file in exp_named.items():
        out[name] = load_graph(file)
    return out


def assert_same_dataset(expected: Dataset, actual: Dataset, what: str):
    for name in set(expected) | set(actual):
        e, a = expected.get(name, []), actual.get(name, [])
        assert graph_isomorphic(e, a), (
            f"{what}: graph {name or 'default'} differs.\nExpected:\n{format_graph(e)}\n"
            f"Actual:\n{format_graph(a)}"
        )


PARAMS = build_params()


def test_w3c_update_tests_were_found():
    # The suite has single DELETE/INSERT operations with and without WITH, USING and GRAPH.
    assert len(PARAMS) >= 30
    ops = [p.values[2] for p in PARAMS]
    assert any(op["with"] for op in ops)
    assert any(op["using"] for op in ops)
    assert any(op["short"] for op in ops)
    assert any("GRAPH" in (op["insert"] or "").upper() for op in ops)
    assert any("GRAPH" in (op["delete"] or "").upper() for op in ops)


@pytest.mark.parametrize("manifest,entry,op", PARAMS)
def test_construct_graph_matches_w3c_update(manifest: Manifest, entry, op: dict):
    action = manifest.value(entry, MF.action)
    request_iri = manifest.value(action, UT.request)
    m, named = input_model(manifest, action)
    deleted = constructed_quads(m, op, op["delete"], request_iri) if op["delete"] else {}
    inserted = constructed_quads(m, op, op["insert"], request_iri) if op["insert"] else {}

    names = {None, *named, *deleted, *inserted}
    data = model_dataset(m, names)
    if any(is_bnode(t) for name in deleted for tr in data.get(name, []) for t in tr):
        pytest.skip("blank nodes of the data can not be matched between the outputs")
    actual: Dataset = {}
    for name in names:
        gone = set(deleted.get(name, []))
        triples = [t for t in data.get(name, []) if t not in gone]
        triples += [t for t in inserted.get(name, []) if t not in set(triples)]
        actual[name] = triples
    assert_same_dataset(expected_dataset(manifest, entry), actual, "input - C(del) + C(ins)")

    if op["delete"] is None:
        # Model.insert puts the default graph part in target_graph and the rest in its graphs.
        m2, _ = input_model(manifest, action)
        m2.insert(
            with_base(construct(op, op["insert"]), request_iri),
            source_graph=source_graph(op),
            target_graph=op["with"],
            include_graphs=[],
        )
        graphs = {
            r[0][1:-1]
            for r in m2.query("SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }").rows()
        }
        expected = expected_dataset(manifest, entry)
        assert_same_dataset(expected, model_dataset(m2, {None, *named, *graphs, *expected}), "Model.insert")

"""What Model.update reports it changed, checked against the W3C SPARQL 1.1 Update test suite and
against rdflib and Oxigraph (pyoxigraph).

For every update evaluation test of the suite, in every storage mode, the update is run with
return_triples=True, and the reported inserted and deleted triples of each graph must be the
difference between the graph after and before the update (as dumped by maplib). The graphs after
the update must also be the expected graphs of the test. Then the same update is run in rdflib and in
Oxigraph, and the number of triples they add to and remove from each graph must be what maplib
reports (where they give the expected result).

The changes of the operations of an update are summed, so a triple that one operation inserts and
a later one deletes is reported as both inserted and deleted. The net change (inserted and not
deleted, deleted and not inserted) is therefore what is compared with the difference of the
graphs; for updates with a single operation, the reported triples must be exactly the difference.
"""

import shutil
from typing import Dict, List, Optional, Set, Tuple

import pytest
import rdflib
from rdflib.plugins.sparql.parser import parseUpdate

from maplib import DEFAULT_GRAPH, Model

from .disk import STORAGE_FOLDER, disk_params, storage_kwargs
from .test_w3c_sparql11 import (
    MF,
    RDF,
    RDFLIB_FORMATS,
    UT,
    collect_tests,
    dump_graph,
    format_graph,
    graph_isomorphic,
    iri_to_path,
    load_graph,
    load_into_model,
    literal,
    load_known_failures,
    make_test_id,
    note_data_literals,
    rdflib_term,
    read_text,
    run_select,
    update_dataset,
    with_base,
)

# Clearing or dropping graphs is not supported with a storage folder (and COPY and MOVE clear the
# target graph first), so these updates run in memory only.
NEEDS_CLEAR = ("CLEAR", "DROP", "COPY", "MOVE")


def update_tests():
    known = load_known_failures()
    params = []
    for directory, g, entry in collect_tests():
        if MF.UpdateEvaluationTest not in set(g.objects(entry, RDF.type)):
            continue
        tid = make_test_id(directory, entry)
        marks = []
        if tid in known:
            marks.append(pytest.mark.skip(reason=f"known W3C failure: {known[tid]}"))
        params.append(pytest.param(g, entry, id=tid, marks=marks))
    return params


UPDATE_TESTS = update_tests()


@pytest.fixture(params=disk_params())
def storage(request):
    if request.param != "memory":
        shutil.rmtree(STORAGE_FOLDER, ignore_errors=True)
    yield request.param
    if request.param != "memory":
        shutil.rmtree(STORAGE_FOLDER, ignore_errors=True)


def named_graphs(m: Model) -> Set[str]:
    rows = run_select(m, "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }")["rows"]
    return {r["g"][1] for r in rows}


def snapshot(m: Model, graphs) -> Dict[Optional[str], Set[tuple]]:
    """The triples of the default graph (None) and of the named graphs."""
    out = {None: set(dump_graph(m, None))}
    for g in graphs:
        out[g] = set(dump_graph(m, g))
    return out


def reported_triples(by_predicate: dict) -> Tuple[Set[tuple], int]:
    """The distinct triples of a dict from predicate to SolutionMappings of subjects and objects,
    as maplib writes them, and the number of rows."""
    m = Model()
    rows = 0
    for p, sm in by_predicate.items():
        rows += sm.mappings.height
        if sm.mappings.height:
            m.map_triples(sm, predicate=p)
    return set(dump_graph(m, None)), rows


def same_triples(a: Set[tuple], b: Set[tuple]) -> bool:
    """Equal up to the labels of blank nodes."""
    return graph_isomorphic(list(a), list(b))


def graph_key(g: Optional[str]) -> str:
    return DEFAULT_GRAPH if g is None else g


def n_operations(text: str) -> int:
    return len(parseUpdate(text).request)


def run_maplib(g, entry, storage: str):
    action = g.value(entry, MF.action)
    request_iri = g.value(action, UT.request)
    default, named = update_dataset(g, action)
    note_data_literals(list(default) + list(named.values()))
    m = Model(**storage_kwargs(storage))
    for d in default:
        load_into_model(m, d)
    for name, file in named.items():
        load_into_model(m, file, graph=name)
    text = with_base(read_text(request_iri), request_iri)
    if "LOAD" in text:
        # The W3C LOAD tests only load sources that cannot be fetched (LOAD SILENT).
        m.set_federation(allow_load=True)
    before = snapshot(m, set(named) | named_graphs(m))
    changes = m.update(text, return_triples=True, solution_mappings=True)
    after = snapshot(m, set(before) - {None} | named_graphs(m))
    for gr in set(after) - set(before):
        before[gr] = set()
    return m, text, before, after, changes


@pytest.mark.parametrize("g, entry", UPDATE_TESTS)
def test_reported_changes_are_the_difference(g, entry, storage):
    action = g.value(entry, MF.action)
    text = read_text(g.value(action, UT.request))
    if storage != "memory" and any(k in text.upper() for k in NEEDS_CLEAR):
        pytest.skip("clearing graphs is not supported with a storage folder")
    m, text, before, after, changes = run_maplib(g, entry, storage)

    # The graphs after the update are the expected ones (in every storage mode).
    exp_default, exp_named = update_dataset(g, g.value(entry, MF.result))
    expected = [t for d in exp_default for t in load_graph(d)]
    assert graph_isomorphic(expected, list(after[None])), (
        f"Default graph differs.\nExpected:\n{format_graph(expected)}\n"
        f"Actual:\n{format_graph(list(after[None]))}"
    )
    for name in (set(after) - {None}) | set(exp_named):
        expected = load_graph(exp_named[name]) if name in exp_named else []
        assert graph_isomorphic(expected, list(after.get(name, set()))), f"Graph <{name}> differs"

    # Graphs that are empty before and after the update may be reported too, e.g. a graph that an
    # operation touched without changing it, or one that is created and dropped again: their net
    # change must be nothing.
    keys = {graph_key(gr) for gr in after}
    for k in set(changes) - keys:
        before[k] = after[k] = set()
    single = n_operations(text) == 1
    for gr in after:
        added = after[gr] - before[gr]
        removed = before[gr] - after[gr]
        c = changes.get(graph_key(gr))
        if c is None:
            assert not added and not removed, f"Changes to {graph_key(gr)} are not reported"
            continue
        ins, n_ins = reported_triples(c["inserted_triples"])
        dele, n_del = reported_triples(c["deleted_triples"])
        assert c["inserted"] == n_ins, "inserted count differs from the inserted triples"
        assert c["deleted"] == n_del, "deleted count differs from the deleted triples"
        assert same_triples(ins - dele, added), (
            f"{graph_key(gr)}: reported net insertions differ from the triples added.\n"
            f"Reported:\n{format_graph(list(ins - dele))}\nAdded:\n{format_graph(list(added))}"
        )
        assert same_triples(dele - ins, removed), (
            f"{graph_key(gr)}: reported net deletions differ from the triples removed.\n"
            f"Reported:\n{format_graph(list(dele - ins))}\nRemoved:\n{format_graph(list(removed))}"
        )
        if single:
            # One operation inserts and deletes each triple at most once, and only real changes.
            assert (n_ins, n_del) == (len(added), len(removed)), (
                f"{graph_key(gr)}: reported (+{n_ins}, -{n_del}), changed (+{len(added)}, -{len(removed)})"
            )


# ---------------------------------------------------------------------------
# rdflib and Oxigraph as other implementations
# ---------------------------------------------------------------------------

Graphs = Dict[Optional[str], Set[tuple]]


def data_files(g, node):
    """(graph name or None, file) of the dataset of an ut:action or ut:result."""
    default, named = update_dataset(g, node)
    return [(None, f) for f in default] + [(name, f) for name, f in named.items()]


def run_rdflib(g, action, text: str) -> Tuple[Graphs, Graphs]:
    ds = rdflib.Dataset()
    for name, f in data_files(g, action):
        graph = ds.default_context if name is None else ds.graph(rdflib.URIRef(name))
        path = iri_to_path(f)
        graph.parse(
            data=path.read_text(encoding="utf-8"),
            format=RDFLIB_FORMATS[path.suffix],
            publicID=f[1],
        )

    def graphs() -> Graphs:
        out = {None: set()}
        for ctx in ds.contexts():
            default = ctx.identifier == rdflib.graph.DATASET_DEFAULT_GRAPH_ID
            name = None if default else str(ctx.identifier)
            triples = {tuple(rdflib_term(x) for x in t) for t in ctx}
            if triples:
                out[name] = triples
        return out

    before = graphs()
    ds.update(text)
    return before, graphs()


def run_oxigraph(g, action, text: str) -> Tuple[Graphs, Graphs]:
    ox = pytest.importorskip("pyoxigraph")
    store = ox.Store()
    for name, f in data_files(g, action):
        path = iri_to_path(f)
        store.load(
            path.read_bytes(),
            format=ox.RdfFormat.from_extension(path.suffix[1:]),
            base_iri=f[1],
            to_graph=ox.DefaultGraph() if name is None else ox.NamedNode(name),
        )

    def term(t):
        if isinstance(t, ox.NamedNode):
            return ("uri", t.value)
        if isinstance(t, ox.BlankNode):
            return ("bnode", t.value)
        return literal(t.value, t.datatype.value, t.language)

    def graphs() -> Graphs:
        out = {None: set()}
        for q in store.quads_for_pattern(None, None, None, None):
            name = None if isinstance(q.graph_name, ox.DefaultGraph) else q.graph_name.value
            out.setdefault(name, set()).add((term(q.subject), term(q.predicate), term(q.object)))
        return out

    before = graphs()
    store.update(text)
    return before, graphs()


def expected_graphs(g, entry) -> Graphs:
    out = {None: set()}
    for name, f in data_files(g, g.value(entry, MF.result)):
        out.setdefault(name, set()).update(load_graph(f))
    return out


@pytest.mark.parametrize("engine", ["rdflib", "oxigraph"])
@pytest.mark.parametrize("g, entry", UPDATE_TESTS)
def test_reported_net_changes_match_other_implementations(g, entry, engine):
    """The net number of triples maplib reports as inserted and deleted per graph is the number
    of triples the other implementation adds to and removes from it. Updates where the other
    implementation does not give the W3C expected result are skipped."""
    action = g.value(entry, MF.action)
    request_iri = g.value(action, UT.request)
    text = with_base(read_text(request_iri), request_iri)
    run = run_rdflib if engine == "rdflib" else run_oxigraph
    try:
        before, after = run(g, action, text)
    except Exception as e:
        if isinstance(e, pytest.skip.Exception):
            raise
        pytest.skip(f"{engine} fails on this update: {type(e).__name__}: {e}")
    expected = expected_graphs(g, entry)
    for name in set(expected) | set(after):
        if not graph_isomorphic(list(expected.get(name, set())), list(after.get(name, set()))):
            pytest.skip(f"{engine}'s graph {graph_key(name)} differs from the W3C expected one")

    _, _, _, _, changes = run_maplib(g, entry, "memory")
    for gr in set(before) | set(after):
        b, a = before.get(gr, set()), after.get(gr, set())
        added, removed = len(a - b), len(b - a)
        c = changes.get(graph_key(gr), {"inserted_triples": {}, "deleted_triples": {}})
        ins, _ = reported_triples(c["inserted_triples"])
        dele, _ = reported_triples(c["deleted_triples"])
        net = (len(ins - dele), len(dele - ins))
        assert net == (added, removed), (
            f"{graph_key(gr)}: maplib reports a net change of (+{net[0]}, -{net[1]}), "
            f"{engine} (+{added}, -{removed})"
        )

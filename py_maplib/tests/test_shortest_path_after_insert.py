"""maplib:shortestPath from the solutions of a subquery, on a model whose triples came from
Model.insert. Found by the release gate on 2026-11-20 through validation_portal_core's capability
probe (api/capabilities.py): the query gave no solutions since the optimizations between
3d2402fb (right) and 056247e9 (wrong), while the same path from a constant or from VALUES, and the
same triples read with Model.reads, gave the path.

The cause: a path search (and a graph algorithm) reads the solutions of each group of its other
patterns more than once, for the keys of the search and in the final join, and each read
collected the lazy solutions again. A subquery's LIMIT without ORDER BY may give another row each
time it is collected, so the paths were found from one start and joined with solutions that held
another: no rows. The solutions are now collected once. The cases are repeated, under each
Polars engine, since a wrong result depended on which row each collect happened to give."""

import os
import shutil
import tempfile

import pytest

from maplib import Model

from .disk import disk_params

P = "PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#>\n"
TRIPLES = "<urn:probe:a> <urn:probe:p> <urn:probe:b> . <urn:probe:b> <urn:probe:p> <urn:probe:c> ."
SUBQUERY = "{ SELECT ?s WHERE { ?s ?p ?o FILTER(isIRI(?o)) } LIMIT 1 }"
FROM_SUBQUERY = P + f"""SELECT ?length WHERE {{
  {SUBQUERY}
  ?s maplib:shortestPath ?path .
  ?path maplib:anyPredicate "both" ; maplib:object ?t ; maplib:length ?length .
}} LIMIT 1"""
FROM_CONSTANT = P + """SELECT ?length WHERE {
  <urn:probe:a> maplib:shortestPath ?path .
  ?path maplib:anyPredicate "both" ; maplib:object ?t ; maplib:length ?length .
}"""
REPEATS = 20


_STORAGE = {"kwargs": {}}


def new_model() -> Model:
    return Model(**_STORAGE["kwargs"])


@pytest.fixture(autouse=True, params=disk_params())
def storage(request):
    """Every case in every storage mode (hybrid and disk need maplib Enterprise Edition)."""
    folder = None
    if request.param == "memory":
        _STORAGE["kwargs"] = {}
    else:
        folder = tempfile.mkdtemp(prefix=f"maplib_sp_insert_{request.param}_")
        _STORAGE["kwargs"] = {"storage": request.param,
                              "storage_folder": os.path.join(folder, "storage")}
    yield request.param
    _STORAGE["kwargs"] = {}
    if folder:
        shutil.rmtree(folder, ignore_errors=True)


def inserted() -> Model:
    m = new_model()
    m.insert(f"CONSTRUCT {{ {TRIPLES} }} WHERE {{}}")
    return m


def read() -> Model:
    m = new_model()
    m.reads(TRIPLES.replace(" . ", " .\n") + "\n", format="ntriples")
    return m


def updated() -> Model:
    """The table of <urn:probe:p> with a segment (c p d), and a p b, b p c in its write buffer
    from a small update: a chain a, b, c, d."""
    m = new_model()
    m.reads("<urn:probe:c> <urn:probe:p> <urn:probe:d> .\n", format="ntriples")
    m.update(f"INSERT DATA {{ {TRIPLES} }}")
    return m


# The nodes of each model (a chain), and the starts the subquery may give (subjects of p).
NODES = {"insert": 3, "small_update": 4}


@pytest.fixture(params=["auto", "in-memory", "streaming"])
def engine(request, monkeypatch):
    if request.param == "auto":
        monkeypatch.delenv("MAPLIB_POLARS_ENGINE", raising=False)
    else:
        monkeypatch.setenv("MAPLIB_POLARS_ENGINE", request.param)
    return request.param


def test_from_a_constant_after_insert():
    assert inserted().query(FROM_CONSTANT).height == 3


def test_from_a_subquery_after_reads(engine):
    m = read()
    for _ in range(REPEATS):
        assert m.query(FROM_SUBQUERY, cache=False).height == 1


@pytest.mark.parametrize("build", [inserted, updated], ids=["insert", "small_update"])
def test_from_a_subquery_after_insert(engine, build):
    m = build()
    for _ in range(REPEATS):
        assert m.query(FROM_SUBQUERY, cache=False).height == 1


@pytest.mark.parametrize("build", [inserted, updated], ids=["insert", "small_update"])
def test_every_path_from_the_subquerys_start(engine, build):
    """All paths from the start the subquery gives: one to each node of the chain, in both
    directions, the start included (there and back, length 2)."""
    m = build()
    n = NODES["insert" if build is inserted else "small_update"]
    q = P + f"""SELECT ?s ?t ?length WHERE {{
  {SUBQUERY}
  ?s maplib:shortestPath ?path .
  ?path maplib:anyPredicate "both" ; maplib:object ?t ; maplib:length ?length .
}}"""
    for _ in range(REPEATS):
        df = m.query(q, cache=False)
        assert df.height == n
        assert df["s"].n_unique() == 1


@pytest.mark.parametrize("build", [inserted, updated], ids=["insert", "small_update"])
def test_component_of_the_subquerys_start(engine, build):
    m = build()
    n = NODES["insert" if build is inserted else "small_update"]
    q = P + f"""SELECT ?s ?id ?size WHERE {{
  {SUBQUERY}
  ?s maplib:component ?c .
  ?c maplib:predicate <urn:probe:p> ; maplib:componentId ?id ; maplib:componentSize ?size .
}}"""
    for _ in range(REPEATS):
        df = m.query(q, cache=False)
        assert df.height == 1
        assert df["size"][0] == n


# The rows of each transitive path from each possible start, per model.
TRANSITIVE = {
    "insert": {"plus": {2, 1}, "star": {3, 2}, "inverse_plus": {0, 1}},
    "small_update": {"plus": {3, 2, 1}, "star": {4, 3, 2}, "inverse_plus": {0, 1, 2}},
}


@pytest.mark.parametrize("build", [inserted, updated], ids=["insert", "small_update"])
@pytest.mark.parametrize("name,path", [("plus", "<urn:probe:p>+"), ("star", "<urn:probe:p>*"),
                                       ("inverse_plus", "^<urn:probe:p>+")])
def test_transitive_paths_from_the_subquerys_start(engine, build, name, path):
    """The same rows whichever start the subquery gives, and one start per evaluation."""
    m = build()
    counts = TRANSITIVE["insert" if build is inserted else "small_update"][name]
    q = f"SELECT ?s ?x WHERE {{ {SUBQUERY} ?s {path} ?x }}"
    for _ in range(REPEATS):
        df = m.query(q, cache=False)
        assert df.height in counts
        assert df["s"].n_unique() <= 1

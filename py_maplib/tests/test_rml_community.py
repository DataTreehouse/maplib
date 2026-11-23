"""The RML test cases of the W3C Knowledge Graph Construction community group (testdata/rml, see
its README), run with Model.map_rml.

Each test case has a mapping, its input files and the expected N-Quads. The mapping is mapped
with the base IRI of the test case, and the result is compared with the expected output graph by
graph (blank nodes up to isomorphism, literals by value). A test case that expects an error must
raise one. The test cases that maplib does not pass are listed with their reason in
rml_community_known_failures.txt: they are run too, and must still fail, so that the list is
kept up to date.
"""

import csv
from pathlib import Path

import pytest
import rdflib
from rdflib import XSD, Literal
from rdflib.compare import isomorphic

from maplib import Model

TESTDATA = Path(__file__).parent / "testdata" / "rml"
KNOWN_FAILURES_FILE = Path(__file__).parent / "rml_community_known_failures.txt"


def _known_failures() -> dict:
    out = {}
    if KNOWN_FAILURES_FILE.exists():
        for line in KNOWN_FAILURES_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                identifier, _, reason = line.partition(" ")
                out[identifier] = reason.strip()
    return out


KNOWN_FAILURES = _known_failures()


def _cases():
    cases = []
    for suite in sorted(p for p in TESTDATA.iterdir() if (p / "metadata.csv").exists()):
        with open(suite / "metadata.csv", newline="") as f:
            for row in csv.DictReader(f):
                identifier = row["ID"]
                if not (suite / identifier).is_dir():
                    continue
                cases.append(
                    pytest.param(
                        suite / identifier,
                        row["mapping"],
                        row["base_iri"],
                        row["output1"] or None,
                        row["error"].strip().lower() == "true",
                        id=f"{suite.name}/{identifier}",
                    )
                )
    return cases


def _normalize(term):
    if isinstance(term, Literal) and term.datatype == XSD.string:
        # The same literal as the one without a datatype.
        return Literal(str(term))
    if isinstance(term, Literal) and term.datatype is not None and term.language is None:
        try:
            value = term.toPython()
        except Exception:
            return term
        if value is not None and not isinstance(value, Literal):
            if isinstance(value, float):
                return Literal(repr(value), datatype=term.datatype)
            normalized = Literal(value, datatype=term.datatype)
            if normalized.toPython() == value:
                return normalized
    return term


def _graphs(ds: rdflib.Dataset) -> dict:
    out = {}
    for s, p, o, g in ds.quads((None, None, None, None)):
        name = g.identifier if isinstance(g, rdflib.Graph) else g
        if name is None or name == rdflib.graph.DATASET_DEFAULT_GRAPH_ID:
            name = None
        out.setdefault(name, rdflib.Graph()).add((s, p, _normalize(o)))
    return out


def _run(folder: Path, mapping: str, base_iri: str, output, error: bool, tmp_path: Path):
    m = Model()
    if error:
        with pytest.raises(Exception):
            m.map_rml(folder / mapping, base_iri=base_iri)
        return
    m.map_rml(folder / mapping, base_iri=base_iri)
    out = tmp_path / "out.nq"
    m.write(out, format="nquads")
    actual = rdflib.Dataset()
    actual.parse(out, format="nquads")
    expected = rdflib.Dataset()
    if output is not None:
        expected.parse(folder / output, format="nquads")
    a = _graphs(actual)
    e = _graphs(expected)
    assert set(a) == set(e), f"graphs differ: {sorted(map(str, a))} != {sorted(map(str, e))}"
    for name in e:
        assert isomorphic(a[name], e[name]), (
            f"graph {name} differs:\nactual:\n{a[name].serialize(format='nt')}\n"
            f"expected:\n{e[name].serialize(format='nt')}"
        )


@pytest.mark.parametrize("folder,mapping,base_iri,output,error", _cases())
def test_rml_community(folder, mapping, base_iri, output, error, tmp_path, request):
    identifier = request.node.callspec.id
    if identifier in KNOWN_FAILURES:
        with pytest.raises(BaseException):
            _run(folder, mapping, base_iri, output, error, tmp_path)
        return
    _run(folder, mapping, base_iri, output, error, tmp_path)


def test_known_failures_are_test_cases():
    identifiers = {c.id for c in _cases()}
    unknown = sorted(set(KNOWN_FAILURES) - identifiers)
    assert not unknown, f"rml_community_known_failures.txt lists test cases that do not exist: {unknown}"

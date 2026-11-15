"""
W3C JSON-LD 1.1 compliance tests for reading (toRdf) and writing JSON-LD.

The test data in ``testdata/w3c_jsonld`` is a copy of ``tests/fromRdf``, ``tests/toRdf`` and
their manifests from https://github.com/w3c/json-ld-api (commit
55514738d854e56bb8bad4c53cda2f0e23be4a50), see ``testdata/w3c_jsonld/LICENSE.md``.

* Reading (``test_w3c_jsonld_to_rdf``, ids ``toRdf/<test>``): each toRdf evaluation test reads
  the input with maplib and compares the dataset with the expected N-Quads, parsed by the
  N-Quads parser in ``w3c_rdf.py`` (not by maplib), up to blank node renaming. Each negative
  evaluation test must fail with an error (a panic does not count).
* Writing (``test_w3c_jsonld_round_trip``, ids ``<manifest>/<test>@round-trip``): maplib writes
  JSON-LD in its own compacted form, so the expected documents of the fromRdf tests are not
  compared literally. Instead the input (N-Quads or JSON-LD) is read, written as JSON-LD with
  all its graphs, read back, and must give the dataset that was read. The reader is tested on
  its own by the toRdf tests, so a round trip of an input maplib can not read is skipped.

Tests for JSON-LD 1.0 processors, and toRdf tests with options ``Model.read`` has no
counterpart for (generalized RDF, ``rdfDirection``, ``expandContext``, 1.0 processing mode),
are left out. Tests that maplib currently does not pass are listed in
``w3c_jsonld_known_failures.txt`` and are marked as strict xfails.
"""

import json
from pathlib import Path

import pytest

from maplib import DEFAULT_GRAPH, Model

from .w3c_rdf import (
    check_known_failures,
    dataset_graphs,
    dump_dataset,
    format_quads,
    isomorphic,
    parse_nquads,
)

SUITE_DIR = Path(__file__).parent / "testdata" / "w3c_jsonld"
KNOWN_FAILURES = Path(__file__).parent / "w3c_jsonld_known_failures.txt"
BASE = "https://w3c.github.io/json-ld-api/tests/"
# toRdf options that Model.read can not be given.
UNSUPPORTED_OPTIONS = ("produceGeneralizedRdf", "rdfDirection", "expandContext")


def load_known_failures():
    known = {}
    if KNOWN_FAILURES.exists():
        for line in KNOWN_FAILURES.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            tid, _, reason = line.partition("#")
            known[tid.strip()] = reason.strip() or "known failure"
    return known


def manifest_tests(manifest: str):
    m = json.loads((SUITE_DIR / f"{manifest}-manifest.jsonld").read_text(encoding="utf-8"))
    for test in m["sequence"]:
        option = test.get("option", {})
        # Tests only for JSON-LD 1.0 processors.
        if option.get("specVersion") == "json-ld-1.0":
            continue
        yield f"{manifest}/{test['@id'].lstrip('#')}", test


def marks_for(tid: str, known: dict):
    return [pytest.mark.xfail(reason=known[tid], strict=True)] if tid in known else []


ALL_IDS = set()


def build_params():
    known = load_known_failures()
    params = []
    for manifest in ("fromRdf", "toRdf"):
        for tid, test in manifest_tests(manifest):
            if "jld:PositiveEvaluationTest" not in test["@type"]:
                continue
            tid = f"{tid}@round-trip"
            params.append(pytest.param(manifest, test, id=tid, marks=marks_for(tid, known)))
    ALL_IDS.update(p.id for p in params)
    return params


def build_to_rdf_params():
    known = load_known_failures()
    params = []
    for tid, test in manifest_tests("toRdf"):
        types = test["@type"]
        if not ("jld:PositiveEvaluationTest" in types or "jld:NegativeEvaluationTest" in types):
            continue
        option = test.get("option", {})
        marks = marks_for(tid, known)
        unsupported = [o for o in UNSUPPORTED_OPTIONS if o in option]
        if option.get("processingMode") == "json-ld-1.0":
            unsupported.append("processingMode json-ld-1.0")
        if unsupported:
            marks = [pytest.mark.skip(reason=f"Model.read has no option for {unsupported}")]
        params.append(pytest.param(test, id=tid, marks=marks))
    ALL_IDS.update(p.id for p in params)
    return params


def contexts_for(text: str) -> dict:
    """The suite's JSON-LD documents that the input refers to by name, e.g. remote contexts."""
    contexts = {}
    for path in (SUITE_DIR / "toRdf").rglob("*.jsonld"):
        if path.name in text and not path.name.endswith("-in.jsonld"):
            rel = path.relative_to(SUITE_DIR).as_posix()
            contexts[f"{BASE}{rel}"] = path.read_text(encoding="utf-8")
    return contexts


def read_input(manifest: str, test: dict) -> Model:
    path = SUITE_DIR / test["input"]
    text = path.read_bytes().decode("utf-8")
    m = Model()
    if manifest == "fromRdf":
        m.reads(text, format="nquads")
    else:
        base = test.get("option", {}).get("base", BASE + test["input"])
        m.reads(text, format="json-ld", base_iri=base, known_contexts=contexts_for(text))
    return m


@pytest.mark.parametrize("test", build_to_rdf_params())
def test_w3c_jsonld_to_rdf(test: dict):
    if "jld:NegativeEvaluationTest" in test["@type"]:
        # An error must be reported: pyo3's PanicException is not an Exception.
        with pytest.raises(Exception):
            read_input("toRdf", test)
        return
    actual = dump_dataset(read_input("toRdf", test))
    expected = parse_nquads((SUITE_DIR / test["expect"]).read_bytes().decode("utf-8"))
    assert isomorphic(expected, actual), (
        f"Datasets differ.\nExpected:\n{format_quads(expected)}\nActual:\n{format_quads(actual)}"
    )


@pytest.mark.parametrize("manifest,test", build_params())
def test_w3c_jsonld_round_trip(manifest: str, test: dict):
    try:
        m = read_input(manifest, test)
    except Exception as e:
        pytest.skip(f"maplib can not read the input (see the toRdf tests): {e}")
    expected = dump_dataset(m)
    written = m.writes(format="json-ld", graph=dataset_graphs(expected))
    # The document must be JSON, not only something the JSON-LD parser accepts.
    json.loads(written)
    m2 = Model()
    m2.reads(written, format="json-ld")
    actual = dump_dataset(m2)
    assert isomorphic(expected, actual), (
        f"Datasets differ after writing JSON-LD.\nExpected:\n{format_quads(expected)}\n"
        f"Actual:\n{format_quads(actual)}\nWritten:\n{written}"
    )


def test_known_failures_are_tests():
    check_known_failures(load_known_failures(), ALL_IDS, KNOWN_FAILURES.name)


def test_writes_compacted_document():
    m = Model()
    m.reads(
        """
        @prefix ex: <http://example.com/> .
        ex:a a ex:Pump, ex:Asset ;
            ex:name "Pump A", "Pumpe A"@nb ;
            ex:weight 1.5 ;
            ex:parts ( ex:b [ ex:name "inner" ] ) ;
            ex:part [ a ex:Bearing ] .
        """,
        format="turtle",
    )
    doc = json.loads(m.writes(format="json-ld", prefixes={"ex": "http://example.com/"}))
    # The model's default prefixes (rdf, xsd) are used as well.
    assert doc["@context"]["ex"] == "http://example.com/"
    [node] = doc["@graph"]
    assert node["@id"] == "ex:a"
    assert sorted(node["@type"]) == ["ex:Asset", "ex:Pump"]
    assert sorted(node["ex:name"], key=str) == sorted(
        ["Pump A", {"@value": "Pumpe A", "@language": "nb"}], key=str
    )
    assert node["ex:weight"] == {"@value": "1.5", "@type": "xsd:decimal"}
    assert node["ex:parts"] == {"@list": [{"@id": "ex:b"}, {"ex:name": "inner"}]}
    assert node["ex:part"] == {"@type": "ex:Bearing"}


def test_writes_empty_graph():
    doc = json.loads(Model().writes(format="json-ld"))
    assert doc == {"@graph": []}


NAMED_GRAPHS_DATA = """
<http://ex.com/a> <http://ex.com/p> "default" .
<http://ex.com/a> <http://ex.com/p> "one" <http://ex.com/g1> .
<http://ex.com/b> <http://ex.com/p> _:x <http://ex.com/g1> .
_:x <http://ex.com/p> "nested" <http://ex.com/g1> .
<http://ex.com/a> <http://ex.com/p> "two" <http://ex.com/g2> .
"""


def test_writes_only_the_given_graphs():
    m = Model()
    m.reads(NAMED_GRAPHS_DATA, format="nquads")
    prefixes = {"ex": "http://ex.com/"}
    # One graph: only the default graph.
    doc = json.loads(m.writes(format="json-ld", prefixes=prefixes))
    assert doc["@graph"] == [{"@id": "ex:a", "ex:p": "default"}]

    doc = json.loads(
        m.writes(format="json-ld", prefixes=prefixes, graph=[DEFAULT_GRAPH, "http://ex.com/g1"])
    )
    assert doc["@graph"][0] == {"@id": "ex:a", "ex:p": "default"}
    [g1] = doc["@graph"][1:]
    assert g1["@id"] == "ex:g1"
    assert sorted(g1["@graph"], key=lambda n: n["@id"]) == [
        {"@id": "ex:a", "ex:p": "one"},
        {"@id": "ex:b", "ex:p": {"ex:p": "nested"}},
    ]

    written = m.writes(
        format="json-ld", graph=[DEFAULT_GRAPH, "http://ex.com/g1", "http://ex.com/g2"]
    )
    m2 = Model()
    m2.reads(written, format="json-ld")
    assert isomorphic(dump_dataset(m), dump_dataset(m2))


def test_graph_list_without_default_graph():
    m = Model()
    m.reads(NAMED_GRAPHS_DATA, format="nquads")
    doc = json.loads(
        m.writes(format="json-ld", graph=["http://ex.com/g2"], prefixes={"ex": "http://ex.com/"})
    )
    assert doc["@graph"] == [{"@id": "ex:g2", "@graph": [{"@id": "ex:a", "ex:p": "two"}]}]
    # A single graph (not in a list) is written as the document's default graph.
    doc = json.loads(
        m.writes(format="json-ld", graph="http://ex.com/g2", prefixes={"ex": "http://ex.com/"})
    )
    assert doc["@graph"] == [{"@id": "ex:a", "ex:p": "two"}]


@pytest.mark.parametrize("format", ["trig", "nquads"])
def test_graph_list_in_trig_and_nquads(format):
    m = Model()
    m.reads(NAMED_GRAPHS_DATA, format="nquads")
    written = m.writes(format=format, graph=[DEFAULT_GRAPH, "http://ex.com/g2"])
    m2 = Model()
    m2.reads(written, format=format)
    expected = [q for q in dump_dataset(m) if q[3] is None or q[3][1] == "http://ex.com/g2"]
    assert isomorphic(expected, dump_dataset(m2))


@pytest.mark.parametrize("format", ["turtle", "ntriples", "rdf/xml"])
def test_graph_list_only_for_dataset_formats(format):
    m = Model()
    m.reads(NAMED_GRAPHS_DATA, format="nquads")
    with pytest.raises(Exception, match="list of graphs"):
        m.writes(format=format, graph=["http://ex.com/g1"])


def test_missing_graph_in_list_fails(tmp_path):
    m = Model()
    m.reads(NAMED_GRAPHS_DATA, format="nquads")
    with pytest.raises(Exception, match="does not exist"):
        m.writes(format="json-ld", graph=["http://ex.com/nothing"])
    path = tmp_path / "out.jsonld"
    with pytest.raises(Exception, match="does not exist"):
        m.write(path, format="json-ld", graph=[DEFAULT_GRAPH, "http://ex.com/nothing"])
    assert not path.exists()
    m.write(path, format="json-ld", graph=[DEFAULT_GRAPH, "http://ex.com/g2"])
    assert json.loads(path.read_text())["@graph"][1]["@graph"]


def test_prefix_named_like_an_iri_scheme_is_left_out():
    m = Model()
    m.reads("<urn:a> <http://example.com/p> <ex:b> .", format="ntriples")
    written = m.writes(format="json-ld", prefixes={"ex": "http://example.com/"})
    # <ex:b> would be read as a compact IRI if ex were a prefix.
    assert "ex" not in json.loads(written).get("@context", {})
    m2 = Model()
    m2.reads(written, format="json-ld")
    assert isomorphic(dump_dataset(m), dump_dataset(m2))

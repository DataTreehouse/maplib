"""
W3C JSON-LD 1.1 compliance tests for the JSON-LD writer.

The test data in ``testdata/w3c_jsonld`` is a copy of ``tests/fromRdf``, ``tests/toRdf`` and
their manifests from https://github.com/w3c/json-ld-api (commit
55514738d854e56bb8bad4c53cda2f0e23be4a50), see ``testdata/w3c_jsonld/LICENSE.md``.

maplib writes JSON-LD in its own compacted form, so the expected documents of the fromRdf tests
are not compared literally. Instead, for each positive fromRdf and toRdf evaluation test the
input (N-Quads or JSON-LD) is read with maplib, written as JSON-LD, read back, and the dataset
must be the one that was read, up to blank node renaming. The toRdf inputs cover lists, nested
and shared blank nodes, language tags, datatypes and unusual IRIs, and the fromRdf inputs are the
graphs the JSON-LD serialization algorithm is tested with. These are tests of the writer: an
input maplib can not read is skipped, the reader is tested by its own suite in oxjsonld.

All graphs of the input are written, as a list in ``graph``.

Tests that maplib currently does not pass are listed in ``w3c_jsonld_known_failures.txt`` and are
marked as strict xfails.
"""

import json
from pathlib import Path

import pytest

from maplib import DEFAULT_GRAPH, Model

from .w3c_rdf import dataset_graphs, dump_dataset, format_quads, isomorphic

SUITE_DIR = Path(__file__).parent / "testdata" / "w3c_jsonld"
KNOWN_FAILURES = Path(__file__).parent / "w3c_jsonld_known_failures.txt"
BASE = "https://w3c.github.io/json-ld-api/tests/"


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


def build_params():
    known = load_known_failures()
    params = []
    for manifest in ("fromRdf", "toRdf"):
        m = json.loads((SUITE_DIR / f"{manifest}-manifest.jsonld").read_text(encoding="utf-8"))
        for test in m["sequence"]:
            if "jld:PositiveEvaluationTest" not in test["@type"]:
                continue
            option = test.get("option", {})
            # Tests only for JSON-LD 1.0 processors.
            if option.get("specVersion") == "json-ld-1.0":
                continue
            tid = f"{manifest}/{test['@id'].lstrip('#')}"
            marks = []
            if tid in known:
                marks.append(pytest.mark.xfail(reason=known[tid], strict=True))
            params.append(pytest.param(manifest, test, id=tid, marks=marks))
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


@pytest.mark.parametrize("manifest,test", build_params())
def test_w3c_jsonld_round_trip(manifest: str, test: dict):
    try:
        m = read_input(manifest, test)
    except Exception as e:
        pytest.skip(f"maplib can not read the input: {e}")
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

import hashlib
import pathlib

import polars as pl
import pytest
import rdflib
from rdflib.compare import isomorphic

from maplib import Model

pl.Config.set_fmt_str_lengths(300)

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"


def model_as_rdflib_graph(m: Model) -> rdflib.Graph:
    g = rdflib.Graph()
    g.parse(data=m.writes(format="ntriples"), format="nt")
    return g


def test_write_read_hdt_round_trip_is_isomorphic(tmp_path):
    m = Model()
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    hdt_path = tmp_path / "out.hdt"
    m.write(str(hdt_path), format="hdt")

    m2 = Model()
    m2.read(str(hdt_path), format="hdt")

    assert isomorphic(model_as_rdflib_graph(m), model_as_rdflib_graph(m2))


def test_read_hdt_infers_format_from_file_extension(tmp_path):
    m = Model()
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    hdt_path = tmp_path / "out.hdt"
    m.write(str(hdt_path), format="hdt")

    m2 = Model()
    m2.read(str(hdt_path))
    res = m2.query(
        """
        SELECT ?v ?o WHERE {
            ?s ?v ?o .
        }
        """
    )
    assert res.height == 8


def test_hdt_round_trip_preserves_typed_and_language_literals(tmp_path):
    m = Model()
    m.reads(
        """
        <urn:maplib:s1> <urn:maplib:p> "plain" .
        <urn:maplib:s1> <urn:maplib:lang> "hello"@en .
        <urn:maplib:s1> <urn:maplib:int> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
        <urn:maplib:s1> <urn:maplib:dec> "1.5"^^<http://www.w3.org/2001/XMLSchema#decimal> .
        <urn:maplib:s1> <urn:maplib:esc> "line1\\nline2 \\"quoted\\"" .
        <urn:maplib:s1> <urn:maplib:obj> <urn:maplib:s2> .
        """,
        format="ntriples",
    )
    hdt_path = tmp_path / "literals.hdt"
    m.write(str(hdt_path), format="hdt")

    m2 = Model()
    m2.read(str(hdt_path))

    assert isomorphic(model_as_rdflib_graph(m), model_as_rdflib_graph(m2))

def test_empty_graph_hdt_round_trip(tmp_path):
    m = Model()
    m.reads("<urn:maplib:s> <urn:maplib:p> <urn:maplib:o> .", format="ntriples")
    m.update("DELETE WHERE { ?s ?p ?o }")
    hdt_path = tmp_path / "empty.hdt"
    m.write(str(hdt_path), format="hdt")

    m2 = Model()
    m2.read(str(hdt_path), format="hdt")
    res = m2.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
    assert res.height == 0


def test_writes_with_hdt_format_raises_since_hdt_is_binary():
    m = Model()
    m.reads("<urn:maplib:s> <urn:maplib:p> <urn:maplib:o> .", format="ntriples")
    with pytest.raises(Exception, match="binary"):
        m.writes(format="hdt")


def test_reads_with_hdt_format_raises_since_hdt_is_binary():
    m = Model()
    with pytest.raises(Exception, match="binary"):
        m.reads("anything", format="hdt")


def test_hdt_round_trip_preserves_object_position_blank_nodes(tmp_path):
    m = Model()
    m.reads(
        """
        <urn:maplib:s1> <urn:maplib:p> _:b1 .
        _:b1 <urn:maplib:p> _:b2 .
        _:b2 <urn:maplib:p> "leaf" .
        """,
        format="ntriples",
    )
    hdt_path = tmp_path / "blanks.hdt"
    m.write(str(hdt_path), format="hdt")

    m2 = Model()
    m2.read(str(hdt_path))

    assert isomorphic(model_as_rdflib_graph(m), model_as_rdflib_graph(m2))


def test_hdt_round_trip_preserves_backslashes_in_literals(tmp_path):
    m = Model()
    m.reads(
        "<urn:maplib:s> <urn:maplib:p> \"C:\\\\new\\\\path\" .",
        format="ntriples",
    )
    hdt_path = tmp_path / "backslash.hdt"
    m.write(str(hdt_path), format="hdt")

    m2 = Model()
    m2.read(str(hdt_path))
    res = m2.query("SELECT ?o WHERE { ?s ?p ?o }")
    assert res.get_column("o").to_list() == ["C:\\new\\path"]


def test_reading_truncated_hdt_file_raises_normal_exception(tmp_path):
    m = Model()
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    hdt_path = tmp_path / "out.hdt"
    m.write(str(hdt_path), format="hdt")

    content = hdt_path.read_bytes()
    truncated_path = tmp_path / "truncated.hdt"
    truncated_path.write_bytes(content[: len(content) // 2])

    m2 = Model()
    with pytest.raises(Exception, match="HDT"):
        m2.read(str(truncated_path))


def test_write_cim_xml_format_does_not_truncate_existing_file(tmp_path):
    m = Model()
    m.reads("<urn:maplib:s> <urn:maplib:p> <urn:maplib:o> .", format="ntriples")
    out_path = tmp_path / "out.xml"
    out_path.write_text("precious")
    with pytest.raises(Exception, match="write_cim_xml"):
        m.write(str(out_path), format="cim/xml")
    assert out_path.read_text() == "precious"


def test_hdt_round_trip_preserves_language_tags(tmp_path):
    m = Model()
    m.reads(
        '<urn:maplib:s> <urn:maplib:p> "hello"@en-US .',
        format="ntriples",
    )
    hdt_path = tmp_path / "lang.hdt"
    m.write(str(hdt_path), format="hdt")

    m2 = Model()
    m2.read(str(hdt_path))

    assert m.writes(format="ntriples").strip() == m2.writes(format="ntriples").strip()


MIXED_NT = """
<urn:maplib:s1> <urn:maplib:p> "plain" .
<urn:maplib:s1> <urn:maplib:lang> "hello"@en .
<urn:maplib:s2> <urn:maplib:lang> "hei"@no .
<urn:maplib:s1> <urn:maplib:int> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
<urn:maplib:s2> <urn:maplib:int> "43"^^<http://www.w3.org/2001/XMLSchema#integer> .
<urn:maplib:s1> <urn:maplib:dbl> "NaN"^^<http://www.w3.org/2001/XMLSchema#double> .
<urn:maplib:s2> <urn:maplib:dbl> "1.5E0"^^<http://www.w3.org/2001/XMLSchema#double> .
<urn:maplib:s1> <urn:maplib:dt> "2020-01-01T10:00:00+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<urn:maplib:s2> <urn:maplib:dt> "2020-01-01T10:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<urn:maplib:s1> <urn:maplib:custom> "x"^^<urn:maplib:customType> .
<urn:maplib:s1> <urn:maplib:obj> <urn:maplib:s2> .
<urn:maplib:s2> <urn:maplib:obj> _:b1 .
_:b1 <urn:maplib:p> "leaf" .
"""


def test_hdt_round_trip_mixed_literals_in_small_read_batches(tmp_path):
    m = Model()
    m.reads(MIXED_NT, format="ntriples")
    hdt_path = tmp_path / "mixed.hdt"
    m.write(str(hdt_path), format="hdt")

    for batch_size in [1, 2, 10_000_000]:
        m2 = Model()
        m2.read(str(hdt_path), triples_batch_size=batch_size)
        res = m2.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
        assert res.height == 13, batch_size
        assert isomorphic(model_as_rdflib_graph(m), model_as_rdflib_graph(m2))


def test_hdt_read_into_model_with_overlapping_terms(tmp_path):
    m = Model()
    m.reads(MIXED_NT, format="ntriples")
    hdt_path = tmp_path / "mixed.hdt"
    m.write(str(hdt_path), format="hdt")

    m2 = Model()
    m2.reads(
        """
        <urn:maplib:s1> <urn:maplib:p> "plain" .
        <urn:maplib:s3> <urn:maplib:p> "other" .
        <urn:maplib:s3> <urn:maplib:lang> "hello"@en .
        """,
        format="ntriples",
    )
    m2.read(str(hdt_path))
    res = m2.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
    # 13 from the HDT file, one duplicate, two new.
    assert res.height == 15
    res = m2.query(
        'SELECT ?s WHERE { ?s <urn:maplib:lang> "hello"@en }'
    ).sort("s")
    assert res.get_column("s").to_list() == ["<urn:maplib:s1>", "<urn:maplib:s3>"]


def test_hdt_write_is_deterministic(tmp_path):
    m = Model()
    m.reads(MIXED_NT, format="ntriples")
    a = tmp_path / "a.hdt"
    b = tmp_path / "b.hdt"
    m.write(str(a), format="hdt")
    m.write(str(b), format="hdt")
    assert a.read_bytes() == b.read_bytes()



# Real RDF data with many kinds of literals and blank nodes, from the W3C test suites: the
# positive N-Triples syntax tests and the expected results of the Turtle evaluation tests of RDF
# 1.1 (everything about strings: escapes, control characters, UTF-8 boundaries, language tags),
# and the data of the SPARQL 1.1 query tests (numbers, dates, booleans, custom datatypes).
W3C_RDF11_PATH = TESTDATA_PATH / "w3c_rdf11"
W3C_SPARQL11_PATH = TESTDATA_PATH / "w3c_sparql11"
# SPARQL 1.1 data files with relative IRIs, which need a base IRI to be read.
W3C_SPARQL11_RELATIVE = {
    "agg-empty-group-count-graph.ttl",
    "graph-no-var.ttl",
    "graph.ttl",
    "clear-default.ttl",
    "drop-default.ttl",
    "exists-graph-variable.ttl",
}


def _w3c_model() -> Model:
    m = Model()
    for f in sorted(W3C_RDF11_PATH.glob("rdf-*/*.nt")):
        # Skip the negative syntax tests, and test-38.nt, which no manifest uses and which
        # escapes a surrogate pair.
        if "-bad-" in f.name or f.name == "test-38.nt":
            continue
        m.read(str(f), format="ntriples")
    for f in sorted(W3C_SPARQL11_PATH.glob("*/*.ttl")):
        if not f.name.startswith("manifest") and f.name not in W3C_SPARQL11_RELATIVE:
            m.read(str(f), format="turtle")
    return m


def _datatype_counts(m: Model) -> dict:
    df = m.query(
        """
        SELECT ?dt (COUNT(*) AS ?n) WHERE {
            ?s ?p ?o . FILTER(isLiteral(?o)) BIND(DATATYPE(?o) AS ?dt)
        } GROUP BY ?dt
        """
    )
    return dict(zip(df.get_column("dt").to_list(), df.get_column("n").to_list()))


def _non_blank_ntriples(m: Model) -> list:
    return sorted(
        line for line in m.writes(format="ntriples").splitlines() if "_:" not in line
    )


def _blank_node_signature(m: Model, rounds: int = 6) -> list:
    """The triples with blank nodes, with each blank node replaced by a colour from a few rounds
    of colour refinement over its neighbourhood. Equal for isomorphic graphs whatever the blank
    node labels, which are renamed on every read. Unlike rdflib's isomorphism check, it stays
    fast with many blank nodes that look alike."""
    triples = []
    for line in m.writes(format="ntriples").splitlines():
        if "_:" in line:
            s, p, o = line[: -len(" .")].split(" ", 2)
            triples.append((s, p, o))
    blank = lambda t: t.startswith("_:")
    colour = {t: "" for s, _, o in triples for t in (s, o) if blank(t)}
    term = lambda t: colour[t] if blank(t) else t
    for _ in range(rounds):
        edges = {b: [] for b in colour}
        for s, p, o in triples:
            if blank(s):
                edges[s].append(("out", p, term(o)))
            if blank(o):
                edges[o].append(("in", p, term(s)))
        colour = {
            b: hashlib.sha256(repr((colour[b], sorted(e))).encode()).hexdigest()[:16]
            for b, e in edges.items()
        }
    return sorted((term(s), p, term(o)) for s, p, o in triples)


def test_hdt_round_trip_w3c_test_data(tmp_path):
    from polars.testing import assert_frame_equal

    m = _w3c_model()
    n = m.query("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }").get_column("n")[0]
    assert n > 900
    counts = _datatype_counts(m)
    assert len(counts) >= 15, counts
    expected_triples = _non_blank_ntriples(m)
    expected_blank = _blank_node_signature(m)
    assert len(expected_blank) > 200
    predicates = sorted(
        m.query("SELECT DISTINCT ?p WHERE { ?s ?p ?o }").get_column("p").to_list()
    )

    hdt_path = tmp_path / "w3c.hdt"
    m.write(str(hdt_path), format="hdt")
    for batch_size in [3, 10_000_000]:
        m2 = Model()
        m2.read(str(hdt_path), triples_batch_size=batch_size)
        assert _datatype_counts(m2) == counts, batch_size
        assert _non_blank_ntriples(m2) == expected_triples, batch_size
        assert _blank_node_signature(m2) == expected_blank, batch_size
        # The values come back with the same native column types, per predicate.
        for p in predicates:
            q = f"SELECT ?s ?o WHERE {{ ?s {p} ?o FILTER(!isBlank(?s) && !isBlank(?o)) }}"
            assert_frame_equal(
                m.query(q).sort(["s", "o"]), m2.query(q).sort(["s", "o"]), check_exact=True
            )

    # A second generation, written from what was read, holds the same triples.
    hdt_path2 = tmp_path / "w3c2.hdt"
    m2.write(str(hdt_path2), format="hdt")
    m3 = Model()
    m3.read(str(hdt_path2))
    assert _non_blank_ntriples(m3) == expected_triples
    assert _blank_node_signature(m3) == expected_blank

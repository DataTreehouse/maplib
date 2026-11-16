"""Serializing after deletes that leave an empty dictionary (cat) behind.

DROP GRAPH, DELETE WHERE and map_csvw (which drops its temporary Facade-X graphs) can leave
a cat that is present but has no live strings. Serialization and deserialization recompute
the code counters from the cats and used to panic on an empty one.
"""
import json
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal

from maplib import Model

ALL = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o"
ALL_G = "SELECT ?s ?p ?o WHERE { GRAPH <urn:g> { ?s ?p ?o } } ORDER BY ?s ?p ?o"


def _roundtrip(m: Model, tmp_path: Path, name: str = "m.maplib") -> Model:
    p = tmp_path / name
    m.serialize(str(p))
    return Model.deserialize(str(p))


def _add_more_and_check(m2: Model):
    # New codes after a round trip must not collide with surviving ones.
    m2.reads(
        '<http://new> <http://p> "y" .\n<http://new> <http://p> _:b1 .\n'
        '<http://new> <http://q> "2"^^<http://www.w3.org/2001/XMLSchema#integer> .',
        format="ntriples",
    )
    df = m2.query(
        "SELECT (STR(?o) AS ?v) WHERE { <http://new> <http://p> ?o FILTER(ISLITERAL(?o)) }"
    )
    assert df["v"].to_list() == ["y"]
    df = m2.query("SELECT ?o WHERE { <http://new> <http://p> ?o FILTER(ISBLANK(?o)) }")
    assert df.height == 1


def test_drop_graph_then_serialize(tmp_path):
    m = Model()
    m.reads('<http://a> <http://b> "x" .\n<http://a> <http://b> _:b0 .', format="ntriples",
            graph="urn:g")
    m.reads('<http://keep> <http://b> <http://c> .', format="ntriples")
    m.update("DROP GRAPH <urn:g>")
    m2 = _roundtrip(m, tmp_path)
    assert_frame_equal(m2.query(ALL), m.query(ALL))
    assert m2.query(ALL_G).height == 0
    _add_more_and_check(m2)
    # And a second round trip of the extended model.
    m3 = _roundtrip(m2, tmp_path, "m3.maplib")
    assert_frame_equal(m3.query(ALL), m2.query(ALL))


def test_drop_only_graph_then_serialize(tmp_path):
    m = Model()
    m.reads('<http://a> <http://b> "x" .', format="ntriples", graph="urn:g")
    m.update("DROP GRAPH <urn:g>")
    m2 = _roundtrip(m, tmp_path)
    assert m2.query(ALL).height == 0
    _add_more_and_check(m2)


def test_delete_where_then_serialize(tmp_path):
    m = Model()
    m.reads(
        '<http://a> <http://b> "x" .\n<http://a> <http://b> _:b0 .\n'
        '<http://a> <http://c> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .\n'
        '<http://keep> <http://k> <http://v> .',
        format="ntriples",
    )
    m.update("DELETE WHERE { <http://a> ?p ?o }")
    m2 = _roundtrip(m, tmp_path)
    assert_frame_equal(m2.query(ALL), m.query(ALL))
    assert m2.query(ALL).height == 1
    _add_more_and_check(m2)


def test_add_graph_after_drop_then_serialize(tmp_path):
    src = Model()
    src.reads('<http://a> <http://b> _:b0 .', format="ntriples", graph="urn:tmp")
    src.reads('<http://a> <http://b> <http://c> .', format="ntriples")
    src.update("DROP GRAPH <urn:tmp>")
    m = Model()
    m.add_graph(src)
    m2 = _roundtrip(m, tmp_path)
    assert_frame_equal(m2.query(ALL), m.query(ALL))
    _add_more_and_check(m2)


CSV = "id,name\n1,alpha\n2,beta\n"
METADATA = {
    "@context": "http://www.w3.org/ns/csvw",
    "url": "t.csv",
    "tableSchema": {
        "columns": [
            {"name": "id", "titles": "id", "datatype": "integer"},
            {"name": "name", "titles": "name"},
        ],
        "aboutUrl": "http://example.org/row/{id}",
    },
}


def _csvw(tmp_path: Path) -> Path:
    (tmp_path / "t.csv").write_text(CSV, encoding="utf-8")
    p = tmp_path / "t.csv-metadata.json"
    p.write_text(json.dumps(METADATA), encoding="utf-8")
    return p


def test_map_csvw_then_serialize(tmp_path):
    m = Model()
    m.reads('<http://keep> <http://k> <http://v> .', format="ntriples")
    m.map_csvw(_csvw(tmp_path), graph="urn:limits", minimal=True)
    q = """SELECT ?s ?p ?o WHERE { GRAPH <urn:limits> { ?s ?p ?o } } ORDER BY ?s ?p ?o"""
    before = m.query(q)
    assert before.height > 0
    m2 = _roundtrip(m, tmp_path)
    assert_frame_equal(m2.query(q), before)
    assert_frame_equal(m2.query(ALL), m.query(ALL))
    _add_more_and_check(m2)


def test_map_csvw_standard_mode_then_serialize(tmp_path):
    m = Model()
    m.map_csvw(_csvw(tmp_path))
    before = m.query(ALL)
    assert before.height > 0
    m2 = _roundtrip(m, tmp_path)
    assert_frame_equal(m2.query(ALL), before)
    _add_more_and_check(m2)

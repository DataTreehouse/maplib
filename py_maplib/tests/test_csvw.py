import json
import warnings
from pathlib import Path

import pytest

from maplib import MaplibException, Model

CSV = """GID,On Street,Species,Trim Cycle,Inventory Date,Keywords
1,ADDISON AV,Celtis australis,Large Tree Routine Prune,10/18/2010,tree;street
2,EMERSON ST,Liquidambar styraciflua,Large Tree Routine Prune,6/2/2010,
3,,Robinia pseudoacacia,Large Tree Routine Prune,6/2/2010,tree
"""

METADATA = {
    "@context": ["http://www.w3.org/ns/csvw", {"@language": "en"}],
    "url": "tree-ops.csv",
    "dc:title": "Tree Operations",
    "tableSchema": {
        "columns": [
            {"name": "GID", "titles": "GID", "datatype": "integer", "suppressOutput": True},
            {"name": "on_street", "titles": "On Street", "propertyUrl": "schema:streetAddress"},
            {"name": "species", "titles": "Species", "propertyUrl": "http://example.org/species", "lang": "la"},
            {"name": "trim_cycle", "titles": "Trim Cycle", "valueUrl": "http://example.org/cycle/{trim_cycle}"},
            {"name": "inventory_date", "titles": "Inventory Date",
             "datatype": {"base": "date", "format": "M/d/yyyy"}},
            {"name": "keywords", "titles": "Keywords", "separator": ";"},
            {"name": "type", "virtual": True, "propertyUrl": "rdf:type", "valueUrl": "schema:Thing"},
        ],
        "aboutUrl": "http://example.org/tree/{GID}",
    },
}


@pytest.fixture
def tree_ops(tmp_path: Path) -> Path:
    (tmp_path / "tree-ops.csv").write_text(CSV, encoding="utf-8")
    p = tmp_path / "tree-ops.csv-metadata.json"
    p.write_text(json.dumps(METADATA), encoding="utf-8")
    return p


def test_minimal_mode(tree_ops):
    m = Model()
    update = m.map_csvw(tree_ops, minimal=True)
    assert "INSERT" in update and "fx:child" in update
    df = m.query(
        """
        PREFIX schema: <http://schema.org/>
        SELECT (STR(?t) AS ?tree) ?street (STR(?sp) AS ?species) (LANG(?sp) AS ?lang)
               (STR(?c) AS ?cycle) (STR(?d) AS ?date) (STR(DATATYPE(?d)) AS ?dt) WHERE {
            ?t a schema:Thing ;
                <http://example.org/species> ?sp ;
                ?cycleProp ?c .
            OPTIONAL { ?t schema:streetAddress ?street }
            OPTIONAL { ?t ?dateProp ?d FILTER(DATATYPE(?d) = xsd:date) }
            FILTER(ISIRI(?c) && ?c != schema:Thing)
        } ORDER BY ?tree"""
    )
    assert df.height == 3
    assert df["tree"].to_list() == [
        "http://example.org/tree/1",
        "http://example.org/tree/2",
        "http://example.org/tree/3",
    ]
    assert df["species"][0] == "Celtis australis"
    assert df["lang"][0] == "la"
    assert df["cycle"][0] == "http://example.org/cycle/Large%20Tree%20Routine%20Prune"
    assert df["date"][0] == "2010-10-18"
    assert df["dt"][0] == "http://www.w3.org/2001/XMLSchema#date"
    # Null cell, no triple
    assert df["street"][2] is None
    # The GID column is suppressed
    df = m.query("SELECT * WHERE { ?s <http://example.org/tree-ops.csv#GID> ?o }")
    assert df.height == 0
    # List values
    df = m.query(
        """SELECT ?tree ?kw WHERE { ?tree ?p ?kw FILTER(STRENDS(STR(?p), "#keywords")) } ORDER BY ?tree ?kw"""
    )
    assert df["kw"].to_list() == ["street", "tree", "tree"]
    # No table, row or Facade-X triples in minimal mode
    df = m.query("SELECT * WHERE { ?s a ?t FILTER(?t IN (<http://www.w3.org/ns/csvw#Row>, fx:root)) }")
    assert df.height == 0


def test_standard_mode_and_temporary_graphs(tree_ops):
    m = Model()
    m.map_csvw(tree_ops)
    df = m.query(
        """
        PREFIX csvw: <http://www.w3.org/ns/csvw#>
        PREFIX dc: <http://purl.org/dc/terms/>
        SELECT ?rownum (STR(?u) AS ?url) ?tree WHERE {
            ?g a csvw:TableGroup ; csvw:table ?t .
            ?t a csvw:Table ; dc:title "Tree Operations"@en ; csvw:row ?r .
            ?r csvw:rownum ?rownum ; csvw:url ?u ; csvw:describes ?tree .
        } ORDER BY ?rownum"""
    )
    assert df["rownum"].to_list() == [1, 2, 3]
    assert df["url"][0].endswith("tree-ops.csv#row=2")
    # The Facade-X representation is removed
    df = m.query("SELECT ?g WHERE { GRAPH ?g { ?s ?p ?o } }")
    assert df.height == 0


def test_named_graph(tree_ops):
    m = Model()
    m.map_csvw(tree_ops, graph="urn:graph:trees", minimal=True)
    assert m.query("SELECT * WHERE { ?s ?p ?o }").height == 0
    assert m.query("SELECT * WHERE { ?s ?p ?o }", graph="urn:graph:trees").height > 0


def test_csv_without_metadata(tmp_path: Path):
    p = tmp_path / "people.csv"
    p.write_text("name,age\nAlice,30\nBob,\n", encoding="utf-8")
    m = Model()
    m.map_csvw(p, minimal=True)
    df = m.query(
        "SELECT ?name ?age WHERE { ?s ?pn ?name FILTER(STRENDS(STR(?pn), '#name')) OPTIONAL { ?s ?pa ?age FILTER(STRENDS(STR(?pa), '#age')) } } ORDER BY ?name"
    )
    assert df["name"].to_list() == ["Alice", "Bob"]
    # Without metadata, all values are strings
    assert df["age"].to_list() == ["30", None]


def test_invalid_cell_warns_and_is_a_string(tmp_path: Path):
    (tmp_path / "n.csv").write_text("n\n1\nx\n", encoding="utf-8")
    md = {
        "@context": "http://www.w3.org/ns/csvw",
        "url": "n.csv",
        "tableSchema": {"columns": [{"name": "n", "datatype": "integer"}]},
    }
    (tmp_path / "n.json").write_text(json.dumps(md), encoding="utf-8")
    m = Model()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        m.map_csvw(tmp_path / "n.json", minimal=True)
    assert any("not a valid integer" in str(x.message) for x in w)
    df = m.query("SELECT (STR(?o) AS ?v) (STR(DATATYPE(?o)) AS ?dt) WHERE { ?s ?p ?o } ORDER BY ?v")
    assert df["v"].to_list() == ["1", "x"]
    assert df["dt"].to_list() == [
        "http://www.w3.org/2001/XMLSchema#integer",
        "http://www.w3.org/2001/XMLSchema#string",
    ]


def test_metadata_error(tmp_path: Path):
    (tmp_path / "bad.json").write_text(json.dumps({"@context": "http://www.w3.org/ns/csvw", "tables": []}))
    m = Model()
    with pytest.raises(MaplibException):
        m.map_csvw(tmp_path / "bad.json")


def test_iri_mappings(tmp_path: Path):
    (tmp_path / "data.csv").write_text("id,v\na,1\n", encoding="utf-8")
    md = {
        "@context": "http://www.w3.org/ns/csvw",
        "url": "data.csv",
        "tableSchema": {"columns": [{"name": "id"}, {"name": "v"}], "aboutUrl": "#{id}"},
    }
    (tmp_path / "md.json").write_text(json.dumps(md), encoding="utf-8")
    m = Model()
    m.map_csvw(
        "https://example.org/data/md.json",
        iri_mappings={"https://example.org/data/": str(tmp_path)},
        minimal=True,
    )
    df = m.query("SELECT ?s ?p WHERE { ?s ?p ?o } ORDER BY ?p")
    assert df["s"].to_list() == ["<https://example.org/data/data.csv#a>"] * 2
    assert df["p"].to_list() == [
        "<https://example.org/data/data.csv#id>",
        "<https://example.org/data/data.csv#v>",
    ]

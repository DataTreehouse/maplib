"""Model.map_rml: RML mappings over CSV, JSON and XML sources.

The JSON and XML sources are mapped with the Façade-X mapping of map_json and map_xml into a
temporary graph, and the JSONPath and XPath iterators and references are evaluated as SPARQL over
it. The tests cover the supported subset of both, the two RML vocabularies, joins, values with
several matches, and the errors for what is not supported."""

import polars as pl
import pytest

from maplib import Model

EX = "http://example.org/"

# The first RML vocabulary: rr: term maps with rml:logicalSource and rml:reference.
OLD = """@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix ql: <http://semweb.mmlab.be/ns/ql#> .
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
"""

# The vocabulary of the W3C community group: every term is in one namespace.
NEW = """@prefix rml: <http://w3id.org/rml/> .
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
"""

PLANT_JSON = """{
  "plant": "North",
  "pumps": [
    {"id": 1, "name": "Feed pump", "kw": 7.5, "running": true,
     "site": {"code": "N1"}, "tags": ["a", "b"], "serial no": "X-1"},
    {"id": 2, "name": "Booster", "kw": 11, "running": false,
     "site": {"code": "N2"}, "tags": [], "spare": null},
    {"id": 3, "site": {"code": "N1"}, "tags": ["c"]}
  ],
  "sites": [{"code": "N1", "label": "North one"}, {"code": "N2", "label": "North two"}]
}"""

PLANT_XML = """<?xml version="1.0"?>
<plant name="North">
  <pump id="1" kind="feed">
    <name>Feed pump</name>
    <site code="N1"/>
    <tag>a</tag><tag>b</tag>
  </pump>
  <pump id="2" kind="booster">
    <name>Booster &amp; spare</name>
    <site code="N2"/>
  </pump>
  <area>
    <pump id="3" kind="feed"><site code="N1"/><tag>c</tag></pump>
  </area>
  <site code="N1"><label>North one</label></site>
  <site code="N2"><label>North two</label></site>
</plant>"""


def _rows(m: Model, query: str, graph=None) -> list:
    return m.query("PREFIX ex: <http://example.org/>\n" + query, graph=graph).rows()


def _triples(m: Model) -> list:
    return _rows(m, "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o")


JSON_MAPPING = """
<#Pump> rml:logicalSource [ rml:source "plant.json" ; rml:referenceFormulation ql:JSONPath ;
        rml:iterator "$.pumps[*]" ] ;
    rr:subjectMap [ rr:template "http://example.org/pump/{id}" ; rr:class ex:Pump ] ;
    rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rml:reference "name" ; rr:language "en" ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:kw ; rr:objectMap [ rml:reference "$.kw" ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:running ; rr:objectMap [ rml:reference "running" ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:serial ; rr:objectMap [ rml:reference "serial no" ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:spare ; rr:objectMap [ rml:reference "spare" ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:tag ; rr:objectMap [ rml:reference "tags[*]" ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:siteCode ; rr:objectMap [ rml:reference "site.code" ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:site ; rr:objectMap [ rr:parentTriplesMap <#Site> ;
        rr:joinCondition [ rr:child "site.code" ; rr:parent "code" ] ] ] .

<#Site> rml:logicalSource [ rml:source "plant.json" ; rml:referenceFormulation ql:JSONPath ;
        rml:iterator "$.sites[*]" ] ;
    rr:subjectMap [ rr:template "http://example.org/site/{code}" ] ;
    rr:predicateObjectMap [ rr:predicate ex:label ; rr:objectMap [ rml:reference "label" ] ] .

<#Plant> rml:logicalSource [ rml:source "plant.json" ; rml:referenceFormulation ql:JSONPath ;
        rml:iterator "$" ] ;
    rr:subjectMap [ rr:template "http://example.org/plant/{plant}" ; rr:class ex:Plant ] .
"""


def test_json_source():
    m = Model()
    m.map_rml(OLD + JSON_MAPPING, sources={"plant.json": PLANT_JSON}, base_iri=EX)
    assert _rows(m, "SELECT ?s WHERE { ?s a ex:Pump } ORDER BY ?s") == [
        ("<http://example.org/pump/1>",),
        ("<http://example.org/pump/2>",),
        ("<http://example.org/pump/3>",),
    ]
    assert _rows(m, "SELECT ?s WHERE { ?s a ex:Plant }") == [("<http://example.org/plant/North>",)]
    # Strings with a language, and no triple for a missing key.
    assert _rows(m, "SELECT (STR(?n) AS ?v) (LANG(?n) AS ?l) WHERE { ?s ex:name ?n } ORDER BY ?v") == [
        ("Booster", "en"),
        ("Feed pump", "en"),
    ]
    # JSON numbers and booleans keep their type. 7.5 and 11 are both numbers, so doubles.
    assert _rows(m, "SELECT ?s (STR(DATATYPE(?v)) AS ?dt) (?v > 8 AS ?big) WHERE { ?s ex:kw ?v } ORDER BY ?s") == [
        ("<http://example.org/pump/1>", "http://www.w3.org/2001/XMLSchema#double", False),
        ("<http://example.org/pump/2>", "http://www.w3.org/2001/XMLSchema#double", True),
    ]
    assert _rows(m, "SELECT ?s ?v WHERE { ?s ex:running ?v } ORDER BY ?s") == [
        ("<http://example.org/pump/1>", True),
        ("<http://example.org/pump/2>", False),
    ]
    # A key with a space, and a null, which gives no triple.
    assert _rows(m, "SELECT ?v WHERE { ?s ex:serial ?v }") == [("X-1",)]
    assert _rows(m, "SELECT ?v WHERE { ?s ex:spare ?v }") == []
    # A reference with several values gives a triple per value, and none for an empty array.
    assert _rows(m, "SELECT ?s ?v WHERE { ?s ex:tag ?v } ORDER BY ?s ?v") == [
        ("<http://example.org/pump/1>", "a"),
        ("<http://example.org/pump/1>", "b"),
        ("<http://example.org/pump/3>", "c"),
    ]
    # A nested key, and a join between two iterators of the same document.
    assert _rows(m, "SELECT ?s ?l WHERE { ?s ex:site/ex:label ?l } ORDER BY ?s") == [
        ("<http://example.org/pump/1>", "North one"),
        ("<http://example.org/pump/2>", "North two"),
        ("<http://example.org/pump/3>", "North one"),
    ]
    assert len(_rows(m, "SELECT ?s ?v WHERE { ?s ex:siteCode ?v }")) == 3


def test_json_source_from_file_bytes_and_mapping_graph(tmp_path):
    (tmp_path / "plant.json").write_text(PLANT_JSON)
    mapping_path = tmp_path / "mapping.ttl"
    mapping_path.write_text(OLD + JSON_MAPPING)
    expected = Model()
    expected.map_rml(OLD + JSON_MAPPING, sources={"plant.json": PLANT_JSON}, base_iri=EX)

    # The source is found next to the mapping file.
    from_file = Model()
    from_file.map_rml(mapping_path, base_iri=EX)
    assert _triples(from_file) == _triples(expected)

    # As bytes, and as a path, in sources.
    for source in (PLANT_JSON.encode(), tmp_path / "plant.json", str(tmp_path / "plant.json")):
        m = Model()
        m.map_rml(OLD + JSON_MAPPING, sources={"plant.json": source}, base_iri=EX)
        assert _triples(m) == _triples(expected)

    # The mapping in a graph of the model.
    m = Model()
    m.reads(OLD + JSON_MAPPING, format="turtle", graph="urn:graph:rml", base_iri=EX)
    m.map_rml(mapping_graph="urn:graph:rml", sources={"plant.json": PLANT_JSON}, base_iri=EX, graph="urn:graph:data")
    assert _rows(m, "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o", graph="urn:graph:data") == _triples(expected)


def test_json_root_array_and_values_as_records():
    mapping = """
    <#Item> rml:logicalSource [ rml:source "items" ; rml:referenceFormulation ql:JSONPath ; rml:iterator "$[*]" ] ;
        rr:subjectMap [ rr:template "http://example.org/item/{id}" ] ;
        rr:predicateObjectMap [ rr:predicate ex:colour ; rr:objectMap [ rml:reference "colours[*]" ] ] .
    <#Colour> rml:logicalSource [ rml:source "items" ; rml:referenceFormulation ql:JSONPath ;
            rml:iterator "$[*].colours[*]" ] ;
        rr:subjectMap [ rr:template "http://example.org/colour/{$}" ; rr:class ex:Colour ] .
    """
    m = Model()
    m.map_rml(
        OLD + mapping,
        sources={"items": '[{"id": "a", "colours": ["red", "blue"]}, {"id": "b", "colours": ["red"]}]'},
    )
    assert _rows(m, "SELECT ?s ?v WHERE { ?s ex:colour ?v } ORDER BY ?s ?v") == [
        ("<http://example.org/item/a>", "blue"),
        ("<http://example.org/item/a>", "red"),
        ("<http://example.org/item/b>", "red"),
    ]
    assert _rows(m, "SELECT ?s WHERE { ?s a ex:Colour } ORDER BY ?s") == [
        ("<http://example.org/colour/blue>",),
        ("<http://example.org/colour/red>",),
    ]


XML_MAPPING = """
<#Pump> rml:logicalSource [ rml:source "plant.xml" ; rml:referenceFormulation ql:XPath ;
        rml:iterator "/plant/pump" ] ;
    rr:subjectMap [ rr:template "http://example.org/pump/{@id}" ; rr:class ex:Pump ] ;
    rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rml:reference "name" ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:nameText ; rr:objectMap [ rml:reference "name/text()" ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:kind ; rr:objectMap [ rml:reference "@kind" ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:tag ; rr:objectMap [ rml:reference "tag" ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:plant ; rr:objectMap [ rml:reference "../@name" ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:site ; rr:objectMap [ rr:parentTriplesMap <#Site> ;
        rr:joinCondition [ rr:child "site/@code" ; rr:parent "@code" ] ] ] .

<#Site> rml:logicalSource [ rml:source "plant.xml" ; rml:referenceFormulation ql:XPath ;
        rml:iterator "/plant/site" ] ;
    rr:subjectMap [ rr:template "http://example.org/site/{@code}" ] ;
    rr:predicateObjectMap [ rr:predicate ex:label ; rr:objectMap [ rml:reference "label" ] ] .

<#AnyPump> rml:logicalSource [ rml:source "plant.xml" ; rml:referenceFormulation ql:XPath ;
        rml:iterator "//pump[@kind='feed']" ] ;
    rr:subjectMap [ rr:template "http://example.org/pump/{@id}" ; rr:class ex:FeedPump ] .

<#Tagged> rml:logicalSource [ rml:source "plant.xml" ; rml:referenceFormulation ql:XPath ;
        rml:iterator "/plant/pump[tag='b']" ] ;
    rr:subjectMap [ rr:template "http://example.org/pump/{@id}" ; rr:class ex:TaggedB ] .
"""


def test_xml_source():
    m = Model()
    m.map_rml(OLD + XML_MAPPING, sources={"plant.xml": PLANT_XML})
    # /plant/pump selects the children of the root, not the pump in the area.
    assert _rows(m, "SELECT ?s WHERE { ?s a ex:Pump } ORDER BY ?s") == [
        ("<http://example.org/pump/1>",),
        ("<http://example.org/pump/2>",),
    ]
    # The value of an element is its text, with entities resolved; text() gives the same.
    names = [("<http://example.org/pump/1>", "Feed pump"), ("<http://example.org/pump/2>", "Booster & spare")]
    assert _rows(m, "SELECT ?s ?v WHERE { ?s ex:name ?v } ORDER BY ?s") == names
    assert _rows(m, "SELECT ?s ?v WHERE { ?s ex:nameText ?v } ORDER BY ?s") == names
    assert _rows(m, "SELECT ?s ?v WHERE { ?s ex:kind ?v } ORDER BY ?s") == [
        ("<http://example.org/pump/1>", "feed"),
        ("<http://example.org/pump/2>", "booster"),
    ]
    # An element that repeats gives a triple per element.
    assert _rows(m, "SELECT ?s ?v WHERE { ?s ex:tag ?v } ORDER BY ?s ?v") == [
        ("<http://example.org/pump/1>", "a"),
        ("<http://example.org/pump/1>", "b"),
    ]
    # The parent, and a join on an attribute of a child.
    assert _rows(m, "SELECT DISTINCT ?v WHERE { ?s ex:plant ?v }") == [("North",)]
    assert _rows(m, "SELECT ?s ?l WHERE { ?s ex:site/ex:label ?l } ORDER BY ?s") == [
        ("<http://example.org/pump/1>", "North one"),
        ("<http://example.org/pump/2>", "North two"),
    ]
    # // selects at any depth, with a predicate on an attribute.
    assert _rows(m, "SELECT ?s WHERE { ?s a ex:FeedPump } ORDER BY ?s") == [
        ("<http://example.org/pump/1>",),
        ("<http://example.org/pump/3>",),
    ]
    # A predicate on the text of a child element.
    assert _rows(m, "SELECT ?s WHERE { ?s a ex:TaggedB }") == [("<http://example.org/pump/1>",)]


def test_csv_sources(tmp_path):
    (tmp_path / "pumps.csv").write_text("id,name,site,kw\n1,Feed pump,N1,7.5\n2,Booster,N2,\n")
    mapping = """
    <#Pump> rml:logicalSource [ rml:source "pumps.csv" ; rml:referenceFormulation ql:CSV ] ;
        rr:subjectMap [ rr:template "http://example.org/pump/{id}" ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rml:reference "name" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:kw ; rr:objectMap [ rml:reference "kw" ; rr:datatype xsd:decimal ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:id ; rr:objectMap [ rml:reference "id" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:site ; rr:objectMap [ rr:parentTriplesMap <#Site> ;
            rr:joinCondition [ rr:child "site" ; rr:parent "code" ] ] ] .
    <#Site> rml:logicalSource [ rml:source "sites" ; rml:referenceFormulation ql:CSV ] ;
        rr:subjectMap [ rr:template "http://example.org/site/{code}" ] ;
        rr:predicateObjectMap [ rr:predicate ex:label ; rr:objectMap [ rml:reference "label" ] ] .
    """
    mapping_path = tmp_path / "mapping.ttl"
    mapping_path.write_text(OLD + mapping)
    m = Model()
    # One source is a file next to the mapping, the other a DataFrame.
    sites = pl.DataFrame({"code": ["N1", "N2"], "label": ["North one", "North two"]})
    m.map_rml(mapping_path, sources={"sites": sites})
    # The values of a CSV file are strings, unless the mapping gives a datatype.
    assert _rows(m, "SELECT ?s ?v (STR(DATATYPE(?v)) AS ?dt) WHERE { ?s ex:id ?v } ORDER BY ?s") == [
        ("<http://example.org/pump/1>", "1", "http://www.w3.org/2001/XMLSchema#string"),
        ("<http://example.org/pump/2>", "2", "http://www.w3.org/2001/XMLSchema#string"),
    ]
    assert _rows(m, "SELECT ?s (STR(?v) AS ?x) (STR(DATATYPE(?v)) AS ?dt) WHERE { ?s ex:kw ?v }") == [
        ("<http://example.org/pump/1>", "7.5", "http://www.w3.org/2001/XMLSchema#decimal"),
    ]
    assert _rows(m, "SELECT ?s ?l WHERE { ?s ex:site/ex:label ?l } ORDER BY ?s") == [
        ("<http://example.org/pump/1>", "North one"),
        ("<http://example.org/pump/2>", "North two"),
    ]
    # CSV text given in sources.
    m2 = Model()
    m2.map_rml(
        mapping_path,
        sources={"sites": b"code,label\nN1,North one\nN2,North two\n"},
    )
    assert _triples(m2) == _triples(m)


def test_community_group_vocabulary():
    """All terms in the http://w3id.org/rml/ namespace, with a source description."""
    mapping = """
    <#Pump> a rml:TriplesMap ;
        rml:logicalSource [ rml:source [ a rml:RelativePathSource ; rml:path "plant.json" ] ;
            rml:referenceFormulation rml:JSONPath ; rml:iterator "$.pumps[*]" ] ;
        rml:subjectMap [ rml:template "http://example.org/pump/{id}" ; rml:class ex:Pump ;
            rml:graph ex:pumps ] ;
        rml:predicateObjectMap [ rml:predicate ex:name ;
            rml:objectMap [ rml:reference "name" ; rml:datatype xsd:token ] ] ;
        rml:predicateObjectMap [ rml:predicate ex:site ; rml:objectMap [
            rml:template "http://example.org/site/{site.code}" ; rml:termType rml:IRI ] ] .
    """
    m = Model()
    m.map_rml(NEW + mapping, sources={"plant.json": PLANT_JSON})
    assert _rows(m, "SELECT ?s WHERE { GRAPH ex:pumps { ?s a ex:Pump } } ORDER BY ?s") == [
        ("<http://example.org/pump/1>",),
        ("<http://example.org/pump/2>",),
        ("<http://example.org/pump/3>",),
    ]
    assert _rows(m, "SELECT (STR(DATATYPE(?v)) AS ?dt) WHERE { GRAPH ex:pumps { ?s ex:name ?v } } LIMIT 1") == [
        ("http://www.w3.org/2001/XMLSchema#token",)
    ]
    assert _rows(m, "SELECT DISTINCT ?o WHERE { GRAPH ex:pumps { ?s ex:site ?o } } ORDER BY ?o") == [
        ("<http://example.org/site/N1>",),
        ("<http://example.org/site/N2>",),
    ]


def test_sql_table_and_json_source_in_one_mapping():
    mapping = """
    <#Pump> rml:logicalSource [ rml:source "plant.json" ; rml:referenceFormulation ql:JSONPath ;
            rml:iterator "$.pumps[*]" ] ;
        rr:subjectMap [ rr:template "http://example.org/pump/{id}" ] ;
        rr:predicateObjectMap [ rr:predicate ex:owner ; rr:objectMap [ rr:parentTriplesMap <#Owner> ;
            rr:joinCondition [ rr:child "id" ; rr:parent "pump" ] ] ] .
    <#Owner> rr:logicalTable [ rr:tableName "owners" ] ;
        rr:subjectMap [ rr:template "http://example.org/owner/{name}" ] .
    """
    owners = pl.DataFrame({"pump": [1, 3], "name": ["ann", "bo"]})
    m = Model()
    m.map_rml(OLD + mapping, sources={"plant.json": PLANT_JSON}, tables={"owners": owners})
    assert _rows(m, "SELECT ?s ?o WHERE { ?s ex:owner ?o } ORDER BY ?s") == [
        ("<http://example.org/pump/1>", "<http://example.org/owner/ann>"),
        ("<http://example.org/pump/3>", "<http://example.org/owner/bo>"),
    ]


def _json_iterator(iterator: str, reference: str = "id") -> str:
    return OLD + f"""
    <#M> rml:logicalSource [ rml:source "plant.json" ; rml:referenceFormulation ql:JSONPath ;
            rml:iterator "{iterator}" ] ;
        rr:subjectMap [ rr:template "http://example.org/x/{{{reference}}}" ] .
    """


def _xml_iterator(iterator: str, reference: str = "@id") -> str:
    return OLD + f"""
    <#M> rml:logicalSource [ rml:source "plant.xml" ; rml:referenceFormulation ql:XPath ;
            rml:iterator "{iterator}" ] ;
        rr:subjectMap [ rr:template "http://example.org/x/{{{reference}}}" ] .
    """


@pytest.mark.parametrize(
    "mapping,source,message",
    [
        (_json_iterator("$..pumps"), "plant.json", "recursive descent"),
        (_json_iterator("$.pumps[0]"), "plant.json", "array index"),
        (_json_iterator("$.pumps[?(@.id > 1)]"), "plant.json", "filter"),
        (_json_iterator("$.*"), "plant.json", "wildcard over keys"),
        (_json_iterator("$.pumps[*]", "tags[1]"), "plant.json", "array index"),
        (_xml_iterator("/plant/pump[1]"), "plant.xml", "position"),
        (_xml_iterator("/p:plant/p:pump"), "plant.xml", "namespaces are not supported"),
        (_xml_iterator("/plant/pump", "count(tag)"), "plant.xml", "function"),
        (_xml_iterator("/plant/pump[@id > 1]"), "plant.xml", "could not be parsed"),
        (_xml_iterator("plant/pump"), "plant.xml", "must start with /"),
    ],
)
def test_unsupported_paths_raise(mapping, source, message):
    m = Model()
    data = PLANT_JSON if source.endswith("json") else PLANT_XML
    with pytest.raises(Exception, match=message):
        m.map_rml(mapping, sources={source: data})
    assert m.query("SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }")["c"].to_list() == [0]


def test_argument_errors():
    m = Model()
    # A source that is not given and is not a file.
    with pytest.raises(Exception, match="not in sources"):
        m.map_rml(OLD + JSON_MAPPING)
    # An RML mapping given to map_r2rml.
    with pytest.raises(Exception, match="map_rml"):
        m.map_r2rml(OLD + JSON_MAPPING)
    # An XML document for a JSONPath source.
    with pytest.raises(Exception):
        m.map_rml(OLD + JSON_MAPPING, sources={"plant.json": PLANT_XML})
    # A reference that is not a column of a CSV source.
    mapping = """
    <#M> rml:logicalSource [ rml:source "t" ; rml:referenceFormulation ql:CSV ] ;
        rr:subjectMap [ rr:template "http://example.org/x/{nope}" ] .
    """
    with pytest.raises(Exception, match="nope"):
        m.map_rml(OLD + mapping, sources={"t": pl.DataFrame({"id": ["1"]})})


def test_map_r2rml_still_maps_tables():
    """map_rml also maps a plain R2RML mapping."""
    mapping = """@prefix rr: <http://www.w3.org/ns/r2rml#> . @prefix ex: <http://example.org/> .
    <#T> rr:logicalTable [ rr:tableName "t" ] ;
        rr:subjectMap [ rr:template "http://example.org/t/{id}" ; rr:class ex:T ] ."""
    t = pl.DataFrame({"id": [1, 2]})
    a, b = Model(), Model()
    a.map_r2rml(mapping, tables={"t": t})
    b.map_rml(mapping, tables={"t": t})
    assert _triples(a) == _triples(b) and len(_triples(a)) == 2

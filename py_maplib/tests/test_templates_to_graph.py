import polars as pl
from maplib import Model

MTPL = "https://datatreehouse.github.io/maplib/vocab#"
GRAPH = "https://example.org/templates"

PERSON = """
@prefix ex: <http://example.net/ns#> .
@prefix ottr: <http://ns.ottr.xyz/0.4/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:Person [ ottr:IRI ?p, xsd:string ?name ] :: {
    ottr:Triple(?p, rdf:type, ex:Person) ,
    ottr:Triple(?p, ex:hasName, ?name)
} .
"""

WIDGET = """
@prefix ex: <http://example.net/ns#> .
@prefix ottr: <http://ns.ottr.xyz/0.4/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:Widget [ ottr:IRI ?w, xsd:string ?label, ? xsd:integer ?weight, List<xsd:string> ?tags ] :: {
    ottr:Triple(?w, rdf:type, ex:Widget) ,
    ottr:Triple(?w, ex:label, ?label) ,
    ottr:Triple(?w, ex:weight, ?weight) ,
    cross | ottr:Triple(?w, ex:tag, ++ ?tags)
} .
"""


def test_get_templates_excludes_ottr_triple():
    m = Model()
    m.add_template(PERSON)
    templates = m.get_templates()
    assert len(templates) == 1
    assert templates[0].iri.iri == "http://example.net/ns#Person"


def test_templates_to_graph_emits_vocab():
    m = Model()
    m.add_template(PERSON)
    m.templates_to_graph(GRAPH)

    types = m.query(
        f"""
        PREFIX mtpl: <{MTPL}>
        SELECT ?t WHERE {{
            GRAPH <{GRAPH}> {{ ?t a mtpl:Template }}
        }}
        """
    )
    assert types.height == 1
    assert "Person" in types["t"][0]


def test_templates_to_graph_references_iri():
    m = Model()
    m.add_template(PERSON)
    m.templates_to_graph(GRAPH)

    refs = m.query(
        f"""
        PREFIX mtpl: <{MTPL}>
        PREFIX ex: <http://example.net/ns#>
        SELECT ?iri WHERE {{
            GRAPH <{GRAPH}> {{ ex:Person mtpl:referencesIri ?iri }}
        }}
        ORDER BY ?iri
        """
    )
    iris = [v for v in refs["iri"]]
    assert "<http://example.net/ns#hasName>" in iris
    assert "<http://example.net/ns#Person>" not in iris


def test_templates_to_graph_uses_predicate():
    m = Model()
    m.add_template(PERSON)
    m.templates_to_graph(GRAPH)

    preds = m.query(
        f"""
        PREFIX mtpl: <{MTPL}>
        PREFIX ex: <http://example.net/ns#>
        SELECT ?p WHERE {{
            GRAPH <{GRAPH}> {{ ex:Person mtpl:usesPredicate ?p }}
        }}
        ORDER BY ?p
        """
    )
    vals = [v for v in preds["p"]]
    assert "<http://example.net/ns#hasName>" in vals
    assert "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" in vals


def test_templates_to_graph_added_alongside_data():
    m = Model()
    m.add_template(PERSON)

    df = pl.DataFrame(
        {
            "p": ["http://example.net/ns#alice"],
            "name": ["Alice"],
        }
    )
    m.map("ex:Person", df)
    m.templates_to_graph(GRAPH)

    data = m.query(
        """
        PREFIX ex: <http://example.net/ns#>
        SELECT ?name WHERE { ?p ex:hasName ?name }
        """
    )
    assert data.height == 1
    assert data["name"][0] == "Alice"

    meta = m.query(
        f"""
        PREFIX mtpl: <{MTPL}>
        SELECT ?t WHERE {{ GRAPH <{GRAPH}> {{ ?t a mtpl:Template }} }}
        """
    )
    assert meta.height == 1


def test_infer_nodeshape_from_template_graph():
    m = Model()
    m.add_template(WIDGET)
    m.templates_to_graph(GRAPH)

    shapes_graph = "https://example.org/shapes"
    m.insert(
        f"""
        PREFIX mtpl: <{MTPL}>
        PREFIX sh: <http://www.w3.org/ns/shacl#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        CONSTRUCT {{
            ?template a sh:NodeShape ;
                sh:targetClass ?class ;
                sh:property _:prop .
            _:prop sh:path ?p ;
                sh:datatype ?datatype ;
                sh:minCount ?minCount ;
                sh:maxCount ?maxCount .
        }} WHERE {{
            GRAPH <{GRAPH}> {{
                ?template a mtpl:Template ;
                          mtpl:hasInstance ?typeInst , ?inst ;
                          mtpl:hasParameter ?param .
                ?typeInst mtpl:hasArgument ?tpArg , ?tcArg .
                ?tpArg mtpl:index 1 ; mtpl:constantValue rdf:type .
                ?tcArg mtpl:index 2 ; mtpl:constantValue ?class .
                ?inst mtpl:hasArgument ?predArg , ?objArg .
                ?predArg mtpl:index 1 ; mtpl:constantValue ?p .
                FILTER(?p != rdf:type)
                ?objArg mtpl:index 2 ; mtpl:variableName ?var .
                ?param mtpl:variableName ?var ;
                       mtpl:type ?datatype ;
                       mtpl:cardinality ?card ;
                       mtpl:optional ?optional .
                BIND(IF(?optional || ?card = "list", 0, 1) AS ?minCount)
                OPTIONAL {{
                    FILTER(?card = "single")
                    BIND(1 AS ?maxCount)
                }}
            }}
        }}
        """,
        target_graph=shapes_graph,
    )

    shapes = m.query(
        f"""
        PREFIX sh: <http://www.w3.org/ns/shacl#>
        SELECT ?target ?path ?datatype ?minCount ?maxCount WHERE {{
            GRAPH <{shapes_graph}> {{
                ?shape a sh:NodeShape ;
                       sh:targetClass ?target ;
                       sh:property ?prop .
                ?prop sh:path ?path ;
                      sh:datatype ?datatype ;
                      sh:minCount ?minCount .
                OPTIONAL {{ ?prop sh:maxCount ?maxCount }}
            }}
        }}
        ORDER BY ?path
        """
    )
    assert shapes.height == 3
    assert shapes["target"].to_list() == ["<http://example.net/ns#Widget>"] * 3

    xsd_string = "<http://www.w3.org/2001/XMLSchema#string>"
    xsd_integer = "<http://www.w3.org/2001/XMLSchema#integer>"
    rows = {r["path"]: r for r in shapes.iter_rows(named=True)}

    label = rows["<http://example.net/ns#label>"]
    assert label["datatype"] == xsd_string
    assert label["minCount"] == 1
    assert label["maxCount"] == 1

    weight = rows["<http://example.net/ns#weight>"]
    assert weight["datatype"] == xsd_integer
    assert weight["minCount"] == 0
    assert weight["maxCount"] == 1

    tag = rows["<http://example.net/ns#tag>"]
    assert tag["datatype"] == xsd_string
    assert tag["minCount"] == 0
    assert tag["maxCount"] is None

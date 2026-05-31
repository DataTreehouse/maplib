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


def test_get_templates_excludes_ottr_triple():
    m = Model()
    m.add_template(PERSON)
    templates = m.get_templates()
    # Only the user's template, not the built-in ottr:Triple.
    assert len(templates) == 1
    assert templates[0].iri.iri == "http://example.net/ns#Person"


def test_templates_to_graph_emits_vocab():
    m = Model()
    m.add_template(PERSON)
    m.templates_to_graph(GRAPH)

    # The template is typed mtpl:Template in the named graph.
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
    # Self is excluded.
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

    # Mapped data remains in the default graph.
    data = m.query(
        """
        PREFIX ex: <http://example.net/ns#>
        SELECT ?name WHERE { ?p ex:hasName ?name }
        """
    )
    assert data.height == 1
    assert data["name"][0] == "Alice"

    # Template metadata is queryable in its own named graph.
    meta = m.query(
        f"""
        PREFIX mtpl: <{MTPL}>
        SELECT ?t WHERE {{ GRAPH <{GRAPH}> {{ ?t a mtpl:Template }} }}
        """
    )
    assert meta.height == 1


def test_infer_nodeshape_from_data():
    """Minimal SHACL shape mining: a CONSTRUCT that infers a sh:NodeShape (with a
    sh:targetClass and sh:property path per predicate) from the materialized instance data."""
    m = Model()
    m.add_template(PERSON)
    m.map(
        "ex:Person",
        pl.DataFrame(
            {"p": ["http://example.net/ns#alice"], "name": ["Alice"]}
        ),
    )

    shapes_graph = "https://example.org/shapes"
    m.insert(
        """
        PREFIX sh: <http://www.w3.org/ns/shacl#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        CONSTRUCT {
            ?class a sh:NodeShape ;
                sh:targetClass ?class ;
                sh:property [ sh:path ?p ] .
        } WHERE {
            ?s a ?class ;
               ?p ?o .
            FILTER(?p != rdf:type)
        }
        """,
        target_graph=shapes_graph,
    )

    shapes = m.query(
        f"""
        PREFIX sh: <http://www.w3.org/ns/shacl#>
        SELECT ?shape ?path WHERE {{
            GRAPH <{shapes_graph}> {{
                ?shape a sh:NodeShape ;
                       sh:targetClass ?target ;
                       sh:property [ sh:path ?path ] .
            }}
        }}
        """
    )
    assert shapes.height == 1
    assert shapes["shape"][0] == "<http://example.net/ns#Person>"
    assert shapes["path"][0] == "<http://example.net/ns#hasName>"

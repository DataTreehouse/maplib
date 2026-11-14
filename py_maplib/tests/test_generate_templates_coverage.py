"""Coverage for maplib.generate_templates (template generation from an ontology)."""

import polars as pl
import pytest

from maplib import Model, generate_templates

EX = "http://example.net/ns#"

ONTOLOGY = f"""
@prefix ex: <{EX}> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:Animal a owl:Class .
ex:Dog a owl:Class ; rdfs:subClassOf ex:Animal .
ex:Person a owl:Class .

ex:name a owl:DatatypeProperty ; rdfs:domain ex:Animal ; rdfs:range xsd:string .
ex:barkVolume a owl:DatatypeProperty ; rdfs:domain ex:Dog ; rdfs:range xsd:integer .
ex:owner a owl:ObjectProperty ; rdfs:domain ex:Dog ; rdfs:range ex:Person .
"""

ONT_GRAPH = "urn:graph:ontology"


@pytest.fixture(scope="module")
def templates():
    m = Model()
    m.reads(ONTOLOGY, format="turtle", graph=ONT_GRAPH)
    return generate_templates(m, ONT_GRAPH)


def test_keys_include_typed_and_untyped(templates):
    for c in ["Animal", "Dog"]:
        assert EX + c in templates
        assert EX + c + "_notype" in templates


def test_typed_template_iris(templates):
    assert templates[EX + "Dog"].iri.iri == EX + "Dog"
    assert templates[EX + "Dog_notype"].iri.iri == EX + "Dog_notype"


def test_parameters(templates):
    dog_params = {p.variable.name: p for p in templates[EX + "Dog"].parameters}
    assert dog_params["id"].optional is False
    for name in ["barkVolume", "owner", "name"]:
        assert name in dog_params
        assert dog_params[name].optional is True

    animal_params = {p.variable.name for p in templates[EX + "Animal"].parameters}
    assert animal_params == {"id", "name"}


def test_map_generated_templates():
    ont = Model()
    ont.reads(ONTOLOGY, format="turtle", graph=ONT_GRAPH)
    templates = generate_templates(ont, ONT_GRAPH)

    m = Model()
    for t in templates.values():
        m.add_template(t)
    m.map(
        EX + "Dog",
        pl.DataFrame(
            {
                "id": [EX + "rex", EX + "fido"],
                "name": ["Rex", "Fido"],
                "barkVolume": [10, None],
                "owner": [EX + "alice", None],
            }
        ),
    )
    df = m.query(
        f"""
        PREFIX ex: <{EX}>
        SELECT ?d ?n WHERE {{ ?d a ex:Dog ; ex:name ?n }} ORDER BY ?n
        """
    )
    assert df["n"].to_list() == ["Fido", "Rex"]
    df = m.query(
        f"PREFIX ex: <{EX}> SELECT ?d ?o WHERE {{ ?d ex:owner ?o }}"
    )
    assert df["d"].to_list() == [f"<{EX}rex>"]
    assert df["o"].to_list() == [f"<{EX}alice>"]
    df = m.query(
        f"PREFIX ex: <{EX}> SELECT ?d ?v WHERE {{ ?d ex:barkVolume ?v }}"
    )
    assert df["v"].to_list() == [10]


def test_generate_templates_empty_ontology():
    m = Model()
    assert generate_templates(m, None) == {}

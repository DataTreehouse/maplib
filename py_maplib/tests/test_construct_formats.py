"""CONSTRUCT results as Turtle and TriG strings, written with maplib's RDF writers.

Each result is parsed and compared, as an RDF graph, with what pyoxigraph constructs from the
same data, including language tags, datatypes, time zones, blank nodes, escapes and objects of
many types for one predicate.
"""

import pyoxigraph as ox
import pytest

from maplib import Model

DATA = r"""@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:a ex:name "Alice" ;
  ex:label "Alice"@en , "Alicia"@es-ES ;
  ex:age 31 ;
  ex:height 1.75 ;
  ex:weight "65.5"^^xsd:double ;
  ex:ok true ;
  ex:born "1990-05-01"^^xsd:date ;
  ex:seen "2024-01-02T03:04:05Z"^^xsd:dateTime , "2024-01-02T03:04:05+02:00"^^xsd:dateTime ,
          "2024-01-02T03:04:05"^^xsd:dateTime ;
  ex:note "quote \" backslash \\ newline \n tab \t cr \r unicode é ☃ 𝄞" ;
  ex:custom "x y"^^ex:dt ;
  ex:knows ex:b , _:anon .
_:anon ex:name "Anon" ; ex:knows ex:a .
ex:b ex:name "Bob" ; ex:x ex:a .
ex:c ex:mixed ex:a , 1 , "s" , "s"@en , _:anon , 2.5 , "2024-01-02"^^xsd:date .
"""

PREFIX = "PREFIX ex: <http://example.org/>\n"

QUERIES = {
    # Only codes of the model's dictionary: written without decoding.
    "all": "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
    "constant_predicate": "CONSTRUCT { ?s ex:label2 ?o } WHERE { ?s ex:label ?o }",
    "mixed_objects": "CONSTRUCT { ?s ?p ?o } WHERE { ?s ex:mixed ?o . ?s ?p ?o }",
    # A predicate and subjects that the model does not have, and blank nodes of the template.
    "new_iris": (
        "CONSTRUCT { ?s2 ex:newPredicate ?o } WHERE { ?s ?p ?o FILTER(isIRI(?s)) "
        'BIND(IRI(CONCAT(STR(?s), "/copy")) AS ?s2) }'
    ),
    "template_blank_nodes": "CONSTRUCT { _:x ex:of ?s ; ex:hasName ?n } WHERE { ?s ex:name ?n }",
    "duplicates": "CONSTRUCT { ?s a ex:Thing } WHERE { ?s ?p ?o }",
    "empty": "CONSTRUCT { ?s ?p ?o } WHERE { ?s ex:nothing ?o . ?s ?p ?o }",
}


def canonical(quads) -> set:
    d = ox.Dataset(quads)
    d.canonicalize(ox.CanonicalizationAlgorithm.UNSTABLE)
    return {str(q) for q in d}


def expected(query: str, graph=ox.DefaultGraph()) -> set:
    store = ox.Store()
    store.load(DATA, format=ox.RdfFormat.TURTLE)
    return canonical(
        ox.Quad(t.subject, t.predicate, t.object, graph) for t in store.query(PREFIX + query)
    )


def parsed(s: str, format) -> set:
    return canonical(ox.parse(s, format=format))


@pytest.fixture(scope="module")
def model() -> Model:
    m = Model()
    m.reads(DATA, format="turtle")
    return m


@pytest.mark.parametrize("name", list(QUERIES))
def test_turtle_and_trig_give_the_constructed_graph(model, name):
    query = QUERIES[name]
    want = expected(query)
    if name != "empty":
        assert want
    ttl = model.query(PREFIX + query, return_format="turtle")
    assert parsed(ttl, ox.RdfFormat.TURTLE) == want, ttl
    trig = model.query(PREFIX + query, return_format="trig")
    assert parsed(trig, ox.RdfFormat.TRIG) == want, trig


def test_duplicate_triples_are_written_once(model):
    ttl = model.query(PREFIX + QUERIES["duplicates"], return_format="turtle")
    # ex:a, ex:b, ex:c and the blank node
    assert len(list(ox.parse(ttl, format=ox.RdfFormat.TURTLE))) == 4


def test_trig_keeps_the_graphs_of_the_template(model):
    query = "CONSTRUCT { GRAPH ex:g { ?s ?p ?o } } WHERE { ?s ?p ?o }"
    want = expected(
        "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }", ox.NamedNode("http://example.org/g")
    )
    trig = model.query(PREFIX + query, return_format="trig")
    assert parsed(trig, ox.RdfFormat.TRIG) == want, trig
    with pytest.raises(Exception, match="trig"):
        model.query(PREFIX + query, return_format="turtle")


def test_turtle_uses_the_prefixes_of_the_model():
    m = Model()
    m.reads(DATA, format="turtle")
    m.add_prefixes({"ex": "http://example.org/"})
    ttl = m.query(PREFIX + QUERIES["all"], return_format="turtle")
    assert "@prefix ex: <http://example.org/>" in ttl
    assert "ex:a" in ttl
    assert parsed(ttl, ox.RdfFormat.TURTLE) == expected(QUERIES["all"])


def test_on_disk_model(tmp_path):
    m = Model(storage="disk", storage_folder=str(tmp_path / "disk"))
    m.reads(DATA, format="turtle")
    for name in ["all", "new_iris"]:
        ttl = m.query(PREFIX + QUERIES[name], return_format="turtle")
        assert parsed(ttl, ox.RdfFormat.TURTLE) == expected(QUERIES[name]), ttl
    # Writing a result does not add to the model.
    before = m.writes(format="ntriples")
    m.query(PREFIX + QUERIES["new_iris"], return_format="turtle")
    assert m.writes(format="ntriples") == before

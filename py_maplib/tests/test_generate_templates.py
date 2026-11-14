import warnings

import polars as pl
import pytest

from maplib import Model
from maplib import generate_templates as _generate_templates

EX = "http://ex.org/"
XSD = "http://www.w3.org/2001/XMLSchema#"
PREFIXES = {"ex": EX, "ex2": "http://ex2.org/"}


def generate_templates(m, graph=None, prefixes=PREFIXES):
    return _generate_templates(m, graph=graph, prefixes=prefixes)

BASE = """
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://ex.org/> .
@prefix ex2: <http://ex2.org/> .
ex:A a owl:Class .
ex:B a owl:Class ; rdfs:subClassOf ex:A .
ex:name a owl:DatatypeProperty ; rdfs:domain ex:A ; rdfs:range xsd:string .
ex:hasB a owl:ObjectProperty ; rdfs:domain ex:A ; rdfs:range ex:B .
"""


def ontology(extra: str = "") -> Model:
    m = Model()
    m.reads(BASE + extra, format="turtle")
    return m


def param_names(t) -> list:
    return [p.variable.name for p in t.parameters]


def populate(templates, template_iri: str, df: pl.DataFrame) -> Model:
    m = Model()
    for t in templates.values():
        m.add_template(t)
    m.map(template_iri, df)
    return m


def triples(m: Model) -> set:
    df = m.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
    return set(df.iter_rows())


def test_graph_defaults_to_none():
    templates = generate_templates(ontology())
    assert set(templates) == {
        EX + "A",
        EX + "A_notype",
        EX + "B",
        EX + "B_notype",
    }


def test_rejects_non_model():
    with pytest.raises(TypeError):
        generate_templates("not a model")


def test_empty_model():
    assert generate_templates(Model()) == {}


def test_class_without_hierarchy_gets_template():
    templates = generate_templates(ontology("ex:Lonely a owl:Class ."))
    assert param_names(templates[EX + "Lonely"]) == ["id"]
    m = populate(templates, EX + "Lonely", pl.DataFrame({"id": [EX + "l1"]}))
    assert triples(m) == {
        (f"<{EX}l1>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", f"<{EX}Lonely>")
    }


def test_domain_only_class_gets_template():
    templates = generate_templates(
        ontology("ex:age a owl:DatatypeProperty ; rdfs:domain ex:Person ; rdfs:range xsd:int .")
    )
    assert param_names(templates[EX + "Person"]) == ["id", "ex:age"]


def test_inheritance_end_to_end():
    templates = generate_templates(ontology())
    assert param_names(templates[EX + "B"]) == ["id", "ex:hasB", "ex:name", "ex:hasB_inverse"]
    df = pl.DataFrame(
        {
            "id": [EX + "b1"],
            "ex:hasB": [EX + "b2"],
            "ex:name": ["Bob"],
            "ex:hasB_inverse": [EX + "a1"],
        }
    )
    m = populate(templates, EX + "B", df)
    assert triples(m) == {
        (f"<{EX}b1>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", f"<{EX}B>"),
        (f"<{EX}b1>", f"<{EX}hasB>", f"<{EX}b2>"),
        (f"<{EX}b1>", f"<{EX}name>", '"Bob"'),
        (f"<{EX}a1>", f"<{EX}hasB>", f"<{EX}b1>"),
    }


@pytest.mark.parametrize("extra", ["ex:A rdfs:subClassOf ex:B .", "ex:A rdfs:subClassOf ex:A ."])
def test_subclass_cycles_do_not_crash(extra):
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        templates = generate_templates(ontology(extra))
    m = Model()
    for t in templates.values():
        m.add_template(t)
    assert EX + "A" in templates and EX + "B" in templates


def test_cycle_warns():
    with pytest.warns(UserWarning, match="Cyclic"):
        generate_templates(ontology("ex:A rdfs:subClassOf ex:B ."))


def test_same_local_name_does_not_shadow_inherited_property():
    templates = generate_templates(
        ontology("ex2:name a owl:DatatypeProperty ; rdfs:domain ex:B ; rdfs:range xsd:int .")
    )
    b = templates[EX + "B"]
    names = param_names(b)
    assert len(names) == len(set(names))
    assert "ex:name" in names and "ex2:name" in names
    df = pl.DataFrame({"id": [EX + "b1"], "ex2:name": pl.Series([3], dtype=pl.Int32), "ex:name": ["Bob"]})
    m = populate(templates, EX + "B", df)
    t = triples(m)
    assert (f"<{EX}b1>", "<http://ex2.org/name>", '"3"^^<http://www.w3.org/2001/XMLSchema#int>') in t
    assert (f"<{EX}b1>", f"<{EX}name>", '"Bob"') in t


def test_diamond_inheritance_has_no_duplicate_parameters_or_triples():
    templates = generate_templates(
        ontology("ex:C rdfs:subClassOf ex:A . ex:D rdfs:subClassOf ex:B, ex:C .")
    )
    names = param_names(templates[EX + "D"])
    assert len(names) == len(set(names))
    df = pl.DataFrame({"id": [EX + "d1"], "ex:name": ["Dee"]})
    m = populate(templates, EX + "D", df)
    res = m.query(
        "SELECT ?o WHERE { <http://ex.org/d1> <http://ex.org/name> ?o }"
    )
    assert res.height == 1


def test_redundant_superclass_is_pruned():
    templates = generate_templates(ontology("ex:D rdfs:subClassOf ex:B, ex:A ."))
    d = str(templates[EX + "D_notype"])
    assert "B_notype(" in d
    assert "A_notype(" not in d


def test_property_named_id_does_not_clobber_subject():
    templates = generate_templates(
        ontology("ex:id a owl:DatatypeProperty ; rdfs:domain ex:B ; rdfs:range xsd:string .")
    )
    names = param_names(templates[EX + "B"])
    assert names.count("id") == 1
    assert "ex:id" in names
    df = pl.DataFrame({"id": [EX + "b1"], "ex:id": ["B-1"], "ex:name": ["Bob"]})
    t = triples(populate(templates, EX + "B", df))
    assert (f"<{EX}b1>", f"<{EX}id>", '"B-1"') in t
    assert (f"<{EX}b1>", f"<{EX}name>", '"Bob"') in t


def test_class_range_with_rdf_property_is_iri():
    templates = generate_templates(
        ontology("ex:q a rdf:Property ; rdfs:domain ex:B ; rdfs:range ex:A .")
    )
    s = str(templates[EX + "B"])
    assert "ottr:IRI ?ex:q" in s


def test_conflicting_ranges_are_untyped():
    templates = generate_templates(
        ontology("ex:q a owl:DatatypeProperty ; rdfs:domain ex:B ; rdfs:range xsd:int, xsd:string .")
    )
    q = [p for p in templates[EX + "B"].parameters if p.variable.name == "ex:q"][0]
    assert q.rdf_type is None


def test_union_domain():
    templates = generate_templates(
        ontology(
            "ex:label a owl:DatatypeProperty ; rdfs:domain [ owl:unionOf (ex:X ex:Y) ] ; rdfs:range xsd:string ."
        )
    )
    assert "ex:label" in param_names(templates[EX + "X"])
    assert "ex:label" in param_names(templates[EX + "Y"])


def test_restriction_with_class_filler_is_iri():
    templates = generate_templates(
        ontology(
            "ex:Z rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:p ; owl:someValuesFrom ex:A ] ."
        )
    )
    assert "ottr:IRI ?ex:p" in str(templates[EX + "Z"])


def test_odd_local_names_are_sanitized():
    templates = generate_templates(
        ontology(
            """
            <http://ex.org/has-part.v2> a owl:ObjectProperty ; rdfs:domain ex:B ; rdfs:range ex:A .
            <http://ex.org/onto/> a owl:DatatypeProperty ; rdfs:domain ex:B ; rdfs:range xsd:string .
            <http://ex.org/høyde> a owl:DatatypeProperty ; rdfs:domain ex:B ; rdfs:range xsd:double .
            """
        )
    )
    names = param_names(templates[EX + "B"])
    assert "ex:has-part.v2" in names
    assert "ex:onto_" in names
    assert "ex:høyde" in names
    df = pl.DataFrame({"id": [EX + "b1"], "ex:høyde": [1.5]})
    t = triples(populate(templates, EX + "B", df))
    assert (f"<{EX}b1>", f"<{EX}høyde>", '"1.5"^^<http://www.w3.org/2001/XMLSchema#double>') in t


def test_named_graph():
    m = Model()
    m.reads(BASE, format="turtle", graph="urn:graph:ontology")
    assert generate_templates(m) == {}
    templates = generate_templates(m, graph="urn:graph:ontology")
    assert EX + "B" in templates


def test_deterministic():
    extra = "ex:C rdfs:subClassOf ex:A . ex:D rdfs:subClassOf ex:B, ex:C ."
    a = {k: str(v) for k, v in generate_templates(ontology(extra)).items()}
    b = {k: str(v) for k, v in generate_templates(ontology(extra)).items()}
    assert a == b


def test_unknown_namespaces_get_generated_prefixes():
    templates = _generate_templates(ontology())
    assert param_names(templates[EX + "B"]) == ["id", "ns1:hasB", "ns1:name", "ns1:hasB_inverse"]


def test_well_known_prefixes_are_used():
    templates = generate_templates(
        ontology(
            """
            ex:B rdfs:subClassOf [ a owl:Restriction ; owl:onProperty <http://www.w3.org/2004/02/skos/core#prefLabel> ] .
            """
        )
    )
    assert "skos:prefLabel" in param_names(templates[EX + "B"])


def test_invalid_prefix_name():
    with pytest.raises(ValueError):
        _generate_templates(ontology(), prefixes={"1bad": EX})


def test_prefixed_names_round_trip_through_ottr():
    templates = generate_templates(ontology())
    m = Model()
    for t in templates.values():
        m.add_template(str(t))
    df = pl.DataFrame({"id": [EX + "b1"], "ex:name": ["Bob"]})
    m.map(EX + "B", df)
    assert (f"<{EX}b1>", f"<{EX}name>", '"Bob"') in triples(m)

"""Tests for Model.infer(profile=...), the built-in reasoning profiles (OWL 2 RL, RDFS-Plus and RDFS).
Reasoning uses the Datalog engine, which is only available in some builds, so the tests skip when
it is not."""

import pytest

from maplib import Model, TRANSIENT_GRAPH, DEFAULT_GRAPH

from .w3c_entailment import reason_or_skip

PREFIXES = """
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://example.net/ns#> .
@prefix cim: <http://iec.ch/TC57/CIM100#> .
@prefix ua: <http://opcfoundation.org/UA/> .
"""

Q_PREFIXES = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX ex: <http://example.net/ns#>
PREFIX cim: <http://iec.ch/TC57/CIM100#>
PREFIX ua: <http://opcfoundation.org/UA/>
"""


def model(ttl: str) -> Model:
    m = Model()
    m.reads(PREFIXES + ttl, format="turtle")
    return m


def ask(m: Model, pattern: str, **kwargs) -> bool:
    return m.query(f"{Q_PREFIXES} ASK {{ {pattern} }}", **kwargs)


def count(m: Model, pattern: str, **kwargs) -> int:
    return m.query(f"{Q_PREFIXES} SELECT * WHERE {{ {pattern} }}", **kwargs).height


# A CIM style schema: deep class hierarchies and associations with an inverse role per end.
CIM = """
cim:IdentifiedObject a owl:Class .
cim:PowerSystemResource a owl:Class ; rdfs:subClassOf cim:IdentifiedObject .
cim:Equipment a owl:Class ; rdfs:subClassOf cim:PowerSystemResource .
cim:ConductingEquipment a owl:Class ; rdfs:subClassOf cim:Equipment .
cim:Conductor a owl:Class ; rdfs:subClassOf cim:ConductingEquipment .
cim:ACLineSegment a owl:Class ; rdfs:subClassOf cim:Conductor .
cim:ACDCTerminal a owl:Class ; rdfs:subClassOf cim:IdentifiedObject .
cim:Terminal a owl:Class ; rdfs:subClassOf cim:ACDCTerminal .
cim:ConnectivityNode a owl:Class ; rdfs:subClassOf cim:IdentifiedObject .
cim:Substation a owl:Class ; rdfs:subClassOf cim:PowerSystemResource .
cim:VoltageLevel a owl:Class ; rdfs:subClassOf cim:PowerSystemResource .

cim:Terminal.ConductingEquipment a owl:ObjectProperty, owl:FunctionalProperty ;
    rdfs:domain cim:Terminal ; rdfs:range cim:ConductingEquipment ;
    owl:inverseOf cim:ConductingEquipment.Terminals .
cim:ConductingEquipment.Terminals a owl:ObjectProperty ;
    rdfs:domain cim:ConductingEquipment ; rdfs:range cim:Terminal .
cim:Terminal.ConnectivityNode a owl:ObjectProperty ;
    rdfs:domain cim:Terminal ; rdfs:range cim:ConnectivityNode ;
    owl:inverseOf cim:ConnectivityNode.Terminals .
cim:VoltageLevel.Substation a owl:ObjectProperty ;
    rdfs:domain cim:VoltageLevel ; rdfs:range cim:Substation ;
    owl:inverseOf cim:Substation.VoltageLevels .
cim:IdentifiedObject.name a owl:DatatypeProperty ;
    rdfs:domain cim:IdentifiedObject ; rdfs:range xsd:string .

# Part-of and connectivity, as in OWL RL rule sets for grid models.
ex:partOf a owl:TransitiveProperty .
cim:VoltageLevel.Substation rdfs:subPropertyOf ex:partOf .
ex:connectedTo a owl:SymmetricProperty .
ex:connectedTo owl:propertyChainAxiom ( cim:Terminal.ConnectivityNode cim:ConnectivityNode.Terminals ) .
"""

CIM_DATA = """
ex:line1 a cim:ACLineSegment ; cim:IdentifiedObject.name "Line 1" .
ex:t1 cim:Terminal.ConductingEquipment ex:line1 ; cim:Terminal.ConnectivityNode ex:cn1 .
ex:t2 cim:Terminal.ConductingEquipment ex:line1 ; cim:Terminal.ConnectivityNode ex:cn2 .
ex:t3 cim:Terminal.ConnectivityNode ex:cn1 .
ex:vl1 cim:VoltageLevel.Substation ex:sub1 .
ex:sub1 ex:partOf ex:region1 .
"""


def test_cim_subclass_and_inverse_associations():
    m = model(CIM + CIM_DATA)
    res = reason_or_skip(m, "owl-rl")
    assert res.profile == "owl-rl"
    assert res.inferred_triples > 0
    assert res.consistent, res.violations
    for c in ["ACLineSegment", "Conductor", "ConductingEquipment", "Equipment",
              "PowerSystemResource", "IdentifiedObject"]:
        assert ask(m, f"ex:line1 a cim:{c}"), c
    # Inverse associations, and typing of terminals from domains and ranges.
    assert ask(m, "ex:line1 cim:ConductingEquipment.Terminals ex:t1, ex:t2")
    assert ask(m, "ex:cn1 cim:ConnectivityNode.Terminals ex:t1, ex:t3")
    assert ask(m, "ex:t3 a cim:Terminal, cim:ACDCTerminal, cim:IdentifiedObject")
    assert ask(m, "ex:cn1 a cim:ConnectivityNode")
    # Transitive part-of through a subproperty.
    assert ask(m, "ex:vl1 ex:partOf ex:sub1, ex:region1")
    assert ask(m, "ex:sub1 cim:Substation.VoltageLevels ex:vl1")
    # Connectivity through a property chain, and symmetry.
    assert ask(m, "ex:t1 ex:connectedTo ex:t3 . ex:t3 ex:connectedTo ex:t1")
    assert not ask(m, "ex:t1 ex:connectedTo ex:t2")
    # Literals are not typed with datatype ranges.
    assert not ask(m, "?x a <http://www.w3.org/2001/XMLSchema#string>")
    # Everything inferred is in the transient graph.
    assert not ask(m, "ex:line1 a cim:Equipment", include_graphs=[])
    assert m.query(
        f"{Q_PREFIXES} SELECT * WHERE {{ ?s ?p ?o }}", graph=TRANSIENT_GRAPH, include_graphs=[]
    ).height == res.inferred_triples


def test_cim_inferred_triples_are_not_duplicated():
    m = model(CIM + CIM_DATA + "ex:line1 a cim:Equipment . ex:line1 cim:ConductingEquipment.Terminals ex:t1 .")
    reason_or_skip(m, "owl-rl")
    assert not ask(m, "ex:line1 a cim:Equipment", graph=TRANSIENT_GRAPH, include_graphs=[])
    assert not ask(m, "ex:line1 cim:ConductingEquipment.Terminals ex:t1", graph=TRANSIENT_GRAPH, include_graphs=[])
    assert ask(m, "ex:line1 cim:ConductingEquipment.Terminals ex:t2", graph=TRANSIENT_GRAPH, include_graphs=[])
    # Reasoning again infers nothing new.
    res = reason_or_skip(m, "owl-rl")
    assert res.inferred_triples == 0


def test_target_graph():
    m = model(CIM + CIM_DATA)
    res = reason_or_skip(m, "rdfs-plus", target_graph=DEFAULT_GRAPH)
    assert res.inferred_triples > 0
    assert ask(m, "ex:line1 a cim:Equipment", include_graphs=[])
    assert count(m, "?s ?p ?o", graph=TRANSIENT_GRAPH, include_graphs=[]) == 0

    m = model(CIM + CIM_DATA)
    res = reason_or_skip(m, "rdfs-plus", target_graph="urn:graph:inferred")
    assert ask(m, "ex:line1 a cim:Equipment", graph="urn:graph:inferred", include_graphs=[])
    assert count(m, "?s ?p ?o", graph="urn:graph:inferred", include_graphs=[]) == res.inferred_triples


def test_named_graph():
    m = Model()
    m.reads(PREFIXES + CIM + CIM_DATA, format="turtle", graph="urn:graph:cim")
    res = reason_or_skip(m, "owl-rl", graph="urn:graph:cim")
    assert res.inferred_triples > 0
    assert ask(m, "ex:line1 a cim:Equipment", graph="urn:graph:cim")
    # The inferences are in the transient graph, not in the named graph or the default graph.
    assert not ask(m, "ex:line1 a cim:Equipment", graph="urn:graph:cim", include_graphs=[])
    assert not ask(m, "ex:line1 a cim:Equipment", include_graphs=[])
    assert ask(m, "ex:line1 a cim:Equipment", graph=TRANSIENT_GRAPH, include_graphs=[])


# An OPC UA style model: reference types with transitive HasSubtype, and inverse references.
OPCUA = """
ua:HasSubtype a owl:TransitiveProperty ; owl:inverseOf ua:SubtypeOf .
ua:References ua:HasSubtype ua:HierarchicalReferences, ua:NonHierarchicalReferences .
ua:HierarchicalReferences ua:HasSubtype ua:Aggregates, ua:Organizes .
ua:Aggregates ua:HasSubtype ua:HasComponent, ua:HasProperty .
ua:HasComponent ua:HasSubtype ua:HasOrderedComponent .
ua:BaseObjectType ua:HasSubtype ua:DeviceType .
ua:DeviceType ua:HasSubtype ua:PumpType .
ua:PumpType ua:HasSubtype ua:CentrifugalPumpType .

ua:HasComponent rdfs:subPropertyOf ua:Aggregates ; owl:inverseOf ua:ComponentOf .
ua:HasProperty rdfs:subPropertyOf ua:Aggregates .
ua:Aggregates rdfs:subPropertyOf ua:HierarchicalReferences .
ua:HierarchicalReferences a owl:TransitiveProperty .

ex:Pump1 ua:HasComponent ex:Motor1 .
ex:Motor1 ua:HasComponent ex:Bearing1 .
ex:Pump1 ua:HasProperty ex:FlowRate .
"""


def test_opcua_transitive_subtypes_and_inverse_references():
    m = model(OPCUA)
    res = reason_or_skip(m, "rdfs-plus")
    assert res.consistent
    assert ask(m, "ua:References ua:HasSubtype ua:HasOrderedComponent")
    assert ask(m, "ua:BaseObjectType ua:HasSubtype ua:CentrifugalPumpType")
    assert ask(m, "ua:CentrifugalPumpType ua:SubtypeOf ua:BaseObjectType")
    assert count(m, "ua:BaseObjectType ua:HasSubtype ?t") == 3
    assert ask(m, "ex:Motor1 ua:ComponentOf ex:Pump1")
    assert ask(m, "ex:Pump1 ua:HierarchicalReferences ex:Motor1, ex:Bearing1, ex:FlowRate")
    assert not ask(m, "ex:Pump1 ua:HasComponent ex:Bearing1")


def test_profiles_differ():
    ttl = """
    ex:A rdfs:subClassOf ex:B . ex:B owl:equivalentClass ex:C .
    ex:p owl:inverseOf ex:q . ex:x a ex:A ; ex:p ex:y .
    ex:r rdfs:domain ex:D . ex:x ex:r ex:z .
    """
    m = model(ttl)
    reason_or_skip(m, "rdfs")
    assert ask(m, "ex:x a ex:B, ex:D")
    assert not ask(m, "ex:x a ex:C")
    assert not ask(m, "ex:y ex:q ex:x")
    # The RDFS profile has the axiomatic triples, rdfD2 and the reflexive triples.
    assert ask(m, "ex:p a rdf:Property . ex:p rdfs:subPropertyOf ex:p")
    assert ask(m, "ex:A a rdfs:Class")

    m = model(ttl)
    reason_or_skip(m, "rdfs-plus")
    assert ask(m, "ex:x a ex:B, ex:C, ex:D")
    assert ask(m, "ex:y ex:q ex:x")
    assert not ask(m, "ex:p a rdf:Property")

    m = model(ttl)
    reason_or_skip(m, "owl-rl")
    assert ask(m, "ex:x a ex:B, ex:C, ex:D")
    assert ask(m, "ex:y ex:q ex:x")


def test_unknown_profile():
    m = model("ex:a ex:b ex:c .")
    with pytest.raises(Exception, match="Unknown reasoning profile|Contact Data Treehouse"):
        m.infer(profile="owl-dl")


def test_infer_takes_rulesets_or_profile():
    m = model("ex:a ex:b ex:c .")
    with pytest.raises(Exception, match="either rulesets or profile"):
        m.infer("[?a, ex:d, ?c] :- [?a, ex:b, ?c] .", profile="rdfs")
    with pytest.raises(Exception, match="rulesets or a profile"):
        m.infer()
    with pytest.raises(Exception, match="only used with a profile"):
        m.infer("[?a, ex:d, ?c] :- [?a, ex:b, ?c] .", target_graph="urn:graph:inferred")
    with pytest.raises(Exception, match="only used with a profile"):
        m.infer("[?a, ex:d, ?c] :- [?a, ex:b, ?c] .", max_violations=5)


def test_same_as_from_functional_and_keys():
    m = model("""
    ex:hasMRID a owl:DatatypeProperty .
    ex:Breaker owl:hasKey ( ex:hasMRID ) .
    ex:b1 a ex:Breaker ; ex:hasMRID "123" ; ex:ratedCurrent "100" .
    ex:b2 a ex:Breaker ; ex:hasMRID "123" ; ex:located ex:site1 .
    ex:locatedIn a owl:FunctionalProperty .
    ex:b3 ex:locatedIn ex:s1, ex:s2 .
    ex:s1 ex:name "Site" .
    """)
    res = reason_or_skip(m, "owl-rl")
    assert res.consistent
    assert ask(m, "ex:b1 owl:sameAs ex:b2 . ex:b2 owl:sameAs ex:b1")
    # eq-rep-s / eq-rep-o copy the triples of equal resources.
    assert ask(m, 'ex:b2 ex:ratedCurrent "100" . ex:b1 ex:located ex:site1')
    assert ask(m, "ex:s1 owl:sameAs ex:s2")
    assert ask(m, 'ex:s2 ex:name "Site"')
    # No eq-ref.
    assert not ask(m, "ex:b3 owl:sameAs ex:b3")


def test_same_as_groups():
    # Two groups from an inverse functional property merge through an asserted owl:sameAs, so
    # every member gets the triples of all members. A class in a group is replaced as well.
    m = model("""
    ex:id a owl:InverseFunctionalProperty .
    ex:a ex:id "1" . ex:b ex:id "1" . ex:c ex:id "2" . ex:d ex:id "2" .
    ex:b owl:sameAs ex:c .
    ex:a a ex:Pump . ex:Pump rdfs:subClassOf ex:Equipment .
    ex:Equipment owl:sameAs ex:Asset .
    ex:d ex:feeds ex:x . ex:x ex:partOf ex:a .
    """)
    res = reason_or_skip(m, "owl-rl")
    assert res.consistent
    members = ["ex:a", "ex:b", "ex:c", "ex:d"]
    for x in members:
        for y in members:
            assert ask(m, f"{x} owl:sameAs {y}")
        assert ask(m, f"{x} a ex:Pump . {x} a ex:Equipment . {x} a ex:Asset")
        assert ask(m, f'{x} ex:feeds ex:x . ex:x ex:partOf {x} . {x} ex:id "1", "2"')
    assert ask(m, "ex:Pump rdfs:subClassOf ex:Asset . ex:Asset owl:sameAs ex:Equipment")
    inferred = m.query(
        Q_PREFIXES + "SELECT ?x ?y WHERE { ?x owl:sameAs ?y }",
        graph=TRANSIENT_GRAPH,
        include_graphs=[],
    )
    # All pairs of each group, except the asserted ones.
    assert inferred.height == 4 * 4 + 2 * 2 - 2


def test_class_expressions():
    m = model("""
    ex:Parent owl:equivalentClass [ a owl:Restriction ; owl:onProperty ex:hasChild ; owl:someValuesFrom ex:Person ] .
    ex:Mother owl:equivalentClass [ owl:intersectionOf ( ex:Parent ex:Woman ) ] .
    ex:Danish owl:equivalentClass [ a owl:Restriction ; owl:onProperty ex:citizenOf ; owl:hasValue ex:Denmark ] .
    ex:PetOwner rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:owns ; owl:allValuesFrom ex:Pet ] .
    ex:Fruit owl:unionOf ( ex:Apple ex:Pear ) .
    ex:Weekend owl:oneOf ( ex:Saturday ex:Sunday ) .
    ex:hasMother rdfs:subPropertyOf ex:hasParent .
    ex:Child rdfs:subClassOf [ a owl:Restriction ; owl:onProperty [ owl:inverseOf ex:hasChild ] ; owl:maxCardinality "1"^^xsd:nonNegativeInteger ] .

    ex:anna a ex:Woman ; ex:hasChild ex:bob ; ex:citizenOf ex:Denmark .
    ex:bob a ex:Person .
    ex:carl a ex:Danish .
    ex:dora a ex:PetOwner ; ex:owns ex:rex .
    ex:apple1 a ex:Apple .
    ex:eve a ex:Child . ex:f1 ex:hasChild ex:eve . ex:f2 ex:hasChild ex:eve .
    """)
    res = reason_or_skip(m, "owl-rl")
    assert res.consistent, res.violations
    assert ask(m, "ex:anna a ex:Parent, ex:Mother, ex:Danish")
    assert ask(m, "ex:carl ex:citizenOf ex:Denmark")
    assert ask(m, "ex:rex a ex:Pet")
    assert ask(m, "ex:apple1 a ex:Fruit")
    assert ask(m, "ex:Saturday a ex:Weekend")
    assert ask(m, "ex:Mother rdfs:subClassOf ex:Parent, ex:Woman")
    assert ask(m, "ex:f1 owl:sameAs ex:f2")
    assert not ask(m, "ex:bob a ex:Mother")
    # The helper triples are removed.
    assert count(m, "?s ?p ?o FILTER(STRSTARTS(STR(?p), 'urn:maplib:reasoning:'))") == 0


def test_violations():
    m = model("""
    ex:Line owl:disjointWith ex:Transformer .
    ex:x a ex:Line, ex:Transformer .
    ex:p a owl:IrreflexiveProperty . ex:y ex:p ex:y .
    ex:Sub rdfs:subClassOf owl:Nothing . ex:z a ex:Sub .
    ex:a owl:sameAs ex:b . ex:a owl:differentFrom ex:b .
    ex:q a owl:AsymmetricProperty . ex:u ex:q ex:v . ex:v ex:q ex:u .
    [ a owl:AllDisjointClasses ; owl:members ( ex:C1 ex:C2 ex:C3 ) ] .
    ex:w a ex:C1, ex:C3 .
    """)
    res = reason_or_skip(m, "owl-rl")
    assert not res.consistent
    rules = {v["rule"] for v in res.violations}
    assert {"cax-dw", "prp-irp", "cls-nothing2", "eq-diff1", "prp-asyp", "cax-adc"} <= rules
    v = next(v for v in res.violations if v["rule"] == "cax-dw")
    assert "<http://example.net/ns#x>" in v["nodes"]
    assert "disjoint" in v["message"]
    # The RDFS-Plus profile does not check consistency.
    m2 = model("ex:Line owl:disjointWith ex:Transformer . ex:x a ex:Line, ex:Transformer .")
    assert reason_or_skip(m2, "rdfs-plus").consistent
    # max_violations=0 skips the checks.
    m3 = model("ex:Line owl:disjointWith ex:Transformer . ex:x a ex:Line, ex:Transformer .")
    assert reason_or_skip(m3, "owl-rl", max_violations=0).consistent


def test_max_violations():
    ttl = "ex:Line owl:disjointWith ex:Transformer .\n" + "\n".join(
        f"ex:x{i} a ex:Line, ex:Transformer ." for i in range(20)
    )
    m = model(ttl)
    res = reason_or_skip(m, "owl-rl", max_violations=5)
    assert len(res.violations) == 5
    assert res.violations_truncated


def test_larger_cim_like_model():
    # Many instances over a deep hierarchy: checks that the fixpoint is reached and counted.
    n = 2000
    lines = [CIM]
    for i in range(n):
        lines.append(f"ex:l{i} a cim:ACLineSegment .")
        lines.append(f"ex:t{i}a cim:Terminal.ConductingEquipment ex:l{i} ; cim:Terminal.ConnectivityNode ex:cn{i} .")
        lines.append(f"ex:t{i}b cim:Terminal.ConductingEquipment ex:l{i} ; cim:Terminal.ConnectivityNode ex:cn{i + 1} .")
    m = model("\n".join(lines))
    res = reason_or_skip(m, "owl-rl")
    assert res.consistent
    assert count(m, "?x a cim:IdentifiedObject") == n + 2 * n + (n + 1)
    assert count(m, "?x cim:ConductingEquipment.Terminals ?t") == 2 * n
    assert count(m, "?x ex:connectedTo ?y FILTER(?x != ?y)") == 2 * (n - 1)

"""RDF/XML entity expansion is bounded, so entity bombs ("billion laughs") fail fast with bounded
memory, while ordinary entity declarations (as in OWL files) still work."""

import resource
import sys
import time

import pytest
from maplib import Model, MaplibException

RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
COUNT = "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }"


def peak_rss_bytes():
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux kibibytes.
    return rss if sys.platform == "darwin" else rss * 1024


def billion_laughs():
    decls = ['<!ENTITY lol0 "lol">']
    for i in range(1, 10):
        decls.append(f'<!ENTITY lol{i} "{f"&lol{i - 1};" * 10}">')
    dtd = "\n  ".join(decls)
    return f"""<?xml version="1.0"?>
<!DOCTYPE rdf:RDF [
  {dtd}
]>
<rdf:RDF xmlns:rdf="{RDF}" xmlns:ex="http://ex/">
  <rdf:Description rdf:about="http://ex/a">
    <ex:p>&lol9;</ex:p>
  </rdf:Description>
</rdf:RDF>"""


@pytest.mark.parametrize("parallel", [False, True])
def test_billion_laughs_fails_fast_with_bounded_memory(parallel):
    before = peak_rss_bytes()
    start = time.monotonic()
    with pytest.raises(MaplibException, match="lol"):
        Model().reads(billion_laughs(), format="rdf/xml", parallel=parallel)
    assert time.monotonic() - start < 10
    # Unbounded, lol9 alone is 3 * 10^9 bytes.
    assert peak_rss_bytes() - before < 500 * 1024 * 1024


def test_entity_bomb_names_the_entity():
    with pytest.raises(MaplibException, match="entity 'lol[0-9]'.*billion laughs"):
        Model().reads(billion_laughs(), format="rdf/xml")


def test_billion_laughs_from_file(tmp_path):
    path = tmp_path / "bomb.rdf"
    path.write_text(billion_laughs())
    with pytest.raises(MaplibException, match="billion laughs"):
        Model().read(path, format="rdf/xml")


def test_many_references_to_a_large_entity_fail():
    """Each entity stays under the per-entity limit, but referencing it many times would
    expand a 2 MB file to 100 MB."""
    big = "x" * (500 * 1024)
    elements = "\n".join(
        f'<rdf:Description rdf:about="http://ex/s{i}"><ex:p>&big;</ex:p></rdf:Description>'
        for i in range(200)
    )
    xml = f"""<?xml version="1.0"?>
<!DOCTYPE rdf:RDF [ <!ENTITY big "{big}"> ]>
<rdf:RDF xmlns:rdf="{RDF}" xmlns:ex="http://ex/">
{elements}
</rdf:RDF>"""
    with pytest.raises(MaplibException, match="entity 'big'.*total entity expansion"):
        Model().reads(xml, format="rdf/xml")


def test_external_entities_are_refused():
    xml = f"""<?xml version="1.0"?>
<!DOCTYPE rdf:RDF [ <!ENTITY ext SYSTEM "file:///etc/passwd"> ]>
<rdf:RDF xmlns:rdf="{RDF}" xmlns:ex="http://ex/">
  <rdf:Description rdf:about="http://ex/a"><ex:p>&ext;</ex:p></rdf:Description>
</rdf:RDF>"""
    with pytest.raises(MaplibException):
        Model().reads(xml, format="rdf/xml")


OWL_WITH_ENTITIES = f"""<?xml version="1.0"?>
<!DOCTYPE rdf:RDF [
  <!ENTITY owl "http://www.w3.org/2002/07/owl#">
  <!ENTITY xsd "http://www.w3.org/2001/XMLSchema#">
  <!ENTITY rdfs "http://www.w3.org/2000/01/rdf-schema#">
  <!ENTITY ex "http://example.org/onto#">
  <!ENTITY exsub "&ex;sub/">
]>
<rdf:RDF xmlns:rdf="{RDF}" xmlns:rdfs="&rdfs;" xmlns:owl="&owl;">
  <owl:Class rdf:about="&ex;Pump">
    <rdfs:label rdf:datatype="&xsd;string">Pump &amp; motor</rdfs:label>
    <rdfs:subClassOf rdf:resource="&exsub;Equipment"/>
  </owl:Class>
  <rdf:Description rdf:about="&ex;p1">
    <rdf:type rdf:resource="&owl;NamedIndividual"/>
  </rdf:Description>
</rdf:RDF>"""


def test_ordinary_entities_still_parse():
    m = Model()
    m.reads(OWL_WITH_ENTITIES, format="rdf/xml")
    df = m.query(
        """
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?label ?super WHERE {
          <http://example.org/onto#Pump> a owl:Class ;
             rdfs:label ?label ;
             rdfs:subClassOf ?super .
          <http://example.org/onto#p1> a owl:NamedIndividual .
        }
        """
    )
    assert df.height == 1
    assert df["label"][0] == "Pump & motor"
    assert df["super"][0] == "<http://example.org/onto#sub/Equipment>"


def test_large_document_with_many_entity_references_parses():
    """Using entities for namespace IRIs throughout a large file expands past the 10 MiB base
    budget, which the per-input-byte allowance covers."""
    ns = "http://example.org/a/rather/long/namespace/for/an/ontology/with/many/terms/" * 2
    n = 100_000
    elements = "\n".join(
        f'<rdf:Description rdf:about="&ex;s{i}"><rdf:type rdf:resource="&ex;C"/></rdf:Description>'
        for i in range(n)
    )
    xml = f"""<?xml version="1.0"?>
<!DOCTYPE rdf:RDF [ <!ENTITY ex "{ns}"> ]>
<rdf:RDF xmlns:rdf="{RDF}">
{elements}
</rdf:RDF>"""
    assert 2 * n * len(ns) > 10 * 1024 * 1024
    m = Model()
    m.reads(xml, format="rdf/xml")
    assert m.query(COUNT)["n"][0] == n


def test_map_xml_does_not_expand_custom_entities():
    """map_xml (also used for OPC UA NodeSet2 files) ignores the DOCTYPE and never expands
    declared entities, so an entity bomb maps quickly to small literals."""
    start = time.monotonic()
    m = Model()
    m.map_xml(billion_laughs())
    assert time.monotonic() - start < 10
    longest = m.query(
        "SELECT (MAX(STRLEN(STR(?o))) AS ?n) WHERE { ?s ?p ?o FILTER(isLiteral(?o)) }"
    )["n"][0]
    assert longest < 100


def test_map_xml_ignores_doctype_without_references():
    xml = """<?xml version="1.0"?>
<!DOCTYPE doc [ <!ENTITY a "aaaaaaaaaa"> <!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;"> ]>
<doc><item>Pump</item></doc>"""
    m = Model()
    m.map_xml(xml)
    assert m.query('ASK { ?s ?p "Pump" }')

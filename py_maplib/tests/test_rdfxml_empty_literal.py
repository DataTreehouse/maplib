"""An empty rdf:parseType="Literal" property element is the empty XML literal (RDF/XML 7.2.17;
RDF Test Cases rdfms-empty-property-elements test003 and test009). It was a syntax error."""

from maplib import Model

RDFXML = """<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:ex="http://example.org/">
  <rdf:Description rdf:about="http://example.org/s">
    <ex:empty rdf:parseType="Literal"/>
    <ex:emptyPair rdf:parseType="Literal"></ex:emptyPair>
    <ex:full rdf:parseType="Literal"><b>x</b></ex:full>
  </rdf:Description>
</rdf:RDF>"""


def test_empty_xml_literal():
    m = Model()
    m.reads(RDFXML, format="rdf/xml")
    df = m.query(
        """PREFIX ex: <http://example.org/>
        SELECT ?p (STR(?o) AS ?s) (DATATYPE(?o) AS ?dt) WHERE { ex:s ?p ?o } ORDER BY ?p"""
    )
    got = {r[0].rsplit("/", 1)[1].rstrip(">"): (r[1], r[2]) for r in df.iter_rows()}
    xml = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral>"
    assert got["empty"] == ("", xml)
    assert got["emptyPair"] == ("", xml)
    assert got["full"][1] == xml and "x" in got["full"][0]

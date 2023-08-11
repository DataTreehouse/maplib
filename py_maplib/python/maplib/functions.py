import logging
from ._maplib import Mapping
try:
    from rdflib import Graph, URIRef, Literal, BNode
except ImportError:
    logging.debug("RDFLib not found, install it to use the function to_graph")

def to_graph(m:Mapping) -> Graph:
    g = Graph()
    triples = m.to_triples()
    for t in triples:
        g.add(to_rdflib_triple(t))
    return g

def to_rdflib_triple(t):
    subject = t.subject
    subject_iri = subject.iri
    if subject_iri is not None:
        rdflib_subject = URIRef(subject_iri.iri)
    else:
        subject_blank_node = subject.blank_node
        rdflib_subject = BNode(subject_blank_node)

    rdbflib_verb = URIRef(t.verb.iri)

    object = t.object
    object_iri = object.iri
    if object_iri is not None:
        rdflib_object = URIRef(object_iri.iri)
    else:
        subject_literal = object.literal
        datatype_iri = subject_literal.datatype_iri
        if datatype_iri is not None:
            rdflib_object = Literal(lexical_or_value=subject_literal.lexical_form, datatype=datatype_iri.iri)
        else:
            rdflib_object = Literal(lexical_or_value=subject_literal.lexical_form, lang=subject_literal.language_tag)

    return (rdflib_subject, rdbflib_verb, rdflib_object)


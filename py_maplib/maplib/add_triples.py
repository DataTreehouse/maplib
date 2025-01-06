from maplib import Mapping, Template, IRI, Triple, Variable


def add_triples(
    source: Mapping, target: Mapping, source_graph: str = None, target_graph: str = None
):
    for p in source.get_predicate_iris(source_graph):
        subject = Variable("subject")
        object = Variable("object")
        template = Template(
            iri=IRI("urn:maplib:tmp"),
            parameters=[subject, object],
            instances=[Triple(subject, p, object)],
        )
        sms = source.get_predicate(p, source_graph)
        for sm in sms:
            target.expand(
                template,
                sm.mappings,
                unique_subset=["subject", "object"],
                types=sm.rdf_types,
                graph=target_graph,
            )

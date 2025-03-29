from maplib.maplib import Mapping, Template, IRI, Triple, Variable


def add_triples(
    source: Mapping, target: Mapping, source_graph: str = None, target_graph: str = None
):
    """(Zero) copy the triples from one Mapping into another.

    :param source: The source mapping
    :param target: The target mapping
    :param source_graph: The named graph in the source mapping to copy from. None means default graph.
    :param target_graph: The named graph in the target mapping to copy into. None means default graph.
    """
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
                types=sm.rdf_types,
                graph=target_graph,
            )

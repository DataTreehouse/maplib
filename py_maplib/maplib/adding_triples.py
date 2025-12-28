from maplib.maplib import Model, Template, IRI, Triple, Variable


def add_triples(
    source: Model, target: Model, source_graph: str = None, target_graph: str = None
):
    """(Zero) copy the triples from one Model into another.

    :param source: The source model
    :param target: The target model
    :param source_graph: The named graph in the source model to copy from. None means default graph.
    :param target_graph: The named graph in the target model to copy into. None means default graph.
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
            target.map(
                template,
                sm.mappings,
                types=sm.rdf_types,
                graph=target_graph,
            )

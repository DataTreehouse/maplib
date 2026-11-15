import logging
from typing import List, Optional

from maplib import Model

from .fully_inherited import (
    infer_browse_path_instances,
    infer_fully_inherited_instance_declaration,
)
from .generate_shacl import (
    add_browse_path_shapes,
    add_mandatory_cardinality_shapes,
    add_type_shapes,
)
from .proper_instance import add_has_proper_instance
from .type_namespace import is_type_namespace

logger = logging.getLogger(__name__)

TYPING_CONCEPT = "https://github.com/DataTreehouse/uamapper#OPCUATypingValidation"

# The category of the generated shapes and its concept scheme.
# Put your own shape categories in the same scheme to have them reported together.
CATEGORIES_TTL = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix uamapper: <https://github.com/DataTreehouse/uamapper#> .

uamapper:OPCUAValidation
    rdfs:label "OPC UA Validation" ;
    rdfs:comment "Validation for OPC UA Information Models" ;
    a skos:ConceptScheme .

uamapper:OPCUATypingValidation
    rdfs:label "OPC UA Typing Validation" ;
    rdfs:comment "Validation that instances are well-formed according to their type definitions" ;
    a skos:Concept ;
    skos:inScheme uamapper:OPCUAValidation .
"""


def add_inferences(
    m: Model,
    type_namespaces: Optional[List[str]] = None,
    instance_namespaces: Optional[List[str]] = None,
) -> Model:
    """
    Add the helper triples (in the uamapper vocabulary) that the typing shapes and many
    hand-written OPC UA shapes rely on:

    - ``?ns uamapper:typeNamespace true/false`` for each namespace,
    - ``uamapper:fiid``/``uamapper:baseFiid``/``uamapper:path`` for the fully inherited
      instance declarations of each type,
    - ``?node a ?instance_declaration`` for instance nodes found on a browse path of a typed node,
    - ``?type uamapper:hasProperInstance true/false``.
    """
    logger.info("Starting inferencing")
    m = is_type_namespace(
        m, type_namespaces=type_namespaces, instance_namespaces=instance_namespaces
    )
    m = infer_fully_inherited_instance_declaration(m)
    m = infer_browse_path_instances(m)
    m = add_has_proper_instance(m)
    logger.info("Finished inferencing")
    return m


def add_typing_shapes(
    m: Model,
    shapes_graph: Optional[str] = None,
    type_namespaces: Optional[List[str]] = None,
    instance_namespaces: Optional[List[str]] = None,
) -> Model:
    """
    Add the inferences (see :func:`add_inferences`) and generate SHACL shapes from the type
    definitions in the mapped nodesets into ``shapes_graph``: one shape per ObjectType and
    VariableType, one per browse path of their instance declarations, and cardinality shapes
    for Mandatory instance declarations. All have ``maplib:shapeCategory uamapper:OPCUATypingValidation``.

    Called by ``m.opcua.add_typing_shapes``.

    :param m: A model with OPC UA nodesets mapped by :meth:`maplib.Model.map_opc_ua`.
    :param shapes_graph: The graph to add the shapes to (default graph if None).
    :param type_namespaces: Namespaces holding types. Cannot be combined with instance_namespaces.
    :param instance_namespaces: Namespaces holding instances, all others are type namespaces.
        If neither is given, a namespace is a type namespace when it has type nodes.
    """
    m = add_inferences(
        m, type_namespaces=type_namespaces, instance_namespaces=instance_namespaces
    )
    m = add_mandatory_cardinality_shapes(
        m, shapes_graph=shapes_graph, concept=TYPING_CONCEPT
    )
    m = add_browse_path_shapes(m, shapes_graph=shapes_graph, concept=TYPING_CONCEPT)
    m = add_type_shapes(m, shapes_graph=shapes_graph, concept=TYPING_CONCEPT)
    m.reads(CATEGORIES_TTL, format="turtle", graph=shapes_graph)
    return m

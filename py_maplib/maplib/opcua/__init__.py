"""
OPC UA support in maplib, next to :meth:`maplib.Model.map_opc_ua`.

- ``m.opcua.add_typing_shapes(...)`` generates SHACL shapes from the type definitions of the
  mapped NodeSet2 files, see :meth:`OPCUA.add_typing_shapes`.
- :class:`OPCUAHistoryDatabase` reads time series from an OPC UA server with Historical Access,
  as a virtualized database.
"""

from typing import TYPE_CHECKING, List, Optional

from .history import OPCUAHistoryDatabase

if TYPE_CHECKING:
    from maplib import Model

#: The helper vocabulary of the inferences and generated shapes (uamapper:typeNamespace, uamapper:path, ...).
UAMAPPER = "https://github.com/DataTreehouse/uamapper#"


class OPCUA:
    """OPC UA functionality of a Model, available as ``m.opcua``."""

    def __init__(self, model: "Model"):
        self._model = model

    def add_typing_shapes(
        self,
        shapes_graph: Optional[str] = None,
        type_namespaces: Optional[List[str]] = None,
        instance_namespaces: Optional[List[str]] = None,
    ):
        """
        Generate SHACL shapes from the type definitions of the OPC UA nodesets in the model,
        mapped with :meth:`maplib.Model.map_opc_ua`.

        The shapes check that instances are well-formed according to their types: one shape
        per ObjectType and VariableType (targeting the instances with that HasTypeDefinition),
        one per browse path of the fully inherited instance declarations, and cardinality shapes
        requiring each Mandatory instance declaration to be present exactly once. The shapes get
        ``maplib:shapeCategory uamapper:OPCUATypingValidation``, a ``skos:Concept`` in the
        ``uamapper:OPCUAValidation`` scheme, which are added with their labels too.
        Shapes of types without instances in the instance namespaces are deactivated.

        The generation also adds helper triples to the model that your own shapes can use:
        ``?ns uamapper:typeNamespace true/false`` for each namespace, ``uamapper:fiid``,
        ``uamapper:baseFiid`` and ``uamapper:path`` for the instance declarations of each type,
        ``?node a ?instance_declaration`` for instance nodes found on a browse path, and
        ``?type uamapper:hasProperInstance true/false``.

        Usage:

        >>> m = Model()
        >>> m.map_opc_ua("nodesets")
        >>> m.opcua.add_typing_shapes(shapes_graph="urn:shapes", instance_namespaces=["urn:my:instances"])
        >>> m.read("my_shapes.ttl", graph="urn:shapes")
        >>> report = m.validate(shape_graph="urn:shapes")

        :param shapes_graph: The graph to add the shapes to, the default graph if None.
        :param type_namespaces: The namespace URIs holding types. Cannot be combined with instance_namespaces.
        :param instance_namespaces: The namespace URIs holding instances, all others hold types.
            If neither is given, a namespace holds types when it has type nodes.
        """
        from .typing_shapes import add_typing_shapes

        if type_namespaces is not None and instance_namespaces is not None:
            raise ValueError("Only one of type_namespaces and instance_namespaces may be given")
        add_typing_shapes(
            self._model,
            shapes_graph=shapes_graph,
            type_namespaces=type_namespaces,
            instance_namespaces=instance_namespaces,
        )


__all__ = ["OPCUA", "OPCUAHistoryDatabase", "UAMAPPER"]

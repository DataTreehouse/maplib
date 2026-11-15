"""Generation of SHACL shapes from the type definitions of OPC UA nodesets, see maplib.opcua.OPCUA.add_typing_shapes."""

from .inferences import TYPING_CONCEPT, add_inferences, add_typing_shapes

__all__ = ["TYPING_CONCEPT", "add_inferences", "add_typing_shapes"]

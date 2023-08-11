from ._maplib import *

__doc__ = _maplib.__doc__
if hasattr(_maplib, "__all__"):
    __all__ = _maplib.__all__

import logging
try:
    import rdflib
    from .functions import to_graph
except:
    logging.debug("RDFLib not found, install it to use the function to_graph")

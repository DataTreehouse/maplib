# r'''
# # Overview
#
# '''

import logging
logger = logging.getLogger(__name__)

__all__ = [
    "Model",
    "a",
    "Triple",
    "SolutionMappings",
    "IndexingOptions",
    "ValidationReport",
    "Instance",
    "Template",
    "Argument",
    "Parameter",
    "Variable",
    "RDFType",
    "xsd",
    "rdf",
    "rdfs",
    "owl",
    "IRI",
    "Literal",
    "Prefix",
    "BlankNode",
    "explore",
    "add_triples",
    "generate_templates",
    "MaplibException",
]

import pathlib
from importlib.metadata import version
from .maplib import *
from .adding_triples import add_triples
from .template_generator import generate_templates

"""
http://www.w3.org/1999/02/22-rdf-syntax-ns#type
"""
a = rdf.type

if (pathlib.Path(__file__).parent.resolve() / "graph_explorer").exists():
    from .graph_explorer import explore as _explore
else:

    def _explore(
        m: "Model",
        host: str = "localhost",
        port: int = 8000,
        bind: str = "localhost",
        popup=True,
        fts=True,
        fts_path:str="fts",
    ):
        print("Contact Data Treehouse to try!")


def explore(*args, **kwargs):
    """Deprecated way to start an explore session.
Use the explore method on a Model object instead
"""
    logger.warn("Calling `maplib.explore` is deprecated, use `m.explore()` on a `Model` object instead")
    if kwargs.get("popup") == None or kwargs.get("popup") == True:
        logger.warn("""Calling explore without a popup argument defaults to it being on.
The popup argument is deprecated, so if you are relying on explore() opening a browser window
please change this to something like

```
import webbrowser
from maplib import Model

m = Model()
...
s = m.explore()
webbrowser.open(s.url, new=2)
```
""")
        kwargs["popup"] = True
    elif kwargs.get("popup") == False:
        logger.warn("The new explore function on a Model, no longer defaults to popping up the browser ")


    return _explore(*args, **kwargs)

__version__ = version("maplib")

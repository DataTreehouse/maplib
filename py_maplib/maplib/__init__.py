# r'''
# # Overview
#
# '''

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
    from .graph_explorer import explore
else:

    def explore(
        m: "Model",
        host: str = "localhost",
        port: int = 8000,
        bind: str = "localhost",
        popup=True,
        fts=True,
        fts_path:str="fts",
    ):
        """Starts a graph explorer session.
        To run from Jupyter Notebook use:
        >>> from maplib import explore
        >>>
        >>> server = explore(m)
        You can later stop the server with
        >>> server.stop()

        :param m: The Model to explore
        :param host: The hostname that we will point the browser to.
        :param port: The port where the graph explorer webserver listens on.
        :param bind: Bind to the following host / ip.
        :param popup: Pop up the browser window.
        :param fts: Enable full text search indexing
        :param fts_path: Path to the fts index
        """
        print("Contact Data Treehouse to try!")

__version__ = version("maplib") 

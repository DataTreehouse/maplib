# r'''
# # Overview
#
# '''

__all__ = [
    "Model",
    "a",
    "Triple",
    "SolutionModels",
    "IndexingOptions",
    "ValidationReport",
    "Instance",
    "Template",
    "Argument",
    "Parameter",
    "Variable",
    "RDFType",
    "XSD",
    "IRI",
    "Literal",
    "Prefix",
    "BlankNode",
    "explore",
    "add_triples",
    "MaplibException",
]

import pathlib
from .maplib import *
from .adding_triples import add_triples

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
        """
        print("Contact Data Treehouse to try!")

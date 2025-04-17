# r'''
# # Overview
#
# '''

__all__ = [
    "Mapping",
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
    "XSD",
    "IRI",
    "Literal",
    "Prefix",
    "BlankNode",
    "explore",
    "add_triples",
    "MaplibException"
]

import pathlib
from .maplib import *
from .add_triples import add_triples

if (pathlib.Path(__file__).parent.resolve() / "graph_explorer").exists():
    from .graph_explorer import explore
else:
    async def explore(
            m: "Mapping",
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
        >>> await explore(m)

        This will block further execution of the notebook until you stop the cell.

        :param m: The Mapping to explore
        :param host: The hostname that we will point the browser to.
        :param port: The port where the graph explorer webserver listens on.
        :param bind: Bind to the following host / ip.
        :param popup: Pop up the browser window.
        :param fts: Enable full text search indexing
        """
        print("Contact Data Treehouse to try!")


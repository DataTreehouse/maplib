import pathlib

from .maplib import *
from .add_triples import add_triples

PATH_HERE = pathlib.Path(__file__).parent.resolve()

if (PATH_HERE / "graph_explorer").exists():
    from .graph_explorer import explore

import pathlib

from .maplib import *
PATH_HERE = pathlib.Path(__file__).parent.resolve()

if (PATH_HERE / "graph_explorer").exists():
    from .graph_explorer import explore
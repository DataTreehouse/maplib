import ast
from pathlib import Path

import maplib


def test_stubs_parse():
    # an unclosed docstring turns the rest of a stub into a string, and type checkers then miss most of Model
    stubs = sorted(Path(maplib.__file__).parent.rglob("*.pyi"))
    assert stubs
    for stub in stubs:
        ast.parse(stub.read_text(), filename=str(stub))

import os
import pathlib
import tempfile

from maplib import Model


def test_issue_72():
    NT = (
        "<https://a> <https://p> _:b0 .\n"
        '_:b0 <https://q> "x" .\n'
        "_:b0 <https://r> _:b1 .\n"
        '_:b1 <https://q> "y" .\n'
    )
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "d.nt")
    pathlib.Path(path).write_text(NT)

    model = Model()
    model.read(path, format="ntriples")

    model.update("""
    DELETE { ?s ?p ?o } WHERE {
      { ?s ?p ?o }
      UNION
      { ?root ?q0 ?m1 . ?m1 ?q1 ?m2 . ?m2 ?q2 ?s . ?s ?p ?o . }
    }
    """)

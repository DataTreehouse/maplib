"""A basic graph pattern that joins on two variables, where many solutions share the value of one
of them, must keep a size linear in the number of solutions.

Each instance of :P has one child, all children have the name of the first of the K children of
:P, and the children of the instances get the declaration with the same name:

    ?parent :ref ?type_child . ?instance a ?parent . ?instance :ref ?instance_child .
    ?type_child :bn ?browse_name . ?instance_child :bn ?browse_name .

Polars' join reordering, when given the whole query as one plan (more than 100 000 solutions
before the last join), moved the join of the two :bn patterns on ?browse_name alone first: every
child meets every other child, N * N rows, 134 GB for the OPC UA typing shapes of 100 000 copies.
The result was right, so the query runs in a child process with a hard memory ceiling
(``abort_above_mb``) that the quadratic join goes far over.
"""

import subprocess
import sys
import textwrap

N_INSTANCES = 10_000
N_TYPE_CHILDREN = 16
# The model is about 20 MB. The quadratic join of 10 000 x 10 000 rows needs over 1.5 GB.
CEILING_MB = 400

CHILD = textwrap.dedent(
    """
    import sys
    import maplib

    n, k, ceiling_mb = (int(x) for x in sys.argv[1:4])
    ex = "http://example.net/"
    a = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
    lines = []
    for j in range(k):
        lines.append(f"<{ex}P> <{ex}ref> <{ex}T{j}> .")
        lines.append(f'<{ex}T{j}> <{ex}bn> "name{j}" .')
    for i in range(n):
        lines.append(f"<{ex}i{i}> {a} <{ex}P> .")
        lines.append(f"<{ex}i{i}> <{ex}ref> <{ex}c{i}> .")
        lines.append(f'<{ex}c{i}> <{ex}bn> "name0" .')
    m = maplib.Model()
    m.reads("\\n".join(lines) + "\\n", format="ntriples")
    maplib.set_memory_limit(abort_above_mb=ceiling_mb)
    df = m.query(
        '''
        PREFIX ex: <http://example.net/>
        SELECT ?instance_child ?type_child WHERE {
            ?parent ex:ref ?type_child .
            ?instance a ?parent .
            ?instance ex:ref ?instance_child .
            ?type_child ex:bn ?browse_name .
            ?instance_child ex:bn ?browse_name .
        }
        '''
    )
    print("rows", df.height, flush=True)
    print("children", df["instance_child"].n_unique(), flush=True)
    print("declarations", df["type_child"].unique().to_list(), flush=True)
    """
)


def test_join_on_two_variables_with_a_shared_literal_stays_linear():
    p = subprocess.run(
        [sys.executable, "-c", CHILD, str(N_INSTANCES), str(N_TYPE_CHILDREN), str(CEILING_MB)],
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert p.returncode == 0, f"exit {p.returncode}\n{p.stdout}\n{p.stderr[-2000:]}"
    lines = dict(line.split(" ", 1) for line in p.stdout.splitlines())
    assert lines["rows"] == str(N_INSTANCES)
    assert lines["children"] == str(N_INSTANCES)
    assert lines["declarations"] == "['<http://example.net/T0>']"

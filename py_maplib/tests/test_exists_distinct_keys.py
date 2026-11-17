"""FILTER EXISTS and FILTER NOT EXISTS are evaluated as semi and anti joins: the pattern is
evaluated once for each distinct combination of the incoming variables it mentions, not once for
each incoming solution.

BSBM business intelligence query 4 pairs every feature with every offer (about a million rows at
300 products) and keeps the pairs whose product lacks the feature:

    { SELECT DISTINCT ?feature { ?p a %ProductType% ; bsbm:productFeature ?feature } }
    ?product a %ProductType% .
    ?offer bsbm:product ?product ; bsbm:price ?price .
    FILTER NOT EXISTS { ?product bsbm:productFeature ?feature }

The filter keeps 94 % of the rows, so it can not prune much: the million rows are what the query
asks for. Evaluating the NOT EXISTS pattern with each of them as its input, joining a boolean
column back and filtering on it held several copies of them. Only ?product and ?feature decide
the answer, and they have 52 thousand distinct pairs.

The small cases compare with pyoxigraph: unbound variables, a variable predicate, a FILTER
inside the pattern that reads an incoming variable, nested EXISTS and incoming variables the
pattern does not mention.
"""

import ctypes
import json
import subprocess
import sys
import textwrap
import time
from collections import Counter
from decimal import Decimal

import pyoxigraph
import pytest

from maplib import Model

EX = "http://example.net/"

# The BI Q4 shape: PRODUCTS products of one type, FEATURES features, PER_PRODUCT features per
# product and OFFERS offers per product. The cross product has 400 * 2400 = 960k rows.
PRODUCTS, FEATURES, PER_PRODUCT, OFFERS = 120, 400, 25, 20

CHILD = textwrap.dedent(
    f"""
    import json, sys
    import polars as pl
    from maplib import Model

    EX = "{EX}"
    PRODUCTS, FEATURES, PER_PRODUCT, OFFERS = {PRODUCTS}, {FEATURES}, {PER_PRODUCT}, {OFFERS}
    m = Model()
    prod = [f"{{EX}}product{{p}}" for p in range(PRODUCTS)]
    m.map_triples(
        pl.DataFrame({{"subject": prod, "object": [f"{{EX}}Type"] * PRODUCTS}}),
        predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
    )
    has = [(p, (p * 7 + k * 13) % FEATURES) for p in range(PRODUCTS) for k in range(PER_PRODUCT)]
    m.map_triples(
        pl.DataFrame({{
            "subject": [prod[p] for p, _ in has],
            "object": [f"{{EX}}feature{{f}}" for _, f in has],
        }}),
        predicate=EX + "feature",
    )
    offers = range(PRODUCTS * OFFERS)
    m.map_triples(
        pl.DataFrame({{
            "subject": [f"{{EX}}offer{{o}}" for o in offers],
            "object": [prod[o % PRODUCTS] for o in offers],
        }}),
        predicate=EX + "product",
    )
    m.map_triples(
        pl.DataFrame({{
            "subject": [f"{{EX}}offer{{o}}" for o in offers],
            "object": [float(o % 97) for o in offers],
        }}),
        predicate=EX + "price",
    )
    q = '''PREFIX ex: <{EX}>
    SELECT ?feature (COUNT(?offer) AS ?n) (SUM(?price) AS ?sum) {{
      {{ SELECT DISTINCT ?feature {{ ?p a ex:Type ; ex:feature ?feature }} }}
      ?product a ex:Type .
      ?offer ex:product ?product ; ex:price ?price .
      FILTER NOT EXISTS {{ ?product ex:feature ?feature }}
    }} GROUP BY ?feature'''
    m.query("SELECT (COUNT(*) AS ?c) WHERE {{ ?s ?p ?o }}", cache=False)
    print("ready", flush=True)
    sys.stdin.readline()
    df = m.query(q, cache=False)
    rows = {{f: [n, s] for f, n, s in df.select("feature", "n", "sum").rows()}}
    print(json.dumps(rows), flush=True)
    """
)

# The child is killed above this.
HARD_CEILING_MB = 1500
# The query's peak above the model's memory (macOS physical footprint). Per incoming solution it
# peaked at about 110 MB, as an anti join on the cross product at about 75-80 MB, and with the
# distinct feature × product pairs answered and joined with each side at about 55 MB.
QUERY_PEAK_LIMIT_MB = 65


def _footprint_mb(pid):
    """The memory of a process in MB: its physical footprint on macOS, its resident and swapped
    memory on Linux."""
    if sys.platform == "darwin":

        class RUsage(ctypes.Structure):
            _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [
                (f, ctypes.c_uint64)
                for f in (
                    "user_time system_time pkg_idle_wkups interrupt_wkups pageins "
                    "wired_size resident_size phys_footprint"
                ).split()
            ] + [("rest", ctypes.c_uint64 * 40)]

        r = RUsage()
        libproc = ctypes.CDLL("libproc.dylib")
        if libproc.proc_pid_rusage(pid, 2, ctypes.byref(r)) != 0:
            return 0
        return r.phys_footprint / 1e6
    kb = 0
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith(("VmRSS:", "VmSwap:")):
                kb += int(line.split()[1])
    return kb * 1024 / 1e6


def _expected():
    has = {(p, (p * 7 + k * 13) % FEATURES) for p in range(PRODUCTS) for k in range(PER_PRODUCT)}
    features = sorted({f for _, f in has})
    out = {}
    for f in features:
        n, s = 0, 0.0
        for o in range(PRODUCTS * OFFERS):
            if (o % PRODUCTS, f) not in has:
                n += 1
                s += float(o % 97)
        out[f"<{EX}feature{f}>"] = (n, s)
    return out


@pytest.mark.skipif(
    sys.platform not in ("darwin", "linux"), reason="reads the child's memory from the OS"
)
def test_bsbm_bi4_not_exists_answers_and_memory():
    p = subprocess.Popen(
        [sys.executable, "-c", CHILD],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    line = p.stdout.readline().strip()
    assert line == "ready", line + p.stdout.read()
    time.sleep(0.2)
    base = _footprint_mb(p.pid)
    peak = base
    p.stdin.write("go\n")
    p.stdin.flush()
    started = time.monotonic()
    killed = False
    while p.poll() is None:
        mb = _footprint_mb(p.pid)
        peak = max(peak, mb)
        if mb > HARD_CEILING_MB or time.monotonic() - started > 120:
            p.kill()
            killed = True
            break
        time.sleep(0.002)
    out = p.stdout.read()
    p.wait()
    assert not killed, f"killed at {peak:.0f} MB\n{out}"
    assert p.returncode == 0, out
    rows = json.loads(out.strip().splitlines()[-1])
    expected = _expected()
    assert set(rows) == set(expected)
    for f, (n, s) in expected.items():
        assert rows[f][0] == n, f
        assert rows[f][1] == pytest.approx(s), f
    assert peak - base < QUERY_PEAK_LIMIT_MB, f"query peaked at +{peak - base:.0f} MB"


DATA = f"""
@prefix ex: <{EX}> .
ex:a ex:p ex:x ; ex:q 1 ; ex:name "a" .
ex:b ex:p ex:y ; ex:q 2 ; ex:name "b"@en .
ex:c ex:q 3 .
ex:d ex:p ex:x ; ex:name 4 .
ex:x ex:r ex:a ; ex:label "x" .
ex:y ex:r ex:c .
"""

QUERIES = {
    # Positive and negative EXISTS on one incoming variable that repeats across solutions.
    "exists": "SELECT ?s ?v WHERE { ?s ex:q ?v FILTER EXISTS { ?s ex:p ?o } }",
    "not_exists": "SELECT ?s ?v WHERE { ?s ex:q ?v FILTER NOT EXISTS { ?s ex:p ?o } }",
    # Two incoming variables in the pattern, and incoming variables it does not mention.
    "two_keys": "SELECT ?s ?o ?v WHERE { ?s ex:p ?o . ?t ex:q ?v FILTER NOT EXISTS { ?o ex:r ?s } }",
    # An incoming variable that is unbound in some solutions.
    "unbound_key": "SELECT ?s ?o WHERE { ?s ex:q ?v OPTIONAL { ?s ex:p ?o } FILTER NOT EXISTS { ?o ex:r ?s } }",
    # An incoming variable only as a predicate of the pattern.
    "variable_predicate": "SELECT ?s ?pr WHERE { ?s ex:q ?v . VALUES ?pr { ex:p ex:name ex:r } FILTER EXISTS { ?s ?pr ?o } }",
    # An incoming variable only inside a FILTER of the pattern.
    "filter_inside": "SELECT ?s ?v WHERE { ?s ex:q ?v FILTER EXISTS { ?t ex:q ?w FILTER(?w > ?v) } }",
    # Nested EXISTS reading the outermost variables.
    "nested": "SELECT ?s WHERE { ?s ex:q ?v FILTER EXISTS { ?s ex:p ?o FILTER NOT EXISTS { ?o ex:r ?s } } }",
    # An incoming variable with values of several types.
    "multi_type_key": "SELECT ?s ?n WHERE { ?s ex:name ?n FILTER NOT EXISTS { ?x ex:label ?n } }",
    # No incoming variable in the pattern.
    "no_keys": "SELECT ?s WHERE { ?s ex:q ?v FILTER EXISTS { ex:x ex:r ?z } }",
    # EXISTS as a value.
    "projected": "SELECT ?s (EXISTS { ?s ex:p ex:x } AS ?e) WHERE { ?s ex:q ?v }",
    # Keys from both sides of a cross product, which is answered for the distinct key pairs
    # and joined with each side (BSBM BI Q4).
    "cross_not_exists": "SELECT ?s ?t WHERE { { SELECT DISTINCT ?t { ?t ex:r ?z } } ?s ex:q ?v FILTER NOT EXISTS { ?t ex:r ?s } }",
    "cross_exists": "SELECT ?s ?t WHERE { { SELECT DISTINCT ?t { ?t ex:r ?z } } ?s ex:q ?v FILTER EXISTS { ?t ex:r ?s } }",
    "cross_and_other_conjunct": "SELECT ?s ?t ?v WHERE { { SELECT DISTINCT ?t { ?t ex:r ?z } } ?s ex:q ?v FILTER (NOT EXISTS { ?t ex:r ?s } && ?v > 1) }",
    # Duplicate solutions on one side are kept as duplicates.
    "cross_duplicates": "SELECT ?s ?t WHERE { { SELECT ?t { ?t ?p ?o } } ?s ex:q ?v FILTER NOT EXISTS { ?t ex:r ?s } }",
    # A side with an OPTIONAL variable that is not a key.
    "cross_optional_side": "SELECT ?s ?n ?t WHERE { { ?s ex:q ?v OPTIONAL { ?s ex:name ?n } } { ?t ex:r ?z } FILTER NOT EXISTS { ?t ex:r ?s } }",
    # Two EXISTS conjuncts, one over both sides and one over one side.
    "cross_two_conjuncts": "SELECT ?s ?t WHERE { { SELECT DISTINCT ?t { ?t ex:r ?z } } ?s ex:q ?v FILTER (NOT EXISTS { ?t ex:r ?s } && EXISTS { ?s ex:p ?o }) }",
}


XSD = "http://www.w3.org/2001/XMLSchema#"
NUMERIC = {XSD + t for t in ("integer", "decimal", "double", "float", "int", "long")}


def _term(t):
    """A term from maplib's SPARQL JSON results or from pyoxigraph, in one comparable form."""
    if isinstance(t, dict):
        kind, value = t["type"], t["value"]
        dt, lang = t.get("datatype"), t.get("xml:lang")
    elif isinstance(t, pyoxigraph.NamedNode):
        kind, value, dt, lang = "uri", t.value, None, None
    else:
        kind, value, dt, lang = "literal", t.value, t.datatype.value, t.language
    if kind == "uri":
        return ("uri", value)
    if lang:
        return ("literal", value, lang.lower())
    if dt in NUMERIC:
        return ("number", Decimal(value).normalize())
    return ("literal", value, None if dt in (None, XSD + "string") else dt)


# ?o is unbound for ex:c, so NOT EXISTS { ?o ex:r ex:c } matches ex:y ex:r ex:c and removes it.
# maplib keeps ex:c on main as well: the per-solution evaluation does not treat an unbound
# incoming variable as free. Such a variable is not bound in every solution, so it is not a key.
KNOWN_FAILURES = {"unbound_key"}


@pytest.mark.parametrize(
    "name",
    [
        pytest.param(n, marks=pytest.mark.xfail(strict=True, reason="unbound variable in EXISTS"))
        if n in KNOWN_FAILURES
        else n
        for n in sorted(QUERIES)
    ],
)
def test_exists_matches_oxigraph(name):
    q = f"PREFIX ex: <{EX}>\n" + QUERIES[name]
    m = Model()
    m.reads(DATA, format="turtle")
    store = pyoxigraph.Store()
    store.load(DATA.encode(), pyoxigraph.RdfFormat.TURTLE)
    res = store.query(q)
    names = [v.value for v in res.variables]
    expected = Counter(
        tuple(sorted((n, _term(sol[n])) for n in names if sol[n] is not None)) for sol in res
    )
    j = json.loads(m.query(q, return_format="json"))
    got = Counter(
        tuple(sorted((n, _term(b)) for n, b in row.items())) for row in j["results"]["bindings"]
    )
    assert got == expected


def test_exists_on_a_random_key():
    """The solutions are read twice, for their keys and for the join with the keys that have a
    solution. Values of RAND() would differ between two evaluations, and no solution would then
    match a key, so they are evaluated once."""
    m = Model()
    m.reads("".join(f"<{EX}s{i}> <{EX}q> {i} .\n" for i in range(200)), format="turtle")
    df = m.query(
        f"""SELECT ?s ?r WHERE {{
          ?s <{EX}q> ?v BIND(RAND() AS ?r)
          FILTER EXISTS {{ FILTER(?r < 0.5) }}
        }}"""
    )
    assert 0 < df.height < 200
    assert (df["r"] < 0.5).all()

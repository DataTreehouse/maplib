"""Basic graph patterns evaluated by index nested loops on dictionary codes (storage/inl.rs),
compared with Oxigraph as multisets of rows, in every storage mode.

The data is a small BSBM-like shop: products with types, features, string labels and integer
properties, producers, offers with vendors, prices and dates, reviews with language-tagged
titles, and blank nodes. The queries are the shapes of BSBM explore (a star from constant
objects, a star around a constant product with chains, a chain from a constant object) and the
cases around them: blank node variables, constant strings, constants not in the data, empty
results, OPTIONAL, UNION and FILTER around the pattern, named graphs, the write buffer after a
small update, several segments, deletes, the kill switch and both sides of the thresholds.

The fast path counters say whether index nested loops answered (in memory) or declined and why
(always in hybrid and disk storage, whose dictionary is on disk)."""
import os
import random
import shutil
import tempfile
from collections import Counter

import pyoxigraph as ox
import pytest

import maplib.maplib as native
from maplib import Model

from .disk import disk_params

EX = "http://ex.org/"
XSD = "http://www.w3.org/2001/XMLSchema#"
PFX = f"PREFIX : <{EX}>\nPREFIX xsd: <{XSD}>\nPREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
HAS_COUNTERS = hasattr(native, "_fast_path_counts")

N_PRODUCTS = 300
N_FEATURES = 40
N_PRODUCERS = 12
N_VENDORS = 8
N_OFFERS = 1200
N_REVIEWS = 600


LABEL = "<http://www.w3.org/2000/01/rdf-schema#label>"
TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"


def iri(name):
    return f"<{EX}{name}>"


def build_data(seed=7):
    rng = random.Random(seed)
    lines = []

    def add(s, p, o):
        lines.append(f"{s} {p} {o} .")

    def lit(v, dt=None, lang=None):
        if lang:
            return f'"{v}"@{lang}'
        return f'"{v}"^^<{XSD}{dt}>' if dt else f'"{v}"'

    for f in range(N_FEATURES):
        add(iri(f"F{f}"), LABEL, lit(f"feature {f}"))
    for p in range(N_PRODUCERS):
        add(iri(f"Producer{p}"), LABEL, lit(f"producer {p}"))
        add(iri(f"Producer{p}"), iri("country"), iri(f"C{p % 3}"))
    for v in range(N_VENDORS):
        add(iri(f"Vendor{v}"), LABEL, lit(f"vendor {v}"))
        add(iri(f"Vendor{v}"), iri("country"), iri(f"C{v % 3}"))
    for i in range(N_PRODUCTS):
        p = iri(f"Product{i}")
        add(p, TYPE, iri("Product"))
        add(p, TYPE, iri(f"Type{i % 5}"))
        add(p, LABEL, lit(f"product {i % 250}"))  # some labels shared
        add(p, iri("comment"), lit(f"comment {i}"))
        producer = iri(f"Producer{i % N_PRODUCERS}")
        add(p, iri("producer"), producer)
        add(p, iri("publisher"), producer)
        for f in rng.sample(range(N_FEATURES), rng.randrange(1, 8)):
            add(p, iri("feature"), iri(f"F{f}"))
        add(p, iri("num1"), lit(rng.randrange(1000), "integer"))
        if i % 3:
            add(p, iri("num2"), lit(rng.randrange(1000), "integer"))
        if i % 4 == 0:
            add(p, iri("text"), lit(f"text {i}"))
    for o in range(N_OFFERS):
        off = iri(f"Offer{o}")
        add(off, iri("product"), iri(f"Product{rng.randrange(N_PRODUCTS)}"))
        vendor = iri(f"Vendor{rng.randrange(N_VENDORS)}")
        add(off, iri("vendor"), vendor)
        add(off, iri("publisher"), vendor)
        add(off, iri("price"), lit(f"{rng.randrange(10, 9999)}.{rng.randrange(1, 100):02d}", "decimal"))
        add(off, iri("validTo"), lit(f"2008-{rng.randrange(1, 13):02d}-{rng.randrange(1, 29):02d}T00:00:00", "dateTime"))
    for r in range(N_REVIEWS):
        rev = iri(f"Review{r}")
        add(rev, iri("reviewFor"), iri(f"Product{rng.randrange(N_PRODUCTS)}"))
        add(rev, iri("reviewer"), f"_:person{r % 50}")
        add(rev, iri("title"), lit(f"title {r}", lang=rng.choice(["en", "de"])))
        if r % 2:
            add(rev, iri("rating"), lit(rng.randrange(1, 11), "integer"))
    for b in range(50):
        add(f"_:person{b}", iri("name"), lit(f"person {b}"))
    # A predicate with objects of two types.
    for i in range(0, N_PRODUCTS - 1, 7):
        add(iri(f"Product{i}"), iri("mixed"), iri(f"F{i % N_FEATURES}"))
        add(iri(f"Product{i + 1}"), iri("mixed"), lit(f"m{i}"))
    return "\n".join(sorted(set(lines)))


DATA = build_data()
UPDATE_INSERT = f"""INSERT DATA {{
  <{EX}Product1> <{EX}feature> <{EX}F39> .
  <{EX}Product1> <http://www.w3.org/2000/01/rdf-schema#label> "product one again" .
  <{EX}NewProduct> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{EX}Type1> .
  <{EX}NewProduct> <{EX}feature> <{EX}F3> .
  <{EX}NewProduct> <http://www.w3.org/2000/01/rdf-schema#label> "new product" .
  <{EX}Offer9999> <{EX}product> <{EX}Product1> .
  <{EX}Offer9999> <{EX}vendor> <{EX}Vendor1> .
}}"""
UPDATE_DELETE = f"""DELETE DATA {{
  <{EX}Product2> <http://www.w3.org/2000/01/rdf-schema#label> "product 2" .
  <{EX}Product10> <http://www.w3.org/2000/01/rdf-schema#label> "product 10" .
  <{EX}Product3> <{EX}producer> <{EX}Producer3> .
}}"""


def ox_term(t):
    if t is None:
        return None
    if isinstance(t, ox.NamedNode):
        return ("uri", t.value)
    if isinstance(t, ox.BlankNode):
        return ("bnode",)
    if t.language:
        return ("lit", t.value, "lang", t.language)
    dt = t.datatype.value
    if dt == f"{XSD}string":
        return ("lit", t.value, "str")
    if dt == f"{XSD}decimal":
        return ("lit", str(float(t.value)), dt)
    if dt == f"{XSD}dateTime":
        return ("lit", t.value.replace("Z", ""), dt)
    return ("lit", t.value, dt)


def json_term(b):
    if b is None:
        return None
    if b["type"] == "uri":
        return ("uri", b["value"])
    if b["type"] == "bnode":
        return ("bnode",)
    if "xml:lang" in b:
        return ("lit", b["value"], "lang", b["xml:lang"])
    dt = b.get("datatype", f"{XSD}string")
    if dt == f"{XSD}string":
        return ("lit", b["value"], "str")
    if dt == f"{XSD}decimal":
        return ("lit", str(float(b["value"])), dt)
    if dt == f"{XSD}dateTime":
        return ("lit", b["value"].replace("Z", ""), dt)
    return ("lit", b["value"], dt)


def counted(f):
    if not HAS_COUNTERS:
        return f(), {}
    native._reset_fast_path_counts()
    out = f()
    return out, {k: v for k, v in native._fast_path_counts().items() if v and k.startswith("inl.")}


class Engines:
    """maplib in one storage mode and Oxigraph, with the same triples."""

    def __init__(self, storage):
        self.storage = storage
        self.folder = tempfile.mkdtemp(prefix=f"maplib_inl_{storage}_")
        kwargs = {"storage": storage}
        if storage != "memory":
            kwargs["storage_folder"] = os.path.join(self.folder, "storage")
        self.m = Model(**kwargs)
        self.m.reads(DATA, format="ntriples")
        self.m.reads(DATA, format="ntriples", graph=f"{EX}g")
        self.ox = ox.Store()
        self.ox.load(DATA.encode(), ox.RdfFormat.N_TRIPLES)
        self.ox.load(DATA.encode(), ox.RdfFormat.N_TRIPLES, to_graph=ox.NamedNode(f"{EX}g"))

    def update(self, u):
        self.m.update(PFX + u)
        self.ox.update(PFX + u)

    def close(self):
        del self.m
        shutil.rmtree(self.folder, ignore_errors=True)


STORAGES = disk_params()


@pytest.fixture(scope="module", params=STORAGES)
def eng(request):
    e = Engines(request.param)
    yield e
    e.close()


def rows_ox(e, q, graph=None):
    kw = {"default_graph": ox.NamedNode(graph)} if graph else {}
    res = e.ox.query(PFX + q, **kw)
    variables = [v.value for v in res.variables]
    return variables, Counter(tuple(ox_term(r[v]) for v in variables) for r in res)


def rows_maplib(e, q, graph=None):
    import json

    kw = {"graph": graph} if graph else {}
    d = json.loads(e.m.query(PFX + q, return_json=True, cache=False, **kw))
    variables = d["head"]["vars"]
    return variables, Counter(
        tuple(json_term(b.get(v)) for v in variables) for b in d["results"]["bindings"]
    )


def check(e, q, expect=None, graph=None):
    """Compares with Oxigraph; `expect` is the INL counter that must be hit in memory storage
    ("inl.taken", "inl.declined_types", ...). In hybrid and disk storage the dictionary is on
    disk, so every pattern is declined for storage."""
    (av, ar), counts = counted(lambda: rows_maplib(e, q, graph))
    ev, er = rows_ox(e, q, graph)
    assert sorted(av) == sorted(ev), q
    idx = [av.index(v) for v in ev]
    ar = Counter({tuple(r[i] for i in idx): c for r, c in ar.items()})
    assert ar == er, q
    if HAS_COUNTERS and expect is not None:
        if e.storage == "memory":
            assert counts.get(expect, 0) > 0, f"{expect} not hit: {counts} for {q}"
            if expect != "inl.taken":
                assert counts.get("inl.taken", 0) == 0, counts
        else:
            assert counts.get("inl.taken", 0) == 0, counts
            assert counts.get("inl.declined_storage", 0) > 0, counts
    return sum(er.values())


P = f"<{EX}Product17>"
T = f"<{EX}Type2>"
F = f"<{EX}F5>"

TAKEN = {
    # BSBM explore Q1's star from two constant objects (type and feature).
    "q1_star": f"SELECT ?p ?l WHERE {{ ?p a {T} . ?p :feature {F} . ?p rdfs:label ?l }}",
    "q1_full": f"""SELECT DISTINCT ?p ?l WHERE {{ ?p rdfs:label ?l . ?p a {T} . ?p :feature {F} .
                   ?p :num1 ?v FILTER(?v > 300) }} ORDER BY ?l LIMIT 10""",
    # Q2's star around a constant product, with chains and native values.
    "q2_star": f"""SELECT * WHERE {{ {P} rdfs:label ?l . {P} :comment ?c . {P} :producer ?pr .
                   ?pr rdfs:label ?prl . {P} :publisher ?pr . {P} :feature ?f . ?f rdfs:label ?fl .
                   {P} :num1 ?n1 }}""",
    "q2_optional": f"""SELECT * WHERE {{ {P} rdfs:label ?l . {P} :producer ?pr . ?pr rdfs:label ?prl
                   OPTIONAL {{ {P} :num2 ?n2 }} OPTIONAL {{ {P} :text ?t }} }}""",
    # Q7's chains from a constant object.
    "q7_chain": f"SELECT * WHERE {{ ?o :product {P} . ?o :vendor ?v . ?v rdfs:label ?vl . ?o :publisher ?v . ?o :price ?pr . ?o :validTo ?d }}",
    "q7_reviews": f"SELECT * WHERE {{ ?r :reviewFor {P} . ?r :reviewer ?who . ?r :rating ?rt }}",
    # Blank node variables and a constant string object.
    "blank_var": f"SELECT ?p WHERE {{ ?p :feature {F} . ?p :producer _:x . _:x rdfs:label ?l }}",
    "const_string": 'SELECT ?p ?f WHERE { ?p rdfs:label "product 17" . ?p :feature ?f }',
    # A shared label: several subjects for one constant object.
    "shared_label": 'SELECT ?p ?c WHERE { ?p rdfs:label "product 3" . ?p :comment ?c }',
    # The object of a mixed predicate, restricted by a later pattern's type.
    "chain_of_three": f"SELECT * WHERE {{ {P} :producer ?pr . ?pr :country ?c . ?p :producer ?pr }}",
}

EMPTY = {
    "absent_constant": f"SELECT * WHERE {{ <{EX}NoSuchProduct> rdfs:label ?l . <{EX}NoSuchProduct> :num1 ?n }}",
    "absent_string": 'SELECT * WHERE { ?p rdfs:label "no such label" . ?p :num1 ?n }',
    "no_match": f"SELECT * WHERE {{ ?p a {T} . ?p :feature <{EX}F5> . ?p :producer <{EX}Vendor1> }}",
}

DECLINED = {
    "variable_predicate": (f"SELECT * WHERE {{ {P} ?pp ?o . {P} rdfs:label ?l }}", "inl.declined_shape"),
    "no_constant": ("SELECT * WHERE { ?p :producer ?pr . ?pr :country ?c }", "inl.declined_shape"),
    "not_connected": (f"SELECT * WHERE {{ {P} rdfs:label ?l . ?x :country ?c }}", "inl.declined_shape"),
    "lang_strings": ("SELECT * WHERE { ?r :reviewFor <http://ex.org/Product17> . ?r :title ?t }", "inl.declined_types"),
    "mixed_objects": (f"SELECT * WHERE {{ <{EX}Product7> :mixed ?m }}", "inl.declined_types"),
    "blank_subjects": (f"SELECT * WHERE {{ ?r :reviewFor {P} . ?r :reviewer ?who . ?who :name ?n }}", "inl.declined_types"),
    "native_constant": ('SELECT * WHERE { ?p :num1 5 . ?p rdfs:label ?l }', "inl.declined_shape"),
}


@pytest.mark.parametrize("name", TAKEN)
def test_taken(eng, name):
    check(eng, TAKEN[name], "inl.taken")


@pytest.mark.parametrize("name", EMPTY)
def test_empty(eng, name):
    # Found empty, and handed to the Polars path, which types the empty columns.
    assert check(eng, EMPTY[name], "inl.empty") == 0


def test_empty_optional_columns_typed_as_without_inl(eng, monkeypatch):
    """An OPTIONAL whose right side has no solutions gives the same frame, column types
    included, with and without index nested loops."""
    q = PFX + f"SELECT ?l ?t WHERE {{ <{EX}Product1> rdfs:label ?l OPTIONAL {{ <{EX}Product1> :text ?t }} }}"
    with_inl = eng.m.query(q, cache=False)
    monkeypatch.setenv("MAPLIB_INL", "0")
    without = eng.m.query(q, cache=False)
    assert with_inl.schema == without.schema
    assert with_inl.equals(without)


@pytest.mark.parametrize("name", DECLINED)
def test_declined(eng, name):
    q, why = DECLINED[name]
    check(eng, q, why)


def test_around(eng):
    # The pattern inside OPTIONAL, UNION and FILTER, and as the left side of joins.
    check(eng, f"SELECT * WHERE {{ {P} rdfs:label ?l OPTIONAL {{ ?o :product {P} . ?o :vendor ?v }} }}")
    check(eng, f"SELECT * WHERE {{ {{ {P} rdfs:label ?l }} UNION {{ ?p a {T} . ?p :feature {F} }} }}")
    check(eng, f"SELECT * WHERE {{ ?p a {T} . ?p :num1 ?n FILTER(?n < 200) }}")
    check(eng, f"SELECT (COUNT(*) AS ?c) WHERE {{ ?p a {T} . ?p :feature ?f }}")
    check(eng, f"SELECT ?l WHERE {{ {P} rdfs:label ?l FILTER NOT EXISTS {{ {P} :text ?t }} }}")


def test_named_graph(eng):
    check(eng, TAKEN["q2_star"], "inl.taken", graph=f"{EX}g")
    check(eng, TAKEN["q7_chain"], "inl.taken", graph=f"{EX}g")
    check(eng, f"SELECT * WHERE {{ GRAPH <{EX}g> {{ {P} rdfs:label ?l . {P} :producer ?pr }} }}")


def test_after_updates(eng):
    """The write buffer after a small insert, several segments after a larger one, and the
    chunked columns that deletes leave."""
    eng.update(UPDATE_INSERT)
    for name in ["q1_star", "q2_star", "q7_chain", "const_string"]:
        check(eng, TAKEN[name], "inl.taken")
    check(eng, f"SELECT * WHERE {{ ?p a <{EX}Type1> . ?p :feature <{EX}F3> . ?p rdfs:label ?l }}", "inl.taken")
    eng.update(UPDATE_DELETE)
    for name in TAKEN:
        check(eng, TAKEN[name], "inl.taken")
    more = "\n".join(
        f"<{EX}Extra{i}> <{EX}feature> <{EX}F{i % N_FEATURES}> .\n"
        f'<{EX}Extra{i}> <http://www.w3.org/2000/01/rdf-schema#label> "extra {i}" .\n'
        f"<{EX}Extra{i}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{EX}Type2> ."
        for i in range(6000)
    )
    eng.m.reads(more, format="ntriples")
    eng.ox.load(more.encode(), ox.RdfFormat.N_TRIPLES)
    for name in TAKEN:
        check(eng, TAKEN[name])
    check(eng, f"SELECT ?p ?l WHERE {{ ?p :feature {F} . ?p rdfs:label ?l }}")


def test_kill_switch(eng, monkeypatch):
    monkeypatch.setenv("MAPLIB_INL", "0")
    _, counts = counted(lambda: rows_maplib(eng, TAKEN["q2_star"]))
    assert not counts


def test_thresholds(eng, monkeypatch):
    """Both sides of the estimate and of the run-time guard give the same results. The cost
    comparison with the Polars path is turned off (ratio 0), so that only the limits decide."""
    monkeypatch.setenv("MAPLIB_INL_COST_RATIO", "0")
    q = f"SELECT * WHERE {{ ?p a {T} . ?p :feature ?f . ?f rdfs:label ?fl }}"
    check(eng, q, "inl.taken")
    monkeypatch.setenv("MAPLIB_INL_MAX_WORK", "50")
    check(eng, q, "inl.declined_estimate")
    monkeypatch.setenv("MAPLIB_INL_MAX_WORK", "100000000")
    monkeypatch.setenv("MAPLIB_INL_MAX_ROWS", "20")
    check(eng, q)
    _, counts = counted(lambda: rows_maplib(eng, q))
    if HAS_COUNTERS and eng.storage == "memory":
        assert counts.get("inl.declined_estimate", 0) + counts.get("inl.aborted", 0) > 0, counts
        assert counts.get("inl.taken", 0) == 0, counts


def test_model_without_iris():
    """A model with no IRIs stored (empty, or only blank nodes and literals) has no IRI
    dictionary; index nested loops decline instead of looking it up."""
    m = Model()
    assert m.query(PFX + f"SELECT * WHERE {{ {P} rdfs:label ?l . ?x :p ?l }}").height == 0
    m.reads('_:a <http://ex.org/p> "x" .', format="ntriples")
    assert m.query(PFX + 'SELECT * WHERE { ?s :p "x" . ?s :p ?o }').height == 1


def test_cost_ratio(eng, monkeypatch):
    """INL is chosen only when its estimated time is below the Polars path's; the ratio moves
    the decision to both sides with the same results."""
    q = f"SELECT * WHERE {{ ?p a {T} . ?p :feature ?f . ?f rdfs:label ?fl }}"
    monkeypatch.setenv("MAPLIB_INL_COST_RATIO", "0")
    check(eng, q, "inl.taken")
    monkeypatch.setenv("MAPLIB_INL_COST_RATIO", "1000000")
    check(eng, q, "inl.declined_estimate")


def test_star_over_every_instance_declines():
    """A star over every instance of a class (trainmarks' OPTIONAL + GROUP BY, 2026-11-20
    regression: 20,000 customers, two probes each) costs more by index nested loops than the
    Polars path's joins, so it is declined by the cost estimate."""
    n = 20_000
    lines = []
    for i in range(n):
        c = f"<{EX}C{i}>"
        lines.append(f"{c} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{EX}Customer> .")
        lines.append(f'{c} <{EX}country> "country {i % 30}" .')
        lines.append(f'{c} <{EX}segment> "segment {i % 4}" .')
    data = "\n".join(lines)
    m = Model()
    m.reads(data, format="ntriples")
    q = PFX + "SELECT ?c ?co ?s WHERE { ?c a :Customer ; :country ?co ; :segment ?s }"
    df, counts = counted(lambda: m.query(q))
    assert df.height == n
    if HAS_COUNTERS:
        assert counts.get("inl.declined_estimate", 0) > 0, counts
        assert counts.get("inl.taken", 0) == 0, counts
    # A selective star over the same tables is still taken.
    q = PFX + f"SELECT ?co ?s WHERE {{ <{EX}C7> a :Customer ; :country ?co ; :segment ?s }}"
    df, counts = counted(lambda: m.query(q))
    assert df.height == 1
    if HAS_COUNTERS:
        assert counts.get("inl.taken", 0) > 0, counts

"""Differential tests of the query engine's fast paths against Oxigraph.

The fast paths of property paths, COUNT DISTINCT, RANK filters, the write buffer, the segment
Bloom filters and the basic graph patterns on dictionary codes each apply up to a threshold
(16,384 rows of a table, 500 EXISTS keys, 10,000 OPTIONAL rows, 64 path starts, 256 walk lookups,
65,536 codes, 4,096 buffered rows, ...). Most tests run on data that never crosses them. Here one
generated dataset has tables just below and just above each threshold, next to columns of mixed
types (IRIs, blank nodes, literals of several datatypes, language tags), named graphs and
duplicates. Each query runs on maplib and on Oxigraph, and the results are compared as multisets
of RDF terms (in order under ORDER BY).

The same final dataset is built in every storage mode (memory, and hybrid and disk with
maplib_ee) and in several states: read at once, read in parts that become several segments, read
in parts that are compacted, reached by small inserts and deletes that leave triples in the write
buffer (with deletes of segment and of buffered triples), and that state serialized and read
again. Each state is also built with the segment Bloom filters turned off
(`MAPLIB_SEGMENT_FILTER_BITS=0`).

Each case also checks, with the engine's fast path counters (`maplib.maplib._fast_path_counts`),
that it took the path it is meant to test, so that a case does not silently test the general
path instead.

Set `MAPLIB_DIFFERENTIAL_STORAGE` (comma separated) to limit the storage modes, and
`MAPLIB_DIFFERENTIAL_STATES` to limit the states."""

import functools
import json
import os
import random
import re
import shutil
import tempfile
from collections import Counter
from decimal import Decimal, InvalidOperation
from typing import Callable, Dict, List, Optional, Tuple

import pyoxigraph as ox
import pytest

import maplib.maplib as native
from maplib import IndexingOptions, Model

from .disk import disk_params

EX = "http://ex.org/"
XSD = "http://www.w3.org/2001/XMLSchema#"
RDF_LANG = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
PFX = f"PREFIX : <{EX}>\nPREFIX xsd: <{XSD}>\n"
G1 = f"{EX}g1"
G2 = f"{EX}g2"

FILTER_ENV = "MAPLIB_SEGMENT_FILTER_BITS"
# Read once per process: `test_dictionary_bloom_off` runs this module again with it set to 0.
DICTIONARY_BLOOM_ENV = "MAPLIB_DISK_DICTIONARY_BLOOM_BITS"
DICTIONARY_BLOOM_OFF = os.environ.get(DICTIONARY_BLOOM_ENV, "").strip() == "0"

# The thresholds, as in the engine's code.
SMALL_TABLE_HEIGHT = 16_384  # query_processing::pushdowns::SMALL_TABLE_HEIGHT
EXISTS_KEYS = 500  # query_processing::pushdowns::SMALL_HEIGHT
OPTIONAL_KEYS_HEIGHT = 10_000  # left_join.rs
WALK_MAX_STARTS = 64  # path.rs
WALK_MAX_LOOKUPS = 256  # path.rs
SERIAL_CODES = 1 << 16  # count.rs
MAX_BUFFER_HEIGHT = 4096  # deduplication.rs
MAX_FILTERED_TARGETS = 128  # the cap of the old term path in dblf.rs, kept as a case size
# storage.rs: seek in the sparse index when targets * log2(keys) <= keys, with a key per 100 rows.
# The 65,537-row :cd65537 table has 656 keys (10 bits): seek up to 65 targets.
SEEK_TARGETS = 65
MAX_COUNT_TERMS = 16
JSON_CHUNK_ROWS = 4096  # sparql_json.rs MIN_CHUNK_ROWS: one chunk is written on the calling thread  # count_joins.rs MAX_TERMS: UNION terms a counted pattern may expand to


# ---------------------------------------------------------------------------
# Fast path counters
# ---------------------------------------------------------------------------


# Builds without the counters (before they were added) compare results only.
HAS_COUNTERS = hasattr(native, "_fast_path_counts")


def counted(f: Callable):
    """The result of f() and the fast path counts it added."""
    if not HAS_COUNTERS:
        return f(), {}
    native._reset_fast_path_counts()
    out = f()
    return out, {k: v for k, v in native._fast_path_counts().items() if v}


def report(storage, state, toggles, name, counts, expected):
    """With MAPLIB_DIFFERENTIAL_REPORT set to a file, adds a JSON line per case run, for the
    coverage table (optimization x threshold side x storage mode x hit proven)."""
    path = os.environ.get("MAPLIB_DIFFERENTIAL_REPORT")
    if not path:
        return
    line = {"storage": storage, "state": state, "toggles": toggles, "case": name,
            "counts": counts, "expected": expected,
            "dictionary_bloom": "off" if DICTIONARY_BLOOM_OFF else "on"}
    with open(path, "a") as f:
        f.write(json.dumps(line) + "\n")


def check_hits(counts: Dict[str, int], expected: Dict[str, bool], what: str):
    if not HAS_COUNTERS:
        return
    # "a|b" means any of the fast paths a and b: taken if one is, not taken if none is.
    for label, taken in expected.items():
        labels = label.split("|")
        for one in labels:
            assert one in native._fast_path_counts(), f"unknown fast path {one}"
        n = sum(counts.get(one, 0) for one in labels)
        if taken:
            assert n > 0, f"{what}: fast path {label} not taken; counts {counts}"
        else:
            assert n == 0, f"{what}: {label} taken; counts {counts}"


# ---------------------------------------------------------------------------
# Terms and results
# ---------------------------------------------------------------------------

INTEGERS = {
    f"{XSD}{t}"
    for t in (
        "integer long int short byte nonNegativeInteger positiveInteger unsignedLong "
        "unsignedInt unsignedShort unsignedByte nonPositiveInteger negativeInteger"
    ).split()
}


def literal_key(value: str, datatype: Optional[str], lang: Optional[str]):
    if lang:
        return ("lit", value, RDF_LANG, lang.lower())
    datatype = datatype or f"{XSD}string"
    try:
        if datatype in INTEGERS:
            value = str(int(value))
        elif datatype == f"{XSD}decimal":
            value = str(Decimal(value).normalize())
        elif datatype in (f"{XSD}double", f"{XSD}float"):
            value = repr(float(value))
        elif datatype == f"{XSD}boolean":
            value = {"1": "true", "0": "false"}.get(value, value)
    except (ValueError, InvalidOperation):
        pass
    return ("lit", value, datatype, None)


def ox_term(t):
    if t is None:
        return None
    if isinstance(t, ox.NamedNode):
        return ("iri", t.value)
    if isinstance(t, ox.BlankNode):
        return ("bnode",)
    lang = t.language
    return literal_key(t.value, t.datatype.value if not lang else None, lang)


def json_term(x):
    if x is None:
        return None
    if x["type"] == "uri":
        return ("iri", x["value"])
    if x["type"] == "bnode":
        return ("bnode",)
    return literal_key(x["value"], x.get("datatype"), x.get("xml:lang"))


def oxigraph_rows(store: ox.Store, query: str, variables: List[str]):
    res = store.query(PFX + query)
    if isinstance(res, ox.QueryBoolean):
        return [(("lit", "true" if bool(res) else "false", f"{XSD}boolean", None),)]
    return [tuple(ox_term(r[v]) for v in variables) for r in res]


def maplib_rows(m: Model, query: str, variables: List[str]):
    d = json.loads(m.query(PFX + query, return_format="json"))
    if "boolean" in d:
        return [(("lit", "true" if d["boolean"] else "false", f"{XSD}boolean", None),)]
    return [tuple(json_term(b.get(v)) for v in variables) for b in d["results"]["bindings"]]


def show(rows, limit=12):
    c = Counter(rows)
    items = sorted(c.items(), key=repr)[:limit]
    return "\n".join(f"  {n} x {r}" for r, n in items) + (
        f"\n  ... ({len(c)} distinct rows)" if len(c) > limit else ""
    )


def assert_same_rows(expected, actual, ordered: bool, what: str):
    if ordered:
        if expected == actual:
            return
    elif Counter(expected) == Counter(actual):
        return
    e, a = Counter(expected), Counter(actual)
    raise AssertionError(
        f"{what}: {len(actual)} rows from maplib, {len(expected)} from Oxigraph\n"
        f"only in Oxigraph:\n{show(list((e - a).elements()))}\n"
        f"only in maplib:\n{show(list((a - e).elements()))}"
    )


# ---------------------------------------------------------------------------
# The dataset
# ---------------------------------------------------------------------------


def iri(name: str) -> str:
    return f"<{EX}{name}>"


def int_lit(i: int) -> str:
    return f'"{i}"^^<{XSD}integer>'


MIXED_OBJECTS = [
    lambda i: iri(f"m{i % 40}"),
    lambda i: f"_:mb{i % 7}",
    lambda i: int_lit(i % 13),
    lambda i: f'"s{i % 11}"',
    lambda i: f'"hei{i % 5}"@no',
    lambda i: f'"hello{i % 5}"@en',
    lambda i: f'"2024-01-{1 + i % 28:02d}"^^<{XSD}date>',
    lambda i: f'"{(i % 9) / 4}"^^<{XSD}double>',
    lambda i: f'"{"true" if i % 2 else "false"}"^^<{XSD}boolean>',
    lambda i: f'"{i % 17}.5"^^<{XSD}decimal>',
]


@functools.lru_cache(maxsize=None)
def dataset() -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    """(lines, pinned): the N-Quads lines of the final dataset, and those that must be read in
    the first part of every state (blank nodes are scoped to the document they are read in)."""
    rng = random.Random(7)
    lines: List[str] = []
    pinned: List[str] = []

    def add(s, p, o, g=None, pin=False):
        line = f"{s} {iri(p)} {o}{' <' + g + '>' if g else ''} ."
        (pinned if pin else lines).append(line)

    # Small tables at the BGP threshold, joined with a small star.
    for n in (SMALL_TABLE_HEIGHT, SMALL_TABLE_HEIGHT + 1):
        for i in range(n):
            add(iri(f"x{i}"), f"t{n}", iri(f"y{i % 997}"))
    for i in range(200):
        add(iri(f"x{i * 3}"), "sa", f'"a{i}"')
        add(iri(f"x{i * 3}"), "sb", int_lit(i % 17))

    # EXISTS keys at 500.
    for n in (EXISTS_KEYS, EXISTS_KEYS + 1):
        for i in range(n):
            add(iri(f"x{i}"), f"e{n}", iri(f"y{i % 31}"))
    for i in range(0, 1200, 2):
        add(iri(f"x{i}"), "ein", f'"in{i % 5}"')

    # OPTIONAL left sides at 10,000 rows.
    for n in (OPTIONAL_KEYS_HEIGHT, OPTIONAL_KEYS_HEIGHT + 1):
        for i in range(n):
            add(iri(f"x{i}"), f"o{n}", int_lit(i % 101))
    for i in range(0, 12_000, 3):
        add(iri(f"x{i}"), "oin", f'"w{i % 19}"')
        if i % 9 == 0:
            add(iri(f"x{i}"), "oin", f'"w{i % 19}"@en')

    # COUNT DISTINCT at 65,536 codes.
    for n in (SERIAL_CODES, SERIAL_CODES + 1):
        for i in range(n):
            add(iri(f"x{i}"), f"cd{n}", iri(f"y{i % 5003}"))

    # Paths: a long chain (walks past the lookup budget), a random graph with cycles (many
    # starts), a second predicate for sequences, literals and blank nodes at the ends.
    for i in range(400):
        add(iri(f"c{i}"), "chain", iri(f"c{i + 1}"))
    for i in range(300):
        add(iri(f"w{i}"), "walk", iri(f"w{rng.randrange(300)}"))
        if i % 3 == 0:
            add(iri(f"w{i}"), "walk", iri(f"w{(i + 1) % 300}"))
        if i % 4 == 0:
            add(iri(f"w{i}"), "walk2", iri(f"w{rng.randrange(300)}"))
    for i in range(10):
        add(iri(f"w{i * 7}"), "walk", f'"end{i}"')
        add(f"_:wb{i}", "walk", iri(f"w{i * 11}"), pin=True)
    for i in range(30):
        add(iri(f"w{i}"), "walk", iri(f"w{i + 1}"), g=G1)
    # A forest of short IRI chains: walks from up to 64 starts stay within the budget. (A
    # predicate with any literal or non-IRI object, like :walk, is never walked on codes.)
    for k in range(100):
        for i in range(3):
            add(iri(f"t{k}_{i}"), "tw", iri(f"t{k}_{i + 1}"))
        if k % 10 == 0:
            add(iri(f"t{k}_3"), "tw", iri(f"t{(k + 1) % 100}_0"))

    # Mixed types in one column, in the default graph and in two named graphs, with duplicates.
    for i in range(600):
        s = iri(f"m{i % 150}") if i % 10 else f"_:ms{i % 6}"
        o = MIXED_OBJECTS[i % len(MIXED_OBJECTS)](i)
        pin = s.startswith("_:") or o.startswith("_:")
        add(s, "mix", o, pin=pin)
        if i % 4 == 0:
            add(s, "mix", o, g=G1 if i % 8 else G2, pin=pin)
        if i % 25 == 0:
            add(s, "mix", o, pin=pin)  # a duplicate
        if i % 3 == 0:
            add(iri(f"m{i % 150}"), "mix2", MIXED_OBJECTS[(i // 3) % len(MIXED_OBJECTS)](i),
                pin=True)
    for i in range(150):
        if i % 2 == 0:
            add(iri(f"m{i}"), "label", f'"label {i % 20}"@en')
        if i % 3 == 0:
            add(iri(f"m{i}"), "label", f'"merke {i % 20}"@no', g=G2)
        add(iri(f"m{i}"), "num", int_lit(i % 9) if i % 5 else f'"{i % 9}.0"^^<{XSD}double>')

    # RANK = 1 with ties and unbound keys.
    for i in range(400):
        add(iri(f"r{i}"), "grp", iri(f"gr{i % 23}"))
        if i % 7:
            add(iri(f"r{i}"), "score", int_lit((i * 37) % 11))

    # Join estimates bounded per key: one :one per subject, many :many.
    for i in range(3000):
        add(iri(f"x{i}"), "one", f'"one{i}"')
        add(iri(f"x{i % 50}"), "many", iri(f"y{i}"))

    # A predicate with only literal objects, for patterns proven empty.
    for i in range(100):
        add(iri(f"x{i}"), "lit", f'"l{i}"')

    return tuple(lines), tuple(pinned)


@functools.lru_cache(maxsize=None)
def oxigraph_store() -> ox.Store:
    lines, pinned = dataset()
    store = ox.Store()
    store.bulk_load(("\n".join(pinned + lines) + "\n").encode(), ox.RdfFormat.N_QUADS)
    return store


@functools.lru_cache(maxsize=None)
def expected_rows(query: str, variables: Tuple[str, ...]):
    return oxigraph_rows(oxigraph_store(), query, list(variables))


# ---------------------------------------------------------------------------
# The states
# ---------------------------------------------------------------------------

STATES = ["fresh", "object_index", "analyzed", "segments", "compacted", "buffered", "reloaded"]
# The states built again with the segment Bloom filters off.
FILTER_OFF_STATES = ["segments", "buffered", "reloaded"]
# The states also queried with the table statistics off (MAPLIB_TABLE_STATISTICS=off, read for
# every basic graph pattern).
STATISTICS_OFF_STATES = ["fresh", "analyzed", "buffered"]
STATISTICS_ENV = "MAPLIB_TABLE_STATISTICS"
# The states also queried with index nested loops off (MAPLIB_INL=0, read per query).
INL_OFF_STATES = ["fresh", "buffered"]
# Environment variables set while a world's queries run, by toggle.
QUERY_ENV = {"stats_off": {STATISTICS_ENV: "off"}, "inl_off": {"MAPLIB_INL": "0"}}


def junk_lines(rng: random.Random, n: int, tag: str) -> List[str]:
    """Triples that are inserted and deleted again, in tables the queries read."""
    out = []
    for i in range(n):
        k = rng.randrange(6)
        if k == 0:
            out.append(f"{iri(f'x{i}')} {iri('many')} {iri(f'j{tag}{i}')} .")
        elif k == 1:
            out.append(f"{iri(f'x{i}')} {iri('sb')} {int_lit(1000 + i)} .")
        elif k == 2:
            out.append(f"{iri(f'm{i % 150}')} {iri('mix')} \"junk{tag}{i}\"@en .")
        elif k == 3:
            out.append(f"{iri(f'w{i % 300}')} {iri('walk')} {iri(f'w{(i * 13) % 300}')} <{G1}> .")
        elif k == 4:
            out.append(f"{iri(f'r{i}')} {iri('score')} {int_lit(50 + i)} .")
        else:
            out.append(f"{iri(f'c{i}')} {iri('ein')} \"junk{tag}{i}\" .")
    final = set(dataset()[0])
    return [line for line in out if line not in final]


def quad_block(lines: List[str]) -> str:
    """N-Quads lines as the body of a SPARQL data block or template, with GRAPH blocks."""
    default, graphs = [], {}
    for line in lines:
        terms = split_terms(line.strip()[:-1])
        if len(terms) == 4:
            graphs.setdefault(terms[3], []).append(" ".join(terms[:3]) + " .")
        else:
            default.append(" ".join(terms) + " .")
    parts = ["\n".join(default)]
    for g, ls in graphs.items():
        parts.append(f"GRAPH {g} {{\n" + "\n".join(ls) + "\n}")
    return "\n".join(parts)


def data_update(kind: str, lines: List[str]) -> str:
    """INSERT DATA or DELETE DATA of N-Quads lines."""
    return f"{kind} {{\n{quad_block(lines)}\n}}"


def split_terms(s: str) -> List[str]:
    """The terms of an N-Triples line without the final dot (no spaces in IRIs or blank nodes;
    literals may hold spaces)."""
    terms, i = [], 0
    s = s.strip()
    while i < len(s):
        if s[i] == '"':
            j = i + 1
            while s[j] != '"' or s[j - 1] == "\\":
                j += 1
            k = s.find(" ", j)
            k = len(s) if k == -1 else k
        else:
            k = s.find(" ", i)
            k = len(s) if k == -1 else k
        terms.append(s[i:k])
        i = k + 1
        while i < len(s) and s[i] == " ":
            i += 1
    return terms


def nquads(lines) -> str:
    return "\n".join(lines) + "\n"


def build_state(state: str, model_kwargs: dict, folder: str) -> Model:
    lines, pinned = dataset()
    lines, pinned = list(lines), list(pinned)
    rng = random.Random(11)
    if state in ("fresh", "object_index", "analyzed"):
        if state == "object_index":
            model_kwargs = dict(model_kwargs, indexing_options=IndexingOptions(object_sort_all=True))
        m = Model(**model_kwargs)
        m.reads(nquads(pinned + lines), format="nquads")
        if state == "analyzed":
            m.analyze(deep=True)
        return m
    shuffled = lines[:]
    rng.shuffle(shuffled)
    n = len(shuffled)
    if state == "segments":
        # Decreasing parts, each less than half the one before: every part becomes a segment
        # of its own (or goes to the write buffer when small).
        cuts = [0, int(n * 0.70), int(n * 0.92), int(n * 0.98), n]
    elif state == "compacted":
        # Increasing parts: each is merged with the segments before it.
        cuts = [0, int(n * 0.1), int(n * 0.3), int(n * 0.6), n]
    else:
        cuts = None
    m = Model(**model_kwargs)
    if cuts:
        for i, (a, b) in enumerate(zip(cuts, cuts[1:])):
            part = shuffled[a:b] + (pinned if i == 0 else [])
            m.reads(nquads(part), format="nquads")
        return m
    # buffered / reloaded: the data without a tail, with junk; then the tail inserted and the
    # junk deleted by small updates, and more junk inserted and deleted while buffered.
    tail = shuffled[-1500:]
    head = shuffled[:-1500]
    junk = junk_lines(rng, 600, "a")
    junk2 = junk_lines(rng, 300, "b")
    m.reads(nquads(pinned + head + junk), format="nquads")
    for i in range(0, len(tail), 250):
        m.update(data_update("INSERT DATA", tail[i:i + 250]))
    m.update(data_update("INSERT DATA", junk2))
    for i in range(0, len(junk), 200):
        m.update(data_update("DELETE DATA", junk[i:i + 200]))
    m.update(data_update("DELETE DATA", junk2[:150]))
    # DELETE/INSERT with an empty WHERE, as data updates.
    tmp = f"{iri('x1')} {iri('tmp')} {iri('x2')} ."
    m.update(f"DELETE {{ {quad_block(junk2[150:200])} }} INSERT {{ {tmp} }} WHERE {{}}")
    m.update(f"DELETE DATA {{ {iri('x1')} {iri('tmp')} {iri('x2')} . }}")
    m.update(data_update("DELETE DATA", junk2[200:]))
    if state == "reloaded":
        path = os.path.join(folder, "model.maplib")
        m.serialize(path)
        del m
        kwargs = dict(model_kwargs)
        if "storage_folder" in kwargs:
            kwargs["storage_folder"] = os.path.join(folder, "reloaded_storage")
        m = Model.deserialize(path, **kwargs)
    return m


def selected(env: str, all_values: List[str]) -> List[str]:
    v = os.environ.get(env)
    if not v:
        return all_values
    want = [x.strip() for x in v.split(",")]
    return [x for x in all_values if x in want]


WORLDS = [
    (storage, state, filters)
    for storage in selected("MAPLIB_DIFFERENTIAL_STORAGE", disk_params())
    for state in selected("MAPLIB_DIFFERENTIAL_STATES", STATES)
    for filters in (["on"] + (["off"] if state in FILTER_OFF_STATES else [])
                    + (["stats_off"] if state in STATISTICS_OFF_STATES else [])
                    + (["inl_off"] if state in INL_OFF_STATES else []))
]


class World:
    def __init__(self, storage: str, state: str, filters: str):
        self.storage, self.state, self.filters = storage, state, filters
        self.folder = tempfile.mkdtemp(prefix=f"maplib_diff_{storage}_{state}_")
        kwargs = {"storage": storage}
        if storage != "memory":
            kwargs["storage_folder"] = os.path.join(self.folder, "storage")
        old = os.environ.get(FILTER_ENV)
        if filters == "off":
            os.environ[FILTER_ENV] = "0"
        try:
            self.model, self.build_counts = counted(lambda: build_state(state, kwargs, self.folder))
        finally:
            if old is None:
                os.environ.pop(FILTER_ENV, None)
            else:
                os.environ[FILTER_ENV] = old

    def close(self):
        del self.model
        shutil.rmtree(self.folder, ignore_errors=True)


@pytest.fixture(scope="module", params=WORLDS, ids=lambda w: "-".join(w))
def world(request):
    w = World(*request.param)
    yield w
    w.close()


# ---------------------------------------------------------------------------
# The cases
# ---------------------------------------------------------------------------


class Case:
    def __init__(self, name, query, variables, hits=None, ordered=False, reference=None,
                 only=None, env=None):
        """`hits`: fast path label -> whether the case must take it (True) or must not (False);
        a value may also be a function of the world. `reference`: the query to run on Oxigraph
        when it differs (e.g. for maplib's window functions). `only`: a function of the world
        that tells whether the case applies."""
        self.name, self.query, self.variables = name, query, tuple(variables)
        self.hits = hits or {}
        self.ordered = ordered
        self.reference = reference or query
        self.only = only
        self.env = env or {}

    def expected_hits(self, w: World) -> Dict[str, bool]:
        out = {}
        for k, v in self.hits.items():
            v = v(w) if callable(v) else v
            if v is not None:
                out[k] = v
        return out


def filters_on(w: World) -> Optional[bool]:
    """Filters skip segments only when there are filters and several segments (or tables) to
    skip; with filters off, they never do."""
    return False if w.filters == "off" else None


def values(prefix: str, ids) -> str:
    return " ".join(iri(f"{prefix}{i}") for i in ids)


CASES: List[Case] = []


def case(*args, **kwargs):
    CASES.append(Case(*args, **kwargs))


# BGPs over small tables (at most 16,384 rows) are joined within the plan.
for n, small in ((SMALL_TABLE_HEIGHT, True), (SMALL_TABLE_HEIGHT + 1, False)):
    case(
        f"bgp_small_table_{n}",
        f"SELECT ?s ?a ?y WHERE {{ ?s :sa ?a . ?s :t{n} ?y }}",
        ["s", "a", "y"],
        hits={"bgp.small_table_in_plan": True if small else None,
              "bgp.values_pushed": None if small else True},
    )
    case(
        f"bgp_small_table_star_{n}",
        f"SELECT ?s ?a ?b ?y WHERE {{ ?s :sa ?a . ?s :sb ?b . ?s :t{n} ?y . ?y ^:t{n} ?s2 }}",
        ["s", "a", "b", "y"],
        hits={"bgp.small_table_in_plan": True if small else None,
              "bgp.values_pushed": None if small else True},
    )
    case(
        f"bgp_small_table_optional_union_{n}",
        f"""SELECT ?s ?a ?y ?l WHERE {{
            {{ ?s :sa ?a }} UNION {{ VALUES (?s ?a) {{ (:x3 UNDEF) (:x999999 "z") }} }}
            ?s :t{n} ?y
            OPTIONAL {{ ?s :lit ?l }} }}""",
        ["s", "a", "y", "l"],
    )

# A pattern whose variables can have no stored type has no solutions.
case("bgp_proven_empty_literal_subject",
     "SELECT ?s ?o ?z WHERE { ?s :lit ?o . ?o :t16384 ?z }", ["s", "o", "z"],
     hits={"bgp.proven_empty": True})
case("bgp_proven_empty_missing_predicate",
     "SELECT ?s ?o WHERE { ?s :nothing ?o . ?s :sa ?a }", ["s", "o"],
     hits={"bgp.proven_empty": True})
case("bgp_proven_empty_constant_type",
     'SELECT ?s WHERE { ?s :sa 3 }', ["s"], hits={"bgp.proven_empty": True})
# The proof only uses the patterns, not the types of the incoming solutions: here ?q is an object
# of :sa (literals) and a subject of :t16384 (IRIs).
case("bgp_proven_empty_in_optional",
     "SELECT ?s ?a ?z WHERE { ?s :sa ?a OPTIONAL { ?s :t16384 ?z . ?z ^:t16384 ?x . ?x :sa ?q . ?q :t16384 ?w } }",
     ["s", "a", "z"], hits={"bgp.proven_empty": True})
case("bgp_not_proven_from_incoming_types",
     "SELECT ?s ?a ?z WHERE { ?s :sa ?a OPTIONAL { ?a :t16384 ?z } }", ["s", "a", "z"],
     hits={"bgp.proven_empty": False})
case("bgp_not_empty_mixed",
     "SELECT ?s ?o ?l WHERE { ?s :mix ?o . ?o :label ?l }", ["s", "o", "l"],
     hits={"bgp.proven_empty": False})
# GRAPH ?g: graphs without one of the predicates are proven empty on their own.
case("bgp_named_graphs",
     "SELECT ?g ?s ?l WHERE { GRAPH ?g { ?s :label ?l . ?s :mix ?o } }", ["g", "s", "l"])
case("bgp_not_empty_named_graph",
     "SELECT ?s ?l WHERE { GRAPH :g2 { ?s :label ?l . ?s :mix ?o } }", ["s", "l"],
     hits={"bgp.proven_empty": False})

# Join estimates bounded by the most triples that share a key.
case("join_estimate_bounded",
     "SELECT ?s ?y ?one WHERE { ?s :many ?y . ?s :one ?one }", ["s", "y", "one"],
     # The most triples per key is known for tables of one segment without a write buffer.
     hits={"join.estimate_bounded_per_key":
           lambda w: w.storage != "disk" and w.state in ("fresh", "object_index", "analyzed", "reloaded")})
# A key with a multi-type (mixed) column: no per-key bound for :mix tables of several types.
case("join_estimate_mixed",
     "SELECT ?s ?o ?l WHERE { ?s :mix ?o . ?s :label ?l }", ["s", "o", "l"])
case("join_estimate_bounded_chain",
     "SELECT ?s ?y ?z WHERE { ?s :e501 ?y . ?s :one ?o . ?s :many ?z }", ["s", "y", "z"])

# EXISTS keys collected when at most 500.
for n, few in ((EXISTS_KEYS, True), (EXISTS_KEYS + 1, False)):
    hits = {"exists.keys_collected": few, "exists.keys_lazy": not few}
    case(f"exists_{n}", f"SELECT ?s ?y WHERE {{ ?s :e{n} ?y FILTER EXISTS {{ ?s :ein ?v }} }}",
         ["s", "y"], hits=hits)
    case(f"not_exists_{n}",
         f"SELECT ?s ?y WHERE {{ ?s :e{n} ?y FILTER NOT EXISTS {{ ?s :ein \"in2\" }} }}",
         ["s", "y"], hits=hits)
    case(f"exists_two_keys_{n}",
         f"SELECT ?s ?y WHERE {{ ?s :e{n} ?y FILTER EXISTS {{ ?s :ein ?v . ?y ^:e{n} ?s }} }}",
         ["s", "y"], hits=hits)
    case(f"exists_unbound_{n}",
         f"""SELECT ?s ?v WHERE {{ ?s :e{n} ?y OPTIONAL {{ ?s :ein ?v }}
             FILTER EXISTS {{ ?s :e{n} ?y2 FILTER(!BOUND(?v) || STR(?v) != "in1") }} }}""",
         ["s", "v"])

# OPTIONAL evaluated with the values of a left side of at most 10,000 rows.
for n, few in ((OPTIONAL_KEYS_HEIGHT, True), (OPTIONAL_KEYS_HEIGHT + 1, False)):
    case(f"optional_keys_{n}", f"SELECT ?s ?k ?w WHERE {{ ?s :o{n} ?k OPTIONAL {{ ?s :oin ?w }} }}",
         ["s", "k", "w"], hits={"optional.left_keys": few, "optional.no_keys": not few})
    case(f"optional_keys_filter_{n}",
         f"""SELECT ?s ?k ?w WHERE {{ ?s :o{n} ?k
             OPTIONAL {{ ?s :oin ?w FILTER(LANG(?w) = "" || ?k > 50) }} }}""",
         ["s", "k", "w"], hits={"optional.left_keys": few})
    case(f"optional_keys_count_{n}",
         f"""SELECT (COUNT(?w) AS ?c) (COUNT(*) AS ?n) WHERE {{ ?s :o{n} ?k
             OPTIONAL {{ ?s :oin ?w }} }}""", ["c", "n"])
case("optional_mixed_left",
     "SELECT ?s ?o ?l WHERE { ?s :mix ?o OPTIONAL { ?o :label ?l } }", ["s", "o", "l"])
case("optional_lang_left",
     "SELECT ?s ?v ?t WHERE { ?s :mix ?v OPTIONAL { ?t :label ?v } }", ["s", "v", "t"])
case("optional_constant_right",
     'SELECT ?s ?k ?w WHERE { ?s :o10000 ?k OPTIONAL { ?s :oin "w3" BIND("y" AS ?w) } }',
     ["s", "k", "w"], hits={"optional.left_keys": False})
case("optional_union_left",
     """SELECT ?s ?a ?w WHERE { { ?s :sa ?a } UNION { ?s :lit ?a } UNION { VALUES ?s { :x0 :nope } }
        OPTIONAL { ?s :oin ?w } }""", ["s", "a", "w"])

# MINUS as an anti join, with mixed types and variables left unbound.
case("minus_mixed",
     "SELECT ?s ?o WHERE { ?s :mix ?o MINUS { ?s :mix2 ?o } }", ["s", "o"],
     hits={"minus.anti_join": True})
case("minus_unbound",
     """SELECT ?s ?o ?l WHERE { ?s :mix ?o OPTIONAL { ?s :label ?l }
        MINUS { ?s :label ?l FILTER(LANG(?l) = "en") } }""", ["s", "o", "l"],
     hits={"minus.anti_join": True})
case("minus_values_undef",
     """SELECT ?s ?o WHERE { ?s :mix ?o
        MINUS { VALUES (?s ?o) { (:m1 UNDEF) (UNDEF "s3") (:m2 "hei2"@no) } } }""", ["s", "o"],
     hits={"minus.anti_join": True})
case("minus_no_shared",
     "SELECT ?s WHERE { ?s :sa ?a MINUS { ?x :lit ?y } }", ["s"])
case("minus_large",
     "SELECT ?s ?k WHERE { ?s :o10001 ?k MINUS { ?s :oin ?w } }", ["s", "k"],
     hits={"minus.anti_join": True})

# COUNT DISTINCT with a bitset over codes, serially up to 65,536 codes. A table with one segment
# and an empty write buffer answers from the segment's exact count instead, and a repeated count
# from the cache keyed by the tables' generations; either takes the place of the bitset.
KNOWN_DISTINCT = "|count_distinct.segment_exact|count_distinct.cached"
for n, serial in ((SERIAL_CODES, True), (SERIAL_CODES + 1, False)):
    hits = {"count_distinct.bitset" + (KNOWN_DISTINCT if serial else ""): serial,
            "count_distinct.bitset_parallel" + ("" if serial else KNOWN_DISTINCT): not serial}
    case(f"count_distinct_s_{n}", f"SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE {{ ?s :cd{n} ?o }}",
         ["c"], hits=hits)
    case(f"count_distinct_o_{n}", f"SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE {{ ?s :cd{n} ?o }}",
         ["c"], hits=hits)
case("count_distinct_mixed_o", "SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s :mix ?o }", ["c"])
case("count_distinct_mixed_s", "SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { ?s :mix ?o }", ["c"],
     hits={"count_distinct.bitset" + KNOWN_DISTINCT: True})
case("count_distinct_named_graph",
     "SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { GRAPH <http://ex.org/g1> { ?s :walk ?o } }", ["c"])
case("count_distinct_s_buffered",
     "SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { ?s :many ?o }", ["c"],
     hits={"count_distinct.bitset" + KNOWN_DISTINCT: True})
case("count_distinct_o_buffered",
     "SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s :ein ?o }", ["c"],
     hits={"count_distinct.bitset" + KNOWN_DISTINCT: True})

# FILTER(?r = 1) over RANK, without ranking. Oxigraph has no window functions: the reference
# keeps the solutions with no smaller score in their group.
RANK_REFERENCE = """SELECT ?s ?g ?v WHERE { ?s :grp ?g OPTIONAL { ?s :score ?v }
    FILTER NOT EXISTS { ?s2 :grp ?g OPTIONAL { ?s2 :score ?v2 }
      FILTER(BOUND(?v2) && (!BOUND(?v) || ?v2 %s ?v)) } }"""
for direction, op, test in (("ASC", "<", "?rk = 1"), ("DESC", ">", "?rk <= 1")):
    case(f"rank_one_{direction.lower()}",
         f"""SELECT ?s ?g ?v WHERE {{ ?s :grp ?g OPTIONAL {{ ?s :score ?v }}
             BIND(RANK() OVER (PARTITION BY ?g ORDER BY {direction}(?v)) AS ?rk)
             FILTER({test}) }}""",
         ["s", "g", "v"], hits={"rank_one.applied": True},
         # Unbound scores come first ascending (they rank 1 when their group has one), and
         # last descending.
         reference=(RANK_REFERENCE % op) if direction == "DESC" else
         """SELECT ?s ?g ?v WHERE { ?s :grp ?g OPTIONAL { ?s :score ?v }
            FILTER NOT EXISTS { ?s2 :grp ?g OPTIONAL { ?s2 :score ?v2 }
              FILTER(!BOUND(?v2) && BOUND(?v) || BOUND(?v2) && BOUND(?v) && ?v2 < ?v) } }""")

# Property paths: walks on codes from at most 64 starts within a budget of lookups, keying the
# tables otherwise.
def walked(w: World) -> bool:
    """Paths are walked on codes from tables in memory only (not in disk storage)."""
    return w.storage != "disk"


def walked_back(w: World) -> bool:
    """Backwards (from objects), only with the object index."""
    return walked(w) and w.state == "object_index"


for k, walk in ((WALK_MAX_STARTS, True), (WALK_MAX_STARTS + 1, False)):
    starts = values("w", random.Random(k).sample(range(300), k))
    tree_starts = " ".join(iri(f"t{j}_0") for j in random.Random(k).sample(range(100), k))
    hits = {"path.code_walk": (lambda w: walked(w)) if walk else False,
            "path.keyed_bound": (lambda w: not walked(w)) if walk else True}
    # Backwards, the walk reads the tables by object: only with the object index.
    back = {"path.code_walk": (lambda w: walked_back(w)) if walk else False,
            "path.keyed_bound": (lambda w: not walked_back(w)) if walk else True}
    for p in (":tw+", "(:tw/:tw)+", "^:tw+"):
        case(f"path_tree_starts_{k}_{p}",
             f"SELECT ?s ?o WHERE {{ VALUES ?s {{ {tree_starts} }} ?s {p} ?o }}", ["s", "o"],
             hits=back if p.startswith("^") else hits)
    case(f"path_tree_starts_object_{k}",
         f"SELECT ?s ?o WHERE {{ VALUES ?o {{ {tree_starts.replace('_0>', '_3>')} }} ?s :tw+ ?o }}",
         ["s", "o"], hits=back)
    case(f"path_tree_joined_{k}",
         f"SELECT ?s ?o ?x WHERE {{ VALUES ?s {{ {tree_starts} }} ?s :tw ?x . ?x :tw+ ?o }}",
         ["s", "o", "x"])
    # :walk has literal objects and blank node subjects: keyed, also from few starts.
    for p in (":walk+", ":walk*", "(:walk|^:walk2)+", "(:walk/:walk2)+", ":walk?"):
        case(f"path_starts_{k}_{p}",
             f"SELECT ?s ?o WHERE {{ VALUES ?s {{ {starts} }} ?s {p} ?o }}", ["s", "o"],
             hits={"path.code_walk": False})
    case(f"path_starts_object_{k}",
         f"SELECT ?s ?o WHERE {{ VALUES ?o {{ {starts} }} ?s :walk+ ?o }}", ["s", "o"],
         hits={"path.code_walk": False})
    case(f"path_joined_{k}",
         f"SELECT ?s ?o WHERE {{ ?s :walk2 ?a . VALUES ?s {{ {starts} }} ?s :walk+ ?o }}", ["s", "o"])
case("path_walk_within_budget", "SELECT ?o WHERE { :c360 :chain+ ?o }", ["o"],
     hits={"path.code_walk": walked, "path.code_walk_aborted": lambda w: not walked(w)})
case("path_walk_past_budget", "SELECT ?o WHERE { :c0 :chain+ ?o }", ["o"],
     hits={"path.code_walk_aborted": True, "path.keyed_bound": True})
case("path_walk_past_budget_reverse", "SELECT ?s WHERE { ?s :chain+ :c400 }", ["s"],
     hits={"path.code_walk_aborted": True})
case("path_fixed_star", "SELECT ?o WHERE { :w3 :walk* ?o }", ["o"],
     hits={"path.code_walk": False, "path.code_walk_aborted": True})
case("path_tree_fixed_star", "SELECT ?o WHERE { :t10_0 :tw* ?o }", ["o"],
     hits={"path.code_walk": walked})
case("path_unbound", "SELECT ?s ?o WHERE { ?s :walk+ ?o }", ["s", "o"],
     hits={"path.keyed_unbound": True})
case("path_unbound_sequence", "SELECT ?s ?o WHERE { ?s (:walk/:walk2)+ ?o }", ["s", "o"],
     hits={"path.keyed_unbound": True, "path.every_node_step": True})
case("path_unbound_zero", "SELECT (COUNT(*) AS ?n) WHERE { ?s :walk* ?o }", ["n"])
case("path_named_graph", "SELECT ?g ?o WHERE { GRAPH ?g { :w0 :walk+ ?o } }", ["g", "o"])
case("path_both_bound", "ASK { :c0 :chain+ :c400 }", ["x"])
case("path_absent_start", "SELECT ?o WHERE { :nope :walk+ ?o }", ["o"])

# Lookups that the segment Bloom filters answer.
def with_filters(w: World) -> bool:
    return w.filters != "off"


# A known subject: the segments of the other predicates' tables that do not hold it are skipped.
for subject in ("x5", "m7", "w299", "y3"):
    case(f"filter_subject_{subject}",
         f"SELECT ?p ?o WHERE {{ :{subject} ?p ?o }}", ["p", "o"],
         hits={"segment_filter.skip": with_filters})
# A subject not in the dictionary is not looked up at all.
case("filter_subject_absent", "SELECT ?p ?o WHERE { :absent ?p ?o }", ["p", "o"],
     hits={"segment_filter.skip": False, "segment_filter.table_skip": False})
# Several subjects: the tables none of them can be in are left out.
case("filter_values_subjects",
     f"SELECT ?s ?o WHERE {{ VALUES ?s {{ {values('x', range(0, 40000, 997))} :absent }} ?s :many ?o }}",
     ["s", "o"], hits={"segment_filter.table_skip": with_filters})
case("filter_object_lookup",
     "SELECT ?s WHERE { ?s :many :y2999 }", ["s"], hits={"segment_filter.skip": filters_on})



def dictionary_bloom(w: World) -> Optional[bool]:
    """Absent strings are rejected by the on-disk dictionary's Bloom filter in hybrid and disk
    storage, unless it is turned off."""
    if w.storage == "memory":
        return False
    return not DICTIONARY_BLOOM_OFF


# Absent constants, answered by the Bloom filter of the on-disk dictionary.
case("dictionary_absent_iri", "SELECT ?p ?o WHERE { :never_stored ?p ?o }", ["p", "o"],
     hits={"disk_dictionary_bloom.absent": dictionary_bloom})
case("dictionary_absent_string",
     'SELECT ?s WHERE { ?s :mix "never stored" }', ["s"],
     hits={"disk_dictionary_bloom.absent": dictionary_bloom})
case("dictionary_absent_values",
     f"""SELECT ?s ?o WHERE {{ VALUES ?s {{ {values('never', range(100))} :m3 :m4 }}
         ?s :mix ?o }}""", ["s", "o"],
     hits={"disk_dictionary_bloom.absent": dictionary_bloom})
case("dictionary_absent_lang",
     'SELECT ?s WHERE { ?s :label "label 3"@de }', ["s"])



def one_segment(w: World) -> bool:
    """The states where :cd65537 is one segment without a buffer, so its sparse index has the
    keys the seek threshold is computed from."""
    return w.state in ("fresh", "object_index")


# Lookups of few targets seek in the sparse index, of many walk it.
for n, seek in ((SEEK_TARGETS, True), (SEEK_TARGETS + 1, False)):
    case(f"lookup_seek_{n}",
         f"SELECT ?s ?o WHERE {{ VALUES ?s {{ {values('x', range(0, n * 997, 997))} }} ?s :cd65537 ?o }}",
         ["s", "o"],
         hits={"lookup.seek": lambda w, seek=seek: seek if one_segment(w) else None,
               "lookup.walk": lambda w, seek=seek: (not seek) if one_segment(w) else None})
case("lookup_seek_few",
     f"SELECT ?s ?o WHERE {{ VALUES ?s {{ {values('x', [3, 77, 4000, 65536, 99999])} }} ?s :cd65537 ?o }}",
     ["s", "o"], hits={"lookup.seek": True})
case("lookup_walk_many",
     f"SELECT ?s ?o WHERE {{ VALUES ?s {{ {values('x', range(0, 60000, 30))} }} ?s :cd65537 ?o }}",
     ["s", "o"], hits={"lookup.walk": True})
case("lookup_seek_absent_and_mixed",
     """SELECT ?s ?o WHERE { VALUES ?s { :x0 :absent :m3 "lit" 7 } ?s :cd65537 ?o }""",
     ["s", "o"])

# Pushed down values reach the lookups as dictionary codes, so every target is checked against
# the segment filters, however many there are (the 128-target cap of the term path is gone).
for n in (MAX_FILTERED_TARGETS, MAX_FILTERED_TARGETS + 1, 20 * MAX_FILTERED_TARGETS):
    case(f"filter_targets_{n}",
         f"SELECT ?s ?o WHERE {{ VALUES ?s {{ {values('x', range(0, n * 401, 401))} }} ?s :many ?o }}",
         ["s", "o"],
         hits={"segment_filter.target_codes": with_filters,
               "segment_filter.too_many_targets": False})

# COUNT over joins of constant-predicate patterns, counted on dictionary codes (no threshold
# but the 16 UNION terms; taken or not by shape).
JOINS = "count.joins_on_codes"
for name, body in [
    ("star", "?s :sa ?a . ?s :t16384 ?y"),
    ("star_large", "?s :o10001 ?k . ?s :cd65537 ?y"),
    ("chain", "?s :many ?y . ?x :t16385 ?s"),
    ("exists", "?s :e501 ?y FILTER EXISTS { ?s :ein ?v }"),
    ("not_exists", "?s :e500 ?y FILTER NOT EXISTS { ?s :oin ?v }"),
    ("optional", "?s :o10000 ?k OPTIONAL { ?s :oin ?w }"),
    ("minus", "?s :sa ?a MINUS { ?s :lit ?l }"),
    ("minus_one_shared", "?a :walk ?b MINUS { ?b :walk2 ?c }"),
    ("union", "{ ?s :sa ?a } UNION { ?s :lit ?a } . ?s :t16384 ?y"),
]:
    case(f"count_joins_{name}", f"SELECT (COUNT(*) AS ?c) WHERE {{ {body} }}", ["c"],
         hits={JOINS: True})
case("count_joins_count_var",
     "SELECT (COUNT(?y) AS ?c) WHERE { ?s :sa ?a . ?s :t16384 ?y }", ["c"], hits={JOINS: True})
TERM_PREDICATES = ["sa", "sb", "lit", "one", "many", "e500", "e501", "ein", "oin", "t16384",
                   "t16385", "o10000", "o10001", "cd65536", "chain", "tw", "walk2"]
for n, taken in ((MAX_COUNT_TERMS, True), (MAX_COUNT_TERMS + 1, False)):
    union = " UNION ".join(f"{{ ?s :{p} ?o }}" for p in TERM_PREDICATES[:n])
    case(f"count_joins_union_{n}_terms", f"SELECT (COUNT(*) AS ?c) WHERE {{ {union} }}", ["c"],
         hits={JOINS: taken})
for name, body in [
    ("cycle", "?a :walk ?b . ?b :walk ?c . ?c :walk ?a"),
    ("minus_two_shared", "?a :walk ?b MINUS { ?a :walk2 ?b }"),
    ("filter_true", "?s :sa ?a . ?s :t16384 ?y FILTER(true)"),
    ("constant", "?s :sa ?a . ?s :t16384 :y3"),
    ("variable_predicate", "?s :sa ?a . ?s ?p ?y"),
    ("optional_filter", '?s :o10000 ?k OPTIONAL { ?s :oin ?w FILTER(?k > 3) }'),
]:
    case(f"count_joins_not_{name}", f"SELECT (COUNT(*) AS ?c) WHERE {{ {body} }}", ["c"],
         hits={JOINS: False})
case("count_joins_not_distinct",
     "SELECT (COUNT(DISTINCT ?y) AS ?c) WHERE { ?s :sa ?a . ?s :t16384 ?y }", ["c"],
     hits={JOINS: False})
# Keys of several types, blank nodes, named graphs: results only.
case("count_joins_mixed_key", "SELECT (COUNT(*) AS ?c) WHERE { ?s :mix ?o . ?o :label ?l }", ["c"])
case("count_joins_mixed_subject", "SELECT (COUNT(*) AS ?c) WHERE { ?s :mix ?o . ?s :num ?n }", ["c"])
case("count_joins_blank_starts", "SELECT (COUNT(*) AS ?c) WHERE { ?b :walk ?w . ?w :walk ?x }", ["c"])
case("count_joins_named_graph",
     "SELECT (COUNT(*) AS ?c) WHERE { GRAPH :g1 { ?s :mix ?o . ?s :walk ?x } }", ["c"])
case("count_joins_graph_var",
     "SELECT ?g (COUNT(*) AS ?c) WHERE { GRAPH ?g { ?s :mix ?o . ?s :label ?l } } GROUP BY ?g",
     ["g", "c"])

# Table statistics order the triple patterns of a basic graph pattern only where a join is
# estimated clearly (2x) cheaper than the written order.
def statistics_on(w: World) -> bool:
    return w.filters != "stats_off"


ORDER_STATS, ORDER_WRITTEN, ORDER_OFF = "bgp_order.statistics", "bgp_order.written", "bgp_order.no_statistics"
def stats_source(w: World) -> Optional[str]:
    """Where the statistics of the star's tables come from, by design (statistics.rs
    table_stats): a deep analyze while fresh; else merged from the sketches each segment gets
    when written; assumed for segments read back from disk storage without a sketch. None when
    statistics are off."""
    if not statistics_on(w):
        return None
    if w.state == "analyzed":
        return "deep"
    if w.storage == "disk" and w.state == "reloaded":
        return "assumed"
    return "merged"


def source_is(name: str):
    return lambda w: stats_source(w) == name


case("statistics_reorder_star",
     "SELECT ?s ?o ?a WHERE { ?s :cd65537 ?o . ?s :sa ?a }", ["s", "o", "a"],
     hits={ORDER_STATS: statistics_on, ORDER_OFF: lambda w: not statistics_on(w),
           "table_stats.deep": source_is("deep"),
           "table_stats.merged": source_is("merged"),
           "table_stats.assumed": source_is("assumed")})
case("statistics_reorder_chain",
     "SELECT ?x ?s ?y WHERE { ?x :t16385 ?y . ?s :many ?y . ?s :sa ?a }", ["x", "s", "y"])
case("statistics_written_near_equal",
     "SELECT ?s ?y ?z WHERE { ?s :t16384 ?y . ?s :t16385 ?z }", ["s", "y", "z"],
     hits={ORDER_STATS: False, ORDER_WRITTEN: statistics_on,
           ORDER_OFF: lambda w: not statistics_on(w)})
case("statistics_single_pattern", "SELECT ?s ?o WHERE { ?s :sa ?o }", ["s", "o"],
     hits={ORDER_STATS: False, ORDER_WRITTEN: False, ORDER_OFF: False})
case("statistics_disjoint_incoming",
     'SELECT ?s ?y ?a WHERE { VALUES ?y { "lit" } ?s :t16384 ?y . ?s :sa ?a }', ["s", "y", "a"])
case("statistics_mixed_star",
     "SELECT ?s ?o ?n ?l WHERE { ?s :mix ?o . ?s :num ?n . OPTIONAL { ?s :label ?l } }",
     ["s", "o", "n", "l"])
case("statistics_graph_star",
     "SELECT ?g ?s ?w WHERE { GRAPH ?g { ?s :mix ?o . ?s :walk ?w } }", ["g", "s", "w"])

# SELECT results as SPARQL JSON: one chunk (at most 4,096 rows) on the calling thread, more in
# parallel chunks. Exactly n rows: the mixed column (blank nodes, language tags, several
# datatypes, duplicates) and the first rows of an IRI table to make up the rest.
@functools.lru_cache(maxsize=None)
def mixed_rows() -> int:
    return len(oxigraph_rows(oxigraph_store(), "SELECT ?s ?o WHERE { ?s :mix ?o }", ["s", "o"]))


def json_rows_case(n: int, one_chunk: bool):
    name = f"sparql_json_{n}_rows"

    def query():
        k = n - mixed_rows()
        return f"""SELECT ?s ?o WHERE {{ {{ ?s :mix ?o }} UNION
            {{ SELECT ?s ?o WHERE {{ ?s :cd65536 ?o }} ORDER BY ?s LIMIT {k} }} }}"""

    c = Case(name, None, ["s", "o"],
             hits={"sparql_json.one_chunk": one_chunk, "sparql_json.chunks": not one_chunk})
    c.query_fn = query
    CASES.append(c)


json_rows_case(JSON_CHUNK_ROWS, True)
json_rows_case(JSON_CHUNK_ROWS + 1, False)
case("sparql_json_limit_10", "SELECT ?s ?n WHERE { ?s :num ?n } ORDER BY ?s LIMIT 10", ["s", "n"],
     ordered=True, hits={"sparql_json.one_chunk": True, "sparql_json.chunks": False})

# Index nested loops on codes (storage/inl.rs): a BGP without incoming solutions, from its
# constant with the fewest matches, in memory storage; declined for other shapes, types, storage,
# and above the work and row limits (MAPLIB_INL_MAX_WORK / MAPLIB_INL_MAX_ROWS, read per query).
INL_LABELS = ["inl.taken", "inl.empty", "inl.declined_shape", "inl.declined_types",
              "inl.declined_storage", "inl.declined_estimate", "inl.aborted"]


def inl_on(w: World) -> bool:
    return w.filters != "inl_off"


def inl_expect(outcome: str):
    """What a case gives with INL: `outcome` in memory storage, declined_storage in hybrid and
    disk (the dictionary is not in memory; checked before the shape), nothing with
    MAPLIB_INL=0."""
    def expected(w: World, label: str) -> bool:
        if not inl_on(w):
            return False
        if w.storage != "memory":
            return label == "inl.declined_storage"
        return label == outcome
    return {label: (lambda w, label=label: expected(w, label)) for label in INL_LABELS}


for name, body, outcome in [
    ("star", ":x6 :sa ?a . :x6 :t16384 ?y . ?y ^:t16385 ?z", "inl.taken"),
    ("chain", ":x0 :many ?y . ?s :t16384 ?y . ?s :sa ?a", "inl.taken"),
    ("object_constant", "?s :t16384 :y3 . ?s :sa ?a", "inl.taken"),
    ("string_constant", '?s :sa "a7" . ?s :t16384 ?y', "inl.taken"),
    ("empty", ":x7 :sa ?a . :x7 :t16384 ?y", "inl.empty"),
    ("absent_constant", ":x6 :t16384 ?y . ?x :t16385 ?y . ?x :many :nope", "inl.empty"),
    ("variable_predicate", ":x6 ?p ?a . ?a :sa ?b", "inl.declined_shape"),
    ("integer_constant", "?s :sb 3 . ?s :sa ?a", "inl.declined_shape"),
    ("no_constant", "?s :sa ?a . ?s :t16384 ?y", "inl.declined_shape"),
    ("mixed_types", ":m3 :mix ?o . :m3 :num ?n", "inl.declined_types"),
]:
    case(f"inl_{name}", f"SELECT * WHERE {{ {body} }}",
         sorted(set(re.findall(r"\?(\w+)", body))), hits=inl_expect(outcome))
case("inl_work_limit", "SELECT * WHERE { :x0 :many ?y . ?s :t16384 ?y }", ["y", "s"],
     env={"MAPLIB_INL_MAX_WORK": "50"},
     hits={"inl.declined_estimate": lambda w: inl_on(w) and w.storage == "memory",
           "inl.taken": False})
case("inl_row_limit", "SELECT * WHERE { :x0 :many ?y . ?s :t16384 ?y }", ["y", "s"],
     env={"MAPLIB_INL_MAX_ROWS": "20"},
     hits={"inl.declined_estimate|inl.aborted": lambda w: inl_on(w) and w.storage == "memory",
           "inl.taken": False})
case("inl_named_graph", "SELECT * WHERE { GRAPH :g1 { :w0 :walk ?x . ?x :walk ?y } }", ["x", "y"])
case("inl_optional_around",
     "SELECT * WHERE { :x6 :sa ?a . :x6 :t16384 ?y OPTIONAL { ?y ^:t16385 ?z } }", ["a", "y", "z"])

# Fixed cost per query: plans of at most 16,384 rows per step are collected on a helper thread
# (utils polars.rs TINY_COLLECT_ROWS), larger ones by Polars' thread pool; the stack grows only
# for deeply nested queries (model.rs on_query_stack; results only, it has no counter).
case("collect_tiny", "SELECT ?s ?a ?b WHERE { ?s :sa ?a . ?s :sb ?b }", ["s", "a", "b"],
     hits={"collect.tiny": True})
case("collect_concurrent", "SELECT ?s ?o ?x WHERE { ?s :cd65537 ?o . ?s :one ?x }",
     ["s", "o", "x"], hits={"collect.concurrent": True})


def nested_optionals(depth: int) -> str:
    body = "?n0 :tw ?n1"
    for i in range(1, depth):
        body = f"{body} OPTIONAL {{ ?n{i} :tw ?n{i + 1}"
    return "SELECT ?n0 ?n1 ?n2 ?n3 WHERE { " + body + " }" * (depth - 1) + " }"


def nested_subqueries(depth: int) -> str:
    q = "SELECT ?s ?a WHERE { ?s :sa ?a }"
    for _ in range(depth):
        q = f"SELECT ?s ?a WHERE {{ {{ {q} }} }}"
    return q


case("deep_optional_40", nested_optionals(40), ["n0", "n1", "n2", "n3"])
case("deep_subqueries_30", nested_subqueries(30), ["s", "a"])
case("deep_union_50",
     "SELECT ?s ?a WHERE { " + " UNION ".join(["{ ?s :sa ?a }"] * 50) + " }", ["s", "a"])

# Mixed types, named graphs and duplicates through the general paths.
case("mixed_all", "SELECT ?s ?o WHERE { ?s :mix ?o }", ["s", "o"])
case("mixed_graphs", "SELECT ?g ?s ?o WHERE { GRAPH ?g { ?s :mix ?o } }", ["g", "s", "o"])
case("mixed_distinct", "SELECT DISTINCT ?o WHERE { ?s :mix ?o }", ["o"])
case("order_by_mixed_numbers", "SELECT ?o ?s WHERE { ?s :num ?o } ORDER BY ?o ?s", ["o", "s"],
     ordered=True)
case("order_by", "SELECT ?o ?s WHERE { ?s :score ?o } ORDER BY DESC(?o) ?s", ["o", "s"],
     ordered=True)
case("order_by_limit", "SELECT ?s WHERE { ?s :score ?o } ORDER BY ?o ?s LIMIT 17", ["s"],
     ordered=True)
case("mixed_group",
     "SELECT ?dt (COUNT(*) AS ?n) WHERE { ?s :mix ?o BIND(DATATYPE(?o) AS ?dt) } GROUP BY ?dt",
     ["dt", "n"])
case("mixed_join_two_types",
     "SELECT ?s ?o ?o2 WHERE { ?s :mix ?o . ?s :mix2 ?o2 FILTER(?o = ?o2) }", ["s", "o", "o2"])
case("count_all", "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }", ["n"])
case("count_per_predicate",
     "SELECT ?p (COUNT(*) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p", ["p", "n"])
case("count_distinct_spo", "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s ?p ?o }", ["n"])


def case_id(c: Case) -> str:
    return c.name


@pytest.mark.parametrize("c", CASES, ids=case_id)
def test_against_oxigraph(world: World, c: Case):
    if c.only is not None and not c.only(world):
        pytest.skip("not applicable")
    if getattr(c, "query_fn", None):
        c.query = c.reference = c.query_fn()
    expected = expected_rows(c.reference, c.variables)
    env = dict(QUERY_ENV.get(world.filters, {}), **c.env)
    old = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        actual, counts = counted(lambda: maplib_rows(world.model, c.query, list(c.variables)))
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    report(world.storage, world.state, world.filters, c.name, counts, c.expected_hits(world))
    assert_same_rows(expected, actual, c.ordered, c.name)
    check_hits(counts, c.expected_hits(world), c.name)


def test_states_take_their_paths(world: World):
    """The states exercise what they are built for: the write buffer, compaction and deletes."""
    if not HAS_COUNTERS:
        pytest.skip("a build without fast path counters")
    b = world.build_counts
    if world.state == "segments":
        assert b.get("write_buffer.insert_segment", 0) > 0, b
    if world.state == "compacted":
        assert b.get("segments.compaction", 0) > 0, b
    if world.state in ("buffered",):
        assert b.get("write_buffer.insert_buffered", 0) > 0, b
        assert b.get("delete.code_pairs", 0) > 0, b
        assert b.get("delete.join", 0) > 0, b
        assert b.get("update.empty_where_as_data", 0) > 0, b


@pytest.mark.skipif(DICTIONARY_BLOOM_OFF or "hybrid" not in disk_params(),
                    reason="needs maplib_ee; already running with the dictionary filter off")
def test_dictionary_bloom_off(request):
    """The cases in hybrid and disk storage give the same results with the on-disk dictionary's
    Bloom filter turned off, which is read once per process: in a process of their own."""
    import subprocess
    import sys

    env = dict(os.environ)
    env[DICTIONARY_BLOOM_ENV] = "0"
    env.setdefault("MAPLIB_DIFFERENTIAL_STORAGE", "hybrid,disk")
    env.setdefault("MAPLIB_DIFFERENTIAL_STATES", "fresh,reloaded")
    here = os.path.dirname(os.path.abspath(__file__))
    r = subprocess.run(
        [sys.executable, "-P", "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider",
         f"--import-mode={request.config.option.importmode}",
         f"tests/{os.path.basename(__file__)}::test_against_oxigraph"],
        cwd=os.path.dirname(here), env=env, capture_output=True, text=True,
    )
    assert r.returncode == 0, r.stdout[-6000:] + r.stderr[-3000:]


def test_order_by_equal_numbers_of_two_types():
    """Found by this suite (2026-11-19, fixed in f8c36235): `0` and `0.0` are equal for `<`, so
    ORDER BY ?o ?s orders them by ?s, as Oxigraph does; maplib ordered the doubles first."""
    data = f"""{iri('a')} {iri('n')} "0.0"^^<{XSD}double> .
{iri('b')} {iri('n')} {int_lit(0)} .
{iri('c')} {iri('n')} "0.0"^^<{XSD}double> .
"""
    store = ox.Store()
    store.load(data.encode(), ox.RdfFormat.N_TRIPLES)
    m = Model()
    m.reads(data, format="ntriples")
    q = "SELECT ?s WHERE { ?s :n ?o } ORDER BY ?o ?s"
    assert maplib_rows(m, q, ["s"]) == oxigraph_rows(store, q, ["s"])


# ---------------------------------------------------------------------------
# The write buffer and small updates, step by step
# ---------------------------------------------------------------------------

BUFFER_QUERIES = [
    ("SELECT (COUNT(*) AS ?n) WHERE { ?s :wb ?o }", ["n"]),
    ("SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE { ?s :wb ?o }", ["n"]),
    ("SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s :wb ?o }", ["n"]),
    ("SELECT ?o WHERE { :b7 :wb ?o }", ["o"]),
    ("SELECT ?s WHERE { ?s :wb :c3 }", ["s"]),
    ("SELECT ?o WHERE { :b0 :wb+ ?o }", ["o"]),
    ("SELECT ?s ?v WHERE { ?s :wv ?v FILTER(?v > 4090) }", ["s", "v"]),
    ("SELECT ?s ?o ?v WHERE { ?s :wb ?o OPTIONAL { ?s :wv ?v } } ", ["s", "o", "v"]),
    ("SELECT ?g ?s WHERE { GRAPH ?g { ?s :wb ?o } }", ["g", "s"]),
]


def wb(i, j=None):
    return f"{iri(f'b{i}')} {iri('wb')} {iri(f'b{j}') if j is not None else iri(f'c{i % 9}')} ."


def wv(i, v):
    return f"{iri(f'b{i}')} {iri('wv')} {int_lit(v)} ."


@pytest.mark.parametrize("storage", selected("MAPLIB_DIFFERENTIAL_STORAGE", disk_params()))
def test_write_buffer_steps(storage, tmp_path):
    kwargs = {"storage": storage}
    if storage != "memory":
        kwargs["storage_folder"] = str(tmp_path / "storage")
    m = Model(**kwargs)
    store = ox.Store()
    base = [wb(i, i + 1) for i in range(5000)] + [wv(i, i) for i in range(5000)]
    base += [f"{iri('b1')} {iri('wb')} {iri('b2')} <{G1}> ."]
    m.reads(nquads(base), format="nquads")
    store.load(nquads(base).encode(), ox.RdfFormat.N_QUADS)

    def step(name, update, hits):
        _, counts = counted(lambda: m.update(PFX + update))
        report(storage, "write_buffer_steps", "on", name, counts, hits)
        store.update(PFX + update)
        check_hits(counts, hits, name)
        for q, vs in BUFFER_QUERIES:
            assert_same_rows(oxigraph_rows(store, q, vs), maplib_rows(m, q, vs), False,
                             f"{name}: {q}")

    # Exactly the buffer's capacity of new rows in one table: buffered, no new segment.
    step("fill buffer", data_update("INSERT DATA", [wb(10_000 + i) for i in range(MAX_BUFFER_HEIGHT)]),
         {"write_buffer.insert_buffered": True, "write_buffer.insert_segment": False})
    # One row more: the buffer is merged into a segment.
    step("overflow buffer", data_update("INSERT DATA", [wb(20_000)]),
         {"write_buffer.insert_segment": True, "write_buffer.flush": True})
    step("small insert", data_update("INSERT DATA", [wb(30_000 + i) for i in range(50)]
                                     + [wv(30_000 + i, 4095 + i) for i in range(50)]),
         {"write_buffer.insert_buffered": True, "write_buffer.insert_segment": False})
    # Deletes of buffered rows and of rows in segments, by code pairs (IRIs) or by a join
    # (integer literals).
    step("delete buffered iris", data_update("DELETE DATA", [wb(30_000 + i) for i in range(0, 50, 2)]),
         {"delete.code_pairs": True})
    step("delete segment iris", data_update("DELETE DATA", [wb(i, i + 1) for i in range(0, 300, 3)]),
         {"delete.code_pairs": True})
    step("delete literals", data_update("DELETE DATA", [wv(30_000 + i, 4095 + i) for i in range(0, 50, 5)]
                                        + [wv(i, i) for i in range(4000, 4100)]),
         {"delete.join": True})
    step("delete absent", data_update("DELETE DATA", [wb(99_999), wv(99_999, 1)]), {})
    # DELETE/INSERT with an empty WHERE and no variables: data updates.
    step("empty where",
         f"DELETE {{ {wb(30_001)} {wb(5, 6)} }} INSERT {{ {wb(40_000)} GRAPH <{G2}> {{ {wb(40_001)} }} }} WHERE {{}}",
         {"update.empty_where_as_data": True})
    step("where with variables",
         f"DELETE {{ ?s :wb ?o }} INSERT {{ ?s :wb :c0 }} WHERE {{ VALUES ?s {{ :b11 :b12 }} ?s :wb ?o }}",
         {"update.empty_where_as_data": False})
    step("delete where", "DELETE WHERE { :b20 :wb ?o . :b20 :wv ?v }", {})
    step("reinsert deleted", data_update("INSERT DATA", [wb(i, i + 1) for i in range(0, 300, 3)]),
         {"write_buffer.insert_buffered": True})
    path = str(tmp_path / "m.maplib")
    m.serialize(path)
    del m
    if storage != "memory":
        kwargs["storage_folder"] = str(tmp_path / "reloaded_storage")
    m = Model.deserialize(path, **kwargs)
    for q, vs in BUFFER_QUERIES:
        assert_same_rows(oxigraph_rows(store, q, vs), maplib_rows(m, q, vs), False, f"reloaded: {q}")


@pytest.mark.parametrize("storage", selected("MAPLIB_DIFFERENTIAL_STORAGE", disk_params()))
def test_statistics_use_exact_distinct_counts_of_one_segment(storage, tmp_path):
    """Merged statistics take a table's exact distinct counts when it is one segment with an
    empty write buffer, and the segments' sketches once a small insert is buffered (statistics.rs
    merged_table_stats). Counted when the statistics are merged, so each step changes the table
    first."""
    kwargs = {"storage": storage}
    if storage != "memory":
        kwargs["storage_folder"] = str(tmp_path / "storage")
    m = Model(**kwargs)
    store = ox.Store()
    base = [f"{iri(f'e{i}')} {iri('ea')} {iri(f'f{i % 700}')} ." for i in range(5000)]
    base += [f"{iri(f'e{i}')} {iri('eb')} {int_lit(i % 13)} ." for i in range(0, 5000, 7)]
    m.reads(nquads(base), format="nquads")
    store.load(nquads(base).encode(), ox.RdfFormat.N_TRIPLES)
    q = "SELECT ?s ?f ?n WHERE { ?s :ea ?f . ?s :eb ?n }"

    def run(name, expected):
        rows, counts = counted(lambda: maplib_rows(m, q, ["s", "f", "n"]))
        assert_same_rows(oxigraph_rows(store, q, ["s", "f", "n"]), rows, False, name)
        if storage != "disk":
            check_hits(counts, expected, name)

    run("one segment", {"table_stats.merged": True, "table_stats.exact_distinct": True})
    extra = data_update("INSERT DATA", [f"{iri(f'e{i}')} {iri('ea')} {iri('f1')} ." for i in range(9000, 9010)])
    m.update(PFX + extra)
    store.update(PFX + extra)
    run("write buffer", {"table_stats.merged": True, "table_stats.exact_distinct": False})


@pytest.mark.parametrize("storage", selected("MAPLIB_DIFFERENTIAL_STORAGE", disk_params()))
def test_counts_and_paths_after_tiny_queries(storage, tmp_path):
    """A distinct count and a transitive path right after tiny queries collected on the helper
    thread, on the same thread, and a deeply nested query in between: each against Oxigraph."""
    kwargs = {"storage": storage}
    if storage != "memory":
        kwargs["storage_folder"] = str(tmp_path / "storage")
    m = Model(**kwargs)
    lines, pinned = dataset()
    m.reads(nquads(list(pinned) + list(lines)), format="nquads")
    steps = [
        ("SELECT ?a WHERE { :x6 :sa ?a }", ["a"]),
        ("SELECT ?s WHERE { ?s :sb 3 }", ["s"]),
        ("SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { ?s :cd65537 ?o }", ["c"]),
        ("SELECT ?o WHERE { :t3_0 :tw+ ?o }", ["o"]),
        (nested_optionals(30), ["n0", "n1", "n2", "n3"]),
        ("SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s :many ?o }", ["c"]),
        ("SELECT ?s ?o WHERE { ?s :walk+ ?o }", ["s", "o"]),
        ("SELECT ?a WHERE { :x9 :sa ?a }", ["a"]),
    ]
    for q, vs in steps * 2:
        assert_same_rows(expected_rows(q, tuple(vs)), maplib_rows(m, q, vs), False, q[:60])

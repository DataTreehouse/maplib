"""Table statistics (`Model.analyze`) and the cost-based order of triple patterns they give.

The order only changes how a basic graph pattern is evaluated, never its solutions: these tests
compare maplib with Oxigraph, as multisets of rows, on random graphs with skewed predicates (one
very common class, rare and common links), for stars, chains, cycles, constants, blank nodes,
VALUES, OPTIONAL and unbound predicates, written in an order that is bad on purpose (blank nodes
compare as one value, since their labels differ between the stores). They also
check the statistics themselves and that they follow updates."""
import json
import random
from collections import Counter

import polars as pl
import pyoxigraph as ox
import pytest
from maplib import Model

EX = "http://ex.org/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
PFX = f"PREFIX : <{EX}>\nPREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"


def graph(seed):
    rng = random.Random(seed)
    nodes = [f"<{EX}n{i}>" for i in range(200)]
    lines = []
    for n in nodes:
        # One class is shared by almost every node, a few classes by few.
        lines.append(f"{n} <{RDF_TYPE}> <{EX}{'Common' if rng.random() < 0.9 else 'Rare' + str(rng.randrange(3))}> .")
    for _ in range(600):
        lines.append(f"{rng.choice(nodes)} <{EX}link> {rng.choice(nodes)} .")
    for _ in range(15):
        lines.append(f"{rng.choice(nodes)} <{EX}rare> {rng.choice(nodes)} .")
    for i, n in enumerate(nodes):
        if rng.random() < 0.5:
            lines.append(f'{n} <{EX}name> "name{i % 40}" .')
        if rng.random() < 0.3:
            lines.append(f'{n} <{EX}age> "{rng.randrange(20, 30)}"^^<http://www.w3.org/2001/XMLSchema#integer> .')
    for _ in range(10):
        lines.append(f"_:b{rng.randrange(5)} <{EX}link> {rng.choice(nodes)} .")
    return "\n".join(lines)


QUERIES = [
    # Stars written with the largest pattern first.
    "SELECT ?s ?o ?c WHERE { ?s rdf:type ?c . ?s :link ?o . ?s :rare ?r }",
    "SELECT ?s ?n WHERE { ?s rdf:type :Common . ?s :name ?n . ?s :age 25 }",
    # A chain with a selective end.
    "SELECT ?a ?b ?c WHERE { ?a :link ?b . ?b :link ?c . ?c :rare ?d }",
    # A cycle.
    "SELECT ?a ?b ?c WHERE { ?a :link ?b . ?b :link ?c . ?c :link ?a }",
    # Constants as subject and object.
    "SELECT ?o ?c WHERE { ?x :link ?o . <http://ex.org/n7> :link ?x . ?o rdf:type ?c }",
    "SELECT ?s WHERE { ?s rdf:type :Rare1 . ?s :link ?o . ?o rdf:type :Common }",
    # Shared literals between subjects: many-to-many through ?n.
    "SELECT ?a ?b WHERE { ?a :name ?n . ?b :name ?n . ?a :rare ?x }",
    # Blank nodes in the query and in the data.
    "SELECT ?o WHERE { _:x :link ?o . _:x rdf:type ?c }",
    "SELECT ?o WHERE { ?s :link ?o . FILTER(isBlank(?s)) }",
    # An unbound predicate next to bound ones.
    "SELECT ?s ?p ?o WHERE { ?s ?p ?o . ?s :rare ?r . ?s rdf:type :Common }",
    "SELECT ?p WHERE { <http://ex.org/n3> ?p ?o . ?o rdf:type ?c }",
    # VALUES and OPTIONAL around a BGP.
    "SELECT ?s ?o ?n WHERE { VALUES ?c { :Rare0 :Rare2 } ?s :link ?o . ?s rdf:type ?c OPTIONAL { ?o :name ?n } }",
    "SELECT ?s ?n ?a WHERE { ?s :rare ?r . OPTIONAL { ?s :name ?n . ?s :age ?a } }",
    # Disconnected patterns (a cross product of two small sides).
    "SELECT ?a ?b WHERE { ?a :rare ?x . ?b rdf:type :Rare0 }",
    # A pattern with no solutions in a large star.
    "SELECT ?s WHERE { ?s rdf:type ?c . ?s :link ?o . ?s :missing ?m }",
]


def term(t):
    return t.value if hasattr(t, "value") else str(t)


def vars_of(q):
    head = q.split("WHERE")[0]
    return [w[1:] for w in head.split() if w.startswith("?")]


def ox_value(t):
    # Blank node labels differ between the stores: blank nodes compare as one value.
    if t is None:
        return None
    return "_:" if isinstance(t, ox.BlankNode) else term(t)


def rows_oxigraph(store, q):
    vs = vars_of(q)
    return Counter(tuple(ox_value(r[v]) for v in vs) for r in store.query(PFX + q))


def rows_maplib(m, q):
    vs = vars_of(q)
    d = json.loads(m.query(PFX + q, return_json=True))
    value = lambda b: "_:" if b["type"] == "bnode" else b["value"]
    return Counter(
        tuple(value(b[v]) if v in b else None for v in vs) for b in d["results"]["bindings"]
    )


@pytest.mark.parametrize("seed", [1, 2, 3])
@pytest.mark.parametrize("analyze", [False, True])
def test_order_keeps_solutions(seed, analyze):
    data = graph(seed)
    m = Model()
    m.reads(data, format="ntriples")
    if analyze:
        m.analyze(deep=True)
    store = ox.Store()
    store.load(data.encode(), format=ox.RdfFormat.N_TRIPLES)
    for q in QUERIES:
        expected = rows_oxigraph(store, q)
        got = rows_maplib(m, q)
        assert got == expected, q


def test_permuted_patterns_agree():
    m = Model()
    m.reads(graph(7), format="ntriples")
    patterns = ["?s rdf:type ?c", "?s :link ?o", "?o :name ?n", "?s :rare ?r", "?o rdf:type :Common"]
    rng = random.Random(3)
    results = set()
    for _ in range(8):
        rng.shuffle(patterns)
        q = PFX + "SELECT (COUNT(*) AS ?k) WHERE { " + " . ".join(patterns) + " }"
        results.add(m.query(q)["k"][0])
    assert len(results) == 1


def test_analyze_counts():
    m = Model()
    lines = []
    for i in range(100):
        lines.append(f"<{EX}s{i}> <{RDF_TYPE}> <{EX}{'A' if i < 70 else 'B' if i < 90 else 'C' + str(i)}> .")
        lines.append(f"<{EX}s{i % 10}> <{EX}p> <{EX}o{i}> .")
        lines.append(f'<{EX}s{i}> <{EX}label> "l{i % 5}"@en .')
    m.reads("\n".join(lines), format="ntriples")
    df = m.analyze(deep=True)
    assert isinstance(df, pl.DataFrame)
    assert set(df.columns) >= {
        "graph", "predicate", "subject_type", "object_type", "triples",
        "distinct_subjects", "distinct_objects", "most_common_objects", "most_common_object_counts",
    }
    t = df.filter(pl.col("predicate") == f"<{RDF_TYPE}>").row(0, named=True)
    assert (t["triples"], t["distinct_subjects"], t["distinct_objects"]) == (100, 100, 12)
    # The two classes shared by more than one subject, most common first.
    assert t["most_common_objects"] == [f"<{EX}A>", f"<{EX}B>"]
    assert t["most_common_object_counts"] == [70, 20]
    assert (t["max_triples_per_subject"], t["max_triples_per_object"]) == (1, 70)
    p = df.filter(pl.col("predicate") == f"<{EX}p>").row(0, named=True)
    assert (p["triples"], p["distinct_subjects"], p["distinct_objects"]) == (100, 10, 100)
    assert p["most_common_objects"] == []
    assert (p["max_triples_per_subject"], p["max_triples_per_object"]) == (10, 1)
    lab = df.filter(pl.col("predicate") == f"<{EX}label>").row(0, named=True)
    assert (lab["triples"], lab["distinct_subjects"], lab["distinct_objects"]) == (100, 100, 5)


def test_analyze_after_updates_and_named_graphs():
    m = Model()
    m.reads(f"<{EX}a> <{EX}p> <{EX}b> .", format="ntriples")
    m.reads(f"<{EX}a> <{EX}p> <{EX}c> .\n<{EX}d> <{EX}p> <{EX}c> .", format="ntriples", graph=f"{EX}g")
    m.analyze(deep=True)
    m.update(f"INSERT DATA {{ <{EX}x> <{EX}p> <{EX}y> . <{EX}x> <{EX}p> <{EX}z> . }}")
    df = m.analyze(deep=True)
    default = df.filter((pl.col("predicate") == f"<{EX}p>") & (pl.col("graph") == "default graph")).row(0, named=True)
    assert (default["triples"], default["distinct_subjects"], default["distinct_objects"]) == (3, 2, 3)
    named = df.filter(pl.col("graph") == f"<{EX}g>").row(0, named=True)
    assert (named["triples"], named["distinct_subjects"], named["distinct_objects"]) == (2, 2, 1)
    assert named["most_common_objects"] == [f"<{EX}c>"]
    # Queries see every update, whatever the statistics say.
    q = f"SELECT (COUNT(*) AS ?n) WHERE {{ ?s <{EX}p> ?o . ?s <{EX}p> ?o2 }}"
    assert m.query(q)["n"][0] == 1 + 4  # a: 1x1, x: 2x2
    m.update(f"DELETE DATA {{ <{EX}x> <{EX}p> <{EX}y> . <{EX}x> <{EX}p> <{EX}z> . }}")
    assert m.query(q)["n"][0] == 1


def test_analyze_empty_model():
    df = Model().analyze()
    assert df.height == 0


def test_join_size_estimates_against_exact():
    m = Model()
    m.reads(graph(11), format="ntriples")
    df = m._join_size_estimates(exact=True)
    assert df.height > 0
    assert set(df["left_position"].unique()) <= {"subject", "object"}
    assert df["exact"].null_count() == 0
    # Within 2x for nearly all joins of more than 50 rows.
    big = df.filter(pl.col("exact") > 50)
    ratio = (big["estimate"] / big["exact"]).to_list()
    within = sum(1 for r in ratio if 0.5 <= r <= 2.0)
    assert within >= 0.9 * len(ratio), sorted(ratio)
    # The subject self-join of a table (?s :link ?o . ?s :link ?o2).
    self_join = df.filter(
        (pl.col("left_predicate") == f"<{EX}link>")
        & (pl.col("right_predicate") == f"<{EX}link>")
        & (pl.col("left_position") == "subject")
        & (pl.col("right_position") == "subject")
    ).row(0, named=True)
    assert 0.5 <= self_join["estimate"] / self_join["exact"] <= 2.0
    assert m._sketch_memory() > 0


def test_statistics_kept_up_to_date_without_analyze():
    """Without a deep analysis, statistics are merged from the segments' sketches, which every
    write keeps current: counts follow inserts and deletes without reading the triples again."""
    m = Model()
    lines = [f"<{EX}s{i}> <{RDF_TYPE}> <{EX}{'A' if i % 4 else 'B'}> ." for i in range(400)]
    m.reads("\n".join(lines), format="ntriples")
    row = lambda df: df.filter(pl.col("predicate") == f"<{RDF_TYPE}>").row(0, named=True)
    t = row(m.analyze())
    assert (t["triples"], t["distinct_subjects"], t["distinct_objects"]) == (400, 400, 2)
    assert t["most_common_objects"] == [f"<{EX}A>", f"<{EX}B>"]
    assert t["exact"]
    added = " ".join(f"<{EX}t{i}> <{RDF_TYPE}> <{EX}C> ." for i in range(200))
    m.update(f"INSERT DATA {{ {added} }}")
    t = row(m.analyze())
    assert t["triples"] == 600
    # Merged from two segments: estimates within a few percent of the truth.
    assert abs(t["distinct_subjects"] - 600) <= 18
    assert t["distinct_objects"] == 3
    assert t["most_common_objects"][:2] == [f"<{EX}A>", f"<{EX}C>"]
    m.update(f"DELETE WHERE {{ ?s <{RDF_TYPE}> <{EX}B> }}")
    t = row(m.analyze())
    assert t["triples"] == 500
    d = row(m.analyze(deep=True))
    assert (d["triples"], d["distinct_subjects"], d["distinct_objects"], d["exact"]) == (500, 500, 2, True)

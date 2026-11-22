"""The steps of a small query that used to be collected by Polars' streaming engine (a plain
collect) or on its thread pool: the columns of VALUES, the format of the result and the solutions
collected to push their values into the next pattern. They give the same results as with
`MAPLIB_POLARS_ENGINE=streaming`, which keeps the streaming engine, and the fast path counters say
which of them ran as tiny plans on the helper thread (`collect.tiny_step`)."""
import polars as pl
import pytest
from polars.testing import assert_frame_equal

import maplib.maplib as native
from maplib import Model

EX = "http://ex.org/"
XSD = "http://www.w3.org/2001/XMLSchema#"
PFX = f"PREFIX : <{EX}>\nPREFIX xsd: <{XSD}>\n"
HAS_COUNTERS = hasattr(native, "_fast_path_counts")


@pytest.fixture(scope="module")
def m():
    lines = []
    for i in range(200):
        s = f"<{EX}s{i}>"
        lines.append(f'{s} <{EX}int> "{i}"^^<{XSD}integer> .')
        lines.append(f'{s} <{EX}dec> "{i}.5"^^<{XSD}decimal> .')
        lines.append(f'{s} <{EX}str> "name {i % 7}" .')
        lines.append(f'{s} <{EX}lang> "navn {i % 5}"@nb .')
        lines.append(f'{s} <{EX}date> "2024-01-{i % 28 + 1:02d}"^^<{XSD}date> .')
        lines.append(f'{s} <{EX}time> "2024-01-01T00:00:{i % 60:02d}Z"^^<{XSD}dateTime> .')
        lines.append(f'{s} <{EX}flag> "{"true" if i % 2 else "false"}"^^<{XSD}boolean> .')
        lines.append(f"{s} <{EX}next> <{EX}s{(i + 1) % 200}> .")
        lines.append(f"{s} <{EX}mixed> " + (f"<{EX}s{i}>" if i % 2 else f'"{i}"^^<{XSD}integer>') + " .")
    model = Model()
    model.reads("\n".join(lines), format="ntriples")
    return model


def counted(f):
    if not HAS_COUNTERS:
        return f(), {}
    native._reset_fast_path_counts()
    out = f()
    return out, {k: v for k, v in native._fast_path_counts().items() if v}


QUERIES = {
    # name: (query, whether the result needs formatting)
    "values_number": ("SELECT * WHERE { VALUES ?x { 1 } }", False),
    # Strings are stored as dictionary codes, so they are encoded and then decoded for the result.
    "values_two_columns": ('SELECT ?b ?a WHERE { VALUES (?a ?b) { (1 "x") (2 "y") (UNDEF "z") } }', True),
    "values_numbers": ("SELECT ?b ?a WHERE { VALUES (?a ?b) { (1 2.5) (2 3.5e0) (UNDEF true) } }", None),
    "values_iri": ("SELECT * WHERE { VALUES ?x { :s1 :s2 } }", True),
    "values_mixed_types": ('SELECT * WHERE { VALUES ?x { :s1 2 "three" "fire"@nb } }', True),
    "numbers": ("SELECT ?i ?d WHERE { :s7 :int ?i ; :dec ?d }", False),
    "plain_string": ("SELECT ?v WHERE { :s7 :str ?v }", None),
    "date_time_boolean": ("SELECT ?d ?t ?f WHERE { :s7 :date ?d ; :time ?t ; :flag ?f }", False),
    "lang_string": ("SELECT ?v WHERE { :s7 :lang ?v }", True),
    "iri": ("SELECT ?o WHERE { :s7 :next ?o }", True),
    "mixed_object": ("SELECT ?s ?o WHERE { ?s :mixed ?o } ORDER BY ?s LIMIT 6", True),
    "aggregate": ("SELECT (SUM(?i) AS ?sum) (COUNT(DISTINCT ?v) AS ?n) WHERE { ?s :int ?i ; :str ?v }", False),
    "unbound_column": ("SELECT ?s ?x WHERE { VALUES ?s { :s1 } OPTIONAL { ?s :none ?x } }", True),
    "chain": ("SELECT ?a ?c ?i WHERE { :s3 :next ?a . ?a :next ?b . ?b :next ?c . ?c :int ?i }", True),
    "path": ("SELECT (COUNT(*) AS ?n) WHERE { :s0 :next+ ?o }", False),
    "no_rows": ("SELECT ?i WHERE { :none :int ?i }", None),
}


@pytest.mark.parametrize("native_df", [False, True])
@pytest.mark.parametrize("name", QUERIES)
def test_same_as_streaming(m, name, native_df, monkeypatch):
    q, formatted = QUERIES[name]

    def run():
        out = m.query(PFX + q, solution_mappings=native_df, cache=False)
        return (out.mappings, out.rdf_types) if native_df else (out, None)

    (df, types), counts = counted(run)
    monkeypatch.setenv("MAPLIB_POLARS_ENGINE", "streaming")
    (expected, expected_types), streaming_counts = counted(run)
    assert df.schema == expected.schema, name
    assert_frame_equal(df, expected, check_row_order="ORDER BY" in q)
    assert types == expected_types
    if HAS_COUNTERS:
        assert streaming_counts.get("collect.tiny_step", 0) == 0, streaming_counts
        assert streaming_counts.get("collect.tiny", 0) == 0, streaming_counts
        # A result of plain values is returned as it is, without a plan to format it.
        if formatted is False and not native_df and name.startswith("values"):
            assert counts.get("collect.tiny_step", 0) == 0, counts
        if formatted is True and not native_df:
            assert counts.get("collect.tiny_step", 0) >= 1, counts


def test_values_column_order(m):
    df = m.query('SELECT * WHERE { VALUES (?z ?a ?m) { (1 2 3) } }')
    assert df.columns == ["z", "a", "m"]
    assert df.row(0) == (1, 2, 3)
    df = m.query('SELECT ?m ?z WHERE { VALUES (?z ?a ?m) { (1 2 3) (4 5 6) } }')
    assert df.columns == ["m", "z"]
    assert df.rows() == [(3, 1), (6, 4)]


def test_pushed_values_from_few_rows(monkeypatch):
    """Solutions of a few rows collected before a pattern over a table of more than 16,384 rows:
    their values are pushed into its lookup (`bgp.values_pushed`), from a collect on the helper
    thread. Without index nested loops, which would answer this chain on their own."""
    monkeypatch.setenv("MAPLIB_INL", "0")
    n = 20_000
    df = pl.DataFrame({"subject": [f"{EX}n{i}" for i in range(n)], "object": [f"{EX}n{(i * 7 + 1) % n}" for i in range(n)]})
    model = Model()
    model.map_triples(df, predicate=EX + "to")
    model.map_triples(df.select("subject", pl.int_range(n).alias("object")), predicate=EX + "w")
    q = PFX + "SELECT ?c ?w WHERE { :n5 :to ?a . ?a :to ?b . ?b :to ?c . ?c :w ?w }"
    out, counts = counted(lambda: model.query(q, cache=False))
    c = (((5 * 7 + 1) * 7 + 1) * 7 + 1) % n
    assert out.rows() == [(f"<{EX}n{c}>", c)]
    if HAS_COUNTERS:
        assert counts.get("bgp.values_pushed", 0) >= 1, counts
        assert counts.get("collect.tiny_step", 0) >= 1, counts

"""Vector literals (maplib:vector) from Polars Array columns, checked against the input arrays.

The lexical form uses the shortest decimal that reads back to the same 32 bit float, so every
component must round-trip exactly through mapping, querying and the RDF serializations.
"""

import math
import re

import numpy as np
import polars as pl
import pytest

from maplib import Model

EX = "http://example.net/ns#"
VECTOR = "https://datatreehouse.github.io/maplib/vocab#vector"
LITERAL = re.compile(r'<([^>]+)> <[^>]+> "(\[[^"]*\])"\^\^<' + re.escape(VECTOR) + ">")


def parse(lexical):
    inner = lexical.strip()[1:-1].strip()
    if not inner:
        return np.zeros(0, dtype=np.float32)
    return np.array([float(x) for x in inner.split(",")], dtype=np.float32)


def same_floats(a, b):
    a = np.asarray(a, dtype=np.float32)
    b = np.asarray(b, dtype=np.float32)
    # Bitwise equal, except that all NaNs are equal.
    return a.shape == b.shape and all(
        (math.isnan(x) and math.isnan(y)) or x == y for x, y in zip(a.tolist(), b.tolist())
    )


def vectors_df(vectors, dtype):
    return pl.DataFrame(
        {
            "subject": [f"{EX}item{i}" for i in range(len(vectors))],
            "object": pl.Series(vectors.tolist(), dtype=pl.Array(dtype, vectors.shape[1])),
        }
    )


def stored_vectors(m):
    df = m.query(
        f"""SELECT ?s ?v WHERE {{ ?s <{EX}embedding> ?v }}"""
    )
    return {s.strip("<>"): parse(v) for s, v in zip(df["s"].to_list(), df["v"].to_list())}


def edge_vectors():
    rng = np.random.default_rng(3)
    v = rng.standard_normal((40, 24)).astype(np.float32)
    # Values whose shortest decimal forms are long or unusual.
    v[0, :6] = [np.float32(0.1), np.float32(1 / 3), 1e-38, 1e38, -0.0, 16777217]
    v[1, :4] = np.finfo(np.float32).tiny, np.finfo(np.float32).max, np.finfo(np.float32).eps, 5e-45
    v[2, :] = 0.0
    v[3, :3] = [np.nan, np.inf, -np.inf]
    return v


@pytest.mark.parametrize("dtype", [pl.Float32, pl.Float64])
def test_map_array_column_round_trips_exactly(dtype):
    vectors = edge_vectors()
    m = Model()
    m.map_triples(vectors_df(vectors, dtype), predicate=EX + "embedding")
    stored = stored_vectors(m)
    assert len(stored) == len(vectors)
    for i, v in enumerate(vectors):
        assert same_floats(stored[f"{EX}item{i}"], v), i


def test_float64_is_rounded_to_float32():
    v = np.array([[0.1, 1 / 3, 1e-300, 2.0**-149]], dtype=np.float64)
    m = Model()
    m.map_triples(vectors_df(v, pl.Float64), predicate=EX + "embedding")
    (stored,) = stored_vectors(m).values()
    assert same_floats(stored, v[0].astype(np.float32))


@pytest.mark.parametrize("dtype", [pl.Int8, pl.Int32, pl.Int64, pl.UInt16, pl.Float32])
def test_integer_and_float_columns(dtype):
    v = np.array([[1, -2, 3], [0, 100, 7]])
    if dtype in (pl.UInt16,):
        v = np.abs(v)
    m = Model()
    m.map_triples(vectors_df(v, dtype), predicate=EX + "embedding")
    stored = stored_vectors(m)
    for i, row in enumerate(v):
        assert same_floats(stored[f"{EX}item{i}"], row.astype(np.float32))


def test_null_arrays_are_rejected_like_other_nulls():
    df = pl.DataFrame(
        {
            "subject": [f"{EX}a", f"{EX}b", f"{EX}c"],
            "object": pl.Series([[1.0, 2.0], None, [3.0, 4.0]], dtype=pl.Array(pl.Float32, 2)),
        }
    )
    m = Model()
    with pytest.raises(Exception, match="null values"):
        m.map_triples(df, predicate=EX + "embedding")
    # Without the null row, the vectors are mapped.
    m.map_triples(df.drop_nulls(), predicate=EX + "embedding")
    stored = stored_vectors(m)
    assert set(stored) == {f"{EX}a", f"{EX}c"}
    assert same_floats(stored[f"{EX}c"], [3.0, 4.0])


def test_datatype_is_vector():
    m = Model()
    m.map_triples(vectors_df(np.ones((2, 3)), pl.Float32), predicate=EX + "embedding")
    df = m.query(f"SELECT DISTINCT (DATATYPE(?v) AS ?dt) WHERE {{ ?s <{EX}embedding> ?v }}")
    assert df["dt"].to_list() == [f"<{VECTOR}>"]


@pytest.mark.parametrize("fmt", ["ntriples", "turtle"])
def test_serialization_round_trip(fmt):
    vectors = edge_vectors()
    m = Model()
    m.map_triples(vectors_df(vectors, pl.Float32), predicate=EX + "embedding")
    text = m.writes(format=fmt)
    m2 = Model()
    m2.reads(text, format=fmt)
    stored = stored_vectors(m2)
    assert len(stored) == len(vectors)
    for i, v in enumerate(vectors):
        assert same_floats(stored[f"{EX}item{i}"], v), i
    if fmt == "ntriples":
        # The written lexical forms parse back to the input, independently of maplib's parser.
        found = {s: parse(l) for s, l in LITERAL.findall(text)}
        assert len(found) == len(vectors)
        for i, v in enumerate(vectors):
            assert same_floats(found[f"{EX}item{i}"], v), i


def test_written_vector_literals_are_read_from_other_sources():
    # Lexical forms written by hand or by other tools, with spaces and exponents.
    nt = (
        f'<{EX}a> <{EX}embedding> "[ 1 , -2.5e0, 3E-1 ]"^^<{VECTOR}> .\n'
        f'<{EX}b> <{EX}embedding> "[]"^^<{VECTOR}> .\n'
    )
    m = Model()
    m.reads(nt, format="ntriples")
    df = m.query(f"SELECT ?s ?v WHERE {{ ?s <{EX}embedding> ?v }} ORDER BY ?s")
    # The lexical form is kept as written.
    assert df["v"].to_list() == ["[ 1 , -2.5e0, 3E-1 ]", "[]"]
    assert same_floats(parse(df["v"][0]), [1.0, -2.5, 0.3])

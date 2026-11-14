"""ORDER BY over more rows than fit in one batch of the streaming engine.

IRIs and strings are ranked batch by batch, so every batch must get ranks from the same order.
"""

import random

import polars as pl
import pytest

from maplib import Model

N = 300_000
EX = "http://example.net/"


@pytest.fixture(scope="module")
def model():
    rnd = random.Random(3)
    names = [f"{rnd.randrange(10**9):09d}" for _ in range(N)]
    words = ["apple", "Banana", "cherry", "äble", "zeta", "Ångström"]
    lines = []
    for i, name in enumerate(names):
        lines.append(f"<{EX}s{i}> <{EX}iri> <{EX}{name}> .")
        word = words[i % len(words)]
        lang = ["en", "nb", "de"][i % 3]
        lines.append(f'<{EX}s{i}> <{EX}lang> "{word}{i % 97}"@{lang} .')
    m = Model()
    m.reads("\n".join(lines) + "\n", format="ntriples")
    return m, names


def test_order_by_iri_over_many_batches(model):
    m, names = model
    df = m.query(f"SELECT ?o WHERE {{ ?s <{EX}iri> ?o }} ORDER BY ?o")
    assert df.height == N
    assert df["o"].to_list() == [f"<{EX}{n}>" for n in sorted(names)]


def test_order_by_iri_desc_with_offset(model):
    m, names = model
    df = m.query(
        f"SELECT ?o WHERE {{ ?s <{EX}iri> ?o }} ORDER BY DESC(?o) LIMIT 5 OFFSET {N // 2}"
    )
    expected = sorted(names, reverse=True)[N // 2 : N // 2 + 5]
    assert df["o"].to_list() == [f"<{EX}{n}>" for n in expected]


def test_order_by_lang_string_over_many_batches(model):
    m, _ = model
    df = m.query(
        f"SELECT ?s ?o WHERE {{ ?s <{EX}lang> ?o }} ORDER BY ?o ?s", return_format="dataframe"
    )
    assert df.height == N
    # Sort by value, then language tag, then subject: the order of a language-tagged string.
    got = df["o"].to_list()
    lit = pl.Series(got).str.extract(r'^"(.*)"@(\w+)$', 1).to_list()
    lang = pl.Series(got).str.extract(r'^"(.*)"@(\w+)$', 2).to_list()
    keys = list(zip(lit, lang))
    assert keys == sorted(keys)


def test_order_by_two_iri_columns(model):
    m, names = model
    df = m.query(
        f"""SELECT ?s ?o WHERE {{ ?s <{EX}iri> ?o }}
        ORDER BY ?o ?s LIMIT 3"""
    )
    first = sorted(range(N), key=lambda i: (names[i], f"{EX}s{i}"))[:3]
    assert df["o"].to_list() == [f"<{EX}{names[i]}>" for i in first]
    assert df["s"].to_list() == [f"<{EX}s{i}>" for i in first]

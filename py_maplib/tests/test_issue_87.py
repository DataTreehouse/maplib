import polars as pl
import pytest

from maplib import Model

TEMPLATE = """
@prefix ex:<http://example.net/ns#>.
ex:ExampleTemplate [ ottr:IRI ?s, xsd:string ?v ] :: {
    ottr:Triple(?s, ex:hasValue, ?v)
} .
"""

N = 1000


def _df():
    return pl.DataFrame(
        {
            "s": [f"http://example.net/ns#s{i}" for i in range(N)],
            "v": [f"v{i}" for i in range(N)],
        }
    )


def _count(m):
    return m.query("SELECT ?s ?v WHERE { ?s ?p ?v }").height


def _map_template(m, data):
    m.add_template(TEMPLATE)
    m.map("http://example.net/ns#ExampleTemplate", data)


def test_map_lazyframe():
    m = Model()
    _map_template(m, _df().lazy())
    assert _count(m) == N


def test_map_lazyframe_with_preprocessing():
    m = Model()
    lf = (
        pl.LazyFrame({"i": list(range(N))})
        .with_columns(
            s=pl.format("http://example.net/ns#s{}", pl.col("i")),
            v=pl.format("v{}", pl.col("i")),
        )
        .select("s", "v")
    )
    _map_template(m, lf)
    assert _count(m) == N


def test_map_iterable_of_dataframes():
    m = Model()
    df = _df()
    _map_template(m, (df.slice(i, 100) for i in range(0, N, 100)))
    assert _count(m) == N


def test_map_list_of_mixed():
    m = Model()
    df = _df()
    _map_template(m, [df.slice(0, 300), df.slice(300, 300).lazy(), df.slice(600)])
    assert _count(m) == N


def test_map_iterable_bad_item():
    m = Model()
    with pytest.raises(Exception, match="(Iterable mapping input|no batches)"):
        _map_template(m, [_df(), 1])


def test_map_triples_lazyframe_and_iterable():
    df = _df().rename({"s": "subject", "v": "object"})
    m = Model()
    m.map_triples(df.lazy(), predicate="http://example.net/ns#p")
    assert _count(m) == N

    m = Model()
    m.map_triples(
        iter([df.slice(0, 500), df.slice(500)]), predicate="http://example.net/ns#p"
    )
    assert _count(m) == N


def test_map_default_lazyframe():
    m = Model()
    m.map_default(_df().lazy(), "s")
    assert _count(m) == N


def test_map_default_iterable_nulls_in_later_batch():
    df = pl.DataFrame(
        {
            "s": [f"http://example.net/ns#s{i}" for i in range(4)],
            "v": ["a", "b", None, "d"],
        }
    )
    m = Model()
    tmpl = m.map_default([df.slice(0, 2), df.slice(2)], "s")
    assert "default_template" in tmpl
    assert _count(m) == 3


def test_map_default_empty_iterable():
    m = Model()
    with pytest.raises(Exception, match="(Iterable mapping input|no batches)"):
        m.map_default([], "s")

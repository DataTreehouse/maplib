"""Keyset pagination of SELECT queries: Model.query_pages and Model.query_page.

Each page after the first starts after the sort keys of the last solution of the previous page,
so the pages put together are the result of the query in a total order (the ORDER BY of the
query, then the projected variables), without duplicates or gaps.
"""

from collections import Counter
import random

import polars as pl
import pytest

from maplib import Model, MaplibException

from .disk import disk_params, storage_kwargs

EX = "http://example.net/"
P = f"<{EX}p>"


def rows(df: pl.DataFrame):
    return [tuple(r) for r in df.iter_rows()]


def all_pages(m, query, page_size, **kwargs):
    pages = list(m.query_pages(query, page_size=page_size, **kwargs))
    for p in pages[:-1]:
        assert p.height == page_size
    return pages


def paged_rows(m, query, page_size, **kwargs):
    out = []
    for p in all_pages(m, query, page_size, **kwargs):
        out.extend(rows(p))
    return out


@pytest.fixture(scope="module", params=disk_params())
def model(request):
    rnd = random.Random(7)
    lines = []
    for i in range(500):
        # Few distinct values, so that the sort keys have many ties.
        lines.append(f'<{EX}s{i:03d}> {P} "{rnd.randrange(20)}"^^<http://www.w3.org/2001/XMLSchema#integer> .')
        lines.append(f"<{EX}s{i:03d}> <{EX}name> \"n{rnd.randrange(50)}\" .")
        if i % 3 == 0:
            lines.append(f"<{EX}s{i:03d}> <{EX}other> <{EX}o{rnd.randrange(30)}> .")
    m = Model(**storage_kwargs(request.param))
    m.reads("\n".join(lines) + "\n", format="ntriples")
    return m


@pytest.mark.parametrize("page_size", [1, 7, 100, 499, 500, 501, 10000])
def test_pages_are_the_ordered_result(model, page_size):
    q = f"SELECT ?s ?v WHERE {{ ?s {P} ?v }} ORDER BY ?v ?s"
    expected = rows(model.query(q))
    assert paged_rows(model, q, page_size) == expected


@pytest.mark.parametrize("page_size", [3, 64])
def test_order_by_with_ties_is_completed_by_the_projected_variables(model, page_size):
    q = f"SELECT ?s ?v WHERE {{ ?s {P} ?v }} ORDER BY DESC(?v)"
    got = paged_rows(model, q, page_size)
    expected = rows(model.query(q))
    assert Counter(got) == Counter(expected)
    assert len(set(got)) == len(got)
    # The order is that of the query, ties ordered by the projected variables.
    vs = [v for _, v in got]
    assert vs == sorted(vs, reverse=True)
    for (s1, v1), (s2, v2) in zip(got, got[1:]):
        if v1 == v2:
            assert s1 < s2


def test_no_order_by(model):
    q = f"SELECT ?s ?n WHERE {{ ?s <{EX}name> ?n }}"
    got = paged_rows(model, q, 33)
    assert Counter(got) == Counter(rows(model.query(q)))
    assert got == sorted(got)


@pytest.mark.parametrize("page_size", [1, 2, 5, 40, 1000])
def test_duplicate_solutions_are_counted(model, page_size):
    # Without DISTINCT, solutions equal in every projected variable are indistinguishable, and
    # are counted across page boundaries.
    q = f"SELECT ?v WHERE {{ ?s {P} ?v }} ORDER BY ?v"
    got = paged_rows(model, q, page_size)
    assert got == rows(model.query(q))


@pytest.mark.parametrize("page_size", [1, 4, 19, 20, 21])
def test_distinct(model, page_size):
    q = f"SELECT DISTINCT ?v WHERE {{ ?s {P} ?v }} ORDER BY DESC(?v)"
    got = paged_rows(model, q, page_size)
    assert got == rows(model.query(q))
    assert len(got) == 20


def test_distinct_ordered_by_unprojected_variable_is_refused(model):
    q = f"SELECT DISTINCT ?v WHERE {{ ?s {P} ?v }} ORDER BY ?s"
    with pytest.raises(MaplibException, match="DISTINCT"):
        list(model.query_pages(q, page_size=10))


def test_order_by_expression_and_unprojected_variable(model):
    q = f"""SELECT ?s WHERE {{ ?s {P} ?v . ?s <{EX}name> ?n }}
            ORDER BY DESC(STRLEN(?n)) ?n ?v"""
    got = paged_rows(model, q, 9)
    expected = rows(model.query(q))
    assert Counter(got) == Counter(expected)
    # The keys are not output.
    page = next(iter(model.query_pages(q, page_size=9)))
    assert page.columns == ["s"]
    full = model.query(
        f"""SELECT ?s ?n ?v WHERE {{ ?s {P} ?v . ?s <{EX}name> ?n }}
            ORDER BY DESC(STRLEN(?n)) ?n ?v ?s"""
    )
    assert got == [(s,) for s in full["s"].to_list()]


def test_optional_unbound_keys(model):
    q = f"""SELECT ?s ?o WHERE {{ ?s <{EX}name> ?n OPTIONAL {{ ?s <{EX}other> ?o }} }}
            ORDER BY ?o ?s"""
    for page_size in [10, 333]:
        assert paged_rows(model, q, page_size) == rows(model.query(q))
    q = q.replace("ORDER BY ?o ?s", "ORDER BY DESC(?o) ?s")
    assert paged_rows(model, q, 10) == rows(model.query(q))


def test_aggregates(model):
    q = f"""SELECT ?v (COUNT(?s) AS ?c) WHERE {{ ?s {P} ?v }}
            GROUP BY ?v ORDER BY DESC(?c) ?v"""
    assert paged_rows(model, q, 3) == rows(model.query(q))


def test_limit_and_offset_apply_to_the_whole_result(model):
    base = f"SELECT ?s ?v WHERE {{ ?s {P} ?v }} ORDER BY ?v ?s"
    full = rows(model.query(base))
    for offset, limit in [(0, 10), (5, 10), (13, 1), (490, 100), (600, 5), (7, 0)]:
        q = f"{base} OFFSET {offset} LIMIT {limit}"
        for page_size in [3, 10, 11]:
            got = paged_rows(model, q, page_size)
            assert got == full[offset : offset + limit], (offset, limit, page_size)


def test_page_tokens(model):
    q = f"SELECT ?s ?v WHERE {{ ?s {P} ?v }} ORDER BY ?v ?s"
    expected = rows(model.query(q))
    got = []
    token = None
    n = 0
    while True:
        page, token = model.query_page(q, page_size=40, page_token=token)
        got.extend(rows(page))
        n += 1
        if token is None:
            break
        assert isinstance(token, str)
    assert got == expected
    assert n == 13

    # An iterator can be continued from its token.
    pages = model.query_pages(q, page_size=100)
    first = next(pages)
    rest = model.query_pages(q, page_size=100, page_token=pages.token)
    assert rows(first) + [r for p in rest for r in rows(p)] == expected
    assert pages.page_size == 100


def test_token_of_another_query_is_refused(model):
    q = f"SELECT ?s ?v WHERE {{ ?s {P} ?v }} ORDER BY ?v ?s"
    _, token = model.query_page(q, page_size=10)
    with pytest.raises(MaplibException, match="another query"):
        model.query_page(q + " LIMIT 50", page_size=10, page_token=token)
    with pytest.raises(Exception, match="page token"):
        model.query_page(q, page_size=10, page_token="nonsense")


def test_iterator_ends(model):
    q = f"SELECT ?s WHERE {{ ?s {P} 3 }} ORDER BY ?s"
    pages = model.query_pages(q, page_size=1000)
    assert len(list(pages)) == 1
    assert pages.token is None
    assert list(pages) == []


def test_empty_result_gives_one_empty_page(model):
    q = f"SELECT ?s ?v WHERE {{ ?s {P} ?v FILTER(?v > 1000) }} ORDER BY ?v"
    pages = list(model.query_pages(q, page_size=10))
    assert len(pages) == 1
    assert pages[0].height == 0
    assert pages[0].columns == ["s", "v"]


def test_invalid_arguments(model):
    with pytest.raises(Exception, match="page_size"):
        model.query_pages(f"SELECT ?s WHERE {{ ?s {P} ?v }}", page_size=0)
    with pytest.raises(MaplibException, match="SELECT"):
        model.query_page(f"CONSTRUCT {{ ?s {P} ?v }} WHERE {{ ?s {P} ?v }}", page_size=10)


def test_solution_mappings(model):
    q = f"SELECT ?s ?v WHERE {{ ?s {P} ?v }} ORDER BY ?v ?s"
    pages = list(model.query_pages(q, page_size=200, solution_mappings=True))
    assert [p.mappings.height for p in pages] == [200, 200, 100]
    assert "v" in pages[0].rdf_types


def mixed_model():
    m = Model()
    m.reads(
        f"""
        <{EX}a> {P} <{EX}x> .
        <{EX}b> {P} <{EX}y> .
        <{EX}c> {P} "x" .
        <{EX}d> {P} "x"@en .
        <{EX}e> {P} "x"@de .
        <{EX}f> {P} "y"@en .
        <{EX}g> {P} 1 .
        <{EX}h> {P} 2.5 .
        <{EX}i> {P} "1.5"^^<http://www.w3.org/2001/XMLSchema#double> .
        <{EX}j> {P} "2020-01-01T10:00:00+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
        <{EX}k> {P} "2020-01-01T09:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
        <{EX}l> {P} "2020-01-01T08:00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
        <{EX}m> {P} true .
        <{EX}n> {P} _:b1 .
        <{EX}o> {P} _:b2 .
        <{EX}p> {P} "2020-01-02"^^<http://www.w3.org/2001/XMLSchema#date> .
        """,
        format="turtle",
    )
    return m


@pytest.mark.parametrize("order", ["?o ?s", "DESC(?o) ?s", "?o DESC(?s)", "DESC(?o) DESC(?s)"])
@pytest.mark.parametrize("page_size", [1, 2, 5])
def test_mixed_types(order, page_size):
    m = mixed_model()
    q = f"SELECT ?s ?o WHERE {{ ?s {P} ?o }} ORDER BY {order}"
    expected = m.query(q)
    # Pages of a column with mixed types can have different dtypes, so they are compared as lists.
    got = [x for p in all_pages(m, q, page_size) for x in p["s"].to_list()]
    assert got == expected["s"].to_list()


def test_lang_strings_and_blank_nodes_as_last_keys():
    m = mixed_model()
    q = f"SELECT ?o WHERE {{ ?s {P} ?o }} ORDER BY ?o"
    expected = m.query(q, solution_mappings=True)
    pages = list(m.query_pages(q, page_size=1))
    assert len(pages) == 16
    assert sum(p.height for p in pages) == expected.mappings.height


def test_updates_between_pages():
    # Pages see the model as it is when they are evaluated: rows are never repeated, rows that
    # stay are returned exactly once, deleted rows that were not yet returned are not, and new
    # rows are returned when they sort after the last row returned.
    m = Model()
    m.reads(
        "\n".join(f"<{EX}s{i:03d}> {P} {i} ." for i in range(0, 100, 2)) + "\n",
        format="turtle",
    )
    q = f"SELECT ?s ?v WHERE {{ ?s {P} ?v }} ORDER BY ?v"
    pages = m.query_pages(q, page_size=10)
    seen = rows(next(pages))
    assert [v for _, v in seen] == list(range(0, 20, 2))
    m.update(
        f"""INSERT DATA {{
            <{EX}s001> {P} 1 .
            <{EX}s051> {P} 51 .
        }}"""
    )
    m.update(f"DELETE DATA {{ <{EX}s060> {P} 60 . <{EX}s004> {P} 4 . }}")
    for p in pages:
        seen.extend(rows(p))
    vs = [v for _, v in seen]
    assert len(vs) == len(set(vs))
    assert vs == sorted(vs)
    assert 1 not in vs  # Before the cursor when it was added.
    assert 51 in vs
    assert 60 not in vs
    assert 4 in vs  # Already returned when it was deleted.
    assert set(range(0, 100, 2)) - {60} <= set(vs)


def test_deleted_last_row():
    m = Model()
    m.reads(
        "\n".join(f"<{EX}s{i:03d}> {P} {i} ." for i in range(30)) + "\n",
        format="turtle",
    )
    q = f"SELECT ?s ?v WHERE {{ ?s {P} ?v }} ORDER BY ?v"
    pages = m.query_pages(q, page_size=10)
    first = rows(next(pages))
    m.update(f"DELETE DATA {{ <{EX}s009> {P} 9 . }}")
    rest = [r for p in pages for r in rows(p)]
    assert [v for _, v in first + rest] == list(range(30))


def test_parameters_and_graph():
    m = Model()
    m.reads(
        "\n".join(f"<{EX}s{i:03d}> {P} {i} ." for i in range(30)) + "\n",
        format="turtle",
        graph=f"{EX}g",
    )
    q = f"SELECT ?s ?v WHERE {{ ?s {P} ?v FILTER(?v >= ?min) }} ORDER BY DESC(?v)"
    got = paged_rows(m, q, 4, graph=f"{EX}g", bindings={"min": "10"})
    assert [v for _, v in got] == list(range(29, 9, -1))
    assert paged_rows(m, q, 4, bindings={"min": "10"}) == []


@pytest.mark.parametrize("page_size", [1, 7, 64])
def test_order_by_computed_strings(model, page_size):
    # Strings computed by the query are not in the model's dictionaries, so the cursor is
    # ranked together with the solutions instead of on its own.
    q = f"""SELECT ?s ?u WHERE {{ ?s <{EX}name> ?n BIND(UCASE(CONCAT(?n, "-", STR(?s))) AS ?u) }}
            ORDER BY DESC(?u)"""
    assert paged_rows(model, q, page_size) == rows(model.query(q))


def test_duplicates_with_deleted_tie():
    m = Model()
    m.reads(
        "\n".join(f"<{EX}s{i:03d}> {P} {i // 4} ." for i in range(40)) + "\n",
        format="turtle",
    )
    q = f"SELECT ?v WHERE {{ ?s {P} ?v }} ORDER BY ?v"
    pages = m.query_pages(q, page_size=6)
    first = rows(next(pages))
    # The page ends in the middle of the four rows with ?v = 1. Removing one of the two not
    # yet returned leaves one.
    assert [v for (v,) in first] == [0, 0, 0, 0, 1, 1]
    m.update(f"DELETE DATA {{ <{EX}s007> {P} 1 . }}")
    rest = [r for p in pages for r in rows(p)]
    vs = [v for (v,) in first + rest]
    assert vs == sorted([i // 4 for i in range(40) if i != 7])

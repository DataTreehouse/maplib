"""Keyset pagination while the model changes, compared with SQL keyset (seek) pagination.

SQL keyset pagination (e.g. ``WHERE (k, id) > (?, ?) ORDER BY k, id LIMIT n``, with a unique
key last) gives each page from the table as it is when the page is read. Pages from
``Model.query_pages`` are documented to behave the same: a solution is never returned twice,
solutions in the result throughout are returned exactly once, removed solutions are not
returned once removed, and added solutions are returned if they sort after the last solution
returned (until the last page, which is the page that finds no solution after it). Here the
same random inserts and deletes are applied to a maplib model and a sqlite table between
pages, and every page must be the same.
"""

import json
import random
import sqlite3

import pytest

from maplib import Model

from .disk import disk_params, storage_kwargs

EX = "http://example.net/"
P = f"<{EX}k>"
Q = f"SELECT ?s ?k ?n WHERE {{ ?s {P} ?k ; <{EX}n> ?n }} ORDER BY DESC(?k) ?n"
SQL = """SELECT id, k, n FROM t WHERE (k < ?) OR (k = ? AND n > ?) OR (k = ? AND n = ? AND id > ?)
         ORDER BY k DESC, n, id LIMIT ?"""
SQL_FIRST = "SELECT id, k, n FROM t ORDER BY k DESC, n, id LIMIT ?"


def triples(rows):
    return "\n".join(f'<{i}> {P} {k} .\n<{i}> <{EX}n> "{n}" .' for i, k, n in rows) + "\n"


def page_rows(s):
    return [
        (b["s"]["value"], int(b["k"]["value"]), b["n"]["value"])
        for b in json.loads(s)["results"]["bindings"]
    ]


@pytest.mark.parametrize("storage", disk_params())
@pytest.mark.parametrize("seed", [1, 2, 3, 4])
@pytest.mark.parametrize("page_size", [1, 3, 8])
def test_pages_follow_sql_keyset_semantics_under_updates(storage, seed, page_size):
    rnd = random.Random(seed)
    next_id = [0]

    def new_row():
        next_id[0] += 1
        # Few distinct values of k and n, so that the order has ties broken by the IRI.
        return (f"{EX}s{next_id[0]:04d}", rnd.randrange(6), f"n{rnd.randrange(3)}")

    rows = [new_row() for _ in range(30)]
    m = Model(**storage_kwargs(storage))
    m.reads(triples(rows), format="turtle")
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE t (id TEXT PRIMARY KEY, k INTEGER, n TEXT)")
    db.executemany("INSERT INTO t VALUES (?, ?, ?)", rows)

    pages = m.query_pages(Q, page_size=page_size, return_format="json")
    last = None
    seen = []
    while True:
        if last is None:
            expected = db.execute(SQL_FIRST, (page_size,)).fetchall()
        else:
            i, k, n = last
            expected = db.execute(SQL, (k, k, n, k, n, i, page_size)).fetchall()
        page = next(pages, None)
        got = page_rows(page) if page is not None else []
        assert got == [tuple(r) for r in expected], f"after {last}"
        if not got:
            break
        seen.extend(got)
        last = got[-1]
        # The iterator ends when a page finds no solution after it, as a SQL loop that
        # stops at a short page would, so later inserts are not read.
        if pages.token is None:
            break
        # Insert and delete rows, some before and some after the cursor.
        for _ in range(rnd.randrange(3)):
            r = new_row()
            m.update(f"INSERT DATA {{ {triples([r])} }}")
            db.execute("INSERT INTO t VALUES (?, ?, ?)", r)
        current = db.execute("SELECT id, k, n FROM t").fetchall()
        for r in rnd.sample(current, min(len(current), rnd.randrange(3))):
            m.update(f"DELETE DATA {{ {triples([r])} }}")
            db.execute("DELETE FROM t WHERE id = ?", (r[0],))
    assert len(seen) == len(set(seen))


def test_duplicates_are_counted_not_keyed():
    # SQL keyset pagination needs a unique key. Paging also works without one: solutions
    # equal in every projected variable are indistinguishable, and are counted.
    m = Model()
    m.reads(triples([(f"{EX}s{i:02d}", i % 3, "n") for i in range(12)]), format="turtle")
    q = f"SELECT ?k WHERE {{ ?s {P} ?k }} ORDER BY ?k"
    for page_size in [1, 2, 3, 5]:
        got = [r for p in m.query_pages(q, page_size=page_size, return_format="json")
               for r in json.loads(p)["results"]["bindings"]]
        assert [int(b["k"]["value"]) for b in got] == sorted(i % 3 for i in range(12))

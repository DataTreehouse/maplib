"""Window functions (ROW_NUMBER, RANK, DENSE_RANK and aggregates OVER (PARTITION BY ... ORDER BY
...)) and ordered aggregates (GROUP_CONCAT(... ORDER BY ...), SAMPLE(... ORDER BY ...)).

No W3C or other open test suite covers these extensions (they are proposals for SPARQL,
w3c-cg/sparql-dev#9 and #47), so the expected results follow SQL window semantics."""

import polars as pl
import pytest

from maplib import Model

PREFIX = "PREFIX : <http://example.org/>\n"

# Sales per region. :n1 and :n2 tie, :s3 has a string amount (mixed types), :s4 has no amount
# (unbound with OPTIONAL) and :x has no region (unbound partition key).
DATA = """
@prefix : <http://example.org/> .
:n1 :region :north ; :amount 20 ; :name "n1" .
:n2 :region :north ; :amount 20 ; :name "n2" .
:n3 :region :north ; :amount 10 ; :name "n3" .
:n4 :region :north ; :amount 30 ; :name "n4" .
:s1 :region :south ; :amount 5 ; :name "s1" .
:s2 :region :south ; :amount 7 ; :name "s2" .
:s3 :region :south ; :amount "many" ; :name "s3" .
:s4 :region :south ; :name "s4" .
:x :amount 1 ; :name "x" .
"""


@pytest.fixture(scope="module")
def m():
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def local(iri):
    if iri is None:
        return None
    return iri.rsplit("/", 1)[-1].rstrip(">")


def rows(df, key="s"):
    """Rows of the result as dicts keyed by the local name of `key`."""
    out = {}
    for r in df.iter_rows(named=True):
        out[local(r[key])] = {k: v for k, v in r.items() if k != key}
    return out


NUMERIC_SALES = """
?s :region ?r ; :amount ?a .
FILTER(isNumeric(?a))
"""


def test_row_number_rank_dense_rank_with_ties(m):
    df = m.query(
        PREFIX
        + f"""SELECT ?s
            (ROW_NUMBER() OVER (PARTITION BY ?r ORDER BY DESC(?a) ?s) AS ?rn)
            (RANK() OVER (PARTITION BY ?r ORDER BY DESC(?a)) AS ?rk)
            (DENSE_RANK() OVER (PARTITION BY ?r ORDER BY DESC(?a)) AS ?dr)
            WHERE {{ {NUMERIC_SALES} }}"""
    )
    r = rows(df)
    assert {s: (v["rn"], v["rk"], v["dr"]) for s, v in r.items()} == {
        "n4": (1, 1, 1),
        "n1": (2, 2, 2),
        "n2": (3, 2, 2),
        "n3": (4, 4, 3),
        "s2": (1, 1, 1),
        "s1": (2, 2, 2),
    }
    assert df.schema["rn"] == pl.Int64


def test_without_partition_and_without_order(m):
    df = m.query(
        PREFIX
        + f"""SELECT ?s (ROW_NUMBER() OVER () AS ?rn) (RANK() OVER () AS ?rk)
            (DENSE_RANK() OVER (ORDER BY ?a) AS ?dr) (COUNT(*) OVER () AS ?n)
            WHERE {{ {NUMERIC_SALES} }}"""
    )
    assert sorted(df["rn"].to_list()) == [1, 2, 3, 4, 5, 6]
    assert set(df["rk"].to_list()) == {1}
    assert set(df["n"].to_list()) == {6}
    r = rows(df)
    # 5, 7, 10, 20, 20, 30
    assert {s: v["dr"] for s, v in r.items()} == {
        "s1": 1, "s2": 2, "n3": 3, "n1": 4, "n2": 4, "n4": 5
    }


def test_unbound_order_keys_are_lowest(m):
    df = m.query(
        PREFIX
        + """SELECT ?s
            (RANK() OVER (PARTITION BY ?r ORDER BY ?a) AS ?asc)
            (RANK() OVER (PARTITION BY ?r ORDER BY DESC(?a)) AS ?desc)
            (DENSE_RANK() OVER (PARTITION BY ?r ORDER BY ?a) AS ?dasc)
            (DENSE_RANK() OVER (PARTITION BY ?r ORDER BY DESC(?a)) AS ?ddesc)
            WHERE { ?s :region :south . BIND(:south AS ?r)
                    OPTIONAL { ?s :amount ?a FILTER(isNumeric(?a)) } }"""
    )
    r = rows(df)
    # s3 and s4 are unbound (s3's amount is filtered out), so they are peers, first ascending
    # and last descending.
    assert {s: v["asc"] for s, v in r.items()} == {"s3": 1, "s4": 1, "s1": 3, "s2": 4}
    assert {s: v["desc"] for s, v in r.items()} == {"s2": 1, "s1": 2, "s3": 3, "s4": 3}
    assert {s: v["dasc"] for s, v in r.items()} == {"s3": 1, "s4": 1, "s1": 2, "s2": 3}
    assert {s: v["ddesc"] for s, v in r.items()} == {"s2": 1, "s1": 2, "s3": 3, "s4": 3}


@pytest.mark.parametrize("function", ["RANK", "DENSE_RANK"])
@pytest.mark.parametrize("direction", ["ASC", "DESC"])
def test_rank_of_one_key_agrees_with_sorting(function, direction):
    # RANK and DENSE_RANK of one key use Polars' rank without sorting, with a second (constant)
    # key they sort: both must agree, with ties, unbound keys and partitions.
    rows_ttl = []
    for i in range(300):
        value = "" if i % 7 == 0 else f" ; :v {(i * 37) % 11}"
        rows_ttl.append(f":i{i} :g :g{i % 5}{value} .")
    m2 = Model()
    m2.reads("@prefix : <http://example.org/> .\n" + "\n".join(rows_ttl), format="turtle")
    df = m2.query(
        PREFIX
        + f"""SELECT ?s
            ({function}() OVER (PARTITION BY ?g ORDER BY {direction}(?v)) AS ?one)
            ({function}() OVER (PARTITION BY ?g ORDER BY {direction}(?v) (1)) AS ?two)
            WHERE {{ ?s :g ?g OPTIONAL {{ ?s :v ?v }} }}"""
    )
    assert df.height == 300
    assert df["one"].to_list() == df["two"].to_list()


def test_unbound_partition_key_is_a_partition(m):
    df = m.query(
        PREFIX
        + """SELECT ?s (COUNT(*) OVER (PARTITION BY ?r) AS ?n)
            WHERE { ?s :name ?name OPTIONAL { ?s :region ?r } }"""
    )
    r = rows(df)
    assert r["x"]["n"] == 1
    assert r["n1"]["n"] == 4
    assert r["s4"]["n"] == 4


def test_mixed_type_order_keys(m):
    # The amounts are integers and a string: numbers and strings are not peers, and the order
    # is that of ORDER BY.
    order = m.query(
        PREFIX + "SELECT ?s WHERE { ?s :region :south ; :amount ?a } ORDER BY ?a ?s"
    )
    expected = [local(s) for s in order["s"].to_list()]
    df = m.query(
        PREFIX
        + """SELECT ?s (ROW_NUMBER() OVER (ORDER BY ?a ?s) AS ?rn)
            (DENSE_RANK() OVER (ORDER BY ?a) AS ?dr)
            WHERE { ?s :region :south ; :amount ?a }"""
    )
    got = [local(s) for s in df.sort("rn")["s"].to_list()]
    assert got == expected
    assert sorted(df["dr"].to_list()) == [1, 2, 3]


def test_mixed_type_partition_keys(m):
    # Partition by the amount, which is an integer, a string or unbound.
    df = m.query(
        PREFIX
        + """SELECT ?s (COUNT(*) OVER (PARTITION BY ?a) AS ?n)
            WHERE { ?s :name ?name OPTIONAL { ?s :amount ?a } }"""
    )
    r = rows(df)
    assert r["n1"]["n"] == 2  # 20 twice
    assert r["s3"]["n"] == 1  # "many"
    assert r["s4"]["n"] == 1  # unbound
    assert r["x"]["n"] == 1


def test_partition_aggregates(m):
    df = m.query(
        PREFIX
        + f"""SELECT ?s (SUM(?a) OVER (PARTITION BY ?r) AS ?sum)
            (AVG(?a) OVER (PARTITION BY ?r) AS ?avg)
            (MIN(?a) OVER (PARTITION BY ?r) AS ?min)
            (MAX(?a) OVER (PARTITION BY ?r) AS ?max)
            (COUNT(?a) OVER (PARTITION BY ?r) AS ?count)
            (COUNT(DISTINCT ?a) OVER (PARTITION BY ?r) AS ?distinct)
            WHERE {{ {NUMERIC_SALES} }}"""
    )
    r = rows(df)
    assert r["n1"]["sum"] == 80
    assert float(r["n1"]["avg"]) == 20
    assert (r["n1"]["min"], r["n1"]["max"]) == (10, 30)
    assert (r["n1"]["count"], r["n1"]["distinct"]) == (4, 3)
    assert r["s1"]["sum"] == 12
    assert (r["s2"]["min"], r["s2"]["max"]) == (5, 7)


def test_partition_aggregate_of_mixed_types(m):
    # SUM is an error (unbound) for a partition with a string, MIN and MAX use the order of
    # ORDER BY, where numbers come before strings.
    df = m.query(
        PREFIX
        + """SELECT ?s (SUM(?a) OVER (PARTITION BY ?r) AS ?sum)
            (STR(MAX(?a) OVER (PARTITION BY ?r)) AS ?max)
            (STR(MIN(?a) OVER (PARTITION BY ?r)) AS ?min)
            WHERE { ?s :region ?r ; :amount ?a }"""
    )
    r = rows(df)
    assert r["s1"]["sum"] is None
    assert r["n1"]["sum"] == 80
    assert r["s1"]["max"] == "many"
    assert r["s1"]["min"] == "5"


def test_running_aggregates_include_peers(m):
    # With ORDER BY, the frame is from the start of the partition to the last peer of the row.
    df = m.query(
        PREFIX
        + f"""SELECT ?s (SUM(?a) OVER (PARTITION BY ?r ORDER BY ?a) AS ?sum)
            (COUNT(*) OVER (PARTITION BY ?r ORDER BY ?a) AS ?n)
            (AVG(?a) OVER (PARTITION BY ?r ORDER BY ?a) AS ?avg)
            (MAX(?a) OVER (PARTITION BY ?r ORDER BY DESC(?a)) AS ?max)
            (MIN(?a) OVER (PARTITION BY ?r ORDER BY DESC(?a)) AS ?min)
            WHERE {{ {NUMERIC_SALES} }}"""
    )
    r = rows(df)
    # north: 10, 20, 20, 30
    assert {s: r[s]["sum"] for s in ["n3", "n1", "n2", "n4"]} == {
        "n3": 10, "n1": 50, "n2": 50, "n4": 80
    }
    assert {s: r[s]["n"] for s in ["n3", "n1", "n2", "n4"]} == {
        "n3": 1, "n1": 3, "n2": 3, "n4": 4
    }
    assert float(r["n1"]["avg"]) == pytest.approx(50 / 3)
    assert {s: r[s]["max"] for s in ["n4", "n1", "n3"]} == {"n4": 30, "n1": 30, "n3": 30}
    assert {s: r[s]["min"] for s in ["n4", "n1", "n3"]} == {"n4": 30, "n1": 20, "n3": 10}
    assert (r["s1"]["sum"], r["s2"]["sum"]) == (5, 12)


def test_running_aggregates_with_unbound_values(m):
    df = m.query(
        PREFIX
        + """SELECT ?s (SUM(?a) OVER (ORDER BY ?name) AS ?sum)
            (COUNT(?a) OVER (ORDER BY ?name) AS ?count)
            (MAX(?a) OVER (ORDER BY ?name) AS ?max)
            WHERE { ?s :region :south ; :name ?name
                    OPTIONAL { ?s :amount ?a FILTER(isNumeric(?a)) } }"""
    )
    r = rows(df)
    # s1: 5, s2: 7, s3 and s4 unbound
    assert [r[s]["sum"] for s in ["s1", "s2", "s3", "s4"]] == [5, 12, 12, 12]
    assert [r[s]["count"] for s in ["s1", "s2", "s3", "s4"]] == [1, 2, 2, 2]
    assert [r[s]["max"] for s in ["s1", "s2", "s3", "s4"]] == [5, 7, 7, 7]


def test_running_aggregate_of_mixed_types(m):
    # Integers and a decimal: the running sum is promoted once the decimal is in the frame, and
    # unbound from the string on.
    m2 = Model()
    m2.reads(
        """@prefix : <http://example.org/> .
        :a :o 1 ; :k 1 . :b :o 2.5 ; :k 2 . :c :o 3 ; :k 3 . :d :o "x" ; :k 4 . :e :o 4 ; :k 5 .""",
        format="turtle",
    )
    df = m2.query(
        PREFIX
        + """SELECT ?s (STR(SUM(?o) OVER (ORDER BY ?k)) AS ?sum)
            (DATATYPE(SUM(?o) OVER (ORDER BY ?k)) AS ?dt)
            (STR(MAX(?o) OVER (ORDER BY ?k)) AS ?max)
            WHERE { ?s :o ?o ; :k ?k }"""
    )
    r = rows(df)
    assert float(r["a"]["sum"]) == 1 and r["a"]["dt"].endswith("#integer>")
    assert float(r["b"]["sum"]) == 3.5 and r["b"]["dt"].endswith("#decimal>")
    assert float(r["c"]["sum"]) == 6.5
    assert r["d"]["sum"] is None and r["e"]["sum"] is None
    assert r["c"]["max"] == "3"
    assert r["d"]["max"] == "x"


def test_rows_frames(m):
    df = m.query(
        PREFIX
        + f"""SELECT ?s
            (SUM(?a) OVER (PARTITION BY ?r ORDER BY ?a ?s ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ?moving)
            (AVG(?a) OVER (PARTITION BY ?r ORDER BY ?a ?s ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS ?centered)
            (COUNT(*) OVER (PARTITION BY ?r ORDER BY ?a ?s ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ?rest)
            (SUM(?a) OVER (PARTITION BY ?r ORDER BY ?a ?s ROWS UNBOUNDED PRECEDING) AS ?running)
            (GROUP_CONCAT(?name; SEPARATOR="") OVER (PARTITION BY ?r ORDER BY ?a ?s ROWS 2 PRECEDING) AS ?names)
            WHERE {{ {NUMERIC_SALES} ?s :name ?name }}"""
    )
    r = rows(df)
    # north in order: n3 10, n1 20, n2 20, n4 30
    order = ["n3", "n1", "n2", "n4"]
    assert [r[s]["moving"] for s in order] == [10, 30, 40, 50]
    assert [float(r[s]["centered"]) for s in order] == [15, 50 / 3, 70 / 3, 25]
    assert [r[s]["rest"] for s in order] == [4, 3, 2, 1]
    # ROWS counts rows, so the tie n1, n2 is not one step
    assert [r[s]["running"] for s in order] == [10, 30, 50, 80]
    assert [r[s]["names"] for s in order] == ["n3", "n3n1", "n3n1n2", "n1n2n4"]
    # Frames do not cross partitions
    assert (r["s1"]["moving"], r["s2"]["moving"]) == (5, 12)


def test_window_in_bind_sees_only_its_group(m):
    # The BIND is evaluated on the group it is in, before the join with the other pattern.
    df = m.query(
        PREFIX
        + """SELECT ?s ?rn WHERE {
            ?s :amount 20 .
            { ?s :name ?name BIND(ROW_NUMBER() OVER (ORDER BY ?name) AS ?rn) }
        }"""
    )
    # names in order: n1 n2 n3 n4 s1 s2 s3 s4 x
    assert rows(df) == {"n1": {"rn": 1}, "n2": {"rn": 2}}


def test_window_in_subquery_is_not_restricted_by_values(m):
    df = m.query(
        PREFIX
        + """SELECT ?s ?rn WHERE {
            VALUES ?s { :s1 :x }
            { SELECT ?s (ROW_NUMBER() OVER (ORDER BY ?name) AS ?rn) WHERE { ?s :name ?name } }
        }"""
    )
    assert rows(df) == {"s1": {"rn": 5}, "x": {"rn": 9}}


def test_top_n_per_group_with_having(m):
    # The syntax of w3c-cg/sparql-dev#47: HAVING without GROUP BY filters on the window.
    df = m.query(
        PREFIX
        + f"""SELECT ?s ?r WHERE {{ {NUMERIC_SALES} }}
            HAVING (ROW_NUMBER() OVER (PARTITION BY ?r ORDER BY DESC(?a) ?s) <= 2)"""
    )
    assert set(rows(df)) == {"n4", "n1", "s2", "s1"}


def test_top_n_per_group_with_filter(m):
    # The FILTER of NUMERIC_SALES is in its own group: a FILTER applies to the whole group it
    # is in, after the BIND, so the window would also see :s3 "many".
    df = m.query(
        PREFIX
        + f"""SELECT ?s ?rn WHERE {{
            {{ {NUMERIC_SALES} }}
            BIND(RANK() OVER (PARTITION BY ?r ORDER BY DESC(?a)) AS ?rn)
            FILTER(?rn <= 2)
        }}"""
    )
    assert {s: v["rn"] for s, v in rows(df).items()} == {
        "n4": 1, "n1": 2, "n2": 2, "s2": 1, "s1": 2
    }


def test_window_filter_with_variable_predicate(m):
    # A filter on a pattern with a variable predicate is split by the query rewriter, which
    # must not move a window function.
    df = m.query(
        PREFIX
        + """SELECT ?s ?p ?o WHERE { ?s ?p ?o FILTER(?s = :n1 || ?s = :n2)
            FILTER(ROW_NUMBER() OVER (ORDER BY ?s ?p ?o) <= 2) }"""
    )
    assert df.height == 2


def test_window_over_aggregates(m):
    df = m.query(
        PREFIX
        + f"""SELECT ?r (SUM(?a) AS ?total)
            (RANK() OVER (ORDER BY DESC(SUM(?a))) AS ?rk)
            (SUM(SUM(?a)) OVER () AS ?all)
            WHERE {{ {NUMERIC_SALES} }} GROUP BY ?r"""
    )
    r = rows(df, "r")
    assert r == {
        "north": {"total": 80, "rk": 1, "all": 92},
        "south": {"total": 12, "rk": 2, "all": 92},
    }


def test_window_partition_by_expression(m):
    df = m.query(
        PREFIX
        + f"""SELECT ?s (COUNT(*) OVER (PARTITION BY (?a > 10), ?r) AS ?n)
            WHERE {{ {NUMERIC_SALES} }}"""
    )
    r = rows(df)
    assert (r["n1"]["n"], r["n3"]["n"], r["s1"]["n"]) == (3, 1, 2)


def test_window_errors(m):
    with pytest.raises(Exception):
        m.query(PREFIX + "SELECT (RANK() OVER (ROWS 1 PRECEDING) AS ?x) WHERE { ?s ?p ?o }")
    with pytest.raises(Exception):
        m.query(
            PREFIX
            + "SELECT (SUM(?o) OVER (ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW) AS ?x) "
            "WHERE { ?s ?p ?o }"
        )


def test_window_keywords_are_case_insensitive(m):
    df = m.query(
        PREFIX
        + f"""SELECT ?s (row_number() over (partition by ?r order by desc(?a) ?s) AS ?rn)
            WHERE {{ {NUMERIC_SALES} }}"""
    )
    assert rows(df)["n4"]["rn"] == 1


# Ordered aggregates


def test_group_concat_order_by(m):
    df = m.query(
        PREFIX
        + f"""SELECT ?r
            (GROUP_CONCAT(?name ; SEPARATOR=',' ORDER BY DESC(?a) ?name) AS ?desc)
            (GROUP_CONCAT(?name ; SEPARATOR=',' ; ORDER BY ?a DESC(?name)) AS ?asc)
            (GROUP_CONCAT(?name ORDER BY ?name) AS ?default_separator)
            (GROUP_CONCAT(?name ; ORDER BY DESC(?name)) AS ?semicolon)
            WHERE {{ {NUMERIC_SALES} ?s :name ?name }} GROUP BY ?r"""
    )
    r = rows(df, "r")
    assert r["north"]["desc"] == "n4,n1,n2,n3"
    assert r["north"]["asc"] == "n3,n2,n1,n4"
    assert r["north"]["default_separator"] == "n1 n2 n3 n4"
    assert r["north"]["semicolon"] == "n4 n3 n2 n1"
    assert r["south"]["desc"] == "s2,s1"


def test_group_concat_order_by_with_unbound_and_mixed_type_keys(m):
    df = m.query(
        PREFIX
        + """SELECT (GROUP_CONCAT(?name; SEPARATOR="," ORDER BY ?a ?name) AS ?names)
            WHERE { ?s :region :south ; :name ?name OPTIONAL { ?s :amount ?a } }"""
    )
    # Unbound first, then the numbers, then the string
    assert df["names"].to_list() == ["s4,s1,s2,s3"]


def test_group_concat_distinct_order_by(m):
    df = m.query(
        PREFIX
        + f"""SELECT (GROUP_CONCAT(DISTINCT ?a; SEPARATOR="|" ORDER BY DESC(?a)) AS ?amounts)
            WHERE {{ {NUMERIC_SALES} }}"""
    )
    assert df["amounts"].to_list() == ["30|20|10|7|5"]


def test_sample_order_by(m):
    df = m.query(
        PREFIX
        + f"""SELECT ?r (SAMPLE(?s ORDER BY DESC(?a) ?s) AS ?top) (SAMPLE(?a ORDER BY ?a) AS ?low)
            WHERE {{ {NUMERIC_SALES} }} GROUP BY ?r"""
    )
    r = rows(df, "r")
    assert local(r["north"]["top"]) == "n4"
    assert r["north"]["low"] == 10
    assert local(r["south"]["top"]) == "s2"


def test_group_concat_order_by_with_other_aggregates(m):
    # The sort keys of an ordered aggregate are not solution variables, so COUNT(*) is not
    # affected by them.
    df = m.query(
        PREFIX
        + """SELECT ?r (GROUP_CONCAT(?name ORDER BY ?name) AS ?names) (COUNT(*) AS ?n)
            WHERE { ?s :region ?r ; :name ?name } GROUP BY ?r"""
    )
    r = rows(df, "r")
    assert r["south"] == {"names": "s1 s2 s3 s4", "n": 4}


@pytest.mark.parametrize("function", ["RANK", "DENSE_RANK"])
@pytest.mark.parametrize("direction", ["ASC", "DESC"])
@pytest.mark.parametrize("test", ["?rk = 1", "1 = ?rk", "?rk <= 1", "?rk < 2"])
@pytest.mark.parametrize("mixed", [False, True])
def test_rank_one_filter_agrees_with_ranking(function, direction, test, mixed):
    # FILTER(?rk = 1) over the BIND of a RANK is evaluated without ranking, FILTER(?rk + 0 = 1)
    # ranks: both must agree, with ties, unbound keys (first ascending), partitions with only
    # unbound keys, and (mixed) a string key among the integers, which ranks.
    rows_ttl = []
    for i in range(200):
        if i % 9 == 0:
            value = ""
        elif mixed and i == 13:
            value = ' ; :v "text"'
        else:
            value = f" ; :v {(i * 37) % 7}"
        group = 99 if i in (0, 9) else i % 4
        rows_ttl.append(f":i{i} :g :g{group}{value} .")
    m2 = Model()
    m2.reads("@prefix : <http://example.org/> .\n" + "\n".join(rows_ttl), format="turtle")

    def query(filter_expr):
        df = m2.query(
            PREFIX
            + f"""SELECT ?s ?rk WHERE {{
                ?s :g ?g OPTIONAL {{ ?s :v ?v }}
                BIND({function}() OVER (PARTITION BY ?g ORDER BY {direction}(?v)) AS ?rk)
                FILTER({filter_expr}) }}"""
        )
        return sorted(df.iter_rows())

    fast = query(test)
    assert fast == query("?rk + 0 = 1")
    assert fast and all(rk == 1 for _, rk in fast)


def test_rank_one_filter_is_joined_with_outer_solutions(m):
    df = m.query(
        PREFIX
        + """SELECT ?s ?rk WHERE {
            VALUES ?r { :north }
            { ?s :region ?r ; :amount ?a FILTER(isNumeric(?a)) }
            BIND(RANK() OVER (PARTITION BY ?r ORDER BY DESC(?a)) AS ?rk)
            FILTER(?rk = 1) }"""
    )
    # The BIND's group includes the VALUES, so the window ranks the north sales only.
    assert rows(df) == {"n4": {"rk": 1}}


@pytest.mark.parametrize("per_row", [False, True])
@pytest.mark.parametrize("rank_filter", [False, True])
def test_window_inside_lateral_is_per_left_solution(m, monkeypatch, per_row, rank_filter):
    # LATERAL evaluates its right side for all left solutions at once (or one by one with
    # MAPLIB_LATERAL_PER_ROW): the window ranks the solutions of each left solution on its own.
    if per_row:
        monkeypatch.setenv("MAPLIB_LATERAL_PER_ROW", "1")
    else:
        monkeypatch.delenv("MAPLIB_LATERAL_PER_ROW", raising=False)
    test = "?rk = 1" if rank_filter else "?rk + 0 = 1"
    df = m.query(
        PREFIX
        + f"""SELECT ?r ?s WHERE {{
            VALUES ?r {{ :north :south }}
            LATERAL {{
                {{ ?s :region ?r ; :amount ?a FILTER(isNumeric(?a)) }}
                BIND(RANK() OVER (ORDER BY DESC(?a)) AS ?rk)
                FILTER({test})
            }} }}"""
    )
    assert sorted((local(r), local(s)) for r, s in df.iter_rows()) == [
        ("north", "n4"),
        ("south", "s2"),
    ]


def test_rank_one_filter_has_the_type_of_rank():
    # FILTER(?rk = 1) over RANK finds the rows with rank 1 without ranking. Its ?rk column must
    # have the type of RANK, or a union with ranked solutions (e.g. inside LATERAL) fails.
    m = Model()
    m.reads("@prefix : <http://example.org/> .\n:a :v 2 .\n:b :v 3 .\n", format="turtle")
    q = PREFIX + "SELECT ?s ?rk WHERE {{ {{ ?s :v ?v }} BIND(RANK() OVER (ORDER BY DESC(?v)) AS ?rk) FILTER({}) }}"
    fast = m.query(q.format("?rk = 1"))
    ranked = m.query(q.format("?rk + 0 = 1"))
    assert fast.schema == ranked.schema
    assert fast["rk"].to_list() == [1]

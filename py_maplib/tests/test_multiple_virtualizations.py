"""Several virtualized databases used together in one model.

Virtualization is not in the open source build, so these tests are skipped there.
"""

from datetime import datetime, timezone

import polars as pl
import pytest

duckdb = pytest.importorskip("duckdb")
sqlalchemy = pytest.importorskip("sqlalchemy")

from sqlalchemy import Column, MetaData, Table, bindparam, select

from maplib import (
    Model,
    Parameter,
    Prefix,
    RDFType,
    Template,
    Triple,
    Variable,
    VirtualizedDatabase,
    xsd,
)

PREFIXES = """
PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
PREFIX maplib:<https://datatreehouse.github.io/maplib/vocab#>
PREFIX types:<http://example.org/types#>
"""

# Widget 1 has a time series in resource_a, which is in one database,
# widget 2 has a time series in resource_b, which is in another database.
# Both time series have the same external id, as ids need only be unique within a resource.
GRAPH = """
PREFIX case: <http://example.org/case#>
PREFIX types: <http://example.org/types#>
PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#>

case:myWidget1 a types:BigWidget .
case:myWidget2 a types:SmallWidget .
case:myWidget1 types:hasSensor case:mySensor1 .
case:myWidget2 types:hasSensor case:mySensor2 .
case:mySensor1 maplib:hasTimeseries case:myTimeseries1 .
case:mySensor2 maplib:hasTimeseries case:myTimeseries2 .
case:myTimeseries1 maplib:hasResource "resource_a" .
case:myTimeseries2 maplib:hasResource "resource_b" .
case:myTimeseries1 maplib:hasExternalId "ts" .
case:myTimeseries2 maplib:hasExternalId "ts" .
"""


def ts(second: int) -> datetime:
    return datetime(2022, 6, 1, 8, 46, second, tzinfo=timezone.utc)


TS_A = pl.DataFrame(
    {
        "timestamp": [ts(s) for s in [52, 53, 54, 55, 56, 57]],
        "value": [1, 10, 100, 301, 3000, 31000],
    }
)
# Shares the timestamps 54-57 with TS_A
TS_B = pl.DataFrame(
    {
        "timestamp": [ts(s) for s in [54, 55, 56, 57, 58, 59]],
        "value": [2, 20, 200, 2000, 19000, 191000],
    }
)


class RecordingDuckDB:
    """A DuckDB database with a single table, which records the SQL queries it gets."""

    def __init__(self, table: str, df: pl.DataFrame):
        con = duckdb.connect()
        con.execute("SET TIME ZONE 'UTC';")
        con.execute(f"""CREATE TABLE {table} ("timestamp" TIMESTAMPTZ, "value" INTEGER)""")
        con.append(table, df=df.to_pandas())
        self.con = con
        self.queries = []

    def query(self, sql: str) -> pl.DataFrame:
        self.queries.append(sql)
        return self.con.sql(sql).pl()


def resource_sql(table: str, resource: str):
    t = Table(table, MetaData(), Column("timestamp"), Column("value"))
    return select(
        bindparam(f"id_{resource}", "ts").label("id"),
        t.c.timestamp.label("timestamp"),
        t.c.value.label("value"),
    )


MAPLIB = "https://datatreehouse.github.io/maplib/vocab#"
LEGACY_CHRONTEXT = "https://github.com/DataTreehouse/chrontext#"


def template(name: str, namespace: str = MAPLIB) -> Template:
    ct = Prefix(namespace)
    tpl = Prefix("http://example.org/templates#")
    id = Variable("id")
    timestamp = Variable("timestamp")
    value = Variable("value")
    dp = Variable("dp")
    return Template(
        iri=tpl.suf(name),
        parameters=[
            Parameter(id, rdf_type=RDFType.Literal(xsd.string)),
            Parameter(timestamp, rdf_type=RDFType.Literal(xsd.dateTime)),
            Parameter(value, rdf_type=RDFType.Literal(xsd.double)),
        ],
        instances=[
            Triple(id, ct.suf("hasDataPoint"), dp),
            Triple(dp, ct.suf("hasValue"), value),
            Triple(dp, ct.suf("hasTimestamp"), timestamp),
        ],
    )


def virtualized_database(db: RecordingDuckDB, table: str, resource: str):
    try:
        return VirtualizedDatabase(
            database=db,
            resource_sql_map={resource: resource_sql(table, resource)},
            sql_dialect="duckdb",
        )
    except NotImplementedError:
        pytest.skip("Virtualization is not in this build")


@pytest.fixture(scope="function")
def dbs():
    db_a = RecordingDuckDB("table_a", TS_A)
    db_b = RecordingDuckDB("table_b", TS_B)
    m = Model()
    m.reads(GRAPH, format="turtle")
    m.add_virtualization(
        virtualized_database(db_a, "table_a", "resource_a"),
        resources={"resource_a": template("resource_a")},
    )
    m.add_virtualization(
        virtualized_database(db_b, "table_b", "resource_b"),
        resources={"resource_b": template("resource_b")},
    )
    return m, db_a, db_b


def widget(i: int) -> str:
    return f"<http://example.org/case#myWidget{i}>"


def assert_only_queries_own_table(db: RecordingDuckDB, own: str, other: str):
    assert len(db.queries) > 0
    for q in db.queries:
        assert own in q
        assert other not in q


def test_query_over_two_virtualizations(dbs):
    m, db_a, db_b = dbs
    q = (
        PREFIXES
        + """
    SELECT ?w ?t ?v WHERE {
        ?w types:hasSensor ?s .
        ?s maplib:hasTimeseries ?ts .
        ?ts maplib:hasDataPoint ?dp .
        ?dp maplib:hasTimestamp ?t .
        ?dp maplib:hasValue ?v .
    }
    """
    )
    df = m.query(q)
    assert df.height == TS_A.height + TS_B.height
    for i, expected in [(1, TS_A), (2, TS_B)]:
        actual = df.filter(pl.col("w") == widget(i)).sort("t")
        assert actual.get_column("v").cast(pl.Int64).to_list() == expected.get_column(
            "value"
        ).to_list()
    assert_only_queries_own_table(db_a, "table_a", "table_b")
    assert_only_queries_own_table(db_b, "table_b", "table_a")


def test_only_the_matching_virtualization_is_queried(dbs):
    m, db_a, db_b = dbs
    q = (
        PREFIXES
        + """
    SELECT ?w ?t ?v WHERE {
        ?w a types:BigWidget .
        ?w types:hasSensor ?s .
        ?s maplib:hasTimeseries ?ts .
        ?ts maplib:hasDataPoint ?dp .
        ?dp maplib:hasTimestamp ?t .
        ?dp maplib:hasValue ?v .
        FILTER(?v > 50)
    }
    """
    )
    df = m.query(q).sort("t")
    assert df.get_column("v").cast(pl.Int64).to_list() == [100, 301, 3000, 31000]
    assert len(db_a.queries) > 0
    assert db_b.queries == []


def test_group_by_over_two_virtualizations(dbs):
    m, db_a, db_b = dbs
    q = (
        PREFIXES
        + """
    SELECT ?w (SUM(?v) as ?sum_v) WHERE {
        ?w types:hasSensor ?s .
        ?s maplib:hasTimeseries ?ts .
        ?ts maplib:hasDataPoint ?dp .
        ?dp maplib:hasTimestamp ?t .
        ?dp maplib:hasValue ?v .
        FILTER(?t > "2022-06-01T08:46:53Z"^^xsd:dateTime) .
    } GROUP BY ?w
    """
    )
    df = m.query(q).sort("w")
    cutoff = ts(53)
    expected = [
        x.filter(pl.col("timestamp") > cutoff).get_column("value").sum()
        for x in [TS_A, TS_B]
    ]
    assert df.get_column("w").to_list() == [widget(1), widget(2)]
    assert [int(x) for x in df.get_column("sum_v").to_list()] == expected
    assert len(db_a.queries) > 0
    assert len(db_b.queries) > 0


def test_aggregate_across_virtualizations(dbs):
    m, _, _ = dbs
    q = (
        PREFIXES
        + """
    SELECT (COUNT(?v) as ?n) (MAX(?v) as ?max_v) WHERE {
        ?w types:hasSensor ?s .
        ?s maplib:hasTimeseries ?ts .
        ?ts maplib:hasDataPoint ?dp .
        ?dp maplib:hasValue ?v .
    }
    """
    )
    df = m.query(q)
    assert df.get_column("n").to_list() == [TS_A.height + TS_B.height]
    assert int(df.get_column("max_v")[0]) == 191000


def test_join_time_series_of_two_virtualizations(dbs):
    # The time series are joined on the timestamp. They are in different databases,
    # so each database gets its own query, and the join is done afterwards.
    m, db_a, db_b = dbs
    q = (
        PREFIXES
        + """
    SELECT ?t ?v1 ?v2 WHERE {
        ?w1 a types:BigWidget .
        ?w2 a types:SmallWidget .
        ?w1 types:hasSensor ?s1 .
        ?w2 types:hasSensor ?s2 .
        ?s1 maplib:hasTimeseries ?ts1 .
        ?s2 maplib:hasTimeseries ?ts2 .
        ?ts1 maplib:hasDataPoint ?dp1 .
        ?ts2 maplib:hasDataPoint ?dp2 .
        ?dp1 maplib:hasTimestamp ?t .
        ?dp2 maplib:hasTimestamp ?t .
        ?dp1 maplib:hasValue ?v1 .
        ?dp2 maplib:hasValue ?v2 .
        FILTER(?v1 > ?v2) .
    }
    """
    )
    df = m.query(q).sort("t")
    expected = (
        TS_A.join(TS_B, on="timestamp", suffix="_b")
        .filter(pl.col("value") > pl.col("value_b"))
        .sort("timestamp")
    )
    assert df.height == expected.height
    assert df.get_column("v1").cast(pl.Int64).to_list() == expected.get_column("value").to_list()
    assert df.get_column("v2").cast(pl.Int64).to_list() == expected.get_column("value_b").to_list()
    assert_only_queries_own_table(db_a, "table_a", "table_b")
    assert_only_queries_own_table(db_b, "table_b", "table_a")


def test_union_over_two_virtualizations(dbs):
    m, _, _ = dbs
    q = (
        PREFIXES
        + """
    SELECT ?w ?v WHERE {
        { ?w a types:BigWidget . } UNION { ?w a types:SmallWidget . }
        ?w types:hasSensor ?s .
        ?s maplib:hasTimeseries ?ts .
        ?ts maplib:hasDataPoint ?dp .
        ?dp maplib:hasTimestamp ?t .
        ?dp maplib:hasValue ?v .
        FILTER(?t = "2022-06-01T08:46:55Z"^^xsd:dateTime)
    }
    """
    )
    df = m.query(q).sort("w")
    assert df.get_column("w").to_list() == [widget(1), widget(2)]
    assert df.get_column("v").cast(pl.Int64).to_list() == [301, 20]


def test_adding_a_resource_again_moves_it_to_the_new_virtualization(dbs):
    m, db_a, db_b = dbs
    # resource_b now reads from a third database, with other values
    ts_c = TS_B.with_columns(pl.col("value") * 10)
    db_c = RecordingDuckDB("table_c", ts_c)
    m.add_virtualization(
        virtualized_database(db_c, "table_c", "resource_b"),
        resources={"resource_b": template("resource_b")},
    )
    q = (
        PREFIXES
        + """
    SELECT ?w ?t ?v WHERE {
        ?w types:hasSensor ?s .
        ?s maplib:hasTimeseries ?ts .
        ?ts maplib:hasDataPoint ?dp .
        ?dp maplib:hasTimestamp ?t .
        ?dp maplib:hasValue ?v .
    }
    """
    )
    df = m.query(q)
    actual = df.filter(pl.col("w") == widget(2)).sort("t")
    assert actual.get_column("v").cast(pl.Int64).to_list() == ts_c.get_column("value").to_list()
    assert db_b.queries == []
    assert len(db_c.queries) > 0
    assert len(db_a.queries) > 0


def test_same_database_added_twice_is_one_virtualization():
    # Two calls with the same database object (and settings) are merged into one virtualized
    # database, which holds both resources.
    class BothTables(RecordingDuckDB):
        def __init__(self):
            super().__init__("table_a", TS_A)
            self.con.execute('CREATE TABLE table_b ("timestamp" TIMESTAMPTZ, "value" INTEGER)')
            self.con.append("table_b", df=TS_B.to_pandas())

    db = BothTables()
    m = Model()
    m.reads(GRAPH, format="turtle")
    m.add_virtualization(
        virtualized_database(db, "table_a", "resource_a"),
        resources={"resource_a": template("resource_a")},
    )
    m.add_virtualization(
        virtualized_database(db, "table_b", "resource_b"),
        resources={"resource_b": template("resource_b")},
    )
    q = (
        PREFIXES
        + """
    SELECT ?t ?v1 ?v2 WHERE {
        ?w1 a types:BigWidget .
        ?w2 a types:SmallWidget .
        ?w1 types:hasSensor ?s1 .
        ?w2 types:hasSensor ?s2 .
        ?s1 maplib:hasTimeseries ?ts1 .
        ?s2 maplib:hasTimeseries ?ts2 .
        ?ts1 maplib:hasDataPoint ?dp1 .
        ?ts2 maplib:hasDataPoint ?dp2 .
        ?dp1 maplib:hasTimestamp ?t .
        ?dp2 maplib:hasTimestamp ?t .
        ?dp1 maplib:hasValue ?v1 .
        ?dp2 maplib:hasValue ?v2 .
    }
    """
    )
    df = m.query(q).sort("t")
    expected = TS_A.join(TS_B, on="timestamp", suffix="_b").sort("timestamp")
    assert df.get_column("v1").cast(pl.Int64).to_list() == expected.get_column("value").to_list()
    assert df.get_column("v2").cast(pl.Int64).to_list() == expected.get_column("value_b").to_list()
    assert len(db.queries) > 0
    # Adding resource_a again to the same database keeps resource_b
    m.add_virtualization(
        virtualized_database(db, "table_a", "resource_a"),
        resources={"resource_a": template("resource_a")},
    )
    df = m.query(q).sort("t")
    assert df.height == expected.height


def test_legacy_chrontext_iris_in_data_templates_and_queries():
    """The vocabulary moved from the chrontext namespace to the maplib vocabulary. The deprecated
    chrontext IRIs are still read as the maplib ones: in the data, in the templates of the
    virtualizations and in queries."""
    db_a = RecordingDuckDB("table_a", TS_A)
    db_b = RecordingDuckDB("table_b", TS_B)
    m = Model()
    m.reads(GRAPH.replace(MAPLIB, LEGACY_CHRONTEXT), format="turtle")
    m.add_virtualization(
        virtualized_database(db_a, "table_a", "resource_a"),
        resources={"resource_a": template("resource_a", LEGACY_CHRONTEXT)},
    )
    m.add_virtualization(
        virtualized_database(db_b, "table_b", "resource_b"),
        resources={"resource_b": template("resource_b", LEGACY_CHRONTEXT)},
    )
    body = """
    SELECT ?w ?t ?v WHERE {
        ?w types:hasSensor ?s .
        ?s ct:hasTimeseries ?ts .
        ?ts ct:hasDataPoint ?dp .
        ?dp ct:hasTimestamp ?t .
        ?dp ct:hasValue ?v .
    }
    """
    for namespace in [LEGACY_CHRONTEXT, MAPLIB]:
        q = PREFIXES + f"PREFIX ct:<{namespace}>\n" + body
        df = m.query(q)
        assert df.height == TS_A.height + TS_B.height, namespace
        for i, expected in [(1, TS_A), (2, TS_B)]:
            actual = df.filter(pl.col("w") == widget(i)).sort("t")
            assert actual.get_column("v").cast(pl.Int64).to_list() == expected.get_column(
                "value"
            ).to_list()
    # The data is stored with the maplib predicates.
    assert m.query(
        f"SELECT ?ts WHERE {{ ?ts <{MAPLIB}hasResource> ?r }}"
    ).height == 2


def test_pvalues_in_a_virtualized_query_is_an_error(dbs):
    from maplib import SolutionMappings

    m, _, _ = dbs
    q = (
        PREFIXES
        + """
    SELECT ?w ?v WHERE {
        ?w types:hasSensor ?s .
        ?s maplib:hasTimeseries ?ts .
        ?ts maplib:hasDataPoint ?dp .
        ?dp maplib:hasValue ?v .
        PVALUES (?w) h
    }
    """
    )
    params = {
        "h": SolutionMappings(
            pl.DataFrame({"w": ["http://example.org/case#myWidget1"]}), {"w": RDFType.IRI}
        )
    }
    with pytest.raises(Exception, match="PVALUES is not supported"):
        m.query(q, parameters=params)


def test_resource_template_with_variable_predicate_is_an_error():
    db = RecordingDuckDB("table_a", TS_A)
    t = template("resource_a")
    t = Template(
        iri=Prefix("http://example.org/templates#").suf("resource_a"),
        parameters=t.parameters,
        instances=t.instances
        + [Triple(Variable("id"), Variable("value"), Variable("timestamp"))],
    )
    m = Model()
    m.reads(GRAPH, format="turtle")
    m.add_virtualization(
        virtualized_database(db, "table_a", "resource_a"),
        resources={"resource_a": t},
    )
    q = PREFIXES + "SELECT ?v WHERE { ?ts maplib:hasDataPoint ?dp . ?dp maplib:hasValue ?v }"
    with pytest.raises(Exception, match="predicate of each triple must be a constant IRI"):
        m.query(q)

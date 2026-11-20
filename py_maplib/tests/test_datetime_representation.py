"""xsd:dateTime values from DataFrames, casts and stored literals share one representation.

Naive and UTC datetimes used to be mixed, so comparisons and unions of them failed.
"""

import pathlib
from datetime import datetime, timezone

import polars as pl
import pytest

import maplib
from maplib import Model

from .disk import disk_params, storage_kwargs

PATH_HERE = pathlib.Path(__file__).parent

PREFIXES = """
PREFIX ex: <http://example.org/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX ottr: <http://ns.ottr.xyz/0.4/>
"""

TEMPLATE = (
    PREFIXES
    + """
ex:WorkOrder [ottr:IRI ?wo, xsd:dateTime ?t] :: {
    ottr:Triple(?wo, ex:time, ?t)
} .
"""
)

NAMED = "http://example.org/g"


def naive_model() -> Model:
    m = Model()
    m.add_template(TEMPLATE)
    df = pl.DataFrame(
        {
            "wo": ["http://example.org/wo1", "http://example.org/wo2"],
            "t": [datetime(2026, 11, 9, 12, 0), datetime(2026, 11, 14, 6, 12)],
        }
    )
    assert df["t"].dtype == pl.Datetime("us")
    m.map("ex:WorkOrder", df)
    return m


def test_filter_naive_mapped_datetime_against_literal():
    m = naive_model()
    df = m.query(
        PREFIXES
        + """
SELECT ?wo ?t WHERE {
    ?wo ex:time ?t .
    FILTER(?t >= "2026-11-10T00:00:00"^^xsd:dateTime)
}"""
    )
    assert df["wo"].to_list() == ["<http://example.org/wo2>"]
    assert df["t"].to_list() == [datetime(2026, 11, 14, 6, 12, tzinfo=timezone.utc)]


def test_naive_mapped_datetime_keeps_no_time_zone():
    m = naive_model()
    nt = m.writes(format="ntriples")
    assert '"2026-11-14T06:12:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>' in nt


def test_filter_naive_mapped_datetime_against_utc_literal():
    m = naive_model()
    df = m.query(
        PREFIXES
        + """
SELECT ?wo WHERE {
    ?wo ex:time ?t .
    FILTER(?t < "2026-11-10T00:00:00Z"^^xsd:dateTime)
}"""
    )
    assert df["wo"].to_list() == ["<http://example.org/wo1>"]


def test_non_utc_time_zone_column_is_stored_as_utc():
    m = Model()
    m.add_template(TEMPLATE)
    df = pl.DataFrame(
        {
            "wo": ["http://example.org/wo1"],
            "t": [datetime(2026, 11, 14, 7, 12)],
        }
    ).with_columns(pl.col("t").dt.replace_time_zone("Europe/Oslo"))
    m.map("ex:WorkOrder", df)
    # The instant is stored in UTC and keeps a time zone (Z).
    nt = m.writes(format="ntriples")
    assert '"2026-11-14T06:12:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>' in nt
    df = m.query(
        PREFIXES
        + """
SELECT ?t WHERE {
    ?wo ex:time ?t .
    FILTER(?t = "2026-11-14T06:12:00Z"^^xsd:dateTime)
}"""
    )
    assert df["t"].to_list() == [datetime(2026, 11, 14, 6, 12, tzinfo=timezone.utc)]


XSD = "http://www.w3.org/2001/XMLSchema#"


@pytest.mark.parametrize("time_zone", ["UTC", "+00:00", "Etc/UTC"])
def test_utc_aware_mapped_datetime_keeps_time_zone(time_zone):
    """Issue #91: UTC-aware columns keep their time zone as xsd:dateTime."""
    m = Model()
    m.add_template(TEMPLATE)
    df = pl.DataFrame(
        {"wo": ["http://example.org/wo1"], "t": [datetime(2026, 7, 7, 14, 3, 41)]}
    ).with_columns(pl.col("t").dt.replace_time_zone(time_zone))
    m.map("ex:WorkOrder", df)
    nt = m.writes(format="ntriples")
    assert f'"2026-07-07T14:03:41Z"^^<{XSD}dateTime>' in nt
    # Equal to a parsed literal with a time zone, as the same term
    df = m.query(
        PREFIXES
        + """
SELECT ?wo WHERE { ?wo ex:time "2026-07-07T14:03:41Z"^^xsd:dateTime }"""
    )
    assert df["wo"].to_list() == ["<http://example.org/wo1>"]


STAMP_TEMPLATE = (
    PREFIXES
    + """
ex:Stamp [ottr:IRI ?wo, xsd:dateTimeStamp ?t] :: {
    ottr:Triple(?wo, ex:time, ?t)
} .
"""
)


@pytest.mark.parametrize("time_zone", ["UTC", "Europe/Oslo"])
def test_mapped_datetime_stamp_has_time_zone(time_zone):
    """Issue #91: an xsd:dateTimeStamp always has a time zone (XSD 1.1 Part 2, 3.4.28)."""
    m = Model()
    m.add_template(STAMP_TEMPLATE)
    df = pl.DataFrame(
        {"wo": ["http://example.org/wo1"], "t": [datetime(2026, 7, 7, 14, 3, 41)]}
    ).with_columns(pl.col("t").dt.replace_time_zone(time_zone))
    m.map("ex:Stamp", df)
    nt = m.writes(format="ntriples")
    expected = "14:03:41" if time_zone == "UTC" else "12:03:41"
    assert f'"2026-07-07T{expected}Z"^^<{XSD}dateTimeStamp>' in nt


def test_naive_column_is_rejected_for_datetime_stamp():
    """A naive column has no time zone, so it can not be an xsd:dateTimeStamp."""
    m = Model()
    m.add_template(STAMP_TEMPLATE)
    df = pl.DataFrame(
        {"wo": ["http://example.org/wo1"], "t": [datetime(2026, 7, 7, 14, 3, 41)]}
    )
    with pytest.raises(Exception, match="incompatible"):
        m.map("ex:Stamp", df)


@pytest.mark.parametrize(
    "lexical,expected",
    [
        ("2026-11-14T06:12:00", datetime(2026, 11, 14, 6, 12)),
        ("2026-11-14T06:12:00Z", datetime(2026, 11, 14, 6, 12)),
        ("2026-11-14T08:12:00+02:00", datetime(2026, 11, 14, 6, 12)),
        ("2026-11-14T06:12:00.5Z", datetime(2026, 11, 14, 6, 12, 0, 500000)),
    ],
)
def test_cast_string_to_datetime(lexical, expected):
    # Values with a time zone become UTC instants, and the cast result has no time zone in
    # its type (one type per column), so it equals the literal without a time zone.
    m = Model()
    df = m.query(
        PREFIXES
        + f"""
SELECT ?o (STR(?o) AS ?s) WHERE {{
    BIND(xsd:dateTime("{lexical}") AS ?o)
    FILTER(?o = "{expected.isoformat()}"^^xsd:dateTime)
}}"""
    )
    assert df["o"].to_list() == [expected.replace(tzinfo=timezone.utc)]
    # The lexical form is the UTC wall clock time without a time zone.
    assert [datetime.fromisoformat(s) for s in df["s"].to_list()] == [expected]


def test_cast_invalid_string_to_datetime_is_unbound():
    m = Model()
    df = m.query(
        PREFIXES
        + """
SELECT ?o WHERE { BIND(xsd:dateTime("not a datetime") AS ?o) }"""
    )
    assert df["o"].to_list() == [None]


JSON_DOC = '{"workOrder": {"id": "wo1", "start": "2026-11-14T06:12:00"}}'


def json_model() -> Model:
    m = Model()
    # A dateTime with a time zone in a named graph, and a JSON string in the default graph.
    m.reads(
        '<http://example.org/x> <http://example.org/time> '
        '"2026-11-10T00:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .',
        format="ntriples",
        graph=NAMED,
    )
    m.map_json(JSON_DOC)
    return m


STRING_TO_DATETIME = """
    {
        ?x ex:time ?o .
    } UNION {
        ?s ?p "2026-11-14T06:12:00" .
        BIND(xsd:dateTime("2026-11-14T06:12:00") AS ?o)
    }
"""


def test_construct_union_of_stored_and_cast_datetimes():
    m = json_model()
    res = m.query(
        PREFIXES + "CONSTRUCT { ex:r ex:time ?o } WHERE {" + STRING_TO_DATETIME + "}",
        graph=NAMED,
        include_graphs=[maplib.DEFAULT_GRAPH],
    )
    # A dateTime with a time zone and one without have different types, so the column
    # holds several types and comes back as N-Triples terms.
    times = sorted(t for df in res for t in df["object"].to_list())
    assert times == [
        '"2026-11-10T00:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>',
        '"2026-11-14T06:12:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>',
    ]


def test_construct_cast_datetime_from_json():
    m = json_model()
    res = m.query(
        PREFIXES
        + """
CONSTRUCT { ?s ex:start ?o } WHERE {
    ?s ?p ?v .
    FILTER(?v = "2026-11-14T06:12:00")
    BIND(xsd:dateTime(?v) AS ?o)
}""",
        graph=NAMED,
        include_graphs=[maplib.DEFAULT_GRAPH],
    )
    assert [t for df in res for t in df["object"].to_list()] == [
        datetime(2026, 11, 14, 6, 12, tzinfo=timezone.utc)
    ]


def test_update_cast_datetime_into_named_graph_then_query():
    m = json_model()
    m.update(
        PREFIXES
        + f"""
INSERT {{ GRAPH <{NAMED}> {{ ex:y ex:time ?o }} }} WHERE {{
    ?s ?p ?v .
    FILTER(?v = "2026-11-14T06:12:00")
    BIND(xsd:dateTime(?v) AS ?o)
}}"""
    )
    df = m.query(
        PREFIXES
        + """
SELECT ?x ?o WHERE {
    ?x ex:time ?o .
    FILTER(?o > "2026-11-01T00:00:00Z"^^xsd:dateTime)
} ORDER BY ?o""",
        graph=NAMED,
    )
    assert df["x"].to_list() == ["<http://example.org/x>", "<http://example.org/y>"]
    assert df["o"].to_list() == [
        '"2026-11-10T00:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>',
        '"2026-11-14T06:12:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>',
    ]
    nt = m.writes(format="ntriples", graph=NAMED)
    assert '"2026-11-14T06:12:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>' in nt


def test_custom_micros_as_datetime_compares_with_literal():
    m = Model()
    df = m.query(
        PREFIXES
        + """
PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#>
SELECT ?o WHERE {
    BIND(1000000 AS ?m)
    BIND(maplib:MicrosAsDateTime(?m) AS ?o)
    FILTER(?o = "1970-01-01T00:00:01"^^xsd:dateTime)
}"""
    )
    assert df["o"].to_list() == [datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc)]


# A model serialized by maplib before this change (main c27bfb65), when naive datetime columns
# were stored as naive datetimes. It was written by:
#   m.add_template(TEMPLATE)
#   m.map("ex:WorkOrder", <wo1 2026-11-09T12:00, wo2 2026-11-14T06:12 as naive datetime[us]>)
#   m.reads('ex:p1 ex:parsed "2026-11-12T00:00:00"^^xsd:dateTime .
#            ex:p2 ex:parsedz "2026-11-12T00:00:00Z"^^xsd:dateTime .')
#   m.serialize("datetimes_serialized_before_utc.maplib")
SERIALIZED_BEFORE_UTC = PATH_HERE / "testdata" / "datetimes_serialized_before_utc.maplib"

SERIALIZED_BEFORE_UTC_NT = {
    '<http://example.org/wo1> <http://example.org/time> "2026-11-09T12:00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .',
    '<http://example.org/wo2> <http://example.org/time> "2026-11-14T06:12:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .',
    '<http://example.org/p1> <http://example.org/parsed> "2026-11-12T00:00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .',
    '<http://example.org/p2> <http://example.org/parsedz> "2026-11-12T00:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .',
}


def check_serialized_before_utc(m: Model):
    df = m.query(PREFIXES + "SELECT ?wo ?t WHERE { ?wo ex:time ?t } ORDER BY ?t")
    assert df["t"].to_list() == [
        datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc),
        datetime(2026, 11, 14, 6, 12, tzinfo=timezone.utc),
    ]
    df = m.query(
        PREFIXES
        + """
SELECT ?wo WHERE {
    ?wo ex:time ?t .
    FILTER(?t >= "2026-11-10T00:00:00"^^xsd:dateTime)
}"""
    )
    assert df["wo"].to_list() == ["<http://example.org/wo2>"]
    # Mapped and parsed dateTimes without a time zone union and compare.
    df = m.query(
        PREFIXES
        + """
SELECT ?s WHERE { { ?s ex:time ?t } UNION { ?s ex:parsed ?t } } ORDER BY ?t"""
    )
    assert df["s"].to_list() == [
        "<http://example.org/wo1>",
        "<http://example.org/p1>",
        "<http://example.org/wo2>",
    ]
    df = m.query(
        PREFIXES + "SELECT ?s WHERE { ?s ex:time ?t . ?x ex:parsed ?u FILTER(?t > ?u) }"
    )
    assert df["s"].to_list() == ["<http://example.org/wo2>"]
    assert set(m.writes(format="ntriples").splitlines()) == SERIALIZED_BEFORE_UTC_NT


@pytest.mark.parametrize("storage", disk_params())
def test_model_serialized_before_utc_reads_as_utc(storage, tmp_path):
    kwargs = storage_kwargs(storage)
    if "storage_folder" in kwargs:
        kwargs["storage_folder"] = str(tmp_path / "storage")
    m = Model.deserialize(str(SERIALIZED_BEFORE_UTC), **kwargs)
    check_serialized_before_utc(m)
    # Values added now are stored the same way as the loaded ones.
    m.map_json(JSON_DOC)
    df = m.query(
        PREFIXES
        + """
SELECT ?o WHERE {
    { ?x ex:time ?o } UNION {
        ?s ?p "2026-11-14T06:12:00" .
        BIND(xsd:dateTime("2026-11-14T06:12:00") AS ?o)
    }
} ORDER BY ?o"""
    )
    assert df["o"].to_list() == [
        datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc),
        datetime(2026, 11, 14, 6, 12, tzinfo=timezone.utc),
        datetime(2026, 11, 14, 6, 12, tzinfo=timezone.utc),
    ]


@pytest.mark.parametrize("single_file", [True, False])
def test_model_serialized_before_utc_round_trips(single_file, tmp_path):
    m = Model.deserialize(str(SERIALIZED_BEFORE_UTC))
    path = str(tmp_path / ("m.maplib" if single_file else "m"))
    m.serialize(path, single_file=single_file)
    check_serialized_before_utc(Model.deserialize(path))

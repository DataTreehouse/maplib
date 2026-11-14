"""Bad input should raise a normal exception, not a Rust panic.

pyo3's PanicException derives from BaseException, not Exception, so pytest.raises(Exception)
fails the test if the call panics.
"""

from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest
from maplib import Literal, MaplibException, Model, xsd


def test_to_native_invalid_duration():
    with pytest.raises(Exception, match="Cannot convert"):
        Literal("abc", xsd.duration).to_native()


def test_to_native_date():
    assert Literal("2020-01-02", xsd.date).to_native() == date(2020, 1, 2)


def test_to_native_datetime_fixed_offset():
    dt = Literal("2020-01-01T00:00:00+05:00", xsd.dateTime).to_native()
    assert dt == datetime(2020, 1, 1, tzinfo=timezone(timedelta(hours=5)))


# N-Triples and N-Quads have no relative IRIs, so they do not use the base IRI.
@pytest.mark.parametrize("fmt", ["turtle", "rdf/xml", "trig", "json-ld"])
def test_reads_invalid_base_iri(fmt):
    with pytest.raises(MaplibException, match="Invalid base iri"):
        Model().reads("<a> <b> <c> .", format=fmt, base_iri="not an iri")


def test_reads_invalid_base_iri_parallel_turtle():
    with pytest.raises(MaplibException, match="Invalid base iri"):
        Model().reads("<a> <b> <c> .", format="turtle", base_iri="not an iri", parallel=True)


def test_read_unknown_extension(tmp_path):
    p = tmp_path / "triples.unknown"
    p.write_text("<http://a> <http://b> <http://c> .")
    with pytest.raises(MaplibException, match="not supported"):
        Model().read(p)


def test_read_empty_file(tmp_path):
    p = tmp_path / "empty.ttl"
    p.write_text("")
    m = Model()
    m.read(p)
    assert m.size() == 0


def test_map_json_invalid():
    with pytest.raises(MaplibException):
        Model().map_json("{ not json")


def test_generate_template_missing_primary_key():
    with pytest.raises(MaplibException, match="missing"):
        Model().generate_template(pl.DataFrame({"a": ["x"], "b": [1]}), "nope")


@pytest.mark.parametrize("value", [[1], {"x": 1}])
def test_generate_template_nested_primary_key(value):
    Model().generate_template(pl.DataFrame({"a": [value], "b": [1]}), "a")


def test_map_blank_node_default_with_nulls():
    m = Model()
    m.add_template("@prefix ex:<http://e/>. ex:T [?a, ?b = _:x] :: { ottr:Triple(?a, ex:p, ?b) } .")
    with pytest.raises(MaplibException, match="blank nodes is not supported"):
        m.map("http://e/T", pl.DataFrame({"a": ["http://x", "http://y"], "b": ["http://z", None]}))


def test_add_graph_from_itself_does_not_deadlock():
    m = Model()
    m.reads("<http://a> <http://b> <http://c> .", format="ntriples")
    with pytest.raises(Exception, match="needs another Model"):
        m.add_graph(m)


@pytest.mark.parametrize(
    "start,length,expected",
    [
        ("2", "", "bc"),
        ("2", "1", "b"),
        ("-5", "-2", ""),
        ("0", "2", "a"),
        ("-1", "4", "ab"),
        ("+2", "", "bc"),
        ("1.5", "1.0", "b"),
    ],
)
def test_substr_literal_arguments(start, length, expected):
    args = f'"abc", {start}' + (f", {length}" if length else "")
    df = Model().query(f"SELECT (SUBSTR({args}) AS ?x) WHERE {{}}")
    assert df["x"][0] == expected


def test_substr_variable_start_is_an_error():
    with pytest.raises(MaplibException, match="Integer argument"):
        Model().query('SELECT (SUBSTR("abc", ?s) AS ?x) WHERE { BIND(2 AS ?s) }')


@pytest.mark.parametrize("agg", ["SUM", "AVG"])
def test_numeric_aggregate_over_iris_is_unbound(agg):
    m = Model()
    m.reads("<http://a> <http://b> <http://c> .", format="ntriples")
    df = m.query(f"SELECT ({agg}(?a) AS ?x) WHERE {{ ?a ?b ?c }}")
    assert df["x"].null_count() == 1


def test_construct_with_literal_predicate_is_skipped():
    m = Model()
    m.reads("<http://a> <http://b> 1 . <http://a> <http://d> <http://e> .", format="turtle")
    m.insert("CONSTRUCT { ?a ?c ?a } WHERE { ?a ?b ?c }")
    # Only <http://e> is an IRI and can be a predicate
    assert m.size() == 3


def test_service_is_an_error():
    with pytest.raises(MaplibException, match="SERVICE"):
        Model().query("SELECT * WHERE { SERVICE <http://localhost:1/sparql> { ?a ?b ?c } }")


TPL = "@prefix ex:<http://e/>. ex:T [?a, ?b] :: { ottr:Triple(?a, ex:p, ?b) } ."


def test_map_duration_column():
    m = Model()
    m.add_template(TPL)
    m.map("http://e/T", pl.DataFrame({"a": ["http://x"], "b": pl.Series([timedelta(seconds=90)])}))
    df = m.query("SELECT ?b WHERE { ?a <http://e/p> ?b }")
    assert df.height == 1
    assert "1M30S" in str(df["b"][0])


def test_map_constant_list_with_mixed_types():
    m = Model()
    m.add_template('@prefix ex:<http://e/>. ex:T [?a] :: { ottr:Triple(?a, ex:p, (1, "a")) } .')
    with pytest.raises(MaplibException, match="inconsistent data types"):
        m.map("http://e/T", pl.DataFrame({"a": ["http://x"]}))


def test_map_empty_constant_list():
    m = Model()
    m.add_template("@prefix ex:<http://e/>. ex:T [?a] :: { ottr:Triple(?a, ex:p, ()) } .")
    with pytest.raises(MaplibException, match="Empty constant list"):
        m.map("http://e/T", pl.DataFrame({"a": ["http://x"]}))


def test_map_list_expand_non_list():
    m = Model()
    m.add_template(
        "@prefix ex:<http://e/>. ex:T [?a, ?b] :: { zipMin | ottr:Triple(?a, ex:p, ++?b) } ."
    )
    with pytest.raises(MaplibException, match="non-list"):
        m.map("http://e/T", pl.DataFrame({"a": ["http://x"], "b": [1]}))


def _map_sm(df, types):
    from maplib import SolutionMappings

    Model().map_triples(SolutionMappings(df, types), predicate="http://p")


@pytest.mark.parametrize(
    "data,object_type",
    [(["x"], "multi"), (["x"], "lang"), (["x"], "datetime"), ([1], "custom")],
)
def test_solution_mappings_type_mismatch(data, object_type):
    from maplib import RDFType

    t = {
        "multi": lambda: RDFType.Multi([RDFType.IRI, RDFType.Literal(xsd.string)]),
        "lang": lambda: RDFType.Literal("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"),
        "datetime": lambda: RDFType.Literal(xsd.dateTime),
        "custom": lambda: RDFType.Literal("http://custom/dt"),
    }[object_type]()
    with pytest.raises(Exception, match="cannot hold values"):
        _map_sm(
            pl.DataFrame({"subject": ["http://a"], "object": data}),
            {"subject": RDFType.IRI, "object": t},
        )


def test_solution_mappings_blank_node_subject_int():
    from maplib import RDFType

    with pytest.raises(Exception, match="cannot hold values"):
        _map_sm(
            pl.DataFrame({"subject": [1], "object": ["x"]}),
            {"subject": RDFType.BlankNode, "object": RDFType.Literal(xsd.string)},
        )


def test_solution_mappings_nested_type():
    from maplib import RDFType, SolutionMappings

    with pytest.raises(Exception, match="Nested"):
        SolutionMappings(
            pl.DataFrame({"a": [["x"]]}), {"a": RDFType.Nested(RDFType.Literal(xsd.string))}
        )


def test_solution_mappings_matching_types_still_work():
    from maplib import RDFType

    _map_sm(
        pl.DataFrame({"subject": ["http://a"], "object": pl.Series([1], dtype=pl.Int32)}),
        {"subject": RDFType.IRI, "object": RDFType.Literal(xsd.integer)},
    )

import polars as pl
import pytest

from maplib import Mapping, RDFType, XSD
from polars.testing import assert_frame_equal
import polars as pl
import pathlib

pl.Config.set_fmt_str_lengths(200)
PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_filter_equals(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    {VALUES (?a) { (:hello2) }} UNION {VALUES (?a) { ("string") }}
    FILTER(?a = :hello2)
    }
    """,
        streaming=streaming,
    )
    assert_frame_equal(df, pl.DataFrame({"a": ["<http://example.net/hello2>"]}))


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_filter_numerical_equals(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?b WHERE {
    {VALUES (?a) { ("1"^^xsd:int) }}
    {VALUES (?b) { ("1"^^xsd:integer) }}
    FILTER(?a = ?b)
    }
    """,
        streaming=streaming,
    )
    assert df.shape == (0, 2)


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_filter_numerical_cast_real_equals(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?b WHERE {
    {VALUES (?a) { ("1"^^xsd:int) }}
    {VALUES (?b) { ("1"^^xsd:integer) }}
    FILTER(xsd:double(?a) = xsd:double(?b))
    }
    """,
        streaming=streaming,
    )
    assert df.shape == (1, 2)


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_filter_numerical_cast_integer_equals(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?b WHERE {
        {VALUES (?a) { ("1"^^xsd:int) }}
        {VALUES (?b) { ("1"^^xsd:integer) }}
    FILTER(xsd:integer(?a) = xsd:integer(?b))
    }
    """,
        streaming=streaming,
    )
    assert df.shape == (1, 2)


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_filter_numerical_cast_string_equals(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?b WHERE {
    VALUES (?a) { ("1"^^xsd:int)}
    VALUES (?b) { ("1"^^xsd:integer)} 
    FILTER(xsd:string(?a) = xsd:string(?b))
    }
    """,
        streaming=streaming,
    )
    assert df.shape == (1, 2)


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_strlen(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a (STRLEN(?a) AS ?len) WHERE {
        VALUES (?a) { ("123"^^xsd:int) ("Hello") ("") ("ABC"^^xsd:string)}
    } 
    """,
        streaming=streaming,
    )
    assert df.get_column("len").sum() == 11


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_filter_uri_cast_string_equals(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a  WHERE {
        { VALUES (?a) { ("1"^^xsd:int) }} UNION { VALUES (?a) { (:something) }}
        FILTER(xsd:string(?a) = "http://example.net/something")
    }
    """,
        streaming=streaming,
    )
    assert df.shape == (1, 1)


@pytest.mark.parametrize("streaming", [True, False])
def test_filter_uri_cast_string_equals(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a  WHERE {
        VALUES (?a) { (:something) }
        FILTER(xsd:string(?a) = "http://example.net/something")
    }
    """,
        streaming=streaming,
    )
    assert df.shape == (1, 1)


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_filter_equals_with_datatypes(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    {VALUES (?a) { (:hello2) }} UNION {VALUES (?a) { ("string") }}
    FILTER(?a = :hello2)
    }
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {"a": RDFType.IRI()}
    assert_frame_equal(
        sm.mappings, pl.DataFrame({"a": ["<http://example.net/hello2>"]})
    )


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_value_different_types_filter_equals_with_datatypes(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    {VALUES (?a) { (:hello2) ("string") (1) (true) }}
    FILTER(?a = true)
    }
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {"a": RDFType.Literal(XSD().boolean)}
    assert_frame_equal(sm.mappings, pl.DataFrame({"a": [True]}))


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_value_different_types_filter_isiri_or_equals_with_datatypes(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    {VALUES (?a) { (:hello2) ("string") (1) (true) }}
    FILTER(isIri(?a) || ?a = 1)
    }
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Multi([RDFType.IRI(), RDFType.Literal(XSD().integer)])
    }
    assert_frame_equal(
        sm.mappings,
        pl.DataFrame(
            {
                "a": [
                    "<http://example.net/hello2>",
                    '"1"^^<http://www.w3.org/2001/XMLSchema#integer>',
                ]
            }
        ),
    )


@pytest.mark.parametrize("streaming", [True, False])
def test_coalesce_both_multi_same_types(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?b (COALESCE(?b, ?a) AS ?c) WHERE {
    {VALUES (?a) { (:hello1) ("string1") (1) (true) }}
    OPTIONAL {
        VALUES (?b) { (:hello2) ("string2") (1) (false) }
        FILTER(isIri(?a))
    }
    }
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    print(sm.mappings)
    print(sm.rdf_types)
    assert sm.mappings.get_column("c").is_null().sum() == 0


@pytest.mark.parametrize("streaming", [True, False])
def test_coalesce_both_multi_different_types(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?b (COALESCE(?b, ?a) AS ?c) WHERE {
    {VALUES (?a) { (:hello1) ("string1") }}
    OPTIONAL {
        VALUES (?b) { (:hello2) ("string2"@en) (1) (false) }
        FILTER(isIri(?a))
    }
    }
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.mappings.get_column("c").is_null().sum() == 0


@pytest.mark.parametrize("streaming", [True, False])
def test_coalesce_both_multi_different_types_rhs_less(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?b (COALESCE(?b, ?a) AS ?c) WHERE {
    VALUES (?a) { (:hello1) ("string1") (1)}
    OPTIONAL {
        VALUES (?b) { (:hello2) ("string2") }
        FILTER(isIri(?a))
    }
    }
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.mappings.get_column("c").is_null().sum() == 0


@pytest.mark.parametrize("streaming", [True, False])
def test_coalesce_only_lhs_multi(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?b (COALESCE(?b, ?a) AS ?c) WHERE {
    VALUES (?a) { (:hello1) ("string1") (1)}
    OPTIONAL {
        VALUES (?b) { (:hello2) (:hello3) }
        FILTER(isIri(?a))
    }
    }
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.mappings.get_column("c").is_null().sum() == 0


@pytest.mark.parametrize("streaming", [True, False])
def test_coalesce_only_rhs_multi(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?b (COALESCE(?b, ?a) AS ?c) WHERE {
    VALUES (?a) { (:hello1) (:hello3) (:hello4)}
    OPTIONAL {
        VALUES (?b) { (:hello2) ("string2"@no) ("string3") }
        FILTER(?a = :hello1)
    }
    }
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.mappings.get_column("c").is_null().sum() == 0


@pytest.mark.parametrize("streaming", [True, False])
def test_div_by_unbound_is_unbound(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?b (?a / ?b AS ?c) WHERE {
    VALUES (?a) { (1) (2) }
    OPTIONAL {
        VALUES (?b) { (1.5) (4.5) }
        FILTER(?a = 1)
    }
    }
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.mappings.height == 3
    assert sm.mappings.get_column("c").is_null().sum() == 1


@pytest.mark.parametrize("streaming", [True, False])
def test_coalesce_no_multi(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?b (COALESCE(?b, ?a) AS ?c) WHERE {
    VALUES (?a) { (:hello1) (:hello2) (:hello3)}
    OPTIONAL {
        VALUES (?b) { (:hello5) (:hello6) }
        FILTER(?a = :hello1)
    }
    }
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {"c": RDFType.IRI(), "b": RDFType.IRI(), "a": RDFType.IRI()}
    assert sm.mappings.get_column("c").is_null().sum() == 0


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_filter_equals_mirrored(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    {VALUES (?a) { (:hello2) }} UNION {VALUES (?a) { ("string") }}
    FILTER(:hello2 = ?a)
    }
    """,
        streaming=streaming,
    )
    assert_frame_equal(df, pl.DataFrame({"a": ["<http://example.net/hello2>"]}))


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_filter_equals_two_multi(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    {VALUES (?a) { (:hello2) }} UNION {VALUES (?a) { ("string") }}
    {VALUES (?b) { (:hello2) }} UNION {VALUES (?b) { (1) }}
    FILTER(?b = ?a)
    }
    """,
        streaming=streaming,
    )
    assert_frame_equal(df, pl.DataFrame({"a": ["<http://example.net/hello2>"]}))


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_filter_is_in(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    {VALUES (?a) { (:hello2) }} UNION {VALUES (?a) { ("string") }}
        FILTER(?a in (:hello2, "abc"))
    }
    """,
        streaming=streaming,
    )
    print(df)
    assert_frame_equal(df, pl.DataFrame({"a": ["<http://example.net/hello2>"]}))


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_filter_comparison(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    VALUES (?a) { (1) (3.0) (89) }
    FILTER(?a > 1.4)
    } ORDER BY ?a
    """,
        streaming=streaming,
    )
    expected = pl.from_repr(
        """
┌───────────────────────────────────────────────────┐
│ a                                                 │
│ ---                                               │
│ str                                               │
╞═══════════════════════════════════════════════════╡
│ "3.0"^^<http://www.w3.org/2001/XMLSchema#decimal> │
│ "89"^^<http://www.w3.org/2001/XMLSchema#integer>  │
└───────────────────────────────────────────────────┘
    """
    )
    assert_frame_equal(df, expected)


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_filter_incompatible_comparison(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    VALUES (?a) { (1) (3.0) (89) ("String") ("2021-01-01T08:00:00"^^xsd:dateTime) }
    FILTER(?a > 1.4)
    } ORDER BY ?a
    """,
        streaming=streaming,
    )
    expected = pl.from_repr(
        """
┌───────────────────────────────────────────────────┐
│ a                                                 │
│ ---                                               │
│ str                                               │
╞═══════════════════════════════════════════════════╡
│ "3.0"^^<http://www.w3.org/2001/XMLSchema#decimal> │
│ "89"^^<http://www.w3.org/2001/XMLSchema#integer>  │
└───────────────────────────────────────────────────┘
    """
    )
    assert_frame_equal(df, expected)


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_filter_incompatible_datetime_comparison(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    VALUES (?a) { (1) (3.0) (89) ("String") ("2021-01-01T08:00:00"^^xsd:dateTime) }
    FILTER(?a > "2021-01-01T07:59:59"^^xsd:dateTime)
    } ORDER BY ?a
    """,
        streaming=streaming,
    )
    expected = pl.from_repr(
        """
┌─────────────────────┐
│ a                   │
│ ---                 │
│ datetime[ns]        │
╞═════════════════════╡
│ 2021-01-01 08:00:00 │
└─────────────────────┘
    """
    )
    assert_frame_equal(df, expected)


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_filter_incompatible_many_comparison(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?b WHERE {
    VALUES (?a) { (1) (3.0) (89) ("String") ("2021-01-01T08:00:00"^^xsd:dateTime) }
    VALUES (?b) { (2) (2.0) (90) ("AString") ("2021-01-01T08:00:01"^^xsd:dateTime) }
    FILTER(?a < ?b)
    } ORDER BY ?a ?b
    """,
        streaming=streaming,
    )
    f = TESTDATA_PATH / "multi_many_comp.csv"
    # df.write_csv(f)
    expected = pl.read_csv(f)
    assert_frame_equal(df, expected)


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_concat(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?b ?c WHERE {
    VALUES (?a) { (1) (3.0) (89) ("String") ("2021-01-01T08:00:00"^^xsd:dateTime) }
    VALUES (?b) { (2) (2.0) (90) ("AString") ("2021-01-01T08:00:01"^^xsd:dateTime) }
    BIND(CONCAT(STR(?a),"_", STR(?b)) AS ?c)
    } ORDER BY ?a ?b
    """,
        streaming=streaming,
    )
    f = TESTDATA_PATH / "multi_concat.csv"
    # df.write_csv(f)
    expected = pl.read_csv(f)
    assert_frame_equal(df, expected)


@pytest.mark.parametrize("streaming", [True, False])
def test_multi_filter_incompatible_datetimestamp_comparison(streaming):
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    VALUES (?a) { (1) (3.0) (89) ("String") ("2021-01-01T08:00:00+00:00"^^xsd:dateTime) }
    FILTER(?a > "2021-01-01T07:59:59+00:00"^^xsd:dateTimeStamp)
    } ORDER BY ?a
    """,
        streaming=streaming,
    )
    expected = pl.from_repr(
        """
┌─────────────────────┐
│ a                   │
│ ---                 │
│ datetime[ns, UTC]   │
╞═════════════════════╡
│ 2021-01-01 08:00:00 │
└─────────────────────┘
    """
    )
    assert_frame_equal(df, expected)


@pytest.mark.parametrize("streaming", [True, False])
def test_generate_uuids(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?uuid WHERE {
    VALUES (?a) { (1) (2) (3) }
    BIND(uuid() as ?uuid)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Literal("http://www.w3.org/2001/XMLSchema#integer"),
        "uuid": RDFType.IRI(),
    }
    assert sm.mappings.height == 3
    df = sm.mappings.with_columns(
        pl.col("uuid").str.starts_with("<urn:uuid:").alias("startswithcorrect")
    )
    assert df.get_column("startswithcorrect").sum() == 3
    assert df.get_column("uuid").unique().len() == 3


@pytest.mark.parametrize("streaming", [True, False])
def test_generate_iri_all_strings(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?iri WHERE {
    VALUES (?a) { ("urn:abc:123") ("http://www.w3.org/2001/XMLSchema#integer") }
    BIND(IRI(?a) as ?iri)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
        "iri": RDFType.IRI(),
    }
    expected = pl.from_repr(
        """
┌─────────────────────────────────────────────────┬────────────────────────────────────────────┐
│ a                                               ┆ iri                                        │
│ ---                                             ┆ ---                                        │
│ str                                             ┆ str                                        │
╞═════════════════════════════════════════════════╪════════════════════════════════════════════╡
│ http://www.w3.org/2001/XMLSchema#integer        ┆ <http://www.w3.org/2001/XMLSchema#integer> │
│ urn:abc:123                                     ┆ <urn:abc:123>                              │
└─────────────────────────────────────────────────┴────────────────────────────────────────────┘
    """
    )
    assert_frame_equal(expected, sm.mappings)


@pytest.mark.parametrize("streaming", [True, False])
def test_generate_iri(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?iri WHERE {
    VALUES (?a) { ("urn:abc:123") ("http://www.w3.org/2001/XMLSchema#integer") (3) (<http://www.w3.org/2001/XMLSchema#abc>) }
    BIND(IRI(?a) as ?iri)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Multi(
            [
                RDFType.IRI(),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#integer"),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
            ]
        ),
        "iri": RDFType.IRI(),
    }
    expected = pl.from_repr(
        """
┌─────────────────────────────────────────────────┬────────────────────────────────────────────┐
│ a                                               ┆ iri                                        │
│ ---                                             ┆ ---                                        │
│ str                                             ┆ str                                        │
╞═════════════════════════════════════════════════╪════════════════════════════════════════════╡
│ <http://www.w3.org/2001/XMLSchema#abc>          ┆ <http://www.w3.org/2001/XMLSchema#abc>     │
│ "3"^^<http://www.w3.org/2001/XMLSchema#integer> ┆ null                                       │
│ "http://www.w3.org/2001/XMLSchema#integer"      ┆ <http://www.w3.org/2001/XMLSchema#integer> │
│ "urn:abc:123"                                   ┆ <urn:abc:123>                              │
└─────────────────────────────────────────────────┴────────────────────────────────────────────┘
    """
    )
    assert_frame_equal(expected, sm.mappings)


@pytest.mark.parametrize("streaming", [True, False])
def test_generate_str_uuids(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?struuid WHERE {
    VALUES (?a) { (1) (2) (3) }
    BIND(struuid() as ?struuid)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Literal("http://www.w3.org/2001/XMLSchema#integer"),
        "struuid": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
    }
    assert sm.mappings.height == 3
    assert sm.mappings.get_column("struuid").unique().len() == 3


@pytest.mark.parametrize("streaming", [True, False])
def test_replace_single(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?replace WHERE {
    VALUES (?a) { ("abcabc") ("ab") ("bb") }
    BIND(REPLACE(?a, "ab", "ba") as ?replace)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
        "replace": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
    }
    assert sm.mappings.height == 3
    assert sm.mappings.get_column("replace").to_list() == ["ba", "bacbac", "bb"]


@pytest.mark.parametrize("streaming", [True, False])
def test_case_single(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?ucase ?lcase WHERE {
    VALUES (?a) { ("abcaAc") ("ab") ("bb") }
    BIND(UCASE(?a) as ?ucase)
    BIND(LCASE(?a) as ?lcase)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
        "ucase": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
        "lcase": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
    }
    assert sm.mappings.height == 3
    assert sm.mappings.get_column("ucase").to_list() == ["AB", "ABCAAC", "BB"]
    assert sm.mappings.get_column("lcase").to_list() == ["ab", "abcaac", "bb"]


@pytest.mark.parametrize("streaming", [True, False])
def test_mul_types_single(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT * WHERE {
    VALUES (?a) { ("dummy") }
    BIND("0.1"^^xsd:float * "0.1"^^xsd:decimal AS ?n1)
    BIND("0.1"^^xsd:double * "0.1"^^xsd:decimal AS ?n2)
    BIND("0.1"^^xsd:decimal * "0.1"^^xsd:decimal AS ?n3)
    BIND("0.1"^^xsd:float * "0.1"^^xsd:float AS ?n4)
    BIND("0.1"^^xsd:double * "0.1"^^xsd:double AS ?n5)
    BIND("21"^^xsd:integer * "0.1"^^xsd:decimal AS ?n6)
    #BIND("true"^^xsd:boolean * "0.1"^^xsd:decimal AS ?n7)
    BIND("1"^^xsd:integer * "2"^^xsd:long AS ?n8)
    BIND("2"^^xsd:long * "2"^^xsd:long AS ?n9)
    BIND("2"^^xsd:int * "2"^^xsd:int AS ?n10)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
        "n1": RDFType.Literal("http://www.w3.org/2001/XMLSchema#decimal"),
        "n2": RDFType.Literal("http://www.w3.org/2001/XMLSchema#decimal"),
        "n3": RDFType.Literal("http://www.w3.org/2001/XMLSchema#decimal"),
        "n4": RDFType.Literal("http://www.w3.org/2001/XMLSchema#float"),
        "n5": RDFType.Literal("http://www.w3.org/2001/XMLSchema#double"),
        "n6": RDFType.Literal("http://www.w3.org/2001/XMLSchema#decimal"),
        "n8": RDFType.Literal("http://www.w3.org/2001/XMLSchema#integer"),
        "n9": RDFType.Literal("http://www.w3.org/2001/XMLSchema#integer"),
        "n10": RDFType.Literal("http://www.w3.org/2001/XMLSchema#int"),
    }


@pytest.mark.parametrize("streaming", [True, False])
def test_substr_single(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?sub1 ?sub2 WHERE {
    VALUES (?a) { ("abcaAc") ("ab") ("bb") }
    BIND(SUBSTR(?a,3) as ?sub1)
    BIND(SUBSTR(?a,0,3) as ?sub2)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
        "sub1": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
        "sub2": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
    }
    assert sm.mappings.height == 3
    assert sm.mappings.get_column("sub1").to_list() == ["", "aAc", ""]
    assert sm.mappings.get_column("sub2").to_list() == ["ab", "abc", "bb"]


@pytest.mark.parametrize("streaming", [True, False])
def test_case_single_lang(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?ucase ?lcase WHERE {
    VALUES (?a) { ("abcaAc"@en) ("ab"@no) ("bb"@se) }
    BIND(UCASE(?a) as ?ucase)
    BIND(LCASE(?a) as ?lcase)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Literal("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"),
        "ucase": RDFType.Literal(
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
        ),
        "lcase": RDFType.Literal(
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
        ),
    }
    assert sm.mappings.height == 3
    assert sm.mappings.get_column("ucase").to_list() == [
        '"AB"@no',
        '"ABCAAC"@en',
        '"BB"@se',
    ]
    assert sm.mappings.get_column("lcase").to_list() == [
        '"ab"@no',
        '"abcaac"@en',
        '"bb"@se',
    ]


@pytest.mark.parametrize("streaming", [True, False])
def test_substr_multi_type_with_only_lang(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?sub1 ?sub2 WHERE {
    VALUES (?a) { ("abcaAc"@en) ("ab"@no) ("bb"@se) (1) }
    BIND(SUBSTR(?a, 2) as ?sub1)
    BIND(SUBSTR(?a, 2, 1) as ?sub2)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Multi(
            [
                RDFType.Literal(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
                ),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#integer"),
            ]
        ),
        "sub1": RDFType.Literal(
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
        ),
        "sub2": RDFType.Literal(
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
        ),
    }
    assert sm.mappings.height == 4
    assert sm.mappings.get_column("sub1").to_list() == [
        '""@no',
        '"caAc"@en',
        '""@se',
        None,
    ]
    assert sm.mappings.get_column("sub2").to_list() == [
        '""@no',
        '"c"@en',
        '""@se',
        None,
    ]


@pytest.mark.parametrize("streaming", [True, False])
def test_case_multi_type_with_only_lang(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?ucase ?lcase WHERE {
    VALUES (?a) { ("abcaAc"@en) ("ab"@no) ("bb"@se) (1) }
    BIND(UCASE(?a) as ?ucase)
    BIND(LCASE(?a) as ?lcase)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Multi(
            [
                RDFType.Literal(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
                ),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#integer"),
            ]
        ),
        "ucase": RDFType.Literal(
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
        ),
        "lcase": RDFType.Literal(
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
        ),
    }
    assert sm.mappings.height == 4
    assert sm.mappings.get_column("ucase").to_list() == [
        '"AB"@no',
        '"ABCAAC"@en',
        '"BB"@se',
        None,
    ]
    assert sm.mappings.get_column("lcase").to_list() == [
        '"ab"@no',
        '"abcaac"@en',
        '"bb"@se',
        None,
    ]


@pytest.mark.parametrize("streaming", [True, False])
def test_case_multi_type_with_string_and_lang_string_and_other(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?ucase ?lcase WHERE {
    VALUES (?a) { ("abcaAc"@en) ("ab") ("bb"@se) (1) (xsd:abc) }
    BIND(UCASE(?a) as ?ucase)
    BIND(LCASE(?a) as ?lcase)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Multi(
            [
                RDFType.IRI(),
                RDFType.Literal(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
                ),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#integer"),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
            ]
        ),
        "ucase": RDFType.Multi(
            [
                RDFType.Literal(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
                ),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
            ]
        ),
        "lcase": RDFType.Multi(
            [
                RDFType.Literal(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
                ),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
            ]
        ),
    }
    assert sm.mappings.height == 5
    assert sm.mappings.get_column("ucase").to_list() == [
        None,
        '"ABCAAC"@en',
        '"BB"@se',
        None,
        '"AB"',
    ]
    assert sm.mappings.get_column("lcase").to_list() == [
        None,
        '"abcaac"@en',
        '"bb"@se',
        None,
        '"ab"',
    ]


@pytest.mark.parametrize("streaming", [True, False])
def test_before_after_multi_type_with_string_and_lang_string_and_other(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?bef ?aft WHERE {
    VALUES (?a) { ("abcaAc"@en) ("abAcACAc") ("bb"@se) (1) (xsd:abc) }
    BIND(STRBEFORE(?a, "Ac") as ?bef)
    BIND(STRAFTER(?a, "ab") as ?aft)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Multi(
            [
                RDFType.IRI(),
                RDFType.Literal(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
                ),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#integer"),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
            ]
        ),
        "bef": RDFType.Multi(
            [
                RDFType.Literal(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
                ),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
            ]
        ),
        "aft": RDFType.Multi(
            [
                RDFType.Literal(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
                ),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
            ]
        ),
    }
    assert sm.mappings.height == 5
    assert sm.mappings.get_column("bef").to_list() == [
        None,
        '"abca"@en',
        '"bb"@se',
        None,
        '"abAcAC"',
    ]
    assert sm.mappings.get_column("aft").to_list() == [
        None,
        '"caAc"@en',
        '"bb"@se',
        None,
        '"AcACAc"',
    ]


@pytest.mark.parametrize("streaming", [True, False])
def test_before_after_only_lang_string_and_other(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?bef ?aft WHERE {
    VALUES (?a) { ("abcaAc"@en) ("bb"@se) (1) (xsd:abc) }
    BIND(STRBEFORE(?a, "Ac") as ?bef)
    BIND(STRAFTER(?a, "ab") as ?aft)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Multi(
            [
                RDFType.IRI(),
                RDFType.Literal(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
                ),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#integer"),
            ]
        ),
        "bef": RDFType.Literal("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"),
        "aft": RDFType.Literal("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"),
    }
    assert sm.mappings.height == 4
    assert sm.mappings.get_column("bef").to_list() == [
        None,
        '"abca"@en',
        '"bb"@se',
        None,
    ]
    assert sm.mappings.get_column("aft").to_list() == [
        None,
        '"caAc"@en',
        '"bb"@se',
        None,
    ]


@pytest.mark.parametrize("streaming", [True, False])
def test_before_after_only_lang_string_and_other(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?bef ?aft WHERE {
    VALUES (?a) { ("abcaAc"@en) ("bb"@se) }
    BIND(STRBEFORE(?a, "Ac") as ?bef)
    BIND(STRAFTER(?a, "ab") as ?aft)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    print(sm.mappings)
    assert sm.rdf_types == {
        "a": RDFType.Literal("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"),
        "bef": RDFType.Literal("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"),
        "aft": RDFType.Literal("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"),
    }
    assert sm.mappings.height == 2
    assert sm.mappings.get_column("bef").to_list() == [
        '"abca"@en',
        '"bb"@se',
    ]
    assert sm.mappings.get_column("aft").to_list() == [
        '"caAc"@en',
        '"bb"@se',
    ]


@pytest.mark.parametrize("streaming", [True, False])
def test_before_after_only_string_and_other(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?bef ?aft WHERE {
    VALUES (?a) { ("abcaAc") ("bb") (1) (xsd:abc) }
    BIND(STRBEFORE(?a, "Ac") as ?bef)
    BIND(STRAFTER(?a, "ab") as ?aft)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Multi(
            [
                RDFType.IRI(),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#integer"),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
            ]
        ),
        "bef": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
        "aft": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
    }
    assert sm.mappings.height == 4
    assert sm.mappings.get_column("bef").to_list() == [
        None,
        None,
        "abca",
        "bb",
    ]
    assert sm.mappings.get_column("aft").to_list() == [
        None,
        None,
        "caAc",
        "bb",
    ]


@pytest.mark.parametrize("streaming", [True, False])
def test_substr_multi_type_with_string_and_lang_string_and_other(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?sub1 ?sub2 WHERE {
    VALUES (?a) { ("abcaAc"@en) ("ab") ("bb"@se) (1) (xsd:abc) }
    BIND(SUBSTR(?a, 3,1) as ?sub1)
    BIND(SUBSTR(?a, 1) as ?sub2)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Multi(
            [
                RDFType.IRI(),
                RDFType.Literal(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
                ),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#integer"),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
            ]
        ),
        "sub1": RDFType.Multi(
            [
                RDFType.Literal(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
                ),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
            ]
        ),
        "sub2": RDFType.Multi(
            [
                RDFType.Literal(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
                ),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
            ]
        ),
    }
    assert sm.mappings.height == 5
    assert sm.mappings.get_column("sub1").to_list() == [
        None,
        '"a"@en',
        '""@se',
        None,
        '""',
    ]
    assert sm.mappings.get_column("sub2").to_list() == [
        None,
        '"bcaAc"@en',
        '"b"@se',
        None,
        '"b"',
    ]


@pytest.mark.parametrize("streaming", [True, False])
def test_replace_multi(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?replace WHERE {
    VALUES (?a) { ("abcabc") ("ab") (3) }
    BIND(REPLACE(?a, "ab", "ba") as ?replace)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Multi(
            [
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#integer"),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
            ]
        ),
        "replace": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
    }
    assert sm.mappings.height == 3
    assert sm.mappings.get_column("replace").to_list() == [None, "ba", "bacbac"]


@pytest.mark.parametrize("streaming", [True, False])
def test_regex_multi(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?replace WHERE {
    VALUES (?a) { ("abcabc") ("ab") (3) }
    BIND(REGEX(?a, "ab") as ?replace)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Multi(
            [
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#integer"),
                RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
            ]
        ),
        "replace": RDFType.Literal("http://www.w3.org/2001/XMLSchema#boolean"),
    }
    assert sm.mappings.height == 3
    assert sm.mappings.get_column("replace").to_list() == [None, True, True]


@pytest.mark.parametrize("streaming", [True, False])
def test_regex(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a ?replace WHERE {
    VALUES (?a) { ("abcabc") ("ab") ("bb") }
    BIND(REGEX(?a, "ab") as ?replace)
    } ORDER BY ?a
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    assert sm.rdf_types == {
        "a": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
        "replace": RDFType.Literal("http://www.w3.org/2001/XMLSchema#boolean"),
    }
    assert sm.mappings.height == 3
    assert sm.mappings.get_column("replace").to_list() == [True, True, False]


@pytest.mark.parametrize("streaming", [True, False])
def test_issue_22(streaming):
    m = Mapping([])
    sm = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a (COUNT(*) as ?c) WHERE {
    VALUES (?a) { ("abbb") ("abbb") ("ab") ("a") ("a") }
    BIND(REPLACE(?a, "b", "") as ?replace)
    } GROUP BY ?a 
    ORDER BY ?a 
    """,
        include_datatypes=True,
        streaming=streaming,
    )
    print(sm.mappings)
    assert sm.rdf_types == {
        "a": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
        "c": RDFType.Literal("http://www.w3.org/2001/XMLSchema#unsignedInt"),
    }
    assert sm.mappings.height == 3
    assert sm.mappings.get_column("c").to_list() == [2, 1, 2]

import polars as pl
from maplib import Mapping, RDFType, XSD
from polars.testing import assert_frame_equal
import polars as pl
import pathlib

pl.Config.set_fmt_str_lengths(200)
PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"


def test_multi_filter_equals():
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    {VALUES (?a) { (:hello2) }} UNION {VALUES (?a) { ("string") }}
    FILTER(?a = :hello2)
    }
    """
    )
    assert_frame_equal(df, pl.DataFrame({"a": ["<http://example.net/hello2>"]}))


def test_multi_filter_numerical_equals():
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
    """
    )
    assert df.shape == (0, 2)


def test_multi_filter_numerical_cast_real_equals():
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
    """
    )
    assert df.shape == (1, 2)


def test_multi_filter_numerical_cast_integer_equals():
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
    """
    )
    assert df.shape == (1, 2)


def test_multi_filter_numerical_cast_string_equals():
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
    """
    )
    assert df.shape == (1, 2)


def test_multi_strlen():
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a (STRLEN(?a) AS ?len) WHERE {
        VALUES (?a) { ("123"^^xsd:int) ("Hello") ("") ("ABC"^^xsd:string)}
    } 
    """
    )
    assert df.get_column("len").sum() == 11


def test_multi_filter_uri_cast_string_equals():
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a  WHERE {
        { VALUES (?a) { ("1"^^xsd:int) }} UNION { VALUES (?a) { (:something) }}
        FILTER(xsd:string(?a) = "http://example.net/something")
    }
    """
    )
    assert df.shape == (1, 1)


def test_filter_uri_cast_string_equals():
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a  WHERE {
        VALUES (?a) { (:something) }
        FILTER(xsd:string(?a) = "http://example.net/something")
    }
    """
    )
    assert df.shape == (1, 1)


def test_multi_filter_equals_with_datatypes():
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
    )
    assert sm.rdf_types == {"a": RDFType.IRI()}
    assert_frame_equal(
        sm.mappings, pl.DataFrame({"a": ["<http://example.net/hello2>"]})
    )


def test_multi_value_different_types_filter_equals_with_datatypes():
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
    )
    assert sm.rdf_types == {"a": RDFType.Literal(XSD().boolean)}
    assert_frame_equal(sm.mappings, pl.DataFrame({"a": [True]}))


def test_multi_value_different_types_filter_isiri_or_equals_with_datatypes():
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


def test_coalesce_both_multi_same_types():
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
    )
    assert sm.mappings.get_column("c").is_null().sum() == 0


def test_coalesce_both_multi_different_types():
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
    )
    assert sm.mappings.get_column("c").is_null().sum() == 0


def test_coalesce_both_multi_different_types_rhs_less():
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
    )
    assert sm.mappings.get_column("c").is_null().sum() == 0


def test_coalesce_only_lhs_multi():
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
    )
    assert sm.mappings.get_column("c").is_null().sum() == 0


def test_coalesce_only_rhs_multi():
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
    )
    assert sm.mappings.get_column("c").is_null().sum() == 0


def test_div_by_unbound_is_unbound():
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
    )
    assert sm.mappings.height == 3
    assert sm.mappings.get_column("c").is_null().sum() == 1



def test_coalesce_no_multi():
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
    )
    assert sm.rdf_types == {"c": RDFType.IRI(), "b": RDFType.IRI(), "a": RDFType.IRI()}
    assert sm.mappings.get_column("c").is_null().sum() == 0


def test_multi_filter_equals_mirrored():
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    {VALUES (?a) { (:hello2) }} UNION {VALUES (?a) { ("string") }}
    FILTER(:hello2 = ?a)
    }
    """
    )
    assert_frame_equal(df, pl.DataFrame({"a": ["<http://example.net/hello2>"]}))


def test_multi_filter_equals_two_multi():
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
    """
    )
    assert_frame_equal(df, pl.DataFrame({"a": ["<http://example.net/hello2>"]}))


def test_multi_filter_is_in():
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    {VALUES (?a) { (:hello2) }} UNION {VALUES (?a) { ("string") }}
    FILTER(?a in (:hello2, "a"))
    }
    """
    )
    assert_frame_equal(df, pl.DataFrame({"a": ["<http://example.net/hello2>"]}))


def test_multi_filter_comparison():
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    VALUES (?a) { (1) (3.0) (89) }
    FILTER(?a > 1.4)
    } ORDER BY ?a
    """
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


def test_multi_filter_incompatible_comparison():
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    VALUES (?a) { (1) (3.0) (89) ("String") ("2021-01-01T08:00:00"^^xsd:dateTime) }
    FILTER(?a > 1.4)
    } ORDER BY ?a
    """
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


def test_multi_filter_incompatible_datetime_comparison():
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    VALUES (?a) { (1) (3.0) (89) ("String") ("2021-01-01T08:00:00"^^xsd:dateTime) }
    FILTER(?a > "2021-01-01T07:59:59"^^xsd:dateTime)
    } ORDER BY ?a
    """
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


def test_multi_filter_incompatible_many_comparison():
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
    """
    )
    f = TESTDATA_PATH / "multi_many_comp.csv"
    # df.write_csv(f)
    expected = pl.read_csv(f)
    assert_frame_equal(df, expected)


def test_multi_concat():
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
    """
    )
    f = TESTDATA_PATH / "multi_concat.csv"
    # df.write_csv(f)
    expected = pl.read_csv(f)
    assert_frame_equal(df, expected)


def test_multi_filter_incompatible_datetime_comparison():
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    VALUES (?a) { (1) (3.0) (89) ("String") ("2021-01-01T08:00:00+00:00"^^xsd:dateTime) }
    FILTER(?a > "2021-01-01T07:59:59+00:00"^^xsd:dateTimeStamp)
    } ORDER BY ?a
    """
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


def test_generate_uuids():
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


def test_generate_str_uuids():
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
    )
    assert sm.rdf_types == {
        "a": RDFType.Literal("http://www.w3.org/2001/XMLSchema#integer"),
        "struuid": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
    }
    assert sm.mappings.height == 3
    assert sm.mappings.get_column("struuid").unique().len() == 3

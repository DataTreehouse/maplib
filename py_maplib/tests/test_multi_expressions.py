import polars as pl
from maplib import Mapping, RDFType, XSD
from polars.testing import assert_frame_equal
import polars as pl
pl.Config.set_fmt_str_lengths(200)

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
    assert df.shape == (0,2)

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
    assert df.shape == (1,2)

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
    assert df.shape == (1,2)

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
    assert df.shape == (1,2)

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
    assert df.shape == (1,1)

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
    assert df.shape == (1,1)

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
    assert_frame_equal(
        sm.mappings, pl.DataFrame({"a": [True]})
    )

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
    assert sm.rdf_types == {"a": RDFType.Multi([RDFType.IRI(), RDFType.Literal(XSD().integer)])}
    assert_frame_equal(
        sm.mappings, pl.DataFrame({"a": ["<http://example.net/hello2>", '"1"^^<http://www.w3.org/2001/XMLSchema#integer>']})
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
    assert sm.rdf_types == {'c': RDFType.IRI(), 'b': RDFType.IRI(), 'a': RDFType.IRI()}
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
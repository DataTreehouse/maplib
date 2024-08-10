import polars as pl
from maplib import Mapping, RDFType
from polars.testing import assert_frame_equal


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

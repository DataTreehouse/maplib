"""Regression tests for inputs that used to reach todo!() panics."""

import polars as pl
import pytest

from maplib import Model

from .disk import is_maplib_ee
from .sparql_mock_endpoint import MockEndpoints

EX = "http://example.net/ns#"


def _people() -> Model:
    m = Model()
    m.reads(
        f"""
        <{EX}a> <{EX}name> "Alice"@en-GB .
        <{EX}b> <{EX}name> "Bob"@fr .
        <{EX}c> <{EX}name> "Carol" .
        <{EX}a> <{EX}lang> "en" .
        <{EX}b> <{EX}lang> "en" .
        <{EX}c> <{EX}lang> "*" .
        """,
        format="ntriples",
    )
    return m


def test_lang_matches_with_variable_range():
    m = _people()
    df = m.query(
        f"""
        SELECT ?s ?m WHERE {{
            ?s <{EX}name> ?n .
            ?s <{EX}lang> ?r .
            BIND(langMatches(lang(?n), ?r) AS ?m)
        }} ORDER BY ?s
        """
    )
    # en-GB matches en, fr does not, and "*" does not match the empty tag of "Carol".
    assert df["m"].to_list() == [True, False, False]


def test_lang_matches_star_does_not_match_empty_tag():
    m = _people()
    df = m.query(
        f"""
        SELECT ?s WHERE {{
            ?s <{EX}name> ?n .
            FILTER(langMatches(lang(?n), "*"))
        }} ORDER BY ?s
        """
    )
    assert df["s"].to_list() == [f"<{EX}a>", f"<{EX}b>"]


@pytest.mark.parametrize(
    "dt,value,expected",
    [
        ("nonPositiveInteger", "-3", -2),
        ("negativeInteger", "-3", -2),
        ("positiveInteger", "3", 4),
        ("nonNegativeInteger", "3", 4),
    ],
)
def test_arithmetic_on_integer_types_stored_as_strings(dt, value, expected):
    """These types are stored as strings, arithmetic must use their values."""
    m = Model()
    m.reads(
        f'<{EX}a> <{EX}v> "{value}"^^<http://www.w3.org/2001/XMLSchema#{dt}> .',
        format="ntriples",
    )
    df = m.query(f"SELECT (?v + 1 AS ?w) (?v * 2 AS ?x) WHERE {{ <{EX}a> <{EX}v> ?v }}")
    assert df["w"].to_list() == [expected]
    assert df["x"].to_list() == [int(value) * 2]


def test_construct_with_literal_subject_skips_the_triple():
    m = _people()
    ttl = m.query(
        f"""
        CONSTRUCT {{ ?n <{EX}of> ?s . ?s <{EX}named> ?n }}
        WHERE {{ ?s <{EX}name> ?n }}
        """,
        return_format="turtle",
    )
    assert "named" in ttl
    assert "of>" not in ttl and "ns#of" not in ttl


def test_map_df_rejects_unsupported_column():
    m = Model()
    with pytest.raises(Exception, match="cannot be mapped"):
        m.map_df(pl.DataFrame({"a": [[1, 2], [3]]}))


def test_map_df_rejects_multi_type_column():
    df = pl.DataFrame(
        {
            "a": [
                {"<http://www.w3.org/2001/XMLSchema#string>": "x", "<http://www.w3.org/2001/XMLSchema#long>": None},
                {"<http://www.w3.org/2001/XMLSchema#string>": None, "<http://www.w3.org/2001/XMLSchema#long>": 1},
            ]
        }
    )
    m = Model()
    with pytest.raises(Exception, match="several RDF types"):
        m.map_df(df)


def test_template_list_argument_with_variables_is_an_error():
    m = Model()
    m.add_template(
        f"""
        @prefix ex:<{EX}>.
        ex:T [ ?x, ?y ] :: {{
            ottr:Triple(?x, ex:p, (?x, ?y))
        }} .
        """
    )
    with pytest.raises(Exception, match="list containing variables"):
        m.map("ex:T", pl.DataFrame({"x": [f"{EX}a"], "y": [f"{EX}b"]}))


def test_template_list_default_value():
    m = Model()
    m.add_template(
        f"""
        @prefix ex:<{EX}>.
        ex:T [ ottr:IRI ?id, List<xsd:string> ?nicks = ("a", "b") ] :: {{
            cross | ottr:Triple(?id, ex:nick, ++?nicks)
        }} .
        """
    )
    t = m.get_templates()[0]
    nicks = next(p for p in t.parameters if p.variable.name == "nicks")
    assert [x.value for x in nicks.default_value] == ["a", "b"]
    # The type of a list parameter is that of its elements.
    assert nicks.rdf_type is not None


def test_ask_against_endpoint():
    remote = _people()
    with MockEndpoints() as mock:
        url = mock.add("people", remote)
        m = Model()
        assert m.query_external(f"ASK {{ <{EX}a> <{EX}name> ?n }}", url) is True
        assert m.query_external(f"ASK {{ <{EX}x> <{EX}name> ?n }}", url) is False


def test_describe_against_endpoint_is_an_error():
    remote = _people()
    with MockEndpoints() as mock:
        url = mock.add("people", remote)
        m = Model()
        with pytest.raises(Exception, match="DESCRIBE queries are not supported"):
            m.query_external(f"DESCRIBE <{EX}a>", url)



@pytest.mark.skipif(not is_maplib_ee(), reason="disk storage needs maplib_ee")
def test_serialize_disk_model_after_deleting_triples(tmp_path):
    """Serializing collects unused dictionary entries, which used to panic on disk storage."""
    m = Model(storage="disk", storage_folder=str(tmp_path / "store"))
    m.reads(
        "\n".join(f"<{EX}s{i}> <{EX}p> <{EX}o{i}> ." for i in range(1000)),
        format="ntriples",
    )
    m.update(f"DELETE {{ ?s <{EX}p> ?o }} WHERE {{ ?s <{EX}p> ?o FILTER(?s != <{EX}s0>) }}")
    path = tmp_path / "model.maplib"
    m.serialize(path)
    m2 = Model.deserialize(path)
    df = m2.query(f"SELECT ?s ?o WHERE {{ ?s <{EX}p> ?o }}")
    assert df["s"].to_list() == [f"<{EX}s0>"]
    assert df["o"].to_list() == [f"<{EX}o0>"]
    # Strings that were collected can be added again.
    m2.reads(f"<{EX}s5> <{EX}p> <{EX}o5> .", format="ntriples")
    assert m2.query(f"SELECT * WHERE {{ <{EX}s5> <{EX}p> ?o }}")["o"].to_list() == [f"<{EX}o5>"]

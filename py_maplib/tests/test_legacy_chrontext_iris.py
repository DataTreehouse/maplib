"""The time series virtualization vocabulary moved from the chrontext namespace
(https://github.com/DataTreehouse/chrontext#) to the maplib vocabulary. The deprecated chrontext
IRIs are still accepted: in SPARQL they are read as the maplib IRIs, and predicates in data are
stored as the maplib predicates."""

import polars as pl

from maplib import Model
from maplib.config import CHRONTEXT, LEGACY_CHRONTEXT, MAPLIB

DATA = f"""
<urn:ts1> <{LEGACY_CHRONTEXT}hasResource> "readings" .
<urn:ts1> <{LEGACY_CHRONTEXT}hasExternalId> "M-1" .
<urn:ts2> <{MAPLIB}hasResource> "readings" .
<urn:ts2> <{MAPLIB}hasExternalId> "M-2" .
"""


def test_namespace_constants():
    assert MAPLIB == "https://datatreehouse.github.io/maplib/vocab#"
    assert CHRONTEXT == MAPLIB
    assert LEGACY_CHRONTEXT == "https://github.com/DataTreehouse/chrontext#"


def test_legacy_predicates_in_data_are_stored_as_maplib_predicates():
    m = Model()
    m.reads(DATA, format="ntriples")
    df = m.query(
        f"SELECT ?ts ?id WHERE {{ ?ts <{MAPLIB}hasResource> \"readings\" ; <{MAPLIB}hasExternalId> ?id }}"
    ).sort("ts")
    assert df.get_column("id").to_list() == ["M-1", "M-2"]
    predicates = m.query("SELECT DISTINCT ?p WHERE { ?s ?p ?o }").get_column("p").to_list()
    assert sorted(predicates) == [f"<{MAPLIB}hasExternalId>", f"<{MAPLIB}hasResource>"]


def test_legacy_iris_in_queries():
    m = Model()
    m.reads(DATA, format="ntriples")
    df = m.query(
        f"""PREFIX ct: <{LEGACY_CHRONTEXT}>
        SELECT ?ts ?id WHERE {{ ?ts ct:hasResource "readings" ; <{LEGACY_CHRONTEXT}hasExternalId> ?id }}"""
    ).sort("ts")
    assert df.get_column("id").to_list() == ["M-1", "M-2"]


def test_legacy_iris_in_updates():
    m = Model()
    m.update(f"""INSERT DATA {{ <urn:ts3> <{LEGACY_CHRONTEXT}hasResource> "other" }}""")
    df = m.query(f"""SELECT ?ts WHERE {{ ?ts <{MAPLIB}hasResource> "other" }}""")
    assert df.get_column("ts").to_list() == ["<urn:ts3>"]


def test_legacy_function_iri():
    m = Model()
    m.reads(DATA, format="ntriples")
    q = """PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX ns: <{ns}>
    SELECT ?b WHERE {{
        BIND(ns:FloorDateTimeToSecondsInterval("2024-01-01T10:17:31Z"^^xsd:dateTime, 600) AS ?b)
    }}"""
    new = m.query(q.format(ns=MAPLIB))
    old = m.query(q.format(ns=LEGACY_CHRONTEXT))
    assert new.height == 1
    assert new.equals(old)
    assert new.get_column("b").dtype != pl.Null


def test_predicate_variable_over_legacy_predicates_alone():
    """A legacy predicate is stored as the maplib predicate: that predicate must be in the
    dictionary even when no data names it, or ?p and zero-length paths cannot look it up."""
    m = Model()
    m.reads(f"<urn:ts1> <{LEGACY_CHRONTEXT}hasResource> \"readings\" .\n", format="ntriples")
    predicates = m.query("SELECT ?p (COUNT(*) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p").get_column("p").to_list()
    assert predicates == [f"<{MAPLIB}hasResource>"]
    df = m.query("SELECT ?a WHERE { VALUES ?x { <urn:ts1> } ?a <urn:p>* ?x }")
    assert df.get_column("a").to_list() == ["<urn:ts1>"]


def test_predicate_variable_after_update_inserting_legacy_predicate():
    m = Model()
    m.reads("<urn:a> <urn:p> <urn:b> .\n", format="ntriples")
    m.update(f"INSERT {{ ?a <{LEGACY_CHRONTEXT}hasExternalId> \"M-1\" }} WHERE {{ ?a <urn:p> ?b }}")
    predicates = sorted(m.query("SELECT DISTINCT ?p WHERE { ?s ?p ?o }").get_column("p").to_list())
    assert predicates == [f"<{MAPLIB}hasExternalId>", "<urn:p>"]

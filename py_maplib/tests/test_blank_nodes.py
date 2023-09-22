import polars as pl
import pytest
from polars.testing import assert_frame_equal

from maplib import Mapping

pl.Config.set_fmt_str_lengths(300)


@pytest.fixture(scope="function")
def blank_person_mapping():
    # The following example comes from https://primer.ottr.xyz/01-basics.html

    doc = """
    @prefix rdf: 	<http://www.w3.org/1999/02/22-rdf-syntax-ns#> . 
    @prefix rdfs: 	<http://www.w3.org/2000/01/rdf-schema#> . 
    @prefix owl: 	<http://www.w3.org/2002/07/owl#> . 
    @prefix xsd: 	<http://www.w3.org/2001/XMLSchema#> . 
    @prefix foaf: 	<http://xmlns.com/foaf/0.1/> . 
    @prefix dbp: 	<http://dbpedia.org/ontology/> . 
    @prefix ex: 	<http://example.com/ns#> . 
    @prefix ottr: 	<http://ns.ottr.xyz/0.4/> . 
    @prefix ax: 	<http://tpl.ottr.xyz/owl/axiom/0.1/> . 
    @prefix rstr: 	<http://tpl.ottr.xyz/owl/restriction/0.1/> .
    ex:Person[ ?firstName, ?lastName, ?email ] :: {
      ottr:Triple(_:person, rdf:type, foaf:Person ),
      ottr:Triple(_:person, foaf:firstName, ?firstName ),
      ottr:Triple(_:person, foaf:lastName, ?lastName ),
      ottr:Triple(_:person, foaf:mbox, ?email )
    } .
    """
    m = Mapping([doc])
    df = pl.DataFrame({"firstName": ["Ann", "Bob"],
                       "lastName": ["Strong", "Brite"],
                       "email": ["mailto:ann.strong@example.com", "mailto:bob.brite@example.com"]})
    m.expand("ex:Person", df)
    return m


def test_simple_query_no_error(blank_person_mapping):
    df = blank_person_mapping.query("""
        PREFIX foaf:<http://xmlns.com/foaf/0.1/>

        SELECT ?firstName ?lastName WHERE {
        ?p a foaf:Person .
        ?p foaf:lastName ?lastName .
        ?p foaf:firstName ?firstName .
        } ORDER BY ?firstName ?lastName
        """)
    expected_df = pl.DataFrame({"firstName": ["Ann", "Bob"],
                                 "lastName": ["Strong", "Brite"]})


    assert_frame_equal(df, expected_df)

import polars as pl
import pytest
import rdflib
from polars.testing import assert_frame_equal
import pathlib
from maplib import Mapping, RDFType

pl.Config.set_fmt_str_lengths(300)

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"


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
    ex:Person[ ?firstName, ?lastName, ottr:IRI ?email ] :: {
      ottr:Triple(_:person, rdf:type, foaf:Person ),
      ottr:Triple(_:person, foaf:firstName, ?firstName ),
      ottr:Triple(_:person, foaf:lastName, ?lastName ),
      ottr:Triple(_:person, foaf:mbox, ?email )
    } .
    """
    m = Mapping([doc])
    df = pl.DataFrame(
        {
            "firstName": ["Ann", "Bob"],
            "lastName": ["Strong", "Brite"],
            "email": ["mailto:ann.strong@example.com", "mailto:bob.brite@example.com"],
        }
    )
    m.expand("ex:Person", df)
    return m


def test_simple_query_no_error(blank_person_mapping):
    qres = blank_person_mapping.query(
        """
        PREFIX foaf:<http://xmlns.com/foaf/0.1/>

        SELECT ?firstName ?lastName WHERE {
        ?p a foaf:Person .
        ?p foaf:lastName ?lastName .
        ?p foaf:firstName ?firstName .
        } ORDER BY ?firstName ?lastName
        """
    ).sort(["firstName", "lastName"])
    # Todo: Fix multitype sorting
    expected_df = pl.DataFrame(
        {"firstName": ["Ann", "Bob"], "lastName": ["Strong", "Brite"]}
    )

    assert_frame_equal(qres, expected_df)


def test_simple_query_blank_node_output_no_error(blank_person_mapping):
    blank_person_mapping.write_ntriples("out.nt")
    gr = rdflib.Graph()
    gr.parse("out.nt", format="ntriples")
    res = gr.query(
        """
            PREFIX foaf:<http://xmlns.com/foaf/0.1/>

            SELECT ?firstName ?lastName WHERE {
            ?p a foaf:Person .
            ?p foaf:lastName ?lastName .
            ?p foaf:firstName ?firstName .
            } ORDER BY ?firstName ?lastName
            """
    )
    assert len(res) == 2


def test_multi_datatype_query_no_error(blank_person_mapping):
    sm = blank_person_mapping.query(
        """
        PREFIX foaf:<http://xmlns.com/foaf/0.1/>

        SELECT ?s ?v ?o WHERE {
        ?s ?v ?o .
        } 
        """,
        include_datatypes=True,
    )
    by = ["s", "v", "o"]
    df = sm.mappings.sort(by=by)
    assert sm.rdf_types == {
        "o": RDFType.Multi(
            [RDFType.IRI(), RDFType.Literal("http://www.w3.org/2001/XMLSchema#string")]
        ),
        "s": RDFType.BlankNode(),
        "v": RDFType.IRI(),
    }
    filename = TESTDATA_PATH / "multi_datatype_query.csv"
    # df.write_csv(filename)
    expected_df = pl.scan_csv(filename).sort(by).collect()
    assert_frame_equal(df, expected_df)


def test_multi_datatype_union_query_no_error(blank_person_mapping):
    res = blank_person_mapping.query(
        """
        PREFIX foaf:<http://xmlns.com/foaf/0.1/>

        SELECT ?s ?o WHERE {
        {?s foaf:firstName ?o .}
        UNION {
        ?s a ?o .
        }
        } 
        """
    )
    by = ["s", "o"]
    df = res.sort(by=by)
    filename = TESTDATA_PATH / "multi_datatype_union_query.csv"
    # df.write_csv(filename)
    expected_df = pl.scan_csv(filename).sort(by).collect()
    assert_frame_equal(df, expected_df)


def test_multi_datatype_union_sort_query(blank_person_mapping):
    df = blank_person_mapping.query(
        """
        PREFIX foaf:<http://xmlns.com/foaf/0.1/>

        SELECT ?s ?o WHERE {
        {?s foaf:firstName ?o .}
        UNION {
        ?s a ?o .
        }
        } ORDER BY ?o ?s
        """
    )
    filename = TESTDATA_PATH / "multi_datatype_union_sort_query.csv"
    # df.write_csv(filename)
    expected_df = pl.scan_csv(filename).collect()
    assert_frame_equal(df, expected_df)


def test_multi_datatype_union_sort_desc1_query(blank_person_mapping):
    df = blank_person_mapping.query(
        """
        PREFIX foaf:<http://xmlns.com/foaf/0.1/>

        SELECT ?s ?o WHERE {
        {?s foaf:firstName ?o .}
        UNION {
        ?s a ?o .
        }
        } ORDER BY DESC(?o) ?s
        """
    )
    filename = TESTDATA_PATH / "multi_datatype_union_sort_desc1_query.csv"
    # df.write_csv(filename)
    expected_df = pl.scan_csv(filename).collect()
    assert_frame_equal(df, expected_df)


def test_multi_datatype_union_query_native_df(blank_person_mapping):
    res = blank_person_mapping.query(
        """
        PREFIX foaf:<http://xmlns.com/foaf/0.1/>

        SELECT ?s ?o WHERE {
        {?s foaf:firstName ?o .}
        UNION {
        ?s a ?o .
        }
        } 
        """,
        native_dataframe=True,
    )
    by = ["s", "o"]
    df = res.sort(by=by)
    filename = TESTDATA_PATH / "multi_datatype_union_query_native_df.parquet"
    # df.write_parquet(filename)
    expected_df = pl.scan_parquet(filename).sort(by).collect()
    assert_frame_equal(df, expected_df)


def test_multi_datatype_left_join_query_no_error(blank_person_mapping):
    res = blank_person_mapping.query(
        """
        PREFIX foaf:<http://xmlns.com/foaf/0.1/>

        SELECT ?s ?o WHERE {
        ?s foaf:firstName ?o .
        OPTIONAL {
        ?s a ?o .
        }
        } 
        """
    )
    by = ["s", "o"]
    df = res.sort(by=by)
    filename = TESTDATA_PATH / "multi_datatype_leftjoin_query.csv"
    # df.write_csv(filename)
    expected_df = pl.scan_csv(filename).sort(by).collect()
    assert_frame_equal(df, expected_df)


# This test is skipped due to a bug in Polars.
def test_multi_datatype_join_query_two_vars_no_error(blank_person_mapping):
    res = blank_person_mapping.query(
        """
        PREFIX foaf:<http://xmlns.com/foaf/0.1/>

        SELECT ?s ?o WHERE {
        ?s foaf:firstName ?o .
        {
        {?s foaf:firstName ?o }
        UNION
        {?s a ?o }.
        }
        } 
        """
    )
    by = ["s", "o"]
    df = res.sort(by=by)
    filename = TESTDATA_PATH / "multi_datatype_join_query_two_vars.csv"
    # df.write_csv(filename)
    expected_df = pl.scan_csv(filename).sort(by).collect()
    assert_frame_equal(df, expected_df)


def test_multi_datatype_join_query_no_error(blank_person_mapping):
    res = blank_person_mapping.query(
        """
        PREFIX foaf:<http://xmlns.com/foaf/0.1/>

        SELECT ?s1 ?s2 ?o WHERE {
        ?s1 foaf:firstName ?o .
        {
        {?s2 foaf:firstName ?o }
        UNION
        {?s2 a ?o }.
        }
        } 
        """
    )
    by = ["s1", "s2", "o"]
    df = res.sort(by=by)
    filename = TESTDATA_PATH / "multi_datatype_join_query.csv"
    # df.write_csv(filename)
    expected_df = pl.scan_csv(filename).sort(by).collect()
    assert_frame_equal(df, expected_df)


def test_multi_datatype_query_sorting_sorting(blank_person_mapping):
    res = blank_person_mapping.query(
        """
        PREFIX foaf:<http://xmlns.com/foaf/0.1/>

        SELECT ?s ?v ?o WHERE {
        ?s ?v ?o .
        } ORDER BY ?s ?v ?o
        """
    ).sort(["s", "v", "o"])
    # TODO: Fix multitype sorting
    filename = TESTDATA_PATH / "multi_datatype_query_sorting.csv"
    # res.write_csv(filename)
    expected_df = pl.scan_csv(filename).collect()
    assert_frame_equal(res, expected_df)

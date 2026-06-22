from maplib import Model
from maplib.maplib import SolutionMappings, RDFType
import polars as pl
import logging

def test_gc_happens():
    df = pl.DataFrame({
        "subject": [f"urn:maplib:a_{i}" for i in range(2_000_000)],
        "object": [f"urn:maplib:b_{i}" for i in range(2_000_000)],
    })
    m = Model()
    sm = SolutionMappings(df, {
        "subject": RDFType.IRI,
        "object": RDFType.IRI
    })

    m.map_triples(sm, predicate="urn:maplib:my_predicate")

    df2 = pl.DataFrame({
        "subject": [f"urn:maplib:a_{i}" for i in range(2_000)],
        "object": [f"urn:maplib:b_{i}" for i in range(2_000)],
    })
    sm2 = SolutionMappings(df2, {
        "subject": RDFType.IRI,
        "object": RDFType.IRI,
    })
    m.map_triples(sm2, predicate="urn:maplib:my_other_predicate")

    m.update("""
        DELETE {
            ?a <urn:maplib:my_predicate> ?c
        } WHERE {
            ?a <urn:maplib:my_predicate> ?c
        }
        """)

    # We are manually checking log output in this test.. 
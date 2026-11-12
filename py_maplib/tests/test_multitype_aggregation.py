import pytest

from maplib import Model

# Each subject has values of one or more numeric types, so ?o has multiple types (a struct).
DATA = """
@prefix : <http://example.org/> .
:ints :int 1, 2, 3 .
:decimals :dec 1.0, 2.2, 3.5 .
:doubles :double 1.0E2, 2.0E3, 3.0E4 .
:mixed1 :int 1 ; :dec 2.2 .
:mixed2 :double 2.0E-1 ; :dec 2.2 .
:withiri :int 1 ; :iri :o .
"""

XSD = "http://www.w3.org/2001/XMLSchema#"


def aggregate(agg: str, distinct: bool = False):
    m = Model()
    m.reads(DATA, format="turtle")
    d = "DISTINCT " if distinct else ""
    df = m.query(
        f"""PREFIX : <http://example.org/>
        SELECT ?s (STR({agg}({d}?o)) AS ?v) (DATATYPE({agg}({d}?o)) AS ?dt)
        WHERE {{ ?s ?p ?o }} GROUP BY ?s"""
    )
    out = {}
    for s, v, dt in df.select(["s", "v", "dt"]).iter_rows():
        name = s.rsplit("/", 1)[-1].rstrip(">")
        out[name] = (v, dt.strip("<>").replace(XSD, "") if dt is not None else None)
    return out


def num(v):
    return float(v) if v is not None else None


def test_sum_promotes_per_group():
    r = aggregate("SUM")
    assert (num(r["ints"][0]), r["ints"][1]) == (6, "integer")
    assert (num(r["decimals"][0]), r["decimals"][1]) == (6.7, "decimal")
    assert (num(r["doubles"][0]), r["doubles"][1]) == (32100, "double")
    assert (num(r["mixed1"][0]), r["mixed1"][1]) == (3.2, "decimal")
    assert (num(r["mixed2"][0]), r["mixed2"][1]) == (pytest.approx(2.4), "double")


def test_avg_promotes_per_group():
    r = aggregate("AVG")
    assert (num(r["ints"][0]), r["ints"][1]) == (2, "decimal")
    assert (num(r["mixed1"][0]), r["mixed1"][1]) == (1.6, "decimal")
    assert (num(r["mixed2"][0]), r["mixed2"][1]) == (pytest.approx(1.2), "double")


@pytest.mark.parametrize("agg", ["SUM", "AVG"])
def test_non_numeric_value_gives_unbound(agg):
    assert aggregate(agg)["withiri"] == (None, None)


def test_min_keeps_type_of_selected_value():
    r = aggregate("MIN")
    assert (num(r["mixed1"][0]), r["mixed1"][1]) == (1, "integer")
    assert (num(r["mixed2"][0]), r["mixed2"][1]) == (0.2, "double")
    assert (num(r["ints"][0]), r["ints"][1]) == (1, "integer")


def test_max_keeps_type_of_selected_value():
    r = aggregate("MAX")
    assert (num(r["mixed1"][0]), r["mixed1"][1]) == (2.2, "decimal")
    assert (num(r["mixed2"][0]), r["mixed2"][1]) == (2.2, "decimal")
    assert (num(r["doubles"][0]), r["doubles"][1]) == (30000, "double")


def test_min_max_order_iris_before_literals():
    # SPARQL ordering: IRIs are ordered before literals
    r_min = aggregate("MIN")
    r_max = aggregate("MAX")
    assert r_min["withiri"] == ("http://example.org/o", None)
    assert (num(r_max["withiri"][0]), r_max["withiri"][1]) == (1, "integer")


def test_sum_distinct_multitype():
    m = Model()
    m.reads(
        """@prefix : <http://example.org/> .
        :a :int 1, 2 ; :dec 2.0, 2.0 .""",
        format="turtle",
    )
    df = m.query(
        """PREFIX : <http://example.org/>
        SELECT (STR(SUM(DISTINCT ?o)) AS ?v) (DATATYPE(SUM(DISTINCT ?o)) AS ?dt)
        WHERE { ?s ?p ?o }"""
    )
    # 1 and 2 (integers) and 2.0 (decimal) are three distinct terms
    assert float(df["v"][0]) == 5
    assert df["dt"][0] == f"<{XSD}decimal>"

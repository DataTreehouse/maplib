"""
The statistical aggregates of Apache Jena ARQ: agg:stdev, agg:stdev_samp, agg:stdev_pop,
agg:variance, agg:var_samp and agg:var_pop in <http://jena.apache.org/ARQ/function/aggregate#>,
also available in <http://jena.apache.org/ARQ/function#> (afn:) as in Jena.

* Compliance: Jena's own tests (jena-arq TestStatisticsAggregates.java, Apache License 2.0, see
  testdata/jena_statistics/README.md), ported with the same data, queries and expected values.
* Differential tests against Python's statistics module (stdev, pstdev, variance, pvariance), over
  groups with mixed numeric datatypes, DISTINCT and unbound values.
* Error cases (non-numeric values, groups too small), the result datatype, HAVING and window
  functions.
"""

import math
import random
import re
import statistics
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from maplib import MaplibException, Model, RDFType

AGG = "http://jena.apache.org/ARQ/function/aggregate#"
AFN = "http://jena.apache.org/ARQ/function#"
PRE = f"PREFIX agg: <{AGG}>\nPREFIX afn: <{AFN}>\nPREFIX : <http://example/>\n"
XSD = "http://www.w3.org/2001/XMLSchema#"

JENA_DIR = Path(__file__).parent / "testdata" / "jena_statistics"

# Jena: (dataset (graph (:x :p1 -1) (:x :p2 2) (:x :p3 3) (:x :p4 3) ))
JENA_DS = """
<http://example/x> <http://example/p1> "-1"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example/x> <http://example/p2> "2"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example/x> <http://example/p3> "3"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example/x> <http://example/p4> "3"^^<http://www.w3.org/2001/XMLSchema#integer> .
"""
# Jena: (dataset (graph (:x :p -1)) )
JENA_DS1 = """
<http://example/x> <http://example/p> "-1"^^<http://www.w3.org/2001/XMLSchema#integer> .
"""

# The expected values of TestStatisticsAggregates, the second of each pair with DISTINCT.
VALUES = {
    "stdev_samp": (1.8929694486000912, 2.0816659994661326),
    "stdev_pop": (1.6393596310755, 1.699673171197595),
    "var_samp": (3.5833333333333333, 4.333333333333333),
    "var_pop": (2.6875, 2.888888888888889),
}
# The six aggregates and the statistic they compute.
FUNCTIONS = {
    "stdev": "stdev_samp",
    "stdev_samp": "stdev_samp",
    "stdev_pop": "stdev_pop",
    "variance": "var_samp",
    "var_samp": "var_samp",
    "var_pop": "var_pop",
}
PYTHON = {
    "stdev_samp": statistics.stdev,
    "stdev_pop": statistics.pstdev,
    "var_samp": statistics.variance,
    "var_pop": statistics.pvariance,
}
# Jena's test method names: the method of the plain and DISTINCT SPARQL 1.1 form of each function.
JENA_URI_TESTS = {
    "stdev": ("agg_stat_stdev_uri", "agg_stat_stdev_uri_distinct"),
    "stdev_samp": ("agg_stat_stdev_samp_uri", "agg_stat_stdev_samp_uri_distinct"),
    "stdev_pop": ("agg_stat_stdevp_uri", "agg_stat_stdev_pop_uri_distinct"),
    "variance": ("agg_stat_var_uri", "agg_stat_var_uri_distinct"),
    "var_samp": ("agg_stat_var_samp_uri", "agg_stat_var_samp_uri_distinct"),
    "var_pop": ("agg_stat_var_pop_uri", "agg_stat_var_pop_uri_distinct"),
}


def model(data: str) -> Model:
    m = Model()
    if data:
        m.reads(data, format="ntriples")
    return m


def group_by(agg: str) -> str:
    # Jena's buildGroupBy
    return PRE + f"SELECT ({agg}\nAS ?X) WHERE {{?s ?p ?x}} GROUP BY ?s"


def no_group_by(agg: str) -> str:
    # Jena's buildNoGroupBy
    return PRE + f"SELECT ({agg}\nAS ?X) WHERE {{?s ?p ?x}}"


def single_value(m: Model, query: str):
    df = m.query(query)
    assert df.height == 1, df
    return df["X"][0]


# ---- Ported from TestStatisticsAggregates.java


@pytest.mark.parametrize("name", list(FUNCTIONS))
@pytest.mark.parametrize("distinct", [False, True])
@pytest.mark.parametrize("form", ["prefixed", "full_iri", "afn"])
def test_jena_value(name, distinct, form):
    """agg_stat_*_uri and agg_stat_*_uri_distinct, and agg_stat_stdev_full_uri for the full IRI."""
    expected = VALUES[FUNCTIONS[name]][1 if distinct else 0]
    fn = {"prefixed": f"agg:{name}", "full_iri": f"<{AGG}{name}>", "afn": f"afn:{name}"}[form]
    arg = "DISTINCT ?x" if distinct else "?x"
    value = single_value(model(JENA_DS), group_by(f"{fn}({arg})"))
    assert value == pytest.approx(expected, abs=0.001)


@pytest.mark.parametrize("name", ["stdev", "stdev_samp", "stdev_pop"])
def test_jena_empty(name):
    """agg_stat_*_empty: no values is an error. Without GROUP BY there is one solution where ?X
    is unbound, with GROUP BY there are no solutions."""
    m = model("")
    df = m.query(no_group_by(f"agg:{name}(?x)"))
    assert df.height == 1
    assert "X" in df.columns
    assert df["X"][0] is None
    assert m.query(group_by(f"agg:{name}(?x)")).height == 0


@pytest.mark.parametrize("name", ["stdev", "stdev_samp", "var_samp", "variance"])
def test_jena_sample_of_one(name):
    """agg_stat_stdev_size_one, agg_stat_stdev_samp_size_one, agg_stat_var_size_one_1: the
    sample statistics of one value are an error, so ?X is unbound."""
    df = model(JENA_DS1).query(group_by(f"agg:{name}(?x)"))
    assert df.height == 1
    assert df["X"][0] is None


@pytest.mark.parametrize("name", ["stdev_pop", "var_pop"])
def test_jena_population_of_one(name):
    """agg_stat_stdev_pop_size_one, agg_stat_var_pop_size_one: 0e0."""
    assert single_value(model(JENA_DS1), group_by(f"agg:{name}(?x)")) == 0.0


@pytest.mark.parametrize("ns", [AGG, AFN])
@pytest.mark.parametrize("name", list(FUNCTIONS))
def test_jena_registry(ns, name):
    """agg_stat_registry: all six are aggregates, in both namespaces."""
    value = single_value(model(JENA_DS), group_by(f"<{ns}{name}>(?x)"))
    assert value == pytest.approx(VALUES[FUNCTIONS[name]][0], abs=0.001)


def test_jena_registry_not_an_aggregate():
    """agg_stat_registry: afn:e is a function, not an aggregate, so ?x is not grouped."""
    with pytest.raises(Exception):
        model(JENA_DS).query(group_by("afn:e(?x)"))


def test_jena_test_methods_are_covered():
    """Every test method of the Java file is ported here, or is an ARQ syntax test that is not
    SPARQL 1.1 (see the README)."""
    java = (JENA_DIR / "TestStatisticsAggregates.java").read_text()
    methods = set(re.findall(r"@Test public void (\w+)\(", java))
    ported = {m for pair in JENA_URI_TESTS.values() for m in pair}
    ported |= {
        "agg_stat_stdev_full_uri",
        "agg_stat_registry",
        "agg_stat_stdev_empty",
        "agg_stat_stdev_samp_empty",
        "agg_stat_stdev_pop_empty",
        "agg_stat_stdev_size_one",
        "agg_stat_stdev_samp_size_one",
        "agg_stat_var_size_one_1",
        "agg_stat_stdev_pop_size_one",
        "agg_stat_var_pop_size_one",
    }
    arq_syntax = {m for m in methods if m.endswith(("_kw", "_kw_distinct", "_agg", "_agg_distinct"))}
    assert methods - ported - arq_syntax == set()
    assert ported - methods == set()
    # The expected values in the Java file are the ones used here.
    for stat, (plain, distinct) in VALUES.items():
        for value, suffix in ((plain, ""), (distinct, "d")):
            m = re.search(rf"VALUE{stat}{suffix}\s*=\s*([0-9.eE+-]+);", java)
            assert m is not None, stat + suffix
            assert float(m.group(1)) == value


# ---- Differential tests against Python's statistics module


def typed(v) -> str:
    if isinstance(v, bool):
        return f'"{str(v).lower()}"^^<{XSD}boolean>'
    if isinstance(v, int):
        return f'"{v}"^^<{XSD}integer>'
    if isinstance(v, Decimal):
        return f'"{v}"^^<{XSD}decimal>'
    if isinstance(v, tuple):
        return f'"{v[1]}"^^<{XSD}{v[0]}>'
    return f'"{v!r}"^^<{XSD}double>'


def as_float(v) -> float:
    return float(v[1]) if isinstance(v, tuple) else float(v)


def random_groups(seed: int, mixed: bool):
    rng = random.Random(seed)
    groups = {}
    for g in range(30):
        n = rng.randint(1, 12)
        values = []
        for _ in range(n):
            kind = rng.randrange(4) if mixed else 0
            if kind == 0:
                values.append(rng.randint(-1000, 1000))
            elif kind == 1:
                values.append(Decimal(rng.randint(-100000, 100000)) / 100)
            elif kind == 2:
                values.append(rng.uniform(-1e3, 1e3))
            else:
                values.append(("float", float(rng.randint(-512, 512)) / 4))
        # Repeated values, for DISTINCT
        if n > 2:
            values.append(values[0])
        groups[g] = values
    return groups


def groups_model(groups) -> Model:
    lines = []
    for g, values in groups.items():
        for i, v in enumerate(values):
            # One triple per value, so that duplicates are kept
            lines.append(f"<http://example/g{g}> <http://example/v{i}> {typed(v)} .")
            lines.append(f"<http://example/g{g}> <http://example/group> <http://example/g{g}> .")
    return model("\n".join(lines))


def expected(stat: str, values, distinct: bool):
    floats = [as_float(v) for v in values]
    if distinct:
        # DISTINCT compares RDF terms: values of different datatypes are distinct.
        seen = {}
        for v, f in zip(values, floats):
            seen.setdefault(typed(v), f)
        floats = list(seen.values())
    minimum = 2 if stat in ("stdev_samp", "var_samp") else 1
    if len(floats) < minimum:
        return None
    return PYTHON[stat](floats)


@pytest.mark.parametrize("mixed", [False, True])
@pytest.mark.parametrize("distinct", [False, True])
def test_against_python_statistics(mixed, distinct):
    groups = random_groups(42 if mixed else 7, mixed)
    m = groups_model(groups)
    arg = "DISTINCT ?x" if distinct else "?x"
    select = " ".join(f"(agg:{name}({arg}) AS ?{name})" for name in FUNCTIONS)
    df = m.query(
        PRE
        + f"""
        SELECT ?s {select} WHERE {{
            ?s ?p ?x .
            FILTER(?p != :group)
        }} GROUP BY ?s
        """
    )
    assert df.height == len(groups)
    for row in df.iter_rows(named=True):
        g = int(row["s"].strip("<>").rsplit("g", 1)[1])
        for name, stat in FUNCTIONS.items():
            exp = expected(stat, groups[g], distinct)
            if exp is None:
                assert row[name] is None, (g, name, groups[g])
            else:
                assert row[name] == pytest.approx(exp, rel=1e-9, abs=1e-9), (g, name, groups[g])


def test_result_datatype_is_double():
    for distinct in ["", "DISTINCT "]:
        sm = model(JENA_DS).query(
            group_by(f"agg:stdev({distinct}?x)"), solution_mappings=True
        )
        assert sm.rdf_types["X"] == RDFType.Literal(XSD + "double")


@pytest.mark.parametrize("datatype", ["integer", "int", "long", "short", "decimal", "float", "double", "unsignedByte"])
def test_numeric_datatypes(datatype):
    values = ["1", "2", "4", "8"]
    data = "\n".join(
        f'<http://example/x> <http://example/p{i}> "{v}"^^<{XSD}{datatype}> .'
        for i, v in enumerate(values)
    )
    df = model(data).query(group_by("agg:var_pop(?x)").replace("AS ?X", "AS ?X"))
    assert df["X"][0] == pytest.approx(statistics.pvariance([1, 2, 4, 8]))


# ---- Errors and unbound values


def test_non_numeric_value_in_group_is_an_error():
    data = JENA_DS + '<http://example/x> <http://example/p5> "a" .\n'
    data += '<http://example/y> <http://example/p1> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .\n'
    data += '<http://example/y> <http://example/p2> "3"^^<http://www.w3.org/2001/XMLSchema#integer> .\n'
    df = model(data).query(
        PRE + "SELECT ?s (agg:stdev(?x) AS ?X) WHERE {?s ?p ?x} GROUP BY ?s ORDER BY ?s"
    )
    assert df["X"].to_list()[0] is None
    assert df["X"].to_list()[1] == pytest.approx(statistics.stdev([1, 3]))


@pytest.mark.parametrize(
    "objects",
    [
        ['"a"', '"b"'],
        ['"true"^^<http://www.w3.org/2001/XMLSchema#boolean>', '"false"^^<http://www.w3.org/2001/XMLSchema#boolean>'],
        ["<http://example/o1>", "<http://example/o2>"],
        ['"2020-01-01"^^<http://www.w3.org/2001/XMLSchema#date>', '"2021-01-01"^^<http://www.w3.org/2001/XMLSchema#date>'],
    ],
)
def test_only_non_numeric_values_is_an_error(objects):
    data = "\n".join(f"<http://example/x> <http://example/p{i}> {o} ." for i, o in enumerate(objects))
    for name in FUNCTIONS:
        df = model(data).query(group_by(f"agg:{name}(?x)"))
        assert df.height == 1
        assert df["X"][0] is None, name


def test_unbound_values_are_skipped():
    m = model(JENA_DS + "<http://example/x> <http://example/q> <http://example/z> .\n")
    df = m.query(
        PRE
        + """
        SELECT (agg:var_samp(?v) AS ?X) WHERE {
            ?s ?p ?o .
            OPTIONAL { ?s ?p ?v FILTER(isNumeric(?v)) }
        } GROUP BY ?s
        """
    )
    assert df["X"][0] == pytest.approx(VALUES["var_samp"][0])


def test_expression_argument():
    df = model(JENA_DS).query(group_by("agg:var_pop(?x * 2)"))
    assert df["X"][0] == pytest.approx(4 * VALUES["var_pop"][0])


def test_ordered_aggregate_ignores_order():
    df = model(JENA_DS).query(group_by("agg:stdev(?x ORDER BY DESC(?x))"))
    assert df["X"][0] == pytest.approx(VALUES["stdev_samp"][0])


def test_having():
    data = JENA_DS + '<http://example/y> <http://example/p1> "5"^^<http://www.w3.org/2001/XMLSchema#integer> .\n'
    data += '<http://example/y> <http://example/p2> "5"^^<http://www.w3.org/2001/XMLSchema#integer> .\n'
    df = model(data).query(
        PRE + "SELECT ?s WHERE {?s ?p ?x} GROUP BY ?s HAVING (agg:stdev_pop(?x) > 0)"
    )
    assert df["s"].to_list() == ["<http://example/x>"]


def test_combined_with_builtin_aggregates():
    df = model(JENA_DS).query(
        PRE
        + "SELECT (AVG(?x) AS ?mean) (agg:stdev(?x) AS ?sd) ((MAX(?x) - AVG(?x)) / agg:stdev(?x) AS ?z) "
        "WHERE {?s ?p ?x} GROUP BY ?s"
    )
    sd = statistics.stdev([-1, 2, 3, 3])
    assert df["sd"][0] == pytest.approx(sd)
    assert float(df["mean"][0]) == pytest.approx(7 / 4)
    # AVG over integers is an xsd:decimal, so MAX - AVG is not truncated
    assert df["z"][0] == pytest.approx((3 - 7 / 4) / sd)


def test_not_a_regular_function():
    with pytest.raises(MaplibException):
        model(JENA_DS).query(PRE + "SELECT ?x (agg:stdev(?x) AS ?X) WHERE {?s ?p ?x}")
    with pytest.raises(MaplibException):
        model(JENA_DS).query(PRE + "SELECT * WHERE {?s ?p ?x FILTER(agg:stdev(?x) > 0)}")


# ---- Window functions


def test_window_partition():
    data = JENA_DS + '<http://example/y> <http://example/p1> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .\n'
    data += '<http://example/y> <http://example/p2> "4"^^<http://www.w3.org/2001/XMLSchema#integer> .\n'
    df = model(data).query(
        PRE
        + """
        SELECT ?s ?x (agg:stdev(?x) OVER (PARTITION BY ?s) AS ?sd)
                     (agg:var_pop(DISTINCT ?x) OVER (PARTITION BY ?s) AS ?vp)
        WHERE {?s ?p ?x} ORDER BY ?s ?x
        """
    )
    assert df.height == 6
    for row in df.iter_rows(named=True):
        xs = [-1, 2, 3, 3] if row["s"] == "<http://example/x>" else [1, 4]
        assert row["sd"] == pytest.approx(statistics.stdev(xs))
        assert row["vp"] == pytest.approx(statistics.pvariance(set(xs)))


def test_window_rolling_frame():
    values = [5, 1, 4, 2, 8, 7]
    data = "\n".join(
        f'<http://example/x> <http://example/p{i}> "{v}"^^<{XSD}integer> .\n'
        f'<http://example/x> <http://example/p{i}> "{i}"^^<{XSD}int> .'
        for i, v in enumerate(values)
    )
    df = model(data).query(
        PRE
        + f"""
        SELECT ?i ?x (agg:stdev_samp(?x) OVER (ORDER BY ?i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ?sd)
        WHERE {{
            ?s ?p ?x . ?s ?p ?i .
            FILTER(datatype(?x) = <{XSD}integer> && datatype(?i) = <{XSD}int>)
        }} ORDER BY ?i
        """
    )
    sds = df["sd"].to_list()
    assert sds[0] is None  # a sample of one
    for i in range(1, len(values)):
        assert sds[i] == pytest.approx(statistics.stdev(values[i - 1 : i + 1]))


def test_window_serialization_round_trip():
    # Window calls are encoded in the algebra; the custom aggregate IRI must survive that.
    m = model(JENA_DS)
    df = m.query(
        PRE + "SELECT DISTINCT (agg:var_pop(?x) OVER () AS ?v) WHERE {?s ?p ?x}"
    )
    assert df["v"].to_list() == [pytest.approx(VALUES["var_pop"][0])]

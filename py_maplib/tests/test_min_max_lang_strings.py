"""MIN and MAX over language-tagged strings follow ORDER BY: by the value, then by the language.

The values of a column of only language-tagged strings are structs of dictionary codes. MIN and MAX
used to take Polars' min and max of that struct: Polars 2.1.0 gave null (an unbound result), and
later Polars compare the codes, which are in insertion order (found with the Polars bump to main
34f40615, where Sparqloscope's MIN(?label) over RINF labels came out differently).
"""

import pytest

from maplib import Model

DATA = """
@prefix : <http://example.org/> .
:a :label "Hovd"@nb, "Alpha"@en, "Beta"@nb, "NR 9"@en, "Alpha"@nb .
:b :label "pære"@nb, "pear"@en, "Birne"@de .
:c :label "eple"@nb .
"""

P = "PREFIX : <http://example.org/> "


@pytest.fixture(scope="module")
def model():
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def ordered(model, subject=None):
    where = f"?s :label ?o" if subject is None else f":{subject} :label ?o"
    return model.query(P + f"SELECT ?o WHERE {{ {where} }} ORDER BY ?o")["o"].to_list()


def test_min_max_ungrouped(model):
    df = model.query(P + "SELECT (MIN(?o) AS ?min) (MAX(?o) AS ?max) WHERE { ?s :label ?o }")
    order = ordered(model)
    assert order[0] == '"Alpha"@en' and order[-1] == '"pære"@nb'
    assert df["min"].to_list() == [order[0]]
    assert df["max"].to_list() == [order[-1]]


@pytest.mark.parametrize("distinct", ["", "DISTINCT "])
def test_min_max_per_group(model, distinct):
    df = model.query(
        P
        + f"SELECT ?s (MIN({distinct}?o) AS ?min) (MAX({distinct}?o) AS ?max) "
        + "WHERE { ?s :label ?o } GROUP BY ?s ORDER BY ?s"
    )
    for s, lo, hi in zip(df["s"].to_list(), df["min"].to_list(), df["max"].to_list()):
        order = ordered(model, s.rsplit("/", 1)[1].rstrip(">"))
        assert (lo, hi) == (order[0], order[-1]), s

import pytest

from maplib import Model

# MIN and MAX over a column with values of several kinds follow the ordering of ORDER BY: blank
# nodes, IRIs, numbers by value, then the other literal types by type and by value. The chosen
# value keeps its own type, and an unbound value is only chosen when nothing else is there.
DATA = """
@prefix : <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
:everything :v [ :tag "blank" ], :iri2, :iri1, 5, 2.5, 7.5E0, "1.5"^^xsd:float, "-2"^^xsd:long,
    "pear", "apple", "eple"@nb, "apple"@en, true, "2020-01-01"^^xsd:date,
    "1999-12-31T23:59:59"^^xsd:dateTime, "2024-02-29T12:00:00Z"^^xsd:dateTime .
:iris_and_literals :v :iri2, :iri1, :iri3, 5, "pear" .
:numbers :v 5, 2.5, 7.5E0, "1.5"^^xsd:float, "-2"^^xsd:long, "11"^^xsd:unsignedInt, "-4"^^xsd:short .
:specials :v "NaN"^^xsd:double, "INF"^^xsd:double, "-INF"^^xsd:double, 3 .
:only_nan :v "NaN"^^xsd:double .
:big :v 9007199254740993, 9007199254740990, 1.0E3 .
:strings :v "pear", "apple", "Zebra", "", "æble" .
:strings_and_others :v "pear", "apple", "eple"@nb, "apple"@en, false, "2020-01-01"^^xsd:date .
:dates :v "2020-01-01"^^xsd:date, "1999-12-31"^^xsd:date, "2024-02-29"^^xsd:date .
:date_times :v "2020-01-01T00:00:00"^^xsd:dateTime, "1999-12-31T23:59:59"^^xsd:dateTime .
:booleans :v true, false .
:blanks_and_numbers :v [ :tag "blank" ], 1, 2 .
:one_iri :v :iri1 .
:one_string :v "pear" .
:one_language :v "eple"@nb .
:nothing :other 1 .
:everything :member :m1, :m2, :m3 . :numbers :member :m1 . :strings :member :m2, :m4 .
:nothing :member :m3, :m4 . :dates :member :m4 . :one_iri :member :m5 . :nothing :member :m6 .
"""

P = "PREFIX : <http://example.org/> "
XSD = "http://www.w3.org/2001/XMLSchema#"
SHOW = "(STR(?m) AS ?v) (DATATYPE(?m) AS ?dt) (LANG(?m) AS ?l) (isBlank(?m) AS ?b) (BOUND(?m) AS ?bound)"
BODIES = {
    # the name of the group is the subject
    "by subject": "SELECT ?g ({agg}(?o) AS ?m) WHERE {{ ?g :member ?x OPTIONAL {{ ?g :v ?o }} }} GROUP BY ?g",
    "by subject, distinct": "SELECT ?g ({agg}(DISTINCT ?o) AS ?m) WHERE {{ ?g :member ?x OPTIONAL {{ ?g :v ?o }} }} GROUP BY ?g",
    "all subjects": "SELECT ?g ({agg}(?o) AS ?m) WHERE {{ ?g :v ?o }} GROUP BY ?g",
    # groups that share subjects, so the rows of a group are spread over the column
    "by member": "SELECT ?g ({agg}(?o) AS ?m) WHERE {{ ?s :member ?g OPTIONAL {{ ?s :v ?o }} }} GROUP BY ?g",
    "ungrouped": "SELECT ({agg}(?o) AS ?m) WHERE {{ ?s :member ?x OPTIONAL {{ ?s :v ?o }} }}",
    "ungrouped literals": "SELECT ({agg}(?o) AS ?m) WHERE {{ ?s :v ?o FILTER(isLiteral(?o)) }}",
    "ungrouped unbound": "SELECT ({agg}(?o) AS ?m) WHERE {{ :nothing :member ?x OPTIONAL {{ :nothing :v ?o }} }}",
    "ungrouped empty": "SELECT ({agg}(?o) AS ?m) WHERE {{ ?s :absent ?o }}",
    # columns of one type
    "iris by member": "SELECT ?g ({agg}(?s) AS ?m) WHERE {{ ?s :member ?g }} GROUP BY ?g",
    "iris ungrouped": "SELECT ({agg}(?s) AS ?m) WHERE {{ ?s :member ?g }}",
    "strings ungrouped": "SELECT ({agg}(?o) AS ?m) WHERE {{ :strings :v ?o }}",
}


def term(v, dt, lang, blank, bound, tag):
    if not bound:
        return None
    if blank:
        return f"_:{tag}"
    if dt is None:
        return f"<{v.replace('http://example.org/', ':')}>"
    dt = dt.strip("<>").replace(XSD, "xsd:")
    if lang:
        return f'"{v}"@{lang}'
    return f'"{v}"^^{dt}'


def run(agg, name):
    m = Model()
    m.reads(DATA, format="turtle")
    inner = BODIES[name].format(agg=agg)
    g = "?g " if "?g (" in inner else ""
    # The blank node is recognised by its tag, as its label differs between runs
    df = m.query(
        f"""{P} SELECT {g}?v ?dt ?l ?b ?bound (?tag AS ?t) WHERE {{
        {{ SELECT {g}?m {SHOW} WHERE {{ {{ {inner} }} }} }} OPTIONAL {{ ?m :tag ?tag }} }}"""
    )
    out = {}
    for row in df.iter_rows(named=True):
        key = row["g"].strip("<>").replace("http://example.org/", ":") if g else ""
        out[key] = term(row["v"], row["dt"], row["l"], row["b"], row["bound"], row["t"])
    return out


# The results of maplib before MIN and MAX became a scan of each group (0.25.2)
EXPECTED = {'MAX all subjects': {':big': '"9007199254740993"^^xsd:integer',
                      ':blanks_and_numbers': '"2"^^xsd:integer',
                      ':booleans': '"true"^^xsd:boolean',
                      ':date_times': '"2020-01-01T00:00:00"^^xsd:dateTime',
                      ':dates': '"2024-02-29"^^xsd:date',
                      ':everything': '"pear"^^xsd:string',
                      ':iris_and_literals': '"pear"^^xsd:string',
                      ':numbers': '"11"^^xsd:unsignedInt',
                      ':one_iri': '<:iri1>',
                      ':one_language': '"eple"@nb',
                      ':one_string': '"pear"^^xsd:string',
                      ':only_nan': '"NaN"^^xsd:double',
                      ':specials': '"NaN"^^xsd:double',
                      ':strings': '"æble"^^xsd:string',
                      ':strings_and_others': '"pear"^^xsd:string'},
 'MAX by member': {':m1': '"pear"^^xsd:string',
                   ':m2': '"æble"^^xsd:string',
                   ':m3': '"pear"^^xsd:string',
                   ':m4': '"æble"^^xsd:string',
                   ':m5': '<:iri1>',
                   ':m6': None},
 'MAX by subject': {':dates': '"2024-02-29"^^xsd:date',
                    ':everything': '"pear"^^xsd:string',
                    ':nothing': None,
                    ':numbers': '"11"^^xsd:unsignedInt',
                    ':one_iri': '<:iri1>',
                    ':strings': '"æble"^^xsd:string'},
 'MAX by subject, distinct': {':dates': '"2024-02-29"^^xsd:date',
                              ':everything': '"pear"^^xsd:string',
                              ':nothing': None,
                              ':numbers': '"11"^^xsd:unsignedInt',
                              ':one_iri': '<:iri1>',
                              ':strings': '"æble"^^xsd:string'},
 'MAX iris by member': {':m1': '<:numbers>',
                        ':m2': '<:strings>',
                        ':m3': '<:nothing>',
                        ':m4': '<:strings>',
                        ':m5': '<:one_iri>',
                        ':m6': '<:nothing>'},
 'MAX iris ungrouped': {'': '<:strings>'},
 'MAX strings ungrouped': {'': '"æble"^^xsd:string'},
 'MAX ungrouped': {'': '"æble"^^xsd:string'},
 'MAX ungrouped empty': {'': None},
 'MAX ungrouped literals': {'': '"æble"^^xsd:string'},
 'MAX ungrouped unbound': {'': None},
 'MIN all subjects': {':big': '"1000.0"^^xsd:double',
                      ':blanks_and_numbers': '_:blank',
                      ':booleans': '"false"^^xsd:boolean',
                      ':date_times': '"1999-12-31T23:59:59"^^xsd:dateTime',
                      ':dates': '"1999-12-31"^^xsd:date',
                      ':everything': '_:blank',
                      ':iris_and_literals': '<:iri1>',
                      ':numbers': '"-4"^^xsd:short',
                      ':one_iri': '<:iri1>',
                      ':one_language': '"eple"@nb',
                      ':one_string': '"pear"^^xsd:string',
                      ':only_nan': '"NaN"^^xsd:double',
                      ':specials': '"-INF"^^xsd:double',
                      ':strings': '""^^xsd:string',
                      ':strings_and_others': '"eple"@nb'},
 'MIN by member': {':m1': '_:blank',
                   ':m2': '_:blank',
                   ':m3': '_:blank',
                   ':m4': '"1999-12-31"^^xsd:date',
                   ':m5': '<:iri1>',
                   ':m6': None},
 'MIN by subject': {':dates': '"1999-12-31"^^xsd:date',
                    ':everything': '_:blank',
                    ':nothing': None,
                    ':numbers': '"-4"^^xsd:short',
                    ':one_iri': '<:iri1>',
                    ':strings': '""^^xsd:string'},
 'MIN by subject, distinct': {':dates': '"1999-12-31"^^xsd:date',
                              ':everything': '_:blank',
                              ':nothing': None,
                              ':numbers': '"-4"^^xsd:short',
                              ':one_iri': '<:iri1>',
                              ':strings': '""^^xsd:string'},
 'MIN iris by member': {':m1': '<:everything>',
                        ':m2': '<:everything>',
                        ':m3': '<:everything>',
                        ':m4': '<:dates>',
                        ':m5': '<:one_iri>',
                        ':m6': '<:nothing>'},
 'MIN iris ungrouped': {'': '<:dates>'},
 'MIN strings ungrouped': {'': '""^^xsd:string'},
 'MIN ungrouped': {'': '_:blank'},
 'MIN ungrouped empty': {'': None},
 'MIN ungrouped literals': {'': '"-INF"^^xsd:double'},
 'MIN ungrouped unbound': {'': None}}


@pytest.mark.parametrize("name", list(BODIES))
@pytest.mark.parametrize("agg", ["MIN", "MAX"])
def test_min_max_mixed_types(agg, name):
    assert run(agg, name) == EXPECTED[f"{agg} {name}"]


@pytest.mark.parametrize("agg", ["MIN", "MAX"])
@pytest.mark.parametrize("group", ["", "GROUP BY ?s"])
def test_equal_numbers_of_several_types(agg, group):
    # 1, 1.0 and 1.0E0 are equal in the ordering, so any of them may be chosen, with its own type
    m = Model()
    m.reads(
        """@prefix : <http://example.org/> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        :a :v 1, 1.0, 1.0E0, "1"^^xsd:float, "-0.0E0"^^xsd:double, 0, "x" .
        :b :v 1.0, 1 .""",
        format="turtle",
    )
    extreme = 0 if agg == "MIN" else 1
    df = m.query(
        f"""{P} SELECT (STR(?m) AS ?v) (DATATYPE(?m) AS ?dt) WHERE {{
        {{ SELECT ({agg}(?o) AS ?m) WHERE {{ ?s :v ?o FILTER(isNumeric(?o)) }} {group} }} }}"""
    )
    assert df.height == (2 if group else 1)
    for v, dt in df.iter_rows():
        assert float(v) in ((extreme, 1) if group else (extreme,))
        assert dt.strip("<>").replace(XSD, "") in ("integer", "decimal", "double", "float")

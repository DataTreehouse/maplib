"""Regression tests for surprising results listed in TO_DOCUMENT.md by the sample analysis
sessions: silent wrong or empty results, and SPARQL that other engines accept."""

import pytest
import polars as pl
from maplib import Model, MaplibException

NS = [10, 2, 100, 9, 1]


def labelled_model() -> Model:
    m = Model()
    m.reads(
        " ".join(f"<urn:n/{n}> a <urn:N> ." for n in NS),
        format="turtle",
    )
    # Stored string literals, dictionary encoded in insertion order (10, 2, 100, 9, 1)
    m.update("INSERT { ?x <urn:lab> ?a } WHERE { ?x a <urn:N> BIND(STR(?x) AS ?a) }")
    return m


LABELS = sorted(f"urn:n/{n}" for n in NS)


def test_min_max_of_stored_strings_use_codepoint_order():
    m = labelled_model()
    df = m.query("SELECT (MIN(?a) AS ?mn) (MAX(?a) AS ?mx) WHERE { ?x <urn:lab> ?a }")
    assert df["mn"].to_list() == [LABELS[0]]
    assert df["mx"].to_list() == [LABELS[-1]]


def test_min_max_of_stored_strings_per_group():
    m = labelled_model()
    m.reads(
        "<urn:n/10> <urn:g> <urn:g1> . <urn:n/1> <urn:g> <urn:g1> . <urn:n/2> <urn:g> <urn:g1> . "
        "<urn:n/100> <urn:g> <urn:g2> . <urn:n/9> <urn:g> <urn:g2> .",
        format="turtle",
    )
    df = m.query(
        "SELECT ?g (MIN(?a) AS ?mn) (MAX(?a) AS ?mx) WHERE { ?x <urn:lab> ?a ; <urn:g> ?g } "
        "GROUP BY ?g ORDER BY ?g"
    )
    assert df["mn"].to_list() == ["urn:n/1", "urn:n/100"]
    assert df["mx"].to_list() == ["urn:n/2", "urn:n/9"]


def test_min_of_stored_strings_in_multitype_column():
    m = labelled_model()
    m.reads("<urn:n/1> <urn:lab> 5 .", format="turtle")
    df = m.query("SELECT (MAX(?a) AS ?mx) WHERE { ?x <urn:lab> ?a FILTER(isLiteral(?a) && !isNumeric(?a)) }")
    assert df["mx"].to_list() == [LABELS[-1]]
    df = m.query("SELECT (MAX(?a) AS ?mx) WHERE { ?x <urn:lab> ?a }")
    # Strings are ordered after numbers
    assert df["mx"].to_list() == [LABELS[-1]]


def test_min_max_of_stored_iris_use_codepoint_order():
    m = labelled_model()
    df = m.query("SELECT (MIN(?x) AS ?mn) (MAX(?x) AS ?mx) WHERE { ?x a <urn:N> }")
    assert df["mn"].to_list() == [f"<{LABELS[0]}>"]
    assert df["mx"].to_list() == [f"<{LABELS[-1]}>"]


def test_compare_stored_strings():
    m = labelled_model()
    df = m.query(
        "SELECT ?a ?b WHERE { ?x <urn:lab> ?a . ?y <urn:lab> ?b FILTER(?a < ?b) }"
    )
    pairs = set(zip(df["a"].to_list(), df["b"].to_list()))
    expected = {(a, b) for a in LABELS for b in LABELS if a < b}
    assert pairs == expected
    df = m.query(
        "SELECT ?a ?b WHERE { ?x <urn:lab> ?a . ?y <urn:lab> ?b FILTER(?a >= ?b) }"
    )
    assert df.height == len(LABELS) * (len(LABELS) + 1) // 2


def test_compare_stored_string_with_constant():
    m = labelled_model()
    df = m.query('SELECT ?a WHERE { ?x <urn:lab> ?a FILTER(?a <= "urn:n/10") } ORDER BY ?a')
    assert df["a"].to_list() == ["urn:n/1", "urn:n/10"]
    df = m.query('SELECT ?a WHERE { ?x <urn:lab> ?a FILTER(?a > "urn:n/10") } ORDER BY ?a')
    assert df["a"].to_list() == ["urn:n/100", "urn:n/2", "urn:n/9"]


def test_compare_values_strings():
    m = Model()
    df = m.query(
        'SELECT ?a ?b (?a < ?b AS ?lt) (?a > ?b AS ?gt) WHERE { '
        'VALUES (?a ?b) { ("n/10" "n/2") ("n/2" "n/10") ("n/2" "n/2") } }'
    )
    rows = {(a, b): (lt, gt) for a, b, lt, gt in df.iter_rows()}
    assert rows[("n/10", "n/2")] == (True, False)
    assert rows[("n/2", "n/10")] == (False, True)
    assert rows[("n/2", "n/2")] == (False, False)


def test_order_by_stored_strings():
    m = labelled_model()
    df = m.query("SELECT ?a WHERE { ?x <urn:lab> ?a } ORDER BY ?a")
    assert df["a"].to_list() == LABELS
    df = m.query("SELECT ?a WHERE { ?x <urn:lab> ?a } ORDER BY DESC(?a)")
    assert df["a"].to_list() == LABELS[::-1]


@pytest.mark.parametrize("var", ["ol", "oI", "oB", "o_right", "ov"])
def test_variable_names_do_not_collide_with_multitype_fields(var):
    m = Model()
    m.reads(
        '@prefix ex:<http://ex/> . ex:a ex:p ex:b , "x"@en , 1 . ex:b ex:label "B" .',
        format="turtle",
    )
    df = m.query(
        f"PREFIX ex:<http://ex/> SELECT ?o ?{var} WHERE {{ ex:a ex:p ?o OPTIONAL {{ ?o ex:label ?{var} }} }} "
        "ORDER BY ?o"
    )
    assert df.height == 3
    labels = dict(zip(df["o"].to_list(), df[var].to_list()))
    assert labels["<http://ex/b>"] == "B"
    assert sum(v is None for v in labels.values()) == 2


def test_union_values_branch_with_outer_values():
    m = Model()
    m.reads("<urn:t> <urn:of> <urn:a> .", format="turtle")
    q = "SELECT ?x WHERE { %s { VALUES ?x { <urn:a> } } UNION { ?x <urn:of> ?eq } } ORDER BY ?x"
    without = m.query(q % "")
    assert without["x"].to_list() == ["<urn:a>", "<urn:t>"]
    df = m.query(q % "VALUES ?eq { <urn:a> }")
    assert df["x"].to_list() == ["<urn:a>", "<urn:t>"]


def test_values_joined_after_union_binding_only_in_one_branch():
    m = Model()
    m.reads("<urn:t> <urn:of> <urn:a> . <urn:u> <urn:of> <urn:b> .", format="turtle")
    df = m.query(
        "SELECT ?x ?eq WHERE { { VALUES ?x { <urn:c> } } UNION { ?x <urn:of> ?eq } "
        "VALUES ?eq { <urn:a> } } ORDER BY ?x"
    )
    assert df["x"].to_list() == ["<urn:c>", "<urn:t>"]
    assert df["eq"].to_list() == ["<urn:a>", "<urn:a>"]


G = "urn:g"
EX = "PREFIX ex: <http://ex/> "


def graph_triples(m: Model, graph=None) -> set:
    df = m.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", graph=graph, include_graphs=[])
    return set(df.iter_rows())


def test_update_with_graph_inserts_and_deletes_in_the_graph():
    m = Model()
    m.reads("<http://ex/a> <http://ex/p> <http://ex/b> .", format="ntriples", graph=G)
    m.reads("<http://ex/x> <http://ex/p> <http://ex/y> .", format="ntriples")
    m.update(EX + "DELETE { ?s ex:p ?o } INSERT { ?s ex:r ?o } WHERE { ?s ex:p ?o }", graph=G)
    assert graph_triples(m, G) == {("<http://ex/a>", "<http://ex/r>", "<http://ex/b>")}
    assert graph_triples(m) == {("<http://ex/x>", "<http://ex/p>", "<http://ex/y>")}


def test_update_with_graph_same_as_with():
    def run(update, graph):
        m = Model()
        m.reads("<http://ex/a> <http://ex/p> <http://ex/b> .", format="ntriples", graph=G)
        m.update(update, graph=graph)
        return graph_triples(m, G), graph_triples(m)

    body = "DELETE { ?s ex:p ?o } INSERT { ?s ex:r ?o . GRAPH <urn:h> { ?s ex:q ?o } } WHERE { ?s ex:p ?o }"
    assert run(EX + body, G) == run(EX + f"WITH <{G}> " + body, None)


def test_update_with_graph_data():
    m = Model()
    m.update(EX + "INSERT DATA { ex:a ex:p ex:b . GRAPH <urn:h> { ex:c ex:p ex:d } }", graph=G)
    assert graph_triples(m, G) == {("<http://ex/a>", "<http://ex/p>", "<http://ex/b>")}
    assert graph_triples(m, "urn:h") == {("<http://ex/c>", "<http://ex/p>", "<http://ex/d>")}
    assert graph_triples(m) == set()
    m.update(EX + "DELETE DATA { ex:a ex:p ex:b }", graph=G)
    assert graph_triples(m, G) == set()


def test_update_with_graph_delete_where_whole_table():
    m = Model()
    m.reads("<http://ex/a> <http://ex/p> <http://ex/b> .", format="ntriples", graph=G)
    m.reads("<http://ex/x> <http://ex/p> <http://ex/y> .", format="ntriples")
    m.update(EX + "DELETE WHERE { ?s ex:p ?o }", graph=G)
    assert graph_triples(m, G) == set()
    assert graph_triples(m) == {("<http://ex/x>", "<http://ex/p>", "<http://ex/y>")}


def test_add_graph_from_itself():
    m = Model()
    m.reads("<http://ex/a> <http://ex/p> <http://ex/b> .", format="ntriples", graph="urn:a")
    m.reads('<http://ex/c> <http://ex/l> "c" .', format="ntriples", graph="urn:a")
    m.add_graph(m, source_graph="urn:a", target_graph="urn:b")
    assert graph_triples(m, "urn:b") == graph_triples(m, "urn:a")
    assert len(graph_triples(m, "urn:b")) == 2
    # Copying into a graph that has triples adds to it without duplicates
    m.add_graph(m, source_graph="urn:a", target_graph="urn:b")
    assert m.size("urn:b") == 2
    m.add_graph(m, source_graph="urn:a", target_graph="urn:a")
    assert m.size("urn:a") == 2
    df = m.query('SELECT ?s WHERE { ?s <http://ex/l> "c" }', graph="urn:b")
    assert df["s"].to_list() == ["<http://ex/c>"]


def test_unknown_maplib_magic_predicate_raises():
    m = Model()
    m.reads("<urn:a> <urn:p> <urn:b> .", format="turtle")
    with pytest.raises(MaplibException, match="noSuchMagic"):
        m.query(
            "PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#> "
            "SELECT ?p WHERE { <urn:a> maplib:noSuchMagic ?p }"
        )


def test_maplib_namespace_predicate_with_triples_is_queried():
    m = Model()
    m.reads(
        "<urn:a> <https://datatreehouse.github.io/maplib/vocab#mine> <urn:b> .",
        format="turtle",
    )
    df = m.query(
        "PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#> "
        "SELECT ?o WHERE { <urn:a> maplib:mine ?o }"
    )
    assert df["o"].to_list() == ["<urn:b>"]


def test_maplib_time_series_predicate_without_triples_is_not_an_error():
    m = Model()
    m.reads("<urn:a> <urn:p> <urn:b> .", format="turtle")
    df = m.query(
        "PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#> "
        "SELECT ?dp WHERE { ?ts maplib:hasDataPoint ?dp }"
    )
    assert df.height == 0


def test_fts_without_index_raises():
    m = Model()
    m.reads('<urn:a> <urn:l> "pump" .', format="turtle")
    with pytest.raises(MaplibException, match="full text index"):
        m.query(
            "PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#> "
            'SELECT ?n WHERE { ?n maplib:fts ?f . ?f maplib:ftsQuery "pump" }'
        )


V = "<http://ex/a> <http://ex/v> 5 . <http://ex/b> <http://ex/v> 20 ."


def test_python_int_binding():
    m = Model()
    m.reads(V, format="turtle")
    df = m.query("SELECT ?s WHERE { ?s <http://ex/v> ?v FILTER(?v >= ?min) }", bindings={"min": 10})
    assert df["s"].to_list() == ["<http://ex/b>"]
    df = m.query("SELECT ?s WHERE { ?s <http://ex/v> ?v }", bindings={"v": 5})
    assert df["s"].to_list() == ["<http://ex/a>"]


def test_python_float_and_bool_bindings():
    m = Model()
    m.reads(
        "<http://ex/a> <http://ex/v> 5.5e0 ; <http://ex/f> true . "
        "<http://ex/b> <http://ex/v> 20.0e0 ; <http://ex/f> false .",
        format="turtle",
    )
    df = m.query(
        "SELECT ?s WHERE { ?s <http://ex/v> ?v ; <http://ex/f> ?f FILTER(?v > ?min && ?f = ?flag) }",
        bindings={"min": 1.5, "flag": True},
    )
    assert df["s"].to_list() == ["<http://ex/a>"]


def test_unsupported_binding_type_raises():
    m = Model()
    with pytest.raises(Exception, match="bool, int or float"):
        m.query("SELECT ?s WHERE { ?s ?p ?o }", bindings={"o": [1, 2]})


def test_update_bindings():
    from maplib import Literal, IRI

    m = Model()
    m.reads(V, format="turtle")
    m.update(
        "INSERT { ?s <http://ex/w> ?x } WHERE { ?s <http://ex/v> ?v FILTER(?v > ?min) }",
        bindings={
            "x": Literal("7", data_type=IRI("http://www.w3.org/2001/XMLSchema#integer")),
            "min": 10,
        },
    )
    df = m.query("SELECT ?s ?w WHERE { ?s <http://ex/w> ?w }")
    assert df.rows() == [("<http://ex/b>", 7)]
    m.update(
        "DELETE { ?s <http://ex/w> ?x } WHERE { ?s <http://ex/w> ?x }",
        bindings={"s": IRI("http://ex/b")},
    )
    assert m.query("SELECT ?s WHERE { ?s <http://ex/w> ?w }").height == 0


def test_select_without_variables_keeps_the_solution():
    m = Model()
    m.reads("<http://ex/a> <http://ex/p> <http://ex/b> .", format="turtle")
    assert m.query("SELECT * WHERE { <http://ex/a> <http://ex/p> <http://ex/b> }").shape == (1, 0)
    assert m.query("SELECT * WHERE { <http://ex/a> <http://ex/p> <http://ex/c> }").shape == (0, 0)
    assert m.query("SELECT * WHERE { }").shape == (1, 0)


def test_group_by_expression_projected_with_the_same_expression():
    m = Model()
    m.reads(
        '<http://ex/x> <http://ex/d> "2020-05-01"^^<http://www.w3.org/2001/XMLSchema#date> . '
        '<http://ex/y> <http://ex/d> "2021-05-01"^^<http://www.w3.org/2001/XMLSchema#date> . '
        '<http://ex/z> <http://ex/d> "2021-07-01"^^<http://www.w3.org/2001/XMLSchema#date> .',
        format="turtle",
    )
    df = m.query(
        "SELECT (YEAR(?a) AS ?y) (COUNT(*) AS ?n) WHERE { ?x <http://ex/d> ?a } "
        "GROUP BY (YEAR(?a)) ORDER BY ?y"
    )
    assert df.rows() == [(2020, 1), (2021, 2)]


def test_count_without_group_by_over_no_rows_is_zero():
    m = Model()
    m.reads("<http://ex/a> <http://ex/p> <http://ex/b> .", format="turtle")
    df = m.query("SELECT (COUNT(?o) AS ?n) WHERE { <http://ex/a> <http://ex/none> ?o }")
    assert df.rows() == [(0,)]


def test_strdt_with_invalid_lexical_form_is_unbound():
    m = Model()
    df = m.query(
        'SELECT ?s ?v WHERE { VALUES ?s { "2020-01-01" "x" } '
        "BIND(STRDT(?s, <http://www.w3.org/2001/XMLSchema#date>) AS ?v) } ORDER BY ?s"
    )
    assert df.height == 2
    assert df["v"].null_count() == 1


TEXT = (
    '<http://ex/a> <http://ex/v> "abc" ; <http://ex/w> "ab" ; <http://ex/pat> "^A" ; <http://ex/fl> "i" . '
    '<http://ex/b> <http://ex/v> "xbc" ; <http://ex/w> "ab" ; <http://ex/pat> "^a" ; <http://ex/fl> "" . '
    '<http://ex/c> <http://ex/v> "cab" ; <http://ex/w> "b" ; <http://ex/pat> "(" ; <http://ex/fl> "" .'
)


def test_regex_with_pattern_from_the_data():
    m = Model()
    m.reads(TEXT, format="turtle")
    df = m.query(
        "SELECT ?s ?m WHERE { ?s <http://ex/v> ?v ; <http://ex/pat> ?p "
        "BIND(REGEX(?v, ?p) AS ?m) } ORDER BY ?s"
    )
    # "^A" does not match "abc" without the i flag, and the invalid pattern "(" is unbound
    assert df["m"].to_list() == [False, False, None]
    df = m.query(
        "SELECT ?s WHERE { ?s <http://ex/v> ?v ; <http://ex/pat> ?p ; <http://ex/fl> ?f "
        "FILTER(REGEX(?v, ?p, ?f)) } ORDER BY ?s"
    )
    assert df["s"].to_list() == ["<http://ex/a>"]
    df = m.query(
        'SELECT ?s WHERE { ?s <http://ex/v> ?v ; <http://ex/w> ?w FILTER(REGEX(?v, CONCAT("^", ?w))) }'
    )
    assert df["s"].to_list() == ["<http://ex/a>"]


def test_string_functions_with_second_argument_from_the_data():
    m = Model()
    m.reads(TEXT, format="turtle")
    df = m.query(
        "SELECT ?s (STRSTARTS(?v, ?w) AS ?st) (CONTAINS(?v, ?w) AS ?co) "
        "(STRBEFORE(?v, ?w) AS ?be) (STRAFTER(?v, ?w) AS ?af) "
        "WHERE { ?s <http://ex/v> ?v ; <http://ex/w> ?w } ORDER BY ?s"
    )
    assert df["st"].to_list() == [True, False, False]
    assert df["co"].to_list() == [True, False, True]
    assert df["be"].to_list() == ["", "", "ca"]
    assert df["af"].to_list() == ["c", "", ""]


def test_string_function_with_non_string_second_argument_is_unbound():
    m = Model()
    m.reads(TEXT, format="turtle")
    df = m.query("SELECT ?s (STRSTARTS(?v, ?s) AS ?st) WHERE { ?s <http://ex/v> ?v } ORDER BY ?s")
    assert df["st"].to_list() == [None, None, None]


def test_describe():
    m = Model()
    m.reads(
        "<http://ex/a> <http://ex/p> <http://ex/b> ; <http://ex/q> 1 . "
        "<http://ex/c> <http://ex/p> <http://ex/a> . _:x <http://ex/p> <http://ex/c> .",
        format="turtle",
    )
    res = m.query("DESCRIBE <http://ex/a>")
    assert sum(df.height for df in res) == 2
    assert {s for df in res for s in df["subject"].to_list()} == {"<http://ex/a>"}
    res = m.query("DESCRIBE ?o WHERE { <http://ex/c> <http://ex/p> ?o }")
    assert sum(df.height for df in res) == 2
    # Blank nodes are not described
    res = m.query("DESCRIBE ?s WHERE { ?s <http://ex/p> <http://ex/c> }")
    assert sum(df.height for df in res) == 0


def test_categorical_columns_are_mapped_as_strings():
    from maplib import SolutionMappings, RDFType

    m = Model()
    df = pl.DataFrame(
        {"subject": ["urn:a", "urn:b"], "object": ["x", "y"]},
        schema={"subject": pl.String, "object": pl.Categorical},
    )
    m.map_triples(
        SolutionMappings(
            df,
            {
                "subject": RDFType.IRI,
                "object": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
            },
        ),
        predicate="urn:p",
    )
    out = m.query("SELECT ?o WHERE { ?s <urn:p> ?o } ORDER BY ?o")
    assert out["o"].to_list() == ["x", "y"]

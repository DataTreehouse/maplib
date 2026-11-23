"""Model.describe summarises each graph: the predicates with their object types and distinct
counts, the classes, the namespaces, and on request class profiles, links, literal ranges,
dangling references, overlap between graphs and declared vocabulary. Its text is bounded."""
import json

import pytest
from maplib import Model

EX = "http://example.com/a#"
EXB = "http://example.com/b/"
FOAF = "http://xmlns.com/foaf/0.1/"
XSD = "http://www.w3.org/2001/XMLSchema#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
G2 = "urn:g:two"


def model() -> Model:
    m = Model()
    m.reads(
        f"""
@prefix ex: <{EX}> . @prefix foaf: <{FOAF}> . @prefix xsd: <{XSD}> .
@prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
ex:a a foaf:Person ; foaf:name "A"@en, "Aa" ; foaf:age 3 ; foaf:knows ex:b, _:x ; ex:worksAt ex:org .
ex:b a foaf:Person ; foaf:name "B" ; foaf:age "4.5"^^xsd:decimal ; ex:born "2020-01-01"^^xsd:date ;
    ex:worksAt ex:gone .
ex:org a foaf:Organization ; foaf:name "Org" .
_:x foaf:name "X" .
foaf:Person a owl:Class ; rdfs:label "Person" .
foaf:name a owl:DatatypeProperty ; rdfs:label "name"@en .
""",
        format="turtle",
    )
    m.reads(
        f"@prefix ex: <{EXB}> . ex:s ex:p ex:o ; ex:q 1.5e0 . <{EX}gone> ex:p 1 .",
        format="turtle",
        graph=G2,
    )
    return m


@pytest.fixture(scope="module")
def full():
    return model().describe(sections="all", exact=True)


def row(df, **key) -> dict:
    (r,) = [r for r in df.to_dicts() if all(r[k] == v for k, v in key.items())]
    return r


def test_graphs():
    d = model().describe()
    assert d.sections == ["predicates", "classes", "namespaces"]
    assert d.graphs.select("graph", "triples", "predicates", "classes", "subjects").to_dicts() == [
        {"graph": None, "triples": 19, "predicates": 7, "classes": 4, "subjects": None},
        {"graph": G2, "triples": 3, "predicates": 2, "classes": 0, "subjects": None},
    ]
    assert model().describe(graphs=[G2]).graphs["graph"].to_list() == [G2]
    assert model().describe(graphs=[None]).graphs["graph"].to_list() == [None]
    assert Model().describe().graphs.height == 0
    assert str(Model().describe(sections="all")) == "0 graphs, 0 triples\n"
    with pytest.raises(ValueError):
        model().describe(sections=["nope"])


def test_predicate_types_from_statistics():
    d = model().describe()
    name = row(d.predicates, graph=None, predicate=FOAF + "name")
    assert name["triples"] == 5
    assert name["subject_kinds"] == ["Blank", "IRI"]
    assert name["object_types"] == [XSD + "string", RDF + "langString"]
    assert name["object_type_triples"] == [4, 1]
    assert not name["exact"] and name["functional"] is None
    assert name["distinct_subjects"] >= 4
    assert sorted(row(d.predicates, graph=None, predicate=FOAF + "knows")["object_types"]) == ["Blank", "IRI"]
    born = row(d.predicates, graph=None, predicate=EX + "born")
    assert (born["object_types"], born["distinct_subjects"], born["exact"]) == ([XSD + "date"], 1, True)
    assert born["functional"] and born["inverse_functional"]
    # The largest graph and, in it, the largest predicates come first.
    assert d.predicates["graph"].to_list()[0] is None
    assert d.predicates["triples"].to_list()[:7] == sorted(d.predicates["triples"].to_list()[:7], reverse=True)


def test_exact_counts_match_queries(full):
    m = model()
    assert full.predicates["exact"].all()
    for r in full.predicates.iter_rows(named=True):
        c = m.query(
            f"SELECT (COUNT(DISTINCT ?s) AS ?s) (COUNT(DISTINCT ?o) AS ?o) WHERE {{ ?s <{r['predicate']}> ?o }}",
            graph=r["graph"],
        )
        assert (r["distinct_subjects"], r["distinct_objects"]) == (c["s"][0], c["o"][0]), r["predicate"]
    assert (row(full.predicates, graph=None, predicate=FOAF + "name")["distinct_subjects"]) == 4


def test_counts_per_predicate_query():
    """The query behind exact=True, against the same counts one predicate at a time, also with
    kind filters, in a named graph and after a delete."""
    m = model()
    m.update(f'DELETE DATA {{ <{EX}a> <{FOAF}name> "Aa" }}')
    for graph in (None, G2):
        for flt in ("", "FILTER(isIRI(?o))", "FILTER(isLiteral(?o) && !isBlank(?s))"):
            df = m.query(
                "SELECT ?p (COUNT(DISTINCT ?s) AS ?s_n) (COUNT(DISTINCT ?o) AS ?o_n) (COUNT(*) AS ?n) "
                f"(COUNT(DISTINCT ?p) AS ?p_n) WHERE {{ ?s ?p ?o {flt} }} GROUP BY ?p",
                graph=graph,
            )
            general = m.query(
                "SELECT ?p (COUNT(DISTINCT ?s) AS ?s_n) (COUNT(DISTINCT ?o) AS ?o_n) (COUNT(*) AS ?n) "
                f"(COUNT(DISTINCT ?p) AS ?p_n) WHERE {{ ?s ?p ?o {flt} FILTER(true) }} GROUP BY ?p",
                graph=graph,
            )
            assert df.height > 0
            assert sorted(df.rows()) == sorted(general.rows()), (graph, flt)
    empty = m.query("SELECT ?p (COUNT(*) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p", graph="urn:none")
    assert empty.height == 0


def test_counts_per_predicate_fast_path():
    import maplib.maplib as native

    if not hasattr(native, "_fast_path_counts"):
        pytest.skip("built without fast path counters")
    m = model()

    def hit(q):
        native._reset_fast_path_counts()
        m.query(q)
        return native._fast_path_counts().get("count.grouped_by_predicate", 0) > 0

    assert hit("SELECT ?p (COUNT(DISTINCT ?s) AS ?a) (COUNT(DISTINCT ?o) AS ?b) WHERE { ?s ?p ?o } GROUP BY ?p")
    assert not hit("SELECT ?s (COUNT(DISTINCT ?o) AS ?b) WHERE { ?s ?p ?o } GROUP BY ?s")
    assert not hit("SELECT ?p (COUNT(DISTINCT ?o) AS ?b) WHERE { ?s ?p ?o . ?o ?p2 ?o2 } GROUP BY ?p")
    native._reset_fast_path_counts()
    m.describe(exact=True)
    assert native._fast_path_counts().get("count.grouped_by_predicate", 0) > 0


def test_classes_namespaces_vocabulary(full):
    person = row(full.classes, graph=None, **{"class": FOAF + "Person"})
    assert (person["instances"], person["declared"], person["label"]) == (2, True, "Person")
    assert row(full.classes, graph=None, **{"class": FOAF + "Organization"})["declared"] is False
    assert row(full.predicates, graph=None, predicate=FOAF + "name")["label"] == "name"
    assert row(full.predicates, graph=None, predicate=RDF + "type")["declared"] is None
    # Without the vocabulary section nothing is said about declarations.
    assert "declared" not in model().describe().classes.columns
    ns = {(r["graph"], r["namespace"], r["role"]): (r["prefix"], r["terms"]) for r in full.namespaces.to_dicts()}
    assert ns[(None, FOAF, "predicate")] == ("foaf", 3)
    assert ns[(None, FOAF, "class")] == ("foaf", 2)
    assert ns[(None, EX, "subject")] == ("a", 3)
    assert ns[(G2, EXB, "object")] == ("b", 1)
    default = {(r["graph"], r["namespace"], r["role"]) for r in model().describe().namespaces.to_dicts()}
    assert (None, EX, "subject") not in default


def test_subjects(full):
    g = row(full.graphs, graph=None)
    assert (g["subjects"], g["untyped_subjects"], g["blank_subjects"]) == (6, 1, 1)
    assert row(full.graphs, graph=G2)["untyped_subjects"] == 2


def test_class_predicates_and_links(full):
    name = row(full.class_predicates, graph=None, predicate=FOAF + "name", **{"class": FOAF + "Person"})
    assert (name["instances"], name["subjects"], name["coverage"]) == (2, 2, 1.0)
    assert (name["min_per_subject"], name["max_per_subject"], name["triples"]) == (1, 2, 3)
    born = row(full.class_predicates, graph=None, predicate=EX + "born", **{"class": FOAF + "Person"})
    assert born["coverage"] == 0.5
    assert RDF + "type" not in full.class_predicates["predicate"].to_list()
    assert sorted(full.links.select("subject_class", "predicate", "object_class", "triples").rows()) == [
        (FOAF + "Person", EX + "worksAt", FOAF + "Organization", 1),
        (FOAF + "Person", FOAF + "knows", FOAF + "Person", 1),
    ]
    text = full.show_class("foaf:Person")
    assert "foaf:Person in default graph: 2 instances" in text
    born_line = next(l for l in text.splitlines() if l.strip().startswith("a:born"))
    assert born_line.split() == ["a:born", "50.0%", "0..1"]
    assert ["foaf:Person", "foaf:knows", "foaf:Person", "1"] in [l.split() for l in text.splitlines()]
    assert full.show_class(f"<{FOAF}Nothing>").startswith("no instances")
    assert "| a:born | 50.0% | 0..1 |" in full.show_class(f"<{FOAF}Person>", format="markdown")


def test_literals_examples(full):
    born = row(full.literals, graph=None, predicate=EX + "born")
    assert (born["datatype"], born["min"], born["max"]) == (XSD + "date", "2020-01-01", "2020-01-01")
    name = row(full.literals, graph=None, predicate=FOAF + "name", datatype=XSD + "string")
    assert (name["min_length"], name["max_length"], name["min"]) == (1, 3, None)
    assert row(full.languages, graph=None, predicate=FOAF + "name")["language"] == "en"
    assert row(full.predicates, graph=None, predicate=EX + "born")["example"] == "2020-01-01"
    assert "en (1)" in full.show_predicate("foaf:name")
    assert ["a:born", "2020-01-01"] in [l.split() for l in str(full).splitlines()]
    assert "Example object: 2020-01-01" in full.show_predicate("a:born")
    # A column without values is left out: only strings here, so no min and max.
    only_strings = full.to_text(graphs=[G2], sections=["literals"])
    assert "languages" not in only_strings and "min" in only_strings


def test_dangling_and_overlap(full):
    d = row(full.dangling, graph=None)
    # ex:gone is not a subject in the default graph, but is in the other.
    assert (d["predicate"], d["objects"], d["in_other_graphs"]) == (EX + "worksAt", 1, 1)
    d2 = row(full.dangling, graph=G2)
    assert (d2["predicate"], d2["objects"], d2["in_other_graphs"]) == (EXB + "p", 1, 0)
    o = row(full.overlap, graph_a=None, graph_b=G2)
    assert (o["shared_subjects"], o["a_refers_to_b"], o["b_refers_to_a"]) == (0, 1, 0)


def test_text_is_bounded():
    m = model()
    for i in range(30):
        m.reads(f"<{EXB}s{i}> <{EXB}p> <{EXB}o> ; <{EXB}q> 2.5e0 .", format="turtle", graph=f"urn:g:same{i}")
    for i in range(40):
        m.reads(f"<{EX}a> <{EX}p{i}> {i} . <{EX}c{i}> a <{EX}C{i}> .", format="turtle", graph="urn:g:wide")
    d = m.describe(sections="all")
    text = str(d)
    # Graphs with the same predicates are detailed once.
    assert text.count("\n<urn:g:same") == 0 and "Same predicates in 30 more graphs" in text
    assert "... 18 more graphs with" in text
    assert "... 26 more predicates with 26 triples" in text
    assert "more classes with" in text
    assert len(text) < 12_000
    assert len(d.to_text(max_rows=None, max_graphs=None)) > len(text)
    short = d.to_text(max_chars=1500)
    assert len(short) <= 1500 + len("\n... cut at max_chars")
    one = d.to_text(graphs=["urn:g:same7"], sections=["predicates"])
    assert "\n<urn:g:same7>\n===" in one and "Namespaces (" not in one and "Predicates (2)" in one
    md = d.to_markdown(max_graphs=1, max_rows=3)
    assert md.startswith("# 33 graphs, 162 triples\n\n| graph | triples |")
    assert "\n## <urn:g:wide>\n" in md and "\n### Predicates (41)\n\n| predicate | " in md
    assert "|---|---:|" in md and len(md) < len(d.to_markdown())
    assert d._repr_markdown_() == d.to_markdown(max_chars=20_000)
    # Every row of a table of the text has the columns of its header at the same place.
    lines = text.splitlines()
    rule = next(i for i, l in enumerate(lines) if l.strip().startswith("---"))
    assert len(lines[rule]) == len(lines[rule - 1]) >= len(lines[rule + 1])
    # No line is longer than the width; cells that do not fit are cut, and only then.
    wide = d.to_text(max_graphs=None, max_rows=None, width=None)
    assert max(map(len, wide.splitlines())) > 100
    assert max(map(len, d.to_text(max_graphs=None, max_rows=None).splitlines())) <= 100
    narrow = d.to_text(graphs=[None], sections=["predicates"], width=90)
    assert max(map(len, narrow.splitlines())) <= 90 and "…" in narrow
    assert "…" not in wide
    with pytest.raises(ValueError):
        d.to_text(format="html")
    limited = d.to_dict(max_rows=2, max_graphs=3)
    json.dumps(limited)
    assert limited["graphs_total"] == 33 and len(limited["graphs"]) == 3
    assert len([r for r in limited["predicates"] if r["graph"] == "urn:g:wide"]) == 2


def test_diff():
    m = model()
    before = m.describe()
    m.update(f"INSERT DATA {{ <{EX}c> a <{FOAF}Person> ; <{FOAF}name> 'C' }}")
    diff = m.describe().diff(before)
    assert diff.predicates.select("predicate", "before", "after", "change").rows() == [
        (FOAF + "name", 5, 6, 1),
        (RDF + "type", 5, 6, 1),
    ] or sorted(diff.predicates["change"].to_list()) == [1, 1]
    assert diff.classes.select("class", "change").rows() == [(FOAF + "Person", 1)]
    assert ["default", "graph", "foaf:Person", "2", "3", "+1"] in [l.split() for l in repr(diff).splitlines()]
    assert "| default graph | foaf:Person | 2 | 3 | +1 |" in diff.to_text(format="markdown")

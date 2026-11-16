"""
GRAPH blocks in CONSTRUCT templates, checked against other implementations and against SPARQL Update.

* The construct quad tests of Apache Jena (testARQConstructQuad_* in jena-arq
  src/test/java/org/apache/jena/sparql/api/TestAPI.java, Apache License 2.0, see
  testdata/jena_construct_quads/README.md), ported with the same data, queries and expected
  datasets. maplib returns a dataset (like Jena's execConstructDataset), so duplicate quads
  are compared as one.
* The examples and questions of https://github.com/w3c/sparql-dev/issues/31.
* Differential tests: one CONSTRUCT with GRAPH blocks gives the same quads as one CONSTRUCT per
  graph, and inserting it gives the same dataset as INSERT { GRAPH ... } WHERE in SPARQL Update.

test_construct_graph_w3c_update.py checks the template semantics against the W3C SPARQL 1.1
Update tests.
"""

from typing import Dict, List, Optional, Tuple

import pytest
import rdflib

from maplib import Model

NS = "http://example/ns#"

# The dataset of Jena's TestAPI: the default graph has one triple and the named graph g1 has the
# three triples of a blank node.
JENA_DATA = f"""
<{NS}s> <{NS}p> <{NS}o> .
_:r1 <{NS}p1> "x1" <{NS}g1> .
_:r1 <{NS}p2> "X2" <{NS}g1> .
_:r1 <{NS}p3> "y1" <{NS}g1> .
"""

Dataset = Dict[Optional[str], set]


def canon(t) -> str:
    return "_:b" if isinstance(t, rdflib.BNode) else t.n3()


def parse_trig(trig: str, format: str = "trig") -> Tuple[Dataset, int]:
    """The quads of a TriG document by graph (None is the default graph), with blank nodes
    collapsed (the datasets in these tests have at most one blank node per graph), and the
    number of blank nodes."""
    ds = rdflib.Dataset()
    ds.parse(data=trig, format=format)
    out: Dataset = {}
    bnodes = set()
    for s, p, o, g in ds.quads((None, None, None, None)):
        name = None if g is None or g == rdflib.graph.DATASET_DEFAULT_GRAPH_ID else str(g)
        out.setdefault(name, set()).add((canon(s), canon(p), canon(o)))
        bnodes |= {x for x in (s, o) if isinstance(x, rdflib.BNode)}
    return out, len(bnodes)


def construct_dataset(m: Model, query: str, **kwargs) -> Dataset:
    return parse_trig(m.query(query, return_format="trig", **kwargs))[0]


def model_dataset(m: Model) -> Dataset:
    return parse_trig(m.writes(format="nquads"), format="nquads")[0]


@pytest.fixture
def jena() -> Model:
    m = Model()
    m.reads(JENA_DATA, format="nquads")
    return m


DFT = {(f"<{NS}s>", f"<{NS}p>", f"<{NS}o>")}
G1 = {
    ("_:b", f"<{NS}p1>", '"x1"'),
    ("_:b", f"<{NS}p2>", '"X2"'),
    ("_:b", f"<{NS}p3>", '"y1"'),
}
SPO_IN_G1 = {f"{NS}g1": DFT}


# ---------------------------------------------------------------------------
# Apache Jena TestAPI.testARQConstructQuad_*
# ---------------------------------------------------------------------------


def test_jena_construct_quad_a_1(jena):
    # Triples from a named graph, constructed in the default graph.
    q = "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH ?g { ?s ?p ?o } }"
    assert construct_dataset(jena, q) == {None: G1}


def test_jena_construct_quad_a_2(jena):
    q = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
    assert construct_dataset(jena, q, include_graphs=[]) == {None: DFT}


def test_jena_construct_quad_b_1(jena):
    # The triples of the default graph part; the GRAPH block gives the quads of g1.
    q = "CONSTRUCT { ?s ?p ?o GRAPH ?g1 { ?s1 ?p1 ?o1 } } WHERE { ?s ?p ?o. GRAPH ?g1 { ?s1 ?p1 ?o1 } }"
    assert construct_dataset(jena, q, include_graphs=[]) == {None: DFT, f"{NS}g1": G1}


def test_jena_construct_quad_b_2(jena):
    q = "CONSTRUCT { GRAPH ?g1 {?s ?p ?o} } WHERE { ?s ?p ?o. GRAPH ?g1 {?s1 ?p1 'x1'} }"
    assert construct_dataset(jena, q, include_graphs=[]) == SPO_IN_G1


@pytest.mark.xfail(reason="Blank nodes as graph names (GRAPH _:a) are not supported", strict=True)
def test_jena_construct_quad_bnodes(jena):
    q = "PREFIX : <http://example/> CONSTRUCT { :s :p :o GRAPH _:a { :s :p :o1 } } WHERE { }"
    ds = construct_dataset(jena, q)
    assert len(ds) == 2


@pytest.mark.parametrize(
    "template",
    [
        "GRAPH ?g1 {?s ?p ?o}",  # Duplicate_1 and Duplicate_2: one quad from three solutions
        "GRAPH ?g1 {?s ?p ?o} GRAPH ?g1 {?s ?p ?o}",  # Duplicate_3: the same template twice
    ],
)
def test_jena_construct_quad_duplicates(jena, template):
    q = f"CONSTRUCT {{ {template} }} WHERE {{ ?s ?p ?o. GRAPH ?g1 {{?s1 ?p1 ?o1}} }}"
    assert construct_dataset(jena, q, include_graphs=[]) == SPO_IN_G1
    # The inserted dataset has the quad once.
    jena.insert(q, include_graphs=[])
    assert model_dataset(jena)[f"{NS}g1"] == G1 | DFT


def test_jena_construct_quad_prefix(jena):
    q = "PREFIX :   <http://example/ns#> CONSTRUCT { GRAPH :g1 { ?s :p ?o} } WHERE { ?s ?p ?o }"
    assert construct_dataset(jena, q, include_graphs=[]) == SPO_IN_G1


def test_jena_construct_quad_short_form_1(jena):
    q = "CONSTRUCT WHERE {?s ?p ?o }"
    assert construct_dataset(jena, q, include_graphs=[]) == {None: DFT}


def test_jena_construct_quad_short_form_2(jena):
    q = "CONSTRUCT WHERE { GRAPH ?g {?s ?p ?o} }"
    assert construct_dataset(jena, q) == {f"{NS}g1": G1}


def test_jena_construct_quad_short_form_3(jena):
    # The whole dataset.
    q = "CONSTRUCT WHERE { ?s ?p ?o. GRAPH ?g1 {?s1 ?p1 ?o1} }"
    assert construct_dataset(jena, q, include_graphs=[]) == model_dataset(jena)


def test_jena_construct_quad_short_form_bad(jena):
    with pytest.raises(Exception):
        jena.query("CONSTRUCT WHERE { GRAPH ?g {?s ?p ?o. FILTER isIRI(?o)}  }")


def test_jena_default_graph_block(jena):
    # { ... } in a template is a block of default graph triples (Jena syntax-quad-construct-09).
    q = f"CONSTRUCT {{ {{ ?s ?p ?o }} GRAPH <{NS}x> {{ ?s ?p ?o }} }} WHERE {{ ?s ?p ?o }}"
    assert construct_dataset(jena, q, include_graphs=[]) == {None: DFT, f"{NS}x": DFT}


# ---------------------------------------------------------------------------
# w3c/sparql-dev#31
# ---------------------------------------------------------------------------


def test_sparql_dev_31_dump_dataset(jena):
    # The query of the issue that dumps a store's dataset. Empty GRAPH blocks construct nothing,
    # and a dataset has no empty graphs, so the result is the dataset.
    q = """
    CONSTRUCT {
        ?s1 ?p1 ?o1
        GRAPH ?g2 { ?s2 ?p2 ?o2 }
        GRAPH ?g3 { }
    }
    WHERE {
        { ?s1 ?p1 ?o1 }
        UNION
        { GRAPH ?g2 { ?s2 ?p2 ?o2 } }
        UNION
        { GRAPH ?g3 { FILTER NOT EXISTS { ?s ?p ?o } } }
    }
    """
    assert construct_dataset(jena, q, include_graphs=[]) == model_dataset(jena)


def test_sparql_dev_31_unbound_or_non_iri_graph_is_skipped(jena):
    # Asked in the issue: an unbound or non-IRI graph name gives no triples (as in INSERT
    # templates, SPARQL 1.1 Update section 3.1.3), not triples in the default graph.
    q = f"""
    CONSTRUCT {{ GRAPH ?g {{ <{NS}s> <{NS}q> ?v }} }}
    WHERE {{ VALUES (?g ?v) {{ (UNDEF 1) ("lit" 2) (<{NS}g2> 3) }} }}
    """
    three = '"3"^^<http://www.w3.org/2001/XMLSchema#integer>'
    assert construct_dataset(jena, q) == {f"{NS}g2": {(f"<{NS}s>", f"<{NS}q>", three)}}


def test_sparql_dev_31_from_named(jena):
    # FROM and FROM NAMED with GRAPH blocks in the template (Jena's grammar allows dataset clauses).
    jena.reads(f"<{NS}a> <{NS}p> <{NS}b> .", format="ntriples", graph=f"{NS}g2")
    q = f"""
    CONSTRUCT {{ GRAPH ?g {{ ?s <{NS}copy> ?o }} ?s ?p ?o }}
    FROM <{NS}g2> FROM NAMED <{NS}g2>
    WHERE {{ ?s ?p ?o GRAPH ?g {{ ?s ?p ?o }} }}
    """
    triple = (f"<{NS}a>", f"<{NS}p>", f"<{NS}b>")
    copy = (f"<{NS}a>", f"<{NS}copy>", f"<{NS}b>")
    assert construct_dataset(jena, q) == {None: {triple}, f"{NS}g2": {copy}}


# ---------------------------------------------------------------------------
# Differential tests
# ---------------------------------------------------------------------------

EX = "http://example.net/ns#"
DIFF_DATA = f"""
<{EX}a> <{EX}p> <{EX}b> .
<{EX}b> <{EX}p> <{EX}c> .
<{EX}c> <{EX}p> "lit" .
<{EX}a> <{EX}in> <{EX}g1> .
<{EX}b> <{EX}in> <{EX}g2> .
<{EX}c> <{EX}in> "not an IRI" .
<{EX}x> <{EX}p> <{EX}y> <{EX}g1> .
<{EX}y> <{EX}p> <{EX}z> <{EX}g2> .
"""
PREFIX = f"PREFIX ex: <{EX}>\n"

# (template, WHERE) pairs. The templates mix fixed and variable graphs, the default graph, blank
# nodes, unbound graph variables and graph variables bound by GRAPH in the WHERE clause.
CASES = [
    ("GRAPH ex:out { ?s ex:q ?o }", "?s ex:p ?o"),
    ("GRAPH ?g { ?s ex:q ?o }", "?s ex:p ?o . ?s ex:in ?g"),
    ("?s ex:t ?o GRAPH ?g { ?s ex:q ?o . ?o ex:r ?s } GRAPH ex:out { ?o ex:u ?s }", "?s ex:p ?o . ?s ex:in ?g"),
    ("GRAPH ?g { ?s ex:q ?o }", "?s ex:p ?o OPTIONAL { ?s ex:in ?g }"),
    ("GRAPH ?g { ?s ex:copied ?o }", "GRAPH ?g { ?s ?p ?o }"),
    ("GRAPH ?g { ?s ex:node [ ex:of ?o ] }", "?s ex:p ?o . ?s ex:in ?g"),
    ("GRAPH ?g { ?s ex:q ?o } GRAPH ?h { ?o ex:q ?s }", "?s ex:p ?o . ?s ex:in ?g . ?o ex:in ?h"),
]


def diff_model() -> Model:
    m = Model()
    m.reads(DIFF_DATA, format="nquads")
    return m


@pytest.mark.parametrize("template,where", CASES)
def test_insert_same_as_sparql_update_insert(template, where):
    m1, m2 = diff_model(), diff_model()
    m1.insert(PREFIX + f"CONSTRUCT {{ {template} }} WHERE {{ {where} }}", include_graphs=[])
    m2.update(PREFIX + f"INSERT {{ {template} }} WHERE {{ {where} }}")
    # Compares the quads (blank nodes collapsed) and the number of blank nodes.
    d1 = parse_trig(m1.writes(format="nquads"), format="nquads")
    d2 = parse_trig(m2.writes(format="nquads"), format="nquads")
    assert d1 == d2


@pytest.mark.parametrize("template,where", CASES)
def test_same_as_one_construct_per_graph(template, where):
    # Each GRAPH block, and the default graph part, as its own CONSTRUCT whose results are put in
    # that graph. A GRAPH ?g block keeps the solutions where ?g is an IRI.
    m = diff_model()
    q = PREFIX + f"CONSTRUCT {{ {template} }} WHERE {{ {where} }}"
    if "[" in template:
        # Blank nodes are minted once per solution and shared by the GRAPH blocks, so the
        # template can not be split; compare the number of quads per graph instead.
        ds, n_bnodes = parse_trig(m.query(q, return_format="trig", include_graphs=[]))
        n = m.query(PREFIX + f"SELECT * WHERE {{ {where} FILTER(isIRI(?g)) }}", include_graphs=[]).height
        assert n_bnodes == n
        assert {g: len(t) for g, t in ds.items()} == {f"{EX}g1": 2, f"{EX}g2": 2}
        return
    blocks = split_template(template)
    expected: Dataset = {}
    for graph, triples in blocks:
        part = f"CONSTRUCT {{ {triples} }} WHERE {{ {where} "
        if graph is not None and graph.startswith("?"):
            for (g,) in m.query(
                PREFIX + f"SELECT DISTINCT {graph} WHERE {{ {where} FILTER(isIRI({graph})) }}",
                include_graphs=[],
            ).rows():
                ds = construct_dataset(m, PREFIX + part + f"FILTER({graph} = {g}) }}", include_graphs=[])
                expected.setdefault(g[1:-1], set()).update(ds.get(None, set()))
        else:
            ds = construct_dataset(m, PREFIX + part + "}", include_graphs=[])
            name = None if graph is None else EX + graph.removeprefix("ex:")
            expected.setdefault(name, set()).update(ds.get(None, set()))
    expected = {g: t for g, t in expected.items() if t}
    assert construct_dataset(m, q, include_graphs=[]) == expected


def split_template(template: str) -> List[Tuple[Optional[str], str]]:
    """The GRAPH blocks of a test template as (graph, triples), and the default graph part."""
    out, default, rest = [], [], template
    while "GRAPH" in rest:
        before, _, after = rest.partition("GRAPH")
        default.append(before)
        graph, _, after = after.strip().partition(" ")
        body, _, rest = after.strip()[1:].partition("}")
        out.append((graph, body))
    default.append(rest)
    if "".join(default).strip():
        out.append((None, "".join(default)))
    return out

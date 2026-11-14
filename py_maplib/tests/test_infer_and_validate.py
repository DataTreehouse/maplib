"""Coverage for Model.infer (Datalog) and Model.validate (SHACL) options.
Both features are only available in some builds, so the tests skip when they are not."""

import polars as pl
import pytest

from maplib import Model, MaplibException

EX = "http://example.net/ns#"
G1 = "urn:graph:one"
SHAPES = "urn:graph:shapes"
REPORT = "urn:graph:report"

DATA = f"""
@prefix ex: <{EX}> .
ex:a ex:parent ex:b .
ex:b ex:parent ex:c .
ex:c ex:parent ex:d .
"""

ANCESTOR = """
PREFIX ex:<http://example.net/ns#>
[?x, ex:ancestor, ?y] :- [?x, ex:parent, ?y] .
[?x, ex:ancestor, ?z] :- [?x, ex:ancestor, ?y], [?y, ex:parent, ?z] .
"""

GRANDPARENT = """
PREFIX ex:<http://example.net/ns#>
[?x, ex:grandparent, ?z] :- [?x, ex:parent, ?y], [?y, ex:parent, ?z] .
"""


def _unavailable(e: BaseException) -> bool:
    return "Contact Data Treehouse" in str(e)


def _infer_or_skip(m, *args, **kwargs):
    try:
        return m.infer(*args, **kwargs)
    except BaseException as e:
        if _unavailable(e):
            pytest.skip("Datalog inference is not available in this build")
        raise


def _validate_or_skip(m, **kwargs):
    try:
        return m.validate(**kwargs)
    except BaseException as e:
        if _unavailable(e):
            pytest.skip("SHACL validation is not available in this build")
        raise


def _count(m, pred, graph=None):
    return m.query(
        f"SELECT ?s ?o WHERE {{ ?s <{EX}{pred}> ?o }}", graph=graph
    ).height


# --- Datalog -------------------------------------------------------------------------


def test_infer_recursive_rule():
    m = Model()
    m.reads(DATA, format="turtle")
    _infer_or_skip(m, ANCESTOR)
    # 3 + 2 + 1 ancestor pairs
    assert _count(m, "ancestor") == 6


def test_infer_list_of_rulesets():
    m = Model()
    m.reads(DATA, format="turtle")
    _infer_or_skip(m, [ANCESTOR, GRANDPARENT])
    assert _count(m, "ancestor") == 6
    assert _count(m, "grandparent") == 2


def test_infer_in_named_graph():
    m = Model()
    m.reads(DATA, format="turtle", graph=G1)
    _infer_or_skip(m, GRANDPARENT, graph=G1)
    assert _count(m, "grandparent", graph=G1) == 2
    assert _count(m, "grandparent") == 0


def test_infer_bad_ruleset_type_raises():
    m = Model()
    with pytest.raises(Exception):
        m.infer(123)


def test_infer_syntax_error_raises():
    m = Model()
    m.reads(DATA, format="turtle")
    with pytest.raises(BaseException) as e:
        m.infer("this is not datalog :- ")
    if _unavailable(e.value):
        pytest.skip("Datalog inference is not available in this build")


# --- SHACL ---------------------------------------------------------------------------

SHAPE_TTL = f"""
@prefix ex: <{EX}> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [
        sh:path ex:name ;
        sh:minCount 1 ;
        sh:datatype xsd:string ;
    ] .

ex:AgeShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [
        sh:path ex:age ;
        sh:maxCount 1 ;
        sh:datatype xsd:integer ;
    ] .
"""

CONFORMING = f"""
@prefix ex: <{EX}> .
ex:alice a ex:Person ; ex:name "Alice" ; ex:age 30 .
ex:bob a ex:Person ; ex:name "Bob" .
"""

NON_CONFORMING = f"""
@prefix ex: <{EX}> .
ex:alice a ex:Person ; ex:name "Alice" ; ex:age 30, 31 .
ex:bob a ex:Person .
"""


def _shacl_model(data):
    m = Model()
    m.reads(SHAPE_TTL, format="turtle", graph=SHAPES)
    m.reads(data, format="turtle")
    return m


def test_validate_conforms():
    m = _shacl_model(CONFORMING)
    report = _validate_or_skip(m, shape_graph=SHAPES)
    assert report.conforms is True
    results = report.results()
    assert results is None or results.height == 0


def test_validate_does_not_conform():
    m = _shacl_model(NON_CONFORMING)
    report = _validate_or_skip(m, shape_graph=SHAPES)
    assert report.conforms is False
    results = report.results()
    assert results is not None
    # bob lacks a name, alice has two ages
    assert results.height == 2
    assert isinstance(report.shape_targets, pl.DataFrame)
    assert isinstance(report.performance, pl.DataFrame)


def test_validate_only_shapes_and_deactivate_shapes():
    m = _shacl_model(NON_CONFORMING)
    report = _validate_or_skip(m, shape_graph=SHAPES, only_shapes=[EX + "AgeShape"])
    assert report.conforms is False
    assert report.results().height == 1

    report = _validate_or_skip(
        m, shape_graph=SHAPES, deactivate_shapes=[EX + "AgeShape"]
    )
    assert report.conforms is False
    assert report.results().height == 1

    report = _validate_or_skip(
        m,
        shape_graph=SHAPES,
        deactivate_shapes=[EX + "AgeShape", EX + "PersonShape"],
    )
    assert report.conforms is True


def test_validate_only_and_deactivate_together_raises():
    m = _shacl_model(CONFORMING)
    with pytest.raises(BaseException) as e:
        m.validate(
            shape_graph=SHAPES,
            only_shapes=[EX + "AgeShape"],
            deactivate_shapes=[EX + "PersonShape"],
        )
    assert "only_shapes" in str(e.value) or _unavailable(e.value)


def test_validate_report_graph():
    m = _shacl_model(NON_CONFORMING)
    report = _validate_or_skip(m, shape_graph=SHAPES, report_graph=REPORT)
    assert report.conforms is False
    assert report.report_graph == REPORT
    df = m.query(
        """
        PREFIX sh: <http://www.w3.org/ns/shacl#>
        SELECT ?r WHERE { ?r a sh:ValidationResult }
        """,
        graph=REPORT,
    )
    assert df.height == 2


OR_SHAPE_TTL = f"""
@prefix ex: <{EX}> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:IdShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [
        sh:path ex:id ;
        sh:or ( [ sh:datatype xsd:string ] [ sh:datatype xsd:integer ] ) ;
    ] .
"""

OR_DATA = f"""
@prefix ex: <{EX}> .
ex:alice a ex:Person ; ex:id "a1" .
ex:bob a ex:Person ; ex:id 2 .
ex:carol a ex:Person ; ex:id 3.5e0 .
"""


def test_validate_plain_constraints_have_no_details():
    m = _shacl_model(NON_CONFORMING)
    report = _validate_or_skip(m, shape_graph=SHAPES, include_details=True)
    # Details are the sub-results of logical constraints (sh:or, sh:and, ...)
    assert report.details() is None


def test_validate_include_details_for_logical_constraints():
    m = Model()
    m.reads(OR_SHAPE_TTL, format="turtle", graph=SHAPES)
    m.reads(OR_DATA, format="turtle")
    report = _validate_or_skip(m, shape_graph=SHAPES, include_details=True)
    assert report.conforms is False
    assert report.results().height == 1
    details = report.details()
    assert details is not None
    assert details.height > 0

    report_all = _validate_or_skip(
        m, shape_graph=SHAPES, include_details=True, include_conforms=True
    )
    assert report_all.results().height >= 3
    assert report_all.details().height >= details.height

    report_none = _validate_or_skip(m, shape_graph=SHAPES)
    assert report_none.details() is None


def test_validate_dry_run_counts_targets():
    m = _shacl_model(NON_CONFORMING)
    report = _validate_or_skip(m, shape_graph=SHAPES, dry_run=True)
    assert report.shape_targets.height > 0


def test_validate_serial_matches_parallel():
    m = _shacl_model(NON_CONFORMING)
    r1 = _validate_or_skip(m, shape_graph=SHAPES, serial=True)
    r2 = _validate_or_skip(m, shape_graph=SHAPES)
    assert r1.conforms == r2.conforms
    assert r1.results().height == r2.results().height

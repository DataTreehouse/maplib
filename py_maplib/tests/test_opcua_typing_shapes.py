"""m.opcua.add_typing_shapes: SHACL shapes generated from the type definitions of OPC UA NodeSet2 files.
Mapping NodeSet2 files and SHACL validation are only available in some builds (maplib Enterprise
Edition), so the tests skip when they are not.
Set MAPLIB_REGENERATE_OPCUA_EXPECTED=1 to rewrite the expected CSVs."""

import os
import pathlib
import shutil

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from maplib import Model

pl.Config.set_tbl_rows(50)
pl.Config.set_fmt_str_lengths(200)

PATH_HERE = pathlib.Path(__file__).parent.resolve()
TESTDATA_PATH = PATH_HERE / "testdata" / "opcua"
BASE_NODESET = TESTDATA_PATH / "Opc.Ua.NodeSet2.xml"
INSTANCE_NS = "urn:DataTreehouse:uamodel"
SHAPES = "urn:maplib:opcua_shapes"
REPORT = "urn:maplib:opcua_report"
REGENERATE = os.environ.get("MAPLIB_REGENERATE_OPCUA_EXPECTED") == "1"

PREFIXES = """
PREFIX sh: <http://www.w3.org/ns/shacl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX ua: <http://opcfoundation.org/UA/>
PREFIX uamapper: <https://github.com/DataTreehouse/uamapper#>
PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#>
"""

REPORT_QUERY = """
    PREFIX sh: <http://www.w3.org/ns/shacl#>
    SELECT ?focus_node ?result_path ?result_severity ?source_constraint_component ?source_shape ?message ?value WHERE {
    ?report a sh:ValidationReport .
    ?report sh:conforms ?conforms .
    ?report sh:result ?result .
    ?result a sh:ValidationResult .
    ?result sh:focusNode ?focus_node .
    ?result sh:resultSeverity ?result_severity .
    ?result sh:sourceConstraintComponent ?source_constraint_component .
    ?result sh:sourceShape ?source_shape .
    OPTIONAL { ?result sh:resultPath ?result_path . }
    OPTIONAL { ?result sh:resultMessage ?message . }
    OPTIONAL { ?result sh:value ?value. }
    }
"""


def _unavailable(e: BaseException) -> bool:
    return "Contact Data Treehouse" in str(e)


def _mapped(name: str, tmp_path: pathlib.Path) -> Model:
    """A model with the nodesets of testdata/opcua/<name> and the OPC UA base nodeset."""
    folder = tmp_path / name
    shutil.copytree(TESTDATA_PATH / name, folder)
    shutil.copy(BASE_NODESET, folder)
    m = Model()
    try:
        m.map_opc_ua(folder)
    except BaseException as e:
        if _unavailable(e):
            pytest.skip("OPC UA mapping is not available in this build")
        raise
    return m


def _validate_report(m: Model) -> pl.DataFrame:
    try:
        m.validate(shape_graph=SHAPES, report_graph=REPORT)
    except BaseException as e:
        if _unavailable(e):
            pytest.skip("SHACL validation is not available in this build")
        raise
    return m.query(REPORT_QUERY, graph=REPORT).sort(
        ["focus_node", "source_shape", "message"]
    )


def _check_expected(df: pl.DataFrame, name: str):
    file_path = TESTDATA_PATH / name
    if REGENERATE:
        df.write_csv(file_path)
    expected_df = pl.read_csv(file_path)
    assert df.get_column("message").to_list() == expected_df.get_column("message").to_list()
    assert_frame_equal(df, expected_df, check_dtypes=False)


def _shapes(m: Model) -> pl.DataFrame:
    return m.query(
        PREFIXES
        + """
        SELECT ?s ?shape_type ?cat ?label ?deactivated WHERE {
            ?s a ?shape_type .
            FILTER(?shape_type IN (sh:NodeShape, sh:PropertyShape))
            OPTIONAL { ?s maplib:shapeCategory ?cat . }
            OPTIONAL { ?s rdfs:label ?label . }
            OPTIONAL { ?s sh:deactivated ?deactivated . }
        }""",
        graph=SHAPES,
    )


def test_generated_shapes(tmp_path):
    m = _mapped("basics", tmp_path)
    m.opcua.add_typing_shapes(shapes_graph=SHAPES, instance_namespaces=[INSTANCE_NS])
    shapes = _shapes(m)
    assert shapes.height > 0
    # Every generated shape is a typing shape
    assert shapes.filter(pl.col("cat").is_null()).height == 0
    assert set(shapes.get_column("cat").to_list()) == {
        "<https://github.com/DataTreehouse/uamapper#OPCUATypingValidation>"
    }

    # The category and its scheme are described in the shapes graph
    cat = m.query(
        PREFIXES
        + """
        SELECT ?cat_label ?scheme_label WHERE {
            uamapper:OPCUATypingValidation rdfs:label ?cat_label ;
                skos:inScheme ?scheme .
            ?scheme rdfs:label ?scheme_label .
        }""",
        graph=SHAPES,
    )
    assert cat.rows() == [("OPC UA Typing Validation", "OPC UA Validation")]

    # TwoStateDiscreteType has TrueState and FalseState as Mandatory properties
    labels = shapes.get_column("label").to_list()
    assert 'TwoStateDiscreteType browse path: "TrueState" mandatory child exists' in labels
    assert 'TwoStateDiscreteType browse path: "FalseState" mandatory child exists' in labels

    # The type shape of TwoStateDiscreteType is active since it has instances, others are not
    active = shapes.filter(
        (pl.col("shape_type") == "<http://www.w3.org/ns/shacl#NodeShape>")
        & pl.col("deactivated").not_()
    )
    assert "TwoStateDiscreteType validation" in active.get_column("label").to_list()
    assert "AnalogItemType validation" not in active.get_column("label").to_list()

    # Helper triples
    type_ns = m.query(
        PREFIXES + "SELECT ?ns ?t WHERE { ?ns uamapper:typeNamespace ?t }"
    )
    assert dict(type_ns.rows()) == {
        f"<{INSTANCE_NS}>": False,
        "<http://opcfoundation.org/UA/>": True,
        # Declared by the base nodeset for engineering units
        "<http://www.opcfoundation.org/UA/units/un/cefact>": True,
    }


def test_namespaces_arguments(tmp_path):
    m = _mapped("basics", tmp_path)
    with pytest.raises(ValueError):
        m.opcua.add_typing_shapes(
            type_namespaces=["http://opcfoundation.org/UA/"],
            instance_namespaces=[INSTANCE_NS],
        )
    m.opcua.add_typing_shapes(
        shapes_graph=SHAPES, type_namespaces=["http://opcfoundation.org/UA/"]
    )
    type_ns = m.query(
        PREFIXES + "SELECT ?ns ?t WHERE { ?ns uamapper:typeNamespace ?t }"
    )
    assert dict(type_ns.rows())[f"<{INSTANCE_NS}>"] is False


def test_basics(tmp_path):
    m = _mapped("basics", tmp_path)
    m.opcua.add_typing_shapes(shapes_graph=SHAPES, instance_namespaces=[INSTANCE_NS])
    df = _validate_report(m)
    _check_expected(df, "expected_basics.csv")
    focus = set(df.get_column("focus_node").to_list())
    assert f"<{INSTANCE_NS}#s%3DMissingStates>" in focus
    assert f"<{INSTANCE_NS}#s%3DWellFormed>" not in focus


def test_basics_derived_type_namespaces(tmp_path):
    # Without namespace arguments, a namespace holds types when it has type nodes
    m = _mapped("basics", tmp_path)
    m.opcua.add_typing_shapes(shapes_graph=SHAPES)
    _check_expected(_validate_report(m), "expected_basics.csv")


def test_61850_fully_inherited(tmp_path):
    m = _mapped("61850", tmp_path)
    m.opcua.add_typing_shapes(shapes_graph=SHAPES, instance_namespaces=[INSTANCE_NS])
    _check_expected(_validate_report(m), "expected_fully_inherited_61850.csv")


def test_own_shapes_use_helper_triples(tmp_path):
    m = _mapped("61850", tmp_path)
    m.opcua.add_typing_shapes(shapes_graph=SHAPES, instance_namespaces=[INSTANCE_NS])
    m.reads(
        """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix uamapper: <https://github.com/DataTreehouse/uamapper#> .
@prefix ex: <urn:example:shapes#> .
ex:StringNodeIds a sh:NodeShape ;
    sh:targetSubjectsOf uamapper:typeNamespace ;
    sh:sparql [
        a sh:SPARQLConstraint ;
        sh:select \"\"\"
        PREFIX ua:<http://opcfoundation.org/UA/>
        PREFIX uamapper: <https://github.com/DataTreehouse/uamapper#>
        SELECT ?this ?value (CONCAT("The node id ", ?node_id, " is not a string") AS ?message) WHERE {
            ?this uamapper:typeNamespace false .
            ?value ua:Namespace ?this .
            ?value ua:NodeId ?node_id .
            FILTER(!STRSTARTS(?node_id, "s="))
        }\"\"\" ;
    ] .
""",
        format="turtle",
        graph=SHAPES,
    )
    df = _validate_report(m)
    own = df.filter(pl.col("source_shape") == "<urn:example:shapes#StringNodeIds>")
    assert sorted(own.get_column("message").to_list()) == [
        "The node id i=456 is not a string",
        "The node id i=567 is not a string",
        "The node id i=678 is not a string",
    ]

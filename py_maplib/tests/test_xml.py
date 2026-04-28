import pathlib

import pytest

from .disk import disk_params
from maplib import Model

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata" / "xmls"


@pytest.mark.parametrize("disk", disk_params())
def test_map_xml_1(disk):
    xml_1 = TESTDATA_PATH / "1.xml"
    m = Model(storage_folder=disk)
    m.map_xml(str(xml_1))

    df = m.query("""SELECT * WHERE {?s ?p ?o}""")
    assert df.height > 0

    df2 = m.query(
        """
        PREFIX fx:  <http://sparql.xyz/facade-x/ns/>
        PREFIX xyz: <http://sparql.xyz/facade-x/data/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

        SELECT ?title WHERE {
            ?root a fx:root .
            ?root rdf:_1 ?glossary .
            ?glossary a xyz:glossary .
            ?glossary rdf:_1 ?title_node .
            ?title_node a xyz:title .
            ?title_node rdf:_1 ?title .
        }
        """
    )
    assert df2.height == 1
    assert df2.get_column("title")[0] == "example glossary"


@pytest.mark.parametrize("disk", disk_params())
def test_map_xml_2_repeated_children(disk):
    xml_2 = TESTDATA_PATH / "2.xml"
    m = Model(storage_folder=disk)
    m.map_xml(str(xml_2))

    df_items = m.query(
        """
        PREFIX fx:  <http://sparql.xyz/facade-x/ns/>
        PREFIX xyz: <http://sparql.xyz/facade-x/data/>

        SELECT ?items WHERE {
            ?root a fx:root .
            ?root ?p1 ?menu .
            ?menu a xyz:menu .
            ?menu ?p2 ?items .
            ?items a xyz:items .
        }
        """
    )
    assert df_items.height == 22


def test_map_xml_3_inline_string():
    xml = """<?xml version="1.0"?>
<root xmlns="http://example.org/ns/">
    <name>abc</name>
    <name>def</name>
</root>"""
    m = Model()
    m.map_xml(xml)

    df = m.query(
        """
        PREFIX fx: <http://sparql.xyz/facade-x/ns/>
        PREFIX ex: <http://example.org/ns/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

        SELECT ?val WHERE {
            ?root a fx:root .
            ?root ?p ?name .
            ?name a ex:name .
            ?name rdf:_1 ?val .
        }
        ORDER BY ?val
        """
    )
    assert df.height == 2
    values = sorted(df.get_column("val").to_list())
    assert values == ["abc", "def"]


def test_map_xml_attributes():
    xml = (
        '<?xml version="1.0"?>'
        '<root xmlns:ex="http://example.org/">'
        '<item ex:id="42" name="hello"/>'
        "</root>"
    )
    m = Model()
    m.map_xml(xml)

    df = m.query(
        """
        PREFIX fx: <http://sparql.xyz/facade-x/ns/>
        PREFIX xyz: <http://sparql.xyz/facade-x/data/>
        PREFIX ex: <http://example.org/>

        SELECT ?id ?name WHERE {
            ?root a fx:root .
            ?root ?p ?item .
            ?item a xyz:item .
            ?item ex:id ?id .
            ?item xyz:name ?name .
        }
        """
    )
    assert df.height == 1
    assert df.get_column("id")[0] == "42"
    assert df.get_column("name")[0] == "hello"

from maplib import Model

gr1 = """
<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "A" .
"""

gr2 = """
<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "B" . 
"""


def test_basic_named_graph():
    ng2 = "urn:graph:gr2"
    m = Model()
    m.reads(gr1, format="ntriples")
    m.reads(gr2, format="ntriples", graph=ng2)

    df = m.query("""
    SELECT * WHERE {
        GRAPH <urn:graph:gr2> {
            ?a ?b ?c .
        }
    }    
    """)
    assert df.get_column("c")[0] == "B"

def test_basic_named_graph_2():
    ng1 = "urn:graph:gr1"
    ng2 = "urn:graph:gr2"
    m = Model()
    m.reads(gr1, format="ntriples", graph=ng1)
    m.reads(gr2, format="ntriples", graph=ng2)

    df = m.query("""
    SELECT * WHERE {
        GRAPH <urn:graph:gr1> {
            ?a ?b1 ?c1 .
        }
        GRAPH <urn:graph:gr2> {
            ?a ?b2 ?c2 .
        }
    }    
    """)
    assert df.get_column("c2")[0] == "B"
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
    assert df.height == 1
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
    assert df.height == 1
    assert df.get_column("c2")[0] == "B"

def test_detach_graph():
    ng1 = "urn:graph:gr1"
    ng2 = "urn:graph:gr2"
    m = Model()
    m.reads(gr1, format="ntriples", graph=ng1)
    m.reads(gr2, format="ntriples", graph=ng2)
    m2 = m.detach_graph(ng2)

    df = m.query("""
    SELECT * WHERE {
        GRAPH <urn:graph:gr2> {
            ?a ?b2 ?c2 .
        }
    }    
    """)
    assert df.height == 0

    df = m.query("""
    SELECT * WHERE {
        GRAPH <urn:graph:gr1> {
            ?a ?b1 ?c1 .
        }
    }    
    """)
    assert df.height == 1


    df2 = m2.query("""
    SELECT * WHERE {
        GRAPH <urn:graph:gr2> {
            ?a ?b2 ?c2 .
        }
    }    
    """)
    assert df2.height == 1
    assert df2.get_column("c2")[0] == "B"

    df2 = m2.query("""
    SELECT * WHERE {
        GRAPH <urn:graph:gr1> {
            ?a ?b1 ?c1 .
        }
    }    
    """)
    assert df2.height == 0

def test_detach_default_graph():
    ng1 = "urn:graph:gr1"
    m = Model()
    m.reads(gr1, format="ntriples", graph=ng1)
    m.reads(gr2, format="ntriples")
    m2 = m.detach_graph()

    df = m.query("""
    SELECT * WHERE {
        ?a ?b2 ?c2 .
    }    
    """)
    assert df.height == 0

    df = m.query("""
    SELECT * WHERE {
        GRAPH <urn:graph:gr1> {
            ?a ?b1 ?c1 .
        }
    }    
    """)
    assert df.height == 1


    df2 = m2.query("""
    SELECT * WHERE {
        ?a ?b2 ?c2 .
    }    
    """)
    assert df2.height == 1
    assert df2.get_column("c2")[0] == "B"

    df2 = m2.query("""
    SELECT * WHERE {
        GRAPH <urn:graph:gr1> {
            ?a ?b1 ?c1 .
        }
    }    
    """)
    assert df2.height == 0
    m.writes(format="ntriples")

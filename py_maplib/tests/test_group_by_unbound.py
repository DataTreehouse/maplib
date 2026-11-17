from maplib import Model

TTL = """
@prefix ex: <http://example.net/> .
ex:o1 ex:product ex:p1 ; ex:price 10 .
ex:o2 ex:product ex:p1 ; ex:price 20 .
ex:o3 ex:product ex:p2 ; ex:price 5 .
"""


def model():
    m = Model()
    m.read(TTL.encode(), format="turtle")
    return m


def test_group_by_variable_not_in_pattern():
    # ?country is not bound by the pattern: it groups as an unbound key (as in BSBM BI Q4).
    df = model().query("""
    PREFIX ex: <http://example.net/>
    SELECT ?country ?product (AVG(?price) AS ?avg) WHERE {
        ?o ex:product ?product ; ex:price ?price .
    } GROUP BY ?country ?product ORDER BY ?product
    """)
    assert df.height == 2
    assert df.get_column("country").null_count() == 2
    assert df.get_column("avg").cast(float).to_list() == [15.0, 5.0]


def test_group_by_only_unbound_variable():
    df = model().query("""
    PREFIX ex: <http://example.net/>
    SELECT ?country (COUNT(?o) AS ?n) WHERE { ?o ex:product ?product . } GROUP BY ?country
    """)
    assert df.height == 1
    assert df.get_column("n").to_list() == [3]


def test_group_by_unbound_joined_with_bound():
    # The unbound ?country of the sub-select joins with any ?country on the other side.
    df = model().query("""
    PREFIX ex: <http://example.net/>
    SELECT ?country ?product ?avg WHERE {
        VALUES ?country { "NO" }
        { SELECT ?country ?product (AVG(?price) AS ?avg) WHERE {
            ?o ex:product ?product ; ex:price ?price .
          } GROUP BY ?country ?product }
    } ORDER BY ?product
    """)
    assert df.height == 2
    assert df.get_column("country").to_list() == ["NO", "NO"]

import pytest
from maplib.maplib import MaplibException
from maplib import Model
from pathlib import Path
import polars as pl

PATH_HERE = Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata" / "w3c_sparql" / "aggregate"


def test_count1():
    m = Model()
    m.read(TESTDATA_PATH / "agg01.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org>

    SELECT (COUNT(?O) AS ?C)
    WHERE { ?S ?P ?O }
    """)

    expected = pl.DataFrame({"C": 5})

    print(df)
    print(expected)

    assert df.equals(expected)


def test_count2():
    m = Model()
    m.read(TESTDATA_PATH / "agg01.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org>

    SELECT ?P (COUNT(?O) AS ?C)
    WHERE { ?S ?P ?O }
    GROUP BY ?P
    """)

    expected = pl.DataFrame({
        "P": ["<http://www.example.org/p1>", "<http://www.example.org/p2>"],
        "C": [3, 2],
    })

    assert df.sort("C").equals(expected.sort("C"))


def test_count3():
    m = Model()
    m.read(TESTDATA_PATH / "agg01.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org>

    SELECT ?P (COUNT(?O) AS ?C)
    WHERE { ?S ?P ?O }
    GROUP BY ?P
    HAVING (COUNT(?O) > 2 )
    """)

    expected = pl.DataFrame({
        "P": "<http://www.example.org/p1>",
        "C": 3
    })

    print(df)
    print(expected)

    assert df.equals(expected)


def test_count4():
    m = Model()
    m.read(TESTDATA_PATH / "agg01.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org>

    SELECT (COUNT(*) AS ?C)
    WHERE { ?S ?P ?O }
    """)

    expected = pl.DataFrame({"C": 5})

    print(df)
    print(expected)

    assert df.equals(expected)


def test_count5():
    m = Model()
    m.read(TESTDATA_PATH / "agg01.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org>

    SELECT ?P (COUNT(*) AS ?C)
    WHERE { ?S ?P ?O }
    GROUP BY ?P
    """)

    expected = pl.DataFrame({
        "P": ["<http://www.example.org/p1>", "<http://www.example.org/p2>"],
        "C": [3, 2]
    })

    print(df)
    print(expected)

    assert df.sort("C").equals(expected.sort("C"))


def test_count6():
    m = Model()
    m.read(TESTDATA_PATH / "agg01.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org>

    SELECT (COUNT(*) AS ?C)
    WHERE { ?S ?P ?O }
    HAVING (COUNT(*) > 0 )
    """)

    expected = pl.DataFrame({"C": 5})

    print(df)
    print(expected)

    assert df.equals(expected)


def test_count7():
    m = Model()
    m.read(TESTDATA_PATH / "agg01.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org>

    SELECT ?P (COUNT(*) AS ?C)
    WHERE { ?S ?P ?O }
    GROUP BY ?P
    HAVING ( COUNT(*) > 2 )
    """)

    expected = pl.DataFrame({
        "P": "<http://www.example.org/p1>",
        "C": 3
    })

    print(df)
    print(expected)

    assert df.equals(expected)


def test_count8():
    m = Model()

    with pytest.raises(MaplibException) as e:
        m.query("""
        PREFIX : <http://www.example.org/>
    
        SELECT ((?O1 + ?O2) AS ?O12) (COUNT(?O1) AS ?C)
        WHERE { ?S :p ?O1; :q ?O2 } GROUP BY (?O1 + ?O2)
        ORDER BY ?O12
        """)

    assert "Variable ?O1 not found in context " in str(e.value)


def test_count08b():
    m = Model()
    m.read(TESTDATA_PATH / "agg08.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>

   SELECT ?O12 (COUNT(?O1) AS ?C)
   WHERE { ?S :p ?O1; :q ?O2 } GROUP BY ((?O1 + ?O2) AS ?O12)
   ORDER BY ?O12
    """)

    expected = pl.DataFrame({
        "O12": [0, 1, 2, 3, 4],
        "C": [1, 2, 3, 2, 1]
    })

    print(df)
    print(expected)

    assert df.equals(expected)


def test_count9():
    m = Model()

    with pytest.raises(MaplibException) as e:
        m.query("""
        PREFIX : <http://www.example.org/>
        
        SELECT ?P (COUNT(?O) AS ?C)
        WHERE { ?S ?P ?O } GROUP BY ?S
        """)

    assert "SPARQL parsing error: " in str(e.value)


def test_count10():
    m = Model()

    with pytest.raises(MaplibException) as e:
        m.query("""
        PREFIX : <http://www.example.org/>
        
        SELECT ?P (COUNT(?O) AS ?C)
        WHERE { ?S ?P ?O }
        """)
    assert "SPARQL parsing error: " in str(e.value)


def test_count11():
    m = Model()

    with pytest.raises(MaplibException) as e:
        m.query("""
        PREFIX : <http://www.example.org/>
    
        SELECT ((?O1 + ?O2) AS ?O12) (COUNT(?O1) AS ?C)
        WHERE { ?S :p ?O1; :q ?O2 } GROUP BY (?S)
        """)
    assert "Variable ?O1 not found in context " in str(e.value)


def test_count12():
    m = Model()

    with pytest.raises(MaplibException) as e:
        m.query("""
        PREFIX : <http://www.example.org/>
    
        SELECT ?O1 (COUNT(?O2) AS ?C)
        WHERE { ?S :p ?O1; :q ?O2 } GROUP BY (?O1 + ?O2)
        """)

    assert "SPARQL parsing error: " in str(e.value)


@pytest.mark.skip(reason="not implemented")
def test_group_concat1():
    m = Model()
    m.read(TESTDATA_PATH / "agg-groupconcat-1.ttl")

    m.query("""
    PREFIX : <http://www.example.org/>
    ASK {
        {SELECT (GROUP_CONCAT(?o) AS ?g) WHERE {
            [] :p1 ?o
        }}
        FILTER(?g = "1 22" || ?g = "22 1")
    }
    """)


def test_group_concat2():
    m = Model()
    m.read(TESTDATA_PATH / "agg-groupconcat-1.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT (COUNT(*) AS ?c) {
        {SELECT ?p (GROUP_CONCAT(?o) AS ?g) WHERE {
            [] ?p ?o
        } GROUP BY ?p}
        FILTER(
            (?p = :p1 && (?g = "1 22" || ?g = "22 1"))
            || (?p = :p2 && (?g = "aaa bb c" || ?g = "aaa c bb" || ?g = "bb aaa c" || ?g = "bb c aaa" || ?g = "c aaa bb" || ?g = "c bb aaa"))
        )
    }
    """)

    expected = pl.DataFrame({"c": 2})

    print(df)
    print(expected)

    assert df.equals(expected)


@pytest.mark.skip(reason="not implemented")
def test_group_concat_with_seperator():
    m = Model()
    m.read(TESTDATA_PATH / "agg-groupconcat-1.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    ASK {
        {SELECT (GROUP_CONCAT(?o;SEPARATOR=":") AS ?g) WHERE {
            [] :p1 ?o
        }}
        FILTER(?g = "1:22" || ?g = "22:1")
    }
    """)

    expected = pl.DataFrame([[True]])

    print(df)
    print(expected)

    assert df.equals(expected)


@pytest.mark.skip(reason="not implemented")
def test_group_concat_with_same_language_tag():
    m = Model()

    df = m.query("""
    PREFIX : <http://www.example.org/>
    ASK {
        {SELECT (GROUP_CONCAT(?o) AS ?g) WHERE {
            VALUES ?o { "1"@en "2"@en }
        }}
        FILTER(?g = "1 2" || ?g = "2 1")
    }
    """)

    expected = pl.DataFrame([[True]])

    print(df)
    print(expected)

    assert df.equals(expected)


@pytest.mark.skip(reason="not implemented")
def test_group_concat_with_different_language_tag():
    m = Model()

    df = m.query("""
    PREFIX : <http://www.example.org/>
    ASK {
        {SELECT (GROUP_CONCAT(?o) AS ?g) WHERE {
            VALUES ?o { "1"@en "2"@fr }
        }}
        FILTER(?g = "1 2" || ?g = "2 1")
    }
    """)

    expected = pl.DataFrame([[True]])

    print(df)
    print(expected)

    assert df.equals(expected)


@pytest.mark.skip(reason="not implemented")
def test_group_concat_with_one_element():
    m = Model()

    df = m.query("""
    PREFIX : <http://www.example.org/>
    ASK {
        {SELECT (GROUP_CONCAT(?o) AS ?g) WHERE {
            VALUES ?o { "1"@en }
        }}
        FILTER(?g = "1")
    }
    """)

    expected = pl.DataFrame([[True]])

    print(df)
    print(expected)

    assert df.equals(expected)


def test_sum():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT (SUM(?o) AS ?sum)
    WHERE {
        ?s :dec ?o
    }
    """)

    expected = pl.DataFrame({"sum": 11.1})

    print(df)
    print(expected)

    assert df.equals(expected)


def test_sum_with_group_by():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric2.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT ?s (SUM(?o) AS ?sum)
    WHERE {
        ?s ?p ?o
    }
    GROUP BY ?s
    """)

    expected = pl.DataFrame({
        "s": ["<http://www.example.org/ints>", "<http://www.example.org/decimals>", "<http://www.example.org/doubles>",
              "<http://www.example.org/mixed1>", "<http://www.example.org/mixed2>"],
        "sum": [6, 6.7, 3210, 3.2, 0.4]
    })

    print(df)
    print(expected)

    assert df.equals(expected)


def test_avg():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT (AVG(?o) AS ?avg)
    WHERE {
        ?s :dec ?o
    }
    """)

    expected = pl.DataFrame({"avg": 2.22})

    print(df)
    print(expected)

    assert df.equals(expected)


def test_avg_with_group_by():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric2.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT ?s (AVG(?o) AS ?avg)
    WHERE {
        ?s ?p ?o
    }
    GROUP BY ?s
    HAVING (AVG(?o) <= 2.0)
    """)

    expected = pl.DataFrame({
        "s": ["<http://www.example.org/mixed1>", "<http://www.example.org/mixed2>", "<http://www.example.org/ints>"],
        "avg": [1.6, 0.2, 2.0]
    })

    print(df)
    print(expected)

    assert df.equals(expected)


def test_avg_with_empty_group():
    m = Model()

    df = m.query("""
    SELECT (AVG(?o) AS ?avg)
    WHERE { ?s ?p ?o }
    """)

    expected = pl.DataFrame({"avg": 0})

    print(df)
    print(expected)

    assert df.equals(expected)


def test_min():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT (MIN(?o) AS ?min)
    WHERE {
        ?s :dec ?o
    }
    """)

    expected = pl.DataFrame({"min": 1.0})

    print(df)
    print(expected)

    assert df.equals(expected)


def test_min_with_group_by():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT ?s (MIN(?o) AS ?min)
    WHERE {
        ?s ?p ?o
    }
    GROUP BY ?s
    """)

    expected = pl.DataFrame({
        "s": ["<http://www.example.org/ints>", "<http://www.example.org/decimals>", "<http://www.example.org/doubles>",
              "<http://www.example.org/mixed1>", "<http://www.example.org/mixed2>"],
        "min": [1.0, 1.0, 10.0, 1.0, 0.2]  # should be [1, 1.0, 10, 1, 0.2]
    })

    print(df)
    print(expected)

    assert df.equals(expected)


def test_max():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT (MAX(?o) AS ?max)
    WHERE {
        ?s ?p ?o
    }
    """)

    expected = pl.DataFrame({"max": 30000})

    print(df)
    print(expected)

    assert df.equals(expected)


def test_max_with_group_by():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT ?s (MAX(?o) AS ?max)
    WHERE {
        ?s ?p ?o
    }
    GROUP BY ?s
    """)

    expected = pl.DataFrame({
        "s": ["<http://www.example.org/ints>", "<http://www.example.org/decimals>", "<http://www.example.org/doubles>",
              "<http://www.example.org/mixed1>", "<http://www.example.org/mixed2>"],
        "max": [3.0, 3.5, 30000, 2.2, 2.2]  # should be [3, 3.5, 30000, 2.2, 2.2]
    })

    print(df)
    print(expected)

    assert df.equals(expected)


@pytest.mark.skip(reason="not implemented")
def test_sample():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    ASK {
        {
            SELECT (SAMPLE(?o) AS ?sample)
            WHERE {
                ?s :dec ?o
            }
        }
        FILTER(?sample = 1.0 || ?sample = 2.2 || ?sample = 3.5)
    }
    """)

    expected = pl.DataFrame([[True]])

    print(df)
    print(expected)

    assert df.equals(expected)


def test_error_in_avg():
    m = Model()
    m.read(TESTDATA_PATH / "agg-err-01.ttl")

    df = m.query("""PREFIX : <http://example.com/data/#>
    SELECT ?g (AVG(?p) AS ?avg) ((MIN(?p) + MAX(?p)) / 2 AS ?c)
    WHERE {
      ?g :p ?p .
    }
    GROUP BY ?g
    """)

    expected = pl.DataFrame({
        "g": ["<http://example.com/data/#x>", "<http://example.com/data/#y>", "<http://example.com/data/#z>"],
        "avg": [2.5, None, 2.5],
        "c": [2.5, None, 2.5]
    })

    print(df)
    print(expected)

    assert df.equals(expected)


@pytest.mark.skip(reason="not implemented")
def test_protect_from_error_in_avg():
    m = Model()
    m.read(TESTDATA_PATH / "agg-err-02.ttl")

    df = m.query("""
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX : <http://example.com/data/#>
    SELECT ?g 
    (AVG(IF(isNumeric(?p), ?p, COALESCE(xsd:double(?p),0))) AS ?avg) 
    WHERE {
      ?g :p ?p .
    }
    GROUP BY ?g
    """)

    expected = pl.DataFrame({
        "g": ["<http://example.com/data/#x>", "<http://example.com/data/#y>", "<http://example.com/data/#z>"],
        "avg": [2.5, 2.0, 2.5]
    })

    print(df)
    print(expected)

    assert df.equals(expected)


def test_agg_on_empty_set_explicit_grouping():
    m = Model()
    m.read(TESTDATA_PATH / "empty.ttl")

    df = m.query("""
    PREFIX ex: <http://example.com/>
    SELECT ?x (MAX(?value) AS ?max)
    WHERE {
        ?x ex:p ?value
    } GROUP BY ?x
    """)

    expected = pl.DataFrame({"x": [], "max": []})

    print(df)
    print(expected)

    assert df.equals(expected)


def test_agg_on_empty_set_no_grouping():
    m = Model()
    m.read(TESTDATA_PATH / "empty.ttl")

    df = m.query("""
    PREFIX ex: <http://example.com/>
    SELECT (MAX(?value) AS ?max)
    WHERE {
        ?x ex:p ?value
    }
    """)

    expected = pl.DataFrame({"max": []}).with_columns("max").cast(pl.String)

    print(df)
    print(expected)

    assert df.equals(expected)


def test_count_no_match_with_group():
    m = Model()
    m.read(TESTDATA_PATH / "empty.ttl")

    df = m.query("""
    PREFIX : <http://example/>

    SELECT (count(*) AS ?C)
    WHERE {
       ?s :p ?x
    }
    GROUP BY ?s
    """)

    expected = pl.DataFrame({"C": []}).with_columns("C").cast(pl.UInt32)

    print(df)
    print(expected)

    assert df.equals(expected)


def test_count_no_match_no_group():
    m = Model()
    m.read(TESTDATA_PATH / "empty.ttl")

    df = m.query("""
    PREFIX : <http://example/>

    SELECT (count(*) AS ?C)
    WHERE {
       ?s :p ?x
    }
    """)

    expected = pl.DataFrame({"C": [0]})

    print(df)
    print(expected)

    assert df.equals(expected)


@pytest.mark.skip(reason="not implemented")
def test_count_no_group_by_inside_graph():
    m = Model()
    m.read(TESTDATA_PATH / "empty.ttl")

    # TODO! finish test. should use 'singleton.ttl' and 'pair.ttl' as well

    df = m.query("""
    PREFIX : <http://example/>

    SELECT ?g ?c WHERE {
       GRAPH ?g {SELECT (count(*) AS ?c) WHERE { ?s :p ?x FILTER(?x != :o) }}
    }
    """)


def test_having_multiple_conditions():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT ?s
    WHERE {
        ?s ?p ?o
    }
    GROUP BY ?s
    HAVING (COUNT(*) > 1) (COUNT(*) < 3)
    """)

    expected = pl.DataFrame({"s": ["<http://www.example.org/mixed1>", "<http://www.example.org/mixed2>"]})

    df = df.sort("s")

    print(df)
    print(expected)

    assert df.equals(expected)


def test_group_by_with_a_function():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric.ttl")

    df = m.query("""
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?i
    WHERE {
        ?s ?p ?o
    }
    GROUP BY (xsd:integer(?o) AS ?i)
    """)

    expected = pl.DataFrame({
        "i": [0, 1, 2, 3, 100, 2000, 30000]
    })

    df = df.sort("i")

    print(df)
    print(expected)

    assert df.equals(expected)


def test_group_by_with_builtin_function():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT ?d (COUNT(*) AS ?c)
    WHERE {
        ?s ?p ?o
    }
    GROUP BY (DATATYPE(?o) AS ?d)
    """)

    expected = pl.DataFrame({
        "d": ["<http://www.w3.org/2001/XMLSchema#decimal>", "<http://www.w3.org/2001/XMLSchema#double>",
              "<http://www.w3.org/2001/XMLSchema#integer>"],
        "c": [5, 4, 4]
    })

    df = df.sort("c", descending=True)

    print(df)
    print(expected)

    assert df.equals(expected)


def test_avg_distinct_with_group_by():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric-duplicates.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT ?s (AVG(DISTINCT ?o) AS ?avg)
    WHERE {
        ?s ?p ?o
    }
    GROUP BY ?s
    """)

    expected = pl.DataFrame({
        "s": ["<http://www.example.org/ints>", "<http://www.example.org/decimals>", "<http://www.example.org/doubles>",
              "<http://www.example.org/mixed1>"],
        "avg": [1.5, 1.6, 1050, 1.6]
    })

    print(df)
    print(expected)

    assert df.equals(expected)


def test_count_distinct_with_group_by():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric-duplicates.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT ?s (COUNT(DISTINCT ?o) AS ?count) WHERE {
        ?s ?p ?o
    } GROUP BY ?s
    """)

    expected = pl.DataFrame({
        "s": ["<http://www.example.org/ints>", "<http://www.example.org/decimals>", "<http://www.example.org/doubles>",
              "<http://www.example.org/mixed1>"],
        "count": [2, 2, 2, 2]
    })

    df = df.with_columns(pl.col("count").cast(pl.Int64)).sort("s")
    expected = expected.sort("s")

    print(df)
    print(expected)

    assert df.equals(expected)


@pytest.mark.skip(reason="not implemented")
def test_count_distinct_all_with_group_by():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric-duplicates.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT ?s (COUNT(DISTINCT *) AS ?count) WHERE {
        ?s ?p ?o
    } GROUP BY ?s
    """)

    expected = pl.DataFrame({
        "s": ["<http://www.example.org/ints>", "<http://www.example.org/decimals>", "<http://www.example.org/doubles>",
              "<http://www.example.org/mixed1>"],
        "count": [2, 2, 2, 2]
    })

    print(df)
    print(expected)

    assert df.equals(expected)


@pytest.mark.skip(reason="not implemented")
def group_concat_distinct():
    m = Model()

    df = m.query("""
    PREFIX : <http://www.example.org/>
    ASK {
        {SELECT (GROUP_CONCAT(DISTINCT ?o) AS ?g) WHERE {
            VALUES ?o { "1" "2" "1" }
        }}
        FILTER(?g = "1 2" || ?g = "2 1")
    }
    """)

    expected = pl.DataFrame([[True]])

    assert df.equals(expected)


def test_max_distinct_with_group_by():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric-duplicates.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT ?s (MAX(DISTINCT ?o) AS ?max)
    WHERE {
        ?s ?p ?o
    }
    GROUP BY ?s
    """)

    expected = pl.DataFrame({
        "s": ["<http://www.example.org/ints>", "<http://www.example.org/decimals>", "<http://www.example.org/doubles>",
              "<http://www.example.org/mixed1>"],
        "max": [2.0, 2.2, 2000.0, 2.2]  # should be [2, 2.2, 2000, 2.2]
    })

    print(df)
    print(expected)

    assert df.equals(expected)


def test_min_distinct_with_group_by():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric-duplicates.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT ?s (MIN(DISTINCT ?o) AS ?min)
    WHERE {
        ?s ?p ?o
    }
    GROUP BY ?s
    """)

    expected = pl.DataFrame({
        "s": ["<http://www.example.org/ints>", "<http://www.example.org/decimals>", "<http://www.example.org/doubles>",
              "<http://www.example.org/mixed1>"],
        "min": [1.0, 1.0, 100, 1.0]  # should be [1, 1.0, 100, 1]
    })

    print(df)
    print(expected)

    assert df.equals(expected)


@pytest.mark.skip(reason="not implemented")
def test_sample_distinct():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric-duplicates.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    ASK {
        {
            SELECT (SAMPLE(DISTINCT ?o) AS ?sample)
            WHERE {
                ?s :dec ?o
            }
        }
        FILTER(?sample = 1.0 || ?sample = 2.2 || ?sample = 3.5)
    }
    """)

    expected = pl.DataFrame([[True]])

    print(df)
    print(expected)

    assert df.equals(expected)


def test_sum_distinct_with_group_by():
    m = Model()
    m.read(TESTDATA_PATH / "agg-numeric-duplicates.ttl")

    df = m.query("""
    PREFIX : <http://www.example.org/>
    SELECT ?s (SUM(DISTINCT ?o) AS ?sum)
    WHERE {
        ?s ?p ?o
    }
    GROUP BY ?s
    """)

    expected = pl.DataFrame({
        "s": ["<http://www.example.org/ints>", "<http://www.example.org/decimals>", "<http://www.example.org/doubles>",
              "<http://www.example.org/mixed1>"],
        "sum": [3.0, 3.2, 2100.0, 3.2]  # should be [3, 3.2, 2100, 3.2]
    })

    print(df)
    print(expected)

    assert df.equals(expected)

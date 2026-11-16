from maplib import Model

TTL = """
@prefix ex: <urn:> .
ex:a ex:name "x" .
ex:b ex:p ex:c1 , ex:c2 ; ex:name "y" .
ex:d ex:p ex:c1 , "lit" ; ex:name "z"@en .
"""


def model():
    m = Model()
    m.read(TTL.encode(), format="turtle")
    return m


def counts(df):
    return {
        n: (k, k2)
        for n, k, k2 in zip(
            df.get_column("n").to_list(),
            df.get_column("k").to_list(),
            df.get_column("k2").to_list(),
        )
    }


def test_count_distinct_all_unbound():
    df = model().query("""
    SELECT ?n (COUNT(DISTINCT ?c) AS ?k) (COUNT(?c) AS ?k2) WHERE {
        VALUES ?n { <urn:a> } OPTIONAL { ?n <urn:p> ?c }
    } GROUP BY ?n
    """)
    assert df.get_column("k").to_list() == [0]
    assert df.get_column("k2").to_list() == [0]


def test_count_distinct_some_unbound():
    # Leaf ex:a has no children, ex:b has two IRIs, ex:d has an IRI and a literal (multi-type).
    df = model().query("""
    SELECT ?n (COUNT(DISTINCT ?c) AS ?k) (COUNT(?c) AS ?k2) WHERE {
        VALUES ?n { <urn:a> <urn:b> <urn:d> } OPTIONAL { ?n <urn:p> ?c }
    } GROUP BY ?n
    """)
    assert counts(df) == {"<urn:a>": (0, 0), "<urn:b>": (2, 2), "<urn:d>": (2, 2)}


def test_count_distinct_unbound_without_group_by():
    df = model().query("""
    SELECT (COUNT(DISTINCT ?c) AS ?k) (COUNT(?c) AS ?k2) WHERE {
        VALUES ?n { <urn:a> <urn:b> } OPTIONAL { ?n <urn:p> ?c }
    }
    """)
    assert df.get_column("k").to_list() == [2]
    assert df.get_column("k2").to_list() == [2]


def test_count_distinct_unbound_lang_string():
    # A column mixing plain and language-tagged strings, with an unbound row.
    df = model().query("""
    SELECT (COUNT(DISTINCT ?l) AS ?k) (COUNT(?l) AS ?k2) WHERE {
        VALUES ?n { <urn:a> <urn:d> <urn:c1> } OPTIONAL { ?n <urn:name> ?l }
    }
    """)
    assert df.get_column("k").to_list() == [2]
    assert df.get_column("k2").to_list() == [2]


def test_count_distinct_unbound_only_lang_string():
    df = model().query("""
    SELECT (COUNT(DISTINCT ?l) AS ?k) (COUNT(?l) AS ?k2) WHERE {
        VALUES ?n { <urn:d> <urn:c1> } OPTIONAL { ?n <urn:name> ?l }
    }
    """)
    assert df.get_column("k").to_list() == [1]
    assert df.get_column("k2").to_list() == [1]


def test_count_distinct_unbound_window():
    # Each row of a partition gets the partition's count; ex:a's partition has only an unbound ?c.
    df = model().query("""
    SELECT ?n ?c (COUNT(DISTINCT ?c) OVER (PARTITION BY ?n) AS ?k)
        (COUNT(?c) OVER (PARTITION BY ?n) AS ?k2) WHERE {
        VALUES ?n { <urn:a> <urn:b> <urn:d> } OPTIONAL { ?n <urn:p> ?c }
    }
    """)
    assert counts(df) == {"<urn:a>": (0, 0), "<urn:b>": (2, 2), "<urn:d>": (2, 2)}
    assert df.height == 5


def test_count_distinct_unbound_window_frame():
    # A running COUNT(DISTINCT) over an ORDER BY frame, with the unbound row sorted first.
    df = model().query("""
    SELECT ?c (COUNT(DISTINCT ?c) OVER (ORDER BY ?c ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ?k)
    WHERE { VALUES ?n { <urn:a> <urn:b> } OPTIONAL { ?n <urn:p> ?c } }
    ORDER BY ?c
    """)
    assert df.get_column("k").to_list() == [0, 1, 2]

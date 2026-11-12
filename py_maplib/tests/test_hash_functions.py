import hashlib

import pytest

from maplib import Model

ALGORITHMS = ["md5", "sha1", "sha256", "sha384", "sha512"]


@pytest.mark.parametrize("algorithm", ALGORITHMS)
@pytest.mark.parametrize("value", ["abc", "", "héllo 日本"])
def test_hash_of_string_literal(algorithm, value):
    df = Model().query(f'SELECT ({algorithm.upper()}("{value}") AS ?h) WHERE {{}}')
    assert df["h"][0] == hashlib.new(algorithm, value.encode()).hexdigest()


@pytest.mark.parametrize("algorithm", ALGORITHMS)
def test_hash_on_data(algorithm):
    m = Model()
    m.reads(
        """@prefix : <http://example.org/> .
        :a :p "one" . :b :p "two" . :c :q "x" .""",
        format="turtle",
    )
    df = m.query(
        f"""PREFIX : <http://example.org/>
        SELECT ?s ({algorithm.upper()}(?o) AS ?h) (DATATYPE({algorithm.upper()}(?o)) AS ?t)
        WHERE {{ ?s :p ?o }} ORDER BY ?s"""
    )
    assert df["h"].to_list() == [
        hashlib.new(algorithm, b"one").hexdigest(),
        hashlib.new(algorithm, b"two").hexdigest(),
    ]
    assert set(df["t"].to_list()) == {"<http://www.w3.org/2001/XMLSchema#string>"}

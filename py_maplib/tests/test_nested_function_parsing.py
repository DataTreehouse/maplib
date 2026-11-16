"""Deeply nested REPLACE, SUBSTR and REGEX calls parse in linear time.

The parser used to try the longer form of these functions first and parse the arguments again
for the shorter form, which is exponential in the nesting depth: the 20 nested REPLACEs that
Model.map_csvw generates to percent-decode reserved characters took minutes (and overflowed the
stack in a debug build). At depth 40 that would never finish.
"""

from maplib import Model

DEPTH = 40


def query_value(m, expr):
    df = m.query(f"SELECT ?v WHERE {{ BIND({expr} AS ?v) }}")
    return df["v"][0]


def test_nested_replace_with_three_and_four_arguments():
    m = Model()
    e3 = '"a"'
    e4 = '"a"'
    for _ in range(DEPTH):
        e3 = f'REPLACE({e3}, "a", "a")'
        e4 = f'REPLACE({e4}, "A", "a", "i")'
    assert query_value(m, e3) == "a"
    assert query_value(m, e4) == "a"


def test_nested_substr_with_two_and_three_arguments():
    m = Model()
    e2 = '"abc"'
    e3 = '"abc"'
    for _ in range(DEPTH):
        e2 = f"SUBSTR({e2}, 1)"
        e3 = f"SUBSTR({e3}, 1, 3)"
    assert query_value(m, e2) == "abc"
    assert query_value(m, e3) == "abc"


def test_nested_regex_with_two_and_three_arguments():
    m = Model()
    e2 = "true"
    e3 = "true"
    for _ in range(DEPTH):
        e2 = f'REGEX(IF({e2}, "a", "b"), "a")'
        e3 = f'REGEX(IF({e3}, "A", "b"), "a", "i")'
    assert query_value(m, e2) is True
    assert query_value(m, e3) is True

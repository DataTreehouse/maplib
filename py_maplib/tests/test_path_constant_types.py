"""Property paths with a constant at an end whose type the path's values do not have.

`<a> <b>+ 1.0` panicked with a SchemaMismatch (comparing the Boolean column of a path without
solutions, or a column of IRIs, with a decimal) instead of giving no solutions (DAWG
syntax-sparql2/syntax-general-07).
"""

import pytest

from maplib import Model


@pytest.fixture(scope="module")
def model():
    m = Model()
    m.reads(
        """
        @prefix : <http://example/> .
        :a :b :c . :c :b :d .
        :n :v 1.0 . :n :w :m . :m :w 2 .
        """,
        format="turtle",
    )
    return m


@pytest.mark.parametrize(
    "pattern",
    [
        "<http://a><http://b>+1.0",
        ":a :b+ 1.0",
        ":a :b* 1.0",
        ':a :b+ "x"',
        "1.0 :b+ :d",
        ":a :w+ :d",
        ":n :w+ 2.5",
    ],
)
def test_no_match_without_panic(model, pattern):
    df = model.query(f"PREFIX : <http://example/> SELECT * WHERE {{ {pattern} }}")
    assert df.height == 0


@pytest.mark.parametrize(
    "pattern",
    [":a :b+ :d", ":n :w+ 2", ":n :v+ 1.0", ":a :b* :a"],
)
def test_matches_still_found(model, pattern):
    df = model.query(f"PREFIX : <http://example/> ASK {{ {pattern} }}")
    assert df is True

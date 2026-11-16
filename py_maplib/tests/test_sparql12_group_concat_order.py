"""Prototype tests for ordered GROUP_CONCAT (w3c-cg/sparql-dev#9).

The test data in ``testdata/sparql12_group_concat_order`` is an unmodified copy of the
prototype tests from the SPARQL 1.2 Community Group fork by Gregory Williams, see
``testdata/sparql12_group_concat_order/LICENSE.md``. They use the syntax
``GROUP_CONCAT(?x; ORDER BY ?y; SEPARATOR=".")``, which maplib accepts along with
``GROUP_CONCAT(?x; SEPARATOR="." ORDER BY ?y)``. No test suite exists for window functions.
"""

import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from maplib import Model

SUITE_DIR = Path(__file__).parent / "testdata" / "sparql12_group_concat_order"
MF = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#"
QT = "http://www.w3.org/2001/sw/DataAccess/tests/test-query#"
SRX = "{http://www.w3.org/2005/sparql-results#}"


def manifest_tests():
    m = Model()
    m.read(
        str(SUITE_DIR / "manifest.ttl"),
        format="turtle",
        base_iri="https://example.org/sparql12/aggregation/manifest.ttl",
    )
    df = m.query(
        f"""PREFIX mf: <{MF}> PREFIX qt: <{QT}>
        SELECT ?name ?query ?result WHERE {{
            ?t mf:name ?name ; mf:action [ qt:query ?query ] ; mf:result ?result .
        }}"""
    )
    tests = []
    for name, query, result in df.iter_rows():
        tests.append(
            (name, query.strip("<>").rsplit("/", 1)[-1], result.strip("<>").rsplit("/", 1)[-1])
        )
    return sorted(tests)


def expected_rows(path: Path):
    root = ET.parse(path).getroot()
    rows = []
    for result in root.iter(f"{SRX}result"):
        row = {}
        for b in result.findall(f"{SRX}binding"):
            row[b.get("name")] = b.find(f"{SRX}literal").text
        rows.append(row)
    return rows


TESTS = manifest_tests()


def test_all_manifest_entries_found():
    assert len(TESTS) == 5


@pytest.mark.parametrize("name,query,result", TESTS, ids=[t[0] for t in TESTS])
def test_group_concat_order(name, query, result):
    m = Model()
    df = m.query((SUITE_DIR / query).read_text())
    actual = sorted(
        ({k: str(v) for k, v in r.items()} for r in df.iter_rows(named=True)),
        key=lambda r: sorted(r.items()),
    )
    expected = sorted(expected_rows(SUITE_DIR / result), key=lambda r: sorted(r.items()))
    assert actual == expected

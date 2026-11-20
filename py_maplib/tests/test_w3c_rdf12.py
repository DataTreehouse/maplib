"""
W3C RDF 1.2 compliance tests.

The test data in ``testdata/w3c_rdf12`` is a copy of ``rdf/rdf12`` from
https://github.com/w3c/rdf-tests (commit 56076c0efec4c353efcae2971be8f6005294ac64), see
``testdata/w3c_rdf12/LICENSE.md``, without the implementation reports. The entailment tests
(``rdf-semantics``) are not included. RDF 1.2 conformance also requires passing the RDF 1.1
tests, see test_w3c_rdf11.py.

The harness is in ``w3c_rdf.py``. Tests that maplib currently does not pass are listed in
``w3c_rdf12_known_failures.txt``.
"""

import pytest

from .w3c_rdf import (
    RDF12,
    Manifest,
    build_params,
    build_round_trip_params,
    run_round_trip,
    run_test,
)


@pytest.mark.parametrize("manifest,entry", build_params(RDF12))
def test_w3c_rdf12(manifest: Manifest, entry):
    run_test(manifest, entry)


@pytest.mark.parametrize("manifest,entry,out_format", build_round_trip_params(RDF12))
def test_w3c_rdf12_round_trip(manifest: Manifest, entry, out_format: str):
    run_round_trip(manifest, entry, out_format)

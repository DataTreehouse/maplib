"""
W3C RDF 1.1 compliance tests.

The test data in ``testdata/w3c_rdf11`` is a copy of ``rdf/rdf11`` from
https://github.com/w3c/rdf-tests (commit 4158bca195a29fe1fd90f92698170bc7cc385273), see
``testdata/w3c_rdf11/LICENSE.md``, without the implementation reports. The entailment tests
(``rdf-mt``) are not included.

The harness is in ``w3c_rdf.py``. Tests that maplib currently does not pass are listed in
``w3c_rdf11_known_failures.txt``.
"""

import pytest

from .w3c_rdf import (
    RDF11,
    Manifest,
    build_params,
    build_round_trip_params,
    run_round_trip,
    run_test,
)


@pytest.mark.parametrize("manifest,entry", build_params(RDF11))
def test_w3c_rdf11(manifest: Manifest, entry):
    run_test(manifest, entry)


@pytest.mark.parametrize("manifest,entry,out_format", build_round_trip_params(RDF11))
def test_w3c_rdf11_round_trip(manifest: Manifest, entry, out_format: str):
    run_round_trip(manifest, entry, out_format)

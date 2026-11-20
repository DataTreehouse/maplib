"""Model.cim: the CIM grid tools of maplib Enterprise Edition, an AttributeError in open source maplib."""

import importlib.util

import pytest

from maplib import Model
from maplib.opcua import OPCUA

ENTERPRISE = importlib.util.find_spec("maplib.cim") is not None


@pytest.mark.skipif(ENTERPRISE, reason="maplib Enterprise Edition has the CIM grid tools")
def test_cim_needs_enterprise_edition():
    m = Model()
    with pytest.raises(AttributeError, match="Model.cim needs maplib Enterprise Edition"):
        m.cim
    assert not hasattr(m, "cim")


@pytest.mark.skipif(not ENTERPRISE, reason="open source maplib has no CIM grid tools")
def test_cim_is_bound_to_the_model():
    m = Model()
    assert hasattr(m, "cim")
    assert m.cim._model is m
    assert callable(m.cim.cut_to_graph)


def test_opcua_is_unchanged():
    m = Model()
    assert hasattr(m, "opcua")
    assert isinstance(m.opcua, OPCUA)

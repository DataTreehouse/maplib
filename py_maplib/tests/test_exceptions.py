import pytest
from maplib import Model
from maplib import MaplibException


def test_model_exception():
    m = Model()

    with pytest.raises(MaplibException) as e:
        m.reads("abc", format="turtle", graph="http://example.com/data")

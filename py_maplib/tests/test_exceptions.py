import pytest
from maplib import Mapping
from maplib import MaplibException

def test_mapping_exception():
    m = Mapping()

    with pytest.raises(MaplibException) as e:
        m.read_triples_string("abc", format="turtle", graph="http://example.com/data")
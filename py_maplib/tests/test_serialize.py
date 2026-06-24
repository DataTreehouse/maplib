from maplib import Model
import pathlib

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"

def test_easy_serialize():
    m = Model()
    m.read(TESTDATA_PATH / "expected_easy_case.ttl")
    m.serialize("serialized")

    m2 = Model.deserialize("serialized")
    df = m2.query("""
        SELECT * WHERE {?a ?b ?c}
    """)
    assert df.height == 4
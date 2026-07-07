import pytest
import pathlib
from maplib import Model
from polars.testing import assert_frame_equal
import polars as pl

pl.Config.set_fmt_str_lengths(200).set_tbl_width_chars(400).set_tbl_rows(200)

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata" / "rdfs"
FOLDERS = [TESTDATA_PATH / f"rdfs{i}" for i in [2,3,5,6,8,9,10,11]]

@pytest.mark.parametrize("folder", FOLDERS)
def test_rdfs_inference(folder:pathlib.Path):
    print(folder)
    m_input = Model()
    m_input.read(folder / "input.ttl")
    df_input = m_input.query("""
    SELECT * WHERE {?a ?b ?c}
    ORDER BY ?a ?b ?c
    """)
    m_input.infer_rdfs()

    m_expected = Model()
    m_expected.read(folder / "expected.ttl")

    df_inferred = m_input.query("""
    SELECT * WHERE {?a ?b ?c}
    ORDER BY ?a ?b ?c
    """)

    df_expected = m_expected.query("""
    SELECT * WHERE {?a ?b ?c }
    ORDER BY ?a ?b ?c
    """)

    print(folder)
    print(df_input)
    print(df_inferred)
    print(df_expected)

    assert_frame_equal(df_inferred, df_expected)





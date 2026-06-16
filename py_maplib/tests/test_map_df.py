import os
from typing import List, Optional

import pytest

from .disk import disk_params
from maplib import Model
from polars.testing import assert_frame_equal
import polars as pl
import pathlib
import time

pl.Config.set_fmt_str_lengths(300)

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata" / "jsons"

@pytest.mark.parametrize("disk", disk_params())
def test_map_df_easy(disk):
    df = pl.DataFrame({
        "a": ["abc", "def"],
        "b": [1,2]
    })
    m = Model()
    m.map_df(df)
    out_df = m.query("""
    SELECT * WHERE {
        ?root a fx:root .
        ?root fx:child ?row .
        ?row xyz:a ?a .
        ?row fx:childNumber ?ch_num .
        BIND(IRI(CONCAT("urn:my_ns:", ?a)) AS ?a_uri)
    }
    """)
    assert out_df.shape == (2,5)
import time


from maplib import Model
from pathlib import Path
import polars as pl

# show all the dataframe:
pl.Config.set_tbl_rows(1000)
pl.Config.set_tbl_cols(1000)

PATH_HERE = Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata" / "pp_data"


def test_issue_73_negated_pp():
    m = Model()
    m.read(TESTDATA_PATH / "pp_data.ttl")

    start_time = time.perf_counter()

    df = m.query("""
   
        PREFIX ex: <http://example.org/>
        PREFIX foaf: <http://friend.com/>

        SELECT *
        WHERE { ?s !foaf:a* ?o }
        """)

    print(f"the time sir: {time.perf_counter() - start_time}")

    print(df)
    df.write_csv("foaftest.csv")


def test_star_path_over_foaf_knows():
    m = Model()
    m.read(TESTDATA_PATH / "pp_data.ttl")

    start_time = time.perf_counter()

    df = m.query("""
    PREFIX ex: <http://example.org/>
    PREFIX foaf: <http://friend.com/>

    SELECT *
    WHERE { ?s foaf:c+ ?o }
    """)

    print(f"the time sir: {time.perf_counter() - start_time}")

    print(df)


def test_star_path_over_foaf_knows_192():
    m = Model()
    m.read(TESTDATA_PATH / "pp_data_192.ttl")

    start_time = time.perf_counter()

    df = m.query("""
    PREFIX ex: <http://example.org/>
    PREFIX foaf: <http://friend.com/>

    SELECT *
    WHERE { ?s foaf:a+ ?o }
    """)

    print(f"the time sir: {time.perf_counter() - start_time}")

    print(df)
    df.write_csv("foaftest.csv")






# ------------------------------- Plus test --------------------------------

def test_plus_path_over_foaf_knows():
    m = Model()
    m.read(TESTDATA_PATH / "pp_data.ttl")

    start_time = time.perf_counter()

    df = m.query("""
    PREFIX ex: <http://example.org/>
    PREFIX foaf: <http://friend.com/>

    SELECT *
    WHERE { ?s foaf:a+ ?o }
    """)

    print(f"the time sir: {time.perf_counter() - start_time}")

    print(df)
    df.write_csv("foaftest.csv")






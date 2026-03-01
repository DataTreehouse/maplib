import pathlib
import time
import os

from maplib import Model

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"

def test_jelly_benchmark():
    m = Model()
    m.read(TESTDATA_PATH / "sunspots.ttl")
    filename = TESTDATA_PATH / "output.jelly"
    if filename.exists():
        os.remove(filename)

    start_time_total = time.perf_counter()
    start_time_write = time.perf_counter()
    m.write(filename, format="jelly")
    end_time_write = time.perf_counter()
    start_time_read = time.perf_counter()
    m.read(filename, format="jelly")
    end_time_read = time.perf_counter()
    end_time_total = time.perf_counter()

    df = m.query(
        """
            SELECT ?s ?p ?o WHERE {
                ?s ?p ?o .
            }
        """
    )

    print(f"\nAmount of triples: {df.height}")

    print(f"\nWrite time: {end_time_write - start_time_write:.4f} seconds")
    print(f"Read time: {end_time_read - start_time_read:.4f} seconds")
    print(f"Total time: {end_time_total - start_time_total:.4f} seconds")

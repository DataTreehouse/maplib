"""Deleting triples that are not in a table leaves the table as it is; deleting a mix of present
and absent triples removes exactly the present ones."""
import shutil

import polars as pl
import pytest

from maplib import Model

from .disk import disk_params, storage_kwargs, STORAGE_FOLDER

EX = "http://example.net/ns#"

DATA = f"""
<{EX}a> <{EX}knows> <{EX}b> .
<{EX}a> <{EX}knows> <{EX}c> .
<{EX}b> <{EX}knows> <{EX}c> .
<{EX}a> <{EX}age> 30 .
<{EX}b> <{EX}age> 40 .
"""

ALL = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o"


@pytest.fixture(params=disk_params())
def model(request):
    storage = request.param
    if storage != "memory":
        shutil.rmtree(STORAGE_FOLDER, ignore_errors=True)
    m = Model(**storage_kwargs(storage))
    m.reads(DATA, format="ntriples")
    yield m
    del m
    if storage != "memory":
        shutil.rmtree(STORAGE_FOLDER, ignore_errors=True)


def triples(m):
    return m.query(ALL).with_columns(pl.all().cast(pl.String))


def test_delete_absent_triples_of_existing_tables(model):
    before = triples(model)
    model.update(
        f"DELETE DATA {{ <{EX}c> <{EX}knows> <{EX}a> . <{EX}c> <{EX}age> 50 . }}"
    )
    assert triples(model).equals(before)
    # The tables still answer lookups by subject and object.
    df = model.query(f"SELECT ?s WHERE {{ ?s <{EX}knows> <{EX}c> }} ORDER BY ?s")
    assert df["s"].to_list() == [f"<{EX}a>", f"<{EX}b>"]
    df = model.query(f"SELECT ?o WHERE {{ <{EX}b> <{EX}age> ?o }}")
    assert df["o"].to_list() == [40]


def test_delete_present_and_absent_triples(model):
    model.update(
        f"""DELETE DATA {{
            <{EX}a> <{EX}knows> <{EX}b> . <{EX}c> <{EX}knows> <{EX}a> .
            <{EX}c> <{EX}age> 50 .
        }}"""
    )
    df = triples(model)
    assert df.height == 4
    assert model.query(f"SELECT * WHERE {{ <{EX}a> <{EX}knows> <{EX}b> }}").height == 0
    model.update(f"DELETE WHERE {{ ?s <{EX}age> 40 }}")
    assert triples(model).height == 3
    # The same deletion again changes nothing.
    model.update(f"DELETE WHERE {{ ?s <{EX}age> 40 }}")
    assert triples(model).height == 3

"""Reading, writing, serializing and deserializing with URLs of object storage: file://, Azure
Blob Storage (az://, the Azurite emulator by default) and Google Cloud Storage (gs://, a real
bucket given with MAPLIB_TEST_GCS_URL), see object_storage_emulators.py. Hybrid and disk
storage are tested in maplib Enterprise Edition."""

import polars as pl
import pytest
from maplib import Model, RDFType
from maplib.maplib import SolutionMappings

from .object_storage_emulators import Backend, backend, emulators  # noqa: F401

TURTLE = """
@prefix ex: <http://example.net/ns#> .
ex:a ex:knows ex:b , ex:c .
ex:b ex:name "Bee" .
ex:c ex:age 42 .
"""

TEMPLATE = """
@prefix ex: <http://example.net/ns#> .
ex:T [ ?x ] :: { ottr:Triple(?x, ex:p, ex:o) } .
"""

COUNT = "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }"


def small_model() -> Model:
    m = Model()
    m.reads(TURTLE, format="turtle")
    return m


def large_model(n: int) -> Model:
    df = pl.DataFrame(
        {
            "subject": [f"http://example.net/ns#s{i}" for i in range(n)],
            "object": [f"a fairly long literal value number {i} to fill the file" for i in range(n)],
        }
    )
    m = Model()
    m.map_triples(
        SolutionMappings(df, {"subject": RDFType.IRI, "object": RDFType.Literal("http://www.w3.org/2001/XMLSchema#string")}),
        predicate="http://example.net/ns#p",
    )
    return m


def count(m: Model) -> int:
    return m.query(COUNT)["n"][0]


@pytest.mark.parametrize("name,format", [("t.nt", None), ("t.ttl", None), ("t.hdt", "hdt"), ("t.nq", "nquads")])
def test_write_and_read_urls(backend: Backend, name, format):
    m = small_model()
    url = backend.unique(name)
    m.write(url, format=format, storage_options=backend.storage_options)
    m2 = Model()
    m2.read(url, format=format, storage_options=backend.storage_options)
    assert count(m2) == 4
    df = m2.query('PREFIX ex: <http://example.net/ns#> SELECT ?n WHERE { ex:b ex:name ?n }')
    assert df["n"].to_list() == ["Bee"]


def test_write_and_read_large_file(backend: Backend):
    # Larger than the part size, so it is uploaded in parts and downloaded with range requests.
    m = large_model(600_000)
    url = backend.unique("large.nt")
    m.write(url, storage_options=backend.storage_options)
    m2 = Model()
    m2.read(url, storage_options=backend.storage_options)
    assert count(m2) == 600_000


def test_read_template_url(emulators):
    # maplib does not write template files, so the file is put in place through the file system.
    b = emulators.get("file")
    p = b.local_root / "tpl" / "templates.stottr"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(TEMPLATE)
    m = Model()
    m.read_template(b.url("tpl", "templates.stottr"))
    assert len(m.get_templates()) == 1


@pytest.mark.parametrize("raw_db", [False])
def test_serialize_single_file_url(backend: Backend, raw_db):
    m = small_model()
    url = backend.unique("model.maplib")
    m.serialize(url, raw_db=raw_db, storage_options=backend.storage_options)
    m2 = Model.deserialize(url, storage_options=backend.storage_options)
    assert count(m2) == 4


def test_serialize_folder_url_generations(backend: Backend):
    url = backend.unique("model")
    m = small_model()
    for i in range(3):
        m.reads(f'<http://example.net/ns#x{i}> <http://example.net/ns#p> "{i}" .', format="ntriples")
        m.serialize(url, single_file=False, storage_options=backend.storage_options)
        m2 = Model.deserialize(url, storage_options=backend.storage_options)
        assert count(m2) == 5 + i
    if backend.local_root is not None:
        folder = backend.local_root / url.split(backend.root.rstrip("/") + "/", 1)[1]
        generations = sorted(p.name for p in folder.iterdir() if p.name.startswith("gen-"))
        # The current generation and the one before are kept.
        assert len(generations) == 2
        assert (folder / "LATEST").read_text() == generations[-1]


def test_missing_url_is_an_error(backend: Backend):
    with pytest.raises(Exception, match="not found|No serialized model|NotFound"):
        Model().read(backend.unique("missing.ttl"), storage_options=backend.storage_options)
    with pytest.raises(Exception, match="not found|No serialized model|NotFound"):
        Model.deserialize(backend.unique("missing"), storage_options=backend.storage_options)


def test_unknown_storage_option_is_an_error(emulators):
    b = emulators.get("file")
    url = "gs://some-bucket/x.ttl"
    with pytest.raises(Exception, match="unknown storage option"):
        Model().read(url, storage_options={"not_an_option": "1"})
    # file:// takes no options, and local paths ignore them.
    small_model().write(b.unique("x.nt"), storage_options=b.storage_options)


def test_local_paths_still_work(tmp_path):
    m = small_model()
    p = tmp_path / "x.ttl"
    m.write(str(p), format="turtle")
    m2 = Model()
    m2.read(p)
    assert count(m2) == 4
    m.serialize(tmp_path / "m.maplib")
    assert count(Model.deserialize(tmp_path / "m.maplib")) == 4

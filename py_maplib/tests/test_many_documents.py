"""Mapping many JSON and XML documents at once: the triples of the documents are added in chunks,
so these check that this gives the same triples as mapping the documents one at a time, across
chunk boundaries, and that a document that fails still leaves the documents before it mapped."""

import json

import pytest

from maplib import MaplibException, Model

from .disk import disk_params, storage_kwargs

N = 2500  # more than one chunk of documents


def json_docs(n=N):
    return [json.dumps({"id": i, "name": f"n{i}", "tags": ["a", "b"], "v": i * 0.5,
                        "ok": i % 2 == 0, "sub": {"x": None}}) for i in range(n)]


def xml_docs(n=N):
    return [f"<r id='{i}'><name>n{i}</name><v>{i}</v><e/></r>" for i in range(n)]


def lines(m):
    return sorted(m.write_bytes(format="ntriples").decode().splitlines())


def one_by_one(kind, docs, **kwargs):
    m = Model(**kwargs)
    for i, d in enumerate(docs):
        getattr(m, f"map_{kind}")(d, uuid_namespace=f"urn:ns/{i}")
    return m


@pytest.mark.parametrize("disk", disk_params())
@pytest.mark.parametrize("kind,docs", [("json", json_docs), ("xml", xml_docs)])
def test_many_documents_same_as_one_by_one(disk, kind, docs):
    expected = lines(one_by_one(kind, docs()))
    m = Model(**storage_kwargs(disk))
    getattr(m, f"map_{kind}")(docs(), uuid_namespace="urn:ns")
    assert lines(m) == expected


@pytest.mark.parametrize("kind,docs", [("json", json_docs), ("xml", xml_docs)])
def test_many_documents_into_a_graph(kind, docs):
    m = Model()
    getattr(m, f"map_{kind}")(docs(10), uuid_namespace="urn:ns", graph="urn:g")
    assert m.size() == 0
    assert m.size("urn:g") == one_by_one(kind, docs(10)).size()


@pytest.mark.parametrize("kind,docs,bad", [("json", json_docs, "{ not json"), ("xml", xml_docs, "<r><unclosed></r>")])
def test_failing_document_keeps_the_documents_before_it(kind, docs, bad):
    good = docs(1500)
    m = Model()
    with pytest.raises(MaplibException):
        getattr(m, f"map_{kind}")(good + [bad] + docs(10), uuid_namespace="urn:ns")
    assert lines(m) == lines(one_by_one(kind, good))


def test_missing_file_keeps_the_documents_before_it(tmp_path):
    paths = []
    for i, d in enumerate(json_docs(5)):
        p = tmp_path / f"d{i}.json"
        p.write_text(d)
        paths.append(p)
    m = Model()
    with pytest.raises(MaplibException):
        m.map_json(paths[:3] + [tmp_path / "missing.json"] + paths[3:])
    expected = Model()
    for p in paths[:3]:
        expected.map_json(p)
    assert lines(m) == lines(expected)


@pytest.mark.parametrize("kind,docs", [("json", json_docs), ("xml", xml_docs)])
def test_stream_of_many_documents_same_as_one_by_one(kind, docs):
    m = Model()
    stream = getattr(m, f"map_{kind}")(iter(docs()), uuid_namespace="urn:ns", stream=True,
                                       documents_per_batch=700, graph="urn:b", in_place=True)
    stream.insert("CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <urn:b> { ?s ?p ?o } }").drop_graphs(["urn:b"]).collect()
    assert lines(m) == lines(one_by_one(kind, docs()))

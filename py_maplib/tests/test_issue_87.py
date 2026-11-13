import polars as pl
import pytest

from maplib import Model

TEMPLATE = """
@prefix ex:<http://example.net/ns#>.
ex:ExampleTemplate [ ottr:IRI ?s, xsd:string ?v ] :: {
    ottr:Triple(?s, ex:hasValue, ?v)
} .
"""

N = 1000


def _df():
    return pl.DataFrame(
        {
            "s": [f"http://example.net/ns#s{i}" for i in range(N)],
            "v": [f"v{i}" for i in range(N)],
        }
    )


def _count(m):
    return m.query("SELECT ?s ?v WHERE { ?s ?p ?v }").height


def _map_template(m, data):
    m.add_template(TEMPLATE)
    m.map("http://example.net/ns#ExampleTemplate", data)


def test_map_lazyframe():
    m = Model()
    _map_template(m, _df().lazy())
    assert _count(m) == N


def test_map_lazyframe_with_preprocessing():
    m = Model()
    lf = (
        pl.LazyFrame({"i": list(range(N))})
        .with_columns(
            s=pl.format("http://example.net/ns#s{}", pl.col("i")),
            v=pl.format("v{}", pl.col("i")),
        )
        .select("s", "v")
    )
    _map_template(m, lf)
    assert _count(m) == N


def test_map_iterable_of_dataframes():
    m = Model()
    df = _df()
    _map_template(m, (df.slice(i, 100) for i in range(0, N, 100)))
    assert _count(m) == N


def test_map_list_of_mixed():
    m = Model()
    df = _df()
    _map_template(m, [df.slice(0, 300), df.slice(300, 300).lazy(), df.slice(600)])
    assert _count(m) == N


def test_map_iterable_bad_item():
    m = Model()
    with pytest.raises(Exception, match="(Iterable mapping input|no batches)"):
        _map_template(m, [_df(), 1])


def test_map_triples_lazyframe_and_iterable():
    df = _df().rename({"s": "subject", "v": "object"})
    m = Model()
    m.map_triples(df.lazy(), predicate="http://example.net/ns#p")
    assert _count(m) == N

    m = Model()
    m.map_triples(
        iter([df.slice(0, 500), df.slice(500)]), predicate="http://example.net/ns#p"
    )
    assert _count(m) == N


def test_map_default_lazyframe():
    m = Model()
    m.map_default(_df().lazy(), "s")
    assert _count(m) == N


def test_map_default_iterable_nulls_in_later_batch():
    df = pl.DataFrame(
        {
            "s": [f"http://example.net/ns#s{i}" for i in range(4)],
            "v": ["a", "b", None, "d"],
        }
    )
    m = Model()
    tmpl = m.map_default([df.slice(0, 2), df.slice(2)], "s")
    assert "default_template" in tmpl
    assert _count(m) == 3


def test_map_default_empty_iterable():
    m = Model()
    with pytest.raises(Exception, match="(Iterable mapping input|no batches)"):
        m.map_default([], "s")


# Streaming: m.map(..., stream=True) returns a lazy ModelStream that maps, transforms and writes
# one batch at a time when collected.

EX = "http://example.net/ns#"

UPDATE = """
PREFIX ex:<http://example.net/ns#>
INSERT { ?s ex:upper ?u } WHERE { ?s ex:hasValue ?v BIND(UCASE(?v) AS ?u) }
"""

DELETE_ODD = """
PREFIX ex:<http://example.net/ns#>
DELETE { ?s ex:hasValue ?v } WHERE { ?s ex:hasValue ?v FILTER(REGEX(?v, "[13579]$")) }
"""

CONSTRUCT = """
PREFIX ex:<http://example.net/ns#>
CONSTRUCT { ?s a ex:Thing } WHERE { ?s ex:upper ?u }
"""

RULES = """
PREFIX ex:<http://example.net/ns#>
[?s, ex:hasUpper, ?u] :- [?s, ex:upper, ?u] .
"""


def _batches(n_batches=10):
    df = _df()
    size = N // n_batches
    return (df.slice(i, size) for i in range(0, N, size))


def _read(path, format):
    m = Model()
    m.read(str(path), format=format)
    return m


def _count_pred(m, pred):
    return m.query(f"SELECT ?s ?o WHERE {{ ?s <{EX}{pred}> ?o }}").height


def _has_datalog():
    m = Model()
    try:
        m.infer(RULES)
        return True
    except BaseException as e:
        if "Contact Data Treehouse" in str(e):
            return False
        raise


def test_stream_update_insert_write_write(tmp_path):
    m = Model()
    m.add_template(TEMPLATE)
    (
        m.map(f"{EX}ExampleTemplate", _batches(), stream=True)
        .update(UPDATE)
        .insert(CONSTRUCT)
        .write(tmp_path / "out.nt", format="ntriples")
        .write(tmp_path / "out.ttl", format="turtle")
        .collect()
    )
    # The model itself is not changed
    assert m.size() == 0
    for path, format in [(tmp_path / "out.nt", "ntriples"), (tmp_path / "out.ttl", "turtle")]:
        out = _read(path, format)
        assert _count_pred(out, "hasValue") == N
        assert _count_pred(out, "upper") == N
        assert out.query(f"SELECT ?s WHERE {{ ?s a <{EX}Thing> }}").height == N
    # Prefixes are declared once in the turtle output
    ttl = (tmp_path / "out.ttl").read_text()
    assert ttl.count("@prefix rdf:") == 1


def test_stream_writes_between_steps(tmp_path):
    m = Model()
    m.add_template(TEMPLATE)
    (
        m.map(f"{EX}ExampleTemplate", _batches(), stream=True)
        .write(tmp_path / "before.nt")
        .update(DELETE_ODD)
        .write(tmp_path / "after.nt")
        .collect()
    )
    assert _count_pred(_read(tmp_path / "before.nt", "ntriples"), "hasValue") == N
    assert _count_pred(_read(tmp_path / "after.nt", "ntriples"), "hasValue") == N // 2


def test_stream_infer(tmp_path):
    if not _has_datalog():
        pytest.skip("Datalog requires maplib Enterprise Edition")
    m = Model()
    m.add_template(TEMPLATE)
    (
        m.map(f"{EX}ExampleTemplate", _batches(), stream=True)
        .update(UPDATE)
        .infer(RULES)
        .insert(CONSTRUCT)
        .write(tmp_path / "out.nt")
        .write(tmp_path / "out.ttl", format="turtle")
        .collect()
    )
    for path, format in [(tmp_path / "out.nt", "ntriples"), (tmp_path / "out.ttl", "turtle")]:
        assert _count_pred(_read(path, format), "hasUpper") == N


def test_stream_builder_is_immutable(tmp_path):
    m = Model()
    m.add_template(TEMPLATE)
    s = m.map(f"{EX}ExampleTemplate", _df(), stream=True).write(tmp_path / "plain.nt")
    s.update(UPDATE).write(tmp_path / "updated.nt")  # never collected
    s.collect()
    out = _read(tmp_path / "plain.nt", "ntriples")
    assert _count_pred(out, "hasValue") == N
    assert _count_pred(out, "upper") == 0
    assert not (tmp_path / "updated.nt").exists()


def test_stream_blank_nodes_unique_across_batches(tmp_path):
    m = Model()
    m.add_template(
        """
@prefix ex:<http://example.net/ns#>.
ex:BlankTemplate [ xsd:string ?v ] :: {
    ottr:Triple(_:b, ex:hasValue, ?v)
} .
"""
    )
    (
        m.map(f"{EX}BlankTemplate", (df.select("v") for df in _batches()), stream=True)
        .update(
            """PREFIX ex:<http://example.net/ns#>
            INSERT { ?s ex:node _:n } WHERE { ?s ex:hasValue ?v }"""
        )
        .write(tmp_path / "out.nt")
        .collect()
    )
    out = _read(tmp_path / "out.nt", "ntriples")
    subjects = out.query(f"SELECT DISTINCT ?s WHERE {{ ?s <{EX}hasValue> ?v }}")
    assert subjects.height == N
    nodes = out.query(f"SELECT DISTINCT ?n WHERE {{ ?s <{EX}node> ?n }}")
    assert nodes.height == N


def test_stream_map_triples(tmp_path):
    df = _df().rename({"s": "subject", "v": "object"})
    m = Model()
    (
        m.map_triples(df.lazy(), predicate=f"{EX}p", stream=True)
        .write(tmp_path / "out.nq", format="nquads")
        .collect()
    )
    assert _count_pred(_read(tmp_path / "out.nq", "nquads"), "p") == N


def test_stream_map_default(tmp_path):
    m = Model()
    s = m.map_default(list(_batches()), "s", stream=True)
    s.write(tmp_path / "out.ttl", format="turtle").collect()
    out = _read(tmp_path / "out.ttl", "turtle")
    assert out.query("SELECT ?s ?v WHERE { ?s ?p ?v FILTER(isLiteral(?v)) }").height == N
    assert m.size() == 0


def test_stream_unsupported_write_format(tmp_path):
    m = Model()
    m.add_template(TEMPLATE)
    s = m.map(f"{EX}ExampleTemplate", _df(), stream=True)
    with pytest.raises(Exception, match="Streams can only be written"):
        s.write(tmp_path / "out.xml", format="rdf/xml")


def test_stream_requires_data():
    m = Model()
    m.add_template(TEMPLATE)
    with pytest.raises(Exception, match="requires data"):
        m.map(f"{EX}ExampleTemplate", stream=True)


# JSON and XML: iterables of documents, and streams over them.

FX = """
PREFIX fx:  <http://sparql.xyz/facade-x/ns/>
PREFIX xyz: <http://sparql.xyz/facade-x/data/>
PREFIX ex: <http://example.net/ns#>
"""

N_DOCS = 50


def _json_docs():
    return (f'{{"id": {i}, "name": "n{i}"}}' for i in range(N_DOCS))


def _xml_docs():
    return (f"<person><name>n{i}</name></person>" for i in range(N_DOCS))


JSON_NAMES = FX + "SELECT ?r ?n WHERE { ?r a fx:root . ?r xyz:name ?n }"

XML_NAMES = FX + """
SELECT ?r ?n WHERE {
    ?r a fx:root . ?r a xyz:person .
    ?r fx:child ?name . ?name a xyz:name . ?name fx:child ?n
}"""

JSON_CONSTRUCT = FX + "CONSTRUCT { ?r ex:name ?n } WHERE { ?r a fx:root . ?r xyz:name ?n }"

XML_CONSTRUCT = FX + """
CONSTRUCT { ?r ex:name ?n } WHERE {
    ?r a fx:root . ?r fx:child ?name . ?name a xyz:name . ?name fx:child ?n
}"""


def _distinct(df, c):
    return df.get_column(c).n_unique()


@pytest.mark.parametrize("uuid_namespace", [None, "http://example.net/docs"])
def test_map_json_iterable(uuid_namespace):
    m = Model()
    m.map_json(_json_docs(), uuid_namespace=uuid_namespace)
    df = m.query(JSON_NAMES)
    assert df.height == N_DOCS
    # Each document gets its own root, also with a shared uuid namespace
    assert _distinct(df, "r") == N_DOCS


def test_map_json_iterable_uuid_namespace_is_deterministic():
    roots = []
    for _ in range(2):
        m = Model()
        m.map_json(list(_json_docs()), uuid_namespace="http://example.net/docs")
        roots.append(set(m.query(JSON_NAMES).get_column("r").to_list()))
    assert roots[0] == roots[1]


def test_map_json_iterable_of_paths(tmp_path):
    paths = []
    for i, doc in enumerate(_json_docs()):
        p = tmp_path / f"{i}.json"
        p.write_text(doc)
        paths.append(p)
    m = Model()
    m.map_json(paths[:25])
    m.map_json(str(p) for p in paths[25:])
    df = m.query(JSON_NAMES)
    assert df.height == N_DOCS
    assert _distinct(df, "r") == N_DOCS


def test_map_xml_iterable():
    m = Model()
    m.map_xml(list(_xml_docs()), uuid_namespace="http://example.net/docs")
    df = m.query(XML_NAMES)
    assert df.height == N_DOCS
    assert _distinct(df, "r") == N_DOCS


def test_map_json_iterable_bad_item():
    m = Model()
    with pytest.raises(Exception, match="only contain strings and paths"):
        m.map_json(['{"a": 1}', 1])


@pytest.mark.parametrize("documents_per_batch", [1, 7, 1000])
def test_stream_map_json_transient_construct(tmp_path, documents_per_batch):
    m = Model()
    (
        m.map_json(
            _json_docs(),
            transient=True,
            stream=True,
            documents_per_batch=documents_per_batch,
        )
        .insert(JSON_CONSTRUCT)
        .write(tmp_path / "out.nt")
        .write(tmp_path / "out.ttl", format="turtle")
        .collect()
    )
    assert m.size() == 0
    for path, format in [(tmp_path / "out.nt", "ntriples"), (tmp_path / "out.ttl", "turtle")]:
        out = _read(path, format)
        # Only the constructed triples are written, the mapped document triples are transient
        assert out.size() == N_DOCS
        df = out.query(f"SELECT ?r ?n WHERE {{ ?r <{EX}name> ?n }}")
        assert _distinct(df, "r") == N_DOCS
        assert _distinct(df, "n") == N_DOCS


def test_stream_map_json_single_document(tmp_path):
    m = Model()
    m.map_json('{"name": "n0"}', stream=True).insert(JSON_CONSTRUCT).write(
        tmp_path / "out.nt"
    ).collect()
    assert _count_pred(_read(tmp_path / "out.nt", "ntriples"), "name") == 1


def test_stream_map_xml_update_write(tmp_path):
    m = Model()
    (
        m.map_xml(
            _xml_docs(),
            uuid_namespace="http://example.net/docs",
            stream=True,
            documents_per_batch=10,
        )
        .write(tmp_path / "raw.nt")
        .insert(XML_CONSTRUCT)
        .write(tmp_path / "out.nq", format="nquads")
        .collect()
    )
    raw = _read(tmp_path / "raw.nt", "ntriples")
    assert raw.query(XML_NAMES).height == N_DOCS
    out = _read(tmp_path / "out.nq", "nquads")
    df = out.query(f"SELECT ?r ?n WHERE {{ ?r <{EX}name> ?n }}")
    assert df.height == N_DOCS
    assert _distinct(df, "r") == N_DOCS


def test_stream_map_json_documents_per_batch_zero():
    m = Model()
    with pytest.raises(Exception, match="documents_per_batch"):
        m.map_json(_json_docs(), stream=True, documents_per_batch=0)

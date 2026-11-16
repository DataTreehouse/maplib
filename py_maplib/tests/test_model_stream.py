"""Tests for ModelStream (m.map(..., stream=True) and friends).

The basic pipeline tests live in test_issue_87.py. These cover the contract around them: that a
streamed pipeline gives the same triples as the eager one when no step depends on other batches,
batch boundaries and empty batches, graph handling in steps and writes, errors in the middle of a
stream, what is read from the parent model and when, re-collecting, and the output formats.
"""

import io

import polars as pl
import pytest

from maplib import Model, RDFType, SolutionMappings, TRANSIENT_GRAPH

EX = "http://example.net/ns#"
G1 = "http://example.net/graphs/g1"
G2 = "http://example.net/graphs/g2"
TEMPLATE_IRI = f"{EX}ExampleTemplate"

TEMPLATE = """
@prefix ex:<http://example.net/ns#>.
ex:ExampleTemplate [ ottr:IRI ?s, xsd:string ?v ] :: {
    ottr:Triple(?s, ex:hasValue, ?v)
} .
"""

UPDATE = """
PREFIX ex:<http://example.net/ns#>
INSERT { ?s ex:upper ?u } WHERE { ?s ex:hasValue ?v BIND(UCASE(?v) AS ?u) }
"""

CONSTRUCT = """
PREFIX ex:<http://example.net/ns#>
CONSTRUCT { ?s a ex:Thing } WHERE { ?s ex:upper ?u }
"""

N = 200


def _df(n=N, start=0):
    return pl.DataFrame(
        {
            "s": [f"{EX}s{i}" for i in range(start, start + n)],
            "v": [f"v{i}" for i in range(start, start + n)],
        }
    )


def _batches(n=N, size=20):
    df = _df(n)
    return [df.slice(i, size) for i in range(0, n, size)]


def _model():
    m = Model()
    m.add_template(TEMPLATE)
    return m


def _read(path, format="ntriples"):
    m = Model()
    m.read(str(path), format=format)
    return m


def _lines(data: bytes):
    return sorted(line for line in data.decode().splitlines() if line.strip())


def _count_pred(m, pred, graph=None):
    return m.query(f"SELECT ?s ?o WHERE {{ ?s <{EX}{pred}> ?o }}", graph=graph).height


# Equivalence with the eager pipeline


@pytest.mark.parametrize("batch_size", [1, 7, N, 10 * N])
def test_stream_equals_eager_pipeline(batch_size):
    eager = _model()
    eager.map(TEMPLATE_IRI, _df())
    eager.update(UPDATE)
    eager.insert(CONSTRUCT)
    expected = _lines(eager.write_bytes(format="ntriples"))

    m = _model()
    buf = io.BytesIO()
    m.map(TEMPLATE_IRI, _batches(size=batch_size), stream=True).update(UPDATE).insert(
        CONSTRUCT
    ).write(buf).collect()
    assert _lines(buf.getvalue()) == expected
    assert len(expected) == 3 * N


@pytest.mark.parametrize("format", ["ntriples", "turtle", "nquads"])
def test_stream_formats_round_trip_to_same_triples(tmp_path, format):
    m = _model()
    path = tmp_path / f"out.{format}"
    m.map(TEMPLATE_IRI, _batches(size=13), stream=True).update(UPDATE).write(
        path, format=format
    ).collect()
    eager = _model()
    eager.map(TEMPLATE_IRI, _df())
    eager.update(UPDATE)
    assert _lines(_read(path, format).write_bytes(format="ntriples")) == _lines(
        eager.write_bytes(format="ntriples")
    )


# Batch boundaries and input kinds


def test_stream_turtle_subject_split_over_batches_is_valid(tmp_path):
    # The same subject is in two batches, so it gets two subject blocks in the turtle output.
    df = pl.DataFrame({"s": [f"{EX}a", f"{EX}a"], "v": ["x", "y"]})
    m = _model()
    path = tmp_path / "out.ttl"
    m.map(TEMPLATE_IRI, [df.slice(0, 1), df.slice(1, 1)], stream=True).write(
        path, format="turtle"
    ).collect()
    assert path.read_text().count(f"<{EX}a>") + path.read_text().count("ex:a") == 2
    out = _read(path, "turtle")
    assert sorted(out.query(f"SELECT ?v WHERE {{ <{EX}a> <{EX}hasValue> ?v }}")["v"]) == [
        "x",
        "y",
    ]


def test_stream_duplicate_triples_across_batches_are_not_deduplicated():
    # Each batch is its own model, so a triple in two batches is written twice. Readers deduplicate.
    df = _df(5)
    m = _model()
    buf = io.BytesIO()
    m.map(TEMPLATE_IRI, [df, df], stream=True).write(buf).collect()
    lines = [x for x in buf.getvalue().decode().splitlines() if x.strip()]
    assert len(lines) == 10
    m2 = Model()
    m2.reads(buf.getvalue().decode(), format="ntriples")
    assert m2.size() == 5


def test_stream_skips_empty_batches():
    m = _model()
    empty = _df(0)
    buf = io.BytesIO()
    m.map(
        TEMPLATE_IRI, [empty, _df(3), empty, empty, _df(2, start=3), empty], stream=True
    ).write(buf).collect()
    assert len(_lines(buf.getvalue())) == 5


@pytest.mark.parametrize("data", ["eager_empty", "list_of_empty", "empty_list"])
def test_stream_with_no_rows_creates_empty_files(tmp_path, data):
    data = {"eager_empty": _df(0), "list_of_empty": [_df(0), _df(0)], "empty_list": []}[data]
    path = tmp_path / "out.nt"
    path.write_text("stale content\n")
    m = _model()
    m.map(TEMPLATE_IRI, data, stream=True).write(path).collect()
    # The file is truncated when the stream is collected
    assert path.read_text() == ""


@pytest.mark.parametrize(
    "make",
    [
        pytest.param(lambda: _df(), id="dataframe"),
        pytest.param(lambda: _df().lazy(), id="lazyframe"),
        pytest.param(lambda: _batches(), id="list"),
        pytest.param(lambda: iter(_batches()), id="iterator"),
        pytest.param(lambda: (b.lazy() for b in _batches()), id="generator_of_lazyframes"),
        pytest.param(lambda: tuple(_batches()), id="tuple"),
    ],
)
def test_stream_input_kinds(make):
    m = _model()
    buf = io.BytesIO()
    m.map(TEMPLATE_IRI, make(), stream=True).write(buf).collect()
    assert len(_lines(buf.getvalue())) == N


def test_stream_solution_mappings_batches():
    m = _model()
    buf = io.BytesIO()
    batches = [SolutionMappings(b, {"s": RDFType.IRI}) for b in _batches()]
    m.map(TEMPLATE_IRI, batches, stream=True).write(buf).collect()
    assert len(_lines(buf.getvalue())) == N


def test_stream_many_tiny_batches(tmp_path):
    n = 2000
    m = _model()
    m.map(TEMPLATE_IRI, (_df(1, start=i) for i in range(n)), stream=True).update(
        UPDATE
    ).write(tmp_path / "out.nt").collect()
    out = _read(tmp_path / "out.nt")
    assert _count_pred(out, "hasValue") == n
    assert _count_pred(out, "upper") == n


# Steps only see their own batch


def test_stream_steps_do_not_see_other_batches():
    # A join across batches finds nothing: s0 and s1 are in different batches.
    link = f"""
    PREFIX ex:<{EX}>
    INSERT {{ ?a ex:sameValueAs ?b }} WHERE {{
        ?a ex:hasValue ?v . ?b ex:hasValue ?v . FILTER(?a != ?b)
    }}"""
    df = pl.DataFrame({"s": [f"{EX}s0", f"{EX}s1"], "v": ["same", "same"]})
    m = _model()
    split, together = io.BytesIO(), io.BytesIO()
    m.map(TEMPLATE_IRI, [df.slice(0, 1), df.slice(1, 1)], stream=True).update(link).write(
        split
    ).collect()
    m.map(TEMPLATE_IRI, [df], stream=True).update(link).write(together).collect()
    assert "sameValueAs" not in split.getvalue().decode()
    assert together.getvalue().decode().count("sameValueAs") == 2


def test_stream_steps_do_not_see_parent_triples_and_do_not_change_them():
    m = _model()
    m.map(TEMPLATE_IRI, _df(3))
    before = _lines(m.write_bytes(format="ntriples"))
    buf = io.BytesIO()
    m.map(TEMPLATE_IRI, _df(2, start=100), stream=True).update(UPDATE).write(buf).collect()
    out = buf.getvalue().decode()
    # Only the two streamed subjects, none of the three already in the model
    assert out.count("hasValue") == 2 and out.count("upper") == 2
    assert f"<{EX}s0>" not in out
    assert _lines(m.write_bytes(format="ntriples")) == before


def test_stream_reads_templates_and_prefixes_at_collect_time(tmp_path):
    m = Model()
    s = m.map(TEMPLATE_IRI, _batches(), stream=True).write(tmp_path / "out.ttl", format="turtle")
    # The template and the prefix are added after the stream is built, before it is collected
    m.add_template(TEMPLATE)
    m.add_prefixes({"zz": EX})
    s.collect()
    ttl = (tmp_path / "out.ttl").read_text()
    assert ttl.count(f"@prefix zz: <{EX}>") == 1
    assert "zz:s0 " in ttl
    assert _count_pred(_read(tmp_path / "out.ttl", "turtle"), "hasValue") == N


def test_stream_write_prefixes_are_merged_with_model_prefixes(tmp_path):
    m = _model()
    m.add_prefixes({"ex": EX})
    m.map(TEMPLATE_IRI, _batches(), stream=True).write(
        tmp_path / "out.ttl", format="turtle", prefixes={"vv": "http://www.w3.org/2001/XMLSchema#"}
    ).collect()
    ttl = (tmp_path / "out.ttl").read_text()
    assert ttl.count(f"@prefix ex: <{EX}>") == 1
    assert ttl.count("@prefix vv: <http://www.w3.org/2001/XMLSchema#>") == 1
    # All prefix declarations come before the first triple
    lines = ttl.splitlines()
    last_prefix = max(i for i, x in enumerate(lines) if x.startswith("@prefix"))
    assert all(not x.startswith("@prefix") for x in lines[last_prefix + 1 :])
    assert _count_pred(_read(tmp_path / "out.ttl", "turtle"), "hasValue") == N


# Graphs


def test_stream_map_into_named_graph_and_write_graphs(tmp_path):
    m = _model()
    (
        m.map(TEMPLATE_IRI, _batches(), graph=G1, stream=True)
        .insert(CONSTRUCT.replace("?s ex:upper ?u", "?s ex:hasValue ?u"), source_graph=G1, target_graph=G2)
        .write(tmp_path / "default.nt")
        .write(tmp_path / "g1.nt", graph=G1)
        .write(tmp_path / "g2.ttl", format="turtle", graph=G2)
        .write(tmp_path / "all.nq", format="nquads")
        .write(tmp_path / "g2.nq", format="nquads", graph=G2)
        .collect()
    )
    assert (tmp_path / "default.nt").read_text() == ""
    assert _count_pred(_read(tmp_path / "g1.nt"), "hasValue") == N
    g2 = _read(tmp_path / "g2.ttl", "turtle")
    assert g2.size() == N
    assert g2.query(f"SELECT ?s WHERE {{ ?s a <{EX}Thing> }}").height == N
    all_nq = (tmp_path / "all.nq").read_text()
    assert all_nq.count(f"<{G1}>") == N and all_nq.count(f"<{G2}>") == N
    g2_nq = (tmp_path / "g2.nq").read_text()
    assert g2_nq.count(f"<{G2}>") == N and f"<{G1}>" not in g2_nq


def test_stream_transient_graph_is_not_written():
    m = _model()
    nt, nq = io.BytesIO(), io.BytesIO()
    (
        m.map(TEMPLATE_IRI, _batches(), stream=True)
        .update(UPDATE)
        .insert(CONSTRUCT, target_graph=TRANSIENT_GRAPH)
        .write(nt)
        .write(nq, format="nquads")
        .collect()
    )
    for buf in (nt, nq):
        out = buf.getvalue().decode()
        assert out.count("hasValue") == N
        assert "Thing" not in out


def test_stream_update_on_named_graph_matches_eager():
    eager = _model()
    eager.map(TEMPLATE_IRI, _df(), graph=G1)
    eager.update(UPDATE, graph=G1)
    m = _model()
    buf = io.BytesIO()
    (
        m.map(TEMPLATE_IRI, _batches(), graph=G1, stream=True)
        .update(UPDATE, graph=G1)
        .write(buf, format="nquads")
        .collect()
    )
    out = buf.getvalue().decode()
    assert out.count("upper") == N
    assert _lines(buf.getvalue()) == _lines(eager.write_bytes(format="nquads"))


def test_stream_update_with_parameters():
    params = {
        "keep": SolutionMappings(
            pl.DataFrame({"s": [f"{EX}s1", f"{EX}s150"]}), {"s": RDFType.IRI}
        )
    }
    upd = f"""
    PREFIX ex:<{EX}>
    INSERT {{ ?s ex:kept true }} WHERE {{ PVALUES (?s) keep ?s ex:hasValue ?v }}"""
    m = _model()
    buf = io.BytesIO()
    m.map(TEMPLATE_IRI, _batches(), stream=True).update(upd, parameters=params).write(
        buf
    ).collect()
    kept = [x for x in buf.getvalue().decode().splitlines() if "kept" in x]
    assert sorted(x.split()[0] for x in kept) == [f"<{EX}s150>", f"<{EX}s1>"]


def test_stream_step_arguments_are_checked_when_added(tmp_path):
    s = _model().map(TEMPLATE_IRI, _df(), stream=True)
    with pytest.raises(Exception):
        s.update(UPDATE, graph="not an iri")
    with pytest.raises(Exception):
        s.insert(CONSTRUCT, target_graph="not an iri")
    with pytest.raises(Exception, match="ruleset"):
        s.infer(3)
    with pytest.raises(Exception):
        s.write(tmp_path / "x.nt", graph="not an iri")
    with pytest.raises(Exception):
        s.write(tmp_path / "x.nt", format="no such format")
    assert not (tmp_path / "x.nt").exists()


# Errors in the middle of a stream


def test_stream_error_from_input_iterator_propagates(tmp_path):
    class Boom(Exception):
        pass

    def gen():
        yield _df(5)
        yield _df(5, start=5)
        raise Boom("input failed")

    m = _model()
    with pytest.raises(Exception, match="input failed"):
        m.map(TEMPLATE_IRI, gen(), stream=True).write(tmp_path / "out.nt").collect()
    # The model is unchanged and still usable, and the stream can be run again
    assert m.size() == 0
    m.map(TEMPLATE_IRI, _batches(), stream=True).write(tmp_path / "out.nt").collect()
    assert _count_pred(_read(tmp_path / "out.nt"), "hasValue") == N


def test_stream_error_in_step_propagates_and_model_keeps_templates(tmp_path):
    m = _model()
    bad = "PREFIX ex:<http://example.net/ns#> INSERT { ?s ex:p ?o } WHERE { ?s ex:nope }"
    with pytest.raises(Exception):
        m.map(TEMPLATE_IRI, _batches(), stream=True).update(bad).write(tmp_path / "x.nt").collect()
    # A batch in a later position fails: the templates are handed on also after an error
    df_bad = pl.DataFrame({"s": ["not an iri"], "v": ["x"]})
    with pytest.raises(Exception):
        m.map(TEMPLATE_IRI, [_df(3), df_bad], stream=True, validate_iris=True).write(
            tmp_path / "x.nt"
        ).collect()
    assert len(m.get_templates()) == 1
    buf = io.BytesIO()
    m.map(TEMPLATE_IRI, _batches(), stream=True).write(buf).collect()
    assert len(_lines(buf.getvalue())) == N
    m.map(TEMPLATE_IRI, _df(3))
    assert m.size() == 3


def test_stream_error_from_write_target_propagates():
    class FailingWriter(io.RawIOBase):
        def writable(self):
            return True

        def write(self, b):
            raise OSError("disk full")

    m = _model()
    with pytest.raises(Exception, match="disk full"):
        m.map(TEMPLATE_IRI, _batches(), stream=True).write(FailingWriter()).collect()


def test_stream_unknown_template_fails_at_collect():
    m = Model()
    s = m.map(f"{EX}Missing", _df(), stream=True)
    with pytest.raises(Exception):
        s.collect()


# Re-collecting and reuse


def test_stream_collect_twice_with_reusable_input_gives_same_output(tmp_path):
    m = _model()
    s = m.map(TEMPLATE_IRI, _batches(), stream=True).update(UPDATE).write(tmp_path / "out.nt")
    s.collect()
    first = _lines((tmp_path / "out.nt").read_bytes())
    s.collect()
    # The file is truncated, not appended to, and the triples are the same (the order may differ)
    assert _lines((tmp_path / "out.nt").read_bytes()) == first
    assert len(first) == 2 * N


def test_stream_collect_twice_consumes_iterator(tmp_path):
    m = _model()
    s = m.map(TEMPLATE_IRI, iter(_batches()), stream=True).write(tmp_path / "out.nt")
    s.collect()
    assert len((tmp_path / "out.nt").read_text().splitlines()) == N
    # The iterator is used up, so the second run writes an empty file
    s.collect()
    assert (tmp_path / "out.nt").read_text() == ""


def test_stream_blank_nodes_unique_across_collects_of_different_streams():
    # Two streams from the same model write blank nodes that do not collide when combined.
    tmpl = """
@prefix ex:<http://example.net/ns#>.
ex:BlankTemplate [ xsd:string ?v ] :: {
    ottr:Triple(_:b, ex:hasValue, ?v)
} .
"""
    m = Model()
    m.add_template(tmpl)
    a, b = io.BytesIO(), io.BytesIO()
    df = _df(10).select("v")
    m.map(f"{EX}BlankTemplate", [df], stream=True).write(a).collect()
    m.map(f"{EX}BlankTemplate", [df], stream=True).write(b).collect()
    labels = [{x.split()[0] for x in buf.getvalue().decode().splitlines() if x.strip()} for buf in (a, b)]
    assert len(labels[0]) == 10 and len(labels[1]) == 10
    # Concatenating the two outputs must not merge distinct blank nodes
    assert not labels[0] & labels[1]
    # Nor may a later eager mapping reuse their labels
    m.map(f"{EX}BlankTemplate", df)
    eager = {x.split()[0] for x in m.writes(format="ntriples").splitlines() if x.strip()}
    assert not eager & (labels[0] | labels[1])


def test_stream_failing_stream_still_advances_blank_node_counters():
    tmpl = """
@prefix ex:<http://example.net/ns#>.
ex:BlankTemplate [ xsd:string ?v ] :: {
    ottr:Triple(_:b, ex:hasValue, ?v)
} .
"""

    def gen():
        yield _df(5).select("v")
        raise ValueError("input failed")

    m = Model()
    m.add_template(tmpl)
    a, b = io.BytesIO(), io.BytesIO()
    with pytest.raises(Exception, match="input failed"):
        m.map(f"{EX}BlankTemplate", gen(), stream=True).write(a).collect()
    m.map(f"{EX}BlankTemplate", [_df(5).select("v")], stream=True).write(b).collect()
    labels = [{x.split()[0] for x in buf.getvalue().decode().splitlines() if x.strip()} for buf in (a, b)]
    # The first batch was written before the error
    assert len(labels[0]) == 5
    assert not labels[0] & labels[1]


def test_stream_without_steps_runs():
    m = _model()
    assert m.map(TEMPLATE_IRI, _batches(), stream=True).collect() is None
    assert m.size() == 0


def test_stream_same_buffer_in_two_write_steps():
    m = _model()
    buf = io.BytesIO()
    m.map(TEMPLATE_IRI, _batches(), stream=True).write(buf).write(buf).collect()
    assert len(_lines(buf.getvalue())) == 2 * N


# Inference


def test_stream_infer_matches_eager():
    rules = f"PREFIX ex:<{EX}>\n[?s, ex:hasUpper, ?u] :- [?s, ex:upper, ?u] ."
    probe = Model()
    try:
        probe.infer(rules)
    except BaseException as e:
        if "Contact Data Treehouse" in str(e):
            pytest.skip("Datalog requires maplib Enterprise Edition")
        raise
    eager = _model()
    eager.map(TEMPLATE_IRI, _df())
    eager.update(UPDATE)
    eager.infer(rules, graph=None)
    expected = _lines(eager.write_bytes(format="nquads"))

    m = _model()
    buf = io.BytesIO()
    m.map(TEMPLATE_IRI, _batches(size=33), stream=True).update(UPDATE).infer(rules).write(
        buf, format="nquads"
    ).collect()
    assert _lines(buf.getvalue()) == expected

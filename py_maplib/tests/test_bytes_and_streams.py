import io
import pathlib

import polars as pl
import pytest

from maplib import Model

NT = """
<urn:maplib:s1> <urn:maplib:p> "plain" .
<urn:maplib:s1> <urn:maplib:lang> "hello"@en .
<urn:maplib:s2> <urn:maplib:int> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
<urn:maplib:s2> <urn:maplib:p> _:b1 .
"""

FORMATS = ["ntriples", "turtle", "trig", "nquads", "rdf/xml", "hdt"]

TEMPLATE = """
@prefix ex:<http://example.net/ns#>.
ex:ExampleTemplate [ ottr:IRI ?s, xsd:string ?v ] :: {
    ottr:Triple(?s, ex:hasValue, ?v)
} .
"""


def _model() -> Model:
    m = Model()
    m.reads(NT, format="ntriples")
    return m


def _read_back(data: bytes, format: str, tmp_path: pathlib.Path) -> Model:
    m = Model()
    if format == "hdt":
        p = tmp_path / "back.hdt"
        p.write_bytes(data)
        m.read(str(p), format="hdt")
    else:
        m.reads(data.decode(), format=format)
    return m


@pytest.mark.parametrize("format", FORMATS)
def test_write_bytes_equals_written_file(tmp_path, format):
    m = _model()
    path = tmp_path / "out"
    m.write(path, format=format)
    data = m.write_bytes(format=format)
    assert isinstance(data, bytes)
    assert data == path.read_bytes()
    assert _read_back(data, format, tmp_path).size() == 4


@pytest.mark.parametrize("format", FORMATS)
def test_write_to_bytes_io(tmp_path, format):
    m = _model()
    buf = io.BytesIO()
    m.write(buf, format=format)
    assert buf.getvalue() == m.write_bytes(format=format)
    # The stream is not closed, so more can be written to it
    assert not buf.closed


def test_write_to_open_binary_file(tmp_path):
    m = _model()
    path = tmp_path / "out.hdt"
    with open(path, "wb") as f:
        f.write(b"")
        m.write(f, format="hdt")
    assert path.read_bytes() == m.write_bytes(format="hdt")


def test_write_to_unbuffered_raw_file(tmp_path):
    m = _model()
    path = tmp_path / "out.nt"
    with open(path, "wb", buffering=0) as f:
        m.write(f, format="ntriples")
    assert path.read_text() == m.writes(format="ntriples")


def test_write_large_output_to_stream_in_chunks():
    # Larger than the internal buffer, so the stream is written to several times
    n = 50_000
    m = Model()
    m.add_template(TEMPLATE)
    df = pl.DataFrame(
        {
            "s": [f"http://example.net/ns#s{i}" for i in range(n)],
            "v": ["x" * 40 + str(i) for i in range(n)],
        }
    )
    m.map("http://example.net/ns#ExampleTemplate", df)

    class Recorder(io.RawIOBase):
        def __init__(self):
            self.chunks = []

        def writable(self):
            return True

        def write(self, b):
            self.chunks.append(bytes(b))
            return len(b)

    r = Recorder()
    m.write(r, format="ntriples")
    assert len(r.chunks) > 1
    assert b"".join(r.chunks) == m.write_bytes(format="ntriples")


def test_write_bytes_default_format_is_ntriples():
    m = _model()
    assert m.write_bytes() == m.writes(format="ntriples").encode()


def test_write_bytes_graph():
    m = Model()
    m.reads(NT, format="ntriples", graph="urn:maplib:g")
    assert m.write_bytes(format="ntriples") == b""
    assert m.write_bytes(format="ntriples", graph="urn:maplib:g") == m.writes(
        format="ntriples", graph="urn:maplib:g"
    ).encode()


def test_writes_hdt_points_to_write_bytes():
    with pytest.raises(Exception, match="write_bytes"):
        _model().writes(format="hdt")


def test_write_to_text_stream_raises():
    with pytest.raises(Exception, match="Python object"):
        _model().write(io.StringIO(), format="ntriples")


def test_python_exception_in_write_is_raised():
    class Failing:
        def write(self, b):
            raise RuntimeError("disk full")

    with pytest.raises(Exception, match="disk full"):
        _model().write(Failing(), format="hdt")


def test_write_rejects_non_path_non_stream():
    with pytest.raises(Exception, match="file-like"):
        _model().write(42, format="ntriples")


def test_write_cim_xml_to_stream():
    m = Model()
    m.reads(
        "<http://example.net/ns#p> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property> .\n",
        format="ntriples",
        graph="urn:maplib:profile",
    )
    m.reads(NT, format="ntriples")
    buf = io.BytesIO()
    try:
        m.write_cim_xml(
            buf,
            profile_graph="urn:maplib:profile",
            model_iri="urn:maplib:model",
            created="2023-09-14T20:27:41Z",
            scenario_time="2023-09-14T02:44:43Z",
        )
    except BaseException as e:
        if "Contact Data Treehouse" in str(e):
            pytest.skip("CIM XML export requires maplib Enterprise Edition")
        raise
    assert b"urn:maplib:model" in buf.getvalue()


def test_stream_write_to_bytes_io(tmp_path):
    m = Model()
    m.add_template(TEMPLATE)
    n = 100
    df = pl.DataFrame(
        {
            "s": [f"http://example.net/ns#s{i}" for i in range(n)],
            "v": [f"v{i}" for i in range(n)],
        }
    )
    batches = [df.slice(0, 50), df.slice(50, 50)]
    buf = io.BytesIO()
    (
        m.map("http://example.net/ns#ExampleTemplate", batches, stream=True)
        .write(buf, format="turtle")
        .write(tmp_path / "out.ttl", format="turtle")
        .collect()
    )
    assert buf.getvalue() == (tmp_path / "out.ttl").read_bytes()
    m2 = Model()
    m2.reads(buf.getvalue().decode(), format="turtle")
    assert m2.size() == n


def _triples(m: Model) -> list:
    return sorted(
        line for line in m.writes(format="ntriples").splitlines() if "_:" not in line
    )


@pytest.mark.parametrize("format", FORMATS)
def test_read_bytes_round_trip(format):
    m = _model()
    data = m.write_bytes(format=format)
    m2 = Model()
    m2.read(data, format=format)
    assert m2.size() == 4
    assert _triples(m2) == _triples(m)


@pytest.mark.parametrize("format", FORMATS)
def test_read_binary_file_like_object(format):
    m = _model()
    m2 = Model()
    m2.read(io.BytesIO(m.write_bytes(format=format)), format=format)
    assert _triples(m2) == _triples(m)


@pytest.mark.parametrize("kind", [bytearray, memoryview])
def test_read_bytearray_and_memoryview(kind):
    m = _model()
    m2 = Model()
    m2.read(kind(m.write_bytes(format="hdt")), format="hdt")
    assert _triples(m2) == _triples(m)


def test_read_text_file_like_object():
    m = Model()
    m.read(io.StringIO('<urn:maplib:s> <urn:maplib:p> "blåbær" .\n'), format="ntriples")
    assert m.writes(format="ntriples") == '<urn:maplib:s> <urn:maplib:p> "blåbær" .\n'


def test_read_open_binary_file(tmp_path):
    m = _model()
    path = tmp_path / "data.hdt"
    m.write(path, format="hdt")
    m2 = Model()
    with open(path, "rb") as f:
        m2.read(f)
    assert _triples(m2) == _triples(m)


def test_read_bytes_detects_hdt_without_format():
    m = _model()
    m2 = Model()
    m2.read(m.write_bytes(format="hdt"))
    assert _triples(m2) == _triples(m)


def test_read_bytes_without_format_raises_for_text():
    with pytest.raises(Exception, match="format must be given"):
        Model().read(NT.encode())


def test_read_bytes_into_graph_and_replace_graph():
    m = _model()
    data = m.write_bytes(format="hdt")
    m2 = Model()
    m2.read(data, format="hdt", graph="urn:maplib:g")
    m2.read(data, format="hdt", graph="urn:maplib:g", replace_graph=True)
    assert m2.size() == 0
    assert m2.size(graph="urn:maplib:g") == 4


def test_read_bytes_skips_utf8_bom():
    m = Model()
    m.read(b"\xef\xbb\xbf" + NT.encode(), format="ntriples")
    assert m.size() == 4


def test_read_still_accepts_paths(tmp_path):
    m = _model()
    path = tmp_path / "data.ttl"
    m.write(path, format="turtle")
    for p in [path, str(path)]:
        m2 = Model()
        m2.read(p)
        assert _triples(m2) == _triples(m)


def test_read_file_like_returning_other_type_raises():
    class Weird:
        def read(self):
            return 42

    with pytest.raises(Exception, match="bytes or str"):
        Model().read(Weird(), format="ntriples")


def test_read_truncated_hdt_bytes_raises():
    data = _model().write_bytes(format="hdt")
    with pytest.raises(Exception):
        Model().read(data[: len(data) // 2], format="hdt")

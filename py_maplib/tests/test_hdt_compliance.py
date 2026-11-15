"""
HDT compliance tests, in both directions:

* Reading: maplib reads HDT files written by the reference implementations (hdt-java,
  hdt-cpp), fetched by ``fetch_hdt_reference.py``. The triples must equal those decoded by
  ``hdt_spec.py``, an independent decoder written from the HDT specification, and their count
  must equal the ``void:triples`` of the file's header. Without the files these tests are
  skipped, unless ``HDT_REFERENCE_REQUIRED=1`` is set (as in CI).
* Writing: the HDT files maplib writes, for the W3C RDF 1.1 test data and the reference
  files, must decode with ``hdt_spec.py`` (format identifiers, CRCs, sorted dictionary
  sections, disjoint shared/subject/object sections, SPO ordered bitmap triples), decode to
  the triples that were written, and have a header that agrees with the data.
* Where the specification leaves details open, the files maplib writes should match what
  hdt-java and hdt-cpp write: one test per such detail (``hdt_spec.DEVIATIONS`` and the
  header vocabulary). The other tests tolerate these deviations, so they still check
  everything else.

The decoder itself is checked against the reference files first, so that a bug in it shows
up as such rather than as a maplib failure. Known failures are strict xfails.
"""

import functools
import hashlib
import os
import re
from pathlib import Path
from typing import List

import pytest

from maplib import Model

from . import hdt_spec
from .fetch_hdt_reference import FILES as REFERENCE_FILES
from .w3c_rdf import isomorphic


def same_graph(expected: List[tuple], actual: List[tuple]) -> bool:
    """Equal up to blank node renaming, with no triple twice in ``actual``."""
    return isomorphic([t + (None,) for t in expected], [t + (None,) for t in actual])

TESTS_DIR = Path(__file__).parent
REFERENCE_DIR = TESTS_DIR / "testdata" / "hdt_reference"
W3C_RDF11_PATH = TESTS_DIR / "testdata" / "w3c_rdf11"
KNOWN_FAILURES_FILE = TESTS_DIR / "hdt_known_failures.txt"

XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"
VOID_TRIPLES = "http://rdfs.org/ns/void#triples"
HDT_NS = "http://purl.org/HDT/hdt#"

# Reference files maplib cannot read yet.
READ_KNOWN_FAILURES = {
    "literals.hdt": "the optional <http://purl.org/HDT/hdt#dictionaryLiteral> is not supported",
}
# Reference files the decoder in hdt_spec.py does not support.
DECODER_UNSUPPORTED = {
    "literals.hdt": "hdt_spec.py only decodes <http://purl.org/HDT/hdt#dictionaryFour>",
}


# ---------------------------------------------------------------------------
# Terms and triples
# ---------------------------------------------------------------------------

_ESCAPES = {"t": "\t", "b": "\b", "n": "\n", "r": "\r", "f": "\f", '"': '"', "'": "'", "\\": "\\"}
_ESCAPE_RE = re.compile(r"\\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8}|.)")
_TERM = (
    r"<(?P<iri>[^>]*)>"
    r"|_:(?P<bnode>\S+)"
    r'|"(?P<lex>(?:[^"\\]|\\.)*)"(?:@(?P<lang>[A-Za-z0-9-]+)|\^\^<(?P<dt>[^>]*)>)?'
)
_TERM_RE = re.compile(_TERM)
_UNNAMED = re.sub(r"\?P<\w+>", "?:", _TERM)
_TRIPLE_RE = re.compile(rf"^\s*({_UNNAMED})\s+({_UNNAMED})\s+({_UNNAMED})\s*\.\s*$")


def unescape(s: str) -> str:
    def repl(m):
        e = m.group(1)
        return chr(int(e[1:], 16)) if e[0] in "uU" else _ESCAPES.get(e, e)

    return _ESCAPE_RE.sub(repl, s)


def parse_term(s: str):
    m = _TERM_RE.fullmatch(s)
    if m is None:
        raise ValueError(f"cannot parse RDF term {s!r}")
    if m.group("iri") is not None:
        return ("uri", unescape(m.group("iri")))
    if m.group("bnode") is not None:
        return ("bnode", m.group("bnode"))
    lang, dt = m.group("lang"), m.group("dt")
    if dt == XSD_STRING:
        dt = None
    return ("literal", unescape(m.group("lex")), dt, lang.lower() if lang else None)


def model_triples(m: Model) -> List[tuple]:
    out = []
    # Not splitlines(), which also splits on control characters inside literals.
    for line in m.writes(format="ntriples").split("\n"):
        if line.strip():
            t = _TRIPLE_RE.match(line)
            assert t is not None, f"cannot parse N-Triples line {line!r}"
            out.append(tuple(parse_term(t.group(i)) for i in (1, 2, 3)))
    return out


def decoded_triples(hdt: hdt_spec.HDT) -> List[tuple]:
    return [tuple(map(hdt_spec.dictionary_term, t)) for t in hdt.triples()]


def canonical(triples: List[tuple], rounds: int = 6):
    """Sorted triples with blank nodes replaced by colours from a few rounds of colour
    refinement, equal for isomorphic graphs whatever the blank node labels."""
    blank = lambda t: t[0] == "bnode"  # noqa: E731
    colour = {t: "" for s, _, o in triples for t in (s, o) if blank(t)}
    term = lambda t: ("bnode", colour[t]) if blank(t) else t  # noqa: E731
    for _ in range(rounds if colour else 0):
        edges = {b: [] for b in colour}
        for s, p, o in triples:
            if blank(s):
                edges[s].append(("out", p, term(o)))
            if blank(o):
                edges[o].append(("in", p, term(s)))
        colour = {
            b: hashlib.sha256(repr((colour[b], sorted(e))).encode()).hexdigest()[:16]
            for b, e in edges.items()
        }
    key = lambda t: tuple(tuple("" if x is None else x for x in term(u)) for u in t)  # noqa: E731
    return sorted(key(t) for t in triples)


def assert_same_triples(expected: List[tuple], actual: List[tuple]):
    assert len(actual) == len(expected)
    # canonical() is only a fingerprint (colour refinement can not tell all non-isomorphic
    # graphs apart), so the check is the exact isomorphism with a search for a blank node
    # bijection; canonical() only shows the differences.
    if not same_graph(expected, actual):
        e, a = canonical(expected), canonical(actual)
        missing = sorted(set(e) - set(a))[:10]
        extra = sorted(set(a) - set(e))[:10]
        pytest.fail(f"triples differ\nmissing: {missing}\nunexpected: {extra}", pytrace=False)


def header_int(hdt: hdt_spec.HDT, predicate: str):
    values = hdt_spec.header_values(hdt.header, predicate)
    return int(values[0]) if values else None


# ---------------------------------------------------------------------------
# Reading reference files
# ---------------------------------------------------------------------------


def reference_params(known: dict):
    if not REFERENCE_DIR.exists() or not all(
        (REFERENCE_DIR / name).exists() for name in REFERENCE_FILES
    ):
        if os.environ.get("HDT_REFERENCE_REQUIRED") == "1":
            raise RuntimeError(
                "HDT_REFERENCE_REQUIRED=1 but the reference HDT files are missing: "
                "run fetch_hdt_reference.py"
            )
        return [
            pytest.param(
                None,
                id="missing",
                marks=pytest.mark.skip(reason="run fetch_hdt_reference.py to fetch them"),
            )
        ]
    params = []
    for name in sorted(REFERENCE_FILES):
        marks = []
        if name in known:
            marks.append(pytest.mark.xfail(reason=known[name], strict=True))
        params.append(pytest.param(name, id=name, marks=marks))
    return params


def decoder_params():
    return [
        pytest.param(
            p.values[0],
            id=p.id,
            marks=[pytest.mark.skip(reason=DECODER_UNSUPPORTED[p.values[0]])]
            if p.values[0] in DECODER_UNSUPPORTED
            else p.marks,
        )
        for p in reference_params({})
    ]


@functools.lru_cache(maxsize=None)
def decode_reference(name: str) -> hdt_spec.HDT:
    return hdt_spec.read_hdt((REFERENCE_DIR / name).read_bytes())


@pytest.mark.parametrize("name", decoder_params())
def test_decoder_reads_reference_file(name):
    hdt = decode_reference(name)
    assert len(hdt.triple_ids) == header_int(hdt, VOID_TRIPLES)


@pytest.mark.parametrize("name", reference_params(READ_KNOWN_FAILURES))
def test_maplib_reads_reference_file(name):
    data = (REFERENCE_DIR / name).read_bytes()
    m = Model()
    m.read(str(REFERENCE_DIR / name), format="hdt")
    actual = model_triples(m)
    count = re.search(rb'<http://rdfs.org/ns/void#triples> "(\d+)"', data)
    assert count is not None, "no void:triples in the header"
    assert len(actual) == int(count.group(1))
    if name not in DECODER_UNSUPPORTED:
        assert_same_triples(decoded_triples(decode_reference(name)), actual)


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def write_and_decode(m: Model, path: Path, tolerate=hdt_spec.DEVIATIONS) -> hdt_spec.HDT:
    m.write(str(path), format="hdt")
    return hdt_spec.read_hdt(path.read_bytes(), tolerate=tolerate)


def check_written_hdt(m: Model, path: Path):
    """Writes ``m`` as HDT and checks the file against the specification."""
    expected = model_triples(m)
    hdt = write_and_decode(m, path)
    assert_same_triples(expected, decoded_triples(hdt))

    # The dictionary holds exactly the terms in use, in the right sections.
    triples = hdt.triples()
    subjects = {s for s, _, _ in triples}
    objects = {o for _, _, o in triples}
    assert set(hdt.shared) == subjects & objects
    assert set(hdt.subjects) == subjects - objects
    assert set(hdt.objects) == objects - subjects
    assert set(hdt.predicates) == {p for _, p, _ in triples}

    # The header describes the data.
    assert header_int(hdt, VOID_TRIPLES) == len(triples)
    for predicate, value in [
        (HDT_NS + "dictionarynumSharedSubjectObject", len(hdt.shared)),
        (HDT_NS + "triplesnumTriples", len(triples)),
        ("http://rdfs.org/ns/void#properties", len(hdt.predicates)),
        ("http://rdfs.org/ns/void#distinctSubjects", len(subjects)),
        ("http://rdfs.org/ns/void#distinctObjects", len(objects)),
    ]:
        stated = header_int(hdt, predicate)
        if stated is not None:
            assert stated == value, predicate


def load_known_failures() -> dict:
    known = {}
    for line in KNOWN_FAILURES_FILE.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            tid, _, reason = line.partition("#")
            known[tid.strip()] = reason.strip() or "known failure"
    return known


def w3c_files(known: dict = {}):
    files = []
    for f in sorted(W3C_RDF11_PATH.glob("rdf-*/*.nt")):
        # The negative syntax tests, and test-38.nt, which no manifest uses and which escapes
        # a surrogate pair.
        if "-bad-" in f.name or f.name == "test-38.nt":
            continue
        tid = f"w3c/{f.parent.name}/{f.name}"
        marks = [pytest.mark.xfail(reason=known[tid], strict=True)] if tid in known else []
        files.append(pytest.param(f, id=tid, marks=marks))
    unknown = {k for k in known if k.startswith("w3c/")} - {p.id for p in files}
    if unknown:
        raise RuntimeError(f"unknown tests in {KNOWN_FAILURES_FILE.name}: {sorted(unknown)}")
    return files


@pytest.mark.parametrize("path", w3c_files(load_known_failures()))
def test_maplib_writes_spec_conformant_hdt_w3c(path, tmp_path):
    m = Model()
    m.read(str(path), format="ntriples")
    if any(t[2][0] == "literal" and "\0" in t[2][1] for t in model_triples(m)):
        # Dictionary strings are NUL terminated, so U+0000 cannot be stored.
        with pytest.raises(Exception, match="U\\+0000"):
            m.write(str(tmp_path / "out.hdt"), format="hdt")
        return
    check_written_hdt(m, tmp_path / "out.hdt")


@pytest.mark.parametrize(
    "name",
    [p for p in reference_params({}) if p.values[0] not in READ_KNOWN_FAILURES],
)
def test_maplib_writes_spec_conformant_hdt_reference(name, tmp_path):
    m = Model()
    m.read(str(REFERENCE_DIR / name), format="hdt")
    check_written_hdt(m, tmp_path / "out.hdt")
    # Re-encoding a reference file gives the same dictionary and triples.
    if name not in DECODER_UNSUPPORTED:
        ref = decode_reference(name)
        ours = hdt_spec.read_hdt(
            (tmp_path / "out.hdt").read_bytes(), tolerate=hdt_spec.DEVIATIONS
        )
        assert same_graph(decoded_triples(ref), decoded_triples(ours))


# ---------------------------------------------------------------------------
# Details where maplib's files should match the reference implementations
# ---------------------------------------------------------------------------


def has_nul(path: Path) -> bool:
    b = path.read_bytes()
    return b"\\u0000" in b or b"\\U00000000" in b or b"\0" in b


def w3c_model() -> Model:
    m = Model()
    for p in w3c_files():
        if not has_nul(p.values[0]):
            m.read(str(p.values[0]), format="ntriples")
    return m


def no_shared_terms_model() -> Model:
    # No term is both a subject and an object, so the shared section is empty.
    m = Model()
    m.reads(
        "<http://a> <http://p> <http://b> .\n<http://a> <http://q> \"x\" .\n",
        format="ntriples",
    )
    return m


@pytest.mark.xfail(
    strict=True,
    reason="the vendored hdt crate writes no block pointer for an empty dictionary section, "
    "hdt-java and hdt-cpp write one (0)",
)
def test_empty_dictionary_section_has_end_pointer(tmp_path):
    hdt = write_and_decode(no_shared_terms_model(), tmp_path / "out.hdt", tolerate=frozenset())
    assert hdt.shared == []


@pytest.mark.xfail(
    strict=True,
    reason="the vendored hdt crate pads bitmap Y to a multiple of 64 bits, hdt-java and "
    "hdt-cpp write one bit per entry of sequence Y",
)
@pytest.mark.parametrize("model", [w3c_model, Model], ids=["w3c", "empty"])
def test_bitmaps_have_one_bit_per_sequence_entry(model, tmp_path):
    hdt = write_and_decode(model(), tmp_path / "out.hdt")
    assert "bitmap-longer-than-sequence" not in hdt.deviations


# The header triples hdt-java and hdt-cpp write, apart from the dataset IRI (here ?d), blank
# node labels and the statistics and publication values. Header triples are compared as
# N-Triples lines.
REFERENCE_HEADER = [
    "?d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/HDT/hdt#Dataset>",
    "?d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdfs.org/ns/void#Dataset>",
    "?d <http://purl.org/HDT/hdt#formatInformation> _:format",
    "?d <http://purl.org/HDT/hdt#statisticalInformation> _:statistics",
    "?d <http://purl.org/HDT/hdt#publicationInformation> _:publicationInformation",
    "_:format <http://purl.org/HDT/hdt#dictionary> _:dictionary",
    "_:format <http://purl.org/HDT/hdt#triples> _:triples",
    "_:dictionary <http://purl.org/dc/terms/format> <http://purl.org/HDT/hdt#dictionaryFour>",
    "_:triples <http://purl.org/dc/terms/format> <http://purl.org/HDT/hdt#triplesBitmap>",
    '_:triples <http://purl.org/HDT/hdt#triplesOrder> "SPO"',
]


def header_lines(hdt: hdt_spec.HDT) -> set:
    lines = [line.strip().removesuffix(".").strip() for line in hdt.header.split("\n")]
    lines = [line for line in lines if line]
    dataset = {
        line.split(" ", 1)[0] for line in lines if "<http://rdfs.org/ns/void#triples>" in line
    }
    assert len(dataset) == 1, f"no single dataset node in the header: {dataset}"
    (d,) = dataset
    return {line.replace(d, "?d", 1) if line.startswith(d + " ") else line for line in lines}


def test_reference_headers_have_the_reference_vocabulary():
    params = [p for p in decoder_params() if not p.marks]
    if not params:
        pytest.skip("no reference HDT files")
    for p in params:
        hdt = decode_reference(p.values[0])
        missing = set(REFERENCE_HEADER) - header_lines(hdt)
        assert not missing, (p.values[0], missing)


def test_header_has_the_reference_vocabulary(tmp_path):
    hdt = write_and_decode(w3c_model(), tmp_path / "out.hdt")
    missing = set(REFERENCE_HEADER) - header_lines(hdt)
    assert not missing, sorted(missing)


ESCAPED_LITERALS = r"""
<http://s> <http://p> "tab\there" .
<http://s> <http://p> "line\nbreak"@en .
<http://s> <http://p> "say \"hi\"" .
<http://s> <http://p> "back\\slash"^^<http://dt> .
"""


def test_literals_are_stored_as_values(tmp_path):
    m = Model()
    m.reads(ESCAPED_LITERALS, format="ntriples")
    hdt = write_and_decode(m, tmp_path / "out.hdt")
    assert sorted(hdt.objects + hdt.shared) == sorted(
        ['"tab\there"', '"line\nbreak"@en', '"say "hi""', '"back\\slash"^^<http://dt>']
    )


def test_reads_escaped_literals_of_earlier_maplib_versions():
    # Written by maplib before literals were stored as values: the values are N-Triples
    # escaped, and the header types the dataset with the literal "...hdt#HDTv1".
    m = Model()
    m.read(str(TESTS_DIR / "testdata" / "hdt_escaped_literals.hdt"), format="hdt")
    values = sorted(t[2][1] for t in model_triples(m))
    assert values == sorted(["tab\there", "line\nbreak", 'say "hi"', "C:\\new", "42"])


def test_backslashes_are_values_in_files_of_other_writers(tmp_path):
    # In a file from maplib, hdt-java or hdt-cpp, a backslash is part of the value.
    m = Model()
    m.reads(r'<http://s> <http://p> "C:\\new\\table" .', format="ntriples")
    m.write(str(tmp_path / "out.hdt"), format="hdt")
    m2 = Model()
    m2.read(str(tmp_path / "out.hdt"), format="hdt")
    assert [t[2][1] for t in model_triples(m2)] == ["C:\\new\\table"]

"""
An independent decoder for HDT files, written from the HDT specification
(https://www.rdfhdt.org/hdt-binary-format/ and the W3C member submission
https://www.w3.org/submissions/2011/SUBM-HDT-20110330/), used to check that the HDT files
maplib writes conform to it. It is deliberately simple and slow, and shares no code with
maplib's reader. It is validated against HDT files written by the reference implementations
(hdt-java, hdt-cpp) in ``test_hdt_compliance.py``.

Supported: the global, header, dictionary and triples sections with
``<http://purl.org/HDT/hdt#dictionaryFour>`` (plain front coding sections) and
``<http://purl.org/HDT/hdt#triplesBitmap>`` in SPO order. Every CRC is verified, and
violations of the format raise ``HDTFormatError``.

Where the specification leaves details open, the decoder follows what hdt-java and hdt-cpp
write. Deviations from them named in ``DEVIATIONS`` can be tolerated with ``tolerate=``; the
ones found are listed in ``HDT.deviations``.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple

COOKIE = b"$HDT"
HDT_V1 = "<http://purl.org/HDT/hdt#HDTv1>"
DICTIONARY_FOUR = "<http://purl.org/HDT/hdt#dictionaryFour>"
TRIPLES_BITMAP = "<http://purl.org/HDT/hdt#triplesBitmap>"

CONTROL_GLOBAL, CONTROL_HEADER, CONTROL_DICTIONARY, CONTROL_TRIPLES = 1, 2, 3, 4
SECTION_PFC = 2
SEQUENCE_LOG = 1
BITMAP_PLAIN = 1


# Deviations from the reference implementations that read_hdt can tolerate.
DEVIATIONS = {
    # hdt-java and hdt-cpp always end the block pointers of a dictionary section with the
    # end of its data, so an empty section has one pointer (0).
    "empty-section-without-pointer",
    # hdt-java and hdt-cpp write as many bits in bitmap Y and Z as there are entries in
    # sequence Y and Z. For an empty graph, hdt-java writes no bits and hdt-cpp one.
    "bitmap-longer-than-sequence",
}


class HDTFormatError(Exception):
    pass


# ---------------------------------------------------------------------------
# Checksums
# ---------------------------------------------------------------------------


def _table(poly: int, bits: int, reflected: bool) -> List[int]:
    table = []
    top = 1 << (bits - 1)
    mask = (1 << bits) - 1
    for i in range(256):
        if reflected:
            c = i
            for _ in range(8):
                c = (c >> 1) ^ poly if c & 1 else c >> 1
        else:
            c = i << (bits - 8)
            for _ in range(8):
                c = ((c << 1) ^ poly) if c & top else c << 1
        table.append(c & mask)
    return table


_CRC8 = _table(0x07, 8, False)  # CRC-8-CCITT
_CRC16 = _table(0xA001, 16, True)  # CRC-16-ANSI (IBM), reflected
_CRC32C = _table(0x82F63B78, 32, True)  # CRC-32C (Castagnoli), reflected


def crc8(data: bytes) -> int:
    c = 0
    for b in data:
        c = _CRC8[c ^ b]
    return c


def crc16(data: bytes) -> int:
    c = 0
    for b in data:
        c = (c >> 8) ^ _CRC16[(c ^ b) & 0xFF]
    return c


def crc32c(data: bytes) -> int:
    try:
        import google_crc32c  # optional, much faster

        return google_crc32c.value(data)
    except ImportError:
        pass
    c = 0xFFFFFFFF
    t = _CRC32C
    for b in data:
        c = (c >> 8) ^ t[(c ^ b) & 0xFF]
    return c ^ 0xFFFFFFFF


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------


class Reader:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def take(self, n: int) -> bytes:
        if self.pos + n > len(self.data):
            raise HDTFormatError(f"unexpected end of file at byte {self.pos} (+{n})")
        out = self.data[self.pos : self.pos + n]
        self.pos += n
        return out

    def byte(self) -> int:
        return self.take(1)[0]

    def vbyte(self) -> int:
        """HDT VByte: 7 bits per byte, least significant first, high bit set on the last."""
        value = shift = 0
        while True:
            b = self.byte()
            value |= (b & 0x7F) << shift
            shift += 7
            if b & 0x80:
                return value
            if shift > 63:
                raise HDTFormatError("VByte longer than 64 bits")

    def cstring(self) -> str:
        end = self.data.find(b"\0", self.pos)
        if end < 0:
            raise HDTFormatError("unterminated string")
        s = self.data[self.pos : end].decode("utf-8")
        self.pos = end + 1
        return s

    def u32le(self) -> int:
        return int.from_bytes(self.take(4), "little")

    def check_crc32(self, start: int, what: str):
        payload = self.data[start : self.pos]
        expected = self.u32le()
        if crc32c(payload) != expected:
            raise HDTFormatError(f"CRC32 mismatch in {what}")

    def check_crc8(self, start: int, what: str):
        payload = self.data[start : self.pos]
        if crc8(payload) != self.byte():
            raise HDTFormatError(f"CRC8 mismatch in {what}")


def parse_properties(s: str) -> Dict[str, str]:
    props = {}
    for part in s.split(";"):
        if part:
            k, sep, v = part.partition("=")
            if not sep:
                raise HDTFormatError(f"malformed property {part!r}")
            props[k] = v
    return props


@dataclass
class ControlInfo:
    type: int
    format: str
    properties: Dict[str, str]


def read_control_info(r: Reader, expected_type: int) -> ControlInfo:
    start = r.pos
    if r.take(4) != COOKIE:
        raise HDTFormatError(f"missing $HDT cookie at byte {start}")
    kind = r.byte()
    fmt = r.cstring()
    props = r.cstring()
    payload = r.data[start : r.pos]
    if crc16(payload) != int.from_bytes(r.take(2), "little"):
        raise HDTFormatError(f"CRC16 mismatch in control information at byte {start}")
    if kind != expected_type:
        raise HDTFormatError(f"control information type {kind}, expected {expected_type}")
    return ControlInfo(kind, fmt, parse_properties(props))


def read_log_sequence(r: Reader) -> List[int]:
    start = r.pos
    if r.byte() != SEQUENCE_LOG:
        raise HDTFormatError("expected a log sequence (type 1)")
    bits = r.byte()
    n = r.vbyte()
    r.check_crc8(start, "log sequence preamble")
    if bits > 64:
        raise HDTFormatError(f"log sequence with {bits} bits per entry")
    data_start = r.pos
    data = r.take((bits * n + 7) // 8)
    r.check_crc32(data_start, "log sequence data")
    if bits == 0:
        return [0] * n
    padded = data + b"\0" * 9
    mask = (1 << bits) - 1
    out = []
    for i in range(n):
        bit = i * bits
        word = int.from_bytes(padded[bit >> 3 : (bit >> 3) + 9], "little")
        out.append((word >> (bit & 7)) & mask)
    return out


def read_bitmap(r: Reader) -> Tuple[int, bytes]:
    start = r.pos
    if r.byte() != BITMAP_PLAIN:
        raise HDTFormatError("expected a plain bitmap (type 1)")
    n = r.vbyte()
    r.check_crc8(start, "bitmap preamble")
    data_start = r.pos
    data = r.take((n + 7) // 8)
    r.check_crc32(data_start, "bitmap data")
    return n, data


def bit(data: bytes, i: int) -> bool:
    return bool((data[i >> 3] >> (i & 7)) & 1)


def read_pfc_section(r: Reader, tolerate=frozenset(), found=None) -> List[str]:
    start = r.pos
    if r.byte() != SECTION_PFC:
        raise HDTFormatError("expected a plain front coding section (type 2)")
    n = r.vbyte()
    size = r.vbyte()
    block_size = r.vbyte()
    r.check_crc8(start, "dictionary section preamble")
    blocks = read_log_sequence(r)
    data_start = r.pos
    data = r.take(size)
    r.check_crc32(data_start, "dictionary section data")
    if n and block_size <= 0:
        raise HDTFormatError("block size must be positive")
    expected_blocks = (n + block_size - 1) // block_size if n else 0
    if n == 0 and blocks == [] and "empty-section-without-pointer" in tolerate:
        found.add("empty-section-without-pointer")
        blocks = [0]
    if len(blocks) != expected_blocks + 1:
        raise HDTFormatError(
            f"{len(blocks)} block pointers for {n} strings in blocks of {block_size}"
        )
    strings: List[bytes] = []
    for b in range(expected_blocks):
        pos = blocks[b]
        if pos > len(data):
            raise HDTFormatError("block pointer out of range")
        prev = b""
        for j in range(min(block_size, n - b * block_size)):
            if j == 0:
                end = data.index(b"\0", pos)
                s = data[pos:end]
            else:
                sub = Reader(data)
                sub.pos = pos
                shared = sub.vbyte()
                end = data.index(b"\0", sub.pos)
                if shared > len(prev):
                    raise HDTFormatError("front coding prefix longer than previous string")
                s = prev[:shared] + data[sub.pos : end]
            pos = end + 1
            strings.append(s)
            prev = s
        if b + 1 < len(blocks) and pos != blocks[b + 1]:
            raise HDTFormatError(f"block {b} ends at {pos}, next block starts at {blocks[b + 1]}")
    if blocks and blocks[-1] != len(data):
        raise HDTFormatError("last block pointer is not the end of the section")
    for a, b in zip(strings, strings[1:]):
        if not a < b:
            raise HDTFormatError(f"dictionary section not strictly sorted: {a!r} >= {b!r}")
    return [s.decode("utf-8") for s in strings]


@dataclass
class HDT:
    global_info: ControlInfo
    header_info: ControlInfo
    header: str
    dictionary_info: ControlInfo
    shared: List[str]
    subjects: List[str]
    predicates: List[str]
    objects: List[str]
    triples_info: ControlInfo
    triple_ids: List[Tuple[int, int, int]] = field(repr=False)
    deviations: set = field(default_factory=set)

    def subject(self, i: int) -> str:
        ns = len(self.shared)
        return self.shared[i - 1] if i <= ns else self.subjects[i - ns - 1]

    def object(self, i: int) -> str:
        ns = len(self.shared)
        return self.shared[i - 1] if i <= ns else self.objects[i - ns - 1]

    def triples(self) -> List[Tuple[str, str, str]]:
        """Triples as HDT dictionary strings."""
        return [
            (self.subject(s), self.predicates[p - 1], self.object(o))
            for s, p, o in self.triple_ids
        ]


def read_hdt(data: bytes, tolerate=frozenset()) -> HDT:
    unknown = set(tolerate) - DEVIATIONS
    if unknown:
        raise ValueError(f"unknown deviations {unknown}")
    found = set()
    r = Reader(data)
    global_info = read_control_info(r, CONTROL_GLOBAL)
    if global_info.format != HDT_V1:
        raise HDTFormatError(f"global format {global_info.format!r}")

    header_info = read_control_info(r, CONTROL_HEADER)
    if header_info.format != "ntriples":
        raise HDTFormatError(f"header format {header_info.format!r}")
    length = int(header_info.properties.get("length", -1))
    if length < 0:
        raise HDTFormatError("header has no length property")
    header = r.take(length).decode("utf-8")

    dictionary_info = read_control_info(r, CONTROL_DICTIONARY)
    if dictionary_info.format != DICTIONARY_FOUR:
        raise HDTFormatError(f"unsupported dictionary {dictionary_info.format!r}")
    shared, subjects, predicates, objects = (
        read_pfc_section(r, tolerate, found) for _ in range(4)
    )
    if set(shared) & set(subjects) or set(shared) & set(objects):
        raise HDTFormatError("shared terms repeated in the subject or object section")

    triples_info = read_control_info(r, CONTROL_TRIPLES)
    if triples_info.format != TRIPLES_BITMAP:
        raise HDTFormatError(f"unsupported triples {triples_info.format!r}")
    if triples_info.properties.get("order", "1") != "1":
        raise HDTFormatError(f"triples order {triples_info.properties['order']} is not SPO")
    ny, by = read_bitmap(r)
    nz, bz = read_bitmap(r)
    seq_y = read_log_sequence(r)
    seq_z = read_log_sequence(r)
    for name, n_bits, bits, seq in (("Y", ny, by, seq_y), ("Z", nz, bz, seq_z)):
        if n_bits == len(seq) or (not seq and n_bits == 1):
            continue
        # Padding must be zero bits, except that a writer may close an empty sequence with
        # a set bit.
        padding_is_zero = n_bits > len(seq) and (
            not seq or not any(bit(bits, i) for i in range(len(seq), n_bits))
        )
        if padding_is_zero and "bitmap-longer-than-sequence" in tolerate:
            found.add("bitmap-longer-than-sequence")
            continue
        raise HDTFormatError(
            f"bitmap {name} has {n_bits} bits for {len(seq)} sequence entries"
        )
    ny, nz = len(seq_y), len(seq_z)

    # Subjects are implicit: subject k owns the predicates up to its k-th set bit in Y.
    ids = []
    s, yi_start = 1, 0
    zi = 0
    for yi in range(ny):
        p = seq_y[yi]
        while True:
            o = seq_z[zi]
            ids.append((s, p, o))
            last = bit(bz, zi)
            zi += 1
            if last:
                break
            if zi >= nz:
                raise HDTFormatError("object bitmap does not close the last pair")
        if bit(by, yi):
            s += 1
            yi_start = yi + 1
    if zi != nz:
        raise HDTFormatError("objects left after the last predicate")
    if ny and yi_start != ny:
        raise HDTFormatError("predicate bitmap does not close the last subject")
    if r.pos != len(data):
        raise HDTFormatError(f"{len(data) - r.pos} trailing bytes")

    hdt = HDT(
        global_info, header_info, header, dictionary_info, shared, subjects, predicates,
        objects, triples_info, ids, found,
    )
    n_s = len(shared) + len(subjects)
    n_o = len(shared) + len(objects)
    for a, b in zip(ids, ids[1:]):
        if not a < b:
            raise HDTFormatError(f"triples not strictly increasing in SPO order: {a} {b}")
    for t in ids:
        if not (1 <= t[0] <= n_s and 1 <= t[1] <= len(predicates) and 1 <= t[2] <= n_o):
            raise HDTFormatError(f"triple ID out of range: {t}")
    return hdt


# ---------------------------------------------------------------------------
# Terms
# ---------------------------------------------------------------------------


def dictionary_term(s: str):
    """An HDT dictionary string as ("uri", iri), ("bnode", label) or
    ("literal", lexical form, datatype or None, language or None).

    Literals are stored with their lexical form unescaped between the first and the last
    double quote, followed by ``@lang`` or ``^^<datatype>``."""
    if s.startswith('"'):
        end = s.rfind('"')
        if end <= 0:
            raise HDTFormatError(f"malformed literal {s!r}")
        lex, rest = s[1:end], s[end + 1 :]
        if rest.startswith("^^<") and rest.endswith(">"):
            dt = rest[3:-1]
            if dt == "http://www.w3.org/2001/XMLSchema#string":
                dt = None
            return ("literal", lex, dt, None)
        if rest.startswith("@"):
            return ("literal", lex, None, rest[1:].lower())
        if rest == "":
            return ("literal", lex, None, None)
        raise HDTFormatError(f"malformed literal {s!r}")
    if s.startswith("_:"):
        return ("bnode", s[2:])
    if s.startswith("<") or s.startswith('"'):
        raise HDTFormatError(f"IRIs are stored without angle brackets: {s!r}")
    return ("uri", s)


def header_values(header: str, predicate: str) -> List[str]:
    """Values of ``predicate`` in the N-Triples header (only simple, unescaped literals)."""
    out = []
    for line in header.splitlines():
        parts = line.strip().split(" ", 2)
        if len(parts) == 3 and parts[1] == f"<{predicate}>":
            obj = parts[2].rstrip(" .")
            out.append(obj[1 : obj.rfind('"')] if obj.startswith('"') else obj)
    return out

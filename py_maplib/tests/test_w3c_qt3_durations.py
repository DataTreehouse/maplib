"""
W3C XQuery and XPath (QT3) compliance tests for arithmetic and comparisons on durations,
dates, dateTimes and times.

SPARQL 1.1 (section 17.3) and SPARQL 1.2 define ``+``, ``-``, ``*``, ``/``, ``=`` and the
ordering operators on these datatypes by the XPath operators, e.g. op:add-yearMonthDurations,
op:subtract-dateTimes and op:multiply-dayTimeDuration. The test data in
``testdata/w3c_qt3_durations/op`` is an unmodified copy of the QT3 test sets for these
operators, see ``testdata/w3c_qt3_durations/LICENSE.md``.

Each test case is an XPath expression with the expected result. The expressions that only use
constructors (``xs:dayTimeDuration("P1D")``), numbers, strings, the arithmetic and comparison
operators, ``and``, ``or``, ``fn:not``, ``fn:string``, ``fn:boolean``, ``fn:true`` and
``fn:false`` are translated to a SPARQL expression, which is evaluated with
``SELECT (STR(expr) AS ?s) {}``. The other test cases (user defined functions, FLWOR
expressions, other functions) are skipped.

The expected results are checked as follows:

* ``assert-string-value``: the string value (STR) of the result.
* ``assert-true`` / ``assert-false``: the result is true / false.
* ``assert-eq``: the result is = to the expected value.
* ``error``: the result is unbound, or the query raises an exception.
* ``any-of``: one of the alternatives holds.

Tests that maplib currently does not pass are listed in
``w3c_qt3_durations_known_failures.txt`` and are marked as strict xfails, so a test that starts
passing is reported (XPASS) and should then be removed from that list. Run
``pytest test_w3c_qt3_durations.py --runxfail`` to see the actual failures.
"""

import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Optional, Tuple

import pytest

from maplib import Model

TESTS_DIR = Path(__file__).parent
SUITE_DIR = TESTS_DIR / "testdata" / "w3c_qt3_durations" / "op"
KNOWN_FAILURES_FILE = TESTS_DIR / "w3c_qt3_durations_known_failures.txt"

QT = "{http://www.w3.org/2010/09/qt-fots-catalog}"
XSD = "http://www.w3.org/2001/XMLSchema#"

# Constructors whose string argument is translated to a typed literal.
LITERAL_TYPES = {
    "duration",
    "yearMonthDuration",
    "dayTimeDuration",
    "date",
    "dateTime",
    "dateTimeStamp",
    "time",
    "decimal",
    "integer",
    "double",
    "float",
    "string",
    "boolean",
}


class Untranslatable(Exception):
    pass


TOKEN_RE = re.compile(
    r"""
    (?P<ws>\s+)
  | (?P<comment>\(:.*?:\))
  | (?P<string>"(?:[^"]|"")*"|'(?:[^']|'')*')
  | (?P<number>(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)
  | (?P<var>\$[A-Za-z_][\w.\-]*)
  | (?P<name>[A-Za-z_][\w.\-]*(?::[A-Za-z_][\w.\-]*)?)
  | (?P<op>!=|<=|>=|[()+\-*=<>,])
    """,
    re.VERBOSE | re.DOTALL,
)

COMPARISONS = {
    "eq": "=",
    "ne": "!=",
    "lt": "<",
    "le": "<=",
    "gt": ">",
    "ge": ">=",
    "=": "=",
    "!=": "!=",
    "<": "<",
    "<=": "<=",
    ">": ">",
    ">=": ">=",
}


def tokenize(s: str) -> List[Tuple[str, str]]:
    tokens = []
    pos = 0
    while pos < len(s):
        m = TOKEN_RE.match(s, pos)
        if not m:
            raise Untranslatable(f"cannot tokenize at {s[pos:pos + 20]!r}")
        pos = m.end()
        kind = m.lastgroup
        if kind in ("ws", "comment"):
            continue
        if kind == "var":
            raise Untranslatable("variables")
        tokens.append((kind, m.group()))
    return tokens


def sparql_string(xpath_string: str) -> str:
    quote = xpath_string[0]
    value = xpath_string[1:-1].replace(quote * 2, quote)
    if "&" in value:
        raise Untranslatable("entity references")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


class Translator:
    """Translates an XPath expression of the supported subset to SPARQL."""

    def __init__(self, xpath: str):
        self.tokens = tokenize(xpath)
        self.pos = 0

    def peek(self) -> Optional[Tuple[str, str]]:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def next(self) -> Tuple[str, str]:
        t = self.peek()
        if t is None:
            raise Untranslatable("unexpected end")
        self.pos += 1
        return t

    def expect(self, value: str):
        t = self.next()
        if t[1] != value:
            raise Untranslatable(f"expected {value}, got {t[1]}")

    def translate(self) -> str:
        e = self.or_expr()
        if self.peek() is not None:
            raise Untranslatable(f"unexpected {self.peek()[1]}")
        return e

    def or_expr(self) -> str:
        e = self.and_expr()
        while self.peek() == ("name", "or"):
            self.next()
            e = f"({e} || {self.and_expr()})"
        return e

    def and_expr(self) -> str:
        e = self.comparison()
        while self.peek() == ("name", "and"):
            self.next()
            e = f"({e} && {self.comparison()})"
        return e

    def comparison(self) -> str:
        e = self.additive()
        t = self.peek()
        if t is not None and t[0] in ("name", "op") and t[1] in COMPARISONS:
            self.next()
            e = f"({e} {COMPARISONS[t[1]]} {self.additive()})"
        return e

    def additive(self) -> str:
        e = self.multiplicative()
        while self.peek() in (("op", "+"), ("op", "-")):
            op = self.next()[1]
            e = f"({e} {op} {self.multiplicative()})"
        return e

    def multiplicative(self) -> str:
        e = self.unary()
        while True:
            t = self.peek()
            if t == ("op", "*"):
                self.next()
                e = f"({e} * {self.unary()})"
            elif t == ("name", "div"):
                self.next()
                e = f"({e} / {self.unary()})"
            elif t in (("name", "idiv"), ("name", "mod")):
                raise Untranslatable(t[1])
            else:
                return e

    def unary(self) -> str:
        t = self.peek()
        if t in (("op", "-"), ("op", "+")):
            self.next()
            inner = self.unary()
            if re.fullmatch(r"[0-9.eE+\-]+", inner):
                return inner if t[1] == "+" else f"-{inner}".replace("--", "")
            return f"({t[1]}{inner})"
        return self.primary()

    def args(self) -> List[str]:
        self.expect("(")
        args = []
        if self.peek() == ("op", ")"):
            self.next()
            return args
        while True:
            args.append(self.or_expr())
            t = self.next()
            if t[1] == ")":
                return args
            if t[1] != ",":
                raise Untranslatable(f"unexpected {t[1]}")

    def primary(self) -> str:
        kind, value = self.next()
        if kind == "number":
            if value.startswith("."):
                value = "0" + value
            if value.endswith("."):
                value = value + "0"
            return value
        if kind == "string":
            return sparql_string(value)
        if kind == "op" and value == "(":
            e = self.or_expr()
            self.expect(")")
            return e
        if kind == "name" and self.peek() == ("op", "("):
            local = value.split(":", 1)[1] if ":" in value else value
            prefix = value.split(":", 1)[0] if ":" in value else "fn"
            if prefix == "xs":
                if local not in LITERAL_TYPES:
                    raise Untranslatable(f"constructor {value}")
                start = self.pos
                if (
                    self.tokens[start + 1][0] == "string"
                    and self.tokens[start + 2] == ("op", ")")
                ):
                    self.pos = start + 3
                    lexical = sparql_string(self.tokens[start + 1][1])
                    return f"{lexical}^^<{XSD}{local}>"
                (arg,) = self.args()
                return f"<{XSD}{local}>({arg})"
            if prefix != "fn":
                raise Untranslatable(f"function {value}")
            args = self.args()
            if local in ("true", "false") and not args:
                return local
            if local == "not" and len(args) == 1:
                return f"(!{args[0]})"
            if local == "string" and len(args) == 1:
                return f"STR({args[0]})"
            if local == "boolean" and len(args) == 1:
                return f"IF({args[0]}, true, false)"
            raise Untranslatable(f"function {value}")
        raise Untranslatable(f"unexpected {value}")


def translate(xpath: str) -> str:
    return Translator(xpath).translate()


class Case:
    def __init__(self, test_set: str, name: str, test: str, result: ET.Element):
        self.id = f"{test_set}/{name}"
        self.test = test
        self.result = result


def assertions(result: ET.Element) -> List[ET.Element]:
    return list(result)


def load_cases() -> List[Case]:
    cases = []
    for path in sorted(SUITE_DIR.glob("*.xml")):
        root = ET.parse(path).getroot()
        test_set = root.get("name")
        for tc in root.findall(f"{QT}test-case"):
            test = tc.find(f"{QT}test")
            result = tc.find(f"{QT}result")
            if test is None or result is None:
                continue
            cases.append(Case(test_set, tc.get("name"), test.text or "", result))
    return cases


def skip_reason(case: Case) -> Optional[str]:
    try:
        translate(case.test)
        check_translatable(case.result)
    except Untranslatable as e:
        return str(e)
    return None


def check_translatable(assertion: ET.Element):
    for a in assertion:
        tag = a.tag[len(QT):]
        if tag in ("any-of", "all-of"):
            check_translatable(a)
        elif tag == "assert-eq":
            translate(a.text or "")
        elif tag not in (
            "assert-string-value",
            "assert-true",
            "assert-false",
            "error",
        ):
            raise Untranslatable(f"assertion {tag}")


def known_failures() -> set:
    failures = set()
    if KNOWN_FAILURES_FILE.exists():
        for line in KNOWN_FAILURES_FILE.read_text().splitlines():
            line = line.split("#", 1)[0].strip()
            if line:
                failures.add(line)
    return failures


def evaluate(expr: str) -> Tuple[Optional[str], Optional[Exception]]:
    """The string value of the expression, None if it is unbound, and the exception raised."""
    query = f"SELECT ?s WHERE {{ BIND(STR({expr}) AS ?s) }}"
    try:
        df = Model().query(query)
    except Exception as e:  # noqa: BLE001 - an error is an expected result for some tests
        return None, e
    if df.height != 1:
        return None, AssertionError(f"expected one solution, got {df.height}")
    return df["s"][0], None


def holds(case: Case, assertion: ET.Element) -> Tuple[bool, str]:
    tag = assertion.tag[len(QT):]
    if tag == "any-of":
        results = [holds(case, a) for a in assertion]
        return any(r for r, _ in results), "; ".join(m for _, m in results)
    if tag == "all-of":
        results = [holds(case, a) for a in assertion]
        return all(r for r, _ in results), "; ".join(m for _, m in results)
    expr = translate(case.test)
    if tag == "assert-eq":
        expected = translate(assertion.text or "")
        value, error = evaluate(f"(({expr}) = ({expected}))")
        actual, _ = evaluate(expr)
        return value == "true", f"expected = {assertion.text}, got {actual!r} {error or ''}"
    value, error = evaluate(expr)
    if tag == "error":
        return value is None, f"expected error {assertion.get('code')}, got {value!r}"
    if tag == "assert-true":
        return value == "true", f"expected true, got {value!r} {error or ''}"
    if tag == "assert-false":
        return value == "false", f"expected false, got {value!r} {error or ''}"
    if tag == "assert-string-value":
        expected = assertion.text or ""
        if assertion.get("normalize-space") == "true":
            expected = " ".join(expected.split())
            value = " ".join(value.split()) if value is not None else None
        return value == expected, f"expected {expected!r}, got {value!r} {error or ''}"
    raise Untranslatable(f"assertion {tag}")


CASES = load_cases()
KNOWN_FAILURES = known_failures()


def params():
    out = []
    for case in CASES:
        marks = []
        reason = skip_reason(case)
        if reason is not None:
            marks.append(pytest.mark.skip(reason=f"not translated to SPARQL: {reason}"))
        elif case.id in KNOWN_FAILURES:
            marks.append(pytest.mark.xfail(strict=True, reason="known failure"))
        out.append(pytest.param(case, id=case.id, marks=marks))
    return out


@pytest.mark.parametrize("case", params())
def test_qt3(case: Case):
    results = [holds(case, a) for a in case.result]
    ok = all(r for r, _ in results)
    assert ok, f"{case.test.strip()}\n  SPARQL: {translate(case.test)}\n  " + "; ".join(
        m for _, m in results
    )


def test_known_failures_exist():
    """Every known failure is the id of a test case that is run."""
    ids = {c.id for c in CASES if skip_reason(c) is None}
    assert KNOWN_FAILURES <= ids, sorted(KNOWN_FAILURES - ids)

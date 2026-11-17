"""
Helpers for the W3C entailment test suites (test_w3c_owl2_rl.py, test_w3c_rdf_mt.py and
test_w3c_sparql11_entailment.py), which run Model.infer with a profile.

A graph G entails a graph E (simple entailment) when the blank nodes of E can be mapped to terms
of G so that every triple of E is in G. Graphs are read back from maplib with SPARQL JSON results,
and RDF terms are the tuples of test_w3c_sparql11.py.
"""

import math
from pathlib import Path
from typing import Dict, List

import pytest

from maplib import Model

from .test_w3c_sparql11 import is_bnode, n3, run_select
from .test_w3c_sparql11 import term_key as value_key


def term_key(t):
    """Literals are compared by value, as in the SPARQL tests, except that positive and negative
    zero are different values of xsd:float and xsd:double."""
    k = value_key(t)
    if isinstance(k, tuple) and k[0] == "num" and isinstance(k[2], float) and k[2] == 0.0:
        return k + (math.copysign(1.0, k[2]),)
    return k


def reason_or_skip(m: Model, profile: str, **kwargs):
    try:
        return m.infer(profile=profile, **kwargs)
    except BaseException as e:
        if "Contact Data Treehouse" in str(e):
            pytest.skip("Reasoning is not available in this build")
        raise


def graph_triples(m: Model) -> List[tuple]:
    """The triples of the default graph and the transient graph."""
    res = run_select(m, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
    return [(r["s"], r["p"], r["o"]) for r in res["rows"]]


def entails(graph: List[tuple], expected: List[tuple]) -> bool:
    """True if the graph simply entails the expected graph."""
    index = {tuple(term_key(x) for x in t) for t in graph if not any(is_bnode(x) for x in t)}
    by_predicate: Dict[object, List[tuple]] = {}
    for t in graph:
        by_predicate.setdefault(term_key(t[1]), []).append(t)

    ground = [t for t in expected if not any(is_bnode(x) for x in t)]
    for t in ground:
        if tuple(term_key(x) for x in t) not in index:
            return False
    rest = sorted(
        (t for t in expected if any(is_bnode(x) for x in t)),
        key=lambda t: sum(is_bnode(x) for x in t),
    )

    def same(a, b):
        if is_bnode(a) or is_bnode(b):
            return a == b
        return term_key(a) == term_key(b)

    def matches(pattern, t, mapping):
        new = {}
        for p, x in zip(pattern, t):
            if is_bnode(p):
                b = mapping.get(p, new.get(p))
                if b is None:
                    new[p] = x
                elif not same(b, x):
                    return None
            elif not same(p, x):
                return None
        return new

    def search(i, mapping):
        if i == len(rest):
            return True
        pattern = rest[i]
        candidates = (
            graph if is_bnode(pattern[1]) else by_predicate.get(term_key(pattern[1]), [])
        )
        for t in candidates:
            new = matches(pattern, t, mapping)
            if new is None:
                continue
            mapping.update(new)
            if search(i + 1, mapping):
                return True
            for k in new:
                del mapping[k]
        return False

    return search(0, {})


def missing_triples(graph: List[tuple], expected: List[tuple], limit=20) -> str:
    """The ground triples of the expected graph that are not in the graph, for messages."""
    index = {tuple(term_key(x) for x in t) for t in graph}
    lines = []
    for t in expected:
        if any(is_bnode(x) for x in t):
            continue
        if tuple(term_key(x) for x in t) not in index:
            lines.append("  " + " ".join(n3(x) for x in t) + " .")
    if len(lines) > limit:
        lines = lines[:limit] + [f"  ... ({len(lines)} triples)"]
    return "\n".join(lines) or "  (only triples with blank nodes)"


def load_known_failures(path: Path) -> Dict[str, str]:
    known = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            tid, _, reason = line.partition("#")
            known[tid.strip()] = reason.strip() or "known failure"
    return known


def xfail_marks(tid: str, known: Dict[str, str]) -> list:
    if tid in known:
        strict = "[flaky]" not in known[tid]
        return [pytest.mark.xfail(reason=known[tid], strict=strict)]
    return []

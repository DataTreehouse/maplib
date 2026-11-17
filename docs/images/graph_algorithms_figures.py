"""Draws the figures of API_USAGE.md › Graph algorithms from what maplib answers for the guide's
example graph, so the pictures and the queries cannot drift apart. Run it from the repository root
with a maplib that has the graph algorithm predicates:

    python docs/images/graph_algorithms_figures.py

It checks the answers it draws (an assertion fails if maplib answers differently) and writes
docs/images/graph_algorithms_*.svg.
"""

from pathlib import Path

from maplib import Model

OUT = Path(__file__).parent

EXAMPLE = """
@prefix ex: <http://example.org/> .
ex:a ex:line ex:b . ex:b ex:line ex:c . ex:c ex:line ex:a .
ex:c ex:line ex:d .
ex:d ex:line ex:e . ex:e ex:line ex:f . ex:f ex:line ex:d .
ex:f ex:line ex:g . ex:g ex:line ex:h .
ex:x ex:line ex:y .
"""

PREFIXES = """PREFIX ex: <http://example.org/>
PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#>
"""

POS = {
    "a": (70, 70), "b": (70, 170), "c": (160, 120), "d": (260, 120), "e": (340, 60),
    "f": (340, 180), "g": (440, 180), "h": (530, 180), "x": (460, 70), "y": (540, 70),
}
EDGES = [("a", "b"), ("b", "c"), ("c", "a"), ("c", "d"), ("d", "e"), ("e", "f"), ("f", "d"),
         ("f", "g"), ("g", "h"), ("x", "y")]

INK = "#2b2f36"
MUTED = "#9aa1ab"
COMPONENT = ["#3a6ea5", "#d9822b", "#3f8f5a"]
ALERT = "#c0392b"


def local(iri: str) -> str:
    return iri.rsplit("/", 1)[-1].strip("<>")


def svg(title: str, body: list[str], legend: list[tuple[str, str, str]]) -> str:
    w, h = 610, 280
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" '
        'font-family="Helvetica, Arial, sans-serif">',
        f'<rect width="{w}" height="{h}" rx="10" fill="#ffffff" stroke="#d8dce2"/>',
        f'<text x="20" y="28" font-size="15" font-weight="bold" fill="{INK}">{title}</text>',
        *body,
    ]
    x = 20
    for kind, colour, text in legend:
        if kind == "node":
            parts.append(f'<circle cx="{x + 7}" cy="{h - 22}" r="7" fill="{colour}"/>')
        elif kind == "ring":
            parts.append(f'<circle cx="{x + 7}" cy="{h - 22}" r="7" fill="#ffffff" stroke="{colour}" stroke-width="3"/>')
        else:
            parts.append(f'<line x1="{x}" y1="{h - 22}" x2="{x + 18}" y2="{h - 22}" stroke="{colour}" '
                         'stroke-width="4" stroke-dasharray="6 3"/>')
        parts.append(f'<text x="{x + 24}" y="{h - 17}" font-size="12" fill="{INK}">{text}</text>')
        x += 34 + 6.6 * len(text)
    parts.append("</svg>")
    return "\n".join(parts)


def edge(a: str, b: str, colour: str = MUTED, dashed: bool = False, width: float = 2.5) -> str:
    (x1, y1), (x2, y2) = POS[a], POS[b]
    dash = ' stroke-dasharray="7 4"' if dashed else ""
    return f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{colour}" stroke-width="{width}"{dash}/>'


def node(n: str, fill: str, ring: str | None = None, label_fill: str = "#ffffff") -> str:
    x, y = POS[n]
    stroke = f' stroke="{ring}" stroke-width="4"' if ring else ""
    return (f'<circle cx="{x}" cy="{y}" r="17" fill="{fill}"{stroke}/>'
            f'<text x="{x}" y="{y + 5}" font-size="14" text-anchor="middle" fill="{label_fill}">{n}</text>')


def note(x: float, y: float, text: str, colour: str = INK) -> str:
    return f'<text x="{x}" y="{y}" font-size="12" text-anchor="middle" fill="{colour}">{text}</text>'


def main() -> None:
    m = Model()
    m.reads(EXAMPLE, format="turtle")

    # components
    rows = m.query(PREFIXES + """SELECT ?node ?id ?size WHERE {
        ?node maplib:component ?c .
        ?c maplib:predicate ex:line ; maplib:componentId ?id ; maplib:componentSize ?size .
    }""")
    comp = {local(r["node"]): (r["id"], r["size"]) for r in rows.iter_rows(named=True)}
    assert {n: c for n, (c, _) in comp.items()} == {**dict.fromkeys("abcdefgh", 1), "x": 2, "y": 2}, comp
    body = [edge(a, b) for a, b in EDGES]
    body += [node(n, COMPONENT[comp[n][0] - 1]) for n in POS]
    body += [note(300, 238, "componentId 1, componentSize 8", COMPONENT[0]),
             note(500, 112, "componentId 2, size 2", COMPONENT[1])]
    (OUT / "graph_algorithms_components.svg").write_text(svg(
        "maplib:component: the connected parts", body,
        [("node", COMPONENT[0], "component 1 (the largest)"), ("node", COMPONENT[1], "component 2")]))

    # bridges
    rows = m.query(PREFIXES + """SELECT ?a ?b ?cut WHERE {
        ?a maplib:bridge ?o .
        ?o maplib:object ?b ; maplib:predicate ex:line ; maplib:cutOff ?cut .
    }""")
    bridges = {frozenset((local(r["a"]), local(r["b"]))): r["cut"] for r in rows.iter_rows(named=True)}
    assert bridges == {frozenset("cd"): 3, frozenset("fg"): 2, frozenset("gh"): 1, frozenset("xy"): 1}, bridges
    body = [edge(a, b, ALERT if frozenset((a, b)) in bridges else MUTED, frozenset((a, b)) in bridges, 4 if frozenset((a, b)) in bridges else 2.5)
            for a, b in EDGES]
    body += [node(n, INK) for n in POS]
    for pair, cut in bridges.items():
        a, b = sorted(pair)
        (x1, y1), (x2, y2) = POS[a], POS[b]
        body.append(note((x1 + x2) / 2, (y1 + y2) / 2 - 10, f"cutOff {cut}", ALERT))
    (OUT / "graph_algorithms_bridges.svg").write_text(svg(
        "maplib:bridge: lines whose loss splits the grid", body,
        [("edge", ALERT, "bridge, labelled with the nodes on its smaller side")]))

    # articulation points
    rows = m.query(PREFIXES + """SELECT ?n ?cut WHERE {
        ?n maplib:articulationPoint ?o .
        ?o maplib:predicate ex:line ; maplib:cutOff ?cut .
    }""")
    points = {local(r["n"]): r["cut"] for r in rows.iter_rows(named=True)}
    assert points == {"c": 2, "d": 3, "f": 2, "g": 1}, points
    body = [edge(a, b) for a, b in EDGES]
    body += [node(n, ALERT if n in points else INK) for n in POS]
    for n, cut in points.items():
        x, y = POS[n]
        body.append(note(x, y + 36 if y > 100 else y - 26, f"cutOff {cut}", ALERT))
    (OUT / "graph_algorithms_articulation.svg").write_text(svg(
        "maplib:articulationPoint: nodes whose loss splits the grid", body,
        [("node", ALERT, "articulation point, labelled with the nodes cut off from the largest part")]))

    # avoid: d out of service
    rows = m.query(PREFIXES + """SELECT ?node ?id ?size WHERE {
        ?node maplib:component ?c .
        ?c maplib:predicate ex:line ; maplib:avoid ex:d ;
           maplib:componentId ?id ; maplib:componentSize ?size .
    }""")
    comp = {local(r["node"]): r["id"] for r in rows.iter_rows(named=True)}
    groups = {}
    for n, c in comp.items():
        groups.setdefault(c, set()).add(n)
    # an avoided node is left out of the graph, so it has no row
    assert sorted(map(frozenset, groups.values()), key=len) == [frozenset("xy"), frozenset("abc"), frozenset("efgh")], groups
    body = [edge(a, b, "#d5d9df" if "d" in (a, b) else MUTED, "d" in (a, b)) for a, b in EDGES]
    colour = {frozenset("efgh"): COMPONENT[0], frozenset("abc"): COMPONENT[1], frozenset("xy"): COMPONENT[2]}
    for g in groups.values():
        for n in g:
            body.append(node(n, colour[frozenset(g)]))
    body.append(node("d", "#ffffff", MUTED, MUTED))
    body.append(note(260, 164, "avoided: no row", MUTED))
    (OUT / "graph_algorithms_avoid.svg").write_text(svg(
        "maplib:avoid ex:d: the components without d", body,
        [("node", COMPONENT[0], "efgh"), ("node", COMPONENT[1], "abc"), ("node", COMPONENT[2], "xy"),
         ("ring", MUTED, "d, left out with its lines")]))
    print("wrote", ", ".join(sorted(p.name for p in OUT.glob("graph_algorithms_*.svg"))))


if __name__ == "__main__":
    main()

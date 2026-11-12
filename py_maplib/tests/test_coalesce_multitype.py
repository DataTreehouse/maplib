import json

from maplib import Model

XSD = "http://www.w3.org/2001/XMLSchema#"


def test_coalesce_returns_first_bound_argument_across_types():
    # The first argument is a decimal and the second an integer: when the first is bound,
    # its value must be returned, never a mix of both arguments.
    m = Model()
    m.reads(
        """@prefix : <http://example.org/> .
        :a :p 1 ; :q 2 .
        :b :p 4 ; :q 0 .
        :c :p 3 .""",
        format="turtle",
    )
    for _ in range(20):
        df = m.query(
            """PREFIX : <http://example.org/>
            SELECT ?s (STR(COALESCE(?o / ?x, -2)) AS ?v) (DATATYPE(COALESCE(?o / ?x, -2)) AS ?t)
            WHERE { ?s :p ?o OPTIONAL { ?s :q ?x } } ORDER BY ?s"""
        )
        rows = [(float(v), t) for v, t in df.select(["v", "t"]).iter_rows()]
        assert rows == [
            (0.5, f"<{XSD}decimal>"),  # 1 / 2
            (-2.0, f"<{XSD}integer>"),  # division by zero is an error
            (-2.0, f"<{XSD}integer>"),  # ?x unbound
        ]


def test_coalesce_lang_strings_and_other_types():
    m = Model()
    m.reads(
        """@prefix : <http://example.org/> .
        :a :p "hei"@no .
        :b :p 7 .
        :c :q :x .""",
        format="turtle",
    )
    for _ in range(20):
        res = m.query(
            """PREFIX : <http://example.org/>
            SELECT ?s (COALESCE(?p, "fallback"@en) AS ?v)
            WHERE { ?s ?pred ?o OPTIONAL { ?s :p ?p } } ORDER BY ?s""",
            return_format="json",
        )
        bindings = json.loads(res)["results"]["bindings"]
        values = [
            (b["v"]["value"], b["v"].get("xml:lang"), b["v"].get("datatype"))
            for b in bindings
        ]
        assert values == [
            ("hei", "no", None),
            ("7", None, f"{XSD}integer"),
            ("fallback", "en", None),
        ]

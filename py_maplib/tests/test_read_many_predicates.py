"""Reading a file with many predicates: the columns each parser thread builds for a predicate are
appended to each other, and every table is ranked, sorted and deduplicated on its own. The values
(short and long strings, lang strings, integers) and the duplicates must come out as with a single
parser thread."""
from maplib import Model

EX = "http://example.net/"
N_PREDICATES = 300
N_LINES = 30_000


def _lines():
    lines = []
    for i in range(N_LINES):
        p = f"<{EX}p{i % N_PREDICATES}>"
        # Short subjects are stored inline in the string views, long ones in the buffers.
        s = f"<{EX}s{i % 7000}>" if i % 3 else f"<x:{i % 50}>"
        kind = i % 4
        if kind == 0:
            o = f'"{i % 900}"^^<http://www.w3.org/2001/XMLSchema#integer>'
        elif kind == 1:
            o = f'"v{i % 40}"'
        elif kind == 2:
            o = f'"a longer string value number {i % 1300}"'
        else:
            o = f'"tekst {i % 60}"@{"no" if i % 2 else "en"}'
        lines.append(f"{s} {p} {o} .")
    # Repeat a slice, so that tables hold duplicates both within and across parser threads.
    lines.extend(lines[: N_LINES // 3])
    return lines


def _all_triples(m):
    df = m.query(
        "SELECT ?s ?p (STR(?o) AS ?v) (DATATYPE(?o) AS ?dt) (LANG(?o) AS ?l) WHERE { ?s ?p ?o }"
    )
    return df.sort(df.columns)


def test_many_predicates_read_like_one_thread(tmp_path):
    lines = _lines()
    path = tmp_path / "many.nt"
    path.write_text("\n".join(lines) + "\n")

    parallel = Model()
    parallel.read(path, parallel=True)
    single = Model()
    single.read(path, parallel=False)

    expected = len(set(lines))
    count = "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }"
    assert parallel.query(count)["c"][0] == expected
    assert single.query(count)["c"][0] == expected
    assert len(parallel.get_predicate_iris()) == N_PREDICATES

    a = _all_triples(parallel)
    assert a.height == expected
    assert a.equals(_all_triples(single))

    one = parallel.query(
        f"SELECT ?s ?o WHERE {{ ?s <{EX}p7> ?o }}"
    )
    assert one.height == len({l for l in lines if f"<{EX}p7>" in l})


def test_many_predicates_values_survive(tmp_path):
    path = tmp_path / "values.nt"
    path.write_text(
        "\n".join(
            [
                f'<{EX}s1> <{EX}q{i}> "a longer string value that is not inlined {i}"@en .'
                for i in range(200)
            ]
            + [f"<{EX}subject-with-a-long-iri-{i}> <{EX}q{i}> <{EX}o> ." for i in range(200)]
        )
        + "\n"
    )
    m = Model()
    m.read(path)
    df = m.query(
        f"""SELECT ?s ?o WHERE {{ ?s <{EX}q42> ?o }} ORDER BY STR(?s)"""
    )
    assert df.height == 2
    lit = m.query(
        f"""SELECT (STR(?o) AS ?v) (LANG(?o) AS ?l) WHERE {{ <{EX}s1> <{EX}q199> ?o }}"""
    )
    assert lit["v"][0] == "a longer string value that is not inlined 199"
    assert lit["l"][0] == "en"
    s = m.query(f"SELECT ?s WHERE {{ ?s <{EX}q3> <{EX}o> }}")
    assert s["s"].to_list() == [f"<{EX}subject-with-a-long-iri-3>"]

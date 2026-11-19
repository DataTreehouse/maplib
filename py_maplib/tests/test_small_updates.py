"""Small updates go to an unsorted write buffer per table instead of a sorted segment, and
INSERT/DELETE with an empty WHERE and no template variables are applied as INSERT DATA and
DELETE DATA. These tests interleave many small inserts and deletes (INSERT DATA, DELETE DATA,
INSERT {..} WHERE {}, DELETE WHERE) with queries, and compare the model after each round with a
model built in one go from the expected triples: triples, counts, lookups by subject and object,
property paths, DESCRIBE, change reports, compaction and serialization."""
import random
import shutil

import pytest

from maplib import Model, IndexingOptions, DEFAULT_GRAPH

from .disk import disk_params, storage_kwargs, STORAGE_FOLDER

EX = "http://example.net/ns#"
N = 3_000


def s(i):
    return f"<{EX}s{i}>"


def lines_of(i):
    """The triples of subject i, by kind."""
    return {
        "type": f"{s(i)} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{EX}T{i % 3}> .",
        "v": f"{s(i)} <{EX}v> {i * 7 % 101} .",
        "next": f"{s(i)} <{EX}next> {s(i + 1)} .",
        "label": f'{s(i)} <{EX}label> "label {i}" .',
        "name": f'{s(i)} <{EX}name> "name {i % 50}"@en .',
        "when": f'{s(i)} <{EX}when> "2020-01-{1 + i % 28:02d}T10:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .',
    }


def stored(m):
    return set(line for line in m.writes(format="ntriples").splitlines() if line)


def built(lines):
    m = Model()
    if lines:
        m.reads("\n".join(lines), format="turtle")
    return m


@pytest.fixture(params=[(st, o) for st in disk_params() for o in (False, True)],
                ids=lambda p: f"{p[0]}-{'object_sort' if p[1] else 'default'}")
def model(request):
    storage, object_sort = request.param
    if storage != "memory":
        shutil.rmtree(STORAGE_FOLDER, ignore_errors=True)
    kwargs = storage_kwargs(storage)
    if object_sort:
        kwargs["indexing_options"] = IndexingOptions(object_sort_all=True)
    m = Model(**kwargs)
    m.reads("\n".join(l for i in range(N) for l in lines_of(i).values()), format="turtle")
    yield m
    del m
    if storage != "memory":
        shutil.rmtree(STORAGE_FOLDER, ignore_errors=True)


QUERIES = [
    "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }",
    "SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { ?s ?p ?o }",
    "SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s ?p ?o }",
    f"SELECT ?t (COUNT(?s) AS ?c) WHERE {{ ?s a ?t }} GROUP BY ?t ORDER BY ?t",
    f"SELECT ?p (COUNT(*) AS ?c) WHERE {{ ?s ?p ?o }} GROUP BY ?p ORDER BY ?p",
    f"SELECT ?s ?l WHERE {{ ?s <{EX}label> ?l . ?s <{EX}v> 5 }} ORDER BY ?s",
    f'SELECT ?s WHERE {{ ?s <{EX}name> "name 7"@en }} ORDER BY ?s',
    f"SELECT ?s WHERE {{ ?s <{EX}next> {s(11)} }}",
    f"SELECT ?o WHERE {{ {s(10)} <{EX}next>+ ?o }} ORDER BY ?o LIMIT 50",
    f"SELECT (COUNT(*) AS ?c) WHERE {{ {s(3)} <{EX}next>* ?o }}",
    f"SELECT ?s WHERE {{ ?s <{EX}next>/<{EX}next> {s(30)} }}",
    f"SELECT ?s WHERE {{ ?s <{EX}when> ?w FILTER(?w > \"2020-01-27T00:00:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>) }} ORDER BY ?s",
]


def subject_queries(i):
    return [
        f"SELECT ?p ?o WHERE {{ {s(i)} ?p ?o }} ORDER BY ?p ?o",
        f"SELECT ?o WHERE {{ {s(i)} <{EX}label> ?o }}",
        f'SELECT ?s WHERE {{ ?s <{EX}label> "label {i}" }}',
        f"SELECT ?s ?p WHERE {{ ?s ?p {s(i)} }} ORDER BY ?s ?p",
        f"ASK {{ {s(i)} a <{EX}T{i % 3}> }}",
    ]


def frame_rows(df):
    return sorted(tuple(str(v) for v in r) for r in df.iter_rows())


def check_same(m, want_lines, subjects):
    ref = built(sorted(want_lines))
    assert stored(m) == stored(ref)
    for q in QUERIES + [q for i in subjects for q in subject_queries(i)]:
        got, exp = m.query(q), ref.query(q)
        if isinstance(got, bool):
            assert got == exp, q
        else:
            assert frame_rows(got) == frame_rows(exp), q
    for i in subjects[:3]:
        d = f"DESCRIBE {s(i)}"
        assert described(m.query(d)) == described(ref.query(d)), d


def described(res):
    return sorted(tuple(str(v) for v in r) for df in res for r in df.iter_rows())

def test_interleaved_small_updates_match_a_model_built_in_one_go(model):
    rng = random.Random(11)
    present = {(i, k): l for i in range(N) for k, l in lines_of(i).items()}
    new_i = N + 10
    touched = []
    for rnd in range(6):
        for step in range(40):
            kind = rng.randrange(5)
            if kind == 0:
                # A new subject, as BSBM inserts a product with its offers.
                ls = lines_of(new_i)
                ch = model.update("INSERT DATA { " + " ".join(ls.values()) + " }")
                assert ch[DEFAULT_GRAPH]["inserted"] == len(ls)
                present.update({(new_i, k): l for k, l in ls.items()})
                touched.append(new_i)
                new_i += 1
            elif kind == 1:
                # The benchmark wrapper's form, with a triple that is already stored.
                ls = lines_of(new_i)
                old = rng.choice(sorted(present))
                ch = model.update("INSERT { " + " ".join(ls.values()) + " " + present[old] + " } WHERE {}")
                assert ch[DEFAULT_GRAPH]["inserted"] == len(ls)
                present.update({(new_i, k): l for k, l in ls.items()})
                touched.append(new_i)
                new_i += 1
            elif kind == 2:
                # All triples of a subject, stored or buffered.
                i = rng.choice(touched) if touched and rng.random() < 0.5 else rng.randrange(N)
                n = sum(1 for (j, _) in present if j == i)
                ch = model.update(f"DELETE WHERE {{ {s(i)} ?p ?o }}")
                assert ch.get(DEFAULT_GRAPH, {}).get("deleted", 0) == n
                present = {k: v for k, v in present.items() if k[0] != i}
                touched.append(i)
            elif kind == 3:
                # Some triples, a missing one among them.
                keys = rng.sample(sorted(present), 3)
                ch = model.update("DELETE DATA { " + " ".join(present[k] for k in keys)
                                  + f" <{EX}nope> <{EX}v> 1 . }}")
                assert ch[DEFAULT_GRAPH]["deleted"] == 3
                for k in keys:
                    del present[k]
                touched.extend(k[0] for k in keys)
            else:
                # Delete and insert again, in one operation without a WHERE pattern.
                k = rng.choice(sorted(present))
                ch = model.update(f"DELETE {{ {present[k]} }} INSERT {{ {present[k]} }} WHERE {{}}")
                assert ch[DEFAULT_GRAPH]["deleted"] == 1
                assert ch[DEFAULT_GRAPH]["inserted"] == 1
                touched.append(k[0])
            if step % 10 == 0:
                assert model.query("SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }")["c"][0] == len(present)
        check_same(model, present.values(), sorted(set(touched))[-8:] + [touched[-1]])


def test_insert_template_with_empty_where_and_blank_nodes():
    m = Model()
    m.update(f"INSERT {{ _:b <{EX}p> 1 . _:b <{EX}q> 2 }} WHERE {{}}")
    m.update(f"INSERT {{ _:b <{EX}p> 1 }} WHERE {{}}")
    df = m.query(f"SELECT ?b WHERE {{ ?b <{EX}p> 1 . OPTIONAL {{ ?b <{EX}q> ?q }} }}")
    assert df.height == 2
    df = m.query(f"SELECT ?b WHERE {{ ?b <{EX}p> 1 . ?b <{EX}q> 2 }}")
    assert df.height == 1


def test_insert_template_with_empty_where_into_graphs():
    m = Model()
    g = f"{EX}g"
    ch = m.update(f"INSERT {{ GRAPH <{g}> {{ {s(1)} <{EX}p> 1 }} {s(2)} <{EX}p> 2 }} WHERE {{}}")
    assert ch[g]["inserted"] == 1
    assert ch[DEFAULT_GRAPH]["inserted"] == 1
    assert m.query(f"SELECT ?o WHERE {{ {s(1)} <{EX}p> ?o }}", graph=g)["o"].to_list() == [1]
    ch = m.update(f"DELETE {{ GRAPH <{g}> {{ {s(1)} <{EX}p> 1 }} }} WHERE {{}}")
    assert ch[g]["deleted"] == 1
    assert m.query(f"SELECT ?o WHERE {{ {s(1)} <{EX}p> ?o }}", graph=g).height == 0


def test_buffer_survives_serialization_and_compaction(model, tmp_path):
    extra = [l for i in range(N, N + 30) for l in lines_of(i).values()]
    for i in range(0, len(extra), 6):
        model.update("INSERT DATA { " + " ".join(extra[i:i + 6]) + " }")
    model.update(f"DELETE WHERE {{ {s(N + 3)} ?p ?o }}")
    model.update(f"DELETE WHERE {{ {s(5)} ?p ?o }}")
    before = stored(model)
    path = tmp_path / "m.maplib"
    model.serialize(str(path))
    assert stored(Model.deserialize(str(path))) == before
    # Changes reported with the triples, from the buffer.
    ch = model.update(f"DELETE WHERE {{ {s(N + 4)} ?p ?o }}", return_triples=True)
    assert ch[DEFAULT_GRAPH]["deleted"] == 6
    assert sum(df.height for df in ch[DEFAULT_GRAPH]["deleted_triples"].values()) == 6
    model.compact()
    assert len(stored(model)) == len(before) - 6
    model.update("INSERT DATA { " + " ".join(lines_of(N + 4).values()) + " }")
    assert stored(model) == before


def test_many_small_inserts_flush_the_buffer(model):
    # More triples than the buffer holds, one table, one triple at a time in batches.
    want = set(stored(model))
    for b in range(25):
        batch = [f"<{EX}x{b}_{j}> <{EX}v> {j} ." for j in range(200)]
        model.update("INSERT DATA { " + " ".join(batch) + " }")
        want |= stored(built(batch))
        assert model.query(f"SELECT (COUNT(*) AS ?c) WHERE {{ ?s <{EX}v> ?o }}")["c"][0] == N + 200 * (b + 1)
    assert stored(model) == want
    assert model.query(f"SELECT ?o WHERE {{ <{EX}x3_17> <{EX}v> ?o }}")["o"].to_list() == [17]
    assert model.query(f"SELECT ?s WHERE {{ ?s <{EX}v> 199 }}").height == 25

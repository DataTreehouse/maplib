from maplib import Model

# Inserting transient triples that all already exist as non-transient triples used to remove the
# earlier transient triples with the same predicate (and subject/object types). This made SHACL
# rule inference re-infer them forever (https://github.com/DataTreehouse/maplib/issues/76).

PREFIX = "PREFIX ex: <http://example.org/>\n"


def count(m: Model, query: str) -> int:
    return m.query(PREFIX + "SELECT (COUNT(*) AS ?c) WHERE { " + query + " }")["c"][0]


def test_transient_insert_of_only_existing_triples_keeps_earlier_transient_triples():
    m = Model()
    m.reads(
        """
        @prefix ex: <http://example.org/> .
        ex:a ex:name "A" .
        """,
        format="turtle",
    )
    # A new, transient triple with the same predicate.
    m.insert(
        PREFIX + 'CONSTRUCT { ex:copy ex:name "A" } WHERE { ex:a ex:name "A" }',
        transient=True,
    )
    assert count(m, "ex:copy ex:name ?n") == 1

    # Re-assert, transiently, a triple that already exists non-transiently.
    m.insert(
        PREFIX + "CONSTRUCT { ex:a ex:name ?n } WHERE { ex:a ex:name ?n }",
        transient=True,
    )
    assert count(m, "ex:copy ex:name ?n") == 1
    assert count(m, "ex:a ex:name ?n") == 1

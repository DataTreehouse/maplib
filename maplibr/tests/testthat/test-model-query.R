# Tests for Model$query() (maplib-l2j). Real query() support removes the
# blocker that limited earlier test files (test-model-basics.R,
# test-model-infer-rdfs.R, etc.) to non-query assertions -- this file covers
# query() itself; expanding the older files to use query() where py_maplib's
# own tests did is left for follow-up (maplib-a2d).

test_that("a simple SELECT returns real, decoded values (not category codes)", {
  # Confirmed live, worth guarding against regressing: query results store
  # repeated IRI/literal values as category codes into a Model-wide
  # dictionary, not plain polars Categorical columns -- an earlier attempt
  # at decoding (a Series-level Categorical->String cast) silently did
  # nothing, and values came back as raw integer codes (e.g. "0", "1")
  # instead of the real strings. Fixed via representation::formatting::
  # format_native_columns (see model.rs's query()).
  m <- Model$new()
  m$reads(paste(
    '<http://ex/a> <http://ex/name> "Alice" .',
    '<http://ex/b> <http://ex/name> "Bob" .',
    sep = "\n"
  ), format = "ntriples")

  df <- m$query("SELECT ?s ?o WHERE { ?s <http://ex/name> ?o }")
  expect_setequal(df$s, c("http://ex/a", "http://ex/b"))
  expect_setequal(df$o, c("Alice", "Bob"))
})

test_that("a column whose RDF node type varies by row is collapsed to one vector", {
  m <- Model$new()
  m$reads(paste(
    '<http://ex/a> <http://ex/name> "Alice" .',
    "<http://ex/a> <http://ex/knows> <http://ex/b> .",
    sep = "\n"
  ), format = "ntriples")

  df <- m$query('
    SELECT ?x WHERE {
      { <http://ex/a> <http://ex/name> ?x }
      UNION
      { <http://ex/a> <http://ex/knows> ?x }
    }
  ')
  expect_setequal(df$x, c("Alice", "http://ex/b"))
  expect_false(is.data.frame(df$x))

  types <- attr(df, "rdf_node_types")$x
  kinds <- vapply(types, function(t) t$kind, character(1))
  expect_setequal(kinds, c("iri", "literal"))
})

test_that("include_transient controls whether inferred triples are visible to query()", {
  m <- Model$new()
  m$reads(paste(
    "<http://ex/moon> <http://www.w3.org/2000/01/rdf-schema#domain> <http://ex/Planet> .",
    "<http://ex/Mars> <http://ex/moon> <http://ex/Phobos> .",
    sep = "\n"
  ), format = "turtle")

  n <- m$infer_rdfs()
  expect_gt(n, 0)

  with_transient <- m$query("SELECT * WHERE { ?s ?p ?o }", include_transient = TRUE)
  without_transient <- m$query("SELECT * WHERE { ?s ?p ?o }", include_transient = FALSE)
  # n is a count of newly-inserted triples (Triplestore::interesting_rdfs_rules,
  # lib/triplestore/src/rdfs_inferencing.rs:170, sums per-rule inserted-triple
  # counts), not a count of rules applied, despite Model$infer_rdfs's own doc
  # comment saying "the number of interesting inference rules applied" -- so
  # this equality is guaranteed, not coincidental.
  expect_equal(nrow(with_transient), nrow(without_transient) + n)
})

test_that("query() restricted to a graph with no matching triples returns zero rows", {
  m <- Model$new()
  m$reads('<http://ex/a> <http://ex/b> <http://ex/c> .', format = "ntriples")
  df <- m$query("SELECT * WHERE { ?s ?p ?o }", graph = "http://nonexistent/graph")
  expect_equal(nrow(df), 0)
})

test_that("query() raises a catchable error for invalid SPARQL syntax", {
  m <- Model$new()
  expect_error(
    m$query("this is not valid SPARQL !!!"),
    class = "maplibr_maplib_error"
  )
})

test_that("CONSTRUCT with a literal predicate returns a subject/predicate/object data.frame", {
  m <- Model$new()
  m$reads(paste(
    '<http://ex/a> <http://ex/name> "Alice" .',
    '<http://ex/b> <http://ex/name> "Bob" .',
    sep = "\n"
  ), format = "ntriples")

  df <- m$query("CONSTRUCT { ?s <http://ex/hasName> ?o } WHERE { ?s <http://ex/name> ?o }")
  expect_setequal(names(df), c("subject", "predicate", "object"))
  expect_equal(nrow(df), 2)
  expect_true(all(df$predicate == "http://ex/hasName"))
  expect_setequal(df$object, c("Alice", "Bob"))
})

test_that("CONSTRUCT with a bound ?p and a row-varying ?o flattens multi-typed values correctly", {
  # Regression case: ?o here is bound to both a string and an integer
  # literal across different rows of the SAME pattern -- format_native_
  # columns represents that as a Struct (one field per possible type), which
  # must be coalesced down to one string column before concatenating across
  # patterns, not cast directly (a direct Struct->String cast silently
  # produces NA for every row instead of erroring).
  m <- Model$new()
  m$reads(paste(
    '<http://ex/a> <http://ex/name> "Alice" .',
    "<http://ex/a> <http://ex/age> 30 .",
    sep = "\n"
  ), format = "turtle")

  df <- m$query("CONSTRUCT { ?s <http://ex/copy> ?o . ?s ?p ?o } WHERE { ?s ?p ?o }")
  expect_equal(nrow(df), 4)
  expect_false(any(is.na(df$object)))
  copy_rows <- df[df$predicate == "http://ex/copy", ]
  expect_setequal(copy_rows$object, c("Alice", "30"))
})

test_that("CONSTRUCT flattens a multi-typed *predicate* variable too, not just subject/object", {
  # Regression case (roborev review of the CONSTRUCT-support commit): the
  # variable-bound-predicate branch of construct_result_to_solution_mappings
  # used to cast the predicate column directly, on the assumption that a
  # predicate variable is always singly IRI-typed -- true of RDF's predicate
  # *position*, but not guaranteed of a *variable*, which can also appear in
  # a non-predicate role elsewhere in the WHERE clause (here, via UNION),
  # making its RDFNodeState multi just like subject/object can be.
  m <- Model$new()
  m$reads('<http://ex/a> <http://ex/name> "Alice" .', format = "ntriples")

  df <- m$query('
    CONSTRUCT { ?a ?p ?b }
    WHERE {
      { ?a ?p ?b }
      UNION
      { BIND("not-an-iri" AS ?p) BIND(<http://ex/x> AS ?a) BIND(<http://ex/y> AS ?b) }
    }
  ')
  expect_equal(nrow(df), 2)
  expect_false(any(is.na(df$predicate)))
  expect_setequal(df$predicate, c("http://ex/name", "not-an-iri"))
})

test_that("CONSTRUCT preserves a language-tagged literal's value (dropping the language tag)", {
  m <- Model$new()
  m$reads('<http://ex/a> <http://ex/label> "hello"@en .', format = "turtle")
  df <- m$query("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")
  expect_equal(df$object, "hello")
})

test_that("CONSTRUCT with multiple template patterns concatenates all of their rows", {
  m <- Model$new()
  m$reads(paste(
    '<http://ex/a> <http://ex/name> "Alice" .',
    "<http://ex/a> <http://ex/knows> <http://ex/b> .",
    sep = "\n"
  ), format = "ntriples")

  df <- m$query("
    CONSTRUCT {
      ?s <http://ex/hasName> ?name .
      ?s <http://ex/hasFriend> ?friend .
    } WHERE {
      ?s <http://ex/name> ?name .
      ?s <http://ex/knows> ?friend .
    }
  ")
  expect_equal(nrow(df), 2)
  expect_setequal(df$predicate, c("http://ex/hasName", "http://ex/hasFriend"))
})

test_that("a CONSTRUCT query matching nothing returns zero rows, not an error", {
  m <- Model$new()
  df <- m$query("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")
  expect_equal(nrow(df), 0)
  expect_setequal(names(df), c("subject", "predicate", "object"))
})

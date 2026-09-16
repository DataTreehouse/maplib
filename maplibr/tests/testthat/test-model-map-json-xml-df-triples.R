# Tests for Model$map_json()/map_json_string()/map_xml()/map_xml_string()/
# map_df()/map_triples() (maplib-b98) -- direct-to-triples mapping entry
# points that don't go through an OTTR template, unlike $map()/$map_no_data().

test_that("map_json_string() maps a JSON string via the Facade-X convention", {
  # `1` is mapped to an xsd:long-typed literal, not a string
  # (lib/triplestore/src/map_json.rs) -- `df$o == "1"` only passes because
  # the resulting multi-typed ?o column gets coalesced to character by
  # .collapse_multitype_columns(). If that coercion's behavior for
  # non-character sub-columns ever changes, this assertion is the one that
  # would need updating, not a sign the mapping itself broke.
  m <- Model$new()
  m$map_json_string('{"a": 1, "b": "x"}')
  df <- m$query("SELECT ?p ?o WHERE { ?s ?p ?o }")
  expect_true(any(df$p == "http://sparql.xyz/facade-x/data/a" & df$o == "1"))
  expect_true(any(df$p == "http://sparql.xyz/facade-x/data/b" & df$o == "x"))
})

test_that("map_json() maps a JSON file the same way as map_json_string()", {
  m <- Model$new()
  path <- tempfile(fileext = ".json")
  on.exit(unlink(path))
  writeLines('{"a": 1, "b": "x"}', path)
  m$map_json(path)
  df <- m$query("SELECT ?p ?o WHERE { ?s ?p ?o }")
  expect_true(any(df$p == "http://sparql.xyz/facade-x/data/a" & df$o == "1"))
})

test_that("map_json()'s graph argument scopes the mapped triples to one named graph", {
  m <- Model$new()
  path <- tempfile(fileext = ".json")
  on.exit(unlink(path))
  writeLines('{"a": 1}', path)
  m$map_json(path, graph = "http://ex/g1")
  expect_gt(m$size(graph = "http://ex/g1"), 0)
  expect_equal(m$size(), 0)
})

test_that("map_xml_string() maps an XML string via the Facade-X convention", {
  # map_xml()'s convention represents each element's tag name as an rdf:type
  # value (e.g. xyz:data/a), and its text content as an fx:child leaf value
  # -- not "predicate named after the tag", unlike map_df().
  m <- Model$new()
  m$map_xml_string("<root><a>1</a><b>x</b></root>")
  df <- m$query("SELECT ?p ?o WHERE { ?s ?p ?o }")
  types <- df$o[df$p == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
  expect_true("http://sparql.xyz/facade-x/data/a" %in% types)
  expect_true("http://sparql.xyz/facade-x/data/b" %in% types)
  leaves <- df$o[df$p == "http://sparql.xyz/facade-x/ns/child"]
  expect_true("1" %in% leaves)
  expect_true("x" %in% leaves)
})

test_that("map_xml() maps an XML file the same way as map_xml_string()", {
  m <- Model$new()
  path <- tempfile(fileext = ".xml")
  on.exit(unlink(path))
  writeLines("<root><a>1</a></root>", path)
  m$map_xml(path)
  df <- m$query("SELECT ?p ?o WHERE { ?s ?p ?o }")
  types <- df$o[df$p == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
  expect_true("http://sparql.xyz/facade-x/data/a" %in% types)
})

test_that("map_df() maps a data.frame's columns directly, one predicate per column", {
  # map_df() wraps the whole data.frame as a Facade-X-style document: a
  # fresh root subject, one fx:child + fx:childNumber pair per row (for
  # ordering), plus each row's own columns as direct data predicates --
  # confirmed against Triplestore::map_df (lib/triplestore/src/map_df.rs),
  # not the flatter "N rows x N columns = N*M triples" shape one might
  # assume from the name alone.
  m <- Model$new()
  d <- data.frame(name = c("Alice", "Bob"), age = c(30L, 25L), stringsAsFactors = FALSE)
  m$map_df(d)
  df <- m$query("SELECT ?p ?o WHERE { ?s ?p ?o }")
  expect_equal(nrow(df), 9)
  name_rows <- df[df$p == "http://sparql.xyz/facade-x/data/name", ]
  expect_setequal(name_rows$o, c("Alice", "Bob"))
})

test_that("map_df() with a zero-row data.frame still adds the root triple", {
  # NOT a no-op, unlike $map(): Triplestore::map_df unconditionally adds one
  # <root> rdf:type fx:root triple regardless of row count, and py_maplib's
  # own map_df has no zero-row early return either -- matched here rather
  # than diverging from it.
  m <- Model$new()
  d <- data.frame(name = character(0), stringsAsFactors = FALSE)
  m$map_df(d)
  expect_equal(m$size(), 1)
  df <- m$query("SELECT ?p ?o WHERE { ?s ?p ?o }")
  expect_equal(df$p, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
  expect_equal(df$o, "http://sparql.xyz/facade-x/ns/root")
})

test_that("map_triples() maps subject/predicate/object columns directly", {
  m <- Model$new()
  d <- data.frame(
    subject = "http://ex/a", predicate = "http://ex/p", object = "http://ex/o",
    stringsAsFactors = FALSE
  )
  m$map_triples(d)
  expect_equal(m$size(), 1)
  df <- m$query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
  expect_equal(df$s, "http://ex/a")
  expect_equal(df$p, "http://ex/p")
  expect_equal(df$o, "http://ex/o")
})

test_that("map_triples()'s predicate argument fixes a constant predicate for every row", {
  m <- Model$new()
  d <- data.frame(
    subject = c("http://ex/a", "http://ex/b"),
    object = c("http://ex/o1", "http://ex/o2"),
    stringsAsFactors = FALSE
  )
  m$map_triples(d, predicate = "http://ex/const")
  expect_equal(m$size(), 2)
  df <- m$query("SELECT ?p WHERE { ?s ?p ?o }")
  expect_true(all(df$p == "http://ex/const"))
})

test_that("map_triples() with a zero-row data.frame and correct columns is a no-op", {
  m <- Model$new()
  d <- data.frame(subject = character(0), predicate = character(0), object = character(0))
  m$map_triples(d)
  expect_equal(m$size(), 0)
})

test_that("map_triples() still validates columns on a zero-row data.frame", {
  # Regression case: an earlier version skipped column validation entirely
  # for a zero-row input, so a misnamed/missing column (e.g. a data.frame
  # upstream-filtered down to zero rows) would silently succeed instead of
  # raising a normal error -- expand_triples's own column-existence check
  # (lib/maplib/src/model/expansion/validation.rs) runs regardless of row
  # count, and this now does too.
  m <- Model$new()
  d <- data.frame(subj = character(0), pred = character(0), obj = character(0))
  expect_error(m$map_triples(d), class = "maplibr_maplib_error")
})

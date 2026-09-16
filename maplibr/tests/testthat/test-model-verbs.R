# Tests for the model_*() pipe-friendly functional wrappers (R/model-verbs.R).
# Each one is a thin passthrough to the matching Model R6 method, so these
# tests check the passthrough is correct (right args, right return value),
# not the underlying behavior itself (already covered by the R6-method
# tests in the other test-model-*.R files).

test_that("mutating model_*() verbs return the model invisibly, and it stays chainable via |>", {
  m <- Model$new() |>
    model_reads('<http://ex/a> <http://ex/p> <http://ex/o> .', format = "ntriples") |>
    model_update("INSERT { ?s <http://ex/q> \"seen\" } WHERE { ?s ?p ?o }") |>
    model_add_prefixes(list(ex = "http://ex/")) |>
    model_create_index()

  expect_s3_class(m, "Model")
  expect_equal(model_size(m), 2)
})

test_that("model_query() returns a data.frame, usable as the end of a pipe", {
  m <- Model$new() |> model_reads('<http://ex/a> <http://ex/p> <http://ex/o> .', format = "ntriples")
  df <- m |> model_query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
  expect_s3_class(df, "data.frame")
  expect_equal(df$s, "http://ex/a")
})

test_that("model_query()'s bindings argument passes through correctly", {
  m <- Model$new()
  df <- m |> model_query(
    'SELECT * WHERE { VALUES ?a { "a" "b" } FILTER(?a = ?b) }',
    bindings = list(b = Literal("b"))
  )
  expect_equal(nrow(df), 1)
})

test_that("model_insert() adds a CONSTRUCT query's results", {
  m <- Model$new() |> model_reads('<http://ex/a> <http://ex/p> <http://ex/o> .', format = "ntriples")
  m |> model_insert("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }", target_graph = "http://ex/g2")
  expect_equal(model_size(m, graph = "http://ex/g2"), 1)
})

test_that("model_read()/model_write() round-trip through a file", {
  m <- Model$new() |> model_reads('<http://ex/a> <http://ex/p> <http://ex/o> .', format = "ntriples")
  path <- tempfile(fileext = ".nt")
  on.exit(unlink(path))
  m |> model_write(path)

  m2 <- Model$new() |> model_read(path)
  expect_equal(model_size(m2), 1)
})

test_that("model_add_template() returns the template IRI (not chainable), model_map() expands it", {
  ex <- Prefix("http://ex/", "ex")
  var <- Variable("V")
  template <- Template(
    iri = suf(ex, "T"),
    parameters = list(Parameter(var)),
    instances = list(Triple(suf(ex, "a"), suf(ex, "b"), var))
  )
  m <- Model$new()
  iri <- m |> model_add_template(template)
  expect_equal(iri, "http://ex/T")

  m |> model_map(template, data.frame(V = "c"))
  expect_equal(model_size(m), 1)
})

test_that("model_map_df()/model_map_triples()/model_map_json_string()/model_map_xml_string() all delegate correctly", {
  m1 <- Model$new()
  m1 |> model_map_df(data.frame(name = "Alice"))
  expect_gt(model_size(m1), 0)

  m2 <- Model$new()
  m2 |> model_map_triples(
    data.frame(subject = "http://ex/a", object = "http://ex/b"),
    predicate = "http://ex/knows"
  )
  expect_equal(model_size(m2), 1)

  m3 <- Model$new()
  m3 |> model_map_json_string('{"a": 1}')
  expect_gt(model_size(m3), 0)

  m4 <- Model$new()
  m4 |> model_map_xml_string("<root><a>1</a></root>")
  expect_gt(model_size(m4), 0)
})

test_that("model_detach_graph() returns a new Model, and model_add_graph() copies into one", {
  m <- Model$new() |> model_reads(
    '<http://ex/a> <http://ex/p> <http://ex/o> .', format = "ntriples", graph = "http://ex/g1"
  )
  sprout <- m |> model_detach_graph(graph = "http://ex/g1")
  expect_s3_class(sprout, "Model")
  expect_equal(model_size(sprout), 1)

  m2 <- Model$new() |> model_add_graph(sprout)
  expect_equal(model_size(m2), 1)
})

test_that("model_infer_rdfs() returns the inferred-triple count", {
  m <- Model$new() |> model_reads(paste(
    "<http://ex/moon> <http://www.w3.org/2000/01/rdf-schema#domain> <http://ex/Planet> .",
    "<http://ex/Mars> <http://ex/moon> <http://ex/Phobos> .",
    sep = "\n"
  ), format = "turtle")
  n <- m |> model_infer_rdfs()
  expect_gt(n, 0)
})

test_that("model_writes() returns a string, and model_truncate_graph() empties a graph", {
  m <- Model$new() |> model_reads('<http://ex/a> <http://ex/p> <http://ex/o> .', format = "ntriples")
  expect_type(model_writes(m), "character")
  m |> model_truncate_graph()
  expect_equal(model_size(m), 0)
})

test_that("model_deserialize() is a pipe source, not a target -- creates a fresh Model from a path", {
  # No `m` argument at all -- unlike every other model_*() verb, it can only
  # start a pipe, not receive one: model_deserialize(path) |> model_query(...).
  expect_equal(names(formals(model_deserialize)), c("path", "storage_folder"))
})

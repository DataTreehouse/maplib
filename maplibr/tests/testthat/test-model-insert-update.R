# Tests for Model$update() and Model$insert() (maplib-zlr).

test_that("update() runs a DELETE/INSERT/WHERE modify against existing data", {
  m <- Model$new()
  m$reads('<http://ex/a> <http://ex/p> <http://ex/o1> .', format = "ntriples")

  m$update("INSERT { <http://ex/a> <http://ex/p2> <http://ex/o2> } WHERE { <http://ex/a> <http://ex/p> <http://ex/o1> }")
  expect_equal(m$size(), 2)

  m$update("DELETE { <http://ex/a> <http://ex/p> <http://ex/o1> } WHERE { <http://ex/a> <http://ex/p> <http://ex/o1> }")
  expect_equal(m$size(), 1)
})

test_that("update() with an empty WHERE {} inserts/deletes constant triples", {
  # INSERT DATA/DELETE DATA panic in the core engine (todo!() in
  # Triplestore::update_parsed) -- WHERE {} is the documented workaround, see
  # Model$update()'s roxygen docs and maplib-cai (upstream issue).
  m <- Model$new()
  m$update("INSERT { <http://ex/a> <http://ex/b> <http://ex/c> } WHERE {}")
  expect_equal(m$size(), 1)

  m$update("DELETE { <http://ex/a> <http://ex/b> <http://ex/c> } WHERE {}")
  expect_equal(m$size(), 0)
})

test_that("update()'s graph argument restricts which graph's data is matched/modified", {
  m <- Model$new()
  m$reads('<http://ex/a> <http://ex/p> <http://ex/o> .', format = "ntriples", graph = "http://ex/g1")

  m$update(
    "INSERT { ?s <http://ex/copy> ?o } WHERE { ?s <http://ex/p> ?o }",
    graph = "http://ex/g2"
  )
  expect_equal(m$size(graph = "http://ex/g1"), 1)
  expect_equal(m$size(graph = "http://ex/g2"), 0)
})

test_that("update() raises a catchable error for invalid SPARQL syntax", {
  m <- Model$new()
  expect_error(
    m$update("this is not valid SPARQL UPDATE !!!"),
    class = "maplibr_maplib_error"
  )
})

test_that("insert() adds a CONSTRUCT query's results into the target graph", {
  m <- Model$new()
  m$reads('<http://ex/a> <http://ex/p> <http://ex/o> .', format = "ntriples")

  m$insert(
    "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
    target_graph = "http://ex/g2"
  )
  expect_equal(m$size(), 1)
  expect_equal(m$size(graph = "http://ex/g2"), 1)

  df <- m$query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", graph = "http://ex/g2")
  expect_equal(df$s, "http://ex/a")
})

test_that("insert()'s source_graph restricts which graph is queried for new triples", {
  m <- Model$new()
  m$reads('<http://ex/a> <http://ex/p> <http://ex/o> .', format = "ntriples", graph = "http://ex/g1")

  m$insert(
    "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
    source_graph = "http://ex/nonexistent",
    target_graph = "http://ex/g2"
  )
  expect_equal(m$size(graph = "http://ex/g2"), 0)
})

test_that("insert() rejects a SELECT query (INSERT needs a triple-shaped CONSTRUCT result)", {
  m <- Model$new()
  m$reads('<http://ex/a> <http://ex/p> <http://ex/o> .', format = "ntriples")
  expect_error(
    m$insert("SELECT ?s ?p ?o WHERE { ?s ?p ?o }"),
    class = "maplibr_maplib_error"
  )
})

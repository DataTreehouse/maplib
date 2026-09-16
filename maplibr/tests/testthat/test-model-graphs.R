# Ports py_maplib/tests/test_named_graphs.py. query() (maplib-l2j) and
# size(graph) (maplib-0mj) are both available now, so this no longer needs to
# route around m.size(graph) via writes(graph=) the way an earlier version of
# this file did.

gr1 <- '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "A" .'
gr2 <- '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "B" .'
gr3 <- '
<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "B" .
<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "C" .
'

test_that("reads() routes triples into the requested named graph", {
  # Ports test_basic_named_graph (py_maplib/tests/test_named_graphs.py:17-30).
  ng2 <- "urn:graph:gr2"
  m <- Model$new()
  m$reads(gr1, format = "ntriples")
  m$reads(gr2, format = "ntriples", graph = ng2)

  df <- m$query("SELECT * WHERE { GRAPH <urn:graph:gr2> { ?a ?b ?c } }")
  expect_equal(nrow(df), 1)
  expect_equal(df$c[1], "B")
  expect_equal(m$size(ng2), 1)
  expect_equal(m$size(), 1)
})

test_that("reads() with no graph routes into the default graph, queryable via maplib:DefaultGraph", {
  # Ports test_basic_named_graph_named_default (py_maplib/tests/
  # test_named_graphs.py:35-51).
  ng2 <- "urn:graph:gr2"
  m <- Model$new()
  m$reads(gr1, format = "ntriples", graph = ng2)
  m$reads(gr2, format = "ntriples")

  df <- m$query("SELECT * WHERE { GRAPH maplib:DefaultGraph { ?a ?b ?c } }")
  expect_equal(nrow(df), 1)
  expect_equal(df$c[1], "B")
  expect_equal(m$size(ng2), 1)
  expect_equal(m$size(), 1)
})

test_that("the default graph's full IRI is also a valid graph argument", {
  # Ports test_basic_named_graph_named_default_as_arg (py_maplib/tests/
  # test_named_graphs.py:53-67).
  ng2 <- "urn:graph:gr2"
  m <- Model$new()
  m$reads(gr1, format = "ntriples", graph = ng2)
  m$reads(gr2, format = "ntriples", graph = "https://datatreehouse.github.io/maplib/vocab#DefaultGraph")

  df <- m$query("SELECT * WHERE { ?a ?b ?c }")
  expect_equal(nrow(df), 1)
  expect_equal(df$c[1], "B")
  expect_equal(m$size(ng2), 1)
  expect_equal(m$size(), 1)
})

test_that("size(graph) counts only that graph's triples", {
  # Ports test_basic_named_graph_more_triples (py_maplib/tests/
  # test_named_graphs.py:69-79).
  ng2 <- "urn:graph:gr2"
  ng3 <- "urn:graph:gr3"
  m <- Model$new()
  m$reads(gr1, format = "ntriples")
  m$reads(gr3, format = "ntriples", graph = ng2)

  expect_equal(m$size(ng2), 2)
  expect_equal(m$size(), 1)
  expect_equal(m$size(ng3), 0)
})

test_that("query() can join across two named graphs in one WHERE clause", {
  # Ports test_basic_named_graph_2 (py_maplib/tests/test_named_graphs.py:82-100).
  ng1 <- "urn:graph:gr1"
  ng2 <- "urn:graph:gr2"
  m <- Model$new()
  m$reads(gr1, format = "ntriples", graph = ng1)
  m$reads(gr2, format = "ntriples", graph = ng2)

  df <- m$query('
    SELECT * WHERE {
      GRAPH <urn:graph:gr1> { ?a ?b1 ?c1 }
      GRAPH <urn:graph:gr2> { ?a ?b2 ?c2 }
    }
  ')
  expect_equal(nrow(df), 1)
  expect_equal(df$c2[1], "B")
})

test_that("add_graph() copies a named graph's triples into another Model", {
  m1 <- Model$new()
  m1$reads(gr1, format = "ntriples")

  m2 <- Model$new()
  m2$add_graph(m1)
  expect_equal(m2$size(), 1)
  expect_match(m2$writes(format = "ntriples"), '"A"', fixed = TRUE)
})

test_that("add_graph() refuses to add a Model to itself", {
  # add_graph()'s underlying Rust call locks other then self in fixed
  # order; other = self would lock the same non-reentrant Mutex twice on
  # one thread and hang the R session. Must error instead.
  m <- Model$new()
  m$reads(gr1, format = "ntriples")
  expect_error(m$add_graph(m), "same underlying Model")
})

test_that("add_graph() refuses two distinct Model wrappers sharing one underlying RModel", {
  # The pointer comparison (not identical(self, other)) exists specifically
  # to also catch this case: two separate R6 objects wrapping the same
  # underlying Rust Mutex, as a shallow clone would produce.
  m <- Model$new()
  m$reads(gr1, format = "ntriples")
  aliased <- Model$new(m$.__enclos_env__$private$rmodel)
  expect_error(m$add_graph(aliased), "same underlying Model")
})

test_that("detach_graph() splits a named graph into a new, standalone Model", {
  # Ports test_detach_graph (py_maplib/tests/test_named_graphs.py:102-146).
  ng1 <- "urn:graph:gr1"
  ng2 <- "urn:graph:gr2"
  m <- Model$new()
  m$reads(gr1, format = "ntriples", graph = ng1)
  m$reads(gr2, format = "ntriples", graph = ng2)
  m2 <- m$detach_graph(preserve_name = TRUE, graph = ng2)

  expect_equal(nrow(m$query("SELECT * WHERE { GRAPH <urn:graph:gr2> { ?a ?b2 ?c2 } }")), 0)
  expect_equal(nrow(m$query("SELECT * WHERE { GRAPH <urn:graph:gr1> { ?a ?b1 ?c1 } }")), 1)

  df2 <- m2$query("SELECT * WHERE { GRAPH <urn:graph:gr2> { ?a ?b2 ?c2 } }")
  expect_equal(nrow(df2), 1)
  expect_equal(df2$c2[1], "B")
  expect_equal(nrow(m2$query("SELECT * WHERE { GRAPH <urn:graph:gr1> { ?a ?b1 ?c1 } }")), 0)
})

test_that("detach_graph() with no graph splits off the default graph", {
  # Ports test_detach_default_graph (py_maplib/tests/test_named_graphs.py:148-188).
  ng1 <- "urn:graph:gr1"
  m <- Model$new()
  m$reads(gr1, format = "ntriples", graph = ng1)
  m$reads(gr2, format = "ntriples")
  m2 <- m$detach_graph()

  expect_equal(nrow(m$query("SELECT * WHERE { ?a ?b2 ?c2 }")), 0)
  expect_equal(nrow(m$query("SELECT * WHERE { GRAPH <urn:graph:gr1> { ?a ?b1 ?c1 } }")), 1)

  df2 <- m2$query("SELECT * WHERE { ?a ?b2 ?c2 }")
  expect_equal(nrow(df2), 1)
  expect_equal(df2$c2[1], "B")
  expect_equal(nrow(m2$query("SELECT * WHERE { GRAPH <urn:graph:gr1> { ?a ?b1 ?c1 } }")), 0)
})

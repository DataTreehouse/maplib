# Tests for Model$query(bindings=) (maplib-aae) -- pre-binding SPARQL
# variables to R values before a query runs. Usage pattern (SELECT * plus a
# FILTER comparing against the bound variable, rather than SELECT-ing the
# bound variable directly) mirrors py_maplib's own test_replace_bindings.py.

test_that("bindings pre-binds a variable to a Literal, usable in a FILTER", {
  m <- Model$new()
  df <- m$query(
    'SELECT * WHERE { VALUES ?a { "a" "b" } FILTER(?a = ?b) }',
    bindings = list(b = Literal("b"))
  )
  expect_equal(nrow(df), 1)
  expect_equal(df$a, "b")
})

test_that("bindings pre-binds a variable to an IRI, usable in a FILTER", {
  m <- Model$new()
  df <- m$query(
    "SELECT * WHERE { VALUES ?a { \"a\" <urn:abc> } FILTER(?a = ?b) }",
    bindings = list(b = IRI("urn:abc"))
  )
  expect_equal(nrow(df), 1)
  expect_equal(df$a, "urn:abc")
})

test_that("a query with no bindings behaves exactly as before", {
  m <- Model$new()
  df <- m$query('SELECT * WHERE { VALUES ?a { "a" "b" } }')
  expect_equal(nrow(df), 2)
})

test_that("bindings values must be IRI or Literal objects", {
  m <- Model$new()
  expect_error(
    m$query('SELECT * WHERE { VALUES ?a { "a" } }', bindings = list(a = "not-a-term-object")),
    "IRI or Literal"
  )
})

test_that("binding a variable that's also SELECT-ed raises a catchable error, not a crash", {
  # Core-engine restriction (Triplestore::query's underlying
  # maybe_replace_bindings/replace_bindings_graph_pattern, lib/query_processing/
  # src/bindings.rs's Project match arm): a bound variable can't be listed
  # directly in SELECT, only used inside the query body (e.g. a FILTER).
  m <- Model$new()
  expect_error(
    m$query(
      'SELECT ?b WHERE { VALUES ?a { "a" } FILTER(?a = ?b) }',
      bindings = list(b = Literal("a"))
    ),
    class = "maplibr_maplib_error"
  )
})

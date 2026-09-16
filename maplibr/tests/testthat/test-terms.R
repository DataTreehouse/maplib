# Tests for terms.R's S7 value classes (maplib-snz). No pytest equivalent
# to port directly against -- py_maplib's IRI/Prefix/Variable/BlankNode/
# Literal tests are folded into larger query()/map()-based test files that
# aren't portable yet -- so these are written fresh, exercising the same
# constructors and validation py_maplib's own classes provide.

test_that("IRI validates syntax and formats", {
  i <- IRI("http://example.org/a")
  expect_equal(i@iri, "http://example.org/a")
  expect_match(format(i), "^IRI\\(")
  expect_error(IRI("not an iri"))
})

test_that("Prefix builds suffixed IRIs via suf()", {
  ex <- Prefix("http://example.org/", "ex")
  a <- suf(ex, "a")
  expect_true(inherits(a, "maplibr::IRI"))
  expect_equal(a@iri, "http://example.org/a")
})

test_that("Variable validates syntax and formats", {
  v <- Variable("x")
  expect_equal(v@name, "x")
  expect_equal(format(v), "?x")
  expect_error(Variable("bad variable name"))
})

test_that("BlankNode validates syntax and formats", {
  b <- BlankNode("b1")
  expect_equal(b@name, "b1")
  expect_equal(format(b), "_:b1")
  expect_error(BlankNode(""))
})

test_that("Literal defaults to xsd:string, and language beats datatype", {
  plain <- Literal("hello")
  expect_equal(plain@datatype@iri, "http://www.w3.org/2001/XMLSchema#string")
  expect_equal(format(plain), '"hello"')

  typed <- Literal("42", datatype = IRI("http://www.w3.org/2001/XMLSchema#integer"))
  expect_equal(format(typed), '"42"^^IRI(http://www.w3.org/2001/XMLSchema#integer)')

  tagged <- Literal("bonjour", language = "fr")
  expect_equal(tagged@datatype@iri, "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")
  expect_equal(format(tagged), '"bonjour"@fr')

  # language takes priority over an explicit datatype, matching py_maplib's
  # constructor precedence (lib/representation/src/python.rs:368-382)
  both <- Literal("x", datatype = IRI("http://example.org/dt"), language = "en")
  expect_equal(both@datatype@iri, "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")
})

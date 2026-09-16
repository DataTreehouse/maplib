# Ports the portable subset of py_maplib/tests/test_exceptions.py. Most of
# that file's exception cases only trigger inside query() (not implemented
# in maplibr yet); test_model_exception (invalid RDF syntax) doesn't need
# query() at all and ports directly. See maplib-dma's close notes for the
# maplibr_maplib_error/maplibr_argument_error condition classes.

test_that("reads() with invalid RDF syntax raises a catchable maplibr_maplib_error", {
  # Mirrors test_model_exception (py_maplib/tests/test_exceptions.py:7-11).
  m <- Model$new()
  expect_error(
    m$reads("abc", format = "turtle", graph = "http://example.com/data"),
    class = "maplibr_maplib_error"
  )
})

test_that("reads() with an unrecognized format raises a catchable maplibr_argument_error", {
  m <- Model$new()
  expect_error(
    m$reads('<http://a> <http://b> <http://c> .', format = "not-a-format"),
    class = "maplibr_argument_error"
  )
})

test_that("an unrecognized list_expander raises an error naming it", {
  # Not reclassed to maplibr_argument_error like Model's methods are --
  # templates.R's plain constructors aren't wired through .rethrow() (see
  # errors.R's top-of-file note) -- but the underlying [maplibr_argument_error]
  # tag is still present in the raw message.
  a <- IRI("http://example.org/a")
  expect_error(
    Triple(a, a, a, list_expander = "not-a-real-expander"),
    regexp = "not-a-real-expander"
  )
})

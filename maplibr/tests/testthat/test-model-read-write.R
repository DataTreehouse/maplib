# Tests for Model$read()/Model$write() (maplib-5us) -- file-based
# counterparts to $reads()/$writes().

test_that("write() then read() round-trips triples through a file, explicit format", {
  m <- Model$new()
  m$reads('<http://ex/a> <http://ex/p> <http://ex/o> .', format = "ntriples")

  path <- tempfile(fileext = ".nt")
  on.exit(unlink(path))
  m$write(path, format = "ntriples")

  m2 <- Model$new()
  m2$read(path, format = "ntriples")
  expect_equal(m2$size(), 1)
  df <- m2$query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
  expect_equal(df$s, "http://ex/a")
})

test_that("write() defaults to N-Triples format", {
  m <- Model$new()
  m$reads('<http://ex/a> <http://ex/p> <http://ex/o> .', format = "ntriples")

  path <- tempfile(fileext = ".nt")
  on.exit(unlink(path))
  m$write(path)
  expect_match(readLines(path), "http://ex/a", fixed = TRUE, all = FALSE)
})

test_that("read() guesses the format from the file extension when format is NULL", {
  m <- Model$new()
  m$reads('<http://ex/a> <http://ex/p> <http://ex/o> .', format = "ntriples")
  path <- tempfile(fileext = ".nt")
  on.exit(unlink(path))
  m$write(path, format = "ntriples")

  m2 <- Model$new()
  m2$read(path)
  expect_equal(m2$size(), 1)
})

test_that("read()/write()'s graph argument scopes to a single named graph", {
  m <- Model$new()
  m$reads('<http://ex/a> <http://ex/p> <http://ex/o> .', format = "ntriples", graph = "http://ex/g1")

  path <- tempfile(fileext = ".nt")
  on.exit(unlink(path))
  m$write(path, graph = "http://ex/g1")

  m2 <- Model$new()
  m2$read(path, graph = "http://ex/g2")
  expect_equal(m2$size(graph = "http://ex/g2"), 1)
  expect_equal(m2$size(), 0)
})

test_that("read() raises a catchable error for a nonexistent file", {
  m <- Model$new()
  expect_error(
    m$read(tempfile(fileext = ".nt")),
    class = "maplibr_maplib_error"
  )
})

test_that("read() raises a catchable error for an unsupported or missing extension, instead of crashing", {
  # Regression case: read()'s format-guessing used to pass format = NULL
  # straight through to the core engine's own extension guesser, whose
  # unrecognized-extension fallback is a bare todo!() -- a panic, and with
  # this crate's release profile set to panic = "abort", that would abort
  # the whole R session rather than raise a catchable error. Guessing the
  # format in the R wrapper itself (guess_format_from_extension) means an
  # unrecognized extension is a normal error well before reaching that code.
  m <- Model$new()
  expect_error(m$read(tempfile()), class = "maplibr_argument_error")
  expect_error(m$read(tempfile(fileext = ".csv")), class = "maplibr_argument_error")
})

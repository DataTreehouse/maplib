# Ports the non-query subset of py_maplib/tests/test_basics.py. Most of
# that file's assertions go through Model.query()/Model.map(), neither of
# which exist in maplibr yet (see maplib-a2d's close notes) -- only the
# handful of assertions reachable without them are ported here.

test_that("writes() on an empty Model returns without erroring", {
  # Mirrors test_write_empty_model (py_maplib/tests/test_basics.py:20-22).
  m <- Model$new()
  s <- m$writes()
  expect_type(s, "character")
})

test_that("reads()/writes() round-trip and chain", {
  m <- Model$new()
  result <- m$reads('<http://a> <http://b> "c" .', format = "ntriples")
  expect_identical(result, m) # chainable, returns self invisibly

  expect_equal(m$size(), 1)
  expect_match(m$writes(format = "ntriples"), "<http://a>", fixed = TRUE)
})

test_that("create_index() is chainable and doesn't error on a populated Model", {
  m <- Model$new()
  m$reads('<http://a> <http://b> "c" .', format = "ntriples")
  result <- m$create_index()
  expect_identical(result, m)
})

test_that("truncate_graph() empties the default graph", {
  m <- Model$new()
  m$reads('<http://a> <http://b> "c" .', format = "ntriples")
  expect_equal(m$size(), 1)
  m$truncate_graph()
  expect_equal(m$size(), 0)
})

test_that("add_prefixes() affects writes() output", {
  m <- Model$new()
  m$reads('<http://example.org/a> <http://example.org/b> <http://example.org/c> .', format = "ntriples")
  m$add_prefixes(list(ex = "http://example.org/"))
  out <- m$writes(format = "turtle")
  expect_match(out, "@prefix ex: <http://example.org/>", fixed = TRUE)
})

test_that("serialize()/deserialize() no longer require path -- it defaults to \"./serialized_triples\"", {
  # Before this fix, `path` was a required argument in the underlying Rust
  # signature -- omitting it would fail immediately with R's own
  # missing-argument error, before ever reaching serialize_triples()/
  # deserialize_triples(). It's optional now (defaulting on the Rust side to
  # "./serialized_triples", matching py_maplib): calling with no path still
  # errors -- the on-disk store is a closed-source stub in this checkout,
  # panicking regardless of path -- but not from a missing argument.
  is_missing_arg_error <- function(msg) grepl("argument", msg, fixed = TRUE) && grepl("missing", msg, fixed = TRUE)

  # serialize() creates real on-disk structure at its (default) path before
  # panicking in the stub -- run from a scratch directory so this doesn't
  # leave a "serialized_triples" directory behind in the working tree.
  scratch_dir <- tempfile("maplibr-test-")
  dir.create(scratch_dir)
  old_wd <- setwd(scratch_dir)
  on.exit({
    setwd(old_wd)
    unlink(scratch_dir, recursive = TRUE)
  }, add = TRUE)

  m <- Model$new()
  m$reads('<http://a> <http://b> "c" .', format = "ntriples")
  err <- tryCatch(m$serialize(), error = function(e) conditionMessage(e))
  expect_false(is_missing_arg_error(err))

  err2 <- tryCatch(Model$deserialize(), error = function(e) conditionMessage(e))
  expect_false(is_missing_arg_error(err2))
})

test_that("clone() is disabled -- R6's default shallow clone would share the underlying Mutex", {
  # R6's default clone() only copies the R-level reference to
  # private$rmodel, not the underlying Rust state -- m2 <- m$clone() would
  # look independent while actually sharing the same mutex-guarded
  # triplestore. cloneable = FALSE removes $clone() entirely rather than
  # leaving a misleading shallow one in place.
  m <- Model$new()
  expect_null(m$clone)
  expect_error(m$clone())
})

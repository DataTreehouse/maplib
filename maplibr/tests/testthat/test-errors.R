# Direct unit tests for .reclass_maplibr_error()/.rethrow()'s regex-parsing
# and fallback logic, flagged by roborev review of commits 5a55ec5/c670e29
# as still untested: every error test elsewhere goes through Model$reads(),
# which only ever exercises the maplibr_maplib_error/maplibr_argument_error
# tags -- never the fallback branch for a malformed/unrecognized prefix, and
# never maplibr_runtime_error at all. No compiled code needed for these.

test_that(".rethrow() reclassifies each known tag with its message intact", {
  for (tag in c("maplibr_maplib_error", "maplibr_argument_error", "maplibr_runtime_error")) {
    err <- tryCatch(
      .rethrow(stop(sprintf("[%s] something went wrong", tag))),
      error = function(e) e
    )
    expect_s3_class(err, tag)
    expect_s3_class(err, "maplibr_error")
    expect_equal(conditionMessage(err), "something went wrong")
  }
})

test_that(".rethrow() falls back to a generic maplibr_error for an unrecognized tag", {
  err <- tryCatch(
    .rethrow(stop("[maplibr_totally_made_up_error] oops")),
    error = function(e) e
  )
  expect_s3_class(err, "maplibr_error")
  expect_false(inherits(err, "maplibr_maplib_error"))
  expect_false(inherits(err, "maplibr_argument_error"))
  expect_false(inherits(err, "maplibr_runtime_error"))
  # Malformed/unrecognized prefixes are not stripped from the message --
  # only a recognized tag is parsed out.
  expect_equal(conditionMessage(err), "[maplibr_totally_made_up_error] oops")
})

test_that(".rethrow() falls back to a generic maplibr_error for a message with no tag at all", {
  err <- tryCatch(
    .rethrow(stop("plain, untagged error")),
    error = function(e) e
  )
  expect_s3_class(err, "maplibr_error")
  expect_equal(conditionMessage(err), "plain, untagged error")
})

test_that(".rethrow() passes through an already-reclassed maplibr_error unchanged", {
  original <- structure(
    class = c("maplibr_argument_error", "maplibr_error", "error", "condition"),
    list(message = "already reclassed", call = NULL)
  )
  err <- tryCatch(.rethrow(stop(original)), error = function(e) e)
  expect_identical(err, original)
})

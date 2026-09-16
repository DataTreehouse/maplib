# R-side half of maplib-dma: mirrors the intent of py_maplib's typed
# PyMaplibError/MaplibException/FunctionArgumentException hierarchy
# (py_maplib/src/error.rs), adapted to savvy's error boundary, which can only
# carry a plain message string across (unlike pyo3, which lets Rust raise an
# actual typed Python exception object). maplibr/src/rust/src/errors.rs
# tags every error it raises with a `[maplibr_xxx_error] ` prefix;
# `.rethrow()` parses that prefix back out here and re-raises as a proper R
# condition carrying a matching class, so R code can catch by class the same
# way Python code catches by exception type:
#
#   tryCatch(m$reads(...), maplibr_argument_error = function(e) ...)
#
# Every Model method that calls into the underlying RModel savvy object goes
# through `.rethrow()` (see model.R) -- S7 term/template constructors
# (terms.R/templates.R) still surface the tagged message as plain text via
# their validators (S7 wraps validator failures in its own condition class
# already), not reclassified the same way; that's lower-value there since
# those are all just simple argument-validation failures to begin with, not
# the richer MaplibError/RuntimeError distinction this exists for.

.maplibr_error_classes <- c(
  "maplibr_maplib_error",
  "maplibr_argument_error",
  "maplibr_runtime_error"
)

.reclass_maplibr_error <- function(e) {
  msg <- conditionMessage(e)
  m <- regmatches(msg, regexec("^\\[(maplibr_[a-z_]+)\\] (.*)$", msg))[[1]]
  if (length(m) == 3 && m[2] %in% .maplibr_error_classes) {
    cls <- c(m[2], "maplibr_error", "error", "condition")
    msg <- m[3]
  } else {
    cls <- c("maplibr_error", "error", "condition")
  }
  stop(structure(class = cls, list(message = msg, call = NULL)))
}

#' Run `expr`, reclassifying any maplibr-tagged error into a proper R
#' condition catchable by class.
#'
#' @param expr An expression to evaluate.
#' @noRd
.rethrow <- function(expr) {
  tryCatch(expr, error = function(e) {
    if (inherits(e, "maplibr_error")) {
      stop(e)
    }
    .reclass_maplibr_error(e)
  })
}

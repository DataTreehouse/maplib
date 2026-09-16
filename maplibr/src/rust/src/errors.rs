use std::fmt::Display;

/// Mirrors py_maplib's `PyMaplibError` (py_maplib/src/error.rs) -- but savvy's
/// FFI boundary can only carry a plain error message string across (unlike
/// pyo3, which lets Rust raise an actual typed Python exception object), so
/// the "typed exception" intent travels as a machine-parseable `[tag]
/// message` prefix instead. `maplibr/R/errors.R`'s `.rethrow()` parses that
/// tag back out and re-raises as a proper R condition with a matching class,
/// so R code can `tryCatch(..., maplibr_argument_error = function(e) ...)`
/// the same way Python code can `except FunctionArgumentException`.
pub enum RMaplibError {
    /// Wraps `maplib::errors::MaplibError` -- mirrors
    /// `PyMaplibError::MaplibError`, the transparent `#[from]` variant
    /// covering all of `MaplibError`'s many inner error types uniformly.
    /// Matches py_maplib's own granularity: one exception class for the
    /// whole enum, not one per inner variant.
    Maplib(maplib::errors::MaplibError),
    /// A bad argument from R: invalid IRI/variable-name/blank-node-id
    /// syntax, an unrecognized format/list_expander string, etc. Mirrors
    /// `PyMaplibError::FunctionArgumentError`.
    Argument(String),
    /// Something that should never happen (e.g. maplib producing triple
    /// output that isn't valid UTF-8). Mirrors `PyMaplibError::RuntimeError`.
    Runtime(String),
}

impl RMaplibError {
    fn tag(&self) -> &'static str {
        match self {
            RMaplibError::Maplib(_) => "maplibr_maplib_error",
            RMaplibError::Argument(_) => "maplibr_argument_error",
            RMaplibError::Runtime(_) => "maplibr_runtime_error",
        }
    }
}

impl Display for RMaplibError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RMaplibError::Maplib(e) => write!(f, "{e}"),
            RMaplibError::Argument(s) => write!(f, "{s}"),
            RMaplibError::Runtime(s) => write!(f, "{s}"),
        }
    }
}

impl From<maplib::errors::MaplibError> for RMaplibError {
    fn from(e: maplib::errors::MaplibError) -> Self {
        RMaplibError::Maplib(e)
    }
}

impl From<RMaplibError> for savvy::Error {
    fn from(e: RMaplibError) -> Self {
        savvy::Error::new(&format!("[{}] {}", e.tag(), e))
    }
}

/// Wrap a bad-argument error (an `IriParseError`, `VariableNameParseError`,
/// `BlankNodeIdParseError`, or a hand-written message) as a `savvy::Error`
/// tagged for R-side reclassification via `.rethrow()`.
pub fn argument_error(msg: impl Display) -> savvy::Error {
    RMaplibError::Argument(msg.to_string()).into()
}

/// Wrap an "this should never happen" error as a `savvy::Error` tagged for
/// R-side reclassification via `.rethrow()`.
pub fn runtime_error(msg: impl Display) -> savvy::Error {
    RMaplibError::Runtime(msg.to_string()).into()
}

/// Wrap a `maplib::errors::MaplibError` as a `savvy::Error` tagged for
/// R-side reclassification via `.rethrow()`.
pub fn maplib_error(e: maplib::errors::MaplibError) -> savvy::Error {
    RMaplibError::from(e).into()
}

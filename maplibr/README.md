# maplibr

An R interface to the [maplib](https://github.com/DataTreehouse/maplib) knowledge
graph engine: build, query, and reason over RDF graphs from tabular data using
OTTR mapping templates, SPARQL, and the Arrow C Stream Interface for zero-copy
data frame results.

## Installation

```r
# from source, requires Cargo/rustc (see SystemRequirements in DESCRIPTION)
devtools::install()
```

## Known issue: `undefined symbol: _Py_Dealloc` when loading the package

If `devtools::load_all()`, `devtools::check()`, or `library(maplibr)` fails
with something like:

```
Error in dyn.load(...) :
  unable to load shared object '.../maplibr.so':
  .../maplibr.so: undefined symbol: _Py_Dealloc
```

this is a known upstream issue in `maplib`, not a bug in this package.
`maplib`'s public API has an unconditional dependency on `pyo3`
(Python's Rust FFI crate), so `maplibr` is forced to compile against it
even though it never uses Python. On Linux, `pyo3-ffi`
never links `libpython` at build time — it expects the *loading process* to
supply those symbols, which works for Python but not for R's `dyn.load()`.

**Workaround:** preload `libpython` into R's process before it loads the
package:

```bash
LD_PRELOAD=$(python3-config --prefix)/lib/libpython3.$(python3 -c 'import sys;print(sys.version_info[1])').so.1.0 \
  Rscript -e 'devtools::load_all("maplibr")'
```

If that path doesn't resolve on your system, find the right one with
`ldconfig -p | grep libpython`. Needed for `load_all()`, `document()`,
`check()`, and any R session that actually calls into `Model`.

.onLoad <- function(libname, pkgname) {
  S7::methods_register()

  # Vocab constants (vocab.R) need a compiled Rust call to build (each is an
  # IRI(), validated via validate_iri()) -- built here, not at vocab.R's own
  # top level, since .onLoad is guaranteed to run after useDynLib()
  # registration completes and top-level R/*.R sourcing is not.
  xsd <<- .build_xsd_vocab()
  rdf <<- .build_rdf_vocab()
  rdfs <<- .build_rdfs_vocab()
  owl <<- .build_owl_vocab()
}

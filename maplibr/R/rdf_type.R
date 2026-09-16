# RDFType (maplib-snz, continued). Mirrors py_maplib's `RDFType`
# (lib/representation/src/python.rs:101-268), used as a Parameter's
# `rdf_type` -- but only its "flat" cases: IRI/BlankNode/Literal/Unknown/
# Multi. All of these ultimately collapse to a single basic-type IRI once
# they reach the Rust side (templates::ast::PType::from(&RDFNodeState),
# lib/templates/src/ast.rs:211-227): IRI/BlankNode/Literal keep their own
# type IRI, Unknown has no type constraint, and Multi (a parameter that can
# hold more than one RDF node type) collapses all the way down to
# rdfs:Resource, the OTTR top type -- there is no richer "one-of these
# specific types" representation to preserve on the Rust side. Hence
# `RDFType` here is intentionally just a wrapper around that one resulting
# IRI (or NULL for Unknown), not a structured value.
#
# Built via plain functions (rdf_type_iri()/rdf_type_blank_node()/...), not
# `RDFType$IRI()`-style "static methods" the way Model$deserialize works --
# S7 class generators refuse `$<-` assignment (`Can't set S7 properties with
# $`), unlike R6 generators or savvy's plain-environment classes, so that
# pattern isn't available here.
#
# `RDFType.Nested(...)` is NOT ported -- it represents a parameter whose
# column itself holds nested/list values (templates::MappingColumnType::
# Nested / PType::List), which needs Parameter's Rust constructor to accept
# a richer type than a single basic-type IRI string. No current consumer
# needs it yet (Model$map() itself isn't ported), so it's left for
# follow-up work alongside the rest of the RDFType port.

#' An RDF node type, for use as a Parameter's `rdf_type`.
#'
#' Mirrors py_maplib's `RDFType` (lib/representation/src/python.rs:101-268).
#' Only represents a single basic type -- see the note in
#' `maplibr/R/rdf_type.R` for why `rdf_type_multi()` collapses multiple types
#' down to one IRI (`rdfs:Resource`) rather than preserving the set, and why
#' `RDFType.Nested()` isn't ported. Build one with `rdf_type_iri()`,
#' `rdf_type_blank_node()`, `rdf_type_literal()`, `rdf_type_unknown()`, or
#' `rdf_type_multi()`, not by calling `RDFType()` directly.
#'
#' @param basic_type_iri The resolved basic-type IRI, or NULL for
#'   `rdf_type_unknown()` (no type constraint).
#' @export
RDFType <- S7::new_class(
  "RDFType",
  package = "maplibr",
  properties = list(basic_type_iri = S7::new_union(IRI, NULL))
)

#' @export
S7::method(format, RDFType) <- function(x, ...) {
  if (is.null(x@basic_type_iri)) {
    "RDFType.Unknown()"
  } else {
    paste0("RDFType(", x@basic_type_iri@iri, ")")
  }
}

#' @export
S7::method(print, RDFType) <- function(x, ...) {
  cat(format(x), "\n", sep = "")
  invisible(x)
}

#' @rdname RDFType
#' @export
rdf_type_iri <- function() {
  RDFType(basic_type_iri = IRI("http://ns.ottr.xyz/0.4/IRI"))
}

#' @rdname RDFType
#' @export
rdf_type_blank_node <- function() {
  RDFType(basic_type_iri = IRI("http://ns.ottr.xyz/0.4/BlankNode"))
}

#' @param iri_or_datatype An IRI (the literal's datatype), or a plain IRI string.
#' @rdname RDFType
#' @export
rdf_type_literal <- function(iri_or_datatype) {
  iri <- if (S7::S7_inherits(iri_or_datatype, IRI)) iri_or_datatype else IRI(iri_or_datatype)
  RDFType(basic_type_iri = iri)
}

#' @rdname RDFType
#' @export
rdf_type_unknown <- function() {
  RDFType(basic_type_iri = NULL)
}

#' @param rdf_types A list of RDFTypes.
#' @rdname RDFType
#' @export
rdf_type_multi <- function(rdf_types) {
  stopifnot(all(vapply(rdf_types, function(t) S7::S7_inherits(t, RDFType), logical(1))))
  RDFType(basic_type_iri = rdfs$Resource)
}

.as_rdf_type_iri <- function(x) {
  if (is.null(x)) {
    NULL
  } else if (S7::S7_inherits(x, IRI)) {
    x@iri
  } else if (S7::S7_inherits(x, RDFType)) {
    if (is.null(x@basic_type_iri)) NULL else x@basic_type_iri@iri
  } else {
    stop("`rdf_type` must be an IRI, an RDFType, or NULL", call. = FALSE)
  }
}

# RDF term value classes (maplib-snz). S7, not R6: these are built once and
# passed into Model$map()/reads() etc., never mutated in place -- see
# CLAUDE.md's "R-side OOP split" note. Mirrors py_maplib's PyIRI/PyPrefix/
# PyVariable/PyLiteral/PyBlankNode (lib/representation/src/python.rs).
#
# Syntax validation (IRI/variable-name/blank-node-id grammar) is delegated to
# the same oxrdf parsers py_maplib uses, via validate_iri()/
# validate_variable_name()/validate_blank_node_id() (maplibr/src/rust/src/terms.rs)
# -- not reimplemented as R regexes, to avoid drifting from oxrdf's grammar.

#' An RDF IRI (Internationalized Resource Identifier)
#'
#' @param iri The IRI string.
#' @export
IRI <- S7::new_class(
  "IRI",
  package = "maplibr",
  properties = list(iri = S7::class_character),
  validator = function(self) {
    if (length(self@iri) != 1 || is.na(self@iri)) {
      return("@iri must be a single, non-NA string")
    }
    tryCatch(
      {
        validate_iri(self@iri)
        NULL
      },
      error = function(e) conditionMessage(e)
    )
  }
)

#' @export
S7::method(format, IRI) <- function(x, ...) paste0("IRI(", x@iri, ")")

#' @export
S7::method(print, IRI) <- function(x, ...) {
  cat(format(x), "\n", sep = "")
  invisible(x)
}

#' An RDF namespace prefix: a short name bound to an IRI.
#'
#' @param iri The IRI the prefix expands to.
#' @param prefix_name Optional short name for the prefix (e.g. "ex").
#' @export
Prefix <- S7::new_class(
  "Prefix",
  package = "maplibr",
  properties = list(
    iri = S7::class_character,
    prefix_name = S7::new_union(S7::class_character, NULL)
  ),
  validator = function(self) {
    if (length(self@iri) != 1 || is.na(self@iri)) {
      return("@iri must be a single, non-NA string")
    }
    tryCatch(
      {
        validate_iri(self@iri)
        NULL
      },
      error = function(e) conditionMessage(e)
    )
  }
)

#' Suffix a Prefix's IRI, returning a new IRI.
#'
#' Mirrors py_maplib's `Prefix.suf()` (lib/representation/src/python.rs:326-328).
#'
#' @param prefix A Prefix.
#' @param suffix String to append to the prefix's IRI.
#' @return A new IRI.
#' @export
suf <- S7::new_generic("suf", "prefix")

#' @export
S7::method(suf, Prefix) <- function(prefix, suffix) {
  IRI(paste0(prefix@iri, suffix))
}

#' @export
S7::method(format, Prefix) <- function(x, ...) {
  if (is.null(x@prefix_name)) {
    paste0("Prefix(", x@iri, ")")
  } else {
    paste0("Prefix(", x@prefix_name, ": ", x@iri, ")")
  }
}

#' @export
S7::method(print, Prefix) <- function(x, ...) {
  cat(format(x), "\n", sep = "")
  invisible(x)
}

#' A SPARQL/OTTR variable.
#'
#' @param name The variable's name, without the leading `?`/`$`.
#' @export
Variable <- S7::new_class(
  "Variable",
  package = "maplibr",
  properties = list(name = S7::class_character),
  validator = function(self) {
    if (length(self@name) != 1 || is.na(self@name)) {
      return("@name must be a single, non-NA string")
    }
    tryCatch(
      {
        validate_variable_name(self@name)
        NULL
      },
      error = function(e) conditionMessage(e)
    )
  }
)

#' @export
S7::method(format, Variable) <- function(x, ...) paste0("?", x@name)

#' @export
S7::method(print, Variable) <- function(x, ...) {
  cat(format(x), "\n", sep = "")
  invisible(x)
}

#' An RDF blank node.
#'
#' @param name The blank node's identifier, without the leading `_:`.
#' @export
BlankNode <- S7::new_class(
  "BlankNode",
  package = "maplibr",
  properties = list(name = S7::class_character),
  validator = function(self) {
    if (length(self@name) != 1 || is.na(self@name)) {
      return("@name must be a single, non-NA string")
    }
    tryCatch(
      {
        validate_blank_node_id(self@name)
        NULL
      },
      error = function(e) conditionMessage(e)
    )
  }
)

#' @export
S7::method(format, BlankNode) <- function(x, ...) paste0("_:", x@name)

#' @export
S7::method(print, BlankNode) <- function(x, ...) {
  cat(format(x), "\n", sep = "")
  invisible(x)
}

.XSD_STRING <- "http://www.w3.org/2001/XMLSchema#string"
.RDF_LANG_STRING <- "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"

#' An RDF literal: a value, with either a datatype IRI or a language tag.
#'
#' Mirrors py_maplib's `Literal` (lib/representation/src/python.rs:359-397).
#' Providing `language` takes priority over `datatype` (matching the Python
#' constructor's precedence); providing neither defaults to `xsd:string`.
#'
#' @param value The literal's lexical value, as a string.
#' @param datatype Optional datatype IRI (an IRI object). Ignored if `language` is given.
#' @param language Optional BCP 47 language tag (not syntax-checked, matching
#'   oxrdf::Literal::new_language_tagged_literal_unchecked -- py_maplib does
#'   the same).
#' @export
Literal <- S7::new_class(
  "Literal",
  package = "maplibr",
  properties = list(
    value = S7::class_character,
    datatype = IRI,
    language = S7::new_union(S7::class_character, NULL)
  ),
  validator = function(self) {
    if (length(self@value) != 1 || is.na(self@value)) {
      return("@value must be a single, non-NA string")
    }
    if (!is.null(self@language) && self@datatype@iri != .RDF_LANG_STRING) {
      return("a language-tagged Literal must have datatype rdf:langString")
    }
    NULL
  },
  constructor = function(value, datatype = NULL, language = NULL) {
    if (!is.null(language)) {
      datatype <- IRI(.RDF_LANG_STRING)
    } else if (is.null(datatype)) {
      datatype <- IRI(.XSD_STRING)
    }
    S7::new_object(S7::S7_object(), value = value, datatype = datatype, language = language)
  }
)

#' @export
S7::method(format, Literal) <- function(x, ...) {
  if (!is.null(x@language)) {
    paste0("\"", x@value, "\"@", x@language)
  } else if (x@datatype@iri == .XSD_STRING) {
    paste0("\"", x@value, "\"")
  } else {
    paste0("\"", x@value, "\"^^", format(x@datatype))
  }
}

#' @export
S7::method(print, Literal) <- function(x, ...) {
  cat(format(x), "\n", sep = "")
  invisible(x)
}

# Converts an IRI/Literal into an RGroundTerm (maplibr/src/rust/src/terms.rs)
# for Model$query()'s bindings= argument. Mirrors templates.R's
# .as_raw_constant_term() -- no BlankNode/"none" cases here, since a query
# variable can only ever be bound to an IRI or a literal value.
.to_ground_term <- function(x) {
  if (S7::S7_inherits(x, IRI)) {
    RGroundTerm$iri(x@iri)
  } else if (S7::S7_inherits(x, Literal)) {
    RGroundTerm$literal(x@value, x@datatype@iri, x@language)
  } else {
    stop("bindings values must be IRI or Literal objects, got: ", class(x)[1], call. = FALSE)
  }
}

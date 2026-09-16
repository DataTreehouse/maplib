# OTTR template/instance/parameter/argument value classes (maplib-snz,
# continued). Unlike the pure-R term classes in terms.R, these need real
# Rust-side construction -- they become part of a templates::ast::Template
# eventually passed to Model$map() -- so, like Model/RModel, there is a thin
# S7 wrapper here over a raw savvy object (RConstantTerm/RArgument/
# RParameter/RInstance/RTemplate, maplibr/src/rust/src/templates.rs). Still
# S7, not R6: built once, never mutated in place.

.as_raw_constant_term <- function(x) {
  if (is.null(x)) {
    RConstantTerm$none()
  } else if (S7::S7_inherits(x, IRI)) {
    RConstantTerm$iri(x@iri)
  } else if (S7::S7_inherits(x, BlankNode)) {
    RConstantTerm$blank_node(x@name)
  } else if (S7::S7_inherits(x, Literal)) {
    RConstantTerm$literal(x@value, x@datatype@iri, x@language)
  } else {
    stop("expected an IRI, BlankNode, Literal, or NULL", call. = FALSE)
  }
}

#' An OTTR template argument: a Variable, or a constant term.
#'
#' Mirrors py_maplib's `Argument` (lib/templates/src/python.rs:203-236).
#' Passing an existing Argument as `term` returns it unchanged (`list_expand`
#' is ignored in that case), matching the Python constructor's behavior.
#'
#' @param term A Variable, IRI, BlankNode, Literal, NULL, or an existing Argument.
#' @param list_expand Whether this argument should be OTTR list-expanded.
#' @export
Argument <- S7::new_class(
  "Argument",
  package = "maplibr",
  properties = list(raw = S7::class_any),
  constructor = function(term, list_expand = FALSE) {
    if (S7::S7_inherits(term, Argument)) {
      return(term)
    }
    raw <- if (S7::S7_inherits(term, Variable)) {
      RArgument$from_variable(term@name, list_expand)
    } else {
      RArgument$from_constant_term(.as_raw_constant_term(term), list_expand)
    }
    S7::new_object(S7::S7_object(), raw = raw)
  }
)

.as_argument_ptr <- function(x) {
  .savvy_extract_ptr(Argument(x)@raw, "maplibr::RArgument")
}

#' An OTTR template parameter.
#'
#' Mirrors py_maplib's `Parameter` (lib/templates/src/python.rs:21-167).
#' `rdf_type` only supports a basic (non-nested, non-list) RDF type --
#' `RDFType$Nested()` is not ported (see `maplibr/R/rdf_type.R`).
#'
#' @param variable A Variable.
#' @param optional Whether this parameter is optional.
#' @param allow_blank Whether a blank node is an acceptable value (default TRUE).
#' @param rdf_type Optional: an IRI (a datatype/class IRI directly) or an
#'   RDFType, giving the parameter's expected RDF type.
#' @param default_value Optional default value: an IRI, BlankNode, or Literal.
#' @export
Parameter <- S7::new_class(
  "Parameter",
  package = "maplibr",
  properties = list(raw = S7::class_any, variable = Variable),
  constructor = function(variable,
                          optional = FALSE,
                          allow_blank = TRUE,
                          rdf_type = NULL,
                          default_value = NULL) {
    stopifnot(S7::S7_inherits(variable, Variable))
    rdf_type_iri <- .as_rdf_type_iri(rdf_type)
    default_raw <- if (is.null(default_value)) {
      NULL
    } else {
      .as_raw_constant_term(default_value)
    }
    raw <- RParameter$new(variable@name, optional, allow_blank, rdf_type_iri, default_raw)
    S7::new_object(S7::S7_object(), raw = raw, variable = variable)
  }
)

.as_parameter_ptr <- function(x) {
  p <- if (S7::S7_inherits(x, Parameter)) x else Parameter(x)
  .savvy_extract_ptr(p@raw, "maplibr::RParameter")
}

#' An OTTR template instance: a call to a template with concrete arguments.
#'
#' Mirrors py_maplib's `Instance` (lib/templates/src/python.rs:245-288).
#'
#' @param template_iri An IRI: the template being instantiated.
#' @param arguments A list of Arguments (or bare Variable/IRI/BlankNode/
#'   Literal/NULL terms, auto-wrapped as non-list-expanded Arguments).
#' @param list_expander Optional: one of "cross", "zipMin", "zipMax".
#' @export
Instance <- S7::new_class(
  "Instance",
  package = "maplibr",
  properties = list(raw = S7::class_any),
  constructor = function(template_iri, arguments = NULL, list_expander = NULL) {
    stopifnot(S7::S7_inherits(template_iri, IRI))
    arg_ptrs <- lapply(arguments, .as_argument_ptr)
    raw <- RInstance$new(template_iri@iri, arg_ptrs, list_expander)
    S7::new_object(S7::S7_object(), raw = raw)
  }
)

# Wraps an already-built RInstance (from Template$instance()/make_triple(),
# which validate their own arguments) as an Instance, without exposing a
# validation-bypassing raw-pointer parameter on Instance()'s own public
# constructor. S7's new_object() can only be called from within the literal
# function registered as some class's constructor -- not from an arbitrary
# unexported helper, even one Instance()'s own constructor delegates to --
# so this is a genuine subclass with its own constructor, not just a thin
# wrapper. Its instances still satisfy S7_inherits(x, Instance) (and base
# inherits()), since S7 subclass instances carry their full ancestor class
# vector, so every existing Instance check downstream is unaffected.
.RawInstance <- S7::new_class(
  "RawInstance",
  package = "maplibr",
  parent = Instance,
  constructor = function(raw) S7::new_object(S7::S7_object(), raw = raw)
)

#' The IRI of the template an Instance calls.
#'
#' @param x An Instance.
#' @export
instance_template_iri <- S7::new_generic("instance_template_iri", "x")

#' @export
S7::method(instance_template_iri, Instance) <- function(x) IRI(x@raw$iri())

#' An OTTR template: a named, parameterized set of instance patterns.
#'
#' Mirrors py_maplib's `Template` (lib/templates/src/python.rs:291-402).
#'
#' @param iri An IRI.
#' @param parameters A list of Parameters (or bare Variables, auto-wrapped as
#'   required, blank-allowed Parameters with no type constraint).
#' @param instances A list of Instances.
#' @export
Template <- S7::new_class(
  "Template",
  package = "maplibr",
  properties = list(raw = S7::class_any, iri = IRI),
  constructor = function(iri, parameters, instances) {
    stopifnot(S7::S7_inherits(iri, IRI))
    param_ptrs <- lapply(parameters, .as_parameter_ptr)
    stopifnot(all(vapply(instances, function(i) S7::S7_inherits(i, Instance), logical(1))))
    instance_ptrs <- lapply(instances, function(i) .savvy_extract_ptr(i@raw, "maplibr::RInstance"))
    raw <- RTemplate$new(iri@iri, param_ptrs, instance_ptrs)
    S7::new_object(S7::S7_object(), raw = raw, iri = iri)
  }
)

#' @export
S7::method(format, Template) <- function(x, ...) x@raw$print_string()

#' @export
S7::method(print, Template) <- function(x, ...) {
  cat(format(x), "\n", sep = "")
  invisible(x)
}

#' Build a new Instance calling a Template with concrete arguments.
#'
#' Mirrors py_maplib's `Template.instance()` (lib/templates/src/python.rs:335-346).
#'
#' @param template A Template.
#' @param arguments A list of Arguments (or bare terms, auto-wrapped).
#' @param list_expander Optional: one of "cross", "zipMin", "zipMax".
#' @return A new Instance.
#' @export
instantiate <- S7::new_generic("instantiate", "template")

#' @export
S7::method(instantiate, Template) <- function(template, arguments, list_expander = NULL) {
  arg_ptrs <- lapply(arguments, .as_argument_ptr)
  raw <- template@raw$instance(arg_ptrs, list_expander)
  .RawInstance(raw)
}

#' Build an rdf:type-style Triple instance.
#'
#' Mirrors py_maplib's `Triple()` helper (lib/templates/src/python.rs:404-417).
#'
#' @param subject Argument-compatible term.
#' @param predicate Argument-compatible term.
#' @param object Argument-compatible term.
#' @param list_expander Optional: one of "cross", "zipMin", "zipMax".
#' @return A new Instance.
#' @export
Triple <- function(subject, predicate, object, list_expander = NULL) {
  s <- Argument(subject)
  p <- Argument(predicate)
  o <- Argument(object)
  raw <- make_triple(s@raw, p@raw, o@raw, list_expander)
  .RawInstance(raw)
}

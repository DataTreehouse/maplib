# Pipe-friendly functional wrappers around Model's R6 methods (`model_*()`),
# so `m |> model_query(...)` works as an alternative to `m$query(...)` --
# both stay available side by side, same convention `arrow` (Table/Dataset
# R6 objects plus dplyr-verb S3 methods) and `torch` (`x$add(y)` alongside
# free-function `torch_add(x, y)`) already use. Named with a `model_`
# prefix rather than bare verbs (`read()`/`write()`/`map()`/`update()`)
# because those would mask `base::write`, `purrr::map`, and the
# `stats::update` S3 generic respectively if this package were attached
# alongside a normal tidyverse session.
#
# Every function here is a one-line passthrough: call the matching `Model`
# method and return exactly what it returns. Mutating `Model` methods
# already return `invisible(self)` (see model.R), so no extra work is
# needed here to make chaining/piping work -- these wrappers don't add
# behavior, only an alternative calling convention. See `?Model` for full
# parameter documentation; the descriptions below are intentionally terse.

#' Number of triples in a graph
#'
#' Pipe-friendly wrapper for `Model$size()`.
#' @param m A `Model`.
#' @param graph Optional named graph IRI to count (default graph if NULL).
#' @return An integer triple count.
#' @seealso [Model]
#' @export
model_size <- function(m, graph = NULL) {
  m$size(graph)
}

#' Parse RDF triples from a string into a Model
#'
#' Pipe-friendly wrapper for `Model$reads()`.
#' @param m A `Model`.
#' @param s RDF data as a string.
#' @param format One of "ntriples", "turtle", "xml" (rdf/xml).
#' @param graph Optional named graph IRI to read into (default graph if NULL).
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_reads <- function(m, s, format, graph = NULL) {
  m$reads(s, format, graph)
}

#' Parse RDF triples from a file into a Model
#'
#' Pipe-friendly wrapper for `Model$read()`.
#' @param m A `Model`.
#' @param path Path to the RDF file to read.
#' @param format One of "ntriples", "turtle", "xml" (rdf/xml). Guessed from
#'   the file extension if NULL.
#' @param graph Optional named graph IRI to read into (default graph if NULL).
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_read <- function(m, path, format = NULL, graph = NULL) {
  m$read(path, format, graph)
}

#' Serialize a Model's triples to a string
#'
#' Pipe-friendly wrapper for `Model$writes()`.
#' @param m A `Model`.
#' @param format One of "ntriples", "turtle", "xml" (rdf/xml). Defaults to "ntriples".
#' @param graph Optional named graph IRI to write (default graph if NULL).
#' @return A string.
#' @seealso [Model]
#' @export
model_writes <- function(m, format = NULL, graph = NULL) {
  m$writes(format, graph)
}

#' Serialize a Model's triples to a file
#'
#' Pipe-friendly wrapper for `Model$write()`.
#' @param m A `Model`.
#' @param path Path to write the RDF file to (overwritten if it exists).
#' @param format One of "ntriples", "turtle", "xml" (rdf/xml). Defaults to "ntriples".
#' @param graph Optional named graph IRI to write (default graph if NULL).
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_write <- function(m, path, format = NULL, graph = NULL) {
  m$write(path, format, graph)
}

#' Build a Model's default indexes
#'
#' Pipe-friendly wrapper for `Model$create_index()`.
#' @param m A `Model`.
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_create_index <- function(m) {
  m$create_index()
}

#' Remove all triples from a graph
#'
#' Pipe-friendly wrapper for `Model$truncate_graph()`.
#' @param m A `Model`.
#' @param graph Optional named graph IRI to truncate (default graph if NULL).
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_truncate_graph <- function(m, graph = NULL) {
  m$truncate_graph(graph)
}

#' Add prefix -> IRI mappings to a Model
#'
#' Pipe-friendly wrapper for `Model$add_prefixes()`.
#' @param m A `Model`.
#' @param prefixes A named R list of single strings: names are prefixes,
#'   values are IRIs, e.g. `list(ex = "http://example.org/")`.
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_add_prefixes <- function(m, prefixes) {
  m$add_prefixes(prefixes)
}

#' Copy a graph from another Model into this one
#'
#' Pipe-friendly wrapper for `Model$add_graph()`.
#' @param m A `Model`.
#' @param other Another Model to copy a graph from.
#' @param source_graph Optional named graph IRI in `other` (default graph if NULL).
#' @param target_graph Optional named graph IRI in `m` to copy into (default graph if NULL).
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_add_graph <- function(m, other, source_graph = NULL, target_graph = NULL) {
  m$add_graph(other, source_graph, target_graph)
}

#' Split a graph out of a Model into a new, standalone Model
#'
#' Pipe-friendly wrapper for `Model$detach_graph()`.
#' @param m A `Model`.
#' @param preserve_name Keep the graph's own name in the new Model rather
#'   than moving it to the default graph there.
#' @param graph Optional named graph IRI to detach (default graph if NULL).
#' @return A new `Model` containing only the detached graph.
#' @seealso [Model]
#' @export
model_detach_graph <- function(m, preserve_name = FALSE, graph = NULL) {
  m$detach_graph(preserve_name, graph)
}

#' Run a SPARQL SELECT or CONSTRUCT query
#'
#' Pipe-friendly wrapper for `Model$query()`.
#' @param m A `Model`.
#' @param sparql The SPARQL query string.
#' @param graph Optional named graph IRI to restrict the query to (searches
#'   across the whole store if NULL).
#' @param include_transient Whether to include transient (e.g. inferred)
#'   triples in the query.
#' @param bindings Optional named list of `IRI`/`Literal` objects, pre-binding
#'   SPARQL variables to fixed values before the query runs.
#' @return A data.frame.
#' @seealso [Model]
#' @export
model_query <- function(m, sparql, graph = NULL, include_transient = FALSE, bindings = NULL) {
  m$query(sparql, graph, include_transient, bindings)
}

#' Run a SPARQL UPDATE against a Model
#'
#' Pipe-friendly wrapper for `Model$update()`.
#' @param m A `Model`.
#' @param update The SPARQL UPDATE string.
#' @param graph Optional named graph IRI to restrict the update to (no
#'   restriction, i.e. the whole store, if NULL).
#' @param include_transient Whether the WHERE clause may match transient
#'   (e.g. inferred) triples.
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_update <- function(m, update, graph = NULL, include_transient = FALSE) {
  m$update(update, graph, include_transient)
}

#' Insert a SPARQL CONSTRUCT query's results as new triples
#'
#' Pipe-friendly wrapper for `Model$insert()`.
#' @param m A `Model`.
#' @param query The SPARQL CONSTRUCT query string to source new triples from.
#' @param source_graph Optional named graph IRI to query from (default
#'   graph if NULL).
#' @param target_graph Optional named graph IRI to insert into (default
#'   graph if NULL).
#' @param include_transient Whether the query may match transient (e.g.
#'   inferred) triples in the source graph.
#' @param transient Whether the newly-inserted triples themselves should be
#'   marked transient rather than permanent.
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_insert <- function(m, query, source_graph = NULL, target_graph = NULL,
                          include_transient = FALSE, transient = FALSE) {
  m$insert(query, source_graph, target_graph, include_transient, transient)
}

#' Register a Template on a Model
#'
#' Pipe-friendly wrapper for `Model$add_template()`.
#' @param m A `Model`.
#' @param template A `Template` object, or an stOTTR document string.
#' @return The IRI of the registered template, invisibly.
#' @seealso [Model]
#' @export
model_add_template <- function(m, template) {
  m$add_template(template)
}

#' Expand a template against a data.frame
#'
#' Pipe-friendly wrapper for `Model$map()`.
#' @param m A `Model`.
#' @param template A `Template` object, the IRI of an already-registered
#'   template, or an stOTTR document string.
#' @param data Optional data.frame, one row per template instantiation.
#' @param graph Optional named graph IRI to add the resulting triples to
#'   (default graph if NULL).
#' @param validate_iris Whether to validate that IRI-typed columns contain
#'   valid IRIs. Defaults to TRUE.
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_map <- function(m, template, data = NULL, graph = NULL, validate_iris = NULL) {
  m$map(template, data, graph, validate_iris)
}

#' Map a JSON file straight to triples via a fixed convention
#'
#' Pipe-friendly wrapper for `Model$map_json()`.
#' @param m A `Model`.
#' @param path Path to the JSON file to map.
#' @param graph Optional named graph IRI to add the resulting triples to
#'   (default graph if NULL).
#' @param transient Whether the resulting triples should be transient
#'   rather than permanent.
#' @param uuid_namespace Optional namespace string for the UUIDv5 blank
#'   node IRIs minted for JSON objects/arrays.
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_map_json <- function(m, path, graph = NULL, transient = FALSE, uuid_namespace = NULL) {
  m$map_json(path, graph, transient, uuid_namespace)
}

#' Map a JSON string straight to triples via a fixed convention
#'
#' Pipe-friendly wrapper for `Model$map_json_string()`.
#' @param m A `Model`.
#' @param json The JSON document, as a string.
#' @param graph Optional named graph IRI to add the resulting triples to
#'   (default graph if NULL).
#' @param transient Whether the resulting triples should be transient
#'   rather than permanent.
#' @param uuid_namespace Optional namespace string for the UUIDv5 blank
#'   node IRIs minted for JSON objects/arrays.
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_map_json_string <- function(m, json, graph = NULL, transient = FALSE, uuid_namespace = NULL) {
  m$map_json_string(json, graph, transient, uuid_namespace)
}

#' Map an XML file straight to triples via a fixed convention
#'
#' Pipe-friendly wrapper for `Model$map_xml()`.
#' @param m A `Model`.
#' @param path Path to the XML file to map.
#' @param graph Optional named graph IRI to add the resulting triples to
#'   (default graph if NULL).
#' @param transient Whether the resulting triples should be transient
#'   rather than permanent.
#' @param uuid_namespace Optional namespace string for the UUIDv5 blank
#'   node IRIs minted for XML elements.
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_map_xml <- function(m, path, graph = NULL, transient = FALSE, uuid_namespace = NULL) {
  m$map_xml(path, graph, transient, uuid_namespace)
}

#' Map an XML string straight to triples via a fixed convention
#'
#' Pipe-friendly wrapper for `Model$map_xml_string()`.
#' @param m A `Model`.
#' @param xml The XML document, as a string.
#' @param graph Optional named graph IRI to add the resulting triples to
#'   (default graph if NULL).
#' @param transient Whether the resulting triples should be transient
#'   rather than permanent.
#' @param uuid_namespace Optional namespace string for the UUIDv5 blank
#'   node IRIs minted for XML elements.
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_map_xml_string <- function(m, xml, graph = NULL, transient = FALSE, uuid_namespace = NULL) {
  m$map_xml_string(xml, graph, transient, uuid_namespace)
}

#' Map a data.frame's columns directly to triples, one predicate per column
#'
#' Pipe-friendly wrapper for `Model$map_df()`.
#' @param m A `Model`.
#' @param data A data.frame, one row per subject; every column becomes a
#'   predicate with that row's value as the object.
#' @param graph Optional named graph IRI to add the resulting triples to
#'   (default graph if NULL).
#' @param uuid_namespace Optional namespace string for the UUIDv5 subject
#'   IRIs minted per row.
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_map_df <- function(m, data, graph = NULL, uuid_namespace = NULL) {
  m$map_df(data, graph, uuid_namespace)
}

#' Map a data.frame's subject/predicate/object columns directly to triples
#'
#' Pipe-friendly wrapper for `Model$map_triples()`.
#' @param m A `Model`.
#' @param data A data.frame with subject/predicate/object columns (or just
#'   subject/object if `predicate` is given).
#' @param predicate Optional constant predicate IRI to use for every row,
#'   instead of a `predicate` column in `data`.
#' @param graph Optional named graph IRI to add the resulting triples to
#'   (default graph if NULL).
#' @param validate_iris Whether to validate that IRI-typed columns contain
#'   valid IRIs. Defaults to TRUE.
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_map_triples <- function(m, data, predicate = NULL, graph = NULL, validate_iris = NULL) {
  m$map_triples(data, predicate, graph, validate_iris)
}

#' Run RDFS inference over a graph in place
#'
#' Pipe-friendly wrapper for `Model$infer_rdfs()`.
#' @param m A `Model`.
#' @param graph Optional named graph IRI to infer over (default graph if NULL).
#' @return The number of new triples inferred.
#' @seealso [Model]
#' @export
model_infer_rdfs <- function(m, graph = NULL) {
  m$infer_rdfs(graph)
}

#' Compact a Model's on-disk storage
#'
#' Pipe-friendly wrapper for `Model$compact()`.
#' @param m A `Model`.
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_compact <- function(m) {
  m$compact()
}

#' Serialize a Model's triples to maplib's own compact on-disk format
#'
#' Pipe-friendly wrapper for `Model$serialize()`.
#' @param m A `Model`.
#' @param path Directory to serialize into. Defaults to "./serialized_triples".
#' @return `m`, invisibly.
#' @seealso [Model]
#' @export
model_serialize <- function(m, path = NULL) {
  m$serialize(path)
}

#' Load a Model previously written by `model_serialize()`/`Model$serialize()`
#'
#' Pipe-friendly wrapper for `Model$deserialize()`. Unlike the other
#' `model_*()` functions, this one has no `m` to take as a first argument --
#' it creates one -- so it's a pipe *source*, not something to pipe an
#' existing Model into: `model_deserialize(path) |> model_query(...)`.
#' @param path Directory previously written by serialize(). Defaults to
#'   "./serialized_triples".
#' @param storage_folder Optional folder for on-disk (rather than
#'   in-memory) triplestore storage.
#' @return A new `Model`.
#' @seealso [Model]
#' @export
model_deserialize <- function(path = NULL, storage_folder = NULL) {
  Model$deserialize(path, storage_folder)
}

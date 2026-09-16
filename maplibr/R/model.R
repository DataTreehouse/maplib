#' A maplib knowledge graph model
#'
#' `Model` is the user-facing wrapper around the underlying `RModel` savvy
#' object. It mirrors py_maplib's `Model` class: methods mutate the
#' underlying graph in place and are chainable (each mutating method
#' returns `self` invisibly).
#'
#' @export
Model <- R6::R6Class(
  "Model",
  # R6's default clone() is a shallow copy: it would copy the R-level
  # reference to `private$rmodel` (a savvy external pointer wrapping a Rust
  # `Mutex<maplib::model::Model>`), not the underlying Rust state itself --
  # so `m2 <- m$clone()` would look like an independent copy while actually
  # sharing the same mutex-guarded triplestore, and mutating one would
  # silently mutate the other with no error or warning. There's no cheap
  # way to deep-copy the underlying Rust Model (that would need a real
  # serialize/deserialize round trip, which itself isn't available for an
  # in-memory Model -- see `$serialize()`). Disabling clone() entirely is
  # safer than a misleading shallow one; `$detach_graph()` is the supported
  # way to get an independent Model.
  cloneable = FALSE,
  public = list(
    #' @description
    #' Create a new, empty Model. Not used to wrap an existing `RModel`
    #' (see `Model$deserialize()` and `$detach_graph()` for that) -- `rmodel`
    #' is for internal use only.
    #' @param rmodel Internal use only.
    initialize = function(rmodel = NULL) {
      if (is.null(rmodel)) {
        rmodel <- .rethrow(RModel$new())
      } else if (!inherits(rmodel, "RModel")) {
        stop("`rmodel` must be an RModel object", call. = FALSE)
      }
      private$rmodel <- rmodel
    },

    #' @description Number of triples in a graph.
    #' @param graph Optional named graph IRI to count (default graph if NULL).
    size = function(graph = NULL) {
      .rethrow(private$rmodel$size(graph))
    },

    #' @description Parse RDF triples from a string into this Model.
    #' @param s RDF data as a string.
    #' @param format One of "ntriples", "turtle", "xml" (rdf/xml).
    #' @param graph Optional named graph IRI to read into (default graph if NULL).
    reads = function(s, format, graph = NULL) {
      .rethrow(private$rmodel$reads(s, format, graph))
      invisible(self)
    },

    #' @description Parse RDF triples from a file into this Model.
    #' @param path Path to the RDF file to read.
    #' @param format One of "ntriples", "turtle", "xml" (rdf/xml). Guessed
    #'   from the file extension if NULL (.ttl -> turtle, .nt -> ntriples,
    #'   .xml/.rdf -> xml) -- any other or missing extension raises a normal
    #'   error, so pass `format` explicitly for those.
    #' @param graph Optional named graph IRI to read into (default graph if NULL).
    read = function(path, format = NULL, graph = NULL) {
      .rethrow(private$rmodel$read(path, format, graph))
      invisible(self)
    },

    #' @description Serialize this Model's triples to a string.
    #' @param format One of "ntriples", "turtle", "xml" (rdf/xml). Defaults to "ntriples".
    #' @param graph Optional named graph IRI to write (default graph if NULL).
    writes = function(format = NULL, graph = NULL) {
      .rethrow(private$rmodel$writes(format, graph))
    },

    #' @description Serialize this Model's triples to a file.
    #' @param path Path to write the RDF file to (overwritten if it exists).
    #' @param format One of "ntriples", "turtle", "xml" (rdf/xml). Defaults to "ntriples".
    #' @param graph Optional named graph IRI to write (default graph if NULL).
    write = function(path, format = NULL, graph = NULL) {
      .rethrow(private$rmodel$write(path, format, graph))
      invisible(self)
    },

    #' @description Build the default (non-FTS) indexes.
    create_index = function() {
      .rethrow(private$rmodel$create_index())
      invisible(self)
    },

    #' @description Remove all triples from a graph.
    #' @param graph Optional named graph IRI to truncate (default graph if NULL).
    truncate_graph = function(graph = NULL) {
      .rethrow(private$rmodel$truncate_graph(graph))
      invisible(self)
    },

    #' @description Add prefix -> IRI mappings, used when serializing.
    #' @param prefixes A named R list of single strings: names are prefixes,
    #'   values are IRIs, e.g. `list(ex = "http://example.org/")`.
    add_prefixes = function(prefixes) {
      .rethrow(private$rmodel$add_prefixes(prefixes))
      invisible(self)
    },

    #' @description Copy a graph from another Model's triplestore into this one.
    #' @param other Another Model to copy a graph from.
    #' @param source_graph Optional named graph IRI in `other` (default graph if NULL).
    #' @param target_graph Optional named graph IRI in this Model to copy into (default graph if NULL).
    add_graph = function(other, source_graph = NULL, target_graph = NULL) {
      if (!inherits(other, "Model")) {
        stop("`other` must be a Model object", call. = FALSE)
      }
      other_rmodel <- private$as_rmodel(other)
      # The underlying Rust call locks `other` then `self` in fixed order,
      # so if both wrap the same Mutex (`other` is `self`, or an aliased
      # Model sharing its underlying pointer -- clone() is disabled, but
      # two Model$new() calls can still be handed the same RModel), it
      # locks the same non-reentrant Mutex twice on one thread and
      # deadlocks the R session permanently. Compared via the raw external
      # pointer (`.ptr`), not `identical(self, other)`, so this also
      # catches two distinct Model wrapper objects that happen to share
      # one underlying RModel.
      if (identical(private$rmodel$.ptr, other_rmodel$.ptr)) {
        stop(
          "`other` must not be the same underlying Model as `self` -- add_graph(m, m) would deadlock (self and other share one lock)",
          call. = FALSE
        )
      }
      .rethrow(private$rmodel$add_graph(other_rmodel, source_graph, target_graph))
      invisible(self)
    },

    #' @description Split a graph out of this Model into a brand new, standalone Model.
    #' @param preserve_name Keep the graph's own name in the new Model rather
    #'   than moving it to the default graph there.
    #' @param graph Optional named graph IRI to detach (default graph if NULL).
    #' @return A new Model containing only the detached graph.
    detach_graph = function(preserve_name = FALSE, graph = NULL) {
      Model$new(.rethrow(private$rmodel$detach_graph(preserve_name, graph)))
    },

    #' @description Run a SPARQL SELECT or CONSTRUCT query and return the
    #' result as a data.frame. Mirrors py_maplib's Model.query()
    #' (py_maplib/src/py_model.rs:344-387). For CONSTRUCT, every matched
    #' triple pattern's results are concatenated into one combined
    #' subject/predicate/object data.frame (py_maplib instead returns one
    #' data.frame per pattern, as a Python list -- not done here since a
    #' single query() call always returns exactly one data.frame). Subject
    #' and object values are always returned as plain strings for CONSTRUCT
    #' results (unlike SELECT, which preserves each column's native type
    #' where a single type applies) -- different patterns can produce
    #' different column types, which a single concatenated data.frame can't
    #' represent without collapsing to a common type regardless.
    #' @param sparql The SPARQL query string.
    #' @param graph Optional named graph IRI to restrict the query to (searches
    #'   across the whole store if NULL).
    #' @param include_transient Whether to include transient (e.g. inferred)
    #'   triples in the query.
    #' @param bindings Optional named list of `IRI`/`Literal` objects
    #'   (see `terms.R`) -- pre-binds each named SPARQL variable to a fixed
    #'   value before the query runs, e.g. `list(b = IRI("http://ex/a"))`
    #'   binds `?b`. A bound variable can only be used inside the query body
    #'   (e.g. a `FILTER`), not listed directly in `SELECT` -- binding a
    #'   variable that's also a `SELECT`-ed output column is a normal error
    #'   (`SELECT *` still works, since it doesn't name variables itself).
    #' @return A data.frame. A column whose values are always the same RDF
    #'   node type (the common case) comes back as a plain vector of that
    #'   type's underlying value (an IRI/blank node id/literal value as a
    #'   plain string, exactly as py_maplib returns by default). A column
    #'   whose RDF node type can vary by row instead comes back from Arrow as
    #'   one sub-column per possible type (only one of which is non-NA on any
    #'   given row) -- collapsed here into a single vector by taking
    #'   whichever sub-column is non-NA per row. The precise per-column RDF
    #'   node type information (which py_maplib exposes via
    #'   SolutionMappings.rdf_types) is attached as the data.frame's
    #'   `"rdf_node_types"` attribute instead of a richer typed object, parsed
    #'   from the underlying JSON side-channel (maplibr/src/rust/src/
    #'   arrow_bridge.rs) -- a full RDFType-based port of that accessor is
    #'   left for later.
    query = function(sparql, graph = NULL, include_transient = FALSE, bindings = NULL) {
      stream <- nanoarrow::nanoarrow_allocate_array_stream()
      raw_bindings <- if (!is.null(bindings)) {
        lapply(bindings, function(x) .savvy_extract_ptr(.to_ground_term(x), "maplibr::RGroundTerm"))
      } else {
        NULL
      }
      rdf_node_types_json <- .rethrow(
        private$rmodel$query(sparql, stream, include_transient, graph, raw_bindings)
      )
      df <- as.data.frame(stream)
      df <- .collapse_multitype_columns(df)
      attr(df, "rdf_node_types") <- jsonlite::fromJSON(rdf_node_types_json, simplifyVector = FALSE)
      df
    },

    #' @description Run a SPARQL UPDATE (modify-style INSERT/DELETE ... WHERE)
    #' against this Model in place.
    #' @param update The SPARQL UPDATE string. Must be a DELETE/INSERT/WHERE
    #'   form (`INSERT { ... } WHERE { ... }`, optionally with a DELETE
    #'   clause too, and an empty `WHERE {}` when there's nothing to match
    #'   against) -- the bare `INSERT DATA { ... }`/`DELETE DATA { ... }`
    #'   forms are unimplemented in the core engine and panic if used (not
    #'   fixable in this package -- see maplib-cai upstream issue).
    #' @param graph Optional named graph IRI to restrict the update to
    #'   (no restriction, i.e. the whole store, if NULL -- same "missing
    #'   means unrestricted" semantics as `$query()`'s `graph` argument, not
    #'   `$reads()`/`$writes()`'s "missing means the default graph").
    #' @param include_transient Whether the WHERE clause may match transient
    #'   (e.g. inferred) triples.
    update = function(update, graph = NULL, include_transient = FALSE) {
      .rethrow(private$rmodel$update(update, include_transient, graph))
      invisible(self)
    },

    #' @description Run a SPARQL CONSTRUCT query and insert its results as
    #' new triples into a (possibly different) graph.
    #' @param query The SPARQL CONSTRUCT query string to source new triples
    #'   from (a SELECT query errors -- INSERT needs a triple-shaped result).
    #' @param source_graph Optional named graph IRI to query from (default
    #'   graph if NULL).
    #' @param target_graph Optional named graph IRI to insert into (default
    #'   graph if NULL).
    #' @param include_transient Whether the query may match transient (e.g.
    #'   inferred) triples in the source graph.
    #' @param transient Whether the newly-inserted triples themselves should
    #'   be marked transient rather than permanent.
    insert = function(query, source_graph = NULL, target_graph = NULL,
                       include_transient = FALSE, transient = FALSE) {
      .rethrow(private$rmodel$insert(query, include_transient, transient, source_graph, target_graph))
      invisible(self)
    },

    #' @description Register a Template so it can be referenced by IRI from
    #' `$map()`.
    #' @param template A `Template` (see `templates.R`), or an stOTTR document
    #'   string defining one or more templates.
    #' @return The IRI of the registered template, invisibly (for a `Template`
    #'   object, this is just `template@iri`; for a document string, it's the
    #'   IRI of the first template the document defines).
    add_template = function(template) {
      invisible(private$.resolve_template_iri(template))
    },

    #' @description Expand a template against a data.frame, adding the
    #' resulting triples to this Model. Mirrors py_maplib's `Model.map()`
    #' (py_maplib/src/py_model.rs:118-142).
    #' @param template A `Template` object, the IRI of an already-registered
    #'   template (`$add_template()`), or an stOTTR document string (defining
    #'   exactly the one template to expand -- registered as a side effect).
    #' @param data Optional data.frame, one row per template instantiation. If
    #'   NULL (or zero rows), the template is expanded once against no
    #'   variables -- only valid for templates whose instances are entirely
    #'   constant terms.
    #' @param graph Optional named graph IRI to add the resulting triples to
    #'   (default graph if NULL).
    #' @param validate_iris Whether to validate that IRI-typed columns contain
    #'   valid IRIs. Defaults to TRUE.
    map = function(template, data = NULL, graph = NULL, validate_iris = NULL) {
      template_iri <- private$.resolve_template_iri(template)
      if (is.null(data)) {
        .rethrow(private$rmodel$map_no_data(template_iri, graph, validate_iris))
      } else {
        # A zero-row data.frame is a silent no-op (RModel$map(), mirroring
        # py_maplib's map_mutex) -- distinct from `data = NULL`, which
        # expands the template once against no variables at all.
        stream <- nanoarrow::as_nanoarrow_array_stream(data)
        .rethrow(private$rmodel$map(template_iri, stream, graph, validate_iris))
      }
      invisible(self)
    },

    #' @description Map a JSON file straight to triples via a fixed
    #' convention (Facade-X; no template involved).
    #' @param path Path to the JSON file to map.
    #' @param graph Optional named graph IRI to add the resulting triples to
    #'   (default graph if NULL).
    #' @param transient Whether the resulting triples should be transient
    #'   rather than permanent.
    #' @param uuid_namespace Optional namespace string for the UUIDv5 blank
    #'   node IRIs minted for JSON objects/arrays. Defaults to the path.
    map_json = function(path, graph = NULL, transient = FALSE, uuid_namespace = NULL) {
      .rethrow(private$rmodel$map_json(path, transient, graph, uuid_namespace))
      invisible(self)
    },

    #' @description Map a JSON string straight to triples via a fixed
    #' convention. See `$map_json()` for the file-based equivalent.
    #' @param json The JSON document, as a string.
    #' @param graph Optional named graph IRI to add the resulting triples to
    #'   (default graph if NULL).
    #' @param transient Whether the resulting triples should be transient
    #'   rather than permanent.
    #' @param uuid_namespace Optional namespace string for the UUIDv5 blank
    #'   node IRIs minted for JSON objects/arrays. Defaults to a random UUID
    #'   if not given.
    map_json_string = function(json, graph = NULL, transient = FALSE, uuid_namespace = NULL) {
      .rethrow(private$rmodel$map_json_string(json, transient, graph, uuid_namespace))
      invisible(self)
    },

    #' @description Map an XML file straight to triples via a fixed
    #' convention.
    #' @param path Path to the XML file to map.
    #' @param graph Optional named graph IRI to add the resulting triples to
    #'   (default graph if NULL).
    #' @param transient Whether the resulting triples should be transient
    #'   rather than permanent.
    #' @param uuid_namespace Optional namespace string for the UUIDv5 blank
    #'   node IRIs minted for XML elements. Defaults to the path.
    map_xml = function(path, graph = NULL, transient = FALSE, uuid_namespace = NULL) {
      .rethrow(private$rmodel$map_xml(path, transient, graph, uuid_namespace))
      invisible(self)
    },

    #' @description Map an XML string straight to triples via a fixed
    #' convention. See `$map_xml()` for the file-based equivalent.
    #' @param xml The XML document, as a string.
    #' @param graph Optional named graph IRI to add the resulting triples to
    #'   (default graph if NULL).
    #' @param transient Whether the resulting triples should be transient
    #'   rather than permanent.
    #' @param uuid_namespace Optional namespace string for the UUIDv5 blank
    #'   node IRIs minted for XML elements. Defaults to a random UUID if not
    #'   given.
    map_xml_string = function(xml, graph = NULL, transient = FALSE, uuid_namespace = NULL) {
      .rethrow(private$rmodel$map_xml_string(xml, transient, graph, uuid_namespace))
      invisible(self)
    },

    #' @description Map a data.frame's columns directly to
    #' subject/predicate/object triples -- one column per predicate, a fresh
    #' IRI subject per row, no OTTR template involved.
    #' @param data A data.frame, one row per subject; every column becomes a
    #'   predicate (named by the column name) with that row's value as the
    #'   object. A zero-row data.frame is NOT a no-op here (unlike `$map()`):
    #'   a single root `rdf:type` triple is still added, matching the
    #'   underlying engine call.
    #' @param graph Optional named graph IRI to add the resulting triples to
    #'   (default graph if NULL).
    #' @param uuid_namespace Optional namespace string for the UUIDv5 subject
    #'   IRIs minted per row. Defaults to a random UUID if not given.
    map_df = function(data, graph = NULL, uuid_namespace = NULL) {
      stream <- nanoarrow::as_nanoarrow_array_stream(data)
      .rethrow(private$rmodel$map_df(stream, graph, uuid_namespace))
      invisible(self)
    },

    #' @description Map a data.frame's columns directly to
    #' subject/predicate/object triples using the built-in `ottr:Triple`
    #' template, optionally fixing every row's predicate to a single
    #' constant IRI instead of reading it from a `predicate` column.
    #' @param data A data.frame with subject/predicate/object columns (or
    #'   just subject/object if `predicate` is given). A zero-row data.frame
    #'   is NOT a no-op: missing/misnamed columns still raise a normal error
    #'   regardless of row count.
    #' @param predicate Optional constant predicate IRI to use for every
    #'   row, instead of a `predicate` column in `data`.
    #' @param graph Optional named graph IRI to add the resulting triples to
    #'   (default graph if NULL).
    #' @param validate_iris Whether to validate that IRI-typed columns
    #'   contain valid IRIs. Defaults to TRUE.
    map_triples = function(data, predicate = NULL, graph = NULL, validate_iris = NULL) {
      stream <- nanoarrow::as_nanoarrow_array_stream(data)
      .rethrow(private$rmodel$map_triples(stream, predicate, graph, validate_iris))
      invisible(self)
    },

    #' @description Run RDFS inference over a graph in place.
    #' @param graph Optional named graph IRI to infer over (default graph if NULL).
    #' @return The number of new triples inferred (a triple count, not a rule
    #'   count -- Triplestore::interesting_rdfs_rules sums per-rule
    #'   inserted-triple counts).
    infer_rdfs = function(graph = NULL) {
      .rethrow(private$rmodel$infer_rdfs(graph))
    },

    #' @description Compact on-disk storage.
    compact = function() {
      .rethrow(private$rmodel$compact())
      invisible(self)
    },

    #' @description Serialize this Model's triples to a directory in
    #' maplib's own compact on-disk format (not an RDF interchange format --
    #' use `$writes()` for that).
    #' @param path Directory to serialize into. Defaults to
    #'   "./serialized_triples".
    serialize = function(path = NULL) {
      .rethrow(private$rmodel$serialize(path))
      invisible(self)
    },

    #' @description Print a short summary of this Model.
    #' @param ... Unused.
    print = function(...) {
      cat("<Model>", private$rmodel$size(NULL), "triples (default graph)\n")
      invisible(self)
    }
  ),
  private = list(
    rmodel = NULL,
    as_rmodel = function(model) {
      model$.__enclos_env__$private$rmodel
    },
    # Resolves `template` (a Template object, a template IRI, or an stOTTR
    # document string) to a template IRI, registering it first if needed.
    # A Template object or document string is always (re-)registered on
    # every call -- mirrors py_maplib's own map_mutex, which re-registers a
    # PyTemplate/doc-string `template` argument on every map() call too
    # (py_maplib/src/mutexes.rs:120-153), rather than requiring a separate
    # add_template() call first.
    .resolve_template_iri = function(template) {
      if (S7::S7_inherits(template, Template)) {
        .rethrow(private$rmodel$add_template(template@raw))
        template@iri@iri
      } else if (is.character(template) && length(template) == 1) {
        # Discriminates a doc string from a bare IRI by the presence of a
        # "{" instance-block brace: every stOTTR document has one (its
        # `[params] :: { ... }` body), and "{" is never a legal IRI
        # character (RFC 3987), so a real template IRI can never contain
        # one -- unlike an earlier "::" substring check (roborev job 22),
        # which would misclassify a bare IRI containing "::" (e.g. some URN
        # forms) as a doc string.
        if (grepl("{", template, fixed = TRUE)) {
          .rethrow(private$rmodel$add_template_string(template))
        } else {
          template
        }
      } else {
        stop(
          "`template` must be a Template, a template IRI, or an stOTTR document string",
          call. = FALSE
        )
      }
    }
  )
)

# A column whose RDF node type varies by row comes back from
# as.data.frame(stream) as a nested data.frame -- one sub-column per
# possible type (e.g. "I" for IRI, "B" for blank node, the datatype IRI for
# a literal; see MULTI_IRI_DT/MULTI_BLANK_DT, lib/representation/src/
# multitype.rs:11-13), with exactly one non-NA per row. Collapses each such
# column into a single vector by coalescing across its sub-columns via
# ifelse(), which is safe today only because every sub-column is plain
# character (IRI/blank-node id/literal all surface as strings via
# format_native_columns) -- ifelse() silently coerces on a type mismatch, so
# if a future multi-typed column ever carried a non-character sub-column
# (e.g. a natively-typed numeric literal), values could be silently
# mangled. Revisit with an explicit type check (or vctrs::vec_coalesce) if
# native-typed literal columns are ever added.
.collapse_multitype_columns <- function(df) {
  for (col in names(df)) {
    if (is.data.frame(df[[col]])) {
      sub <- df[[col]]
      collapsed <- sub[[1]]
      if (ncol(sub) > 1) {
        for (j in 2:ncol(sub)) {
          collapsed <- ifelse(is.na(collapsed), sub[[j]], collapsed)
        }
      }
      df[[col]] <- collapsed
    }
  }
  df
}

# Load a Model previously written by `$serialize()`. Attached directly to
# the (already @export'd) Model generator, so users call it as
# `Model$deserialize(path)` -- mirrors RModel's own associated-function
# pattern in 000-wrappers.R.
Model$deserialize <- function(path = NULL, storage_folder = NULL) {
  Model$new(.rethrow(RModel$deserialize(path, storage_folder)))
}

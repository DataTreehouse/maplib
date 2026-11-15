pub const LANG_LANG_FIELD_ALT_NAME: &str = "language";
pub const LANG_VALUE_FIELD_ALT_NAME: &str = "value";

pub const XYZ_PREFIX: &str = "xyz";
pub const XYZ_PREFIX_IRI: &str = "http://sparql.xyz/facade-x/data/";
pub const FX_PREFIX: &str = "fx";
pub const FX_PREFIX_IRI: &str = "http://sparql.xyz/facade-x/ns/";
pub const FX_XYZ_PREFIX_IRI: &str = "http://sparql.xyz/facade-x/ns/prefixIRI";
pub const FX_PREFIX_SEP: &str = "http://sparql.xyz/facade-x/ns/prefixSeparator";
pub const FX_ROOT: &str = "http://sparql.xyz/facade-x/ns/root";
pub const FX_CHILD: &str = "http://sparql.xyz/facade-x/ns/child";
pub const FX_CHILD_NUMBER: &str = "http://sparql.xyz/facade-x/ns/childNumber";
pub const FX_XMLNS: &str = "http://sparql.xyz/facade-x/ns/xmlns";
pub const FX_XMLNS_SEP: &str = "http://sparql.xyz/facade-x/ns/xmlnsSeparator";
pub const FX_PREFIX_NAME: &str = "http://sparql.xyz/facade-x/ns/prefixName";
pub const FX_NULL: &str = "http://sparql.xyz/facade-x/ns/null";
pub const OTTR_TRIPLE: &str = "http://ns.ottr.xyz/0.4/Triple";
pub const OTTR_PREFIX: &str = "ottr";
pub const OTTR_PREFIX_IRI: &str = "http://ns.ottr.xyz/0.4/";
pub const RDF_PREFIX: &str = "rdf";
pub const RDF_PREFIX_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
pub const RDFS_PREFIX: &str = "rdfs";
pub const RDFS_PREFIX_IRI: &str = "http://www.w3.org/2000/01/rdf-schema#";
pub const XSD_PREFIX: &str = "xsd";
pub const XSD_PREFIX_IRI: &str = "http://www.w3.org/2001/XMLSchema#";

pub const DEFAULT_PREFIX: &str = "def";
pub const DEFAULT_PREFIX_IRI: &str = "urn:maplib_default:";
pub const OTTR_IRI: &str = "http://ns.ottr.xyz/0.4/IRI";
pub const OTTR_BLANK_NODE: &str = "http://ns.ottr.xyz/0.4/BlankNode";
pub const OWL_PREFIX: &str = "owl";
pub const OWL_PREFIX_IRI: &str = "http://www.w3.org/2002/07/owl#";

pub const SHACL_PREFIX: &str = "sh";

pub const SHACL_PREFIX_IRI: &str = "http://www.w3.org/ns/shacl#";

pub const XSD_ANY_URI: &str = "http://www.w3.org/2001/XMLSchema#anyURI";

pub const FOAF_PREFIX: &str = "foaf";
pub const FOAF_PREFIX_IRI: &str = "http://xmlns.com/foaf/0.1/";

pub const MAPLIB_PREFIX: &str = "maplib";
pub const MAPLIB_PREFIX_IRI: &str = "https://datatreehouse.github.io/maplib/vocab#";

/// The former namespace of the time series virtualization vocabulary (`hasTimeseries`,
/// `hasDataPoint`, `hasResource`, `FloorDateTimeToSecondsInterval`, ...), which is now in
/// [MAPLIB_PREFIX_IRI].
///
/// Deprecated: IRIs in it are still accepted in SPARQL, in the predicates of data and in the
/// templates of virtualizations, and are read as the IRI with the same local name in the
/// maplib vocabulary, see [from_legacy_chrontext_iri].
pub const LEGACY_CHRONTEXT_PREFIX_IRI: &str = "https://github.com/DataTreehouse/chrontext#";

/// The IRI in the maplib vocabulary for an IRI in the former chrontext namespace
/// ([LEGACY_CHRONTEXT_PREFIX_IRI]), or `None` for any other IRI.
pub fn from_legacy_chrontext_iri(iri: &str) -> Option<String> {
    iri.strip_prefix(LEGACY_CHRONTEXT_PREFIX_IRI)
        .map(|local| format!("{MAPLIB_PREFIX_IRI}{local}"))
}

/// [from_legacy_chrontext_iri] for a named node, which is returned unchanged when it is not in
/// the former chrontext namespace.
pub fn normalize_legacy_chrontext_named_node(nn: oxrdf::NamedNode) -> oxrdf::NamedNode {
    match from_legacy_chrontext_iri(nn.as_str()) {
        Some(iri) => oxrdf::NamedNode::new_unchecked(iri),
        None => nn,
    }
}

pub const SKOS_PREFIX: &str = "skos";
pub const SKOS_PREFIX_IRI: &str = "http://www.w3.org/2004/02/skos/core#";

pub const DEFAULT_GRAPH_IRI: &str = "https://datatreehouse.github.io/maplib/vocab#DefaultGraph";
/// The reserved named graph for triples that are used for querying, validation and reasoning,
/// but that should not end up in the output. It is included in queries with `include_graphs`.
pub const TRANSIENT_GRAPH_IRI: &str = "https://datatreehouse.github.io/maplib/vocab#TransientGraph";

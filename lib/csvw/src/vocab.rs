pub const CSVW_NS: &str = "http://www.w3.org/ns/csvw#";
pub const CSVW_CONTEXT: &str = "http://www.w3.org/ns/csvw";
pub const RDF_NS: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
pub const XSD_NS: &str = "http://www.w3.org/2001/XMLSchema#";

/// Namespace for the helper predicates maplib adds to the Facade-X representation of a table.
pub const MAPLIB_CSVW_NS: &str = "urn:maplib:csvw:";

/// The prefixes of the CSVW namespace context (the RDFa initial context and csvw).
pub const PREFIXES: &[(&str, &str)] = &[
    ("as", "https://www.w3.org/ns/activitystreams#"),
    ("cc", "http://creativecommons.org/ns#"),
    ("csvw", "http://www.w3.org/ns/csvw#"),
    ("ctag", "http://commontag.org/ns#"),
    ("dc", "http://purl.org/dc/terms/"),
    ("dc11", "http://purl.org/dc/elements/1.1/"),
    ("dcat", "http://www.w3.org/ns/dcat#"),
    ("dcterms", "http://purl.org/dc/terms/"),
    ("dqv", "http://www.w3.org/ns/dqv#"),
    ("duv", "https://www.w3.org/ns/duv#"),
    ("foaf", "http://xmlns.com/foaf/0.1/"),
    ("gr", "http://purl.org/goodrelations/v1#"),
    ("grddl", "http://www.w3.org/2003/g/data-view#"),
    ("ical", "http://www.w3.org/2002/12/cal/icaltzd#"),
    ("jsonld", "http://www.w3.org/ns/json-ld#"),
    ("ldp", "http://www.w3.org/ns/ldp#"),
    ("ma", "http://www.w3.org/ns/ma-ont#"),
    ("oa", "http://www.w3.org/ns/oa#"),
    ("odrl", "http://www.w3.org/ns/odrl/2/"),
    ("og", "http://ogp.me/ns#"),
    ("org", "http://www.w3.org/ns/org#"),
    ("owl", "http://www.w3.org/2002/07/owl#"),
    ("prov", "http://www.w3.org/ns/prov#"),
    ("qb", "http://purl.org/linked-data/cube#"),
    ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    ("rdfa", "http://www.w3.org/ns/rdfa#"),
    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
    ("rev", "http://purl.org/stuff/rev#"),
    ("rif", "http://www.w3.org/2007/rif#"),
    ("rr", "http://www.w3.org/ns/r2rml#"),
    ("schema", "http://schema.org/"),
    ("sd", "http://www.w3.org/ns/sparql-service-description#"),
    ("sioc", "http://rdfs.org/sioc/ns#"),
    ("skos", "http://www.w3.org/2004/02/skos/core#"),
    ("skosxl", "http://www.w3.org/2008/05/skos-xl#"),
    ("sosa", "http://www.w3.org/ns/sosa/"),
    ("ssn", "http://www.w3.org/ns/ssn/"),
    ("time", "http://www.w3.org/2006/time#"),
    ("v", "http://rdf.data-vocabulary.org/#"),
    ("vcard", "http://www.w3.org/2006/vcard/ns#"),
    ("void", "http://rdfs.org/ns/void#"),
    ("wdr", "http://www.w3.org/2007/05/powder#"),
    ("wrds", "http://www.w3.org/2007/05/powder-s#"),
    ("xhv", "http://www.w3.org/1999/xhtml/vocab#"),
    ("xml", "http://www.w3.org/XML/1998/namespace"),
    ("xsd", "http://www.w3.org/2001/XMLSchema#"),
];

pub fn prefix_iri(prefix: &str) -> Option<&'static str> {
    PREFIXES
        .iter()
        .find(|(p, _)| *p == prefix)
        .map(|(_, iri)| *iri)
}

/// Expands a prefixed name using the CSVW context prefixes, returns None if the string is not
/// a prefixed name with a known prefix.
pub fn expand_prefixed(s: &str) -> Option<String> {
    let (p, local) = s.split_once(':')?;
    if local.starts_with("//") {
        return None;
    }
    prefix_iri(p).map(|ns| format!("{ns}{local}"))
}

/// Terms of the CSVW context that may be used as @type values or properties of common
/// properties. They expand into the csvw namespace.
pub const CSVW_TERMS: &[&str] = &[
    "Cell",
    "Column",
    "Datatype",
    "Dialect",
    "Direction",
    "ForeignKey",
    "JSON",
    "NCName",
    "NumericFormat",
    "Row",
    "Schema",
    "Table",
    "TableGroup",
    "TableReference",
    "Transformation",
    "aboutUrl",
    "base",
    "columnReference",
    "columns",
    "commentPrefix",
    "datatype",
    "decimalChar",
    "default",
    "delimiter",
    "describes",
    "dialect",
    "doubleQuote",
    "encoding",
    "foreignKeys",
    "format",
    "groupChar",
    "header",
    "headerRowCount",
    "lang",
    "length",
    "lineTerminators",
    "maxExclusive",
    "maxInclusive",
    "maxLength",
    "minExclusive",
    "minInclusive",
    "minLength",
    "name",
    "note",
    "null",
    "ordered",
    "pattern",
    "primaryKey",
    "propertyUrl",
    "quoteChar",
    "reference",
    "referencedRows",
    "required",
    "resource",
    "row",
    "rowTitle",
    "rownum",
    "schemaReference",
    "scriptFormat",
    "separator",
    "skipBlankRows",
    "skipColumns",
    "skipInitialSpace",
    "skipRows",
    "source",
    "suppressOutput",
    "table",
    "tableDirection",
    "tableSchema",
    "targetFormat",
    "textDirection",
    "title",
    "transformations",
    "trim",
    "uriTemplate",
    "url",
    "valueUrl",
    "virtual",
];

/// Built-in datatypes, with their RDF datatype IRIs.
pub const BUILTIN_DATATYPES: &[(&str, &str)] = &[
    (
        "anyAtomicType",
        "http://www.w3.org/2001/XMLSchema#anyAtomicType",
    ),
    ("anyURI", "http://www.w3.org/2001/XMLSchema#anyURI"),
    (
        "base64Binary",
        "http://www.w3.org/2001/XMLSchema#base64Binary",
    ),
    ("boolean", "http://www.w3.org/2001/XMLSchema#boolean"),
    ("date", "http://www.w3.org/2001/XMLSchema#date"),
    ("dateTime", "http://www.w3.org/2001/XMLSchema#dateTime"),
    (
        "dateTimeStamp",
        "http://www.w3.org/2001/XMLSchema#dateTimeStamp",
    ),
    ("decimal", "http://www.w3.org/2001/XMLSchema#decimal"),
    ("integer", "http://www.w3.org/2001/XMLSchema#integer"),
    ("long", "http://www.w3.org/2001/XMLSchema#long"),
    ("int", "http://www.w3.org/2001/XMLSchema#int"),
    ("short", "http://www.w3.org/2001/XMLSchema#short"),
    ("byte", "http://www.w3.org/2001/XMLSchema#byte"),
    (
        "nonNegativeInteger",
        "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
    ),
    (
        "positiveInteger",
        "http://www.w3.org/2001/XMLSchema#positiveInteger",
    ),
    (
        "unsignedLong",
        "http://www.w3.org/2001/XMLSchema#unsignedLong",
    ),
    (
        "unsignedInt",
        "http://www.w3.org/2001/XMLSchema#unsignedInt",
    ),
    (
        "unsignedShort",
        "http://www.w3.org/2001/XMLSchema#unsignedShort",
    ),
    (
        "unsignedByte",
        "http://www.w3.org/2001/XMLSchema#unsignedByte",
    ),
    (
        "nonPositiveInteger",
        "http://www.w3.org/2001/XMLSchema#nonPositiveInteger",
    ),
    (
        "negativeInteger",
        "http://www.w3.org/2001/XMLSchema#negativeInteger",
    ),
    ("double", "http://www.w3.org/2001/XMLSchema#double"),
    ("duration", "http://www.w3.org/2001/XMLSchema#duration"),
    (
        "dayTimeDuration",
        "http://www.w3.org/2001/XMLSchema#dayTimeDuration",
    ),
    (
        "yearMonthDuration",
        "http://www.w3.org/2001/XMLSchema#yearMonthDuration",
    ),
    ("float", "http://www.w3.org/2001/XMLSchema#float"),
    ("gDay", "http://www.w3.org/2001/XMLSchema#gDay"),
    ("gMonth", "http://www.w3.org/2001/XMLSchema#gMonth"),
    ("gMonthDay", "http://www.w3.org/2001/XMLSchema#gMonthDay"),
    ("gYear", "http://www.w3.org/2001/XMLSchema#gYear"),
    ("gYearMonth", "http://www.w3.org/2001/XMLSchema#gYearMonth"),
    ("hexBinary", "http://www.w3.org/2001/XMLSchema#hexBinary"),
    ("QName", "http://www.w3.org/2001/XMLSchema#QName"),
    ("string", "http://www.w3.org/2001/XMLSchema#string"),
    (
        "normalizedString",
        "http://www.w3.org/2001/XMLSchema#normalizedString",
    ),
    ("token", "http://www.w3.org/2001/XMLSchema#token"),
    ("language", "http://www.w3.org/2001/XMLSchema#language"),
    ("Name", "http://www.w3.org/2001/XMLSchema#Name"),
    ("NMTOKEN", "http://www.w3.org/2001/XMLSchema#NMTOKEN"),
    (
        "xml",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral",
    ),
    ("html", "http://www.w3.org/1999/02/22-rdf-syntax-ns#HTML"),
    ("json", "http://www.w3.org/ns/csvw#JSON"),
    ("time", "http://www.w3.org/2001/XMLSchema#time"),
];

/// Alternative names of built-in datatypes.
pub const DATATYPE_ALIASES: &[(&str, &str)] = &[
    ("number", "double"),
    ("binary", "base64Binary"),
    ("datetime", "dateTime"),
    ("any", "anyAtomicType"),
];

/// The canonical name of a built-in datatype, given a name, an alias or its IRI.
pub fn builtin_datatype_name(s: &str) -> Option<&'static str> {
    if let Some((_, n)) = DATATYPE_ALIASES.iter().find(|(a, _)| *a == s) {
        return Some(n);
    }
    BUILTIN_DATATYPES
        .iter()
        .find(|(n, iri)| *n == s || *iri == s)
        .map(|(n, _)| *n)
}

pub fn builtin_datatype_iri(name: &str) -> &'static str {
    BUILTIN_DATATYPES
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, iri)| *iri)
        .unwrap_or("http://www.w3.org/2001/XMLSchema#string")
}

pub fn is_builtin_datatype_iri(s: &str) -> bool {
    BUILTIN_DATATYPES.iter().any(|(_, iri)| *iri == s)
        || (s.starts_with(XSD_NS) && builtin_datatype_name(&s[XSD_NS.len()..]).is_some())
}

pub const INTEGER_TYPES: &[&str] = &[
    "integer",
    "long",
    "int",
    "short",
    "byte",
    "nonNegativeInteger",
    "positiveInteger",
    "unsignedLong",
    "unsignedInt",
    "unsignedShort",
    "unsignedByte",
    "nonPositiveInteger",
    "negativeInteger",
];

pub fn is_integer_type(b: &str) -> bool {
    INTEGER_TYPES.contains(&b)
}

pub fn is_decimal_type(b: &str) -> bool {
    b == "decimal" || is_integer_type(b)
}

pub fn is_numeric_type(b: &str) -> bool {
    is_decimal_type(b) || b == "double" || b == "float"
}

pub const DATE_TIME_TYPES: &[&str] = &[
    "date",
    "dateTime",
    "dateTimeStamp",
    "time",
    "gDay",
    "gMonth",
    "gMonthDay",
    "gYear",
    "gYearMonth",
];

pub fn is_date_time_type(b: &str) -> bool {
    DATE_TIME_TYPES.contains(&b)
}

pub fn is_duration_type(b: &str) -> bool {
    matches!(b, "duration" | "dayTimeDuration" | "yearMonthDuration")
}

pub fn is_string_type(b: &str) -> bool {
    matches!(
        b,
        "string"
            | "normalizedString"
            | "token"
            | "language"
            | "Name"
            | "NMTOKEN"
            | "xml"
            | "html"
            | "json"
            | "anyURI"
            | "QName"
    )
}

pub fn is_binary_type(b: &str) -> bool {
    matches!(b, "base64Binary" | "hexBinary")
}

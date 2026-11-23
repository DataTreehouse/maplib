//! The logical sources of RML mappings: CSV, JSON and XML.
//!
//! Each logical source becomes a DataFrame with one column per reference that the mapping
//! evaluates on it, named by the reference, which the R2RML engine then maps as it maps the
//! result of a logical table.
//!
//! A JSON or XML source is first mapped with the Façade-X mapping of `map_json` and `map_xml`
//! into a temporary triplestore of its own. The iterator and the references, which are JSONPath
//! or XPath expressions, are translated to one SPARQL query over that graph: the iterator binds
//! the records, and each reference is an OPTIONAL pattern from the record to a value. A reference
//! with several values for a record gives a row per value.
//!
//! XML namespaces: map_xml maps a name in a namespace to an IRI in that namespace, and a name
//! without one to an IRI in the xyz: namespace. A prefixed name in an XPath expression (p:pump) is
//! resolved with the namespaces given by the caller, those the mapping declares (rml:namespace)
//! and, for prefixes that neither declares, those the document itself declares (xmlns:p). As in
//! XPath, an element name without a prefix has no namespace, unless a namespace is given for the
//! empty prefix. map_xml puts an attribute without a prefix in the default namespace of its
//! element, so an attribute name without a prefix matches in no namespace and in the default
//! namespaces of the document.

use crate::errors::MaplibError;
use crate::model::on_query_stack;
use oxrdf::vocab::xsd;
use oxrdf::Term;
use polars::prelude::{Column, DataFrame, IntoColumn, IntoSeries, NamedFrom, Series};
use representation::constants::{FX_PREFIX_NAME, FX_XMLNS, FX_XYZ_PREFIX_IRI};
use representation::solution_mapping::EagerSolutionMappings;
use std::collections::BTreeSet;
use triplestore::xml_name_iri;
use r2rml::mapping::{LogicalSource, LogicalTable, ReferenceFormulation, RECORD_COLUMN};
use representation::constants::FX_NULL;
use r2rml::{R2RMLError, R2RMLMapping};
use representation::constants::{FX_CHILD, FX_ROOT, XYZ_PREFIX_IRI};
use representation::dataset::NamedGraph;
use representation::polars_to_rdf::column_as_terms;
use representation::result::QueryResultKind;
use std::collections::HashMap;
use triplestore::sparql::QuerySettings;
use triplestore::{json_key_local_name, StorageOptions, Triplestore};

/// The data of a source of an RML mapping.
pub enum RmlSource {
    /// A table, e.g. a CSV file that was read: the references are its column names.
    Table(DataFrame),
    /// A JSON document.
    Json(Vec<u8>),
    /// An XML document.
    Xml(Vec<u8>),
}

fn err<T>(s: String) -> Result<T, MaplibError> {
    Err(R2RMLError::MappingError(s).into())
}

/// A step of the supported subset of JSONPath.
#[derive(Debug, Clone, PartialEq)]
enum JsonStep {
    /// `.key` or `['key']`
    Key(String),
    /// `[*]`: the items of an array
    Items,
}

/// Parses the supported subset of JSONPath: `$`, `.key`, `['key']` and `[*]`. A reference may
/// leave out the leading `$.`, and a reference without `.` and `[` is one key.
fn parse_json_path(path: &str, is_reference: bool) -> Result<Vec<JsonStep>, MaplibError> {
    let original = path;
    let path = path.trim();
    let unsupported = |what: &str| -> Result<Vec<JsonStep>, MaplibError> {
        err(format!(
            "the JSONPath {original:?} uses {what}, which is not supported: \
             the supported steps are $, .key, ['key'] and [*]"
        ))
    };
    let mut rest: String = if let Some(r) = path.strip_prefix('$').or_else(|| path.strip_prefix('@')) {
        r.to_string()
    } else if is_reference && !path.is_empty() {
        if !path.contains(['.', '[']) {
            return Ok(vec![JsonStep::Key(path.to_string())]);
        } else if path.starts_with('[') {
            path.to_string()
        } else {
            format!(".{path}")
        }
    } else {
        return err(format!("the JSONPath {original:?} must start with $"));
    };
    let mut steps = vec![];
    while !rest.is_empty() {
        if rest.starts_with("..") {
            return unsupported("recursive descent (..)");
        } else if let Some(r) = rest.strip_prefix('.') {
            let end = r.find(['.', '[']).unwrap_or(r.len());
            let key = &r[..end];
            if key == "*" {
                return unsupported("a wildcard over keys (.*)");
            }
            if key.is_empty() {
                return err(format!("the JSONPath {original:?} has an empty key"));
            }
            steps.push(JsonStep::Key(key.to_string()));
            rest = r[end..].to_string();
        } else if let Some(r) = rest.strip_prefix('[') {
            let Some(end) = r.find(']') else {
                return err(format!("the JSONPath {original:?} has an unclosed ["));
            };
            let inner = r[..end].trim();
            if inner == "*" {
                steps.push(JsonStep::Items);
            } else if inner.len() >= 2
                && ((inner.starts_with('\'') && inner.ends_with('\''))
                    || (inner.starts_with('"') && inner.ends_with('"')))
            {
                steps.push(JsonStep::Key(inner[1..inner.len() - 1].to_string()));
            } else if inner.starts_with('?') {
                return unsupported("a filter ([?(...)])");
            } else {
                return unsupported("an array index or slice ([n])");
            }
            rest = r[end + 1..].to_string();
        } else {
            return err(format!("the JSONPath {original:?} could not be parsed at {rest:?}"));
        }
    }
    Ok(steps)
}

/// Makes the variables of the patterns of a query.
struct Variables {
    next: usize,
}

impl Variables {
    fn fresh(&mut self) -> String {
        self.next += 1;
        format!("?n{}", self.next)
    }
}

/// The triple patterns of JSONPath steps from the variable `from`, and the variable they end at.
fn json_patterns(steps: &[JsonStep], from: &str, to: Option<&str>, vars: &mut Variables) -> (String, String) {
    let mut patterns = String::new();
    let mut current = from.to_string();
    for (i, step) in steps.iter().enumerate() {
        let next = match to {
            Some(to) if i + 1 == steps.len() => to.to_string(),
            _ => vars.fresh(),
        };
        let predicate = match step {
            JsonStep::Key(k) => format!("{XYZ_PREFIX_IRI}{}", json_key_local_name(k)),
            JsonStep::Items => FX_CHILD.to_string(),
        };
        patterns.push_str(&format!("{current} <{predicate}> {next} . "));
        current = next;
    }
    (patterns, current)
}

/// What an XPath step selects.
#[derive(Debug, Clone, PartialEq)]
enum XmlTest {
    /// An element by name
    Element(String),
    /// `*`
    AnyElement,
    /// `.`
    Current,
    /// `..`
    Parent,
    /// `@name`
    Attribute(String),
    /// `text()`
    Text,
}

#[derive(Debug, Clone, PartialEq)]
enum XmlPredicate {
    /// `[@a='v']`, or `[@a]` without a value
    Attribute(String, Option<String>),
    /// `[e='v']`, or `[e]` without a value
    Child(String, Option<String>),
    /// `[.='v']`
    Text(String),
}

#[derive(Debug, Clone, PartialEq)]
struct XmlStep {
    /// The step comes after `//`: it selects among all descendants.
    descendant: bool,
    test: XmlTest,
    predicates: Vec<XmlPredicate>,
}

/// How the names in the XPath expressions over a source are resolved to the IRIs that map_xml
/// gives the elements and attributes of the source.
#[derive(Debug, Clone, Default)]
struct XmlNames {
    /// Namespaces by prefix: those of the caller and of the mapping, and those the document
    /// declares for the other prefixes.
    prefixes: HashMap<String, String>,
    /// Prefixes that only the document declares, with more than one namespace.
    ambiguous: HashMap<String, Vec<String>>,
    /// The namespace of element names without a prefix, given for the empty prefix.
    default_namespace: Option<String>,
    /// The default namespaces (xmlns="...") that the document declares.
    document_defaults: Vec<String>,
}

impl XmlNames {
    /// `document_defaults` and `document_prefixes` are what the document declares, `mapping`
    /// what the logical source declares and `given` what the caller gave, which wins.
    fn new(
        document_defaults: Vec<String>,
        document_prefixes: Vec<(String, String)>,
        mapping: &[(String, String)],
        given: &HashMap<String, String>,
    ) -> Result<Self, MaplibError> {
        let mut declared: HashMap<String, String> = HashMap::new();
        for (p, ns) in mapping.iter().map(|(p, ns)| (p, ns)).chain(given.iter()) {
            if ns.is_empty() || ns.contains(['<', '>', '"', '{', '}', '|', '\\', '^', '`', ' ', '\n', '\t']) {
                return err(format!("the namespace {ns:?} of the prefix {p:?} is not an IRI"));
            }
            declared.insert(p.clone(), ns.clone());
        }
        let default_namespace = declared.remove("");
        let mut by_prefix: HashMap<String, BTreeSet<String>> = HashMap::new();
        for (p, ns) in document_prefixes {
            by_prefix.entry(p).or_default().insert(ns);
        }
        let mut prefixes = HashMap::new();
        let mut ambiguous = HashMap::new();
        for (p, namespaces) in by_prefix {
            if declared.contains_key(&p) {
                continue;
            }
            let mut namespaces: Vec<String> = namespaces.into_iter().collect();
            if namespaces.len() == 1 {
                prefixes.insert(p, namespaces.pop().unwrap());
            } else {
                ambiguous.insert(p, namespaces);
            }
        }
        prefixes.extend(declared);
        Ok(XmlNames {
            prefixes,
            ambiguous,
            default_namespace,
            document_defaults,
        })
    }

    fn namespace(&self, prefix: &str, path: &str) -> Result<&str, MaplibError> {
        if let Some(ns) = self.prefixes.get(prefix) {
            return Ok(ns.as_str());
        }
        if let Some(namespaces) = self.ambiguous.get(prefix) {
            return err(format!(
                "the XPath {path:?} uses the prefix {prefix}, which the document binds to several \
                 namespaces ({}): give the one that is meant in namespaces",
                namespaces.join(", ")
            ));
        }
        err(format!(
            "the XPath {path:?} uses the prefix {prefix}, which is not declared: give its namespace \
             in namespaces, or with rml:namespace on the reference formulation of the mapping"
        ))
    }

    /// The IRI, in angle brackets, of the type of the elements with a name.
    fn element(&self, name: &str, path: &str) -> Result<String, MaplibError> {
        let iri = match name.split_once(':') {
            Some((prefix, local)) => xml_name_iri(self.namespace(prefix, path)?, local),
            None => xml_name_iri(self.default_namespace.as_deref().unwrap_or(XYZ_PREFIX_IRI), name),
        };
        Ok(format!("<{iri}>"))
    }

    /// The property, or the alternatives of properties, of the attributes with a name.
    fn attribute(&self, name: &str, path: &str) -> Result<String, MaplibError> {
        if let Some((prefix, local)) = name.split_once(':') {
            return Ok(format!("<{}>", xml_name_iri(self.namespace(prefix, path)?, local)));
        }
        let mut namespaces: Vec<&str> = vec![XYZ_PREFIX_IRI];
        for ns in self.document_defaults.iter().chain(self.default_namespace.iter()) {
            if !namespaces.contains(&ns.as_str()) {
                namespaces.push(ns);
            }
        }
        let iris: Vec<String> = namespaces
            .iter()
            .map(|ns| format!("<{}>", xml_name_iri(ns, name)))
            .collect();
        Ok(if iris.len() == 1 {
            iris[0].clone()
        } else {
            format!("({})", iris.join("|"))
        })
    }
}

/// Checks a name of an XPath expression, which may have a prefix.
fn xml_name(name: &str, path: &str) -> Result<String, MaplibError> {
    let name = name.trim();
    let part_is_valid = |part: &str| {
        !part.is_empty()
            && part.chars().all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'))
            && !part.starts_with(|c: char| c.is_numeric() || c == '-' || c == '.')
    };
    let valid = match name.split_once(':') {
        Some((prefix, local)) => part_is_valid(prefix) && part_is_valid(local),
        None => part_is_valid(name),
    };
    if !valid {
        return err(format!(
            "the XPath {path:?} could not be parsed at {name:?}: the supported steps are element \
             names, *, ., .., @attribute and text(), with the predicates [@a='v'], [e='v'], [.='v'], [@a] and [e]"
        ));
    }
    Ok(name.to_string())
}

fn parse_xml_predicate(p: &str, path: &str) -> Result<XmlPredicate, MaplibError> {
    let p = p.trim();
    if p.chars().all(|c| c.is_ascii_digit()) || p.contains("position()") || p.contains("last()") {
        return err(format!(
            "the XPath {path:?} selects by position ([{p}]), which is not supported"
        ));
    }
    let (left, value) = match p.split_once('=') {
        Some((l, v)) => {
            let v = v.trim();
            let quoted = v.len() >= 2
                && ((v.starts_with('\'') && v.ends_with('\'')) || (v.starts_with('"') && v.ends_with('"')));
            if !quoted || l.trim().ends_with(['!', '<', '>']) {
                return err(format!(
                    "the predicate [{p}] of the XPath {path:?} is not supported: a predicate compares \
                     an attribute, a child element or . to a quoted string with ="
                ));
            }
            (l.trim(), Some(v[1..v.len() - 1].to_string()))
        }
        None => (p, None),
    };
    if left == "." {
        return match value {
            Some(v) => Ok(XmlPredicate::Text(v)),
            None => err(format!("the predicate [.] of the XPath {path:?} is not supported")),
        };
    }
    if let Some(a) = left.strip_prefix('@') {
        Ok(XmlPredicate::Attribute(xml_name(a, path)?, value))
    } else {
        Ok(XmlPredicate::Child(xml_name(left, path)?, value))
    }
}

/// Parses the supported subset of XPath. Returns whether the path is absolute, and its steps.
fn parse_xml_path(path: &str) -> Result<(bool, Vec<XmlStep>), MaplibError> {
    let original = path;
    let path = path.trim();
    if path.is_empty() {
        return err("an XPath expression is empty".to_string());
    }
    if path.contains('|') || path.contains("::") {
        return err(format!(
            "the XPath {original:?} uses a union (|) or an axis (::), which is not supported"
        ));
    }
    let absolute = path.starts_with('/');
    // Split into steps at / outside of predicates and quotes, noting // before a step.
    let mut raw_steps: Vec<(bool, String)> = vec![];
    let mut current = String::new();
    let mut descendant = false;
    let mut depth = 0;
    let mut quote: Option<char> = None;
    let mut chars = path.chars().peekable();
    let mut started = false;
    while let Some(c) = chars.next() {
        match (c, quote) {
            (c, Some(q)) => {
                if c == q {
                    quote = None;
                }
                current.push(c);
            }
            ('\'' | '"', None) => {
                quote = Some(c);
                current.push(c);
            }
            ('[', None) => {
                depth += 1;
                current.push(c);
            }
            (']', None) => {
                depth -= 1;
                current.push(c);
            }
            ('/', None) if depth == 0 => {
                if started || !current.is_empty() {
                    if current.trim().is_empty() {
                        return err(format!("the XPath {original:?} has an empty step"));
                    }
                    raw_steps.push((descendant, std::mem::take(&mut current)));
                }
                started = true;
                descendant = chars.peek() == Some(&'/');
                if descendant {
                    chars.next();
                }
            }
            (c, None) => current.push(c),
        }
    }
    if current.trim().is_empty() {
        return err(format!("the XPath {original:?} ends with /"));
    }
    raw_steps.push((descendant, current));

    let mut steps = vec![];
    for (descendant, raw) in raw_steps {
        let raw = raw.trim();
        let (test, mut rest) = match raw.find('[') {
            Some(i) => (raw[..i].trim(), &raw[i..]),
            None => (raw, ""),
        };
        let mut predicates = vec![];
        while !rest.is_empty() {
            let Some(end) = rest.find(']') else {
                return err(format!("the XPath {original:?} has an unclosed ["));
            };
            predicates.push(parse_xml_predicate(&rest[1..end], original)?);
            rest = rest[end + 1..].trim_start();
            if !rest.is_empty() && !rest.starts_with('[') {
                return err(format!("the XPath {original:?} could not be parsed at {rest:?}"));
            }
        }
        let test = match test {
            "." => XmlTest::Current,
            ".." => XmlTest::Parent,
            "*" => XmlTest::AnyElement,
            "text()" => XmlTest::Text,
            t if t.ends_with(')') => {
                return err(format!(
                    "the XPath {original:?} uses the function {t}, which is not supported"
                ))
            }
            t => match t.strip_prefix('@') {
                Some(a) => XmlTest::Attribute(xml_name(a, original)?),
                None => XmlTest::Element(xml_name(t, original)?),
            },
        };
        if !predicates.is_empty() && !matches!(test, XmlTest::Element(_) | XmlTest::AnyElement) {
            return err(format!(
                "the XPath {original:?} has a predicate on a step that is not an element"
            ));
        }
        steps.push(XmlStep {
            descendant,
            test,
            predicates,
        });
    }
    Ok((absolute, steps))
}

fn string_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

const RDF_TYPE: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";

/// The patterns of XPath steps. `from` is the variable of the context element, or None at the
/// document, whose only child is the root element. Returns the patterns and the variable of the
/// element or value the steps end at, and whether that is a value (an attribute or a text).
fn xml_patterns(
    steps: &[XmlStep],
    from: Option<&str>,
    path: &str,
    names: &XmlNames,
    vars: &mut Variables,
) -> Result<(String, String, bool), MaplibError> {
    let child = format!("<{FX_CHILD}>");
    let root = format!("<{FX_ROOT}>");
    let mut patterns = String::new();
    let mut current: Option<String> = from.map(|x| x.to_string());
    let mut is_value = false;
    for step in steps {
        if is_value {
            return err(format!(
                "the XPath {path:?} continues after an attribute or text() step"
            ));
        }
        match &step.test {
            XmlTest::Element(_) | XmlTest::AnyElement => {
                let next = vars.fresh();
                match (&current, step.descendant) {
                    (None, false) => patterns.push_str(&format!("{next} {RDF_TYPE} {root} . ")),
                    // Any element of the document, the root included.
                    (None, true) => {}
                    (Some(c), false) => patterns.push_str(&format!("{c} {child} {next} . ")),
                    (Some(c), true) => patterns.push_str(&format!("{c} {child}+ {next} . ")),
                }
                match &step.test {
                    XmlTest::Element(name) => {
                        patterns.push_str(&format!("{next} {RDF_TYPE} {} . ", names.element(name, path)?))
                    }
                    _ => {
                        if current.is_none() && step.descendant {
                            // Every element has a type.
                            let t = vars.fresh();
                            patterns.push_str(&format!("{next} {RDF_TYPE} {t} . FILTER({t} != {root}) "));
                        } else if current.is_some() {
                            patterns.push_str(&format!("FILTER(isIRI({next})) "));
                        }
                    }
                }
                for p in &step.predicates {
                    match p {
                        XmlPredicate::Attribute(a, v) => {
                            let o = v.as_deref().map(string_literal).unwrap_or_else(|| vars.fresh());
                            patterns.push_str(&format!("{next} {} {o} . ", names.attribute(a, path)?));
                        }
                        XmlPredicate::Child(e, v) => {
                            let c = vars.fresh();
                            patterns.push_str(&format!(
                                "{next} {child} {c} . {c} {RDF_TYPE} {} . ",
                                names.element(e, path)?
                            ));
                            if let Some(v) = v {
                                patterns.push_str(&format!("{c} {child} {} . ", string_literal(v)));
                            }
                        }
                        XmlPredicate::Text(v) => {
                            patterns.push_str(&format!("{next} {child} {} . ", string_literal(v)))
                        }
                    }
                }
                current = Some(next);
            }
            XmlTest::Current => {
                if step.descendant {
                    return err(format!("the XPath {path:?} has //. , which is not supported"));
                }
                if current.is_none() {
                    return err(format!("the XPath {path:?} selects the document, not an element"));
                }
            }
            XmlTest::Parent => {
                let Some(c) = &current else {
                    return err(format!("the XPath {path:?} selects the parent of the document"));
                };
                if step.descendant {
                    return err(format!("the XPath {path:?} has //.. , which is not supported"));
                }
                let next = vars.fresh();
                patterns.push_str(&format!("{next} {child} {c} . "));
                current = Some(next);
            }
            XmlTest::Attribute(_) | XmlTest::Text => {
                let Some(c) = &current else {
                    return err(format!(
                        "the XPath {path:?} selects an attribute or text of the document, not of an element"
                    ));
                };
                let element = if step.descendant {
                    let d = vars.fresh();
                    patterns.push_str(&format!("{c} {child}* {d} . "));
                    d
                } else {
                    c.clone()
                };
                let next = vars.fresh();
                match &step.test {
                    XmlTest::Attribute(a) => {
                        patterns.push_str(&format!("{element} {} {next} . ", names.attribute(a, path)?))
                    }
                    _ => patterns.push_str(&format!("{element} {child} {next} . FILTER(isLiteral({next})) ")),
                }
                current = Some(next);
                is_value = true;
            }
        }
    }
    match current {
        Some(c) => Ok((patterns, c, is_value)),
        None => err(format!("the XPath {path:?} selects the document, not an element")),
    }
}

/// The SPARQL query over the Façade-X graph of a source for the records of a logical source
/// and the values of its references, and the variable (without ?) of the values of each
/// reference, in their order.
fn source_query(
    source: &LogicalSource,
    references: &[String],
    names: &XmlNames,
) -> Result<(String, Vec<String>, String), MaplibError> {
    let mut vars = Variables { next: 0 };
    let mut query = String::new();
    let mut value_variables: Vec<String> = vec![];
    match source.formulation {
        ReferenceFormulation::JSONPath => {
            let steps = match &source.iterator {
                Some(i) => parse_json_path(i, false)?,
                None => vec![],
            };
            let doc = if steps.is_empty() { "?record" } else { "?doc" };
            query.push_str(&format!("{doc} {RDF_TYPE} <{FX_ROOT}> . "));
            let (patterns, _) = json_patterns(&steps, doc, Some("?record"), &mut vars);
            query.push_str(&patterns);
            for (i, r) in references.iter().enumerate() {
                let steps = parse_json_path(r, true)?;
                if steps.is_empty() {
                    // The record itself, when it is a value.
                    value_variables.push("record".to_string());
                } else {
                    let v = format!("?v{i}");
                    let (patterns, _) = json_patterns(&steps, "?record", Some(&v), &mut vars);
                    // Not only the values: an object or an array is an error, see values_column.
                    query.push_str(&format!("OPTIONAL {{ {patterns}}} "));
                    value_variables.push(format!("v{i}"));
                }
            }
        }
        ReferenceFormulation::XPath => {
            let iterator = source.iterator.as_deref().unwrap_or("/*");
            let (absolute, steps) = parse_xml_path(iterator)?;
            if !absolute {
                return err(format!("the XPath iterator {iterator:?} must start with /"));
            }
            let (patterns, record, is_value) = xml_patterns(&steps, None, iterator, names, &mut vars)?;
            if is_value {
                return err(format!(
                    "the XPath iterator {iterator:?} must select elements, not attributes or text"
                ));
            }
            query.push_str(&patterns);
            for (i, r) in references.iter().enumerate() {
                let (absolute, steps) = parse_xml_path(r)?;
                if absolute {
                    return err(format!(
                        "the XPath reference {r:?} must be relative to the record, not start with /"
                    ));
                }
                let (mut patterns, end, is_value) = xml_patterns(&steps, Some(&record), r, names, &mut vars)?;
                if is_value {
                    value_variables.push(end.trim_start_matches('?').to_string());
                } else {
                    // The value of an element is its text.
                    let v = format!("?v{i}");
                    patterns.push_str(&format!("{end} <{FX_CHILD}> {v} . FILTER(isLiteral({v})) "));
                    value_variables.push(format!("v{i}"));
                }
                query.push_str(&format!("OPTIONAL {{ {patterns}}} "));
            }
            // The records are counted by their element.
            value_variables.push(record.trim_start_matches('?').to_string());
        }
        ReferenceFormulation::CSV => unreachable!("CSV sources are tables"),
    }
    if source.formulation == ReferenceFormulation::JSONPath {
        value_variables.push("record".to_string());
    }
    let mut projection: Vec<&str> = vec![];
    for v in &value_variables {
        if !projection.contains(&v.as_str()) {
            projection.push(v);
        }
    }
    let projection: Vec<String> = projection.iter().map(|v| format!("?{v}")).collect();
    let query = format!("SELECT {} WHERE {{ {query}}}", projection.join(" "));
    // The last variable is that of the records, which is not a reference.
    let record_variable = value_variables.pop().unwrap_or_default();
    Ok((query, value_variables, record_variable))
}

/// A column of the values of a reference. JSON numbers and booleans keep their type when all
/// the values of the reference have it, so that they give typed literals as columns of a table do.
fn values_column(name: &str, terms: Vec<Option<Term>>) -> Result<Column, MaplibError> {
    // A JSON null is no value. An object or an array is not a value either, but a mistake.
    if terms.iter().flatten().any(|t| match t {
        Term::NamedNode(nn) => nn.as_str() != FX_NULL,
        Term::Literal(_) => false,
        _ => true,
    }) {
        return err(format!(
            "the reference {name:?} selects an object or an array of the source, not a value: \
             select its values, e.g. the items of an array with [*]"
        ));
    }
    Ok(values_column_of_literals(name, terms))
}

fn values_column_of_literals(name: &str, terms: Vec<Option<Term>>) -> Column {
    let literals: Vec<Option<(&str, &str)>> = terms
        .iter()
        .map(|t| match t {
            Some(Term::Literal(l)) => Some((l.value(), l.datatype().as_str())),
            _ => None,
        })
        .collect();
    let all = |datatypes: &[&str]| {
        let mut any = false;
        for (_, dt) in literals.iter().flatten() {
            if !datatypes.contains(dt) {
                return false;
            }
            any = true;
        }
        any
    };
    let integers = [xsd::INTEGER.as_str(), xsd::LONG.as_str(), xsd::INT.as_str()];
    let numbers = [
        xsd::INTEGER.as_str(),
        xsd::LONG.as_str(),
        xsd::INT.as_str(),
        xsd::DOUBLE.as_str(),
        xsd::FLOAT.as_str(),
    ];
    if all(&integers) {
        let parsed: Option<Vec<Option<i64>>> = literals
            .iter()
            .map(|l| match l {
                Some((v, _)) => v.parse::<i64>().ok().map(Some),
                None => Some(None),
            })
            .collect();
        if let Some(parsed) = parsed {
            return Series::new(name.into(), parsed).into_column();
        }
    } else if all(&numbers) {
        let parsed: Option<Vec<Option<f64>>> = literals
            .iter()
            .map(|l| match l {
                Some((v, _)) => v.parse::<f64>().ok().map(Some),
                None => Some(None),
            })
            .collect();
        if let Some(parsed) = parsed {
            return Series::new(name.into(), parsed).into_column();
        }
    } else if all(&[xsd::BOOLEAN.as_str()]) {
        let parsed: Vec<Option<bool>> = literals
            .iter()
            .map(|l| l.map(|(v, _)| v == "true" || v == "1"))
            .collect();
        return Series::new(name.into(), parsed).into_column();
    }
    let strings: Vec<Option<&str>> = literals.iter().map(|l| l.map(|(v, _)| v)).collect();
    Series::new(name.into(), strings).into_column()
}

/// The records of a logical source over the Façade-X graph of its source, as a DataFrame with a
/// column per reference.
fn select(store: &Triplestore, query: &str) -> Result<EagerSolutionMappings, MaplibError> {
    let settings = QuerySettings {
        include_graphs: vec![],
        max_rows: None,
        strict_project: false,
        federation: None,
        evaluation_hook: None,
    };
    let result = on_query_stack(query, || {
        store.query(query, None, &settings, None, None, false, None)
    })?;
    let QueryResultKind::Select(sm) = result.kind else {
        unreachable!("a SELECT query")
    };
    Ok(sm)
}

/// The values of a variable of solutions, as strings: the IRIs and the lexical forms.
fn strings(store: &Triplestore, sm: &EagerSolutionMappings, variable: &str) -> Vec<Option<String>> {
    match (sm.mappings.column(variable), sm.rdf_node_types.get(variable)) {
        (Ok(c), Some(t)) => column_as_terms(c, t, store.global_cats.clone(), false)
            .into_iter()
            .map(|t| match t {
                Some(Term::NamedNode(nn)) => Some(nn.as_str().to_string()),
                Some(Term::Literal(l)) => Some(l.value().to_string()),
                _ => None,
            })
            .collect(),
        _ => vec![None; sm.mappings.height()],
    }
}

/// The namespaces that an XML document declares, from its Façade-X graph: the default
/// namespaces (xmlns="...") and the prefixed ones (xmlns:p="...") as (prefix, namespace).
fn document_namespaces(store: &Triplestore) -> Result<(Vec<String>, Vec<(String, String)>), MaplibError> {
    let sm = select(store, &format!("SELECT DISTINCT ?ns WHERE {{ ?e <{FX_XMLNS}> ?ns }}"))?;
    let mut defaults: Vec<String> = strings(store, &sm, "ns").into_iter().flatten().collect();
    defaults.sort();
    let sm = select(
        store,
        &format!(
            "SELECT DISTINCT ?p ?ns WHERE {{ ?d <{FX_PREFIX_NAME}> ?p . ?d <{FX_XYZ_PREFIX_IRI}> ?ns }}"
        ),
    )?;
    let prefixes = strings(store, &sm, "p")
        .into_iter()
        .zip(strings(store, &sm, "ns"))
        .filter_map(|(p, ns)| Some((p?, ns?)))
        .collect();
    Ok((defaults, prefixes))
}

fn source_table(
    store: &Triplestore,
    source: &LogicalSource,
    references: &[String],
    names: &XmlNames,
    with_record: bool,
) -> Result<DataFrame, MaplibError> {
    let (query, value_variables, record_variable) = source_query(source, references, names)?;
    let sm = select(store, &query)?;
    let height = sm.mappings.height();
    if height == 0
        && source.formulation == ReferenceFormulation::XPath
        && names.default_namespace.is_none()
        && !names.document_defaults.is_empty()
    {
        // No records, where the names of the iterator have no namespace and the elements of the
        // document do: an empty result here is a mapping that does not fit the document.
        let iterator = source.iterator.as_deref().unwrap_or("/*");
        let (_, steps) = parse_xml_path(iterator)?;
        let unqualified = steps
            .iter()
            .any(|s| matches!(&s.test, XmlTest::Element(n) if !n.contains(':')));
        if unqualified {
            let ns = &names.document_defaults[0];
            return err(format!(
                "the XPath iterator {iterator:?} selects no elements of the source {:?}. Its element \
                 names have no prefix, so they are names without a namespace, but the document declares \
                 the default namespace {}. Bind a prefix to it and use the prefix in the paths, e.g. \
                 namespaces={{\"p\": \"{ns}\"}} with /p:name, or make it the namespace of names without a \
                 prefix with namespaces={{\"\": \"{ns}\"}}",
                source.source,
                names.document_defaults.join(", "),
            ));
        }
    }
    let mut columns = Vec::with_capacity(references.len());
    for (i, r) in references.iter().enumerate() {
        let variable = &value_variables[i];
        let terms = match (sm.mappings.column(variable), sm.rdf_node_types.get(variable)) {
            (Ok(c), Some(t)) => column_as_terms(c, t, store.global_cats.clone(), false),
            // A reference without a value for any record.
            _ => vec![None; height],
        };
        columns.push(values_column(r, terms)?);
    }
    if with_record {
        // The record of each row, for the blank nodes that the mapping generates per record.
        let records = strings(store, &sm, &record_variable);
        columns.push(Series::new(RECORD_COLUMN.into(), records).into_column());
    }
    if columns.is_empty() {
        // Only the number of records matters to a mapping of constants.
        columns.push(Series::new("\u{1}rows".into(), vec![0u32; height]).into_column());
    }
    Ok(DataFrame::new(height, columns).map_err(R2RMLError::from)?)
}

/// The table with the strings that the source declares as null values (rml:null) made null.
fn without_nulls(table: DataFrame, nulls: &[String]) -> Result<DataFrame, MaplibError> {
    if nulls.is_empty() {
        return Ok(table);
    }
    let mut columns = Vec::with_capacity(table.width());
    for c in table.columns() {
        let s = c.as_materialized_series();
        let column = match s.str() {
            Ok(strings) if strings.iter().flatten().any(|v| nulls.iter().any(|n| n == v)) => {
                let kept: polars::prelude::StringChunked = strings
                    .iter()
                    .map(|v| v.filter(|v| !nulls.iter().any(|n| n == v)))
                    .collect();
                kept.with_name(c.name().clone()).into_series().into_column()
            }
            _ => c.clone(),
        };
        columns.push(column);
    }
    Ok(DataFrame::new(table.height(), columns).map_err(R2RMLError::from)?)
}

/// The rows of the logical sources of an RML mapping, keyed as the R2RML engine looks up the
/// result of a logical table (LogicalTable::key). `sources` holds the data of each rml:source
/// by its name in the mapping.
pub fn source_tables(
    mapping: &R2RMLMapping,
    sources: &mut HashMap<String, RmlSource>,
    xml_namespaces: &HashMap<String, String>,
) -> Result<HashMap<String, DataFrame>, MaplibError> {
    let mut out = HashMap::new();
    // The namespaces that each XML source declares.
    let mut declared: HashMap<String, (Vec<String>, Vec<(String, String)>)> = HashMap::new();
    let no_names = XmlNames::default();
    let with_record = mapping.uses_record();
    // The Façade-X graph of each JSON or XML source, mapped once for all its iterators.
    let mut stores: HashMap<String, Triplestore> = HashMap::new();
    for (source, references) in mapping.logical_sources() {
        let key = LogicalTable::Source(source.clone()).key();
        let Some(data) = sources.get_mut(&source.source) else {
            return err(format!(
                "no source was given for the rml:source {:?}",
                source.source
            ));
        };
        let table = match (data, source.formulation) {
            // A table is used as it is, whatever the reference formulation says.
            (RmlSource::Table(df), _) => df.clone(),
            (_, ReferenceFormulation::CSV) => {
                return err(format!(
                    "the source {:?} has the reference formulation CSV, but is not a table",
                    source.source
                ))
            }
            (RmlSource::Json(bytes), ReferenceFormulation::JSONPath) => {
                if !stores.contains_key(&source.source) {
                    let mut store = Triplestore::new(StorageOptions::memory(), None)?;
                    // Parsing changes the bytes in place, so they are parsed from a copy.
                    let mut copy = bytes.clone();
                    store.map_json(&mut copy, None, &NamedGraph::DefaultGraph)?;
                    stores.insert(source.source.clone(), store);
                }
                source_table(&stores[&source.source], &source, &references, &no_names, with_record)?
            }
            (RmlSource::Xml(bytes), ReferenceFormulation::XPath) => {
                if !stores.contains_key(&source.source) {
                    let mut store = Triplestore::new(StorageOptions::memory(), None)?;
                    store.map_xml(bytes, &NamedGraph::DefaultGraph, None)?;
                    declared.insert(source.source.clone(), document_namespaces(&store)?);
                    stores.insert(source.source.clone(), store);
                }
                let (defaults, prefixes) = declared[&source.source].clone();
                let names = XmlNames::new(defaults, prefixes, &source.namespaces, xml_namespaces)?;
                source_table(&stores[&source.source], &source, &references, &names, with_record)?
            }
            (RmlSource::Json(_), ReferenceFormulation::XPath) => {
                return err(format!(
                    "the source {:?} is JSON, but its reference formulation is XPath",
                    source.source
                ))
            }
            (RmlSource::Xml(_), ReferenceFormulation::JSONPath) => {
                return err(format!(
                    "the source {:?} is XML, but its reference formulation is JSONPath",
                    source.source
                ))
            }
        };
        out.insert(key, without_nulls(table, &source.nulls)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_paths() {
        assert_eq!(
            parse_json_path("$.a['b c'][*].d", false).unwrap(),
            vec![
                JsonStep::Key("a".into()),
                JsonStep::Key("b c".into()),
                JsonStep::Items,
                JsonStep::Key("d".into())
            ]
        );
        assert_eq!(parse_json_path("first name", true).unwrap(), vec![JsonStep::Key("first name".into())]);
        assert_eq!(
            parse_json_path("a.b[*]", true).unwrap(),
            vec![JsonStep::Key("a".into()), JsonStep::Key("b".into()), JsonStep::Items]
        );
        assert!(parse_json_path("$", false).unwrap().is_empty());
        assert!(parse_json_path("$..a", false).is_err());
        assert!(parse_json_path("$.a[0]", false).is_err());
        assert!(parse_json_path("a", false).is_err());
    }

    #[test]
    fn xml_paths() {
        let (absolute, steps) = parse_xml_path("/plants//plant[@id='a/b'][site]/@name").unwrap();
        assert!(absolute);
        assert_eq!(steps.len(), 3);
        assert!(steps[1].descendant);
        assert_eq!(
            steps[1].predicates,
            vec![
                XmlPredicate::Attribute("id".into(), Some("a/b".into())),
                XmlPredicate::Child("site".into(), None)
            ]
        );
        assert_eq!(steps[2].test, XmlTest::Attribute("name".into()));
        assert!(!parse_xml_path("name/text()").unwrap().0);
        assert!(parse_xml_path("/a/b[1]").is_err());
        assert_eq!(parse_xml_path("/p:a/@q:b").unwrap().1[0].test, XmlTest::Element("p:a".into()));
        assert!(parse_xml_path("/p:/a").is_err());
        assert!(parse_xml_path("count(a)").is_err());
    }
}

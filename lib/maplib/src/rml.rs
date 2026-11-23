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

use crate::errors::MaplibError;
use crate::model::on_query_stack;
use oxrdf::vocab::xsd;
use oxrdf::Term;
use polars::prelude::{Column, DataFrame, IntoColumn, NamedFrom, Series};
use r2rml::mapping::{LogicalSource, LogicalTable, ReferenceFormulation};
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

fn xml_name(name: &str, path: &str) -> Result<String, MaplibError> {
    let name = name.trim();
    if name.contains(':') {
        return err(format!(
            "the XPath {path:?} uses the prefixed name {name}, but namespaces are not supported"
        ));
    }
    let valid = !name.is_empty()
        && name.chars().all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'))
        && !name.starts_with(|c: char| c.is_numeric() || c == '-' || c == '.');
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

fn xyz(name: &str) -> String {
    format!("<{XYZ_PREFIX_IRI}{name}>")
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
                        patterns.push_str(&format!("{next} {RDF_TYPE} {} . ", xyz(name)))
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
                            patterns.push_str(&format!("{next} {} {o} . ", xyz(a)));
                        }
                        XmlPredicate::Child(e, v) => {
                            let c = vars.fresh();
                            patterns.push_str(&format!(
                                "{next} {child} {c} . {c} {RDF_TYPE} {} . ",
                                xyz(e)
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
                        patterns.push_str(&format!("{element} {} {next} . ", xyz(a)))
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
) -> Result<(String, Vec<String>), MaplibError> {
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
                    query.push_str(&format!("OPTIONAL {{ {patterns}FILTER(isLiteral({v})) }} "));
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
            let (patterns, record, is_value) = xml_patterns(&steps, None, iterator, &mut vars)?;
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
                let (mut patterns, end, is_value) = xml_patterns(&steps, Some(&record), r, &mut vars)?;
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
    value_variables.pop();
    Ok((query, value_variables))
}

/// A column of the values of a reference. JSON numbers and booleans keep their type when all
/// the values of the reference have it, so that they give typed literals as columns of a table do.
fn values_column(name: &str, terms: Vec<Option<Term>>) -> Column {
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
fn source_table(
    store: &Triplestore,
    source: &LogicalSource,
    references: &[String],
) -> Result<DataFrame, MaplibError> {
    let (query, value_variables) = source_query(source, references)?;
    let settings = QuerySettings {
        include_graphs: vec![],
        max_rows: None,
        strict_project: false,
        federation: None,
        evaluation_hook: None,
    };
    let result = on_query_stack(&query, || {
        store.query(&query, None, &settings, None, None, false, None)
    })?;
    let QueryResultKind::Select(sm) = result.kind else {
        unreachable!("a SELECT query")
    };
    let height = sm.mappings.height();
    let mut columns = Vec::with_capacity(references.len());
    for (i, r) in references.iter().enumerate() {
        let variable = &value_variables[i];
        let terms = match (sm.mappings.column(variable), sm.rdf_node_types.get(variable)) {
            (Ok(c), Some(t)) => column_as_terms(c, t, store.global_cats.clone(), false),
            // A reference without a value for any record.
            _ => vec![None; height],
        };
        columns.push(values_column(r, terms));
    }
    if columns.is_empty() {
        // Only the number of records matters to a mapping of constants.
        columns.push(Series::new("\u{1}record".into(), vec![0u32; height]).into_column());
    }
    Ok(DataFrame::new(height, columns).map_err(R2RMLError::from)?)
}

/// The rows of the logical sources of an RML mapping, keyed as the R2RML engine looks up the
/// result of a logical table (LogicalTable::key). `sources` holds the data of each rml:source
/// by its name in the mapping.
pub fn source_tables(
    mapping: &R2RMLMapping,
    sources: &mut HashMap<String, RmlSource>,
) -> Result<HashMap<String, DataFrame>, MaplibError> {
    let mut out = HashMap::new();
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
                source_table(&stores[&source.source], &source, &references)?
            }
            (RmlSource::Xml(bytes), ReferenceFormulation::XPath) => {
                if !stores.contains_key(&source.source) {
                    let mut store = Triplestore::new(StorageOptions::memory(), None)?;
                    store.map_xml(bytes, &NamedGraph::DefaultGraph, None)?;
                    stores.insert(source.source.clone(), store);
                }
                source_table(&stores[&source.source], &source, &references)?
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
        out.insert(key, table);
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
        assert!(parse_xml_path("/p:a").is_err());
        assert!(parse_xml_path("count(a)").is_err());
    }
}

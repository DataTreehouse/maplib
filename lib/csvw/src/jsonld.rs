//! Conversion of notes and common properties (the JSON-LD dialect of CSVW metadata) to RDF, see
//! <https://www.w3.org/TR/csv2rdf/#json-ld-to-rdf>.

use crate::errors::{meta_err, CsvwError};
use crate::loader::resolve;
use crate::metadata::Ctx;
use crate::vocab::{expand_prefixed, CSVW_NS, CSVW_TERMS, XSD_NS};
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};

static BLANK_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, PartialEq)]
pub enum ATerm {
    /// The node that the annotation is on (group, table, ...).
    This,
    Iri(String),
    /// A blank node label, unique within one conversion.
    Blank(String),
    Literal {
        lexical: String,
        datatype: Option<String>,
        language: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AnnotationTriple {
    pub subject: ATerm,
    pub predicate: String,
    pub object: ATerm,
}

fn new_blank() -> ATerm {
    ATerm::Blank(format!(
        "csvwa{}",
        BLANK_COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Expands a property name or @type value: a term of the CSVW context, a prefixed name or an
/// absolute IRI.
fn expand_term(s: &str, ctx: &Ctx, allow_relative: bool) -> Result<String, CsvwError> {
    if s.starts_with("_:") {
        return meta_err(format!("blank nodes are not allowed here: {s}"));
    }
    if CSVW_TERMS.contains(&s) {
        return Ok(format!("{CSVW_NS}{s}"));
    }
    if let Some(e) = expand_prefixed(s) {
        return Ok(e);
    }
    if is_absolute_iri(s) {
        return Ok(s.to_string());
    }
    if allow_relative {
        return resolve(&ctx.base, s);
    }
    meta_err(format!(
        "{s} is not a term of the CSVW context, a prefixed name or an absolute IRI"
    ))
}

pub fn is_absolute_iri(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    for c in chars {
        if c == ':' {
            return true;
        }
        if !(c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.') {
            return false;
        }
    }
    false
}

pub fn common_property_triples(
    property: &str,
    value: &Value,
    ctx: &Ctx,
) -> Result<Vec<AnnotationTriple>, CsvwError> {
    let p = expand_term(property, ctx, false)?;
    let mut out = vec![];
    value_triples(ATerm::This, &p, value, ctx, &mut out)?;
    Ok(out)
}

fn value_triples(
    subject: ATerm,
    property: &str,
    value: &Value,
    ctx: &Ctx,
    out: &mut Vec<AnnotationTriple>,
) -> Result<(), CsvwError> {
    let push = |out: &mut Vec<AnnotationTriple>, object: ATerm| {
        out.push(AnnotationTriple {
            subject: subject.clone(),
            predicate: property.to_string(),
            object,
        })
    };
    match value {
        Value::Null => {}
        Value::Array(a) => {
            for v in a {
                if v.is_array() {
                    return meta_err("nested arrays are not allowed in common properties");
                }
                value_triples(subject.clone(), property, v, ctx, out)?;
            }
        }
        Value::Object(o) => {
            for k in o.keys() {
                match k.as_str() {
                    "@value" | "@type" | "@language" | "@id" => {}
                    "@list" | "@set" => {
                        return meta_err("list and set objects are not allowed in metadata")
                    }
                    "@context" => {
                        return meta_err("a metadata document must not add a new context")
                    }
                    k if k.starts_with('@') => {
                        return meta_err(format!("invalid keyword {k} in a common property"))
                    }
                    _ => {}
                }
            }
            if let Some(v) = o.get("@value") {
                if o.len() > 2
                    || (o.len() == 2 && !(o.contains_key("@type") || o.contains_key("@language")))
                {
                    return meta_err("a @value object may only have @type or @language");
                }
                let lexical = match v {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => number_lexical(n),
                    Value::Bool(b) => b.to_string(),
                    _ => return meta_err("@value must be a string, number or boolean"),
                };
                let mut datatype = None;
                let mut language = None;
                if let Some(t) = o.get("@type") {
                    let Value::String(t) = t else {
                        return meta_err("@type must be a string");
                    };
                    datatype = Some(expand_term(t, ctx, false)?);
                }
                if let Some(l) = o.get("@language") {
                    let Value::String(l) = l else {
                        return meta_err("@language must be a string");
                    };
                    if oxilangtag::LanguageTag::parse(l.as_str()).is_err() {
                        return meta_err(format!("invalid language tag {l}"));
                    }
                    language = Some(l.clone());
                }
                if datatype.is_none() && language.is_none() {
                    if let Value::Number(n) = v {
                        datatype = Some(number_datatype(n));
                    } else if v.is_boolean() {
                        datatype = Some(format!("{XSD_NS}boolean"));
                    }
                }
                push(
                    out,
                    ATerm::Literal {
                        lexical,
                        datatype,
                        language,
                    },
                );
            } else {
                if o.contains_key("@language") {
                    return meta_err("@language may only be used together with @value");
                }
                let node = if let Some(id) = o.get("@id") {
                    let Value::String(id) = id else {
                        return meta_err("@id must be a string");
                    };
                    if id.starts_with("_:") {
                        return meta_err(format!("@id must not be a blank node, found {id}"));
                    }
                    ATerm::Iri(resolve(&ctx.base, id)?)
                } else {
                    new_blank()
                };
                push(out, node.clone());
                if let Some(t) = o.get("@type") {
                    let types = match t {
                        Value::String(s) => vec![s.clone()],
                        Value::Array(a) => {
                            let mut ts = vec![];
                            for x in a {
                                let Value::String(s) = x else {
                                    return meta_err("@type values must be strings");
                                };
                                ts.push(s.clone());
                            }
                            ts
                        }
                        _ => return meta_err("@type must be a string or an array of strings"),
                    };
                    for t in types {
                        out.push(AnnotationTriple {
                            subject: node.clone(),
                            predicate: format!("{}type", crate::vocab::RDF_NS),
                            object: ATerm::Iri(expand_term(&t, ctx, false)?),
                        });
                    }
                }
                for (k, v) in o {
                    if k.starts_with('@') {
                        continue;
                    }
                    let p = expand_term(k, ctx, false)?;
                    value_triples(node.clone(), &p, v, ctx, out)?;
                }
            }
        }
        Value::String(s) => push(
            out,
            ATerm::Literal {
                lexical: s.clone(),
                datatype: None,
                language: ctx.lang.clone(),
            },
        ),
        Value::Bool(b) => push(
            out,
            ATerm::Literal {
                lexical: b.to_string(),
                datatype: Some(format!("{XSD_NS}boolean")),
                language: None,
            },
        ),
        Value::Number(n) => push(
            out,
            ATerm::Literal {
                lexical: number_lexical(n),
                datatype: Some(number_datatype(n)),
                language: None,
            },
        ),
    }
    Ok(())
}

fn number_lexical(n: &serde_json::Number) -> String {
    if n.is_i64() || n.is_u64() {
        n.to_string()
    } else {
        let f = n.as_f64().unwrap_or(0.0);
        if f.fract() == 0.0 && f.abs() < 1e18 {
            format!("{}", f as i64)
        } else {
            format!("{f:E}")
        }
    }
}

fn number_datatype(n: &serde_json::Number) -> String {
    if n.is_i64() || n.is_u64() || n.as_f64().map(|f| f.fract() == 0.0).unwrap_or(false) {
        format!("{XSD_NS}integer")
    } else {
        format!("{XSD_NS}double")
    }
}

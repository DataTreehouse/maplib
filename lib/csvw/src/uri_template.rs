//! Compiling URI templates ([RFC 6570](https://www.rfc-editor.org/rfc/rfc6570)) into SPARQL
//! expressions, including the prefix expansion and IRI resolution that CSVW applies to the
//! expanded template.

use crate::errors::CsvwError;
use crate::loader::resolve;
use crate::sparql_util::{lit, Bindings};
use crate::vocab::expand_prefixed;

#[derive(Debug, Clone)]
pub struct VarSpec {
    pub name: String,
    pub prefix: Option<usize>,
    pub explode: bool,
}

#[derive(Debug, Clone)]
pub enum Part {
    Lit(String),
    Expr {
        op: Option<char>,
        vars: Vec<VarSpec>,
    },
}

pub fn parse_template(t: &str) -> Result<Vec<Part>, CsvwError> {
    let mut parts = vec![];
    let mut rest = t;
    while !rest.is_empty() {
        if let Some(i) = rest.find('{') {
            if i > 0 {
                parts.push(Part::Lit(rest[..i].to_string()));
            }
            let Some(j) = rest[i..].find('}') else {
                return Err(CsvwError::UriTemplateError(
                    t.to_string(),
                    "unclosed expression".to_string(),
                ));
            };
            let inner = &rest[i + 1..i + j];
            let (op, vars) = match inner.chars().next() {
                Some(c) if "+#./;?&".contains(c) => (Some(c), &inner[1..]),
                _ => (None, inner),
            };
            let mut specs = vec![];
            for v in vars.split(',') {
                let (v, explode) = match v.strip_suffix('*') {
                    Some(v) => (v, true),
                    None => (v, false),
                };
                let (name, prefix) = match v.split_once(':') {
                    Some((n, p)) => (
                        n,
                        Some(p.parse::<usize>().map_err(|_| {
                            CsvwError::UriTemplateError(
                                t.to_string(),
                                format!("invalid prefix {p}"),
                            )
                        })?),
                    ),
                    None => (v, None),
                };
                if name.is_empty() {
                    return Err(CsvwError::UriTemplateError(
                        t.to_string(),
                        "empty variable name".to_string(),
                    ));
                }
                specs.push(VarSpec {
                    name: crate::loader::percent_decode(name),
                    prefix,
                    explode,
                });
            }
            parts.push(Part::Expr { op, vars: specs });
            rest = &rest[i + j + 1..];
        } else {
            parts.push(Part::Lit(rest.to_string()));
            rest = "";
        }
    }
    Ok(parts)
}

/// The value of a template variable.
#[derive(Debug, Clone)]
pub enum VarValue {
    /// Known when generating the update, None means undefined.
    Const(Option<String>),
    /// A SPARQL variable holding the string value, unbound if the value is null.
    /// list is true if the value is a list joined with ",".
    Var {
        var: String,
        list: bool,
        always_bound: bool,
    },
}

pub trait VarResolver {
    fn resolve_var(&self, name: &str) -> VarValue;
}

struct OpSpec {
    first: &'static str,
    sep: &'static str,
    named: bool,
    ifemp: &'static str,
    reserved: bool,
}

fn op_spec(op: Option<char>) -> OpSpec {
    match op {
        None => OpSpec {
            first: "",
            sep: ",",
            named: false,
            ifemp: "",
            reserved: false,
        },
        Some('+') => OpSpec {
            first: "",
            sep: ",",
            named: false,
            ifemp: "",
            reserved: true,
        },
        Some('.') => OpSpec {
            first: ".",
            sep: ".",
            named: false,
            ifemp: "",
            reserved: false,
        },
        Some('/') => OpSpec {
            first: "/",
            sep: "/",
            named: false,
            ifemp: "",
            reserved: false,
        },
        Some(';') => OpSpec {
            first: ";",
            sep: ";",
            named: true,
            ifemp: "",
            reserved: false,
        },
        Some('?') => OpSpec {
            first: "?",
            sep: "&",
            named: true,
            ifemp: "=",
            reserved: false,
        },
        Some('&') => OpSpec {
            first: "&",
            sep: "&",
            named: true,
            ifemp: "=",
            reserved: false,
        },
        Some('#') => OpSpec {
            first: "#",
            sep: ",",
            named: false,
            ifemp: "",
            reserved: true,
        },
        _ => unreachable!(),
    }
}

const RESERVED: &str = ":/?#[]@!$&'()*+,;=";

pub fn encode_const(s: &str, reserved: bool) -> String {
    let mut out = String::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        let ch = c as char;
        if ch.is_ascii_alphanumeric() || "-._~".contains(ch) || (reserved && RESERVED.contains(ch))
        {
            out.push(ch);
        } else if reserved
            && ch == '%'
            && i + 2 < bytes.len()
            && bytes[i + 1].is_ascii_hexdigit()
            && bytes[i + 2].is_ascii_hexdigit()
        {
            out.push('%');
        } else {
            out.push_str(&format!("%{c:02X}"));
        }
        i += 1;
    }
    out
}

/// SPARQL expression percent-encoding a string expression.
fn encode_sparql(e: &str, reserved: bool, list: bool) -> String {
    let mut out = format!("ENCODE_FOR_URI({e})");
    if reserved {
        for c in RESERVED.chars() {
            out = format!(
                "REPLACE({out}, {}, {})",
                lit(&format!("%{:02X}", c as u32)),
                lit(&c.to_string())
            );
        }
        out = format!("REPLACE({out}, \"%25([0-9A-Fa-f]{{2}})\", \"%$1\")");
    } else if list {
        out = format!("REPLACE({out}, \"%2C\", \",\")");
    }
    out
}

/// A string valued result of expanding (a part of) a template.
#[derive(Debug, Clone)]
pub enum StrExpr {
    Const(String),
    Sparql(String),
}

fn concat(parts: Vec<StrExpr>) -> StrExpr {
    let mut merged: Vec<StrExpr> = vec![];
    for p in parts {
        match (merged.last_mut(), p) {
            (Some(StrExpr::Const(a)), StrExpr::Const(b)) => a.push_str(&b),
            (_, p) => merged.push(p),
        }
    }
    merged.retain(|x| !matches!(x, StrExpr::Const(s) if s.is_empty()));
    match merged.len() {
        0 => StrExpr::Const(String::new()),
        1 => merged.pop().unwrap(),
        _ => StrExpr::Sparql(format!(
            "CONCAT({})",
            merged
                .iter()
                .map(|x| match x {
                    StrExpr::Const(s) => lit(s),
                    StrExpr::Sparql(s) => s.clone(),
                })
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn expand_expression(op: Option<char>, vars: &[VarSpec], r: &dyn VarResolver) -> StrExpr {
    let spec = op_spec(op);
    // Each piece is (always defined, sep + piece)
    let mut pieces: Vec<(bool, StrExpr, Option<String>)> = vec![];
    for v in vars {
        match r.resolve_var(&v.name) {
            VarValue::Const(None) => {}
            VarValue::Const(Some(s)) => {
                let s = match v.prefix {
                    Some(n) => s.chars().take(n).collect(),
                    None => s,
                };
                let enc = encode_const(&s, spec.reserved);
                let piece = if spec.named {
                    if s.is_empty() {
                        format!("{}{}", v.name, spec.ifemp)
                    } else {
                        format!("{}={enc}", v.name)
                    }
                } else {
                    enc
                };
                pieces.push((true, StrExpr::Const(format!("{}{piece}", spec.sep)), None));
            }
            VarValue::Var {
                var,
                list,
                always_bound,
            } => {
                let value = match v.prefix {
                    Some(n) => format!("SUBSTR({var}, 1, {n})"),
                    None => var.clone(),
                };
                let enc = encode_sparql(&value, spec.reserved, list);
                let piece = if spec.named {
                    format!(
                        "IF({var} = \"\", {}, CONCAT({}, {enc}))",
                        lit(&format!("{}{}", v.name, spec.ifemp)),
                        lit(&format!("{}=", v.name))
                    )
                } else {
                    enc
                };
                let piece = concat(vec![
                    StrExpr::Const(spec.sep.to_string()),
                    StrExpr::Sparql(piece),
                ]);
                pieces.push((
                    always_bound,
                    piece,
                    if always_bound { None } else { Some(var) },
                ));
            }
        }
    }
    if pieces.is_empty() {
        return StrExpr::Const(String::new());
    }
    let any_always = pieces.iter().any(|(a, _, _)| *a);
    let joined = concat(
        pieces
            .into_iter()
            .map(|(always, p, var)| {
                if always {
                    p
                } else {
                    let p = match p {
                        StrExpr::Const(s) => lit(&s),
                        StrExpr::Sparql(s) => s,
                    };
                    StrExpr::Sparql(format!("IF(BOUND({}), {p}, \"\")", var.unwrap()))
                }
            })
            .collect(),
    );
    let strip = spec.sep.chars().count() + 1;
    match joined {
        StrExpr::Const(s) => {
            let stripped: String = s.chars().skip(strip - 1).collect();
            StrExpr::Const(format!("{}{stripped}", spec.first))
        }
        StrExpr::Sparql(j) => {
            let stripped = format!("SUBSTR({j}, {strip})");
            let with_first = if spec.first.is_empty() {
                stripped
            } else {
                format!("CONCAT({}, {stripped})", lit(spec.first))
            };
            if any_always {
                StrExpr::Sparql(with_first)
            } else {
                StrExpr::Sparql(format!("IF({j} = \"\", \"\", {with_first})"))
            }
        }
    }
}

pub fn expand(parts: &[Part], r: &dyn VarResolver) -> StrExpr {
    let mut out = vec![];
    for p in parts {
        match p {
            Part::Lit(s) => out.push(StrExpr::Const(s.clone())),
            Part::Expr { op, vars } => out.push(expand_expression(*op, vars, r)),
        }
    }
    concat(out)
}

/// The result of compiling a template into an IRI.
#[derive(Debug, Clone)]
pub enum IriExpr {
    Const(String),
    /// A SPARQL expression evaluating to an IRI.
    Sparql(String),
}

fn has_scheme_or_prefix(s: &str) -> Option<usize> {
    let i = s.find(':')?;
    if s[..i].contains(['/', '?', '#']) || i == 0 {
        return None;
    }
    Some(i)
}

/// Compiles a URI template into an expression for an absolute IRI. The expanded template has
/// prefixes expanded and is resolved against base (the url of the table).
/// The bindings are used for intermediate variables and the prefix table lookup.
pub fn compile_iri(
    template: &str,
    r: &dyn VarResolver,
    base: &str,
    bindings: &mut Bindings,
) -> Result<IriExpr, CsvwError> {
    let parts = parse_template(template)?;
    let expanded = expand(&parts, r);
    match expanded {
        StrExpr::Const(s) => {
            if has_scheme_or_prefix(&s).is_some() {
                if let Some(e) = expand_prefixed(&s) {
                    return Ok(IriExpr::Const(e));
                }
                return Ok(IriExpr::Const(s));
            }
            Ok(IriExpr::Const(resolve(base, &s).unwrap_or(s)))
        }
        StrExpr::Sparql(e) => {
            // The literal prefix of the template decides how the result is resolved.
            let leading = match parts.first() {
                Some(Part::Lit(l)) => l.clone(),
                _ => String::new(),
            };
            if !leading.is_empty() {
                let rest_parts = &parts[1..];
                let rest = match expand(rest_parts, r) {
                    StrExpr::Const(c) => lit(&c),
                    StrExpr::Sparql(s) => s,
                };
                let new_leading = if has_scheme_or_prefix(&leading).is_some() {
                    expand_prefixed(&leading).unwrap_or(leading.clone())
                } else {
                    match resolve(base, &leading) {
                        Ok(l) => l,
                        Err(_) => leading.clone(),
                    }
                };
                return Ok(IriExpr::Sparql(format!(
                    "IRI(CONCAT({}, {rest}))",
                    lit(&new_leading)
                )));
            }
            // The template starts with an expression, resolve when the update runs.
            let ev = bindings.bind(&e);
            let pfx = bindings.new_var("pfx");
            let ns = bindings.new_var("ns");
            bindings.add_pattern(format!("BIND(STRBEFORE({ev}, \":\") AS {pfx})"));
            bindings.add_prefix_lookup(&pfx, &ns);
            let (no_frag, no_query, dir, origin, scheme) = base_parts(base);
            let relative = format!(
                "IF(REGEX({ev}, \"^[A-Za-z][A-Za-z0-9+.-]*:\"), {ev}, IF({ev} = \"\", {}, IF(STRSTARTS({ev}, \"#\"), CONCAT({}, {ev}), IF(STRSTARTS({ev}, \"?\"), CONCAT({}, {ev}), IF(STRSTARTS({ev}, \"//\"), CONCAT({}, {ev}), IF(STRSTARTS({ev}, \"/\"), CONCAT({}, {ev}), CONCAT({}, {ev})))))))",
                lit(&no_frag),
                lit(&no_frag),
                lit(&no_query),
                lit(&scheme),
                lit(&origin),
                lit(&dir),
            );
            Ok(IriExpr::Sparql(format!(
                "IRI(IF(BOUND({ns}) && !STRSTARTS(STRAFTER({ev}, \":\"), \"//\"), CONCAT({ns}, STRAFTER({ev}, \":\")), {relative}))"
            )))
        }
    }
}

/// (without fragment, without query and fragment, directory, origin, scheme with colon)
fn base_parts(base: &str) -> (String, String, String, String, String) {
    let no_frag = base.split('#').next().unwrap().to_string();
    let no_query = no_frag.split('?').next().unwrap().to_string();
    let dir = match no_query.rfind('/') {
        Some(i) => no_query[..=i].to_string(),
        None => no_query.clone(),
    };
    let scheme = match base.find(':') {
        Some(i) => base[..=i].to_string(),
        None => String::new(),
    };
    let origin = match no_query.find("://") {
        Some(i) => {
            let after = &no_query[i + 3..];
            match after.find('/') {
                Some(j) => no_query[..i + 3 + j].to_string(),
                None => no_query.clone(),
            }
        }
        None => scheme.clone(),
    };
    (no_frag, no_query, dir, origin, scheme)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct R;
    impl VarResolver for R {
        fn resolve_var(&self, name: &str) -> VarValue {
            match name {
                "a" => VarValue::Const(Some("x y".to_string())),
                "b" => VarValue::Var {
                    var: "?b".to_string(),
                    list: false,
                    always_bound: false,
                },
                _ => VarValue::Const(None),
            }
        }
    }

    #[test]
    fn constants() {
        let p = parse_template("http://ex.org/{a}{#a}{?a,zz}").unwrap();
        match expand(&p, &R) {
            StrExpr::Const(s) => assert_eq!(s, "http://ex.org/x%20y#x%20y?a=x%20y"),
            _ => panic!(),
        }
        let p = parse_template("{zz}").unwrap();
        match expand(&p, &R) {
            StrExpr::Const(s) => assert_eq!(s, ""),
            _ => panic!(),
        }
    }

    #[test]
    fn resolve_const() {
        let mut b = Bindings::default();
        match compile_iri("#{a}", &R, "http://ex.org/t.csv", &mut b).unwrap() {
            IriExpr::Const(s) => assert_eq!(s, "http://ex.org/t.csv#x%20y"),
            _ => panic!(),
        }
        match compile_iri("schema:{a}", &R, "http://ex.org/t.csv", &mut b).unwrap() {
            IriExpr::Const(s) => assert_eq!(s, "http://schema.org/x%20y"),
            _ => panic!(),
        }
    }
}

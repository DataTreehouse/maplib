//! Parsing cells into values according to the datatype and format annotations of a column, see
//! <https://www.w3.org/TR/tabular-data-model/#parsing-cells>.

use crate::metadata::Datatype;
use crate::vocab::*;
use oxrdf::NamedNode;
use regex::Regex;
use representation::{BaseRDFNodeType, SeriesBuilder};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

/// A parsed value. If the value is not valid, lexical is the original string and it is output as
/// a string literal.
#[derive(Debug, Clone, PartialEq)]
pub struct Val {
    pub lexical: String,
    pub valid: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CellValue {
    Null,
    Single(Val),
    List(Vec<Option<Val>>),
}

/// The annotations of a column that determine how its cells are parsed.
#[derive(Debug, Clone)]
pub struct CellParsing {
    pub datatype: Datatype,
    pub default: String,
    pub null: Vec<String>,
    pub required: bool,
    pub separator: Option<String>,
}

static REGEX_CACHE: LazyLock<Mutex<HashMap<String, Option<Regex>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn cached_regex(pattern: &str) -> Option<Regex> {
    let mut c = REGEX_CACHE.lock().unwrap();
    c.entry(pattern.to_string())
        .or_insert_with(|| Regex::new(pattern).ok())
        .clone()
}

fn full_match(pattern: &str, s: &str) -> bool {
    match cached_regex(&format!("^(?:{pattern})$")) {
        Some(r) => r.is_match(s),
        None => true,
    }
}

fn normalize_whitespace(s: &str, base: &str) -> String {
    let mut s = s.to_string();
    if !matches!(base, "string" | "json" | "xml" | "html" | "anyAtomicType") {
        s = s.replace(['\r', '\n', '\t'], " ");
    }
    if !matches!(
        base,
        "string" | "json" | "xml" | "html" | "anyAtomicType" | "normalizedString"
    ) {
        s = s
            .split(' ')
            .filter(|x| !x.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
    }
    s
}

/// Parses the string value of a cell. Returns the value and the errors of the cell.
pub fn parse_cell(raw: &str, p: &CellParsing) -> (CellValue, Vec<String>) {
    let base = p.datatype.base.as_str();
    let mut errors = vec![];
    let mut normalized = normalize_whitespace(raw, base);
    if normalized.is_empty() {
        normalized = p.default.clone();
    }
    if let Some(sep) = &p.separator {
        if normalized.is_empty() {
            if p.required {
                errors.push("required cell is empty".to_string());
            }
            return (CellValue::List(vec![]), errors);
        }
        if p.null.contains(&normalized) {
            return (CellValue::Null, errors);
        }
        let mut items = vec![];
        for item in normalized.split(sep.as_str()) {
            let item = if matches!(base, "string" | "anyAtomicType") {
                item.to_string()
            } else {
                item.trim().to_string()
            };
            let item = if item.is_empty() {
                p.default.clone()
            } else {
                item
            };
            if p.null.contains(&item) {
                items.push(None);
                continue;
            }
            let v = parse_value(&item, &p.datatype, &mut errors);
            items.push(Some(v));
        }
        (CellValue::List(items), errors)
    } else {
        if p.null.contains(&normalized) {
            if p.required {
                errors.push("required cell is null".to_string());
            }
            return (CellValue::Null, errors);
        }
        let v = parse_value(&normalized, &p.datatype, &mut errors);
        (CellValue::Single(v), errors)
    }
}

fn invalid(s: &str, errors: &mut Vec<String>, e: String) -> Val {
    errors.push(e);
    Val {
        lexical: s.to_string(),
        valid: false,
    }
}

/// Checks the format annotation of a datatype, returning a description of the problem when it is
/// invalid. Invalid formats are ignored (with a warning) by the metadata parser.
pub fn format_problem(dt: &Datatype) -> Option<String> {
    let base = dt.base.as_str();
    let format = dt.format.as_ref()?;
    if is_numeric_type(base) {
        let nf = number_format(Some(format));
        if let Value::Object(o) = format {
            for k in ["decimalChar", "groupChar", "pattern"] {
                if let Some(v) = o.get(k) {
                    if !v.is_string() {
                        return Some(format!("the {k} of a number format must be a string"));
                    }
                }
            }
        } else if !format.is_string() {
            return Some(format!("invalid number format {format}"));
        }
        if let Some(p) = &nf.pattern {
            match ldml_number_regex(p, &nf) {
                Some(re) if cached_regex(&format!("^(?:{re})$")).is_some() => {}
                _ => return Some(format!("invalid number pattern {p}")),
            }
        }
        None
    } else if base == "boolean" {
        match format {
            Value::String(f) if f.split('|').count() == 2 => None,
            _ => Some(format!(
                "invalid boolean format {format}, expected \"true|false\""
            )),
        }
    } else if matches!(base, "date" | "dateTime" | "dateTimeStamp" | "time") {
        match format_regex(Some(format)) {
            Some(p) => match date_pattern_regex(&p) {
                Some(re) if cached_regex(&format!("^{re}$")).is_some() => None,
                _ => Some(format!("invalid date and time format {p}")),
            },
            None => Some(format!("invalid date and time format {format}")),
        }
    } else if matches!(base, "xml" | "html" | "json") {
        None
    } else {
        match format_regex(Some(format)) {
            Some(p) if cached_regex(&format!("^(?:{p})$")).is_some() => None,
            _ => Some(format!("invalid regular expression format {format}")),
        }
    }
}

/// Checks that maplib's native representation of the datatype can hold the value (for instance,
/// dates are stored without time zones), so that STRDT in the generated update does not fail.
fn maplib_can_store(lexical: &str, datatype_iri: &str) -> bool {
    let Ok(nn) = NamedNode::new(datatype_iri) else {
        return false;
    };
    let mut builder = SeriesBuilder::new(&BaseRDFNodeType::Literal(nn, None));
    builder.parse_literal(lexical, None).is_ok()
}

fn parse_value(s: &str, dt: &Datatype, errors: &mut Vec<String>) -> Val {
    let base = dt.base.as_str();
    let parsed = if is_numeric_type(base) {
        parse_number(s, base, dt.format.as_ref())
    } else if base == "boolean" {
        parse_boolean(s, dt.format.as_ref())
    } else if matches!(base, "date" | "dateTime" | "dateTimeStamp" | "time") {
        parse_date_time(s, base, dt.format.as_ref())
    } else {
        parse_other(s, base, dt.format.as_ref())
    };
    let lexical = match parsed {
        Ok(l) => l,
        Err(e) => return invalid(s, errors, format!("{s} is not a valid {base}: {e}")),
    };
    if !maplib_can_store(&lexical, &dt.iri()) {
        return invalid(
            s,
            errors,
            format!(
                "{s} is a valid {base}, but maplib can not store it with the datatype {}, so it is output as a string",
                dt.iri()
            ),
        );
    }
    // Length constraints
    let len = if is_binary_type(base) {
        binary_length(&lexical, base)
    } else {
        lexical.chars().count()
    };
    if let Some(l) = dt.length {
        if len != l {
            return invalid(s, errors, format!("{s} does not have length {l}"));
        }
    }
    if let Some(l) = dt.min_length {
        if len < l {
            return invalid(s, errors, format!("{s} is shorter than {l}"));
        }
    }
    if let Some(l) = dt.max_length {
        if len > l {
            return invalid(s, errors, format!("{s} is longer than {l}"));
        }
    }
    // Value constraints
    let checks: [(&Option<String>, fn(Ordering) -> bool, &str); 4] = [
        (&dt.min_inclusive, |o| o != Ordering::Less, "minimum"),
        (&dt.max_inclusive, |o| o != Ordering::Greater, "maximum"),
        (
            &dt.min_exclusive,
            |o| o == Ordering::Greater,
            "minExclusive",
        ),
        (&dt.max_exclusive, |o| o == Ordering::Less, "maxExclusive"),
    ];
    for (bound, ok, name) in checks {
        if let Some(b) = bound {
            if let Some(o) = compare_bound(base, &lexical, b) {
                if !ok(o) {
                    return invalid(s, errors, format!("{s} violates the {name} {b}"));
                }
            }
        }
    }
    Val {
        lexical,
        valid: true,
    }
}

fn binary_length(lexical: &str, base: &str) -> usize {
    if base == "hexBinary" {
        lexical.len() / 2
    } else {
        let stripped: String = lexical.chars().filter(|c| !c.is_whitespace()).collect();
        let padding = stripped.chars().rev().take_while(|c| *c == '=').count();
        stripped.len() / 4 * 3 - padding
    }
}

/// Compares two values of the datatype, a and b being canonical (or XSD) lexical forms.
pub fn compare_bound(base: &str, a: &str, b: &str) -> Option<Ordering> {
    if is_numeric_type(base) {
        let a = parse_xsd_number(a)?;
        let b = parse_xsd_number(b)?;
        a.partial_cmp(&b)
    } else if is_date_time_type(base) || is_duration_type(base) {
        Some(a.cmp(b))
    } else {
        None
    }
}

fn parse_xsd_number(s: &str) -> Option<f64> {
    match s {
        "INF" => Some(f64::INFINITY),
        "-INF" => Some(f64::NEG_INFINITY),
        "NaN" => Some(f64::NAN),
        s => s.parse::<f64>().ok(),
    }
}

// ---------------------------------------------------------------------------------------------
// Numbers

struct NumberFormat {
    decimal_char: String,
    group_char: Option<String>,
    pattern: Option<String>,
}

fn number_format(format: Option<&Value>) -> NumberFormat {
    let mut nf = NumberFormat {
        decimal_char: ".".to_string(),
        group_char: None,
        pattern: None,
    };
    match format {
        Some(Value::String(p)) => nf.pattern = Some(p.clone()),
        Some(Value::Object(o)) => {
            if let Some(Value::String(d)) = o.get("decimalChar") {
                nf.decimal_char = d.clone();
            }
            if let Some(Value::String(g)) = o.get("groupChar") {
                nf.group_char = Some(g.clone());
            }
            if let Some(Value::String(p)) = o.get("pattern") {
                nf.pattern = Some(p.clone());
            }
        }
        _ => {}
    }
    nf
}

/// Builds a regex from an LDML number pattern (subset: 0 # . , E + - % ‰ and literal
/// prefixes / suffixes).
fn ldml_number_regex(pattern: &str, nf: &NumberFormat) -> Option<String> {
    let positive = pattern.split(';').next()?;
    let dec = nf.decimal_char.as_str();
    let grp = nf.group_char.clone().unwrap_or(",".to_string());
    let chars: Vec<char> = positive.chars().collect();
    let is_num_char =
        |c: char| c == '0' || c == '#' || dec.starts_with(c) || grp.starts_with(c) || c == 'E';
    let start = chars.iter().position(|c| is_num_char(*c))?;
    let end = chars.len()
        - chars
            .iter()
            .rev()
            .position(|c| is_num_char(*c) || c.is_ascii_digit())?;
    let prefix: String = chars[..start].iter().collect();
    let number: String = chars[start..end].iter().collect();
    let suffix: String = chars[end..].iter().collect();
    let (mantissa, exponent) = match number.split_once('E') {
        Some((m, e)) => (m.to_string(), Some(e.to_string())),
        None => (number.clone(), None),
    };
    let (int_part, frac_part) = match mantissa.split_once(dec) {
        Some((i, f)) => (i.to_string(), Some(f.to_string())),
        None => (mantissa.clone(), None),
    };
    let int_digits: String = int_part
        .chars()
        .filter(|c| *c == '0' || *c == '#')
        .collect();
    let min_int = int_digits.chars().filter(|c| *c == '0').count();
    let groups: Vec<&str> = int_part.split(grp.as_str()).collect();
    let g = regex::escape(&grp);
    let int_re = if groups.len() > 1 {
        let primary = groups.last().unwrap().len();
        let secondary = if groups.len() > 2 {
            groups[groups.len() - 2].len()
        } else {
            primary
        };
        format!(
            r"(?:\d{{1,{secondary}}}(?:{g}\d{{{secondary}}})*{g}\d{{{primary}}}|\d{{{},{primary}}})",
            min_int.max(1).min(primary)
        )
    } else if min_int == 0 {
        r"\d*".to_string()
    } else {
        format!(r"\d{{{min_int},}}")
    };
    let frac_re = match &frac_part {
        None => String::new(),
        Some(f) => {
            let min_f = f.chars().filter(|c| *c == '0').count();
            let max_f = f.chars().filter(|c| *c == '0' || *c == '#').count();
            let d = regex::escape(dec);
            if f.contains(grp.as_str()) {
                // Fractional grouping, the digit count is checked by fraction_digits_ok
                let size = f
                    .split(grp.as_str())
                    .next()
                    .unwrap_or("")
                    .chars()
                    .count()
                    .max(1);
                let g = regex::escape(&grp);
                if min_f > 0 {
                    format!(r"{d}(?:\d{{{size}}}{g})*\d{{1,{size}}}")
                } else {
                    format!(r"(?:{d}(?:\d{{{size}}}{g})*\d{{1,{size}}})?")
                }
            } else if min_f > 0 {
                format!(r"{d}\d{{{min_f},{max_f}}}")
            } else {
                format!(r"(?:{d}\d{{0,{max_f}}})?")
            }
        }
    };
    let exp_re = match &exponent {
        None => String::new(),
        Some(e) => {
            let n = e.chars().filter(|c| *c == '0').count().max(1);
            if e.starts_with('+') {
                format!(r"E[+-]\d{{{n},}}")
            } else {
                format!(r"E-?\d{{{n},}}")
            }
        }
    };
    let has_sign = prefix.contains('+') || prefix.contains('-');
    let sign_re = if has_sign { "" } else { "[+-]?" };
    Some(format!(
        "(?:{sign_re}{p}|{p}{sign_re}){int_re}{frac_re}{exp_re}{s}",
        p = regex::escape(&prefix),
        s = regex::escape(&suffix)
    ))
}

/// Checks the number of fraction digits when the fraction of the pattern is grouped, which the
/// regex of the pattern does not.
fn fraction_digits_ok(s: &str, pattern: &str, nf: &NumberFormat) -> bool {
    let grp = nf.group_char.clone().unwrap_or(",".to_string());
    let dec = nf.decimal_char.as_str();
    let positive = pattern.split(';').next().unwrap_or("");
    let Some((_, f)) = positive.split_once(dec) else {
        return true;
    };
    let f = f.split('E').next().unwrap_or("");
    if !f.contains(grp.as_str()) {
        return true;
    }
    let min_f = f.chars().filter(|c| *c == '0').count();
    let max_f = f.chars().filter(|c| *c == '0' || *c == '#').count();
    let n = match s.split_once(dec) {
        Some((_, frac)) => frac
            .split('E')
            .next()
            .unwrap_or("")
            .chars()
            .filter(|c| c.is_ascii_digit())
            .count(),
        None => 0,
    };
    n >= min_f && n <= max_f
}

fn parse_number(s: &str, base: &str, format: Option<&Value>) -> Result<String, String> {
    let nf = number_format(format);
    if matches!(s, "NaN" | "INF" | "-INF") {
        if is_decimal_type(base) {
            return Err("special values are not allowed for decimals".to_string());
        }
        return Ok(s.to_string());
    }
    if let Some(p) = &nf.pattern {
        if let Some(re) = ldml_number_regex(p, &nf) {
            if !full_match(&re, s) || !fraction_digits_ok(s, p, &nf) {
                return Err(format!("does not match the pattern {p}"));
            }
        }
    }
    let mut t = s.to_string();
    if let Some(g) = &nf.group_char {
        if t.contains(&format!("{g}{g}")) {
            return Err("contains consecutive group characters".to_string());
        }
        t = t.replace(g.as_str(), "");
    }
    // Strip literal prefixes and suffixes of the pattern
    let mut factor = 1.0;
    if t.ends_with('%') || t.starts_with('%') {
        factor = 100.0;
        t = t.replace('%', "");
    } else if t.contains('‰') {
        factor = 1000.0;
        t = t.replace('‰', "");
    }
    if nf.decimal_char != "." {
        if t.contains('.') && nf.group_char.as_deref() != Some(".") {
            return Err("contains '.' which is not the decimal character".to_string());
        }
        t = t.replace(nf.decimal_char.as_str(), ".");
    }
    if nf.pattern.is_some() {
        t = t
            .chars()
            .filter(|c| c.is_ascii_digit() || matches!(c, '.' | 'E' | 'e' | '+' | '-'))
            .collect();
    }
    if !full_match(r"[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?", &t) {
        return Err("not a number".to_string());
    }
    let has_exp = t.contains(['e', 'E']);
    let has_dec = t.contains('.');
    if is_integer_type(base) && has_dec {
        return Err("integers can not contain a decimal character".to_string());
    }
    if is_decimal_type(base) && has_exp {
        return Err("decimals can not have an exponent".to_string());
    }
    let canonical = if factor != 1.0 || (is_decimal_type(base) && has_exp) {
        let f: f64 = t.parse().map_err(|_| "not a number".to_string())?;
        let v = f / factor;
        if is_integer_type(base) {
            if v.fract() != 0.0 {
                return Err("not an integer".to_string());
            }
            format!("{}", v as i128)
        } else if is_decimal_type(base) {
            canonical_decimal(&format!("{v}"))
        } else {
            canonical_double(v)
        }
    } else if is_integer_type(base) {
        canonical_integer(&t)
    } else if is_decimal_type(base) {
        canonical_decimal(&t)
    } else {
        let f: f64 = t.parse().map_err(|_| "not a number".to_string())?;
        canonical_double(f)
    };
    check_integer_range(base, &canonical)?;
    Ok(canonical)
}

fn canonical_integer(t: &str) -> String {
    let (neg, digits) = match t.strip_prefix('-') {
        Some(d) => (true, d),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        "0".to_string()
    } else if neg {
        format!("-{digits}")
    } else {
        digits.to_string()
    }
}

fn canonical_decimal(t: &str) -> String {
    let (neg, rest) = match t.strip_prefix('-') {
        Some(d) => (true, d),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let (i, f) = rest.split_once('.').unwrap_or((rest, ""));
    let i = i.trim_start_matches('0');
    let f = f.trim_end_matches('0');
    let i = if i.is_empty() { "0" } else { i };
    let out = if f.is_empty() {
        i.to_string()
    } else {
        format!("{i}.{f}")
    };
    if neg && out != "0" {
        format!("-{out}")
    } else {
        out
    }
}

fn canonical_double(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f > 0.0 {
            "INF".to_string()
        } else {
            "-INF".to_string()
        }
    } else {
        let s = format!("{f:E}");
        let (m, e) = s.split_once('E').unwrap();
        let m = if m.contains('.') {
            m.to_string()
        } else {
            format!("{m}.0")
        };
        format!("{m}E{e}")
    }
}

fn check_integer_range(base: &str, canonical: &str) -> Result<(), String> {
    let (min, max): (Option<i128>, Option<i128>) = match base {
        "long" => (Some(i64::MIN as i128), Some(i64::MAX as i128)),
        "int" => (Some(i32::MIN as i128), Some(i32::MAX as i128)),
        "short" => (Some(i16::MIN as i128), Some(i16::MAX as i128)),
        "byte" => (Some(i8::MIN as i128), Some(i8::MAX as i128)),
        "nonNegativeInteger" => (Some(0), None),
        "positiveInteger" => (Some(1), None),
        "unsignedLong" => (Some(0), Some(u64::MAX as i128)),
        "unsignedInt" => (Some(0), Some(u32::MAX as i128)),
        "unsignedShort" => (Some(0), Some(u16::MAX as i128)),
        "unsignedByte" => (Some(0), Some(u8::MAX as i128)),
        "nonPositiveInteger" => (None, Some(0)),
        "negativeInteger" => (None, Some(-1)),
        _ => (None, None),
    };
    if min.is_none() && max.is_none() {
        return Ok(());
    }
    let v: i128 = match canonical.parse() {
        Ok(v) => v,
        Err(_) => {
            // Too large for i128, only unbounded directions are ok
            let neg = canonical.starts_with('-');
            if (neg && min.is_some()) || (!neg && max.is_some()) {
                return Err(format!("out of range for {base}"));
            }
            return Ok(());
        }
    };
    if min.is_some_and(|m| v < m) || max.is_some_and(|m| v > m) {
        return Err(format!("out of range for {base}"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Booleans

fn parse_boolean(s: &str, format: Option<&Value>) -> Result<String, String> {
    if let Some(Value::String(f)) = format {
        if let Some((t, f)) = f.split_once('|') {
            if s == t {
                return Ok("true".to_string());
            } else if s == f {
                return Ok("false".to_string());
            } else {
                return Err(format!("expected {t} or {f}"));
            }
        }
    }
    match s {
        "true" | "1" => Ok("true".to_string()),
        "false" | "0" => Ok("false".to_string()),
        _ => Err("not a boolean".to_string()),
    }
}

// ---------------------------------------------------------------------------------------------
// Dates and times

const TZ_RE: &str = r"(Z|[+-]\d\d:\d\d)";

fn xsd_date_time_regex(base: &str) -> String {
    match base {
        "date" => format!(r"-?\d{{4,}}-\d\d-\d\d{TZ_RE}?"),
        "dateTime" => format!(r"-?\d{{4,}}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?{TZ_RE}?"),
        "dateTimeStamp" => format!(r"-?\d{{4,}}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?{TZ_RE}"),
        "time" => format!(r"\d\d:\d\d:\d\d(\.\d+)?{TZ_RE}?"),
        "gYear" => format!(r"-?\d{{4,}}{TZ_RE}?"),
        "gYearMonth" => format!(r"-?\d{{4,}}-\d\d{TZ_RE}?"),
        "gMonth" => format!(r"--\d\d{TZ_RE}?"),
        "gMonthDay" => format!(r"--\d\d-\d\d{TZ_RE}?"),
        "gDay" => format!(r"---\d\d{TZ_RE}?"),
        _ => ".*".to_string(),
    }
}

/// Converts a UAX35 date / time pattern into a regex with named groups.
fn date_pattern_regex(pattern: &str) -> Option<String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let mut n = 1;
        while i + n < chars.len() && chars[i + n] == c {
            n += 1;
        }
        let part = match (c, n) {
            ('y', 4) => r"(?P<year>-?\d{4})".to_string(),
            ('M', 2) => r"(?P<month>\d{2})".to_string(),
            ('M', 1) => r"(?P<month>\d{1,2})".to_string(),
            ('d', 2) => r"(?P<day>\d{2})".to_string(),
            ('d', 1) => r"(?P<day>\d{1,2})".to_string(),
            ('H', 2) => r"(?P<hour>\d{2})".to_string(),
            ('H', 1) => r"(?P<hour>\d{1,2})".to_string(),
            ('m', 2) => r"(?P<minute>\d{2})".to_string(),
            ('m', 1) => r"(?P<minute>\d{1,2})".to_string(),
            ('s', 2) => r"(?P<second>\d{2})".to_string(),
            ('s', 1) => r"(?P<second>\d{1,2})".to_string(),
            ('S', n) => format!(r"(?P<fraction>\d{{1,{n}}})"),
            ('X', 1) => r"(?P<tz>Z|[+-]\d\d(?:\d\d)?)".to_string(),
            ('X', 2) => r"(?P<tz>Z|[+-]\d\d\d\d)".to_string(),
            ('X', 3) => r"(?P<tz>Z|[+-]\d\d:\d\d)".to_string(),
            ('x', 1) => r"(?P<tz>[+-]\d\d(?:\d\d)?)".to_string(),
            ('x', 2) => r"(?P<tz>[+-]\d\d\d\d)".to_string(),
            ('x', 3) => r"(?P<tz>[+-]\d\d:\d\d)".to_string(),
            (c, _) if c.is_ascii_alphabetic() && c != 'T' => return None,
            (c, n) => regex::escape(&c.to_string().repeat(n)),
        };
        out.push_str(&part);
        i += n;
    }
    Some(out)
}

fn canonical_tz(tz: &str) -> String {
    if tz == "Z" {
        return "Z".to_string();
    }
    let sign = &tz[..1];
    let digits: String = tz[1..].chars().filter(|c| c.is_ascii_digit()).collect();
    let (h, m) = if digits.len() >= 4 {
        (&digits[..2], &digits[2..4])
    } else {
        (&digits[..2], "00")
    };
    if h == "00" && m == "00" {
        "Z".to_string()
    } else {
        format!("{sign}{h}:{m}")
    }
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

fn check_date(year: &str, month: &str, day: &str) -> Result<(), String> {
    let y: i64 = year.parse().map_err(|_| "invalid year")?;
    let m: u32 = month.parse().map_err(|_| "invalid month")?;
    let d: u32 = day.parse().map_err(|_| "invalid day")?;
    if !(1..=12).contains(&m) || d < 1 || d > days_in_month(y, m) {
        return Err("invalid date".to_string());
    }
    Ok(())
}

fn check_time(h: &str, m: &str, s: &str) -> Result<(), String> {
    let h: u32 = h.parse().map_err(|_| "invalid hour")?;
    let m: u32 = m.parse().map_err(|_| "invalid minute")?;
    let s: f64 = s.parse().map_err(|_| "invalid second")?;
    if h > 24 || m > 59 || s >= 60.0 || (h == 24 && (m != 0 || s != 0.0)) {
        return Err("invalid time".to_string());
    }
    Ok(())
}

fn parse_date_time(s: &str, base: &str, format: Option<&Value>) -> Result<String, String> {
    let pattern = match format {
        Some(Value::String(p)) => Some(p.clone()),
        Some(Value::Object(o)) => o
            .get("pattern")
            .and_then(|x| x.as_str().map(|x| x.to_string())),
        _ => None,
    };
    let regex = pattern.as_ref().and_then(|p| date_pattern_regex(p));
    let Some(re) = regex else {
        // XSD lexical form
        if !full_match(&xsd_date_time_regex(base), s) {
            return Err("not in the XSD lexical form".to_string());
        }
        let caps = cached_regex(
            r"^(?P<year>-?\d{4,})-(?P<month>\d\d)-(?P<day>\d\d)|(?P<hour>\d\d):(?P<minute>\d\d):(?P<second>\d\d(\.\d+)?)",
        )
        .unwrap();
        for c in caps.captures_iter(s) {
            if let (Some(y), Some(m), Some(d)) = (c.name("year"), c.name("month"), c.name("day")) {
                check_date(y.as_str(), m.as_str(), d.as_str())?;
            }
            if let (Some(h), Some(m), Some(sec)) =
                (c.name("hour"), c.name("minute"), c.name("second"))
            {
                check_time(h.as_str(), m.as_str(), sec.as_str())?;
            }
        }
        let canonical = match s.strip_suffix("+00:00").or(s.strip_suffix("-00:00")) {
            Some(x) => format!("{x}Z"),
            None => s.to_string(),
        };
        return Ok(canonical);
    };
    let Some(r) = cached_regex(&format!("^{re}$")) else {
        return Err("invalid format".to_string());
    };
    let Some(c) = r.captures(s) else {
        return Err(format!("does not match the format {}", pattern.unwrap()));
    };
    let get = |n: &str| c.name(n).map(|x| x.as_str().to_string());
    let date = match (get("year"), get("month"), get("day")) {
        (Some(y), Some(m), Some(d)) => {
            check_date(&y, &m, &d)?;
            Some(format!("{y}-{:0>2}-{:0>2}", m, d))
        }
        _ => None,
    };
    let time = match (get("hour"), get("minute")) {
        (Some(h), Some(m)) => {
            let sec = get("second").unwrap_or("00".to_string());
            check_time(&h, &m, &sec)?;
            let mut t = format!("{:0>2}:{:0>2}:{:0>2}", h, m, sec);
            if let Some(f) = get("fraction") {
                let f = f.trim_end_matches('0');
                if !f.is_empty() {
                    t.push('.');
                    t.push_str(f);
                }
            }
            Some(t)
        }
        _ => None,
    };
    let tz = get("tz").map(|x| canonical_tz(&x)).unwrap_or_default();
    let out = match base {
        "date" => format!("{}{tz}", date.ok_or("no date in the format")?),
        "time" => format!("{}{tz}", time.ok_or("no time in the format")?),
        _ => {
            let d = date.ok_or("no date in the format")?;
            let t = time.unwrap_or("00:00:00".to_string());
            if base == "dateTimeStamp" && tz.is_empty() {
                return Err("dateTimeStamp requires a time zone".to_string());
            }
            format!("{d}T{t}{tz}")
        }
    };
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// Other datatypes

fn format_regex(format: Option<&Value>) -> Option<String> {
    match format {
        Some(Value::String(p)) => Some(p.clone()),
        Some(Value::Object(o)) => o
            .get("pattern")
            .and_then(|x| x.as_str().map(|x| x.to_string())),
        _ => None,
    }
}

fn parse_other(s: &str, base: &str, format: Option<&Value>) -> Result<String, String> {
    let lexical_re = match base {
        "gYear" | "gYearMonth" | "gMonth" | "gMonthDay" | "gDay" => Some(xsd_date_time_regex(base)),
        "duration" => {
            Some(r"-?P((\d+Y)?(\d+M)?(\d+D)?(T(\d+H)?(\d+M)?(\d+(\.\d+)?S)?)?)".to_string())
        }
        "dayTimeDuration" => Some(r"-?P((\d+D)?(T(\d+H)?(\d+M)?(\d+(\.\d+)?S)?)?)".to_string()),
        "yearMonthDuration" => Some(r"-?P((\d+Y)?(\d+M)?)".to_string()),
        "hexBinary" => Some(r"([0-9a-fA-F]{2})*".to_string()),
        "base64Binary" => Some(r"[A-Za-z0-9+/= \n\r\t]*".to_string()),
        "language" => Some(r"[a-zA-Z]{1,8}(-[a-zA-Z0-9]{1,8})*".to_string()),
        "Name" => Some(r"[\p{L}_:][\p{L}\p{N}_:.\-]*".to_string()),
        "NMTOKEN" => Some(r"[\p{L}\p{N}_:.\-]+".to_string()),
        _ => None,
    };
    if let Some(re) = lexical_re {
        if !full_match(&re, s) {
            return Err("not in the lexical space".to_string());
        }
        if is_duration_type(base) && (s.ends_with('P') || s.ends_with('T')) {
            return Err("not in the lexical space".to_string());
        }
    }
    if !matches!(base, "xml" | "html" | "json") {
        if let Some(f) = format_regex(format) {
            if cached_regex(&format!("^(?:{f})$")).is_some() && !full_match(&f, s) {
                return Err(format!("does not match the format {f}"));
            }
        }
    }
    if base == "hexBinary" {
        return Ok(s.to_ascii_uppercase());
    }
    Ok(s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers() {
        assert_eq!(parse_number("+007", "integer", None).unwrap(), "7");
        assert_eq!(parse_number("1.50", "decimal", None).unwrap(), "1.5");
        assert!(parse_number("1.5", "integer", None).is_err());
        let f = serde_json::json!({"groupChar": ",", "decimalChar": "."});
        assert_eq!(
            parse_number("1,234.5", "decimal", Some(&f)).unwrap(),
            "1234.5"
        );
        assert_eq!(parse_number("-25%", "double", None).unwrap(), "-2.5E-1");
        let p = serde_json::json!("#,##0.00");
        assert_eq!(
            parse_number("1,234.50", "decimal", Some(&p)).unwrap(),
            "1234.5"
        );
        assert!(parse_number("1234.5", "decimal", Some(&p)).is_err());
        assert!(parse_number("300", "byte", None).is_err());
    }

    #[test]
    fn dates() {
        let f = serde_json::json!("M/d/yyyy");
        assert_eq!(
            parse_date_time("10/18/2010", "date", Some(&f)).unwrap(),
            "2010-10-18"
        );
        assert!(parse_date_time("13/18/2010", "date", Some(&f)).is_err());
        let f = serde_json::json!("yyyy-MM-ddTHH:mm:ssXXX");
        assert_eq!(
            parse_date_time("2015-03-15T15:02:37+00:00", "dateTime", Some(&f)).unwrap(),
            "2015-03-15T15:02:37Z"
        );
        assert_eq!(
            parse_date_time("2015-03-15", "date", None).unwrap(),
            "2015-03-15"
        );
        assert!(parse_date_time("2015-02-30", "date", None).is_err());
    }
}

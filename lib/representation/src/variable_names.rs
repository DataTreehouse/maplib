//! Variable names for templates.
//!
//! In addition to SPARQL variable names, template variables may be prefixed names such as `ex:name`
//! (`PN_PREFIX? ":" PN_LOCAL` without escapes), so that the columns of a mapped data frame can be
//! named after the properties they hold.

use oxrdf::{Variable, VariableNameParseError};

/// Create a template variable, accepting both SPARQL variable names and prefixed names.
pub fn new_template_variable(name: impl Into<String>) -> Result<Variable, VariableNameParseError> {
    let name = name.into();
    if is_prefixed_variable_name(&name) {
        return Ok(Variable::new_unchecked(name));
    }
    Variable::new(name)
}

/// Is the name a prefixed name, such as `ex:name`, `:name` or `ex:has-part.v2`?
pub fn is_prefixed_variable_name(name: &str) -> bool {
    let Some((prefix, local)) = name.split_once(':') else {
        return false;
    };
    is_pn_prefix(prefix) && is_pn_local(local)
}

fn is_pn_prefix(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        // The prefix may be empty.
        return true;
    };
    is_pn_chars_base(first)
        && chars.all(|c| is_pn_chars(c) || c == '.')
        && !s.ends_with('.')
}

fn is_pn_local(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (is_pn_chars_u(first) || first == ':' || first.is_ascii_digit())
        && chars.all(|c| is_pn_chars(c) || c == '.' || c == ':')
        && !s.ends_with('.')
}

fn is_pn_chars_base(c: char) -> bool {
    matches!(c,
        'A'..='Z'
        | 'a'..='z'
        | '\u{00C0}'..='\u{00D6}'
        | '\u{00D8}'..='\u{00F6}'
        | '\u{00F8}'..='\u{02FF}'
        | '\u{0370}'..='\u{037D}'
        | '\u{037F}'..='\u{1FFF}'
        | '\u{200C}'..='\u{200D}'
        | '\u{2070}'..='\u{218F}'
        | '\u{2C00}'..='\u{2FEF}'
        | '\u{3001}'..='\u{D7FF}'
        | '\u{F900}'..='\u{FDCF}'
        | '\u{FDF0}'..='\u{FFFD}'
        | '\u{10000}'..='\u{EFFFF}')
}

fn is_pn_chars_u(c: char) -> bool {
    c == '_' || is_pn_chars_base(c)
}

fn is_pn_chars(c: char) -> bool {
    is_pn_chars_u(c)
        || matches!(c,
            '-' | '0'..='9' | '\u{00B7}' | '\u{0300}'..='\u{036F}' | '\u{203F}'..='\u{2040}')
}

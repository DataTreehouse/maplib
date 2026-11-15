//! Human-readable rendering of PEG parse errors for SPARQL and OTTR documents.
//!
//! A raw [`peg::error::ParseError`] only carries the furthest byte offset the parser reached
//! and the set of tokens it tried there, printed in the grammar's internal notation
//! (`"\"{\""`, `['0' ..= '9']`, ...). This module turns that into a compiler-style report:
//! the position as line and column, the offending source lines with a caret under the
//! problem, what was found there, what would have been accepted (in user-facing terms),
//! and targeted hints such as unclosed brackets, unterminated strings, undefined prefixes
//! and misspelled keywords.

use peg::error::ParseError;
use peg::str::LineCol;
use std::collections::BTreeSet;
use std::fmt::Write;

/// How prefix declarations are written in the language being parsed, used in hints.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrefixSyntax {
    /// `PREFIX ex: <http://example.com/>`
    Sparql,
    /// `@prefix ex: <http://example.com/> .`
    Turtle,
}

/// What is being parsed, used to phrase the report.
#[derive(Clone, Debug)]
pub struct DiagnosticContext {
    /// Name of the language, e.g. `SPARQL` or `OTTR`.
    pub language: &'static str,
    /// What the input is called, e.g. `query`, `update` or `document`.
    pub input_noun: &'static str,
    /// The file the input came from, if any.
    pub source_name: Option<String>,
    /// Prefixes that were in scope when the parser failed (without the trailing colon).
    pub known_prefixes: Vec<String>,
    pub prefix_syntax: PrefixSyntax,
}

const MAX_EXPECTED_SHOWN: usize = 16;
const MAX_LINE_CHARS: usize = 100;
const CONTEXT_CHARS_BEFORE: usize = 50;
const MAX_TOKEN_CHARS: usize = 40;
const CONTEXT_LINES_BEFORE: usize = 2;
const CONTEXT_LINES_AFTER: usize = 1;

/// Renders a PEG parse error over `input` as a multi-line, human-readable report.
pub fn render_peg_error(
    input: &str,
    error: &ParseError<LineCol>,
    ctx: &DiagnosticContext,
) -> String {
    let offset = floor_char_boundary(input, error.location.offset);
    let expected = Expected::from_tokens(error.expected.tokens(), input, offset);
    let delimiters = scan_delimiters(input, offset);
    let found = found_token(input, offset);

    let mut notes = Vec::new();
    let headline;
    let label;
    // Where the caret goes and how wide the underline is, as a byte range.
    let mut span = match found {
        Some(f) => (offset, offset + f.text.len()),
        None => {
            let end = input.trim_end().len();
            (end, end)
        }
    };

    let in_unterminated_string = delimiters
        .unterminated_string
        .is_some_and(|start| start < offset);
    if let Some(start) = delimiters.unterminated_string.filter(|_| in_unterminated_string) {
        let quote_len = if input[start..].starts_with("\"\"\"") || input[start..].starts_with("'''") {
            3
        } else {
            1
        };
        headline = "unterminated string literal".to_string();
        label = format!(
            "this string is never closed; add a matching `{}`",
            &input[start..start + quote_len]
        );
        span = (start, start + quote_len);
        if !input[start..].starts_with("\"\"\"") && !input[start..].starts_with("'''") {
            notes.push(
                "note: single-quoted strings cannot span lines; use `\"\"\"...\"\"\"` for multi-line strings"
                    .to_string(),
            );
        }
    } else if let Some((start, word)) = bare_word_before(input, offset, found, &expected) {
        headline = format!("`{word}` is not a valid term");
        label = "not a variable, IRI, prefixed name or literal".to_string();
        span = (start, offset);
        notes.push(bare_word_hint(word, ctx));
    } else if let Some(message) = expected.messages.first() {
        let (headline_, label_, span_, mut notes_) = describe_message(message, input, offset, ctx);
        headline = headline_;
        label = label_;
        span = span_;
        for other in expected.messages.iter().skip(1) {
            notes_.push(format!("also: {}", lowercase_first(other)));
        }
        notes.append(&mut notes_);
    } else {
        headline = match found {
            Some(f) => format!("unexpected {}", f.describe()),
            None => format!("unexpected end of {}", ctx.input_noun),
        };
        label = if expected.is_empty() {
            "the parser could not continue here".to_string()
        } else {
            format!("expected {}", expected.summary(3))
        };
        if expected.len() > 3 {
            notes.push(format!("expected {}", expected.summary(MAX_EXPECTED_SHOWN)));
        }
        notes.extend(hints(input, offset, found, &expected, &delimiters, ctx));
    }

    let (line, column) = line_col(input, span.0);
    let mut out = String::new();
    let _ = write!(out, "{} syntax error", ctx.language);
    if let Some(source) = &ctx.source_name {
        let _ = write!(out, " in {source}");
    }
    let _ = writeln!(out, " at line {line}, column {column}: {headline}");
    render_snippet(&mut out, input, span, &label);
    for note in notes {
        let _ = writeln!(out, "  = {note}");
    }
    out.truncate(out.trim_end().len());
    out
}

/// The text found at the error position.
#[derive(Clone, Copy)]
struct Found<'a> {
    text: &'a str,
    truncated: bool,
}

impl Found<'_> {
    fn describe(&self) -> String {
        let first = self.text.chars().next().unwrap_or(' ');
        if first == '\n' || first == '\r' {
            "line break".to_string()
        } else if first.is_whitespace() {
            "whitespace".to_string()
        } else if self.truncated {
            format!("`{}…`", self.text)
        } else {
            format!("`{}`", self.text)
        }
    }

    /// A bare word such as `SELEC` or `@prefx` (no prefix colon, no sigil other than `@`).
    fn is_word(&self) -> bool {
        let text = self.text.strip_prefix('@').unwrap_or(self.text);
        !self.truncated
            && text
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
            && text.chars().next().is_some_and(|c| c.is_alphabetic())
    }
}

fn found_token(input: &str, offset: usize) -> Option<Found<'_>> {
    let rest = &input[offset..];
    let first = rest.chars().next()?;
    let end = if first.is_whitespace() {
        first.len_utf8()
    } else if is_name_char(first) || matches!(first, '?' | '$' | ':' | '@') {
        let mut end = first.len_utf8();
        for (i, c) in rest.char_indices().skip(1) {
            if is_name_char(c) || c == ':' || c == '-' {
                end = i + c.len_utf8();
            } else {
                break;
            }
        }
        end
    } else if first == '<' || first == '"' || first == '\'' {
        let close = if first == '<' { '>' } else { first };
        let mut end = rest.len();
        for (i, c) in rest.char_indices().skip(1) {
            if c == close {
                end = i + c.len_utf8();
                break;
            }
            if c == '\n' || (first == '<' && c.is_whitespace()) {
                end = i;
                break;
            }
        }
        end
    } else {
        first.len_utf8()
    };
    let token = &rest[..end];
    if token.chars().count() > MAX_TOKEN_CHARS {
        let cut = token
            .char_indices()
            .nth(MAX_TOKEN_CHARS)
            .map_or(token.len(), |(i, _)| i);
        Some(Found {
            text: &token[..cut],
            truncated: true,
        })
    } else {
        Some(Found {
            text: token,
            truncated: false,
        })
    }
}

fn is_name_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// The tokens the parser would have accepted, grouped into user-facing categories.
#[derive(Default)]
struct Expected {
    /// Semantic error messages produced by grammar actions, e.g. "Prefix not found".
    messages: Vec<&'static str>,
    punctuation: BTreeSet<String>,
    categories: BTreeSet<&'static str>,
    keywords: BTreeSet<String>,
    end_of_input: bool,
}

const VARIABLE: &str = "a variable";
const VARIABLE_NAME: &str = "a variable name";
const IRI: &str = "an IRI";
const PREFIXED_NAME: &str = "a prefixed name";
const NAME: &str = "a name";
const NUMBER: &str = "a number";
const STRING: &str = "a string literal";
const BLANK_NODE: &str = "a blank node";
const LANGUAGE_TAG: &str = "a language tag";
const EXPRESSION: &str = "an expression";

/// Keywords that remain meaningful where an expression is expected; all other keywords there
/// are built-in function names, which are summarised as "an expression".
const NON_FUNCTION_KEYWORDS: &[&str] = &["ASC", "DESC", "AS", "BY"];

impl Expected {
    fn from_tokens(tokens: impl Iterator<Item = &'static str>, input: &str, offset: usize) -> Self {
        let tokens: Vec<&'static str> = tokens.collect();
        let literals: Vec<String> = tokens.iter().filter_map(|t| unquote(t)).collect();
        let comparison_context = literals.iter().any(|l| l == ">" || l == "=" || l == "<=");
        let mut e = Expected::default();
        for token in tokens {
            if token == "EOF" {
                e.end_of_input = true;
            } else if let Some(lit) = unquote(token) {
                match lit.as_str() {
                    "?" | "$" => {
                        e.categories.insert(VARIABLE);
                    }
                    "_:" => {
                        e.categories.insert(BLANK_NODE);
                    }
                    "\"" | "'" | "\"\"\"" | "'''" => {
                        e.categories.insert(STRING);
                    }
                    "<" if !comparison_context => {
                        e.categories.insert(IRI);
                    }
                    "@" => {
                        e.categories.insert(LANGUAGE_TAG);
                    }
                    "" => {}
                    l if l.chars().all(|c| c.is_ascii_alphabetic()) => {
                        e.keywords.insert(l.to_string());
                    }
                    _ => {
                        e.punctuation.insert(lit);
                    }
                }
            } else if token.starts_with('[') {
                if token.contains("'0'") && !token.contains("'A'") {
                    e.categories.insert(NUMBER);
                } else if token.contains("'A'") || token.contains("'a'") || token.contains("'_'") {
                    e.categories.insert(NAME);
                }
                // Other character classes (escape characters, hex digits, ...) are noise.
            } else if token.trim_start_matches('@').chars().next().is_some_and(|c| c.is_ascii_alphabetic())
                && token
                    .trim_start_matches('@')
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                // Case-insensitive keywords from the grammar's `i("...")` rule.
                e.keywords.insert(token.to_string());
            } else {
                e.messages.push(token);
            }
        }
        // Characters right after `?`/`$` are a variable name.
        let previous = input[..offset].chars().next_back();
        if matches!(previous, Some('?') | Some('$')) && (e.categories.contains(NAME) || e.categories.contains(NUMBER)) {
            e.categories.remove(NAME);
            e.categories.remove(NUMBER);
            e.categories.insert(VARIABLE_NAME);
        }
        // A name followed by `:` is how a prefixed name starts.
        if e.punctuation.contains(":") && (e.categories.contains(NAME) || e.categories.contains(IRI)) {
            e.punctuation.remove(":");
            e.categories.remove(NAME);
            e.categories.insert(PREFIXED_NAME);
        }
        // A name character class while a prefixed name is possible is subsumed by it.
        if e.categories.contains(PREFIXED_NAME) {
            e.categories.remove(NAME);
        }
        // Where an expression may start, the parser tries every built-in function name.
        let upper: BTreeSet<String> = e.keywords.iter().map(|k| k.to_ascii_uppercase()).collect();
        if ["BOUND", "REGEX", "STR", "COALESCE"]
            .iter()
            .all(|k| upper.contains(*k))
        {
            e.keywords
                .retain(|k| NON_FUNCTION_KEYWORDS.contains(&k.to_ascii_uppercase().as_str()));
            for c in [VARIABLE, IRI, PREFIXED_NAME, NAME, NUMBER, STRING] {
                e.categories.remove(c);
            }
            // `.` here is the start of a decimal such as `.5`.
            for p in ["(", "!", "+", "-", "<<(", "."] {
                e.punctuation.remove(p);
            }
            e.categories.insert(EXPRESSION);
        }
        e
    }

    fn len(&self) -> usize {
        self.punctuation.len()
            + self.categories.len()
            + self.keywords.len()
            + usize::from(self.end_of_input)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn items(&self) -> Vec<String> {
        let mut punctuation: Vec<&String> = self.punctuation.iter().collect();
        // Closing delimiters and separators are the most likely fixes, so list them first.
        let rank = |p: &str| match p {
            "}" => 0,
            ")" => 1,
            "]" => 2,
            "." => 3,
            ";" => 4,
            "," => 5,
            _ => 10,
        };
        punctuation.sort_by_key(|p| (rank(p), p.len(), (*p).clone()));
        let category_rank = |c: &str| match c {
            VARIABLE => 0,
            VARIABLE_NAME => 0,
            IRI => 1,
            PREFIXED_NAME => 2,
            STRING => 3,
            NUMBER => 4,
            BLANK_NODE => 5,
            LANGUAGE_TAG => 6,
            EXPRESSION => 7,
            _ => 10,
        };
        let mut categories: Vec<&&str> = self.categories.iter().collect();
        categories.sort_by_key(|c| category_rank(c));
        let mut items: Vec<String> = Vec::new();
        items.extend(punctuation.into_iter().map(|p| format!("`{p}`")));
        items.extend(categories.into_iter().map(|c| c.to_string()));
        items.extend(self.keywords.iter().map(|k| {
            if k.starts_with('@') {
                format!("`{}`", k.to_ascii_lowercase())
            } else {
                format!("`{k}`")
            }
        }));
        if self.end_of_input {
            items.push("end of input".to_string());
        }
        items
    }

    fn summary(&self, max: usize) -> String {
        let items = self.items();
        if items.len() <= max {
            join_alternatives(&items)
        } else {
            let mut shown = items[..max].to_vec();
            shown.push(format!("{} more", items.len() - max));
            let rest = shown.join(", ");
            if max <= 3 {
                rest
            } else {
                format!("one of {rest}")
            }
        }
    }

    fn accepts_term(&self) -> bool {
        self.categories.contains(EXPRESSION)
            || self.categories.contains(VARIABLE)
            || self.categories.contains(IRI)
            || self.categories.contains(PREFIXED_NAME)
    }
}

fn join_alternatives(items: &[String]) -> String {
    match items {
        [] => String::new(),
        [one] => one.clone(),
        [a, b] => format!("{a} or {b}"),
        [init @ .., last] => format!("one of {}, or {last}", init.join(", ")),
    }
}

/// Decodes a quoted literal token as produced by peg (`"\"{\""` -> `{`).
fn unquote(token: &str) -> Option<String> {
    let inner = token.strip_prefix('"')?.strip_suffix('"')?;
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('r') => out.push('\r'),
                Some('0') => out.push('\0'),
                Some(other) => out.push(other),
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    Some(out)
}

/// Builds headline, caret label, span and notes for a semantic error raised by a grammar
/// action. These are reported by peg at the end of the construct that was rejected.
fn describe_message(
    message: &str,
    input: &str,
    offset: usize,
    ctx: &DiagnosticContext,
) -> (String, String, (usize, usize), Vec<String>) {
    let mut notes = Vec::new();
    if message == "Prefix not found" {
        let start = scan_back(input, offset, |c| {
            is_name_char(c) || matches!(c, ':' | '-' | '.' | '%' | '\\')
        });
        let name = &input[start..offset];
        if let Some((prefix, _)) = name.split_once(':') {
            let prefix = prefix.trim_start_matches('.');
            let mut known: Vec<&str> = ctx.known_prefixes.iter().map(String::as_str).collect();
            known.sort_unstable();
            known.dedup();
            if let Some(suggestion) = closest(prefix, known.iter().copied()) {
                notes.push(format!("help: did you mean `{suggestion}:`?"));
            }
            let declaration = match ctx.prefix_syntax {
                PrefixSyntax::Sparql => format!("PREFIX {prefix}: <http://example.com/{prefix}#>"),
                PrefixSyntax::Turtle => format!("@prefix {prefix}: <http://example.com/{prefix}#> ."),
            };
            notes.push(format!(
                "help: declare the prefix at the start of the {}, e.g. `{declaration}`",
                ctx.input_noun
            ));
            if known.is_empty() {
                notes.push("note: no prefixes are declared".to_string());
            } else {
                let shown: Vec<String> = known.iter().take(20).map(|p| format!("{p}:")).collect();
                let more = if known.len() > 20 {
                    format!(" and {} more", known.len() - 20)
                } else {
                    String::new()
                };
                notes.push(format!("note: declared prefixes are {}{more}", shown.join(" ")));
            }
            return (
                format!("undefined prefix `{prefix}:` in `{name}`"),
                format!("the prefix `{prefix}:` is not declared"),
                (start, offset),
                notes,
            );
        }
    }
    if message.contains("IRI") {
        let line_start = input[..offset].rfind('\n').map_or(0, |i| i + 1);
        if let Some(open) = input[line_start..offset].rfind('<') {
            let start = line_start + open;
            let iri = &input[start..offset];
            let mut notes = Vec::new();
            if iri.chars().any(char::is_whitespace) {
                notes.push("note: IRIs cannot contain whitespace; encode spaces as `%20`".to_string());
            } else if !iri.contains(':') {
                notes.push(
                    "note: relative IRIs need a base IRI; use an absolute IRI such as `<http://example.com/x>`"
                        .to_string(),
                );
            } else {
                notes.push("note: IRIs cannot contain `<`, `>`, `\"`, `{`, `}`, `|`, `^`, `` ` `` or `\\`".to_string());
            }
            return (format!("invalid IRI `{iri}`"), "invalid IRI".to_string(), (start, offset), notes);
        }
    }
    if message.starts_with("Duplicated variable name in SELECT") {
        if let Some((span, name)) = duplicated_projection_variable(input, offset) {
            return (
                format!("variable `?{name}` is selected more than once"),
                format!("`?{name}` is already selected"),
                span,
                vec!["help: remove the duplicate, or rename it with `(?x AS ?other)`".to_string()],
            );
        }
    }
    // Errors about a bracketed construct (`BIND(...)`, `{ ... }`) are reported right after it:
    // underline the whole construct, including the keyword in front of it.
    let end = input[..offset].trim_end().len();
    if let Some(close) = input[..end]
        .chars()
        .next_back()
        .filter(|c| matches!(c, ')' | '}'))
    {
        if let Some(open) = matching_open(input, end - 1, close) {
            let start = scan_back(input, open, |c| c.is_ascii_alphabetic());
            let text = lowercase_first(message);
            return (text.clone(), text, (start, end), notes);
        }
    }
    // Other semantic messages: underline the token that ends at the reported position.
    let start = scan_back(input, offset, |c| !c.is_whitespace() && !matches!(c, '(' | '{' | '[' | ',' | ';'));
    let span = if start < offset {
        (start, offset)
    } else {
        let end = found_token(input, offset).map_or(offset, |f| offset + f.text.len());
        (offset, end)
    };
    let text = lowercase_first(message);
    (text.clone(), text, span, notes)
}

fn hints(
    input: &str,
    offset: usize,
    found: Option<Found<'_>>,
    expected: &Expected,
    delimiters: &Delimiters,
    ctx: &DiagnosticContext,
) -> Vec<String> {
    let mut hints = Vec::new();
    match found {
        None => {
            if let Some(open) = delimiters.open.last() {
                let (l, c) = line_col(input, open.offset);
                let closing = closing_for(open.ch);
                let others = delimiters.open.len() - 1;
                let extra = if others > 0 {
                    format!(" ({others} more enclosing bracket{} also open)", if others == 1 { " is" } else { "s are" })
                } else {
                    String::new()
                };
                hints.push(format!(
                    "help: the `{}` opened at line {l}, column {c} is never closed; add a matching `{closing}`{extra}",
                    open.ch
                ));
            }
        }
        Some(f) => {
            let first = f.text.chars().next().unwrap_or(' ');
            if matches!(first, ')' | '}' | ']') {
                match delimiters.open.last() {
                    Some(open) if closing_for(open.ch) != first => {
                        let (l, c) = line_col(input, open.offset);
                        hints.push(format!(
                            "help: `{first}` does not match the `{}` opened at line {l}, column {c}; expected `{}` to close it",
                            open.ch,
                            closing_for(open.ch)
                        ));
                    }
                    None => hints.push(format!("help: `{first}` has no matching opening bracket")),
                    _ => {}
                }
            }
            let starts_term = matches!(first, '?' | '$' | '<' | '"' | '\'' | '_' | '(')
                || first.is_ascii_digit()
                || (f.is_word() || f.text.contains(':'));
            let missing_separator = starts_term
                && expected.punctuation.contains(",")
                && expected.punctuation.contains(")")
                && !expected.punctuation.contains(".");
            if missing_separator {
                hints.push(format!(
                    "help: arguments are separated by `,`; add a `,` before {}",
                    f.describe()
                ));
            } else if let Some(open) = delimiters
                .open
                .last()
                .filter(|_| !matches!(first, ')' | '}' | ']'))
            {
                let closing = closing_for(open.ch);
                if expected.punctuation.contains(closing.to_string().as_str()) {
                    let (l, c) = line_col(input, open.offset);
                    hints.push(format!(
                        "help: if the `{}` opened at line {l}, column {c} should end here, add `{closing}` before {}",
                        open.ch,
                        f.describe()
                    ));
                }
            }
            if (first == '?' || first == '$') && expected.keywords.contains("AS") {
                hints.push(format!(
                    "help: name the expression with `AS`, e.g. `(... AS {})`",
                    f.text
                ));
            }
            if f.is_word() {
                if let Some(keyword) = closest_keyword(f.text, &expected.keywords) {
                    let keyword = if keyword.starts_with('@') {
                        keyword.to_ascii_lowercase()
                    } else {
                        keyword
                    };
                    hints.push(format!("help: did you mean `{keyword}`?"));
                } else if expected.accepts_term() {
                    hints.push(bare_word_hint(f.text, ctx));
                }
            }
            let previous = input[..offset].trim_end().chars().next_back();
            if previous == Some(',') && expected.accepts_term() {
                hints.push(
                    "help: a `,` must be followed by another object; remove the trailing `,` or add the missing object"
                        .to_string(),
                );
            }
        }
    }
    hints
}

/// Detects a bare word such as `b` in `?a b ?c`: the parser reads it as the start of a
/// prefixed name and stops right after it, expecting `:`.
fn bare_word_before<'a>(
    input: &'a str,
    offset: usize,
    found: Option<Found<'_>>,
    expected: &Expected,
) -> Option<(usize, &'a str)> {
    let at_boundary = match found {
        None => true,
        Some(f) => {
            let c = f.text.chars().next().unwrap_or(' ');
            c.is_whitespace() || matches!(c, ',' | ')' | '}' | ']' | ';' | '.')
        }
    };
    if !at_boundary || !expected.categories.contains(PREFIXED_NAME) {
        return None;
    }
    let start = scan_back(input, offset, |c| is_name_char(c) || c == '-');
    let word = &input[start..offset];
    let before = input[..start].chars().next_back();
    if word.is_empty()
        || !word.chars().next().is_some_and(char::is_alphabetic)
        || matches!(before, Some('?' | '$' | ':' | '_' | '"' | '\'' | '@' | '<'))
    {
        return None;
    }
    Some((start, word))
}

/// Finds the byte offset of the bracket opening the one closed at `close_at`.
fn matching_open(input: &str, close_at: usize, close: char) -> Option<usize> {
    let open = if close == ')' { '(' } else { '{' };
    let mut depth = 0usize;
    for (i, c) in input[..=close_at].char_indices().rev() {
        if c == close {
            depth += 1;
        } else if c == open {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Some(i);
            }
        }
    }
    None
}

/// Finds the second occurrence of a variable in the last SELECT projection before `offset`.
fn duplicated_projection_variable(input: &str, offset: usize) -> Option<((usize, usize), &str)> {
    let upper = input[..offset].to_ascii_uppercase();
    let select = upper.rfind("SELECT")? + "SELECT".len();
    let projection_end = upper[select..]
        .find("WHERE")
        .or_else(|| upper[select..].find('{'))
        .map_or(offset, |i| select + i);
    let bytes = input.as_bytes();
    let mut seen = Vec::new();
    let mut i = select;
    while i < projection_end {
        if bytes[i] == b'?' || bytes[i] == b'$' {
            let start = i;
            i += 1;
            while i < projection_end
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] >= 0x80)
            {
                i += 1;
            }
            let name = &input[start + 1..i];
            if seen.contains(&name) {
                return Some(((start, i), name));
            }
            seen.push(name);
        } else {
            i += 1;
        }
    }
    None
}

fn bare_word_hint(word: &str, ctx: &DiagnosticContext) -> String {
    let example_prefix = if ctx.known_prefixes.iter().any(|p| p == "ex") {
        "ex"
    } else {
        ctx.known_prefixes
            .iter()
            .filter(|p| {
                !p.is_empty()
                    && !matches!(
                        p.as_str(),
                        "rdf" | "rdfs" | "xsd" | "owl" | "def" | "ottr" | "maplib" | "sh" | "skos"
                    )
            })
            .min()
            .map_or("ex", String::as_str)
    };
    format!(
        "help: write a variable as `?{word}`, a prefixed name as `{example_prefix}:{word}`, or a full IRI as `<http://example.com/{word}>`"
    )
}

fn closest_keyword(word: &str, keywords: &BTreeSet<String>) -> Option<String> {
    let upper = word.to_ascii_uppercase();
    if keywords.iter().any(|k| k.eq_ignore_ascii_case(word)) {
        return None;
    }
    closest(&upper, keywords.iter().map(|k| k.to_ascii_uppercase()))
        .map(|k| k.to_string())
}

/// Finds the candidate closest to `word` by edit distance, if it is close enough to be a typo.
fn closest<S: AsRef<str>>(word: &str, candidates: impl Iterator<Item = S>) -> Option<S> {
    let len = word.chars().count();
    if len == 0 {
        return None;
    }
    let max_distance = match len {
        0..=2 => 0,
        3..=5 => 1,
        _ => 2,
    };
    let lower = word.to_lowercase();
    candidates
        .map(|c| {
            let d = edit_distance(&lower, &c.as_ref().to_lowercase());
            (d, c)
        })
        .filter(|(d, _)| *d > 0 && *d <= max_distance)
        .min_by_key(|(d, _)| *d)
        .map(|(_, c)| c)
}

fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    let mut current = vec![0; b.len() + 1];
    for i in 1..=a.len() {
        current[0] = i;
        for j in 1..=b.len() {
            let substitution = previous[j - 1] + usize::from(a[i - 1] != b[j - 1]);
            current[j] = substitution.min(previous[j] + 1).min(current[j - 1] + 1);
            // Adjacent transposition counts as a single edit.
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                current[j] = current[j].min(previous[j - 1]);
            }
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[b.len()]
}

struct OpenDelimiter {
    ch: char,
    offset: usize,
}

#[derive(Default)]
struct Delimiters {
    /// Brackets still open at the error position, innermost last.
    open: Vec<OpenDelimiter>,
    /// Start of a string literal that is never closed.
    unterminated_string: Option<usize>,
}

fn closing_for(open: char) -> char {
    match open {
        '(' => ')',
        '[' => ']',
        _ => '}',
    }
}

/// Lightweight lexical scan that skips comments, strings and IRIs and tracks bracket nesting.
fn scan_delimiters(input: &str, until: usize) -> Delimiters {
    let mut result = Delimiters::default();
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'#' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'"' | b'\'' => {
                let long = bytes[i..].starts_with(&[b, b, b]);
                let start = i;
                i += if long { 3 } else { 1 };
                let mut closed = false;
                while i < bytes.len() {
                    if bytes[i] == b'\\' {
                        i += 2;
                        continue;
                    }
                    if long {
                        if bytes[i..].starts_with(&[b, b, b]) {
                            i += 3;
                            closed = true;
                            break;
                        }
                    } else if bytes[i] == b {
                        i += 1;
                        closed = true;
                        break;
                    } else if bytes[i] == b'\n' {
                        break;
                    }
                    i += 1;
                }
                if !closed {
                    result.unterminated_string = Some(start);
                    break;
                }
                continue;
            }
            b'<' => {
                // An IRI if it closes with `>` before any whitespace; otherwise an operator.
                if let Some(len) = bytes[i + 1..]
                    .iter()
                    .position(|c| *c == b'>' || c.is_ascii_whitespace() || *c == b'<')
                {
                    if bytes[i + 1 + len] == b'>' {
                        i += len + 2;
                        continue;
                    }
                }
            }
            _ if i < until => match b {
                b'(' | b'[' | b'{' => result.open.push(OpenDelimiter {
                    ch: b as char,
                    offset: i,
                }),
                b')' | b']' | b'}' => {
                    if result
                        .open
                        .last()
                        .is_some_and(|o| closing_for(o.ch) == b as char)
                    {
                        result.open.pop();
                    }
                }
                _ => {}
            },
            _ => {}
        }
        i += 1;
    }
    result
}

fn scan_back(input: &str, offset: usize, keep: impl Fn(char) -> bool) -> usize {
    let mut start = offset;
    for (i, c) in input[..offset].char_indices().rev() {
        if keep(c) {
            start = i;
        } else {
            break;
        }
    }
    start
}

fn floor_char_boundary(input: &str, offset: usize) -> usize {
    let mut offset = offset.min(input.len());
    while !input.is_char_boundary(offset) {
        offset -= 1;
    }
    offset
}

/// 1-based line and column (in characters) of a byte offset.
fn line_col(input: &str, offset: usize) -> (usize, usize) {
    let before = &input[..offset];
    let line = before.matches('\n').count() + 1;
    let line_start = before.rfind('\n').map_or(0, |i| i + 1);
    let column = before[line_start..].chars().count() + 1;
    (line, column)
}

fn lowercase_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) => {
            // Keep acronyms like "IRIs" or "SELECT" as they are.
            if chars.clone().next().is_some_and(|c| c.is_uppercase()) {
                s.to_string()
            } else {
                first.to_lowercase().chain(chars).collect()
            }
        }
        None => String::new(),
    }
}

fn render_snippet(out: &mut String, input: &str, span: (usize, usize), label: &str) {
    let lines: Vec<&str> = input.split('\n').collect();
    let (line, column) = line_col(input, span.0);
    let line_index = line - 1;
    let first = line_index.saturating_sub(CONTEXT_LINES_BEFORE);
    let last = (line_index + CONTEXT_LINES_AFTER).min(lines.len() - 1);
    // Skip blank context lines at the edges so the snippet stays compact.
    let first = (first..line_index)
        .find(|i| !lines[*i].trim().is_empty())
        .unwrap_or(line_index);
    let last = (line_index + 1..=last)
        .rev()
        .find(|i| !lines[*i].trim().is_empty())
        .unwrap_or(line_index);
    let gutter = (last + 1).to_string().len();

    // Long lines (e.g. single-line generated queries) are shown in a window around the error.
    let focus = column - 1;
    let window_start = if lines[line_index].chars().count() > MAX_LINE_CHARS {
        focus.saturating_sub(CONTEXT_CHARS_BEFORE)
    } else {
        0
    };

    let _ = writeln!(out, "{:gutter$} |", "");
    for (i, text) in lines.iter().enumerate().take(last + 1).skip(first) {
        let (shown, _) = window(text, window_start);
        let _ = writeln!(out, "{:>gutter$} | {}", i + 1, shown);
        if i == line_index {
            let (_, prefix_len) = window(text, window_start);
            let caret_col = focus - window_start + prefix_len;
            let span_text = &input[span.0..span.1.max(span.0)];
            let span_chars = span_text
                .split('\n')
                .next()
                .unwrap_or("")
                .chars()
                .count()
                .clamp(1, MAX_LINE_CHARS.saturating_sub(caret_col).max(1));
            let _ = writeln!(
                out,
                "{:gutter$} | {}{} {}",
                "",
                " ".repeat(caret_col),
                "^".repeat(span_chars),
                label
            );
        }
    }
    let _ = writeln!(out, "{:gutter$} |", "");
}

/// Returns the visible part of a line starting at `start` (in characters), with ellipses
/// where it is cut, and how many characters the leading ellipsis takes.
fn window(line: &str, start: usize) -> (String, usize) {
    let line = line.trim_end_matches('\r');
    let chars: Vec<char> = line
        .chars()
        .map(|c| if c == '\t' { ' ' } else { c })
        .collect();
    if start == 0 && chars.len() <= MAX_LINE_CHARS {
        return (chars.into_iter().collect(), 0);
    }
    let start = start.min(chars.len());
    let end = (start + MAX_LINE_CHARS).min(chars.len());
    let mut shown = String::new();
    let prefix = if start > 0 {
        shown.push('…');
        1
    } else {
        0
    };
    shown.extend(&chars[start..end]);
    if end < chars.len() {
        shown.push('…');
    }
    (shown, prefix)
}

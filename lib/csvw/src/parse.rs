//! Parsing tabular data files according to a dialect description, see
//! <https://www.w3.org/TR/tabular-data-model/#parsing>.

use crate::errors::CsvwError;
use crate::metadata::{Dialect, Trim};

#[derive(Debug, Clone)]
pub struct ParsedRow {
    /// The row number, starting at 1.
    pub number: usize,
    /// The line number in the file, starting at 1.
    pub source_number: usize,
    pub cells: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ParsedTable {
    /// The titles found in the header rows for each column.
    pub titles: Vec<Vec<String>>,
    /// The number of columns in the header, or of the first row if there is no header.
    pub n_columns: usize,
    pub rows: Vec<ParsedRow>,
    pub comments: Vec<String>,
}

pub fn is_supported_encoding(encoding: &str) -> bool {
    matches!(
        encoding.to_ascii_lowercase().as_str(),
        "utf-8"
            | "utf8"
            | "iso-8859-1"
            | "latin1"
            | "latin-1"
            | "iso8859-1"
            | "windows-1252"
            | "cp1252"
            | "us-ascii"
            | "ascii"
            | "utf-16"
            | "utf-16le"
            | "utf-16be"
    )
}

pub fn decode(bytes: &[u8], encoding: &str) -> String {
    let enc = encoding.to_ascii_lowercase();
    match enc.as_str() {
        "iso-8859-1" | "latin1" | "latin-1" | "iso8859-1" | "windows-1252" | "cp1252"
        | "us-ascii" | "ascii" => bytes.iter().map(|b| *b as char).collect(),
        "utf-16" | "utf-16le" => {
            let u16s: Vec<u16> = bytes
                .chunks(2)
                .map(|c| u16::from_le_bytes([c[0], *c.get(1).unwrap_or(&0)]))
                .collect();
            String::from_utf16_lossy(&u16s)
                .trim_start_matches('\u{feff}')
                .to_string()
        }
        "utf-16be" => {
            let u16s: Vec<u16> = bytes
                .chunks(2)
                .map(|c| u16::from_be_bytes([c[0], *c.get(1).unwrap_or(&0)]))
                .collect();
            String::from_utf16_lossy(&u16s)
                .trim_start_matches('\u{feff}')
                .to_string()
        }
        _ => String::from_utf8_lossy(bytes)
            .trim_start_matches('\u{feff}')
            .to_string(),
    }
}

struct Reader<'a> {
    s: &'a str,
    pos: usize,
    quote: Option<&'a str>,
    escape: Option<&'a str>,
    terminators: Vec<&'a str>,
}

impl<'a> Reader<'a> {
    fn rest(&self) -> &'a str {
        &self.s[self.pos..]
    }

    fn at_end(&self) -> bool {
        self.pos >= self.s.len()
    }

    fn take_char(&mut self) -> &'a str {
        let rest = self.rest();
        let n = rest.chars().next().map(|c| c.len_utf8()).unwrap_or(0);
        let out = &rest[..n];
        self.pos += n;
        out
    }

    /// Reads a row, returning the row content without the line terminator.
    fn read_row(&mut self) -> String {
        let mut content = String::new();
        loop {
            if self.at_end() {
                return content;
            }
            let rest = self.rest();
            if let (Some(e), Some(q)) = (self.escape, self.quote) {
                if rest.starts_with(e) && rest[e.len()..].starts_with(q) {
                    content.push_str(e);
                    content.push_str(q);
                    self.pos += e.len() + q.len();
                    continue;
                }
                if e != q && rest.starts_with(e) {
                    content.push_str(e);
                    self.pos += e.len();
                    if !self.at_end() {
                        content.push_str(self.take_char());
                    }
                    continue;
                }
            }
            if let Some(q) = self.quote {
                if rest.starts_with(q) {
                    content.push_str(&self.read_quoted());
                    continue;
                }
            }
            if let Some(t) = self.terminators.iter().find(|t| rest.starts_with(**t)) {
                self.pos += t.len();
                return content;
            }
            content.push_str(self.take_char());
        }
    }

    fn read_quoted(&mut self) -> String {
        let q = self.quote.unwrap();
        let mut out = String::from(q);
        self.pos += q.len();
        loop {
            if self.at_end() {
                return out;
            }
            let rest = self.rest();
            if let Some(e) = self.escape {
                if rest.starts_with(e) && rest[e.len()..].starts_with(q) {
                    out.push_str(e);
                    out.push_str(q);
                    self.pos += e.len() + q.len();
                    continue;
                }
                if e != q && rest.starts_with(e) {
                    out.push_str(e);
                    self.pos += e.len();
                    if !self.at_end() {
                        out.push_str(self.take_char());
                    }
                    continue;
                }
            }
            if rest.starts_with(q) {
                out.push_str(q);
                self.pos += q.len();
                return out;
            }
            out.push_str(self.take_char());
        }
    }
}

fn trim_cell(s: &str, trim: Trim) -> String {
    match trim {
        Trim::True => s.trim().to_string(),
        Trim::Start => s.trim_start().to_string(),
        Trim::End => s.trim_end().to_string(),
        Trim::False => s.to_string(),
    }
}

fn parse_row(row: &str, d: &Dialect, url: &str) -> Result<Vec<String>, CsvwError> {
    let quote = d.quote_char.as_deref();
    let escape: Option<&str> = if d.double_quote { quote } else { Some("\\") };
    let delim = d.delimiter.as_str();
    let mut cells = vec![];
    let mut current = String::new();
    let mut quoted = false;
    let mut pos = 0;
    while pos < row.len() {
        let rest = &row[pos..];
        if let (Some(e), Some(q)) = (escape, quote) {
            if rest.starts_with(e) && rest[e.len()..].starts_with(q) && (quoted || e != q) {
                current.push_str(q);
                pos += e.len() + q.len();
                continue;
            }
            if e != q && rest.starts_with(e) {
                pos += e.len();
                if let Some(c) = row[pos..].chars().next() {
                    current.push(c);
                    pos += c.len_utf8();
                }
                continue;
            }
        }
        if let Some(q) = quote {
            if rest.starts_with(q) {
                pos += q.len();
                if !quoted {
                    quoted = true;
                    if !current.trim().is_empty() {
                        return Err(CsvwError::TabularDataError(
                            url.to_string(),
                            format!("unexpected quote in row {row}"),
                        ));
                    }
                    current.clear();
                } else {
                    quoted = false;
                    let after = &row[pos..];
                    if !after.is_empty() && !after.trim_start().starts_with(delim) {
                        return Err(CsvwError::TabularDataError(
                            url.to_string(),
                            format!("expected a delimiter after a quoted value in row {row}"),
                        ));
                    }
                }
                continue;
            }
        }
        if rest.starts_with(delim) {
            if quoted {
                current.push_str(delim);
            } else {
                cells.push(trim_cell(&current, d.trim));
                current.clear();
            }
            pos += delim.len();
            continue;
        }
        let c = rest.chars().next().unwrap();
        current.push(c);
        pos += c.len_utf8();
    }
    cells.push(trim_cell(&current, d.trim));
    Ok(cells)
}

pub fn parse_tabular(bytes: &[u8], d: &Dialect, url: &str) -> Result<ParsedTable, CsvwError> {
    let s = decode(bytes, &d.encoding);
    let quote = d.quote_char.as_deref();
    let escape: Option<&str> = if d.double_quote { quote } else { Some("\\") };
    let mut terminators: Vec<&str> = d.line_terminators.iter().map(|x| x.as_str()).collect();
    if terminators.is_empty() {
        terminators = vec!["\r\n", "\n"];
    }
    terminators.sort_by_key(|b| std::cmp::Reverse(b.len()));
    let mut reader = Reader {
        s: &s,
        pos: 0,
        quote,
        escape,
        terminators,
    };
    let mut out = ParsedTable::default();
    let mut source_row = 1;
    let is_comment = |content: &str| -> Option<String> {
        if let Some(cp) = &d.comment_prefix {
            content
                .strip_prefix(cp.as_str())
                .map(|x| x.trim().to_string())
        } else {
            None
        }
    };
    for _ in 0..d.skip_rows {
        if reader.at_end() {
            break;
        }
        let content = reader.read_row();
        if let Some(c) = is_comment(&content) {
            out.comments.push(c);
        } else if !content.is_empty() {
            out.comments.push(content);
        }
        source_row += 1;
    }
    let mut header_rows_read = 0;
    while header_rows_read < d.header_row_count && !reader.at_end() {
        let content = reader.read_row();
        source_row += 1;
        if let Some(c) = is_comment(&content) {
            out.comments.push(c);
            continue;
        }
        header_rows_read += 1;
        let cells = parse_row(&content, d, url)?;
        let cells: Vec<String> = cells.into_iter().skip(d.skip_columns).collect();
        if out.titles.len() < cells.len() {
            out.titles.resize(cells.len(), vec![]);
        }
        for (i, c) in cells.into_iter().enumerate() {
            if !c.trim().is_empty() {
                out.titles[i].push(c);
            }
        }
        out.n_columns = out.n_columns.max(out.titles.len());
    }
    let mut row_number = 1;
    while !reader.at_end() {
        let content = reader.read_row();
        if let Some(c) = is_comment(&content) {
            out.comments.push(c);
            source_row += 1;
            continue;
        }
        let cells = parse_row(&content, d, url)?;
        if d.skip_blank_rows && cells.iter().all(|c| c.is_empty()) {
            source_row += 1;
            continue;
        }
        let cells: Vec<String> = cells.into_iter().skip(d.skip_columns).collect();
        if d.header_row_count == 0 && out.n_columns == 0 {
            out.n_columns = cells.len();
        }
        out.rows.push(ParsedRow {
            number: row_number,
            source_number: source_row,
            cells,
        });
        row_number += 1;
        source_row += 1;
    }
    for r in &out.rows {
        out.n_columns = out.n_columns.max(r.cells.len());
    }
    Ok(out)
}

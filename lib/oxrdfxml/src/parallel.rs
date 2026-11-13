//! Splitting an RDF/XML document into independently parseable documents (maplib addition).
//!
//! An RDF/XML document with an `rdf:RDF` root is a sequence of top-level node elements. Each
//! chunk of consecutive top-level elements becomes a document of its own: the original prolog
//! (XML declaration, DOCTYPE with entity declarations, comments) and the `rdf:RDF` start tag
//! (with its namespace declarations, `xml:base` and `xml:lang`), the chunk, and the original
//! end of the document (`</rdf:RDF>` and anything after it).
//!
//! Parsing the chunks separately gives the same triples as parsing the whole document:
//! `rdf:nodeID` blank nodes keep their ids, anonymous blank nodes are fresh in both cases, and
//! everything a top-level element inherits comes from the root start tag, which every chunk has.
//! `rdf:ID` uniqueness is only checked within a chunk.

use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use std::borrow::Cow;

const RDF_NS: &[u8] = b"http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const MIN_CHUNK_SIZE: usize = 1 << 20;

/// Splits `slice` into at most about `target_parallelism` RDF/XML documents that can be parsed
/// independently and in parallel, and together give the same triples as `slice`.
///
/// Returns `slice` itself as the only document if it is small, does not have an `rdf:RDF` root,
/// or is not well-formed enough to find split points (the parser then reports the error).
pub fn split_slice_for_parallel_parsing(
    slice: &[u8],
    target_parallelism: usize,
) -> Vec<Cow<'_, [u8]>> {
    split_with_min_chunk_size(slice, target_parallelism, MIN_CHUNK_SIZE)
}

fn split_with_min_chunk_size(
    slice: &[u8],
    target_parallelism: usize,
    min_chunk_size: usize,
) -> Vec<Cow<'_, [u8]>> {
    if target_parallelism <= 1 || slice.len() < 2 * min_chunk_size {
        return vec![Cow::Borrowed(slice)];
    }
    let chunk_size = (slice.len() / target_parallelism).max(min_chunk_size);
    let Some(layout) = find_layout(slice, chunk_size) else {
        return vec![Cow::Borrowed(slice)];
    };
    if layout.boundaries.is_empty() {
        return vec![Cow::Borrowed(slice)];
    }
    let prolog = &slice[..layout.prolog_end];
    let epilogue = &slice[layout.content_end..];
    let mut starts = vec![layout.prolog_end];
    starts.extend_from_slice(&layout.boundaries);
    let mut ends = layout.boundaries.clone();
    ends.push(layout.content_end);
    starts
        .into_iter()
        .zip(ends)
        .filter(|(start, end)| start < end)
        .map(|(start, end)| {
            let content = &slice[start..end];
            let mut doc = Vec::with_capacity(prolog.len() + content.len() + epilogue.len());
            doc.extend_from_slice(prolog);
            doc.extend_from_slice(content);
            doc.extend_from_slice(epilogue);
            Cow::Owned(doc)
        })
        .collect()
}

struct Layout {
    /// End of the `rdf:RDF` start tag.
    prolog_end: usize,
    /// Start of the `</rdf:RDF>` end tag.
    content_end: usize,
    /// Positions right after top-level elements where the content is split.
    boundaries: Vec<usize>,
}

fn find_layout(slice: &[u8], chunk_size: usize) -> Option<Layout> {
    let mut reader = Reader::from_reader(slice);
    reader.config_mut().check_end_names = false;
    let mut depth = 0usize;
    let mut prolog_end = 0usize;
    let mut boundaries = Vec::new();
    let mut next_split = chunk_size;
    loop {
        let before = usize::try_from(reader.buffer_position()).ok()?;
        let event = reader.read_event().ok()?;
        let after = usize::try_from(reader.buffer_position()).ok()?;
        match event {
            Event::Start(e) => {
                if depth == 0 {
                    if !is_rdf_root(&e) {
                        return None;
                    }
                    prolog_end = after;
                    next_split = after + chunk_size;
                }
                depth += 1;
            }
            Event::Empty(_) => {
                if depth == 0 {
                    return None;
                }
                if depth == 1 && after >= next_split {
                    boundaries.push(after);
                    next_split = after + chunk_size;
                }
            }
            Event::End(_) => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                if depth == 0 {
                    // Do not split right before the end tag.
                    if boundaries.last() == Some(&before) {
                        boundaries.pop();
                    }
                    return Some(Layout {
                        prolog_end,
                        content_end: before,
                        boundaries,
                    });
                }
                if depth == 1 && after >= next_split {
                    boundaries.push(after);
                    next_split = after + chunk_size;
                }
            }
            Event::Eof => return None,
            _ => {}
        }
    }
}

/// Is this the start tag of an `rdf:RDF` element (resolving its prefix on the tag itself)?
fn is_rdf_root(e: &BytesStart<'_>) -> bool {
    let name = e.name();
    let name = name.as_ref();
    let (prefix, local) = match name.iter().position(|b| *b == b':') {
        Some(i) => (&name[..i], &name[i + 1..]),
        None => (&name[..0], name),
    };
    if local != b"RDF" {
        return false;
    }
    let xmlns_key: Vec<u8> = if prefix.is_empty() {
        b"xmlns".to_vec()
    } else {
        [b"xmlns:".as_slice(), prefix].concat()
    };
    e.attributes()
        .with_checks(false)
        .flatten()
        .any(|a| a.key.as_ref() == xmlns_key.as_slice() && a.value.as_ref() == RDF_NS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RdfXmlParser;
    use oxrdf::{Term, Triple};
    use std::collections::BTreeSet;

    fn normalized(triples: impl IntoIterator<Item = Triple>) -> (usize, BTreeSet<String>) {
        // Anonymous blank nodes get fresh ids in every parse, so compare everything else.
        let mut n = 0;
        let set = triples
            .into_iter()
            .inspect(|_| n += 1)
            .map(|t| {
                let s = if t.subject.is_blank_node() && !t.subject.to_string().starts_with("_:n") {
                    "_:anon".to_string()
                } else {
                    t.subject.to_string()
                };
                let o = match &t.object {
                    Term::BlankNode(b) if !b.as_str().starts_with('n') => "_:anon".to_string(),
                    o => o.to_string(),
                };
                format!("{s} {} {o}", t.predicate)
            })
            .collect();
        (n, set)
    }

    fn parse(doc: &[u8]) -> Vec<Triple> {
        RdfXmlParser::new()
            .with_base_iri("http://example.com/base/")
            .unwrap()
            .for_slice(doc)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn document(n: usize) -> Vec<u8> {
        let mut doc = String::from(
            r#"<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE rdf:RDF [ <!ENTITY ex "http://example.com/ns#"> ]>
<!-- a comment -->
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:ex="http://example.com/ns#" xml:base="http://example.com/doc/" xml:lang="en">
"#,
        );
        for i in 0..n {
            doc.push_str(&format!(
                r#"  <ex:Thing rdf:about="thing{i}">
    <ex:label>Thing &amp; number {i}</ex:label>
    <ex:ref rdf:resource="&ex;r{i}"/>
    <ex:node rdf:nodeID="n{i}"/>
    <ex:anon><ex:Inner ex:v="{i}"/></ex:anon>
    <ex:xml rdf:parseType="Literal"><b>bold <i>{i}</i></b></ex:xml>
    <!-- comment with <ex:fake> tag -->
    <ex:cdata><![CDATA[<not a="tag">]]></ex:cdata>
  </ex:Thing>
  <rdf:Description rdf:nodeID="n{i}" ex:p="{i}"/>
  <rdf:Description rdf:ID="id{i}"><ex:q xml:lang="nb">hei</ex:q></rdf:Description>
"#
            ));
        }
        doc.push_str("</rdf:RDF>\n<!-- trailing -->\n");
        doc.into_bytes()
    }

    #[test]
    fn split_gives_same_triples() {
        let doc = document(500);
        let chunks = split_with_min_chunk_size(&doc, 8, 1000);
        assert!(
            chunks.len() > 4,
            "expected several chunks, got {}",
            chunks.len()
        );
        let sequential = normalized(parse(&doc));
        let split = normalized(chunks.iter().flat_map(|c| parse(c)));
        assert_eq!(sequential, split);
    }

    #[test]
    fn small_documents_are_not_split() {
        let doc = document(5);
        assert_eq!(split_slice_for_parallel_parsing(&doc, 8).len(), 1);
    }

    #[test]
    fn non_rdf_root_is_not_split() {
        let mut doc = String::from(
            r#"<ex:Thing xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:ex="http://example.com/ns#" rdf:about="http://example.com/a">"#,
        );
        for i in 0..2000 {
            doc.push_str(&format!("<ex:p>{i}</ex:p>"));
        }
        doc.push_str("</ex:Thing>");
        assert_eq!(split_with_min_chunk_size(doc.as_bytes(), 8, 100).len(), 1);
    }

    #[test]
    fn malformed_documents_are_not_split() {
        let mut doc = document(500);
        doc.truncate(doc.len() / 2);
        assert_eq!(split_with_min_chunk_size(&doc, 8, 1000).len(), 1);
    }
}

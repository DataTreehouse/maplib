//! Pretty JSON-LD output for the blocks built by the pretty turtle writer.
//!
//! The document is a compacted JSON-LD 1.1 document with a `@context` of the used prefixes and a
//! `@graph` with one node object per subject. rdf:type is written as `@type`, xsd:string literals
//! as JSON strings, other literals as value objects (keeping their lexical form), blank nodes that
//! are the object of exactly one triple as nested node objects and RDF lists as `@list` objects.

use super::{TermOrList, TurtleBlock, TurtleBlockOrTermOrList};
use aho_corasick::{AhoCorasick, MatchKind};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{NamedNode, Term, TermRef};
use representation::dataset::NamedGraph;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};

const INDENT: &[u8] = b"  ";
/// Top level node objects are inside `{ "@graph": [ ... ] }`.
pub(super) const NODE_DEPTH: usize = 2;
/// Node objects of named graphs are inside `{ "@graph": [ { "@id": ..., "@graph": [ ... ] } ] }`.
pub(super) const GRAPH_NODE_DEPTH: usize = 4;
/// Written before each top level node object. The writer drops it before the first one.
pub(super) const NODE_SEPARATOR: &[u8] = b",\n";

/// The prefixes of the `@context`, used to write IRIs as compact IRIs.
pub(super) struct JsonLdContext {
    prefixes: Vec<(String, String)>,
    names: HashSet<String>,
    matcher: AhoCorasick,
}

impl JsonLdContext {
    /// The given (prefix, namespace) pairs that can be JSON-LD prefixes. The empty prefix and `_`
    /// can not be.
    pub(super) fn new<'a>(prefixes: impl Iterator<Item = (&'a String, &'a NamedNode)>) -> Self {
        let mut prefixes: Vec<(String, String)> = prefixes
            .filter(|(name, _)| is_valid_prefix_name(name))
            .map(|(name, ns)| (name.clone(), ns.as_str().to_string()))
            .collect();
        prefixes.sort();
        prefixes.dedup_by(|a, b| a.0 == b.0);
        let names = prefixes.iter().map(|(n, _)| n.clone()).collect();
        let mut builder = AhoCorasick::builder();
        builder.match_kind(MatchKind::LeftmostLongest);
        let matcher = builder
            .build(prefixes.iter().map(|(_, ns)| ns.as_str()))
            .expect("could not build the prefix matcher");
        JsonLdContext {
            prefixes,
            names,
            matcher,
        }
    }

    pub(super) fn write_start<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(b"{\n")?;
        if !self.prefixes.is_empty() {
            write_indent(w, 1)?;
            w.write_all(b"\"@context\": {")?;
            for (i, (name, ns)) in self.prefixes.iter().enumerate() {
                w.write_all(if i == 0 { b"\n" } else { b",\n" })?;
                write_indent(w, 2)?;
                write_string(w, name)?;
                w.write_all(b": ")?;
                if ns.ends_with(is_gen_delim) {
                    write_string(w, ns)?;
                } else {
                    // Only terms whose IRI ends with a gen-delim character are used as prefixes
                    // without asking for it.
                    w.write_all(b"{\"@id\": ")?;
                    write_string(w, ns)?;
                    w.write_all(b", \"@prefix\": true}")?;
                }
            }
            w.write_all(b"\n")?;
            write_indent(w, 1)?;
            w.write_all(b"},\n")?;
        }
        write_indent(w, 1)?;
        w.write_all(b"\"@graph\": [\n")
    }

    pub(super) fn write_end<W: Write>(w: &mut W, wrote_nodes: bool) -> io::Result<()> {
        if wrote_nodes {
            w.write_all(b"\n")?;
        }
        write_indent(w, 1)?;
        w.write_all(b"]\n}\n")
    }

    /// Opens the graph object of a named graph, after the top level items written before.
    pub(super) fn write_graph_start<W: Write>(
        &self,
        w: &mut W,
        graph: &NamedGraph,
        after_items: bool,
    ) -> io::Result<()> {
        if after_items {
            w.write_all(NODE_SEPARATOR)?;
        }
        write_indent(w, NODE_DEPTH)?;
        w.write_all(b"{\n")?;
        write_indent(w, NODE_DEPTH + 1)?;
        w.write_all(b"\"@id\": ")?;
        match graph {
            NamedGraph::NamedGraph(nn) => self.write_iri(w, nn.as_str())?,
            NamedGraph::BlankNode(b) => write_string(w, &format!("_:{}", b.as_str()))?,
            NamedGraph::DefaultGraph => unreachable!("The default graph has no graph object"),
        }
        w.write_all(b",\n")?;
        write_indent(w, NODE_DEPTH + 1)?;
        w.write_all(b"\"@graph\": [\n")
    }

    pub(super) fn write_graph_end<W: Write>(w: &mut W, wrote_nodes: bool) -> io::Result<()> {
        if wrote_nodes {
            w.write_all(b"\n")?;
        }
        write_indent(w, NODE_DEPTH + 1)?;
        w.write_all(b"]\n")?;
        write_indent(w, NODE_DEPTH)?;
        w.write_all(b"}")
    }

    /// The IRI as a compact IRI if a prefix matches, otherwise as it is. Fails if the IRI would be
    /// read as a compact IRI (its scheme is a prefix), as the JSON-LD compaction algorithm does.
    fn compact<'a>(&self, iri: &'a str) -> io::Result<Cow<'a, str>> {
        if let Some(m) = self.matcher.find(iri) {
            let suffix = &iri[m.end()..];
            // A suffix starting with // makes it an absolute IRI.
            if m.start() == 0 && !suffix.starts_with("//") {
                let name = &self.prefixes[m.pattern().as_usize()].0;
                return Ok(Cow::Owned(format!("{name}:{suffix}")));
            }
        }
        if let Some((scheme, rest)) = iri.split_once(':') {
            if !rest.starts_with("//") && self.names.contains(scheme) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "The IRI <{iri}> can not be written in JSON-LD with the prefix {scheme}, \
                         it would be read as a compact IRI. Use another name for the prefix."
                    ),
                ));
            }
        }
        Ok(Cow::Borrowed(iri))
    }

    fn write_iri<W: Write>(&self, w: &mut W, iri: &str) -> io::Result<()> {
        write_string(w, &self.compact(iri)?)
    }

    /// The `@id` or `@type` value of an IRI or blank node.
    fn write_id<W: Write>(&self, w: &mut W, term: &Term) -> io::Result<()> {
        match term {
            Term::NamedNode(nn) => self.write_iri(w, nn.as_str()),
            Term::BlankNode(b) => write_string(w, &format!("_:{}", b.as_str())),
            _ => unreachable!("Only IRIs and blank nodes are identifiers"),
        }
    }
}

/// Writes a subject block as a node object, starting at the current position and ending after its
/// closing brace. Returns the in-degree-one blank nodes it inlined.
pub(super) fn write_block<W: Write>(
    w: &mut W,
    block: &TurtleBlock,
    ctx: &JsonLdContext,
    in_degree_one_map: &HashMap<u32, TurtleBlockOrTermOrList>,
    depth: usize,
    write_subject: bool,
) -> io::Result<Vec<u32>> {
    let mut inlined = Vec::new();
    let mut first = true;
    let mut key = |w: &mut W, key: &dyn Fn(&mut W) -> io::Result<()>| -> io::Result<()> {
        w.write_all(if first { b"{\n" } else { b",\n" })?;
        first = false;
        write_indent(w, depth + 1)?;
        key(w)?;
        w.write_all(b": ")
    };

    if write_subject {
        if let Some(subject) = &block.subject {
            key(w, &|w| w.write_all(b"\"@id\""))?;
            ctx.write_id(w, subject)?;
        }
    }

    let type_nn = rdf::TYPE.into_owned();
    let (types, other_types): (Vec<_>, Vec<_>) = block
        .pred_term_map
        .get(&type_nn)
        .map(|ts| ts.iter().partition(|t| is_identifier(t)))
        .unwrap_or_default();
    if !types.is_empty() {
        key(w, &|w| w.write_all(b"\"@type\""))?;
        write_values(w, &types, depth + 1, |w, t, _| match t {
            TermOrList::Elem(term) => ctx.write_id(w, term),
            _ => unreachable!("Checked by is_identifier"),
        })?;
    }

    for (p, values) in &block.pred_term_map {
        let values: Vec<_> = if p == &type_nn {
            if other_types.is_empty() {
                continue;
            }
            other_types.clone()
        } else {
            values.iter().collect()
        };
        key(w, &|w| ctx.write_iri(w, p.as_str()))?;
        write_values(w, &values, depth + 1, |w, v, d| {
            inlined.extend(write_value(w, v, ctx, in_degree_one_map, d)?);
            Ok(())
        })?;
    }

    if first {
        w.write_all(b"{}")?;
    } else {
        w.write_all(b"\n")?;
        write_indent(w, depth)?;
        w.write_all(b"}")?;
    }
    Ok(inlined)
}

/// A single value as it is, several in an array, one per line.
fn write_values<W: Write, T>(
    w: &mut W,
    values: &[T],
    depth: usize,
    mut write: impl FnMut(&mut W, &T, usize) -> io::Result<()>,
) -> io::Result<()> {
    if let [value] = values {
        return write(w, value, depth);
    }
    w.write_all(b"[")?;
    for (i, v) in values.iter().enumerate() {
        w.write_all(if i == 0 { b"\n" } else { b",\n" })?;
        write_indent(w, depth + 1)?;
        write(w, v, depth + 1)?;
    }
    w.write_all(b"\n")?;
    write_indent(w, depth)?;
    w.write_all(b"]")
}

fn write_value<W: Write>(
    w: &mut W,
    value: &TermOrList,
    ctx: &JsonLdContext,
    in_degree_one_map: &HashMap<u32, TurtleBlockOrTermOrList>,
    depth: usize,
) -> io::Result<Vec<u32>> {
    match value {
        TermOrList::Elem(term) => {
            match term.as_ref().as_ref() {
                TermRef::NamedNode(_) | TermRef::BlankNode(_) => {
                    w.write_all(b"{\"@id\": ")?;
                    ctx.write_id(w, term)?;
                    w.write_all(b"}")?;
                }
                TermRef::Literal(l) => {
                    if let Some(language) = l.language() {
                        w.write_all(b"{\"@value\": ")?;
                        write_string(w, l.value())?;
                        w.write_all(b", \"@language\": ")?;
                        write_string(w, language)?;
                        w.write_all(b"}")?;
                    } else if l.datatype() == xsd::STRING {
                        write_string(w, l.value())?;
                    } else {
                        w.write_all(b"{\"@value\": ")?;
                        write_string(w, l.value())?;
                        w.write_all(b", \"@type\": ")?;
                        ctx.write_iri(w, l.datatype().as_str())?;
                        w.write_all(b"}")?;
                    }
                }
                #[allow(unreachable_patterns)]
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Triple terms can not be written in JSON-LD",
                    ))
                }
            }
            Ok(vec![])
        }
        TermOrList::BlankPlaceholder(u) => {
            let mut inlined = vec![*u];
            match in_degree_one_map.get(u).expect("Should never happen") {
                TurtleBlockOrTermOrList::TurtleBlock(b) => {
                    inlined.extend(write_block(w, b, ctx, in_degree_one_map, depth, false)?)
                }
                TurtleBlockOrTermOrList::Term(t) => {
                    inlined.extend(write_value(w, t, ctx, in_degree_one_map, depth)?)
                }
            }
            Ok(inlined)
        }
        TermOrList::List(items) => {
            let mut inlined = Vec::new();
            w.write_all(b"{\"@list\": ")?;
            if items.is_empty() {
                w.write_all(b"[]")?;
            } else {
                w.write_all(b"[")?;
                for (i, item) in items.iter().enumerate() {
                    w.write_all(if i == 0 { b"\n" } else { b",\n" })?;
                    write_indent(w, depth + 1)?;
                    inlined.extend(write_value(w, item, ctx, in_degree_one_map, depth + 1)?);
                }
                w.write_all(b"\n")?;
                write_indent(w, depth)?;
                w.write_all(b"]")?;
            }
            w.write_all(b"}")?;
            Ok(inlined)
        }
    }
}

/// Can this rdf:type value be written in `@type`?
fn is_identifier(t: &TermOrList) -> bool {
    matches!(t, TermOrList::Elem(term) if matches!(term.as_ref(), Term::NamedNode(_) | Term::BlankNode(_)))
}

/// Terms used as prefixes may not be empty, `_`, have the form of a keyword or contain a colon.
fn is_valid_prefix_name(name: &str) -> bool {
    !name.is_empty() && name != "_" && !name.starts_with('@') && !name.contains([':', '/'])
}

fn is_gen_delim(c: char) -> bool {
    matches!(c, ':' | '/' | '?' | '#' | '[' | ']' | '@')
}

pub(super) fn write_indent<W: Write>(w: &mut W, depth: usize) -> io::Result<()> {
    for _ in 0..depth {
        w.write_all(INDENT)?;
    }
    Ok(())
}

fn write_string<W: Write>(w: &mut W, s: &str) -> io::Result<()> {
    serde_json::to_writer(w, s).map_err(io::Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(prefixes: &[(&str, &str)]) -> JsonLdContext {
        let prefixes: HashMap<String, NamedNode> = prefixes
            .iter()
            .map(|(n, ns)| (n.to_string(), NamedNode::new_unchecked(*ns)))
            .collect();
        JsonLdContext::new(prefixes.iter())
    }

    #[test]
    fn compacts_iris() {
        let c = ctx(&[
            ("ex", "http://example.com/"),
            ("exns", "http://example.com/ns#"),
            ("", "http://empty.com/"),
        ]);
        assert_eq!(c.compact("http://example.com/a").unwrap(), "ex:a");
        assert_eq!(c.compact("http://example.com/ns#a").unwrap(), "exns:a");
        assert_eq!(c.compact("http://example.com//a").unwrap(), "http://example.com//a");
        assert_eq!(c.compact("http://empty.com/a").unwrap(), "http://empty.com/a");
        assert_eq!(
            c.compact("http://other.com/http://example.com/a").unwrap(),
            "http://other.com/http://example.com/a"
        );
        assert!(c.compact("ex:a").is_err());
        assert_eq!(c.compact("ex://a").unwrap(), "ex://a");
    }

    #[test]
    fn writes_context() {
        let c = ctx(&[("ex", "http://example.com/"), ("u", "urn:x_")]);
        let mut out = Vec::new();
        c.write_start(&mut out).unwrap();
        JsonLdContext::write_end(&mut out, false).unwrap();
        let doc: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(doc["@context"]["ex"], "http://example.com/");
        assert_eq!(doc["@context"]["u"]["@id"], "urn:x_");
        assert_eq!(doc["@context"]["u"]["@prefix"], true);
        assert_eq!(doc["@graph"], serde_json::json!([]));
    }
}

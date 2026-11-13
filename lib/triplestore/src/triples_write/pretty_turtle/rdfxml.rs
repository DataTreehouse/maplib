//! Pretty RDF/XML output for the blocks built by the pretty turtle writer.
//!
//! Each subject is written once as a node element with all its properties: a typed node element
//! (e.g. `<ex:Pump rdf:about="...">`) when one of its types can be written as a QName, otherwise
//! `rdf:Description`. Blank nodes that are the object of exactly one triple are nested inside the
//! property element that refers to them, and RDF lists are written with
//! `rdf:parseType="Collection"` when all members are nodes. All namespaces are declared on the
//! root element.

use super::{TermOrList, TurtleBlock, TurtleBlockOrTermOrList};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{BlankNodeRef, NamedNode, Term, TermRef};
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};

const RDF_NS: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const INDENT: &[u8] = b"  ";

/// The namespace prefixes declared on the root element.
pub(super) struct XmlNamespaces {
    ns_to_prefix: HashMap<String, String>,
    declared: Vec<(String, String)>,
    fragment_base: Option<String>,
}

impl XmlNamespaces {
    /// User prefixes that are valid XML prefixes, `rdf`, and generated prefixes (`ns1`, `ns2`,
    /// ...) for the namespaces of all predicates not covered by them.
    pub(super) fn new<'a>(
        prefixes: &HashMap<String, NamedNode>,
        predicates: impl Iterator<Item = &'a NamedNode>,
    ) -> Self {
        let mut ns = XmlNamespaces {
            ns_to_prefix: HashMap::new(),
            declared: Vec::new(),
            fragment_base: None,
        };
        let mut used_prefixes = HashSet::new();
        ns.declare("rdf", RDF_NS, &mut used_prefixes);
        let mut user: Vec<_> = prefixes.iter().collect();
        user.sort();
        for (prefix, nn) in user {
            if is_ncname(prefix) && !prefix.to_ascii_lowercase().starts_with("xml") {
                ns.declare(prefix, nn.as_str(), &mut used_prefixes);
            }
        }
        let mut predicate_namespaces: Vec<_> = predicates
            .filter_map(|p| split_iri(p.as_str()).map(|(n, _)| n.to_string()))
            .collect();
        predicate_namespaces.sort();
        predicate_namespaces.dedup();
        let mut i = 0;
        for n in predicate_namespaces {
            if ns.ns_to_prefix.contains_key(&n) {
                continue;
            }
            let prefix = loop {
                i += 1;
                let candidate = format!("ns{i}");
                if !used_prefixes.contains(&candidate) {
                    break candidate;
                }
            };
            ns.declare(&prefix, &n, &mut used_prefixes);
        }
        ns
    }

    /// Write IRIs starting with `base` (ending with '#') relative to the document.
    pub(super) fn with_fragment_base(mut self, base: Option<&str>) -> Self {
        self.fragment_base = base.map(|b| b.to_string());
        self
    }

    /// The part after the fragment base, if `iri` starts with it and the rest is an XML name
    /// (required for rdf:ID).
    fn fragment<'a>(&self, iri: &'a str) -> Option<&'a str> {
        let rest = iri.strip_prefix(self.fragment_base.as_deref()?)?;
        is_ncname(rest).then_some(rest)
    }

    fn declare(&mut self, prefix: &str, namespace: &str, used_prefixes: &mut HashSet<String>) {
        if self.ns_to_prefix.contains_key(namespace) || used_prefixes.contains(prefix) {
            return;
        }
        used_prefixes.insert(prefix.to_string());
        self.ns_to_prefix
            .insert(namespace.to_string(), prefix.to_string());
        self.declared
            .push((prefix.to_string(), namespace.to_string()));
    }

    fn qname<'a>(&'a self, iri: &'a str) -> Option<(&'a str, &'a str)> {
        let (namespace, local) = split_iri(iri)?;
        self.ns_to_prefix
            .get(namespace)
            .map(|p| (p.as_str(), local))
    }

    pub(super) fn write_root_start<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(b"<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<rdf:RDF")?;
        for (prefix, namespace) in &self.declared {
            w.write_all(b"\n    xmlns:")?;
            w.write_all(prefix.as_bytes())?;
            w.write_all(b"=\"")?;
            write_escaped(w, namespace, true)?;
            w.write_all(b"\"")?;
        }
        w.write_all(b">\n")
    }

    pub(super) fn write_root_end<W: Write>(w: &mut W) -> io::Result<()> {
        w.write_all(b"</rdf:RDF>\n")
    }
}

/// Writes a subject block as a node element. Returns the in-degree-one blank nodes it inlined.
pub(super) fn write_block<W: Write>(
    w: &mut W,
    block: &TurtleBlock,
    ns: &XmlNamespaces,
    in_degree_one_map: &HashMap<u32, TurtleBlockOrTermOrList>,
    depth: usize,
    write_subject: bool,
) -> io::Result<Vec<u32>> {
    let mut inlined = Vec::new();
    let type_nn = rdf::TYPE.into_owned();
    let types = block.pred_term_map.get(&type_nn);
    // Use the first type that has a QName as the element name.
    let element_type = types.and_then(|ts| {
        ts.iter().enumerate().find_map(|(i, t)| match t {
            TermOrList::Elem(term) => match term.as_ref() {
                Term::NamedNode(nn) => ns.qname(nn.as_str()).map(|q| (i, q)),
                _ => None,
            },
            _ => None,
        })
    });
    let element_name = match element_type {
        Some((_, (prefix, local))) => format!("{prefix}:{local}"),
        None => "rdf:Description".to_string(),
    };

    write_indent(w, depth)?;
    w.write_all(b"<")?;
    w.write_all(element_name.as_bytes())?;
    if write_subject {
        match &block.subject {
            Some(Term::NamedNode(nn)) => {
                if let Some(id) = ns.fragment(nn.as_str()) {
                    w.write_all(b" rdf:ID=\"")?;
                    write_escaped(w, id, true)?;
                } else {
                    w.write_all(b" rdf:about=\"")?;
                    write_escaped(w, nn.as_str(), true)?;
                }
                w.write_all(b"\"")?;
            }
            Some(Term::BlankNode(b)) => write_node_id_attr(w, b.as_ref())?,
            _ => {}
        }
    }

    let n_props: usize = block
        .pred_term_map
        .iter()
        .map(|(p, v)| {
            if p == &type_nn && element_type.is_some() {
                v.len() - 1
            } else {
                v.len()
            }
        })
        .sum();
    if n_props == 0 {
        w.write_all(b"/>\n")?;
        return Ok(inlined);
    }
    w.write_all(b">\n")?;

    // rdf:type first, as in turtle, then the other predicates in order.
    if let Some(ts) = types {
        for (i, t) in ts.iter().enumerate() {
            if element_type.map(|(j, _)| j) != Some(i) {
                inlined.extend(write_property(
                    w,
                    &type_nn,
                    t,
                    ns,
                    in_degree_one_map,
                    depth + 1,
                )?);
            }
        }
    }
    for (p, values) in &block.pred_term_map {
        if p == &type_nn {
            continue;
        }
        for v in values {
            inlined.extend(write_property(w, p, v, ns, in_degree_one_map, depth + 1)?);
        }
    }

    write_indent(w, depth)?;
    w.write_all(b"</")?;
    w.write_all(element_name.as_bytes())?;
    w.write_all(b">\n")?;
    Ok(inlined)
}

fn write_property<W: Write>(
    w: &mut W,
    predicate: &NamedNode,
    value: &TermOrList,
    ns: &XmlNamespaces,
    in_degree_one_map: &HashMap<u32, TurtleBlockOrTermOrList>,
    depth: usize,
) -> io::Result<Vec<u32>> {
    let (prefix, local) = ns.qname(predicate.as_str()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("The predicate {predicate} can not be written in RDF/XML, it does not end with a valid XML name"),
        )
    })?;
    let mut inlined = Vec::new();
    let open = |w: &mut W| -> io::Result<()> {
        write_indent(w, depth)?;
        w.write_all(b"<")?;
        w.write_all(prefix.as_bytes())?;
        w.write_all(b":")?;
        w.write_all(local.as_bytes())
    };
    let close = |w: &mut W, indent: bool| -> io::Result<()> {
        if indent {
            write_indent(w, depth)?;
        }
        w.write_all(b"</")?;
        w.write_all(prefix.as_bytes())?;
        w.write_all(b":")?;
        w.write_all(local.as_bytes())?;
        w.write_all(b">\n")
    };
    match value {
        TermOrList::Elem(term) => match term.as_ref().as_ref() {
            TermRef::NamedNode(nn) => {
                open(w)?;
                w.write_all(b" rdf:resource=\"")?;
                if let Some(id) = ns.fragment(nn.as_str()) {
                    w.write_all(b"#")?;
                    write_escaped(w, id, true)?;
                } else {
                    write_escaped(w, nn.as_str(), true)?;
                }
                w.write_all(b"\"/>\n")?;
            }
            TermRef::BlankNode(b) => {
                open(w)?;
                write_node_id_attr(w, b)?;
                w.write_all(b"/>\n")?;
            }
            TermRef::Literal(l) => {
                open(w)?;
                if let Some(language) = l.language() {
                    w.write_all(b" xml:lang=\"")?;
                    write_escaped(w, language, true)?;
                    w.write_all(b"\"")?;
                } else if l.datatype() != xsd::STRING {
                    w.write_all(b" rdf:datatype=\"")?;
                    write_escaped(w, l.datatype().as_str(), true)?;
                    w.write_all(b"\"")?;
                }
                w.write_all(b">")?;
                write_escaped(w, l.value(), false)?;
                close(w, false)?;
            }
            #[allow(unreachable_patterns)]
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Triple terms can not be written in RDF/XML",
                ))
            }
        },
        TermOrList::BlankPlaceholder(u) => {
            inlined.push(*u);
            match in_degree_one_map.get(u).expect("Should never happen") {
                TurtleBlockOrTermOrList::TurtleBlock(b) => {
                    open(w)?;
                    w.write_all(b">\n")?;
                    inlined.extend(write_block(w, b, ns, in_degree_one_map, depth + 1, false)?);
                    close(w, true)?;
                }
                TurtleBlockOrTermOrList::Term(t) => {
                    inlined.extend(write_property(
                        w,
                        predicate,
                        t,
                        ns,
                        in_degree_one_map,
                        depth,
                    )?);
                }
            }
        }
        TermOrList::List(items) => {
            open(w)?;
            if items.iter().all(|i| is_node(i, in_degree_one_map)) {
                w.write_all(b" rdf:parseType=\"Collection\">\n")?;
                for i in items {
                    inlined.extend(write_node(w, i, ns, in_degree_one_map, depth + 1)?);
                }
            } else {
                w.write_all(b">\n")?;
                inlined.extend(write_expanded_list(
                    w,
                    items,
                    ns,
                    in_degree_one_map,
                    depth + 1,
                )?);
            }
            close(w, true)?;
        }
    }
    Ok(inlined)
}

/// Can this list member be written as a node element in an rdf:parseType="Collection"?
fn is_node(t: &TermOrList, in_degree_one_map: &HashMap<u32, TurtleBlockOrTermOrList>) -> bool {
    match t {
        TermOrList::Elem(term) => matches!(term.as_ref(), Term::NamedNode(_) | Term::BlankNode(_)),
        TermOrList::BlankPlaceholder(u) => match in_degree_one_map.get(u) {
            Some(TurtleBlockOrTermOrList::TurtleBlock(_)) => true,
            Some(TurtleBlockOrTermOrList::Term(t)) => is_node(t, in_degree_one_map),
            None => false,
        },
        TermOrList::List(_) => false,
    }
}

fn write_node<W: Write>(
    w: &mut W,
    t: &TermOrList,
    ns: &XmlNamespaces,
    in_degree_one_map: &HashMap<u32, TurtleBlockOrTermOrList>,
    depth: usize,
) -> io::Result<Vec<u32>> {
    match t {
        TermOrList::Elem(term) => {
            write_indent(w, depth)?;
            match term.as_ref() {
                Term::NamedNode(nn) => {
                    w.write_all(b"<rdf:Description rdf:about=\"")?;
                    if let Some(id) = ns.fragment(nn.as_str()) {
                        w.write_all(b"#")?;
                        write_escaped(w, id, true)?;
                    } else {
                        write_escaped(w, nn.as_str(), true)?;
                    }
                    w.write_all(b"\"/>\n")?;
                }
                Term::BlankNode(b) => {
                    w.write_all(b"<rdf:Description")?;
                    write_node_id_attr(w, b.as_ref())?;
                    w.write_all(b"/>\n")?;
                }
                _ => unreachable!("Checked by is_node"),
            }
            Ok(vec![])
        }
        TermOrList::BlankPlaceholder(u) => {
            let mut inlined = vec![*u];
            match in_degree_one_map.get(u).expect("Checked by is_node") {
                TurtleBlockOrTermOrList::TurtleBlock(b) => {
                    inlined.extend(write_block(w, b, ns, in_degree_one_map, depth, false)?)
                }
                TurtleBlockOrTermOrList::Term(t) => {
                    inlined.extend(write_node(w, t, ns, in_degree_one_map, depth)?)
                }
            }
            Ok(inlined)
        }
        TermOrList::List(_) => unreachable!("Checked by is_node"),
    }
}

/// A list with literal or list members, as nested rdf:first / rdf:rest descriptions.
fn write_expanded_list<W: Write>(
    w: &mut W,
    items: &[TermOrList],
    ns: &XmlNamespaces,
    in_degree_one_map: &HashMap<u32, TurtleBlockOrTermOrList>,
    depth: usize,
) -> io::Result<Vec<u32>> {
    let mut inlined = Vec::new();
    let first = rdf::FIRST.into_owned();
    for (i, item) in items.iter().enumerate() {
        let d = depth + 2 * i;
        write_indent(w, d)?;
        w.write_all(b"<rdf:Description>\n")?;
        inlined.extend(write_property(
            w,
            &first,
            item,
            ns,
            in_degree_one_map,
            d + 1,
        )?);
        write_indent(w, d + 1)?;
        if i + 1 == items.len() {
            w.write_all(
                b"<rdf:rest rdf:resource=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#nil\"/>\n",
            )?;
        } else {
            w.write_all(b"<rdf:rest>\n")?;
        }
    }
    for i in (0..items.len()).rev() {
        let d = depth + 2 * i;
        if i + 1 != items.len() {
            write_indent(w, d + 1)?;
            w.write_all(b"</rdf:rest>\n")?;
        }
        write_indent(w, d)?;
        w.write_all(b"</rdf:Description>\n")?;
    }
    Ok(inlined)
}

fn write_node_id_attr<W: Write>(w: &mut W, b: BlankNodeRef<'_>) -> io::Result<()> {
    w.write_all(b" rdf:nodeID=\"")?;
    let id = b.as_str();
    // rdf:nodeID values must be XML names; blank node ids may start with a digit.
    if !id.chars().next().is_some_and(is_ncname_start_char) {
        w.write_all(b"b")?;
    }
    write_escaped(w, id, true)?;
    w.write_all(b"\"")
}

fn write_indent<W: Write>(w: &mut W, depth: usize) -> io::Result<()> {
    for _ in 0..depth {
        w.write_all(INDENT)?;
    }
    Ok(())
}

fn write_escaped<W: Write>(w: &mut W, s: &str, attribute: bool) -> io::Result<()> {
    let bytes = s.as_bytes();
    let mut start = 0;
    for (i, c) in s.char_indices() {
        let replacement: &[u8] = match c {
            '&' => b"&amp;",
            '<' => b"&lt;",
            '>' => b"&gt;",
            '"' if attribute => b"&quot;",
            '\n' if attribute => b"&#10;",
            '\t' if attribute => b"&#9;",
            '\r' => b"&#13;",
            '\n' | '\t' => continue,
            c if (c as u32) < 0x20 || c == '\u{FFFE}' || c == '\u{FFFF}' => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "The character U+{:04X} can not be written in XML 1.0",
                        c as u32
                    ),
                ))
            }
            _ => continue,
        };
        w.write_all(&bytes[start..i])?;
        w.write_all(replacement)?;
        start = i + c.len_utf8();
    }
    w.write_all(&bytes[start..])
}

/// Splits an IRI into a namespace and the longest suffix that is a valid XML local name.
fn split_iri(iri: &str) -> Option<(&str, &str)> {
    let mut start = iri.len();
    for (i, c) in iri.char_indices().rev() {
        if is_ncname_char(c) {
            start = i;
        } else {
            break;
        }
    }
    // The local name must start with a name start character.
    let local = &iri[start..];
    let offset = local
        .char_indices()
        .find(|(_, c)| is_ncname_start_char(*c))?
        .0;
    let split = start + offset;
    if split == 0 {
        return None;
    }
    Some((&iri[..split], &iri[split..]))
}

fn is_ncname(s: &str) -> bool {
    let mut chars = s.chars();
    chars.next().is_some_and(is_ncname_start_char) && chars.all(is_ncname_char)
}

fn is_ncname_start_char(c: char) -> bool {
    c.is_ascii_alphabetic()
        || c == '_'
        || matches!(c as u32, 0xC0..=0xD6 | 0xD8..=0xF6 | 0xF8..=0x2FF | 0x370..=0x37D | 0x37F..=0x1FFF
            | 0x200C..=0x200D | 0x2070..=0x218F | 0x2C00..=0x2FEF | 0x3001..=0xD7FF | 0xF900..=0xFDCF
            | 0xFDF0..=0xFFFD | 0x10000..=0xEFFFF)
}

fn is_ncname_char(c: char) -> bool {
    is_ncname_start_char(c)
        || c.is_ascii_digit()
        || c == '-'
        || c == '.'
        || c == '\u{B7}'
        || matches!(c as u32, 0x300..=0x36F | 0x203F..=0x2040)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_iris_at_xml_names() {
        assert_eq!(
            split_iri("http://ex.com/ns#Pump"),
            Some(("http://ex.com/ns#", "Pump"))
        );
        assert_eq!(
            split_iri("http://ex.com/a/b.c"),
            Some(("http://ex.com/a/", "b.c"))
        );
        assert_eq!(
            split_iri("http://ex.com/a/1x"),
            Some(("http://ex.com/a/1", "x"))
        );
        assert_eq!(split_iri("http://ex.com/a/123"), None);
        assert_eq!(split_iri("http://ex.com/a/"), None);
        assert_eq!(
            split_iri("http://iec.ch/TC57/CIM100#Terminal.ConductingEquipment"),
            Some(("http://iec.ch/TC57/CIM100#", "Terminal.ConductingEquipment"))
        );
    }

    #[test]
    fn escapes() {
        let mut out = Vec::new();
        write_escaped(&mut out, "a<b>&\"c\"\n", true).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "a&lt;b&gt;&amp;&quot;c&quot;&#10;"
        );
        let mut out = Vec::new();
        write_escaped(&mut out, "a<b>&\"c\"\n\r", false).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "a&lt;b&gt;&amp;\"c\"\n&#13;"
        );
        assert!(write_escaped(&mut Vec::new(), "a\u{1}", false).is_err());
    }
}

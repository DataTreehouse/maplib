//! Parsing R2RML mapping documents (<https://www.w3.org/TR/r2rml/>) into triples maps.
//!
//! RML mappings are read into the same triples maps: a logical source (rml:logicalSource) takes
//! the place of the logical table, and a reference (rml:reference) that of a column. The terms of
//! R2RML are accepted in the R2RML namespace, in the RML namespace of the W3C community group
//! (<http://w3id.org/rml/>) and in the earlier RML namespace
//! (<http://semweb.mmlab.be/ns/rml#>), also mixed.

use crate::errors::{mapping_err, R2RMLError};
use oxrdf::vocab::rdf;
use oxrdf::{Graph, NamedNode, NamedOrBlankNode, Term, TermRef};
use oxttl::TurtleParser;
use std::collections::{BTreeMap, BTreeSet};

pub const RR: &str = "http://www.w3.org/ns/r2rml#";
/// The RML namespace of the W3C Knowledge Graph Construction community group.
pub const RML: &str = "http://w3id.org/rml/";
/// The RML namespace of the first RML specification, used together with rr: terms.
pub const RML_OLD: &str = "http://semweb.mmlab.be/ns/rml#";
/// The namespace of the reference formulations of the first RML specification.
pub const QL: &str = "http://semweb.mmlab.be/ns/ql#";

/// The namespaces a term of R2RML or RML may be in.
const TERM_NAMESPACES: [&str; 3] = [RR, RML, RML_OLD];

/// The local name of an IRI in one of the given namespaces.
fn local_name<'a>(iri: &'a str, namespaces: &[&str]) -> Option<&'a str> {
    namespaces.iter().find_map(|ns| iri.strip_prefix(ns))
}

fn rr(local: &str) -> NamedNode {
    NamedNode::new_unchecked(format!("{RR}{local}"))
}

/// A parsed R2RML mapping: its triples maps in a stable order.
#[derive(Debug, Clone)]
pub struct R2RMLMapping {
    pub triples_maps: Vec<TriplesMap>,
}

#[derive(Debug, Clone)]
pub struct TriplesMap {
    /// The IRI or blank node label of the triples map, used in messages.
    pub name: String,
    pub logical_table: LogicalTable,
    pub subject_map: TermMap,
    pub classes: Vec<NamedNode>,
    pub subject_graphs: Vec<TermMap>,
    pub predicate_object_maps: Vec<PredicateObjectMap>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LogicalTable {
    /// rr:tableName, the (possibly schema qualified, possibly delimited) table or view name.
    Table(String),
    /// rr:sqlQuery
    Query(String),
    /// rml:logicalSource: a CSV, JSON or XML source.
    Source(LogicalSource),
}

/// How the references of a logical source are written, rml:referenceFormulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ReferenceFormulation {
    /// References are column names.
    CSV,
    /// The iterator and the references are JSONPath expressions.
    JSONPath,
    /// The iterator and the references are XPath expressions.
    XPath,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogicalSource {
    /// rml:source, the name of the source: a path, or the key of a source given by the caller.
    pub source: String,
    pub formulation: ReferenceFormulation,
    /// rml:iterator, which selects the records of the source. Without one, the records are the
    /// rows of a CSV source, the root of a JSON source and the root element of an XML source.
    pub iterator: Option<String>,
}

impl LogicalTable {
    /// The effective SQL query of the logical table. For a logical source, which has none, a
    /// key that identifies the source, its reference formulation and its iterator.
    pub fn sql(&self) -> String {
        match self {
            LogicalTable::Table(t) => format!("SELECT * FROM {t}"),
            LogicalTable::Query(q) => q.trim().trim_end_matches(';').trim().to_string(),
            LogicalTable::Source(s) => format!(
                "rml:logicalSource {:?} {:?} {:?}",
                s.source,
                s.formulation,
                s.iterator.as_deref().unwrap_or("")
            ),
        }
    }

    /// The key of the rows of the logical table in the tables given to `execute`.
    pub fn key(&self) -> String {
        self.sql()
    }

    pub fn is_sql(&self) -> bool {
        !matches!(self, LogicalTable::Source(_))
    }
}

#[derive(Debug, Clone)]
pub struct PredicateObjectMap {
    pub predicates: Vec<TermMap>,
    pub objects: Vec<ObjectMap>,
    pub graphs: Vec<TermMap>,
}

#[derive(Debug, Clone)]
pub enum ObjectMap {
    Term(TermMap),
    Ref(RefObjectMap),
}

#[derive(Debug, Clone)]
pub struct RefObjectMap {
    /// Index of the parent triples map in R2RMLMapping::triples_maps.
    pub parent: usize,
    /// (child column, parent column)
    pub join_conditions: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TermType {
    IRI,
    BlankNode,
    Literal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplatePart {
    Text(String),
    Column(String),
}

#[derive(Debug, Clone)]
pub enum TermValue {
    Constant(Term),
    Column(String),
    Template(Vec<TemplatePart>),
}

#[derive(Debug, Clone)]
pub struct TermMap {
    pub value: TermValue,
    pub term_type: TermType,
    pub language: Option<String>,
    pub datatype: Option<NamedNode>,
}

impl TermMap {
    pub fn is_default_graph(&self) -> bool {
        matches!(&self.value, TermValue::Constant(Term::NamedNode(nn))
            if local_name(nn.as_str(), &TERM_NAMESPACES) == Some("defaultGraph"))
    }

    /// The columns the term map references.
    pub fn columns(&self) -> Vec<&str> {
        match &self.value {
            TermValue::Constant(_) => vec![],
            TermValue::Column(c) => vec![c.as_str()],
            TermValue::Template(parts) => parts
                .iter()
                .filter_map(|p| match p {
                    TemplatePart::Column(c) => Some(c.as_str()),
                    TemplatePart::Text(_) => None,
                })
                .collect(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Position {
    Subject,
    Predicate,
    Object,
    Graph,
}

impl Position {
    fn name(&self) -> &'static str {
        match self {
            Position::Subject => "subject map",
            Position::Predicate => "predicate map",
            Position::Object => "object map",
            Position::Graph => "graph map",
        }
    }
}

struct Parser<'a> {
    g: &'a Graph,
}

fn node_name(n: &NamedOrBlankNode) -> String {
    match n {
        NamedOrBlankNode::NamedNode(nn) => nn.to_string(),
        NamedOrBlankNode::BlankNode(b) => b.to_string(),
    }
}

fn as_node(t: TermRef) -> Option<NamedOrBlankNode> {
    match t {
        TermRef::NamedNode(n) => Some(NamedOrBlankNode::NamedNode(n.into_owned())),
        TermRef::BlankNode(b) => Some(NamedOrBlankNode::BlankNode(b.into_owned())),
        _ => None,
    }
}

impl<'a> Parser<'a> {
    /// The objects of a property, which is given in the R2RML namespace and is also looked up
    /// in the RML namespaces.
    fn objects(&self, s: &NamedOrBlankNode, p: &NamedNode) -> Vec<Term> {
        let mut v: Vec<Term> = match p.as_str().strip_prefix(RR) {
            Some(local) => TERM_NAMESPACES
                .iter()
                .flat_map(|ns| {
                    let p = NamedNode::new_unchecked(format!("{ns}{local}"));
                    self.g
                        .objects_for_subject_predicate(s, &p)
                        .map(|t| t.into_owned())
                        .collect::<Vec<Term>>()
                })
                .collect(),
            None => self
                .g
                .objects_for_subject_predicate(s, p)
                .map(|t| t.into_owned())
                .collect(),
        };
        v.sort_by_key(|t| t.to_string());
        v.dedup();
        v
    }

    fn nodes(&self, s: &NamedOrBlankNode, p: &NamedNode) -> Result<Vec<NamedOrBlankNode>, R2RMLError> {
        self.objects(s, p)
            .into_iter()
            .map(|t| {
                as_node(t.as_ref()).ok_or_else(|| {
                    R2RMLError::MappingError(format!(
                        "the object of {p} of {} must be an IRI or blank node, not {t}",
                        node_name(s)
                    ))
                })
            })
            .collect()
    }

    fn string(&self, s: &NamedOrBlankNode, p: &NamedNode) -> Result<Option<String>, R2RMLError> {
        let objects = self.objects(s, p);
        match objects.as_slice() {
            [] => Ok(None),
            [Term::Literal(l)] => Ok(Some(l.value().to_string())),
            [t] => mapping_err(format!("{p} of {} must be a literal, not {t}", node_name(s))),
            _ => mapping_err(format!("{} has more than one {p}", node_name(s))),
        }
    }

    fn iri(&self, s: &NamedOrBlankNode, p: &NamedNode) -> Result<Option<NamedNode>, R2RMLError> {
        let objects = self.objects(s, p);
        match objects.as_slice() {
            [] => Ok(None),
            [Term::NamedNode(n)] => Ok(Some(n.clone())),
            [t] => mapping_err(format!("{p} of {} must be an IRI, not {t}", node_name(s))),
            _ => mapping_err(format!("{} has more than one {p}", node_name(s))),
        }
    }

    fn logical_table(&self, tm: &NamedOrBlankNode) -> Result<LogicalTable, R2RMLError> {
        let lts = self.nodes(tm, &rr("logicalTable"))?;
        let sources = self.nodes(tm, &rr("logicalSource"))?;
        let lt = match (lts.as_slice(), sources.as_slice()) {
            ([lt], []) => lt.clone(),
            ([], [source]) => return self.logical_source(tm, source),
            ([], []) => {
                return mapping_err(format!(
                    "the triples map {} has no rr:logicalTable or rml:logicalSource",
                    node_name(tm)
                ))
            }
            _ => {
                return mapping_err(format!(
                    "the triples map {} has more than one rr:logicalTable or rml:logicalSource",
                    node_name(tm)
                ))
            }
        };
        if self.string(&lt, &rr("tableName"))?.is_none()
            && self.string(&lt, &rr("sqlQuery"))?.is_none()
            && !self.objects(&lt, &rr("source")).is_empty()
        {
            // A logical source given with rr:logicalTable.
            return self.logical_source(tm, &lt);
        }
        let table = self.string(&lt, &rr("tableName"))?;
        let query = self.string(&lt, &rr("sqlQuery"))?;
        match (table, query) {
            (Some(t), None) => Ok(LogicalTable::Table(t)),
            (None, Some(q)) => Ok(LogicalTable::Query(q)),
            (None, None) => mapping_err(format!(
                "the logical table of {} has neither rr:tableName nor rr:sqlQuery",
                node_name(tm)
            )),
            (Some(_), Some(_)) => mapping_err(format!(
                "the logical table of {} has both rr:tableName and rr:sqlQuery",
                node_name(tm)
            )),
        }
    }

    fn logical_source(
        &self,
        tm: &NamedOrBlankNode,
        ls: &NamedOrBlankNode,
    ) -> Result<LogicalTable, R2RMLError> {
        let name = node_name(tm);
        let sources = self.objects(ls, &rr("source"));
        let source = match sources.as_slice() {
            [Term::Literal(l)] => l.value().to_string(),
            [t] => {
                // A source description: only a path is read from it.
                let path = match as_node(t.as_ref()) {
                    Some(n) => self.string(&n, &rr("path"))?,
                    None => None,
                };
                match (path, t) {
                    (Some(p), _) => p,
                    (None, Term::NamedNode(nn)) => nn.as_str().to_string(),
                    (None, _) => {
                        return mapping_err(format!(
                            "the rml:source of the logical source of {name} must be a string, an IRI or have an rml:path"
                        ))
                    }
                }
            }
            [] => return mapping_err(format!("the logical source of {name} has no rml:source")),
            _ => return mapping_err(format!("the logical source of {name} has more than one rml:source")),
        };
        let iterator = self.string(ls, &rr("iterator"))?;
        let formulations = self.objects(ls, &rr("referenceFormulation"));
        let formulation = match formulations.as_slice() {
            [] => {
                // By the extension of the source, and CSV otherwise.
                let lower = source.to_lowercase();
                if lower.ends_with(".json") {
                    ReferenceFormulation::JSONPath
                } else if lower.ends_with(".xml") {
                    ReferenceFormulation::XPath
                } else {
                    ReferenceFormulation::CSV
                }
            }
            [Term::NamedNode(nn)] => match local_name(nn.as_str(), &[QL, RML, RML_OLD]) {
                Some("CSV") => ReferenceFormulation::CSV,
                Some("JSONPath") => ReferenceFormulation::JSONPath,
                Some("XPath") => ReferenceFormulation::XPath,
                _ => {
                    return mapping_err(format!(
                        "the rml:referenceFormulation {nn} of {name} is not supported: use CSV, JSONPath or XPath"
                    ))
                }
            },
            [t] => {
                return mapping_err(format!(
                    "the rml:referenceFormulation {t} of {name} must be one of the IRIs for CSV, JSONPath and XPath"
                ))
            }
            _ => return mapping_err(format!("the logical source of {name} has more than one rml:referenceFormulation")),
        };
        Ok(LogicalTable::Source(LogicalSource {
            source,
            formulation,
            iterator,
        }))
    }

    /// The term maps given by `map_property` (e.g. rr:subjectMap) and the constant shortcut
    /// `shortcut_property` (e.g. rr:subject).
    fn term_maps(
        &self,
        s: &NamedOrBlankNode,
        map_property: &str,
        shortcut_property: &str,
        position: Position,
    ) -> Result<Vec<(TermMap, Option<NamedOrBlankNode>)>, R2RMLError> {
        let mut out = vec![];
        for n in self.nodes(s, &rr(map_property))? {
            out.push((self.term_map(&n, position)?, Some(n)));
        }
        for t in self.objects(s, &rr(shortcut_property)) {
            out.push((constant_term_map(t, position)?, None));
        }
        Ok(out)
    }

    fn term_map(&self, n: &NamedOrBlankNode, position: Position) -> Result<TermMap, R2RMLError> {
        let constants = self.objects(n, &rr("constant"));
        let column = self.string(n, &rr("column"))?;
        // A reference of RML is evaluated as a column of the records of the logical source.
        let reference = self.string(n, &rr("reference"))?;
        let template = self.string(n, &rr("template"))?;
        let count = constants.len()
            + column.is_some() as usize
            + reference.is_some() as usize
            + template.is_some() as usize;
        let column = column.or(reference);
        if count != 1 {
            return mapping_err(format!(
                "the {} {} must have exactly one of rr:constant, rr:column, rml:reference and rr:template",
                position.name(),
                node_name(n)
            ));
        }
        if let Some(c) = constants.into_iter().next() {
            let tm = constant_term_map(c, position)?;
            // A term type, language or datatype on a constant must agree with the constant.
            if let Some(tt) = self.iri(n, &rr("termType"))? {
                if parse_term_type(&tt)? != tm.term_type {
                    return mapping_err(format!(
                        "the rr:termType of the constant valued {} {} does not match the constant",
                        position.name(),
                        node_name(n)
                    ));
                }
            }
            return Ok(tm);
        }
        let value = if let Some(c) = column {
            TermValue::Column(c)
        } else {
            let t = template.unwrap();
            TermValue::Template(parse_template(&t)?)
        };
        let language = self.string(n, &rr("language"))?;
        let datatype = self.iri(n, &rr("datatype"))?;
        let term_type = match self.iri(n, &rr("termType"))? {
            Some(tt) => parse_term_type(&tt)?,
            None => {
                if position == Position::Object
                    && (matches!(value, TermValue::Column(_)) || language.is_some() || datatype.is_some())
                {
                    TermType::Literal
                } else {
                    TermType::IRI
                }
            }
        };
        let tm = TermMap {
            value,
            term_type,
            language,
            datatype,
        };
        check_term_map(&tm, position, &node_name(n))?;
        Ok(tm)
    }

    fn object_maps(&self, pom: &NamedOrBlankNode, index: &BTreeMap<String, usize>) -> Result<Vec<ObjectMap>, R2RMLError> {
        let mut out = vec![];
        for n in self.nodes(pom, &rr("objectMap"))? {
            let parents = self.nodes(&n, &rr("parentTriplesMap"))?;
            match parents.as_slice() {
                [] => out.push(ObjectMap::Term(self.term_map(&n, Position::Object)?)),
                [parent] => {
                    let Some(parent) = index.get(&node_name(parent)) else {
                        return mapping_err(format!(
                            "the rr:parentTriplesMap {} of {} is not a triples map",
                            node_name(parent),
                            node_name(&n)
                        ));
                    };
                    let mut join_conditions = vec![];
                    for jc in self.nodes(&n, &rr("joinCondition"))? {
                        let child = self.string(&jc, &rr("child"))?;
                        let parent = self.string(&jc, &rr("parent"))?;
                        match (child, parent) {
                            (Some(c), Some(p)) => join_conditions.push((c, p)),
                            _ => {
                                return mapping_err(format!(
                                    "the join condition {} must have one rr:child and one rr:parent",
                                    node_name(&jc)
                                ))
                            }
                        }
                    }
                    out.push(ObjectMap::Ref(RefObjectMap {
                        parent: *parent,
                        join_conditions,
                    }))
                }
                _ => {
                    return mapping_err(format!(
                        "the object map {} has more than one rr:parentTriplesMap",
                        node_name(&n)
                    ))
                }
            }
        }
        for t in self.objects(pom, &rr("object")) {
            out.push(ObjectMap::Term(constant_term_map(t, Position::Object)?));
        }
        Ok(out)
    }
}

fn parse_term_type(tt: &NamedNode) -> Result<TermType, R2RMLError> {
    match local_name(tt.as_str(), &TERM_NAMESPACES) {
        Some("IRI") => Ok(TermType::IRI),
        Some("BlankNode") => Ok(TermType::BlankNode),
        Some("Literal") => Ok(TermType::Literal),
        _ => mapping_err(format!("unknown rr:termType {tt}")),
    }
}

fn check_term_map(tm: &TermMap, position: Position, name: &str) -> Result<(), R2RMLError> {
    let allowed = match position {
        Position::Subject => tm.term_type != TermType::Literal,
        Position::Predicate | Position::Graph => tm.term_type == TermType::IRI,
        Position::Object => true,
    };
    if !allowed {
        return mapping_err(format!(
            "the {} {name} can not have the term type {:?}",
            position.name(),
            tm.term_type
        ));
    }
    if (tm.language.is_some() || tm.datatype.is_some()) && tm.term_type != TermType::Literal {
        return mapping_err(format!(
            "the {} {name} has rr:language or rr:datatype, but does not generate literals",
            position.name()
        ));
    }
    if tm.language.is_some() && tm.datatype.is_some() {
        return mapping_err(format!(
            "the {} {name} has both rr:language and rr:datatype",
            position.name()
        ));
    }
    if let Some(l) = &tm.language {
        // Primary language subtags of 5 to 8 letters are well-formed BCP 47, but reserved for
        // registered languages, of which there are none: "english" is not a language tag.
        let primary = l.split('-').next().unwrap_or("");
        let registered = primary.len() <= 3 || primary.eq_ignore_ascii_case("x") || primary.eq_ignore_ascii_case("i");
        if oxilangtag::LanguageTag::parse(l.as_str()).is_err() || !registered {
            return mapping_err(format!("the {} {name} has the invalid language tag {l:?}", position.name()));
        }
    }
    Ok(())
}

fn constant_term_map(t: Term, position: Position) -> Result<TermMap, R2RMLError> {
    let (term_type, language, datatype) = match &t {
        Term::NamedNode(_) => (TermType::IRI, None, None),
        Term::Literal(l) => (
            TermType::Literal,
            l.language().map(|s| s.to_string()),
            None::<NamedNode>,
        ),
        _ => {
            return mapping_err(format!(
                "the constant {t} of a {} must be an IRI or a literal",
                position.name()
            ))
        }
    };
    let tm = TermMap {
        value: TermValue::Constant(t),
        term_type,
        language,
        datatype,
    };
    // The language of a constant is part of the constant.
    let check = TermMap {
        language: None,
        ..tm.clone()
    };
    check_term_map(&check, position, "constant")?;
    Ok(tm)
}

/// Parses a string template, see <https://www.w3.org/TR/r2rml/#from-template>.
/// Backslashes escape curly braces and backslashes.
pub fn parse_template(t: &str) -> Result<Vec<TemplatePart>, R2RMLError> {
    let mut parts = vec![];
    let mut text = String::new();
    let mut column: Option<String> = None;
    let mut chars = t.chars();
    while let Some(c) = chars.next() {
        let target = column.as_mut().unwrap_or(&mut text);
        match c {
            '\\' => match chars.next() {
                Some(e @ ('{' | '}' | '\\')) => target.push(e),
                Some(e) => {
                    target.push('\\');
                    target.push(e);
                }
                None => return mapping_err(format!("the template {t:?} ends with a backslash")),
            },
            '{' => {
                if column.is_some() {
                    return mapping_err(format!("unescaped {{ inside a column reference in the template {t:?}"));
                }
                if !text.is_empty() {
                    parts.push(TemplatePart::Text(std::mem::take(&mut text)));
                }
                column = Some(String::new());
            }
            '}' => match column.take() {
                Some(c) if !c.is_empty() => parts.push(TemplatePart::Column(c)),
                Some(_) => return mapping_err(format!("empty column reference in the template {t:?}")),
                None => return mapping_err(format!("unescaped }} in the template {t:?}")),
            },
            c => target.push(c),
        }
    }
    if column.is_some() {
        return mapping_err(format!("unclosed column reference in the template {t:?}"));
    }
    if !text.is_empty() {
        parts.push(TemplatePart::Text(text));
    }
    Ok(parts)
}

/// Parses an R2RML mapping document in Turtle.
pub fn parse_mapping(turtle: &str, base_iri: Option<&str>) -> Result<R2RMLMapping, R2RMLError> {
    let mut parser = TurtleParser::new();
    if let Some(b) = base_iri {
        parser = parser
            .with_base_iri(b)
            .map_err(|e| R2RMLError::ParseError(format!("invalid base IRI {b}: {e}")))?;
    }
    let mut g = Graph::new();
    for t in parser.for_slice(turtle.as_bytes()) {
        let t = t.map_err(|e| R2RMLError::ParseError(e.to_string()))?;
        g.insert(&t);
    }
    mapping_from_graph(&g)
}

/// Reads the triples maps of an R2RML mapping graph.
pub fn mapping_from_graph(g: &Graph) -> Result<R2RMLMapping, R2RMLError> {
    let p = Parser { g };
    // Triples maps: typed as rr:TriplesMap, or with a logical table.
    let mut nodes: BTreeMap<String, NamedOrBlankNode> = BTreeMap::new();
    for ns in TERM_NAMESPACES {
        let tm_type = NamedNode::new_unchecked(format!("{ns}TriplesMap"));
        for s in g.subjects_for_predicate_object(rdf::TYPE, &tm_type) {
            let s = s.into_owned();
            nodes.insert(node_name(&s), s);
        }
    }
    for t in g.iter() {
        if matches!(
            local_name(t.predicate.as_str(), &TERM_NAMESPACES),
            Some("logicalTable" | "logicalSource")
        ) {
            let s = t.subject.into_owned();
            nodes.insert(node_name(&s), s);
        }
    }
    let nodes: Vec<_> = nodes.into_iter().collect();
    let index: BTreeMap<String, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, (name, _))| (name.clone(), i))
        .collect();
    let mut triples_maps = vec![];
    for (name, n) in &nodes {
        let logical_table = p.logical_table(n)?;
        let mut subject_maps = p.term_maps(n, "subjectMap", "subject", Position::Subject)?;
        if subject_maps.len() != 1 {
            return mapping_err(format!(
                "the triples map {name} must have exactly one subject map, but has {}",
                subject_maps.len()
            ));
        }
        let (subject_map, subject_node) = subject_maps.pop().unwrap();
        let mut classes = vec![];
        let mut subject_graphs = vec![];
        if let Some(sn) = &subject_node {
            for c in p.objects(sn, &rr("class")) {
                match c {
                    Term::NamedNode(c) => classes.push(c),
                    c => return mapping_err(format!("the rr:class {c} of {name} must be an IRI")),
                }
            }
            subject_graphs = p
                .term_maps(sn, "graphMap", "graph", Position::Graph)?
                .into_iter()
                .map(|x| x.0)
                .collect();
        }
        let mut predicate_object_maps = vec![];
        for pom in p.nodes(n, &rr("predicateObjectMap"))? {
            let predicates: Vec<TermMap> = p
                .term_maps(&pom, "predicateMap", "predicate", Position::Predicate)?
                .into_iter()
                .map(|x| x.0)
                .collect();
            let objects = p.object_maps(&pom, &index)?;
            if predicates.is_empty() || objects.is_empty() {
                return mapping_err(format!(
                    "the predicate-object map {} of {name} needs at least one predicate map and one object map",
                    node_name(&pom)
                ));
            }
            let graphs = p
                .term_maps(&pom, "graphMap", "graph", Position::Graph)?
                .into_iter()
                .map(|x| x.0)
                .collect();
            predicate_object_maps.push(PredicateObjectMap {
                predicates,
                objects,
                graphs,
            });
        }
        triples_maps.push(TriplesMap {
            name: name.clone(),
            logical_table,
            subject_map,
            classes,
            subject_graphs,
            predicate_object_maps,
        });
    }
    // Referencing object maps without join conditions need the same logical table.
    for tm in &triples_maps {
        for pom in &tm.predicate_object_maps {
            for om in &pom.objects {
                if let ObjectMap::Ref(r) = om {
                    let parent = &triples_maps[r.parent];
                    if r.join_conditions.is_empty() && parent.logical_table.sql() != tm.logical_table.sql() {
                        return mapping_err(format!(
                            "the referencing object map in {} to {} has no join condition, but the logical tables differ",
                            tm.name, parent.name
                        ));
                    }
                }
            }
        }
    }
    Ok(R2RMLMapping { triples_maps })
}

impl R2RMLMapping {
    /// The distinct effective SQL queries of the logical tables, in a stable order.
    pub fn queries(&self) -> Vec<String> {
        let set: BTreeSet<String> = self
            .triples_maps
            .iter()
            .filter(|t| t.logical_table.is_sql())
            .map(|t| t.logical_table.sql())
            .collect();
        set.into_iter().collect()
    }

    /// The distinct logical sources of the mapping, each with the references that are evaluated
    /// on its records: those of the term maps of its triples maps, and of the join conditions
    /// that it is the child or the parent of. In a stable order.
    pub fn logical_sources(&self) -> Vec<(LogicalSource, Vec<String>)> {
        fn references(t: &TermMap) -> Vec<String> {
            t.columns().into_iter().map(|c| c.to_string()).collect()
        }
        let mut out: BTreeMap<LogicalSource, BTreeSet<String>> = BTreeMap::new();
        for tm in &self.triples_maps {
            if let LogicalTable::Source(source) = &tm.logical_table {
                let found = out.entry(source.clone()).or_default();
                found.extend(references(&tm.subject_map));
                for g in &tm.subject_graphs {
                    found.extend(references(g));
                }
                for pom in &tm.predicate_object_maps {
                    for t in pom.predicates.iter().chain(pom.graphs.iter()) {
                        found.extend(references(t));
                    }
                    for om in &pom.objects {
                        match om {
                            ObjectMap::Term(t) => found.extend(references(t)),
                            ObjectMap::Ref(r) => {
                                found.extend(r.join_conditions.iter().map(|(c, _)| c.clone()))
                            }
                        }
                    }
                }
            }
            // The parent sides of the join conditions.
            for pom in &tm.predicate_object_maps {
                for om in &pom.objects {
                    if let ObjectMap::Ref(r) = om {
                        if let LogicalTable::Source(source) = &self.triples_maps[r.parent].logical_table {
                            out.entry(source.clone())
                                .or_default()
                                .extend(r.join_conditions.iter().map(|(_, p)| p.clone()));
                        }
                    }
                }
            }
        }
        out.into_iter()
            .map(|(s, r)| (s, r.into_iter().collect()))
            .collect()
    }
}

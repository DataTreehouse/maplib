use super::Triplestore;
use crate::errors::TriplestoreError;
use crate::sparql::QuerySettings;
use crate::storage::Triples;
use aho_corasick::{AhoCorasick, MatchKind};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{BlankNode, NamedNode, NamedNodeRef, Term, TermRef, Variable};
use polars::prelude::{col, concat, LazyFrame, UnionArgs};
use polars_core::frame::DataFrame;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::cats::LockedCats;
use representation::dataset::NamedGraph;
use representation::polars_to_rdf::{blank_node_to_normal_string, column_as_terms};
use representation::result::QueryResultKind;
use representation::{BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use spargebra::algebra::{Expression, Function, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::sync::{Arc, LazyLock};

mod jsonld;
mod rdfxml;

const STRIDE: usize = 20_000;

/// Threads for the pretty writer's own parallelism, separate from polars' and rayon's global pools
/// (see write_pretty).
static PRETTY_WRITER_POOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
    rayon::ThreadPoolBuilder::new()
        .num_threads(std::thread::available_parallelism().map_or(1, |n| n.get()))
        .thread_name(|i| format!("maplib-pretty-writer-{i}"))
        .build()
        .expect("could not spawn pretty writer threads")
});

/// Output syntax of the pretty writer: both share how subject blocks are built.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrettyFormat {
    Turtle,
    RdfXml,
}

/// The syntax the subject blocks are written in, with what it needs to write IRIs.
#[derive(Clone, Copy)]
enum PrettySyntax<'a> {
    Turtle,
    RdfXml(&'a rdfxml::XmlNamespaces),
    /// With the indentation depth of the node objects.
    JsonLd(&'a jsonld::JsonLdContext, usize),
}

#[derive(Debug, Clone)]
struct TurtleBlock {
    subject: Option<Term>,
    pred_term_map: BTreeMap<NamedNode, Vec<TermOrList>>,
}

enum TurtleBlockOrTermOrList {
    TurtleBlock(TurtleBlock),
    Term(TermOrList),
}

#[derive(Clone, Debug)]
enum TermOrList {
    List(Vec<TermOrList>),
    Elem(Arc<Term>),
    BlankPlaceholder(u32),
}

impl TermOrList {
    pub fn remove_first(&mut self) -> TermOrList {
        match self {
            TermOrList::List(list) => {
                let el = list.remove(0);
                if list.is_empty() {
                    *self = TermOrList::Elem(Arc::new(Term::NamedNode(rdf::NIL.into_owned())));
                }
                el
            }
            _ => {
                unreachable!("Should never happen")
            }
        }
    }

    pub fn push(&mut self, term: TermOrList) {
        match self {
            TermOrList::List(l) => {
                l.push(term);
            }
            _ => {
                unreachable!("Should never be called when not a list")
            }
        }
    }

    pub fn reverse(&mut self) {
        match self {
            TermOrList::List(l) => {
                l.reverse();
            }
            _ => {
                unreachable!("Should never be called when not a list")
            }
        }
    }

    fn write_prefixed<W: Write>(
        &self,
        writer: &mut W,
        type_nn: &NamedNode,
        prefix_replacer: &PrefixReplacer,
        in_degree_one_map: &HashMap<u32, TurtleBlockOrTermOrList>,
        nesting: usize,
    ) -> std::io::Result<Vec<u32>> {
        match self {
            TermOrList::List(list) => {
                write!(writer, "( ")?;
                let mut outs = vec![];
                for tol in list {
                    let out = tol.write_prefixed(
                        writer,
                        type_nn,
                        prefix_replacer,
                        in_degree_one_map,
                        nesting + 1,
                    )?;
                    outs.extend(out);
                    write!(writer, " ")?;
                }
                write!(writer, ")")?;
                Ok(outs)
            }
            TermOrList::Elem(term) => {
                write_term_prefixed(writer, term.as_ref(), prefix_replacer)?;
                Ok(vec![])
            }
            TermOrList::BlankPlaceholder(u) => {
                let mut outs = vec![*u];
                let t = in_degree_one_map.get(u).expect("Should never happen");
                match t {
                    TurtleBlockOrTermOrList::TurtleBlock(t) => {
                        let out = t.write_block(
                            writer,
                            type_nn,
                            prefix_replacer,
                            false,
                            in_degree_one_map,
                            nesting,
                        )?;
                        outs.extend(out);
                    }
                    TurtleBlockOrTermOrList::Term(t) => {
                        let out = t.write_prefixed(
                            writer,
                            type_nn,
                            prefix_replacer,
                            in_degree_one_map,
                            nesting,
                        )?;
                        outs.extend(out);
                    }
                }
                Ok(outs)
            }
        }
    }
}

fn write_term_prefixed<W: Write>(
    writer: &mut W,
    term: &Term,
    prefix_replacer: &PrefixReplacer,
) -> std::io::Result<()> {
    match term.as_ref() {
        TermRef::NamedNode(nn) => prefix_replacer.write_with_prefix(writer, nn),
        TermRef::Literal(l) => {
            write!(writer, "\"")?;
            for c in l.value().chars() {
                write_escaped_char(c, writer)?;
            }
            write!(writer, "\"")?;
            if l.datatype() != xsd::STRING {
                if let Some(language) = l.language() {
                    write!(writer, "@{}", language)?;
                } else {
                    write!(writer, "^^")?;
                    prefix_replacer.write_with_prefix(writer, l.datatype())?;
                }
            }
            Ok(())
        }
        t => write!(writer, "{}", t),
    }
}

fn write_escaped_char<W: Write>(c: char, w: &mut W) -> std::io::Result<()> {
    match c {
        '\n' => {
            write!(w, "\\n")
        }
        '\t' => {
            write!(w, "\\t")
        }
        '\r' => {
            write!(w, "\\r")
        }
        '"' | '\\' => {
            write!(w, "\\{c}")
        }
        _ => {
            write!(w, "{c}")
        }
    }
}

impl TurtleBlock {
    pub(crate) fn write_block<W: Write>(
        &self,
        writer: &mut W,
        type_nn: &NamedNode,
        prefix_replacer: &PrefixReplacer,
        try_write_subject: bool,
        in_degree_one_map: &HashMap<u32, TurtleBlockOrTermOrList>,
        nesting: usize,
    ) -> std::io::Result<Vec<u32>> {
        let mut outs = Vec::new();
        let use_write_subject = if try_write_subject {
            if let Some(subject) = &self.subject {
                write_term_prefixed(writer, subject, prefix_replacer)?;
                true
            } else {
                false
            }
        } else {
            false
        };
        if !use_write_subject {
            writeln!(writer, "[")?;
        }
        let mut to_write = self.pred_term_map.len();
        if let Some(ts) = self.pred_term_map.get(type_nn) {
            for _ in 0..nesting {
                write!(writer, "    ")?;
            }
            write!(writer, "a ")?;

            let replaced = write_term_objects(
                writer,
                ts.as_slice(),
                type_nn,
                prefix_replacer,
                in_degree_one_map,
                nesting + 1,
            )?;
            outs.extend(replaced);
            to_write -= 1;
            if to_write > 0 {
                writeln!(writer, " ; ")?;
            }
        }
        for (key, values) in self.pred_term_map.range(..) {
            if key != type_nn {
                for _ in 0..nesting {
                    write!(writer, "    ")?;
                }
                prefix_replacer.write_with_prefix(writer, key.as_ref())?;
                write!(writer, " ")?;
                let replaced = write_term_objects(
                    writer,
                    values.as_slice(),
                    type_nn,
                    prefix_replacer,
                    in_degree_one_map,
                    nesting + 1,
                )?;
                outs.extend(replaced);
                to_write -= 1;
                if to_write > 0 {
                    writeln!(writer, " ;")?;
                }
            }
        }
        if !use_write_subject {
            writeln!(writer)?;
            if nesting > 1 {
                for _ in 0..nesting - 1 {
                    write!(writer, "    ")?;
                }
            }
            write!(writer, "]")?;
        }
        Ok(outs)
    }
}

fn write_term_objects<W: Write>(
    writer: &mut W,
    terms: &[TermOrList],
    type_nn: &NamedNode,
    prefix_replacer: &PrefixReplacer,
    in_degree_one_map: &HashMap<u32, TurtleBlockOrTermOrList>,
    nesting: usize,
) -> std::io::Result<Vec<u32>> {
    let mut outs = Vec::new();
    if terms.len() == 1 {
        let out = terms.first().unwrap().write_prefixed(
            writer,
            type_nn,
            prefix_replacer,
            in_degree_one_map,
            nesting,
        )?;
        outs.extend(out);
    } else {
        for (i, v) in terms.iter().enumerate() {
            let out =
                v.write_prefixed(writer, type_nn, prefix_replacer, in_degree_one_map, nesting)?;
            outs.extend(out);
            if i < terms.len() - 1 {
                write!(writer, ", ")?;
            }
        }
    }
    Ok(outs)
}

impl Triplestore {
    pub fn write_pretty_turtle<W: Write>(
        &self,
        writer: &mut W,
        graph: &NamedGraph,
        prefixes: &HashMap<String, NamedNode>,
    ) -> Result<(), TriplestoreError> {
        self.write_pretty(writer, graph, prefixes, PrettyFormat::Turtle)
    }

    /// Pretty Turtle without the prefix declarations (the prefixes are still used to abbreviate
    /// IRIs). Used to append batches to a Turtle document whose prefixes were declared once at
    /// the start. Writes nothing if the graph does not exist.
    pub fn write_pretty_turtle_without_prefix_declarations<W: Write>(
        &self,
        writer: &mut W,
        graph: &NamedGraph,
        prefixes: &HashMap<String, NamedNode>,
    ) -> Result<(), TriplestoreError> {
        if let Some(map) = self.graph_triples_map.get(graph) {
            self.write_pretty_body(
                writer,
                graph,
                map,
                prefixes,
                PrettySyntax::Turtle,
                &HashSet::new(),
            )?;
        }
        Ok(())
    }

    /// Pretty RDF/XML: one node element per subject, nested blank nodes and collections.
    pub fn write_pretty_rdfxml<W: Write>(
        &self,
        writer: &mut W,
        graph: &NamedGraph,
        prefixes: &HashMap<String, NamedNode>,
    ) -> Result<(), TriplestoreError> {
        self.write_pretty(writer, graph, prefixes, PrettyFormat::RdfXml)
    }

    fn write_pretty<W: Write>(
        &self,
        writer: &mut W,
        graph: &NamedGraph,
        prefixes: &HashMap<String, NamedNode>,
        format: PrettyFormat,
    ) -> Result<(), TriplestoreError> {
        let map = if let Some(map) = self.graph_triples_map.get(graph) {
            map
        } else {
            return Err(TriplestoreError::GraphDoesNotExist(graph.to_string()));
        };

        let write_err = |x: std::io::Error| TriplestoreError::WriteTurtleError(x.to_string());
        match format {
            PrettyFormat::Turtle => {
                let used = self.possibly_used_prefixes(map, prefixes)?;
                for (k, v) in &used {
                    writeln!(writer, "@prefix {}: {} .", k, v)
                        .map_err(|x| TriplestoreError::WriteTurtleError(x.to_string()))?;
                }
                if !used.is_empty() {
                    writeln!(writer)
                        .map_err(|x| TriplestoreError::WriteTurtleError(x.to_string()))?;
                }
                self.write_pretty_body(
                writer,
                graph,
                map,
                prefixes,
                PrettySyntax::Turtle,
                &HashSet::new(),
            )?;
            }
            PrettyFormat::RdfXml => {
                let ns = rdfxml::XmlNamespaces::new(prefixes, map.keys());
                ns.write_root_start(writer).map_err(write_err)?;
                self.write_pretty_body(
                    writer,
                    graph,
                    map,
                    prefixes,
                    PrettySyntax::RdfXml(&ns),
                    &HashSet::new(),
                )?;
                rdfxml::XmlNamespaces::write_root_end(writer).map_err(write_err)?;
            }
        }
        Ok(())
    }

    /// The blank nodes that are in more than one of the graphs. Blank node labels are shared
    /// by the graphs of a document, so these must be written with their label.
    fn blanks_in_several_graphs(
        &self,
        maps: &[&HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>],
    ) -> Result<HashSet<u32>, TriplestoreError> {
        let mut shared = HashSet::new();
        if maps.len() < 2 {
            return Ok(shared);
        }
        let mut seen: HashMap<u32, usize> = HashMap::new();
        for (i, map) in maps.iter().enumerate() {
            for m in map.values() {
                for ((s, o), t) in m {
                    let mut cols = vec![];
                    if s.is_blank_node() {
                        cols.push(SUBJECT_COL_NAME);
                    }
                    if o.is_blank_node() {
                        cols.push(OBJECT_COL_NAME);
                    }
                    if cols.is_empty() {
                        continue;
                    }
                    if let Some(lfs) = t.get_lazy_frame_slices()? {
                        for lf in lfs {
                            let exprs: Vec<_> = cols.iter().map(|c| col(*c)).collect();
                            let df = lf.select(exprs).collect().unwrap();
                            for c in &cols {
                                for u in df.column(c).unwrap().u32().unwrap().iter().flatten() {
                                    match seen.get(&u) {
                                        Some(j) if *j != i => {
                                            shared.insert(u);
                                        }
                                        Some(_) => {}
                                        None => {
                                            seen.insert(u, i);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(shared)
    }

    /// Pretty JSON-LD: a compacted document with one node object per subject of `top_graph` in
    /// the top level `@graph` (none if it is None or does not exist), followed by a graph object
    /// (`{"@id": ..., "@graph": [...]}`) for each of `named_graphs` that exists, in order.
    pub fn write_pretty_jsonld<W: Write>(
        &self,
        writer: &mut W,
        top_graph: Option<&NamedGraph>,
        named_graphs: &[NamedGraph],
        prefixes: &HashMap<String, NamedNode>,
    ) -> Result<(), TriplestoreError> {
        let mut named = Vec::with_capacity(named_graphs.len());
        for g in named_graphs {
            if matches!(g, NamedGraph::DefaultGraph) {
                return Err(TriplestoreError::UnsupportedWriteFormat(
                    "the default graph as a named graph in JSON-LD".to_string(),
                ));
            }
            if Some(g) == top_graph || named.iter().any(|(n, _)| *n == g) {
                continue;
            }
            if let Some(map) = self.graph_triples_map.get(g) {
                named.push((g, map));
            }
        }
        let top = top_graph.and_then(|g| self.graph_triples_map.get(g).map(|m| (g, m)));

        let all_maps: Vec<_> = top
            .iter()
            .chain(named.iter())
            .map(|(_, m)| *m)
            .collect();
        let mut used: BTreeMap<&String, &NamedNode> = BTreeMap::new();
        for map in &all_maps {
            used.extend(self.possibly_used_prefixes(map, prefixes)?);
        }
        // An IRI starting with "<prefix name>:" (e.g. <foaf:Person>) would be read as a compact
        // IRI, so such prefixes are left out (they only abbreviate). Where the dictionary can not
        // tell (on disk), the prefix is kept, and such an IRI fails when it is written.
        {
            let cats = self.global_cats.read()?;
            used.retain(|name, _| {
                let scheme = format!("{name}:");
                cats.has_string_with_prefix(&BaseRDFNodeType::IRI, &scheme) != Some(true)
                    && !all_maps
                        .iter()
                        .flat_map(|m| m.keys())
                        .any(|p| p.as_str().starts_with(&scheme))
            });
        }
        let ctx = jsonld::JsonLdContext::new(used.into_iter());
        let shared_blanks = self.blanks_in_several_graphs(&all_maps)?;

        let write_err = |x: std::io::Error| TriplestoreError::WriteTurtleError(x.to_string());
        ctx.write_start(writer).map_err(write_err)?;
        let mut wrote_top = false;
        if let Some((graph, map)) = top {
            wrote_top = self.write_pretty_body(
                writer,
                graph,
                map,
                prefixes,
                PrettySyntax::JsonLd(&ctx, jsonld::NODE_DEPTH),
                &shared_blanks,
            )?;
        }
        for (g, map) in named {
            ctx.write_graph_start(writer, g, wrote_top)
                .map_err(write_err)?;
            let wrote_nodes = self.write_pretty_body(
                writer,
                g,
                map,
                prefixes,
                PrettySyntax::JsonLd(&ctx, jsonld::GRAPH_NODE_DEPTH),
                &shared_blanks,
            )?;
            jsonld::JsonLdContext::write_graph_end(writer, wrote_nodes).map_err(write_err)?;
            wrote_top = true;
        }
        jsonld::JsonLdContext::write_end(writer, wrote_top).map_err(write_err)
    }

    /// The prefixes whose namespace starts an IRI that may be written: a predicate, a literal
    /// datatype or an IRI in the (global) IRI dictionary. The dictionary is shared by all graphs
    /// and probed with one ordered lookup per prefix, so this may keep a prefix that is not used
    /// in the graph, but never drops one that is.
    fn possibly_used_prefixes<'a>(
        &self,
        map: &HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
        prefixes: &'a HashMap<String, NamedNode>,
    ) -> Result<Vec<(&'a String, &'a NamedNode)>, TriplestoreError> {
        let mut datatypes = HashSet::new();
        for m in map.values() {
            for (s, o) in m.keys() {
                for t in [s, o] {
                    if let BaseRDFNodeType::Literal(dt, _) = t {
                        if dt.as_ref() != xsd::STRING && dt.as_ref() != rdf::LANG_STRING {
                            datatypes.insert(dt.as_str());
                        }
                    }
                }
            }
        }
        let cats = self.global_cats.read()?;
        let mut used: Vec<_> = prefixes
            .iter()
            .filter(|(_, ns)| {
                let ns = ns.as_str();
                map.keys().any(|p| p.as_str().starts_with(ns))
                    || datatypes.iter().any(|dt| dt.starts_with(ns))
                    || cats.may_have_string_with_prefix(&BaseRDFNodeType::IRI, ns)
            })
            .collect();
        used.sort_unstable_by(|a, b| a.0.cmp(b.0));
        Ok(used)
    }

    /// Pretty RDF/XML for several graphs in one document, written in the given order (each graph's
    /// subjects in their own section). `extra_namespace_iris` are IRIs (e.g. the classes used as
    /// node element names) whose namespaces should be declared besides those of the predicates.
    /// IRIs starting with `fragment_base` (which should end with '#') are written relative to the
    /// document, as rdf:ID / "#..." references, as CIM/XML requires.
    pub fn write_pretty_rdfxml_graphs<W: Write>(
        &self,
        writer: &mut W,
        graphs: &[NamedGraph],
        prefixes: &HashMap<String, NamedNode>,
        extra_namespace_iris: &[NamedNode],
        fragment_base: Option<&str>,
    ) -> Result<(), TriplestoreError> {
        let mut maps = Vec::with_capacity(graphs.len());
        for graph in graphs {
            if let Some(map) = self.graph_triples_map.get(graph) {
                maps.push((graph, map));
            } else {
                return Err(TriplestoreError::GraphDoesNotExist(graph.to_string()));
            }
        }
        let ns = rdfxml::XmlNamespaces::new(
            prefixes,
            maps.iter()
                .flat_map(|(_, map)| map.keys())
                .chain(extra_namespace_iris.iter()),
        )
        .with_fragment_base(fragment_base);
        ns.write_root_start(writer)
            .map_err(|x| TriplestoreError::WriteTurtleError(x.to_string()))?;
        let shared_blanks =
            self.blanks_in_several_graphs(&maps.iter().map(|(_, m)| *m).collect::<Vec<_>>())?;
        for (graph, map) in maps {
            self.write_pretty_body(
                writer,
                graph,
                map,
                prefixes,
                PrettySyntax::RdfXml(&ns),
                &shared_blanks,
            )?;
        }
        rdfxml::XmlNamespaces::write_root_end(writer)
            .map_err(|x| TriplestoreError::WriteTurtleError(x.to_string()))
    }

    fn write_pretty_body<W: Write>(
        &self,
        writer: &mut W,
        graph: &NamedGraph,
        map: &HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
        prefixes: &HashMap<String, NamedNode>,
        syntax: PrettySyntax,
        // Blank nodes that must be written with their label (not as [ ... ], inlined or as part
        // of a list), e.g. as they are also used in another graph of the same document.
        labeled_blanks: &HashSet<u32>,
    ) -> Result<bool, TriplestoreError> {
        let prefix_replacer = PrefixReplacer::new(prefixes);
        // JSON-LD node objects are written with a separator before each, which is dropped for the
        // first one written.
        let mut wrote_any = false;
        let mut emit = |writer: &mut W, w: &[u8]| -> Result<(), TriplestoreError> {
            let w = if !wrote_any && matches!(syntax, PrettySyntax::JsonLd(..)) {
                w.strip_prefix(jsonld::NODE_SEPARATOR).unwrap_or(w)
            } else {
                w
            };
            wrote_any |= !w.is_empty();
            writer
                .write_all(w)
                .map_err(|x| TriplestoreError::WriteTurtleError(x.to_string()))
        };

        let (mut blanks_in_degree_zero, mut blanks_in_degree_one) =
            self.create_blank_in_degree_zero_and_one_sets(map)?;
        // List nodes are written separately from their other properties, so they must be
        // labeled rather than written as [ ... ] or inlined, or they would split into
        // several blank nodes.
        for u in self
            .list_nodes_with_other_properties(map)?
            .iter()
            .chain(labeled_blanks)
        {
            blanks_in_degree_zero.remove(u);
            blanks_in_degree_one.remove(u);
        }

        // The rdf:first and rdf:rest triples of these nodes are written by the lists.
        let mut list_members = HashSet::new();
        let mut lists_map =
            self.create_lists_map(map, graph, &blanks_in_degree_one, &mut list_members)?;

        let no_subjects = HashSet::new();
        let mut in_degree_one_blocks_map = HashMap::new();
        for (p, m) in map.iter() {
            for (k, t) in m.iter() {
                let (s, o) = k;
                if !s.is_blank_node() {
                    continue;
                }
                let skip_subjects = if is_list_pred_and_subject_type(p, s) {
                    &list_members
                } else {
                    &no_subjects
                };

                if let Some(lfs) = t.get_lazy_frame_slices()? {
                    for lf in lfs {
                        let df = lf.collect().unwrap();
                        update_blocks_map(
                            &mut in_degree_one_blocks_map,
                            &df,
                            p,
                            s,
                            o,
                            self.global_cats.clone(),
                            Some(&blanks_in_degree_one),
                            skip_subjects,
                            &blanks_in_degree_zero,
                            &blanks_in_degree_one,
                        )?;
                    }
                }
            }
        }
        let mut in_degree_one_blocks_map: HashMap<_, _> = in_degree_one_blocks_map
            .into_iter()
            .map(|(k, v)| (k, TurtleBlockOrTermOrList::TurtleBlock(v)))
            .collect();
        if let Some(lists) = &mut lists_map {
            let keys: Vec<_> = lists.keys().cloned().collect();
            for k in keys {
                if blanks_in_degree_one.contains(&k) {
                    let l = lists.remove(&k).unwrap();
                    in_degree_one_blocks_map.insert(k, TurtleBlockOrTermOrList::Term(l));
                }
            }
        }
        for u in &blanks_in_degree_one {
            if !in_degree_one_blocks_map.contains_key(u) {
                in_degree_one_blocks_map.insert(
                    *u,
                    TurtleBlockOrTermOrList::Term(TermOrList::Elem(Arc::new(Term::BlankNode(
                        BlankNode::new_unchecked(blank_node_to_normal_string(u)),
                    )))),
                );
            }
        }

        let used_iri_subjects = HashSet::new();
        let used_blank_subjects: HashSet<u32> = in_degree_one_blocks_map.keys().cloned().collect();
        let used_blank_list_subjects: HashSet<u32> =
            used_blank_subjects.union(&list_members).cloned().collect();
        let type_nn = rdf::TYPE.into_owned();

        // Max 2 iterations, iri and blank
        for is_iri in [true, false] {
            // Safe because this was how we found the driver and k
            let subject_type = if is_iri {
                BaseRDFNodeType::IRI
            } else {
                BaseRDFNodeType::BlankNode
            };
            // The parallel work below runs polars collects (column decoding and lazy frame
            // collects) that need free rayon workers. Running it on polars' pool or rayon's global
            // pool deadlocks when all their threads are blocked in this loop, so it gets its own.
            let n_threads = PRETTY_WRITER_POOL.current_num_threads();
            let mut spaced_strings = self
                .global_cats
                .read()?
                .get_spaced_strings(STRIDE, &subject_type);
            spaced_strings.reverse();
            while !spaced_strings.is_empty() {
                let use_used_subjects = if is_iri {
                    &used_iri_subjects
                } else {
                    &used_blank_subjects
                };
                let mut thread_strings = Vec::with_capacity(n_threads);
                for _ in 0..n_threads {
                    if !spaced_strings.is_empty() {
                        thread_strings.push(spaced_strings.pop().unwrap());
                    }
                }
                let r: Result<Vec<_>, TriplestoreError> = PRETTY_WRITER_POOL.install(|| {
                    thread_strings
                        .into_par_iter()
                        .map(|(from_string, to_string)| {
                            let r = self.create_block_segment(
                                subject_type.is_iri(),
                                map,
                                from_string.as_str(),
                                to_string.as_str(),
                                use_used_subjects,
                                &used_blank_list_subjects,
                                &blanks_in_degree_zero,
                                &blanks_in_degree_one,
                            );
                            r
                        })
                        .collect()
                });

                let r = r?;

                let written: Result<(Vec<_>, Vec<_>), TriplestoreError> = PRETTY_WRITER_POOL
                    .install(|| {
                        r.into_par_iter()
                            .map(|new_map| {
                                let mut writer: Vec<u8> = Vec::new();
                                let replaced = write_blocks(
                                    &mut writer,
                                    new_map,
                                    &type_nn,
                                    &prefix_replacer,
                                    syntax,
                                    &in_degree_one_blocks_map,
                                    self.global_cats.clone(),
                                    &subject_type,
                                )?;
                                Ok((writer, replaced))
                            })
                            .collect()
                    });
                let (written, replaced) = written?;
                for r in replaced {
                    for u in r {
                        in_degree_one_blocks_map.remove(&u).unwrap();
                    }
                }
                for w in written {
                    emit(writer, &w)?;
                }
            }
        }

        if let Some(lists_map) = lists_map {
            for (u, mut l) in lists_map {
                let subject = if blanks_in_degree_zero.contains(&u) {
                    None
                } else {
                    let bl = BlankNode::new_unchecked(blank_node_to_normal_string(&u));
                    Some(Term::BlankNode(bl))
                };
                let first = l.remove_first();
                let has_subject = subject.is_some();
                let turtle_block = TurtleBlock {
                    subject,
                    pred_term_map: BTreeMap::from_iter([
                        (rdf::FIRST.into_owned(), vec![first]),
                        (rdf::REST.into_owned(), vec![l]),
                    ]),
                };
                let mut w: Vec<u8> = Vec::new();
                write_block(
                    &mut w,
                    &turtle_block,
                    &type_nn,
                    &prefix_replacer,
                    syntax,
                    &in_degree_one_blocks_map,
                    has_subject,
                )?;
                emit(writer, &w)?;
            }
        }
        Ok(wrote_any)
    }

    fn create_block_segment(
        &self,
        is_iri: bool,
        map: &HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
        string_start: &str,
        string_ends: &str,
        used_subjects: &HashSet<u32>,
        // Used instead of used_subjects for the rdf:first and rdf:rest of blank nodes: these
        // also include the list nodes, whose rdf:first and rdf:rest are written by their list.
        used_list_subjects: &HashSet<u32>,
        blanks_in_degree_zero: &HashSet<u32>,
        blanks_in_degree_one: &HashSet<u32>,
    ) -> Result<HashMap<u32, TurtleBlock>, TriplestoreError> {
        let mut blocks_map = HashMap::new();

        // Stride through and do the "left join"
        for (p, m) in map.iter() {
            for (k, t) in m.iter() {
                if k.0.is_iri() != is_iri {
                    continue;
                }
                let used_subjects = if is_list_pred_and_subject_type(p, &k.0) {
                    used_list_subjects
                } else {
                    used_subjects
                };

                let (s, o) = k;

                if &t.subject_type != s {
                    continue;
                }

                if let Some(lf) = t.get_lazy_frame_between_subject_strings(
                    string_start,
                    string_ends,
                    self.global_cats.clone(),
                )? {
                    let other_df = lf.collect().unwrap();
                    update_blocks_map(
                        &mut blocks_map,
                        &other_df,
                        p,
                        s,
                        o,
                        self.global_cats.clone(),
                        None,
                        used_subjects,
                        blanks_in_degree_zero,
                        blanks_in_degree_one,
                    )?;
                }
            }
        }
        Ok(blocks_map)
    }

    /// The well-formed RDF lists with blank node list nodes, by their first node. Each list node
    /// has one rdf:first and one rdf:rest, and the nodes after the first are only the object of
    /// the rdf:rest of the node before (and have no other properties). The list nodes are added to
    /// `list_members`: their rdf:first and rdf:rest triples are written by the list. The
    /// rdf:first and rdf:rest triples of other nodes (e.g. of a list without rdf:nil, or with a
    /// node that is referred to twice) are written as any other triples.
    fn create_lists_map(
        &self,
        map: &HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
        graph: &NamedGraph,
        blank_in_degree_one_set: &HashSet<u32>,
        list_members: &mut HashSet<u32>,
    ) -> Result<Option<HashMap<u32, TermOrList>>, TriplestoreError> {
        let mut inv_rest_blank_blank_map: HashMap<u32, Vec<u32>> = HashMap::new();
        let mut first_blank_term_map = HashMap::new();
        let mut first_counts: HashMap<u32, usize> = HashMap::new();
        let mut rest_counts: HashMap<u32, usize> = HashMap::new();
        let mut blank_lists_map = HashMap::new();

        if let Some(first_triple_map) = map.get(&rdf::FIRST.into_owned()) {
            for ((s, o), v) in first_triple_map {
                if s.is_blank_node() {
                    let lfs = v.get_lazy_frame_slices()?;
                    if let Some(lfs) = lfs {
                        let df = concat_lfs_subject_object(lfs).collect().unwrap();
                        let subject_u32s = df.column(SUBJECT_COL_NAME).unwrap().u32().unwrap();
                        for s in subject_u32s.iter().flatten() {
                            *first_counts.entry(s).or_default() += 1;
                        }
                        let os = o.default_stored_cat_state();

                        let to_replace_terms = if o.is_blank_node() {
                            let to_replace_terms: Vec<_> = df
                                .column(OBJECT_COL_NAME)
                                .unwrap()
                                .u32()
                                .unwrap()
                                .iter()
                                .map(|x| {
                                    if blank_in_degree_one_set.contains(&x.unwrap()) {
                                        Some(TermOrList::BlankPlaceholder(x.unwrap()))
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            Some(to_replace_terms)
                        } else {
                            None
                        };

                        let terms = column_as_terms(
                            df.column(OBJECT_COL_NAME).unwrap(),
                            &RDFNodeState::from_bases(o.clone(), os),
                            self.global_cats.clone(),
                            true,
                        );
                        if let Some(to_replace_terms) = to_replace_terms {
                            for ((s, t), r) in subject_u32s.iter().zip(terms).zip(to_replace_terms)
                            {
                                if let Some(r) = r {
                                    first_blank_term_map.insert(s.unwrap(), r);
                                } else {
                                    first_blank_term_map
                                        .insert(s.unwrap(), TermOrList::Elem(Arc::new(t.unwrap())));
                                }
                            }
                        } else {
                            for (s, t) in subject_u32s.iter().zip(terms) {
                                first_blank_term_map
                                    .insert(s.unwrap(), TermOrList::Elem(Arc::new(t.unwrap())));
                            }
                        }
                    }
                }
            }
        } else {
            return Ok(None);
        }
        let res = self
            .query_parsed(
                &Query::Select {
                    pattern: GraphPattern::Project {
                        inner: Box::new(GraphPattern::Filter {
                            expr: Expression::FunctionCall(
                                Function::IsBlank,
                                vec![Expression::Variable(Variable::new_unchecked(
                                    SUBJECT_COL_NAME,
                                ))],
                            ),
                            inner: Box::new(GraphPattern::Bgp {
                                patterns: vec![TriplePattern {
                                    subject: TermPattern::Variable(Variable::new_unchecked(
                                        SUBJECT_COL_NAME,
                                    )),
                                    predicate: NamedNodePattern::NamedNode(rdf::REST.into_owned()),
                                    object: TermPattern::NamedNode(rdf::NIL.into_owned()),
                                }],
                            }),
                        }),
                        variables: vec![Variable::new_unchecked(SUBJECT_COL_NAME)],
                    },
                    dataset: None,
                    base_iri: None,
                },
                None,
                &QuerySettings {
                    include_graphs: vec![],
                    max_rows: None,
                    strict_project: false,
                },
                Some(graph),
                false,
            )
            .map_err(|x| TriplestoreError::SparqlQueryError(x.to_string()))?;
        if let QueryResultKind::Select(sm) = res.kind {
            if sm.mappings.height() == 0 {
                return Ok(None);
            }
            if sm
                .rdf_node_types
                .get(SUBJECT_COL_NAME)
                .unwrap()
                .map
                .contains_key(&BaseRDFNodeType::IRI)
            {
                return Err(TriplestoreError::WriteTurtleError(
                    "No support for pretty turtle when IRI is subject of rdf:rest".to_string(),
                ));
            }
            let su32 = sm.mappings.column(SUBJECT_COL_NAME).unwrap().u32().unwrap();
            if let Some(rest_triple_map) = map.get(&rdf::REST.into_owned()) {
                for ((s, o), v) in rest_triple_map {
                    if !s.is_blank_node() {
                        continue;
                    }
                    let lf = v.get_lazy_frame_slices()?;
                    if let Some(lfs) = lf {
                        let df = concat_lfs_subject_object(lfs).collect().unwrap();
                        let subject_u32s = df.column(SUBJECT_COL_NAME).unwrap().u32().unwrap();
                        for s in subject_u32s.iter().flatten() {
                            *rest_counts.entry(s).or_default() += 1;
                        }
                        // The last nodes are found above, their rdf:rest is rdf:nil.
                        if o.is_blank_node() {
                            let object_u32s = df.column(OBJECT_COL_NAME).unwrap().u32().unwrap();
                            for (s, o) in subject_u32s.iter().zip(object_u32s.iter()) {
                                inv_rest_blank_blank_map
                                    .entry(o.unwrap())
                                    .or_default()
                                    .push(s.unwrap());
                            }
                        }
                    }
                }
            }
            let is_list_node = |u: &u32| {
                first_counts.get(u) == Some(&1)
                    && rest_counts.get(u) == Some(&1)
                    && first_blank_term_map.contains_key(u)
            };
            for u in su32.iter().flatten() {
                if is_list_node(&u) {
                    blank_lists_map.insert(
                        u,
                        TermOrList::List(vec![first_blank_term_map.get(&u).unwrap().clone()]),
                    );
                }
            }
            // Walk the lists from their last node towards their first. A node is inside a list
            // (not its first node) if its only referrer is the rdf:rest of the node before.
            let mut finished_blank_lists_map = HashMap::new();
            while !blank_lists_map.is_empty() {
                let mut new_blank_lists_map = HashMap::new();
                for (blank, mut list) in blank_lists_map {
                    list_members.insert(blank);
                    let previous = match inv_rest_blank_blank_map.get(&blank) {
                        Some(previous)
                            if previous.len() == 1
                                && blank_in_degree_one_set.contains(&blank)
                                && is_list_node(&previous[0])
                                && !list_members.contains(&previous[0]) =>
                        {
                            Some(previous[0])
                        }
                        _ => None,
                    };
                    if let Some(previous) = previous {
                        list.push(first_blank_term_map.get(&previous).unwrap().clone());
                        new_blank_lists_map.insert(previous, list);
                    } else {
                        list.reverse();
                        finished_blank_lists_map.insert(blank, list);
                    }
                }
                blank_lists_map = new_blank_lists_map;
            }
            blank_lists_map = finished_blank_lists_map;
            if blank_lists_map.is_empty() {
                return Ok(None);
            }
        } else {
            unreachable!("Should never happen")
        }
        // We fix the nested lists here
        let mut seen_lists = HashMap::new();
        let keys = blank_lists_map.keys().cloned().collect::<Vec<_>>();
        for k in keys {
            if let Some(l) = blank_lists_map.remove(&k) {
                if let TermOrList::List(l) = l {
                    let mut new_l = Vec::with_capacity(l.len());
                    for l in l {
                        if let TermOrList::BlankPlaceholder(u) = l {
                            if let Some(l) = blank_lists_map.remove(&u) {
                                new_l.push(l);
                            } else if let Some(l) = seen_lists.remove(&u) {
                                new_l.push(l);
                            } else {
                                new_l.push(TermOrList::BlankPlaceholder(u));
                            }
                        } else {
                            new_l.push(l);
                        }
                    }
                    seen_lists.insert(k, TermOrList::List(new_l));
                } else {
                    seen_lists.insert(k, l);
                }
            }
        }

        Ok(Some(seen_lists))
    }

    /// Blank nodes that are subjects of rdf:first and also subjects of other predicates.
    fn list_nodes_with_other_properties(
        &self,
        map: &HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
    ) -> Result<HashSet<u32>, TriplestoreError> {
        let blank_subjects = |t: &Triples| -> Result<Vec<u32>, TriplestoreError> {
            let mut out = vec![];
            if let Some(lfs) = t.get_lazy_frame_slices()? {
                for lf in lfs {
                    let df = lf.select([col(SUBJECT_COL_NAME)]).collect().unwrap();
                    out.extend(
                        df.column(SUBJECT_COL_NAME)
                            .unwrap()
                            .u32()
                            .unwrap()
                            .iter()
                            .flatten(),
                    );
                }
            }
            Ok(out)
        };
        let mut list_nodes = HashSet::new();
        if let Some(m) = map.get(&rdf::FIRST.into_owned()) {
            for ((s, _), t) in m {
                if s.is_blank_node() {
                    list_nodes.extend(blank_subjects(t)?);
                }
            }
        }
        let mut out = HashSet::new();
        if list_nodes.is_empty() {
            return Ok(out);
        }
        for (p, m) in map {
            if p.as_ref() == rdf::FIRST || p.as_ref() == rdf::REST {
                continue;
            }
            for ((s, _), t) in m {
                if s.is_blank_node() {
                    out.extend(
                        blank_subjects(t)?
                            .into_iter()
                            .filter(|u| list_nodes.contains(u)),
                    );
                }
            }
        }
        Ok(out)
    }

    fn create_blank_in_degree_zero_and_one_sets(
        &self,
        map: &HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
    ) -> Result<(HashSet<u32>, HashSet<u32>), TriplestoreError> {
        let mut out_map: HashMap<u32, u8> = HashMap::new();
        // The blank node subject of the last edge into each blank node. For blank nodes with
        // in-degree one this is the block they are inlined into.
        let mut blank_parent: HashMap<u32, u32> = HashMap::new();
        for m in map.values() {
            for (k, v) in m {
                let (subject_type, object_type) = k;
                if object_type.is_blank_node() {
                    if let Some(lfs) = v.get_lazy_frame_slices()? {
                        for lf in lfs {
                            let df = lf
                                .select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)])
                                .collect()
                                .unwrap();
                            let objects_u32_iter =
                                df.column(OBJECT_COL_NAME).unwrap().u32().unwrap();
                            let subjects_u32 = if subject_type.is_blank_node() {
                                Some(df.column(SUBJECT_COL_NAME).unwrap().u32().unwrap())
                            } else {
                                None
                            };
                            for (i, u) in objects_u32_iter.iter().enumerate() {
                                let u = u.unwrap();
                                if let Some(in_deg) = out_map.get_mut(&u) {
                                    *in_deg = in_deg.saturating_add(1);
                                } else {
                                    out_map.insert(u, 1);
                                }
                                if let Some(subjects_u32) = subjects_u32 {
                                    blank_parent.insert(u, subjects_u32.get(i).unwrap());
                                } else {
                                    blank_parent.remove(&u);
                                }
                            }
                        }
                    }
                }
            }
        }
        let mut zero_out_set: HashSet<u32> = HashSet::new();
        for m in map.values() {
            for (k, v) in m {
                let subject_type = &k.0;
                if subject_type.is_blank_node() {
                    if let Some(lfs) = v.get_lazy_frame_slices()? {
                        for lf in lfs {
                            let df = lf.select([col(SUBJECT_COL_NAME)]).collect().unwrap();
                            let subjects_u32_iter =
                                df.column(SUBJECT_COL_NAME).unwrap().u32().unwrap();
                            for u in subjects_u32_iter.iter() {
                                let u = u.unwrap();
                                if !out_map.contains_key(&u) {
                                    zero_out_set.insert(u);
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut out_one = HashSet::new();
        for (u, v) in out_map {
            if v == 1 {
                out_one.insert(u);
            }
        }
        // A blank node with in-degree one is inlined into the block of its only referrer. If
        // following referrers leads back to the node itself (e.g. _:a :p _:a, or _:a :p _:b .
        // _:b :p _:a), no block on the cycle is ever written as a top-level block, and the
        // triples would be lost. Such nodes are labeled instead.
        for u in find_parent_cycles(&out_one, &blank_parent) {
            out_one.remove(&u);
        }
        Ok((zero_out_set, out_one))
    }
}

/// Returns the nodes of `nodes` that lie on a cycle when following `parent` edges, where the
/// walk stops at nodes outside `nodes`. Each node has at most one parent, so every cycle is
/// found by walking from each node until the walk reaches a node already seen.
fn find_parent_cycles(nodes: &HashSet<u32>, parent: &HashMap<u32, u32>) -> Vec<u32> {
    const ON_PATH: u8 = 1;
    const DONE: u8 = 2;
    let mut state: HashMap<u32, u8> = HashMap::with_capacity(nodes.len());
    let mut on_cycle = vec![];
    let mut path = vec![];
    for start in nodes {
        path.clear();
        let mut cur = *start;
        loop {
            match state.get(&cur) {
                None => {
                    state.insert(cur, ON_PATH);
                    path.push(cur);
                    match parent.get(&cur) {
                        Some(p) if nodes.contains(p) => cur = *p,
                        _ => break,
                    }
                }
                Some(&ON_PATH) => {
                    let pos = path.iter().position(|x| *x == cur).unwrap();
                    on_cycle.extend_from_slice(&path[pos..]);
                    break;
                }
                Some(_) => break,
            }
        }
        for u in &path {
            state.insert(*u, DONE);
        }
    }
    on_cycle
}

fn update_blocks_map(
    map: &mut HashMap<u32, TurtleBlock>,
    df: &DataFrame,
    pred: &NamedNode,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
    global_cats: LockedCats,
    maybe_current_subjects: Option<&HashSet<u32>>,
    used_subjects: &HashSet<u32>,
    in_degree_zero_blanks: &HashSet<u32>,
    in_degree_one_blanks: &HashSet<u32>,
) -> Result<(), TriplestoreError> {
    let subj_u32s = df.column(SUBJECT_COL_NAME).unwrap().u32().unwrap();
    let subj_terms = column_as_terms(
        df.column(SUBJECT_COL_NAME).unwrap(),
        &RDFNodeState::from_bases(
            subject_type.clone(),
            subject_type.default_stored_cat_state(),
        ),
        global_cats.clone(),
        true,
    );
    let mut to_replace = if object_type.is_blank_node() {
        let obj_u32s = df.column(OBJECT_COL_NAME).unwrap().u32().unwrap();
        let mut obj_deferred = Vec::with_capacity(obj_u32s.len());
        for u in obj_u32s.iter() {
            let u = u.unwrap();
            if in_degree_one_blanks.contains(&u) {
                obj_deferred.push(Some(TermOrList::BlankPlaceholder(u)));
            } else {
                obj_deferred.push(None)
            }
        }
        obj_deferred.reverse();
        Some(obj_deferred)
    } else {
        None
    };
    let obj_terms = column_as_terms(
        df.column(OBJECT_COL_NAME).unwrap(),
        &RDFNodeState::from_bases(object_type.clone(), object_type.default_stored_cat_state()),
        global_cats.clone(),
        true,
    );
    for ((s_u32, s), o) in subj_u32s.iter().zip(subj_terms).zip(obj_terms) {
        let s_u32 = s_u32.unwrap();
        let s = if subject_type.is_blank_node() && in_degree_zero_blanks.contains(&s_u32) {
            None
        } else {
            s
        };
        let o = if let Some(to_replace) = &mut to_replace {
            if let Some(defer) = to_replace.pop().unwrap() {
                defer
            } else {
                TermOrList::Elem(Arc::new(o.unwrap()))
            }
        } else {
            TermOrList::Elem(Arc::new(o.unwrap()))
        };
        // Very important that this happens after the replace iterator has been popped
        if used_subjects.contains(&s_u32) {
            continue;
        }
        if let Some(current_subjects) = maybe_current_subjects {
            if !current_subjects.contains(&s_u32) {
                continue;
            }
        }

        let block = if let Some(block) = map.get_mut(&s_u32) {
            block
        } else {
            map.insert(
                s_u32,
                TurtleBlock {
                    subject: s,
                    pred_term_map: Default::default(),
                },
            );
            map.get_mut(&s_u32).unwrap()
        };
        if let Some(m) = block.pred_term_map.get_mut(pred) {
            m.push(o);
        } else {
            block.pred_term_map.insert(pred.clone(), vec![o]);
        }
    }
    Ok(())
}

fn write_blocks<W: Write>(
    writer: &mut W,
    blocks_map: HashMap<u32, TurtleBlock>,
    type_nn: &NamedNode,
    prefix_replacer: &PrefixReplacer,
    syntax: PrettySyntax,
    in_degree_one_map: &HashMap<u32, TurtleBlockOrTermOrList>,
    global_cats: LockedCats,
    subject_dt: &BaseRDFNodeType,
) -> Result<Vec<u32>, TriplestoreError> {
    let keys: HashSet<_> = blocks_map.keys().cloned().collect();
    // Only the order of the subjects in this segment matters, so a sort rank map suffices (a
    // global rank map walks the whole dictionary, for every segment).
    let rank_map = global_cats.read()?.sort_rank_map(&keys, subject_dt);
    let inv_rank_map: BTreeMap<_, _> = rank_map.into_iter().map(|(x, y)| (y, x)).collect();

    let out: Result<Vec<Vec<_>>, TriplestoreError> = inv_rank_map
        .range(..)
        .map(|(_, k)| {
            if let Some(block) = blocks_map.get(k) {
                write_block(
                    writer,
                    block,
                    type_nn,
                    prefix_replacer,
                    syntax,
                    in_degree_one_map,
                    true,
                )
            } else {
                Ok(vec![])
            }
        })
        .collect();
    let outs = out?.into_iter().flatten().collect();
    Ok(outs)
}

/// Writes a top level subject block in the given syntax. Returns the in-degree-one blank nodes it
/// inlined.
fn write_block<W: Write>(
    writer: &mut W,
    block: &TurtleBlock,
    type_nn: &NamedNode,
    prefix_replacer: &PrefixReplacer,
    syntax: PrettySyntax,
    in_degree_one_map: &HashMap<u32, TurtleBlockOrTermOrList>,
    write_subject: bool,
) -> Result<Vec<u32>, TriplestoreError> {
    let r = match syntax {
        PrettySyntax::Turtle => block
            .write_block(
                writer,
                type_nn,
                prefix_replacer,
                write_subject,
                in_degree_one_map,
                1,
            )
            .and_then(|replaced| writeln!(writer, " .\n").map(|_| replaced)),
        PrettySyntax::RdfXml(ns) => {
            rdfxml::write_block(writer, block, ns, in_degree_one_map, 1, write_subject)
        }
        PrettySyntax::JsonLd(ctx, depth) => writer
            .write_all(jsonld::NODE_SEPARATOR)
            .and_then(|_| jsonld::write_indent(writer, depth))
            .and_then(|_| {
                jsonld::write_block(writer, block, ctx, in_degree_one_map, depth, write_subject)
            }),
    };
    r.map_err(|x| TriplestoreError::WriteTurtleError(x.to_string()))
}

struct PrefixReplacer {
    aho_corasick: AhoCorasick,
    replacements: Vec<String>,
}

impl PrefixReplacer {
    pub fn new(prefixes: &HashMap<String, NamedNode>) -> Self {
        let (patterns, replacements): (Vec<_>, Vec<_>) = prefixes
            .iter()
            .map(|(x, y)| (y.as_str(), x.to_string()))
            .unzip();
        let mut builder = AhoCorasick::builder();
        builder.match_kind(MatchKind::LeftmostLongest);
        let aho_corasick = builder.build(patterns).unwrap();
        Self {
            aho_corasick,
            replacements,
        }
    }

    pub fn write_with_prefix<W: Write>(
        &self,
        writer: &mut W,
        nn: NamedNodeRef,
    ) -> std::io::Result<()> {
        let mut repl = self.aho_corasick.find_iter(nn.as_str());
        if let Some(find) = repl.next() {
            let rest = &nn.as_str()[find.end()..];
            write!(
                writer,
                "{}:",
                self.replacements.get(find.pattern().as_usize()).unwrap(),
            )?;
            for c in rest.chars() {
                if matches!(
                    c,
                    '~' | '!'
                        | '$'
                        | '&'
                        | '\''
                        | '('
                        | ')'
                        | '*'
                        | '+'
                        | ','
                        | ';'
                        | '='
                        | '/'
                        | '?'
                        | '#'
                        | '@'
                        | '%'
                ) {
                    write!(writer, "\\{}", c)?;
                } else {
                    write!(writer, "{}", c)?;
                }
            }
            Ok(())
        } else {
            write!(writer, "{}", nn)
        }
    }
}

fn concat_lfs_subject_object(lfs: Vec<LazyFrame>) -> LazyFrame {
    let sel_lfs: Vec<_> = lfs
        .into_iter()
        .map(|lf| lf.select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)]))
        .collect();

    concat(
        sel_lfs,
        UnionArgs {
            parallel: true,
            rechunk: false,
            to_supertypes: false,
            diagonal: false,
            from_partitioned_ds: false,
            maintain_order: true,
            strict: false,
        },
    )
    .unwrap()
}

fn is_list_pred_and_subject_type(nn: &NamedNode, subject_type: &BaseRDFNodeType) -> bool {
    (nn.as_str() == rdf::FIRST || nn.as_str() == rdf::REST) && subject_type.is_blank_node()
}

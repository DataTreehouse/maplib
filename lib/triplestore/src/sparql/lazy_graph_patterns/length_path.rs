//! The `maplib:path` magic predicate: paths whose length (number of steps) is between given
//! bounds, optionally with the length and the nodes of each path.
//!
//! ```sparql
//! PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#>
//! SELECT ?s ?o ?len WHERE {
//!   ?s maplib:path [
//!     maplib:predicate ex:p, ex:q ;      # steps follow ex:p or ex:q forwards
//!     maplib:inversePredicate ex:r ;     # ... or ex:r backwards (from object to subject)
//!     maplib:minLength 2 ;               # default 1
//!     maplib:maxLength 4 ;               # default unbounded
//!     maplib:object ?o ;                 # the other end of the path
//!     maplib:length ?len                 # optional: the length of the path
//!   ]
//! }
//! ```
//!
//! Further options: `maplib:pathMode "walk" | "acyclic" | "simple"` (default "walk"),
//! `maplib:shortest true`, and the path outputs `maplib:pathIndex ?i`, `maplib:nodeIndex ?j`
//! and `maplib:pathNode ?n`.
//!
//! Evaluation works on the integer keys of the nodes of the step edges (as the property path
//! evaluation in [super::path] does): a breadth-first search from each start node, level by
//! level, deduplicated per (start, node, depth), with the start nodes processed in parallel.
use super::path::{
    bound_start_keys, path_leaves, term_variable_name, PathLeaf, U32DataFrameCreator, VALUE_COLUMN,
};
use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::QuerySettings;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Variable};
use polars::prelude::{
    col, lit, Column, DataFrame, IdxCa, Int64Chunked, IntoColumn, IntoLazy, JoinType,
};
use query_processing::expressions::{literal_enc, named_node_enc};
use query_processing::graph_patterns::join;
use query_processing::pushdowns::Pushdowns;
use rayon::prelude::*;
use representation::constants::MAPLIB_PREFIX_IRI;
use representation::dataset::QueryGraph;
use representation::multitype::{compress_actual_multitypes, force_convert_multicol_to_single_col};
use representation::solution_mapping::SolutionMappings;
use representation::subtypes::is_literal_subtype;
use representation::{BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};

/// The magic predicate.
pub const PATH: &str = "https://datatreehouse.github.io/maplib/vocab#path";

const PREDICATE: &str = "predicate";
const INVERSE_PREDICATE: &str = "inversePredicate";
const MIN_LENGTH: &str = "minLength";
const MAX_LENGTH: &str = "maxLength";
const OBJECT: &str = "object";
const LENGTH: &str = "length";
const PATH_MODE: &str = "pathMode";
const SHORTEST: &str = "shortest";
const PATH_INDEX: &str = "pathIndex";
const NODE_INDEX: &str = "nodeIndex";
const PATH_NODE: &str = "pathNode";
const OPTIONS: [&str; 11] = [
    PREDICATE,
    INVERSE_PREDICATE,
    MIN_LENGTH,
    MAX_LENGTH,
    OBJECT,
    LENGTH,
    PATH_MODE,
    SHORTEST,
    PATH_INDEX,
    NODE_INDEX,
    PATH_NODE,
];

/// Which paths are allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathMode {
    /// Nodes and edges may repeat.
    Walk,
    /// No node occurs twice.
    Acyclic,
    /// No node occurs twice, except that the last node may be the first (a cycle).
    Simple,
}

/// What to find, in terms of lengths and modes.
#[derive(Debug, Clone)]
pub struct PathSpec {
    pub min: usize,
    pub max: Option<usize>,
    pub mode: PathMode,
    /// Only the shortest paths (of length at least `min`) between each pair of nodes.
    pub shortest: bool,
    /// One result per length (rather than per pair of nodes).
    pub lengths: bool,
    /// One result per path (rather than per pair of nodes or per length).
    pub paths: bool,
}

/// A `maplib:path` pattern of a basic graph pattern.
#[derive(Debug, Clone)]
pub struct LengthPathPattern {
    pub subject: TermPattern,
    pub object: Option<TermPattern>,
    pub predicates: Vec<NamedNode>,
    pub inverse_predicates: Vec<NamedNode>,
    pub spec: PathSpec,
    pub length: Option<Variable>,
    pub path_index: Option<Variable>,
    pub node_index: Option<Variable>,
    pub path_node: Option<Variable>,
}

fn path_error(msg: impl Into<String>) -> SparqlError {
    SparqlError::PathError(msg.into())
}

/// Takes the `maplib:path` patterns, and the patterns describing their options, out of the
/// patterns of a basic graph pattern.
pub fn extract_length_path_patterns(
    patterns: &[TriplePattern],
) -> Result<(Vec<TriplePattern>, Vec<LengthPathPattern>), SparqlError> {
    let is_path = |tp: &TriplePattern| matches!(&tp.predicate, NamedNodePattern::NamedNode(nn) if nn.as_str() == PATH);
    if !patterns.iter().any(is_path) {
        return Ok((patterns.to_vec(), vec![]));
    }
    let mut option_nodes = vec![];
    for tp in patterns.iter().filter(|tp| is_path(tp)) {
        match &tp.object {
            TermPattern::Variable(_) | TermPattern::BlankNode(_) => {}
            o => {
                return Err(path_error(format!(
                    "The object of maplib:path must be a blank node or a variable holding the options, got {o}"
                )))
            }
        }
        if option_nodes.contains(&tp.object) {
            return Err(path_error(format!(
                "The options {} are used by more than one maplib:path pattern",
                tp.object
            )));
        }
        option_nodes.push(tp.object.clone());
    }
    let mut rest = vec![];
    let mut options: HashMap<TermPattern, Vec<(String, TermPattern)>> = HashMap::new();
    for tp in patterns {
        if is_path(tp) {
            continue;
        }
        if option_nodes.contains(&tp.subject) {
            let name = match &tp.predicate {
                NamedNodePattern::NamedNode(nn) => nn
                    .as_str()
                    .strip_prefix(MAPLIB_PREFIX_IRI)
                    .filter(|n| OPTIONS.contains(n)),
                NamedNodePattern::Variable(_) => None,
            };
            let Some(name) = name else {
                return Err(path_error(format!(
                    "Unknown maplib:path option {}, the options are {}",
                    tp.predicate,
                    OPTIONS.map(|o| format!("maplib:{o}")).join(", ")
                )));
            };
            options
                .entry(tp.subject.clone())
                .or_default()
                .push((name.to_string(), tp.object.clone()));
        } else {
            rest.push(tp.clone());
        }
    }
    let mut out = vec![];
    for tp in patterns.iter().filter(|tp| is_path(tp)) {
        let opts = options.remove(&tp.object).unwrap_or_default();
        out.push(parse_options(tp.subject.clone(), opts)?);
    }
    Ok((rest, out))
}

fn parse_options(
    subject: TermPattern,
    opts: Vec<(String, TermPattern)>,
) -> Result<LengthPathPattern, SparqlError> {
    let mut seen = HashSet::new();
    let mut predicates = vec![];
    let mut inverse_predicates = vec![];
    let mut min = None;
    let mut max = None;
    let mut object = None;
    let mut length = None;
    let mut mode = PathMode::Walk;
    let mut shortest = false;
    let mut path_index = None;
    let mut node_index = None;
    let mut path_node = None;
    for (name, value) in opts {
        let repeatable = name == PREDICATE || name == INVERSE_PREDICATE;
        if !seen.insert(name.clone()) && !repeatable {
            return Err(path_error(format!("maplib:{name} is given more than once")));
        }
        match name.as_str() {
            PREDICATE | INVERSE_PREDICATE => {
                let TermPattern::NamedNode(nn) = value else {
                    return Err(path_error(format!(
                        "maplib:{name} must be an IRI, got {value}"
                    )));
                };
                if name == PREDICATE {
                    predicates.push(nn);
                } else {
                    inverse_predicates.push(nn);
                }
            }
            MIN_LENGTH => min = Some(parse_length(&name, &value)?),
            MAX_LENGTH => max = Some(parse_length(&name, &value)?),
            OBJECT => object = Some(value),
            LENGTH => length = Some(output_variable(&name, value)?),
            PATH_INDEX => path_index = Some(output_variable(&name, value)?),
            NODE_INDEX => node_index = Some(output_variable(&name, value)?),
            PATH_NODE => path_node = Some(output_variable(&name, value)?),
            PATH_MODE => {
                let TermPattern::Literal(l) = &value else {
                    return Err(path_error(format!(
                        "maplib:pathMode must be \"walk\", \"acyclic\" or \"simple\", got {value}"
                    )));
                };
                mode = match l.value().to_lowercase().as_str() {
                    "walk" => PathMode::Walk,
                    "acyclic" => PathMode::Acyclic,
                    "simple" => PathMode::Simple,
                    _ => return Err(path_error(format!(
                        "maplib:pathMode must be \"walk\", \"acyclic\" or \"simple\", got {value}"
                    ))),
                };
            }
            SHORTEST => {
                shortest = match &value {
                    TermPattern::Literal(l) if l.datatype() == xsd::BOOLEAN => {
                        matches!(l.value(), "true" | "1")
                    }
                    _ => {
                        return Err(path_error(format!(
                            "maplib:shortest must be true or false, got {value}"
                        )))
                    }
                };
            }
            _ => unreachable!("Should never happen"),
        }
    }
    let min = min.unwrap_or(1);
    if let Some(max) = max {
        if min > max {
            return Err(path_error(format!(
                "maplib:minLength {min} is greater than maplib:maxLength {max}"
            )));
        }
    }
    let paths = path_index.is_some() || node_index.is_some() || path_node.is_some();
    let lengths = length.is_some();
    let spec = PathSpec {
        min,
        max,
        mode,
        shortest,
        lengths,
        paths,
    };
    if mode == PathMode::Walk && max.is_none() && !shortest && (lengths || paths) {
        return Err(path_error(
            "With maplib:length or path outputs, walks (maplib:pathMode \"walk\") need a \
             maplib:maxLength, as a cycle gives walks of every length. Set maplib:maxLength, \
             maplib:shortest true, or maplib:pathMode \"acyclic\"",
        ));
    }
    Ok(LengthPathPattern {
        subject,
        object,
        predicates,
        inverse_predicates,
        spec,
        length,
        path_index,
        node_index,
        path_node,
    })
}

fn parse_length(name: &str, value: &TermPattern) -> Result<usize, SparqlError> {
    if let TermPattern::Literal(l) = value {
        if is_literal_subtype(l.datatype(), xsd::INTEGER) {
            if let Ok(n) = l.value().parse::<usize>() {
                return Ok(n);
            }
        }
    }
    Err(path_error(format!(
        "maplib:{name} must be a non-negative integer, got {value}"
    )))
}

fn output_variable(name: &str, value: TermPattern) -> Result<Variable, SparqlError> {
    match value {
        TermPattern::Variable(v) => Ok(v),
        v => Err(path_error(format!(
            "maplib:{name} must be a variable, got {v}"
        ))),
    }
}

/// Directed edges over node keys as compressed sparse rows, without duplicate edges.
pub struct StepGraph {
    offsets: Vec<usize>,
    targets: Vec<u32>,
}

impl StepGraph {
    /// A graph over the nodes 0..n.
    pub fn from_edges(n: usize, mut edges: Vec<(u32, u32)>) -> Self {
        edges.par_sort_unstable();
        edges.dedup();
        let mut offsets = vec![0usize; n + 1];
        for (f, _) in &edges {
            offsets[*f as usize + 1] += 1;
        }
        for i in 1..offsets.len() {
            offsets[i] += offsets[i - 1];
        }
        let targets = edges.into_iter().map(|(_, t)| t).collect();
        StepGraph { offsets, targets }
    }

    fn neighbours(&self, u: u32) -> &[u32] {
        let u = u as usize;
        if u + 1 < self.offsets.len() {
            &self.targets[self.offsets[u]..self.offsets[u + 1]]
        } else {
            &[]
        }
    }

    /// The sorted, distinct nodes one step from `nodes`.
    fn step(&self, nodes: &[u32]) -> Vec<u32> {
        let mut out = vec![];
        for n in nodes {
            out.extend_from_slice(self.neighbours(*n));
        }
        out.sort_unstable();
        out.dedup();
        out
    }
}

/// A path found from a start node: its end, its length, and its nodes if they were asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Found {
    pub start: u32,
    pub end: u32,
    pub length: u32,
    pub nodes: Option<Vec<u32>>,
}

/// The paths matching `spec` from each of `starts`. `max_rows` bounds the number of partial
/// paths kept while enumerating paths, so that a query with very many paths fails instead of
/// exhausting memory.
pub fn find_paths(
    graph: &StepGraph,
    starts: &[u32],
    spec: &PathSpec,
    max_rows: Option<usize>,
) -> Result<Vec<Found>, String> {
    let enumerate = spec.paths || spec.mode != PathMode::Walk;
    let per_start: Result<Vec<Vec<Found>>, String> = starts
        .par_iter()
        .map(|s| {
            if enumerate {
                enumerate_from(graph, *s, spec, max_rows)
            } else {
                Ok(walks_from(graph, *s, spec))
            }
        })
        .collect();
    let per_start = per_start?;
    let n: usize = per_start.iter().map(|f| f.len()).sum();
    if let Some(max_rows) = max_rows {
        if n > max_rows {
            return Err(too_many(n, max_rows));
        }
    }
    let mut out = Vec::with_capacity(n);
    for f in per_start {
        out.extend(f);
    }
    Ok(out)
}

fn too_many(n: usize, max_rows: usize) -> String {
    format!(
        "maplib:path found at least {n} paths, more than the configured maximum of {max_rows} rows. \
         Lower maplib:maxLength, bind an end of the path, or raise max_rows"
    )
}

/// Ends and lengths of walks from `s`, without enumerating the walks. The nodes at each depth
/// are deduplicated, so the cost is bounded by (number of nodes) x (number of depths).
fn walks_from(graph: &StepGraph, s: u32, spec: &PathSpec) -> Vec<Found> {
    let found = |end: u32, length: usize| Found {
        start: s,
        end,
        length: length as u32,
        nodes: None,
    };
    let mut out = vec![];
    let mut frontier = vec![s];
    let mut depth = 0;
    if spec.lengths && !spec.shortest {
        // Every length at which a node is reached.
        let max = spec.max.expect("checked when parsing");
        loop {
            if depth >= spec.min {
                out.extend(frontier.iter().map(|n| found(*n, depth)));
            }
            if depth == max || frontier.is_empty() {
                break;
            }
            frontier = graph.step(&frontier);
            depth += 1;
        }
    } else {
        // The state of a walk is its node and its length capped at min: walks in the same state
        // have the same continuations, except that longer ones run out of maxLength sooner. So
        // a node is followed from depth min onwards only the first time it is reached, which
        // also is the shortest length at least min, and the search ends even without maxLength.
        let mut seen = HashSet::new();
        loop {
            if depth >= spec.min {
                frontier.retain(|n| seen.insert(*n));
                out.extend(frontier.iter().map(|n| found(*n, depth)));
            }
            if frontier.is_empty() || Some(depth) == spec.max {
                break;
            }
            frontier = graph.step(&frontier);
            depth += 1;
        }
    }
    out
}

/// The partial paths of one length: the node each ends in, and the index of the path it extends
/// in the level before.
struct Level {
    parents: Vec<u32>,
    nodes: Vec<u32>,
}

/// Enumerates the paths from `s` level by level, as a trie of partial paths.
fn enumerate_from(
    graph: &StepGraph,
    s: u32,
    spec: &PathSpec,
    max_rows: Option<usize>,
) -> Result<Vec<Found>, String> {
    let mut levels = vec![Level {
        parents: vec![u32::MAX],
        nodes: vec![s],
    }];
    // The nodes on the partial path (depth, i), from the end backwards.
    let path_of = |levels: &Vec<Level>, depth: usize, mut i: usize| {
        let mut nodes = Vec::with_capacity(depth + 1);
        for d in (0..=depth).rev() {
            nodes.push(levels[d].nodes[i]);
            i = levels[d].parents[i] as usize;
        }
        nodes.reverse();
        nodes
    };
    let on_path = |levels: &Vec<Level>, depth: usize, mut i: usize, w: u32| {
        for d in (0..=depth).rev() {
            if levels[d].nodes[i] == w {
                return true;
            }
            i = levels[d].parents[i] as usize;
        }
        false
    };
    // For shortest walks: the depth at which a node was first reached at depth >= min.
    let mut first_reached: HashMap<u32, usize> = HashMap::new();
    if spec.min == 0 {
        first_reached.insert(s, 0);
    }
    // For shortest acyclic and simple paths: the length of the shortest path to each end.
    let mut shortest_to: HashMap<u32, usize> = HashMap::new();
    let mut emitted: HashSet<(u32, usize)> = HashSet::new();
    let mut out = vec![];
    let mut kept = 1usize;
    let mut depth = 0;
    loop {
        let level = &levels[depth];
        if depth >= spec.min {
            for (i, end) in level.nodes.iter().enumerate() {
                if spec.shortest {
                    let best = *shortest_to.entry(*end).or_insert(depth);
                    if best < depth {
                        continue;
                    }
                }
                let nodes = if spec.paths {
                    Some(path_of(&levels, depth, i))
                } else {
                    // One result per end (and length, if asked for).
                    let key_length = if spec.lengths && !spec.shortest {
                        depth
                    } else {
                        0
                    };
                    if !emitted.insert((*end, key_length)) {
                        continue;
                    }
                    None
                };
                out.push(Found {
                    start: s,
                    end: *end,
                    length: depth as u32,
                    nodes,
                });
            }
        }
        if Some(depth) == spec.max {
            break;
        }
        let mut next = Level {
            parents: vec![],
            nodes: vec![],
        };
        for (i, u) in level.nodes.iter().enumerate() {
            // A simple path stops when it gets back to its start.
            if spec.mode == PathMode::Simple && depth > 0 && *u == s {
                continue;
            }
            for w in graph.neighbours(*u) {
                let allowed = match spec.mode {
                    PathMode::Walk => {
                        if spec.shortest && depth + 1 >= spec.min {
                            let first = *first_reached.entry(*w).or_insert(depth + 1);
                            first == depth + 1
                        } else {
                            true
                        }
                    }
                    PathMode::Acyclic => !on_path(&levels, depth, i, *w),
                    PathMode::Simple => *w == s || !on_path(&levels, depth, i, *w),
                };
                if allowed {
                    next.parents.push(i as u32);
                    next.nodes.push(*w);
                }
            }
        }
        if next.nodes.is_empty() {
            break;
        }
        kept += next.nodes.len();
        if let Some(max_rows) = max_rows {
            if kept > max_rows {
                return Err(too_many(kept, max_rows));
            }
        }
        levels.push(next);
        depth += 1;
    }
    Ok(out)
}

/// The steps of the path: the predicates followed forwards or backwards, or any predicate if
/// none are given.
fn step_expression(lp: &LengthPathPattern) -> PropertyPathExpression {
    let steps: Vec<PropertyPathExpression> = lp
        .predicates
        .iter()
        .map(|p| PropertyPathExpression::NamedNode(p.clone()))
        .chain(lp.inverse_predicates.iter().map(|p| {
            PropertyPathExpression::Reverse(Box::new(PropertyPathExpression::NamedNode(p.clone())))
        }))
        .collect();
    steps
        .into_iter()
        .reduce(|a, b| PropertyPathExpression::Alternative(Box::new(a), Box::new(b)))
        .unwrap_or(PropertyPathExpression::NegatedPropertySet(vec![]))
}

/// The column of a path end: a variable or blank node, or a fresh name for a constant (whose
/// column is filtered and dropped) or a missing object.
fn end_column(t: Option<&TermPattern>) -> String {
    t.and_then(term_variable_name)
        .unwrap_or_else(|| format!("path_end_{}", uuid::Uuid::new_v4().simple()))
}

fn integer_state() -> RDFNodeState {
    BaseRDFNodeType::Literal(xsd::INTEGER.into_owned(), None).into_default_stored_rdf_node_state()
}

impl Triplestore {
    /// Evaluates a `maplib:path` pattern and joins it with `solution_mappings`, whose values for
    /// the ends of the path (and those in `pushdowns`) restrict where the search starts.
    pub fn lazy_length_path(
        &self,
        lp: &LengthPathPattern,
        solution_mappings: Option<SolutionMappings>,
        pushdowns: &Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        let missing_object = TermPattern::Variable(Variable::new_unchecked(end_column(None)));
        let subject = &lp.subject;
        let object = lp.object.as_ref().unwrap_or(&missing_object);
        let ppe = step_expression(lp);

        let mut pushdowns = pushdowns.clone();
        let focus: Vec<&str> = [subject, object]
            .into_iter()
            .filter_map(|t| match t {
                TermPattern::Variable(v) => Some(v.as_str()),
                _ => None,
            })
            .collect();
        let solution_mappings = solution_mappings.map(|sm| {
            pushdowns.add_from_solution_mappings(sm, self.global_cats.clone(), &focus, None)
        });

        let mut df_creator = U32DataFrameCreator::new(dataset.clone());
        df_creator.gather_namednode_dfs(&ppe, self)?;
        let (lookup_df, lookup_dtypes, leaf_dfs) =
            df_creator.create_u32_dfs(self.global_cats.clone())?;
        let n = lookup_df.height();

        let bound = {
            let cats = self.global_cats.read()?;
            bound_start_keys(
                subject,
                object,
                &pushdowns,
                &lookup_df,
                &lookup_dtypes,
                &cats,
            )
        };
        let (starts, reversed) = bound.unwrap_or_else(|| ((0..n as u32).collect(), false));

        let mut leaves = HashSet::new();
        path_leaves(&ppe, false, &mut leaves);
        let mut edges = vec![];
        for (leaf, leaf_reversed) in leaves {
            let leaf: PathLeaf = leaf;
            let Some(df) = leaf_dfs.get(&leaf) else {
                continue;
            };
            let subjects = df
                .column(representation::SUBJECT_COL_NAME)
                .unwrap()
                .u32()
                .unwrap();
            let objects = df
                .column(representation::OBJECT_COL_NAME)
                .unwrap()
                .u32()
                .unwrap();
            for (s, o) in subjects.iter().zip(objects.iter()) {
                if let (Some(s), Some(o)) = (s, o) {
                    // Searching from the object end follows the steps backwards.
                    if leaf_reversed != reversed {
                        edges.push((o, s));
                    } else {
                        edges.push((s, o));
                    }
                }
            }
        }
        let graph = StepGraph::from_edges(n, edges);
        let mut found =
            find_paths(&graph, &starts, &lp.spec, query_settings.max_rows).map_err(path_error)?;
        if reversed {
            for f in &mut found {
                std::mem::swap(&mut f.start, &mut f.end);
                if let Some(nodes) = &mut f.nodes {
                    nodes.reverse();
                }
            }
        }
        let subject_col = end_column(Some(subject));
        let object_col = end_column(Some(object));
        let same_variable = subject_col == object_col;
        if same_variable {
            found.retain(|f| f.start == f.end);
        }

        // The rows: one per path node if nodes are asked for, otherwise one per found path.
        let per_node = lp.node_index.is_some() || lp.path_node.is_some();
        let mut start_keys = vec![];
        let mut end_keys = vec![];
        let mut lengths = vec![];
        let mut path_indices = vec![];
        let mut node_indices = vec![];
        let mut node_keys = vec![];
        for (path_index, f) in found.into_iter().enumerate() {
            match (&f.nodes, per_node) {
                (Some(nodes), true) => {
                    for (i, node) in nodes.iter().enumerate() {
                        start_keys.push(f.start);
                        end_keys.push(f.end);
                        lengths.push(f.length as i64);
                        path_indices.push(path_index as i64);
                        node_indices.push(i as i64);
                        node_keys.push(*node);
                    }
                }
                _ => {
                    start_keys.push(f.start);
                    end_keys.push(f.end);
                    lengths.push(f.length as i64);
                    path_indices.push(path_index as i64);
                }
            }
        }
        let height = start_keys.len();
        let values = lookup_df.column(VALUE_COLUMN).unwrap();
        let value_state = lookup_dtypes.get(VALUE_COLUMN).unwrap().clone();
        let take = |keys: Vec<u32>, name: &str| -> Column {
            values
                .take(&IdxCa::from_vec("".into(), keys))
                .unwrap()
                .with_name(name.into())
        };
        let mut columns = vec![take(start_keys, &subject_col)];
        let mut dtypes = HashMap::from([(subject_col.clone(), value_state.clone())]);
        if !same_variable {
            columns.push(take(end_keys, &object_col));
            dtypes.insert(object_col.clone(), value_state.clone());
        }
        let mut int_column = |v: &Option<Variable>, data: &Vec<i64>| {
            if let Some(v) = v {
                columns.push(Int64Chunked::from_vec(v.as_str().into(), data.clone()).into_column());
                dtypes.insert(v.as_str().to_string(), integer_state());
            }
        };
        int_column(&lp.length, &lengths);
        int_column(&lp.path_index, &path_indices);
        int_column(&lp.node_index, &node_indices);
        if let Some(v) = &lp.path_node {
            columns.push(take(node_keys, v.as_str()));
            dtypes.insert(v.as_str().to_string(), value_state.clone());
        }
        let mut out_df = DataFrame::new(height, columns).unwrap();

        // Constant ends are matched by filtering their columns, which are then dropped.
        let mut constants = vec![];
        for (t, c) in [(subject, &subject_col), (object, &object_col)] {
            if matches!(t, TermPattern::NamedNode(_) | TermPattern::Literal(_))
                && !constants.iter().any(|(_, x)| x == c)
            {
                constants.push((t, c.clone()));
            }
        }
        for (t, c) in &constants {
            let base = match t {
                TermPattern::NamedNode(_) => BaseRDFNodeType::IRI,
                TermPattern::Literal(l) => BaseRDFNodeType::from_literal(l),
                _ => unreachable!("Should never happen"),
            };
            if value_state.is_multi() {
                out_df = force_convert_multicol_to_single_col(out_df.lazy(), c, &base)
                    .collect()
                    .unwrap();
                dtypes.insert(c.clone(), base.into_default_stored_rdf_node_state());
            }
        }
        (out_df, dtypes) = compress_actual_multitypes(out_df, dtypes);
        if !constants.is_empty() {
            let cats = self.global_cats.read()?;
            for (t, c) in &constants {
                let e = match t {
                    TermPattern::NamedNode(nn) => named_node_enc(nn, &cats).unwrap_or(lit(false)),
                    TermPattern::Literal(l) => literal_enc(l, &cats)?.0,
                    _ => unreachable!("Should never happen"),
                };
                out_df = out_df.lazy().filter(col(c).eq(e)).collect().unwrap();
            }
            let height = out_df.height();
            for (_, c) in &constants {
                out_df = out_df.drop(c).unwrap();
                dtypes.remove(c);
            }
            if out_df.width() == 0 {
                out_df = DataFrame::empty_with_height(height);
            }
        }
        if lp.object.is_none() {
            out_df = out_df.drop(&object_col).unwrap();
            dtypes.remove(&object_col);
            if out_df.width() == 0 {
                out_df = DataFrame::empty_with_height(height);
            }
        }
        let out_height = out_df.height();
        let mut path_solution_mappings = SolutionMappings::new(out_df.lazy(), dtypes, out_height);
        if let Some(mappings) = solution_mappings {
            path_solution_mappings = join(
                path_solution_mappings,
                mappings,
                JoinType::Inner,
                self.global_cats.clone(),
                query_settings.max_rows,
            )?;
        }
        Ok(path_solution_mappings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(min: usize, max: Option<usize>) -> PathSpec {
        PathSpec {
            min,
            max,
            mode: PathMode::Walk,
            shortest: false,
            lengths: false,
            paths: false,
        }
    }

    fn triples(found: &[Found]) -> Vec<(u32, u32, u32)> {
        let mut v: Vec<_> = found.iter().map(|f| (f.start, f.end, f.length)).collect();
        v.sort();
        v
    }

    fn pairs(found: &[Found]) -> Vec<(u32, u32)> {
        let mut v: Vec<_> = found.iter().map(|f| (f.start, f.end)).collect();
        v.sort();
        v.dedup();
        v
    }

    /// The (start, end, length) triples of all walks of length min..=max, by multiplying out
    /// the edges, as an equivalent of a union of fixed-length sequence paths.
    fn brute_force(n: usize, edges: &[(u32, u32)], min: usize, max: usize) -> Vec<(u32, u32, u32)> {
        let mut out = HashSet::new();
        let mut reach: HashSet<(u32, u32)> = (0..n as u32).map(|i| (i, i)).collect();
        for d in 0..=max {
            if d >= min {
                out.extend(reach.iter().map(|(s, o)| (*s, *o, d as u32)));
            }
            let mut next = HashSet::new();
            for (s, m) in &reach {
                for (a, b) in edges {
                    if a == m {
                        next.insert((*s, *b));
                    }
                }
            }
            reach = next;
        }
        let mut v: Vec<_> = out.into_iter().collect();
        v.sort();
        v
    }

    fn all(n: usize) -> Vec<u32> {
        (0..n as u32).collect()
    }

    #[test]
    fn chain_lengths() {
        let edges = vec![(0, 1), (1, 2), (2, 3)];
        let g = StepGraph::from_edges(4, edges.clone());
        let mut s = spec(2, Some(3));
        s.lengths = true;
        let found = find_paths(&g, &all(4), &s, None).unwrap();
        assert_eq!(triples(&found), vec![(0, 2, 2), (0, 3, 3), (1, 3, 2)]);
        assert_eq!(triples(&found), brute_force(4, &edges, 2, 3));
    }

    #[test]
    fn walks_match_brute_force() {
        // A cycle 0->1->2->0 with a tail 2->3, a diamond 3->4,5->6, and a self-loop on 6.
        let edges = vec![
            (0, 1),
            (1, 2),
            (2, 0),
            (2, 3),
            (3, 4),
            (3, 5),
            (4, 6),
            (5, 6),
            (6, 6),
        ];
        let g = StepGraph::from_edges(7, edges.clone());
        for min in 0..5 {
            for max in min..7 {
                let mut s = spec(min, Some(max));
                s.lengths = true;
                let found = find_paths(&g, &all(7), &s, None).unwrap();
                let expected = brute_force(7, &edges, min, max);
                assert_eq!(triples(&found), expected, "min {min} max {max}");
                // Without lengths: the distinct pairs.
                let s = spec(min, Some(max));
                let found = find_paths(&g, &all(7), &s, None).unwrap();
                let mut expected_pairs: Vec<_> =
                    expected.iter().map(|(a, b, _)| (*a, *b)).collect();
                expected_pairs.dedup();
                assert_eq!(pairs(&found), expected_pairs, "min {min} max {max}");
                assert_eq!(
                    found.len(),
                    expected_pairs.len(),
                    "duplicates for {min} {max}"
                );
                // Shortest: the smallest length per pair.
                let mut s = spec(min, Some(max));
                s.shortest = true;
                s.lengths = true;
                let found = find_paths(&g, &all(7), &s, None).unwrap();
                let mut shortest: HashMap<(u32, u32), u32> = HashMap::new();
                for (a, b, d) in &expected {
                    let e = shortest.entry((*a, *b)).or_insert(*d);
                    *e = (*e).min(*d);
                }
                let mut expected_shortest: Vec<_> =
                    shortest.into_iter().map(|((a, b), d)| (a, b, d)).collect();
                expected_shortest.sort();
                assert_eq!(triples(&found), expected_shortest, "min {min} max {max}");
            }
        }
    }

    #[test]
    fn unbounded_pairs_terminate_on_cycles() {
        let edges = vec![(0, 1), (1, 0)];
        let g = StepGraph::from_edges(2, edges);
        let found = find_paths(&g, &[0], &spec(3, None), None).unwrap();
        assert_eq!(pairs(&found), vec![(0, 0), (0, 1)]);
        let mut s = spec(3, None);
        s.shortest = true;
        s.lengths = true;
        let found = find_paths(&g, &[0], &s, None).unwrap();
        assert_eq!(triples(&found), vec![(0, 0, 4), (0, 1, 3)]);
    }

    #[test]
    fn diamond_paths() {
        let edges = vec![(0, 1), (0, 2), (1, 3), (2, 3)];
        let g = StepGraph::from_edges(4, edges);
        let mut s = spec(2, Some(2));
        s.paths = true;
        let found = find_paths(&g, &[0], &s, None).unwrap();
        let mut paths: Vec<_> = found.iter().map(|f| f.nodes.clone().unwrap()).collect();
        paths.sort();
        assert_eq!(paths, vec![vec![0, 1, 3], vec![0, 2, 3]]);
        // The pair is found once without paths.
        let found = find_paths(&g, &[0], &spec(2, Some(2)), None).unwrap();
        assert_eq!(found.len(), 1);
    }

    #[test]
    fn acyclic_and_simple() {
        // 0->1->2->0 and 2->3
        let edges = vec![(0, 1), (1, 2), (2, 0), (2, 3)];
        let g = StepGraph::from_edges(4, edges);
        let mut s = spec(1, None);
        s.mode = PathMode::Acyclic;
        s.lengths = true;
        let found = find_paths(&g, &[0], &s, None).unwrap();
        assert_eq!(triples(&found), vec![(0, 1, 1), (0, 2, 2), (0, 3, 3)]);
        s.mode = PathMode::Simple;
        let found = find_paths(&g, &[0], &s, None).unwrap();
        assert_eq!(
            triples(&found),
            vec![(0, 0, 3), (0, 1, 1), (0, 2, 2), (0, 3, 3)]
        );
        // Walks reach 0 at 3 and 6.
        let mut s = spec(1, Some(6));
        s.lengths = true;
        let found = find_paths(&g, &[0], &s, None).unwrap();
        assert!(triples(&found).contains(&(0, 0, 6)));
    }

    #[test]
    fn self_loop() {
        let g = StepGraph::from_edges(1, vec![(0, 0)]);
        let mut s = spec(0, Some(3));
        s.lengths = true;
        let found = find_paths(&g, &[0], &s, None).unwrap();
        assert_eq!(
            triples(&found),
            vec![(0, 0, 0), (0, 0, 1), (0, 0, 2), (0, 0, 3)]
        );
        s.mode = PathMode::Acyclic;
        let found = find_paths(&g, &[0], &s, None).unwrap();
        assert_eq!(triples(&found), vec![(0, 0, 0)]);
        s.mode = PathMode::Simple;
        let found = find_paths(&g, &[0], &s, None).unwrap();
        assert_eq!(triples(&found), vec![(0, 0, 0), (0, 0, 1)]);
    }

    #[test]
    fn shortest_walk_paths() {
        // Two shortest paths 0->1->3 and 0->2->3 and a longer one 0->4->5->3.
        let edges = vec![(0, 1), (0, 2), (1, 3), (2, 3), (0, 4), (4, 5), (5, 3)];
        let g = StepGraph::from_edges(6, edges);
        let mut s = spec(1, None);
        s.paths = true;
        s.shortest = true;
        let found = find_paths(&g, &[0], &s, None).unwrap();
        let mut to3: Vec<_> = found
            .iter()
            .filter(|f| f.end == 3)
            .map(|f| f.nodes.clone().unwrap())
            .collect();
        to3.sort();
        assert_eq!(to3, vec![vec![0, 1, 3], vec![0, 2, 3]]);
    }

    #[test]
    fn max_rows_bounds_enumeration() {
        // A complete graph on 6 nodes has very many walks.
        let mut edges = vec![];
        for a in 0..6 {
            for b in 0..6 {
                edges.push((a, b));
            }
        }
        let g = StepGraph::from_edges(6, edges);
        let mut s = spec(1, Some(8));
        s.paths = true;
        assert!(find_paths(&g, &all(6), &s, Some(1000)).is_err());
    }

    #[test]
    fn bounded_path_syntax() {
        let parse = |p: &str| {
            spargebra::Query::parse(
                &format!("PREFIX ex: <http://ex/> SELECT * WHERE {{ ?s {p} ?o }}"),
                None,
                None,
            )
            .map(|q| q.to_string())
        };
        let same =
            |a: &str, b: &str| assert_eq!(parse(a).unwrap(), parse(b).unwrap(), "{a} vs {b}");
        same("ex:p{2}", "ex:p/ex:p");
        same("ex:p{1}", "ex:p");
        same("ex:p{2,3}", "ex:p/ex:p/ex:p?");
        same("ex:p{0,2}", "ex:p?/ex:p?");
        same("ex:p{,2}", "ex:p?/ex:p?");
        same("ex:p{ 1 , 2 }", "ex:p/ex:p?");
        same("ex:p{0,}", "ex:p*");
        same("ex:p{1,}", "ex:p+");
        same("ex:p{3,}", "ex:p/ex:p/ex:p+");
        same("(ex:p|^ex:q){2}", "(ex:p|^ex:q)/(ex:p|^ex:q)");
        same("^ex:p{2}/ex:q", "^(ex:p/ex:p)/ex:q");
        for bad in [
            "ex:p{3,2}",
            "ex:p{0}",
            "ex:p{,0}",
            "ex:p{1001}",
            "ex:p{}",
            "ex:p{-1}",
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    fn tp(s: TermPattern, p: &str, o: TermPattern) -> TriplePattern {
        TriplePattern {
            subject: s,
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(p)),
            object: o,
        }
    }

    fn opt(name: &str) -> String {
        format!("{MAPLIB_PREFIX_IRI}{name}")
    }

    #[test]
    fn extract_options() {
        let s = TermPattern::Variable(Variable::new_unchecked("s"));
        let b = TermPattern::BlankNode(oxrdf::BlankNode::new_unchecked("b"));
        let int = |i: i64| TermPattern::Literal(oxrdf::Literal::from(i));
        let patterns = vec![
            tp(s.clone(), "http://ex/type", s.clone()),
            tp(s.clone(), PATH, b.clone()),
            tp(b.clone(), &opt("minLength"), int(2)),
            tp(b.clone(), &opt("maxLength"), int(1)),
        ];
        let err = extract_length_path_patterns(&patterns).unwrap_err();
        assert!(err.to_string().contains("greater than"), "{err}");

        let patterns = vec![
            tp(s.clone(), PATH, b.clone()),
            tp(
                b.clone(),
                &opt("predicate"),
                TermPattern::NamedNode(NamedNode::new_unchecked("http://ex/p")),
            ),
            tp(
                b.clone(),
                &opt("predicate"),
                TermPattern::NamedNode(NamedNode::new_unchecked("http://ex/q")),
            ),
            tp(b.clone(), &opt("maxLength"), int(3)),
            tp(s.clone(), "http://ex/type", s.clone()),
        ];
        let (rest, lps) = extract_length_path_patterns(&patterns).unwrap();
        assert_eq!(rest.len(), 1);
        assert_eq!(lps.len(), 1);
        assert_eq!(lps[0].predicates.len(), 2);
        assert_eq!(lps[0].spec.min, 1);
        assert_eq!(lps[0].spec.max, Some(3));

        let patterns = vec![
            tp(s.clone(), PATH, b.clone()),
            tp(b.clone(), &opt("maxLenght"), int(3)),
        ];
        assert!(extract_length_path_patterns(&patterns).is_err());

        // Walk lengths without a maximum never end on a cycle.
        let patterns = vec![
            tp(s.clone(), PATH, b.clone()),
            tp(
                b.clone(),
                &opt("length"),
                TermPattern::Variable(Variable::new_unchecked("len")),
            ),
        ];
        assert!(extract_length_path_patterns(&patterns).is_err());
    }
}

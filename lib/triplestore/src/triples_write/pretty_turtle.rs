use super::Triplestore;
use crate::errors::TriplestoreError;
use crate::sparql::{QueryResultKind, QuerySettings};
use crate::storage::Triples;
use aho_corasick::{AhoCorasick, MatchKind};
use oxrdf::vocab::rdf;
use oxrdf::{BlankNode, NamedNode, NamedNodeRef, Term, TermRef, Variable};
use polars::prelude::{col, concat, LazyFrame, UnionArgs};
use polars_core::frame::DataFrame;
use polars_core::POOL;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::cats::LockedCats;
use representation::dataset::NamedGraph;
use representation::polars_to_rdf::column_as_terms;
use representation::{BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use spargebra::algebra::{Expression, Function, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::sync::Arc;
use polars_core::prelude::BooleanChunked;

const STRIDE: usize = 2_000;

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
                let t = in_degree_one_map.get(u).unwrap();
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
            if !l.is_plain() {
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
            write!(writer, "\n")?;
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
        let out = terms.get(0).unwrap().write_prefixed(
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
        for (k, v) in prefixes {
            writeln!(writer, "@prefix {}: {} .", k, v)
                .map_err(|x| TriplestoreError::WriteTurtleError(x.to_string()))?;
        }
        if !prefixes.is_empty() {
            writeln!(writer, "").map_err(|x| TriplestoreError::WriteTurtleError(x.to_string()))?;
        }

        let prefix_replacer = PrefixReplacer::new(prefixes);

        let map = if let Some(map) = self.graph_triples_map.get(graph) {
            map
        } else {
            return Err(TriplestoreError::GraphDoesNotExist(graph.to_string()));
        };

        let (blanks_in_degree_zero, blanks_in_degree_one) =
            self.create_blank_in_degree_zero_and_one_sets(map)?;

        let mut in_degree_one_blocks_map = HashMap::new();
        for (p, m) in map.iter() {
            for (k, t) in m.iter() {
                let (s, o) = k;
                if !s.is_blank_node() {
                    continue;
                }

                if p.as_ref() == rdf::REST || p.as_ref() == rdf::FIRST {
                    continue;
                }

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
                            &blanks_in_degree_one,
                            &HashSet::new(),
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
        let mut lists_map = self.create_lists_map(map, graph, &blanks_in_degree_one)?;
        if let Some(lists) = &mut lists_map {
            let keys: Vec<_> = lists.keys().cloned().collect();
            for k in keys {
                if blanks_in_degree_one.contains(&k) {
                    let l = lists.remove(&k).unwrap();
                    in_degree_one_blocks_map.insert(k, TurtleBlockOrTermOrList::Term(l));
                }
            }
        }

        let mut drivers = vec![];
        for (pred, m) in map.iter() {
            for k in m.keys() {
                if pred.as_ref() != rdf::TYPE
                    && !(lists_map.is_some() && is_list_pred_and_subject_type(pred, &k.0))
                {
                    drivers.push((pred.clone(), k.clone()));
                }
            }
        }
        drivers.sort();
        if let Some(m) = map.get(&rdf::TYPE.into_owned()) {
            let k1 = (BaseRDFNodeType::IRI, BaseRDFNodeType::IRI);
            if m.contains_key(&k1) {
                drivers.push((rdf::TYPE.into_owned(), k1));
            }

            let k2 = (BaseRDFNodeType::BlankNode, BaseRDFNodeType::IRI);
            if m.contains_key(&k2) {
                drivers.push((rdf::TYPE.into_owned(), k2));
            }
        }
        let mut used_drivers: HashMap<_, HashSet<(BaseRDFNodeType, BaseRDFNodeType)>> =
            HashMap::new();
        let mut used_iri_subjects = HashSet::new();
        let mut used_blank_subjects = in_degree_one_blocks_map.keys().cloned().collect();
        let type_nn = rdf::TYPE.into_owned();

        for (driver_predicate, k) in drivers.into_iter().rev() {
            if let Some(ks) = used_drivers.get_mut(&driver_predicate) {
                ks.insert(k.clone());
            } else {
                used_drivers.insert(
                    driver_predicate.clone(),
                    HashSet::from_iter(vec![k.clone()]),
                );
            }
            let triples = map.get(&driver_predicate).unwrap().get(&(k)).unwrap();

            let n_threads = POOL.current_num_threads();
            let mut exhausted_driver = false;
            let mut found_first = false;
            let mut last_string: Option<String> = None;
            while !exhausted_driver {
                let (subject_type, _) = &k;
                let use_used_subjects = if subject_type.is_iri() {
                    &used_iri_subjects
                } else {
                    &used_blank_subjects
                };
                let mut thread_strings = Vec::with_capacity(n_threads);
                for _ in 0..n_threads {
                    println!("Last string {:?}", last_string);
                    let mut start_string = if let Some(last_string) = last_string.take() {
                        if let Some((next_string)) = triples
                            .get_next_different_subject(
                                self.global_cats.clone(),
                                last_string.as_str(),
                            )?
                        {
                            assert!(next_string > last_string);
                            next_string
                        } else {
                            exhausted_driver = true;
                            break;
                        }
                    } else {
                        assert!(!found_first);
                        if let Some(start_string) =
                            triples.get_first_subject_string()?
                        {
                            found_first = true;
                            start_string
                        } else {
                            exhausted_driver = true;
                            break;
                        }
                    };
                    let end_string = triples.get_next_different_approximately_n_distance_away(
                        &start_string,
                        STRIDE / n_threads
                    )?;
                    let end_string = if let Some(end_string) = end_string {
                        assert!(start_string < end_string);
                        last_string = Some(end_string.clone());
                        end_string
                    } else {
                        exhausted_driver = true;
                        start_string.clone()
                    };
                    let start_end_equal = start_string == end_string;

                    thread_strings.push((start_string, end_string));
                    if start_end_equal {
                        break;
                    }
                }

                let r: Result<Vec<_>, TriplestoreError> = POOL
                    .install(|| {
                        thread_strings
                            .into_par_iter()
                            .map(|(from_string, to_string)| {
                                let r = self.create_block_segment(
                                    &driver_predicate,
                                    map,
                                    triples,
                                    from_string.as_str(),
                                    to_string.as_str(),
                                    &used_drivers,
                                    use_used_subjects,
                                    &blanks_in_degree_zero,
                                    &blanks_in_degree_one,
                                    lists_map.is_some(),
                                );
                                r
                            })
                    })
                    .collect();

                let r = r?;

                let mut new_r = Vec::with_capacity(n_threads);
                for (new_map, subjects_ordering) in r {
                    if subject_type.is_iri() {
                        used_iri_subjects.extend(new_map.keys().cloned());
                    } else if subject_type.is_blank_node() {
                        used_blank_subjects.extend(new_map.keys().cloned());
                    };
                    new_r.push((new_map, subjects_ordering));
                }

                let written: Result<(Vec<_>, Vec<_>), TriplestoreError> = POOL
                    .install(|| {
                        new_r.into_par_iter().map(|(new_map, subjects_ordering)| {
                            let mut writer: Vec<u8> = Vec::new();
                            let replaced = write_blocks(
                                &mut writer,
                                new_map,
                                &type_nn,
                                &prefix_replacer,
                                &subjects_ordering,
                                &in_degree_one_blocks_map,
                            )?;
                            Ok((writer, replaced))
                        })
                    })
                    .collect();
                let (written, replaced) = written?;
                for r in replaced {
                    for u in r {
                        in_degree_one_blocks_map.remove(&u).unwrap();
                    }
                }
                for mut w in written {
                    writer
                        .write_all(&mut w)
                        .map_err(|x| TriplestoreError::WriteTurtleError(x.to_string()))?;
                }
            }
        }

        if let Some(lists_map) = lists_map {
            for (u, mut l) in lists_map {
                let subject = if blanks_in_degree_zero.contains(&u) {
                    None
                } else {
                    let bl = self
                        .global_cats
                        .read()?
                        .maybe_decode_of_type(&u, &BaseRDFNodeType::BlankNode)
                        .unwrap()
                        .into_owned();
                    let bl = BlankNode::new_unchecked(bl);
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
                turtle_block
                    .write_block(
                        writer,
                        &type_nn,
                        &prefix_replacer,
                        has_subject,
                        &mut in_degree_one_blocks_map,
                        1,
                    )
                    .map_err(|x| TriplestoreError::WriteTurtleError(x.to_string()))?;
                writeln!(writer, " .\n")
                    .map_err(|x| TriplestoreError::WriteTurtleError(x.to_string()))?;
            }
        }
        Ok(())
    }

    fn create_block_segment(
        &self,
        driver_predicate: &NamedNode,
        map: &HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
        triples: &Triples,
        string_start: &str,
        string_ends: &str,
        used_drivers: &HashMap<NamedNode, HashSet<(BaseRDFNodeType, BaseRDFNodeType)>>,
        used_subjects: &HashSet<u32>,
        blanks_in_degree_zero: &HashSet<u32>,
        blanks_in_degree_one: &HashSet<u32>,
        has_lists: bool,
    ) -> Result<(HashMap<u32, TurtleBlock>, Vec<u32>), TriplestoreError> {
        let lf = triples.get_lazy_frame_between_subject_strings(
            string_start,
            string_ends,
            self.global_cats.clone(),
        )?;
        let mut df = if let Some(lf) = lf {
            lf.collect().unwrap()
        } else {
            return Ok((HashMap::new(), Vec::new()));
        };
        let mut keep = vec![];
        for s in df.column(SUBJECT_COL_NAME).unwrap().u32().unwrap() {
            let s = s.unwrap();
            keep.push(!used_subjects.contains(&s));
        }
        df = df.filter(&BooleanChunked::from_iter(keep)).unwrap();
        if df.is_empty() {
            return Ok((HashMap::new(), Vec::new()));
        }
        df.as_single_chunk();

        let new_subj_u32: HashSet<u32> = df
            .column(SUBJECT_COL_NAME)
            .unwrap()
            .u32()
            .unwrap()
            .iter()
            .map(|x| x.unwrap())
            .collect();
        let subjects_ordering: Vec<_> = df
            .column(SUBJECT_COL_NAME)
            .unwrap()
            .unique_stable()
            .unwrap()
            .u32()
            .unwrap()
            .iter()
            .map(|x| x.unwrap())
            .collect();
        let mut blocks_map = HashMap::new();

        update_blocks_map(
            &mut blocks_map,
            &df,
            &driver_predicate,
            &triples.subject_type,
            &triples.object_type,
            self.global_cats.clone(),
            &new_subj_u32,
            &used_subjects,
            &blanks_in_degree_zero,
            &blanks_in_degree_one,
        )?;

        // Stride through and do the "left join"
        for (p, m) in map.iter() {
            for (k, t) in m.iter() {
                if let Some(ks) = used_drivers.get(p) {
                    if ks.contains(k) {
                        continue;
                    }
                }

                if has_lists && is_list_pred_and_subject_type(p, &k.0) {
                    continue;
                }

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
                        &new_subj_u32,
                        &used_subjects,
                        &blanks_in_degree_zero,
                        &blanks_in_degree_one,
                    )?;
                }
            }
        }
        Ok((blocks_map, subjects_ordering))
    }

    fn create_lists_map(
        &self,
        map: &HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
        graph: &NamedGraph,
        blank_in_degree_one_set: &HashSet<u32>,
    ) -> Result<Option<HashMap<u32, TermOrList>>, TriplestoreError> {
        let mut inv_rest_blank_blank_map: HashMap<u32, Vec<u32>> = HashMap::new();
        let mut first_blank_term_map = HashMap::new();
        let mut blank_lists_map = HashMap::new();

        if let Some(first_triple_map) = map.get(&rdf::FIRST.into_owned()) {
            for ((s, o), v) in first_triple_map {
                if s.is_blank_node() {
                    let lfs = v.get_lazy_frame_slices()?;
                    if let Some(lfs) = lfs {
                        let df = concat_lfs_subject_object(lfs).collect().unwrap();
                        let subject_u32s = df.column(SUBJECT_COL_NAME).unwrap().u32().unwrap();
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
                        );
                        if let Some(to_replace_terms) = to_replace_terms {
                            for ((s, t), r) in subject_u32s
                                .iter()
                                .zip(terms.into_iter())
                                .zip(to_replace_terms.into_iter())
                            {
                                if let Some(r) = r {
                                    first_blank_term_map.insert(s.unwrap(), r);
                                } else {
                                    first_blank_term_map
                                        .insert(s.unwrap(), TermOrList::Elem(Arc::new(t.unwrap())));
                                }
                            }
                        } else {
                            for (s, t) in subject_u32s.iter().zip(terms.into_iter()) {
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
                &None,
                false,
                &QuerySettings {
                    include_transient: false,
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
            let su32 = sm.mappings.column(SUBJECT_COL_NAME).unwrap().u32().unwrap();
            for u in su32 {
                if let Some(first) = first_blank_term_map.get(&u.unwrap()) {
                    blank_lists_map.insert(u.unwrap(), TermOrList::List(vec![first.clone()]));
                } else {
                    return Err(TriplestoreError::BadListError(
                        "Some list is missing rdf:first".to_string(),
                    ));
                }
            }
        } else {
            unreachable!("Should never happen")
        }
        if let Some(rest_triple_map) = map.get(&rdf::REST.into_owned()) {
            for ((s, o), v) in rest_triple_map {
                //The last elements must already be added (see above), since o is iri rdf:nil for last elems.
                if s.is_blank_node() && o.is_blank_node() {
                    let lf = v.get_lazy_frame_slices()?;
                    if let Some(lfs) = lf {
                        let df = concat_lfs_subject_object(lfs).collect().unwrap();
                        let subject_u32s = df.column(SUBJECT_COL_NAME).unwrap().u32().unwrap();
                        let object_u32s = df.column(OBJECT_COL_NAME).unwrap().u32().unwrap();

                        for (s, o) in subject_u32s.iter().zip(object_u32s.iter()) {
                            let s = s.unwrap();
                            let o = o.unwrap();
                            if let Some(subjects) = inv_rest_blank_blank_map.get_mut(&o) {
                                subjects.push(s);
                            } else {
                                inv_rest_blank_blank_map.insert(o, vec![s]);
                            }
                        }
                    }
                }
            }
            let mut finished_blank_lists_map = HashMap::new();
            while !blank_lists_map.is_empty() {
                let mut new_blank_lists_map = HashMap::new();

                for (blank, mut list) in blank_lists_map {
                    if let Some(blanks) = inv_rest_blank_blank_map.get(&blank) {
                        for blank in blanks {
                            if let Some(first) = first_blank_term_map.get(&blank) {
                                list.push(first.clone());
                                new_blank_lists_map.insert(*blank, list.clone());
                            } else {
                                return Err(TriplestoreError::BadListError(
                                    "Some list is missing rdf:first".to_string(),
                                ));
                            }
                        }
                    } else {
                        list.reverse();
                        finished_blank_lists_map.insert(blank, list);
                    }
                }
                blank_lists_map = new_blank_lists_map;
            }
            blank_lists_map = finished_blank_lists_map;
        } else {
            return Ok(None);
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

    fn create_blank_in_degree_zero_and_one_sets(
        &self,
        map: &HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
    ) -> Result<(HashSet<u32>, HashSet<u32>), TriplestoreError> {
        let mut out_map: HashMap<u32, u8> = HashMap::new();
        for m in map.values() {
            for (k, v) in m {
                let object_type = &k.1;
                if object_type.is_blank_node() {
                    if let Some(lfs) = v.get_lazy_frame_slices()? {
                        for lf in lfs {
                            let df = lf.select([col(OBJECT_COL_NAME)]).collect().unwrap();
                            let objects_u32_iter =
                                df.column(OBJECT_COL_NAME).unwrap().u32().unwrap();
                            for u in objects_u32_iter {
                                let u = u.unwrap();
                                if let Some(in_deg) = out_map.get_mut(&u) {
                                    *in_deg = in_deg.saturating_add(1);
                                } else {
                                    out_map.insert(u, 1);
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
                            for u in subjects_u32_iter {
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
        Ok((zero_out_set, out_one))
    }
}

fn update_blocks_map(
    map: &mut HashMap<u32, TurtleBlock>,
    df: &DataFrame,
    pred: &NamedNode,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
    global_cats: LockedCats,
    current_subjects: &HashSet<u32>,
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
    );
    let mut to_replace = if object_type.is_blank_node() {
        let obj_u32s = df.column(OBJECT_COL_NAME).unwrap().u32().unwrap();
        let mut obj_deferred = Vec::with_capacity(obj_u32s.len());
        for u in obj_u32s {
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
    );
    for ((s_u32, s), o) in subj_u32s.iter().zip(subj_terms).zip(obj_terms) {
        let s_u32 = s_u32.unwrap();
        let s = if in_degree_zero_blanks.contains(&s_u32) {
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
        if used_subjects.contains(&s_u32) || !current_subjects.contains(&s_u32) {
            continue;
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
    subjects_ordering: &Vec<u32>,
    in_degree_one_map: &HashMap<u32, TurtleBlockOrTermOrList>,
) -> Result<Vec<u32>, TriplestoreError> {
    let out: Result<Vec<Vec<_>>, TriplestoreError> = subjects_ordering
        .iter()
        .map(|k| {
            let r = if let Some(block) = blocks_map.get(k) {
                let r = block
                    .write_block(writer, type_nn, prefix_replacer, true, in_degree_one_map, 1)
                    .map_err(|x| TriplestoreError::WriteTurtleError(x.to_string()));
                let replaced = r?;
                writeln!(writer, " .\n")
                    .map_err(|x| TriplestoreError::WriteTurtleError(x.to_string()))?;
                replaced
            } else {
                vec![]
            };
            Ok(r)
        })
        .collect();
    let outs = out?.into_iter().flatten().collect();
    Ok(outs)
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
                "{}:{}",
                self.replacements.get(find.pattern().as_usize()).unwrap(),
                rest
            )
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
    let lf = concat(
        sel_lfs,
        UnionArgs {
            parallel: true,
            rechunk: false,
            to_supertypes: false,
            diagonal: false,
            from_partitioned_ds: false,
            maintain_order: true,
        },
    )
    .unwrap();
    lf
}

fn is_list_pred_and_subject_type(nn: &NamedNode, subject_type: &BaseRDFNodeType) -> bool {
    (nn.as_str() == rdf::FIRST || nn.as_str() == rdf::REST) && subject_type.is_blank_node()
}

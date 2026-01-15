use super::Triplestore;
use crate::errors::TriplestoreError;
use crate::sparql::{QueryResultKind, QuerySettings};
use crate::storage::Triples;
use aho_corasick::{AhoCorasick, MatchKind};
use oxrdf::vocab::rdf;
use oxrdf::{NamedNode, Term, Variable};
use polars::prelude::{col, len};
use polars_core::frame::DataFrame;
use polars_core::utils::arrow::array::ViewType;
use representation::cats::LockedCats;
use representation::dataset::NamedGraph;
use representation::polars_to_rdf::column_as_terms;
use representation::{BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use spargebra::algebra::{Expression, Function, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use std::cmp;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::sync::Arc;

const STRIDE: usize = 10_000;

struct TurtleBlock {
    subject: Term,
    pred_term_map: BTreeMap<NamedNode, Vec<Term>>,
}

impl TurtleBlock {
    pub(crate) fn write_block<W: Write>(
        &self,
        writer: &mut W,
        type_nn: &NamedNode,
        prefixes: &mut HashMap<String, NamedNode>,
        lists_map: Option<&Vec<Arc<Term>>>,
        prefix_replacer: &PrefixReplacer,
    ) -> std::io::Result<()> {
        write_term_prefixed(writer, &self.subject, prefix_replacer)?;
        let mut has_type = false;
        if let Some(ts) = self.pred_term_map.get(type_nn) {
            write!(writer, " a ")?;
            write_term_objects(writer, ts.as_slice(), prefix_replacer)?;
            if self.pred_term_map.len() > 1 {
                writeln!(writer, " ; ")?;
            }
            has_type = true;
        }
        let mut i = 0usize;
        for (key, values) in &self.pred_term_map {
            if key != type_nn {
                if i > 0 || has_type {
                    write!(writer, "\t")?;
                } else {
                    write!(writer, " ")?;
                }
                prefix_replacer.write_with_prefix(writer, key)?;
                write!(writer, " ")?;
                write_term_objects(writer, values.as_slice(), prefix_replacer)?;
                if i < self.pred_term_map.len() - 1 {
                    writeln!(writer, " ;")?;
                }
            }
            i += 1;
        }
        writeln!(writer, " .\n")?;
        Ok(())
    }
}

fn write_term_objects<W: Write>(writer: &mut W, terms: &[Term], prefix_replacer: &PrefixReplacer) -> std::io::Result<()> {
    if terms.len() == 1 {
        write_term_prefixed(writer, terms.get(0).unwrap(), prefix_replacer)?;
    } else {
        for (i,v) in terms.iter().enumerate() {
            write_term_prefixed(writer, v, prefix_replacer)?;
            if i < terms.len() - 1 {
                write!(writer, ", ")?;
            }
        }
    }
    Ok(())
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

        let map = if let Some(map) = self.graph_triples_map.get(graph) {
            map
        } else {
            return Err(TriplestoreError::GraphDoesNotExist(graph.to_string()));
        };

        let lists_map = None; //self.create_lists_map(map, graph)?;

        let mut drivers = vec![];
        for (pred, m) in map.iter() {
            for k in m.keys() {
                if pred.as_ref() != rdf::TYPE
                    && (lists_map.is_none()
                        || !(pred.as_ref() == rdf::FIRST && &k.0 == &BaseRDFNodeType::BlankNode))
                    && (lists_map.is_none()
                        || !(pred.as_ref() == rdf::REST && &k.0 == &BaseRDFNodeType::BlankNode))
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
        let mut used_blank_subjects = HashSet::new();
        for (driver_predicate, k) in drivers.into_iter().rev() {
            if let Some(ks) = used_drivers.get_mut(&driver_predicate) {
                ks.insert(k.clone());
            } else {
                used_drivers.insert(
                    driver_predicate.clone(),
                    HashSet::from_iter(vec![k.clone()]),
                );
            }
            let t = map.get(&driver_predicate).unwrap().get(&(k)).unwrap();
            let driver_height = t.height();

            let mut current_offset = 0;
            for i in 0..((driver_height % STRIDE) + 1) {
                let triples = map.get(&driver_predicate).unwrap().get(&k).unwrap();
                let offset_start = i * STRIDE;
                let height = cmp::min(STRIDE, driver_height - current_offset);
                current_offset += height;
                let df =
                    if let Some(lf) = triples.get_lazy_frame_slice(offset_start, Some(height))? {
                        lf.collect().unwrap()
                    } else {
                        break;
                    };
                let (subject_type, object_type) = &k;
                let new_subj_u32: HashSet<u32> = df
                    .column(SUBJECT_COL_NAME)
                    .unwrap()
                    .u32()
                    .unwrap()
                    .iter()
                    .map(|x| x.unwrap())
                    .collect();
                let mut blocks_map = HashMap::new();
                //let mut blocks_iri_ordering = Vec::new();
                //let mut blocks_blank_ordering = Vec::new();

                update_blocks_map(
                    &mut blocks_map,
                    &df,
                    &driver_predicate,
                    subject_type,
                    object_type,
                    self.global_cats.clone(),
                    &used_iri_subjects,
                    &used_blank_subjects,
                )?;
                let first_u32 = df
                    .column(SUBJECT_COL_NAME)
                    .unwrap()
                    .u32()
                    .unwrap()
                    .first()
                    .unwrap();
                let last_u32 = df
                    .column(SUBJECT_COL_NAME)
                    .unwrap()
                    .u32()
                    .unwrap().last().unwrap();

                let first = self
                    .global_cats
                    .read()?
                    .maybe_decode_of_type(&first_u32, subject_type)
                    .unwrap()
                    .into_owned();
                let last = self
                    .global_cats
                    .read()?
                    .maybe_decode_of_type(&last_u32, subject_type)
                    .unwrap()
                    .into_owned();

                // First stride through and do the "left join"
                for (p, m) in map.iter() {
                    for (k, t) in m.iter() {
                        if let Some(ks) = used_drivers.get(p) {
                            if ks.contains(k) {
                                continue;
                            }
                        }

                        let (s, o) = k;
                        if let Some(lf) = t.get_lazy_frame_between_subject_strings(
                            first.as_str(),
                            last.as_str(),
                            self.global_cats.clone(),
                            subject_type,
                        )? {
                            let other_df = lf.collect().unwrap();
                            update_blocks_map(
                                &mut blocks_map,
                                &other_df,
                                p,
                                s,
                                o,
                                self.global_cats.clone(),
                                &used_iri_subjects,
                                &used_blank_subjects,
                            )?;
                        }
                    }
                }
                if subject_type.is_iri() {
                    used_iri_subjects.extend(new_subj_u32);
                } else if subject_type.is_blank_node() {
                    used_blank_subjects.extend(new_subj_u32);
                }
                let type_nn = rdf::TYPE.into_owned();
                let prefix_replacer = PrefixReplacer::new(prefixes);
                write_blocks(
                    writer,
                    blocks_map,
                    &type_nn,
                    lists_map.as_ref(),
                    &prefix_replacer,
                )?;
            }
        }
        Ok(())
    }
    fn create_lists_map(
        &self,
        map: &HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
        graph: &NamedGraph,
    ) -> Result<Option<(HashMap<u32, Vec<Arc<Term>>>)>, TriplestoreError> {
        let mut inv_rest_blank_blank_map: HashMap<u32, Vec<u32>> = HashMap::new();
        let mut first_blank_term_map = HashMap::new();
        let mut blank_lists_map = HashMap::new();

        if let Some(first_triple_map) = map.get(&rdf::FIRST.into_owned()) {
            for ((s, o), v) in first_triple_map {
                if s.is_blank_node() {
                    let lf = v.get_lazy_frame_slice(0, None)?;
                    if let Some(lf) = lf {
                        let df = lf
                            .select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)])
                            .collect()
                            .unwrap();
                        let subject_u32s = df.column(SUBJECT_COL_NAME).unwrap().u32().unwrap();
                        let os = o.default_stored_cat_state();
                        let terms = column_as_terms(
                            df.column(OBJECT_COL_NAME).unwrap(),
                            &RDFNodeState::from_bases(o.clone(), os),
                            self.global_cats.clone(),
                        );

                        for (s, t) in subject_u32s.iter().zip(terms.into_iter()) {
                            first_blank_term_map.insert(s.unwrap(), Arc::new(t.unwrap()));
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
                if let Some(term) = first_blank_term_map.get(&u.unwrap()) {
                    blank_lists_map.insert(u.unwrap(), vec![term.clone()]);
                } else {
                    // Todo handle invalid list
                    return Ok(None);
                }
            }
        } else {
            unreachable!("Should never happen")
        }

        if let Some(rest_triple_map) = map.get(&rdf::REST.into_owned()) {
            for ((s, o), v) in rest_triple_map {
                if s.is_blank_node() && o.is_blank_node() {
                    let lf = v.get_lazy_frame_slice(0, None)?;
                    if let Some(lf) = lf {
                        let df = lf
                            .select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)])
                            .collect()
                            .unwrap();
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
            let mut finished_iri_lists_map = HashMap::new();

            while !blank_lists_map.is_empty() {
                let mut new_blank_lists_map = HashMap::new();

                for (blank, mut list) in blank_lists_map {
                    if let Some(blanks) = inv_rest_blank_blank_map.remove(&blank) {
                        let first = first_blank_term_map.get(&blank).unwrap();
                        list.push(first.clone());
                        for blank in blanks {
                            new_blank_lists_map.insert(blank, list.clone());
                        }
                    } else {
                        finished_iri_lists_map.insert(blank, list);
                    }
                }
                blank_lists_map = new_blank_lists_map;
            }
        } else {
            return Ok(None);
        }

        Ok(Some(blank_lists_map))
    }
}

fn update_blocks_map(
    map: &mut HashMap<u32, TurtleBlock>,
    df: &DataFrame,
    pred: &NamedNode,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
    global_cats: LockedCats,
    used_iri_subjects: &HashSet<u32>,
    used_blank_subjects: &HashSet<u32>,
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
    let obj_terms = column_as_terms(
        df.column(OBJECT_COL_NAME).unwrap(),
        &RDFNodeState::from_bases(object_type.clone(), object_type.default_stored_cat_state()),
        global_cats.clone(),
    );
    for ((s_u32, s), o) in subj_u32s.iter().zip(subj_terms).zip(obj_terms) {
        let s_u32 = s_u32.unwrap();
        if subject_type.is_iri() {
            if used_iri_subjects.contains(&s_u32) {
                continue;
            }
        }

        if subject_type.is_blank_node() {
            if used_blank_subjects.contains(&s_u32) {
                continue;
            }
        }
        let s = s.unwrap();
        let o = o.unwrap();

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
    lists_map: Option<&HashMap<u32, Vec<Arc<Term>>>>,
    prefix_replacer: &PrefixReplacer,
) -> Result<(), TriplestoreError> {
    let mut sorted_keys: Vec<_> = blocks_map.keys().collect();
    sorted_keys.sort();

    let out: Result<Vec<()>, TriplestoreError> = sorted_keys
        .into_iter()
        .map(|k| {
            let lists_map = if let Some(lists_map) = lists_map {
                lists_map.get(k)
            } else {
                None
            };
            let r = blocks_map
                .get(k)
                .unwrap()
                .write_block(
                    writer,
                    type_nn,
                    &mut HashMap::new(),
                    lists_map,
                    prefix_replacer,
                )
                .map_err(|x| TriplestoreError::WriteTurtleError(x.to_string()));
            r?;
            Ok(())
        })
        .collect();
    out?;
    Ok(())
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
        nn: &NamedNode,
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

fn write_term_prefixed<W: Write>(
    writer: &mut W,
    term: &Term,
    prefix_replacer: &PrefixReplacer,
) -> std::io::Result<()> {
    match term {
        Term::NamedNode(nn) => prefix_replacer.write_with_prefix(writer, nn),
        t => write!(writer, "{}", t),
    }
}

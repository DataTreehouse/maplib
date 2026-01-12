use super::Triplestore;
use crate::errors::TriplestoreError;
use oxrdf::vocab::rdf;
use oxrdf::{NamedNode, Term};
use polars_core::frame::DataFrame;
use representation::cats::LockedCats;
use representation::dataset::NamedGraph;
use representation::polars_to_rdf::column_as_terms;
use representation::{BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::cmp;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use polars::prelude::len;

const STRIDE: usize = 10_000;

struct TurtleBlock {
    subject: Term,
    pred_term_map: BTreeMap<NamedNode, Vec<Term>>,
}

impl TurtleBlock {
    pub(crate) fn write_block<W: Write>(&self, writer: &mut W, type_nn:&NamedNode, prefixes:&mut HashMap<String, NamedNode>) -> Result<(), std::io::Error> {
        write!(writer, "{}", self.subject)?;
        if let Some(ts) = self.pred_term_map.get(type_nn) {
            write!(writer, " a ")?;
            let mut i = 0usize;
            for t in ts {
                write!(writer, "{}", t)?;
                if i < ts.len() - 1 {
                    write!(writer, ", ")?;
                }
                i += 1;
            }
            if self.pred_term_map.len() > 1 {
                writeln!(writer, "; ")?;
            }
        }
        let mut i = 0usize;
        for (key, value) in &self.pred_term_map {
            write!(writer, "{}", key)?;
            if value.len() == 1 {

            }
            i += 1;
        }
        Ok(())
    }
}

impl Triplestore {
    pub fn write_pretty_turtle<W: Write>(
        &self,
        writer: &mut W,
        graph: &NamedGraph,
    ) -> Result<(), TriplestoreError> {
        let map = if let Some(map) = self.graph_triples_map.get(graph) {
            map
        } else {
            return Err(TriplestoreError::GraphDoesNotExist(graph.to_string()));
        };

        let mut drivers = vec![];
        for (pred, m) in map.iter() {
            for k in m.keys() {
                if pred.as_ref() != rdf::TYPE && pred.as_ref() != rdf::FIRST && pred.as_ref() != rdf::REST {
                    drivers.push((pred.clone(), k.clone()));
                }
            }
        }
        drivers.sort();
        if let Some(m) = map.get(&rdf::TYPE.into_owned()) {
            let k = (BaseRDFNodeType::IRI, BaseRDFNodeType::IRI);
            if m.contains_key(&k) {
                drivers.push((rdf::TYPE.into_owned(), k));
            }
        }
        let mut used_drivers: HashMap<_, HashSet<(BaseRDFNodeType, BaseRDFNodeType)>> =
            HashMap::new();
        let mut used_iri_subjects = HashSet::new();
        let mut used_blank_subjects = HashSet::new();
        for (driver_predicate, k) in drivers {
            if let Some(ks) = used_drivers.get_mut(&driver_predicate) {
                ks.insert(k.clone());
            } else {
                used_drivers.insert(driver_predicate.clone(), HashSet::from_iter(vec![k.clone()]));
            }
            let mut driver_height = 0;
            let t = map.get(&driver_predicate).unwrap().get(&(k)).unwrap();
            driver_height = t.height();

            let mut current_offset = 0;
            for i in 0..(driver_height % STRIDE + 1) {
                let triples = map.get(&driver_predicate).unwrap().get(&k).unwrap();
                let offset_start = i * STRIDE;
                let offset_end = cmp::min(STRIDE, driver_height - current_offset);
                current_offset += STRIDE;
                let df = if let Some(lf) =
                    triples.get_lazy_frame_between_offsets(offset_start, offset_end)?
                {
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
                    .unwrap()
                    .last()
                    .unwrap();

                let last = self.global_cats.read()?.decode_iri_u32(&last_u32, None);
                let first = self.global_cats.read()?.decode_iri_u32(&first_u32, None);
                // First stride through and do the "left join"
                for (p, m) in map.iter() {
                    for (k, t) in m.iter() {
                        if let Some(ks) = used_drivers.get(p) {
                            if ks.contains(k) {
                                continue;
                            }
                        }
                        let (s, o) = k;
                        if let Some(lf) =
                            t.get_lazy_frame_between_subject_strings(first.as_str(), last.as_str())?
                        {
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
                write_blocks(writer, blocks_map, &type_nn)?;
            }
        }
        Ok(())
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
) -> Result<(), TriplestoreError> {
    let mut sorted_keys: Vec<_> = blocks_map.keys().collect();
    sorted_keys.sort();
    let out: Result<Vec<()>, TriplestoreError> = sorted_keys.into_iter().map(|k|{
        let r = blocks_map.get(k).unwrap().write_block(writer, type_nn, &mut HashMap::new()).map_err(|x|TriplestoreError::WriteTurtleError(x.to_string()));
        r?;
        Ok(())
    }).collect();
    out?;
    Ok(())
}


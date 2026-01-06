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
use std::collections::{HashMap};
use std::io::Write;

const STRIDE: usize = 10_000;

struct TurtleBlock {
    subject: Term,
    pred_term_map: HashMap<NamedNode, Vec<Term>>,
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

        let driver_predicate = if map.contains_key(&rdf::TYPE.into_owned()) {
            rdf::TYPE.into_owned()
        } else {
            // Find min that has nn,nn
            map.keys().min().unwrap().clone()
        };

        let mut driver_height = 0;
        for ((s, o), t) in map.get(&driver_predicate).unwrap() {
            if s.is_iri() && o.is_iri() {
                driver_height = driver_height + t.height();
            }
        }
        let mut current_offset = 0;
        for i in 0..(driver_height % STRIDE + 1) {
            let triples = map
                .get(&driver_predicate)
                .unwrap()
                .get(&(BaseRDFNodeType::IRI, BaseRDFNodeType::IRI))
                .unwrap();
            let offset_start = i * STRIDE;
            let offset_end = cmp::min(STRIDE, driver_height - current_offset);
            let df = if let Some(lf) =
                triples.get_lazy_frame_between_offsets(offset_start, offset_end)?
            {
                lf.collect().unwrap()
            } else {
                break;
            };
            let mut blocks_map = HashMap::new();
            update_blocks_map(
                &mut blocks_map,
                &df,
                &driver_predicate,
                &BaseRDFNodeType::IRI,
                &BaseRDFNodeType::IRI,
                self.global_cats.clone(),
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
                for ((s, o), t) in m.iter() {
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
                        )?;
                    }
                }
            }
        }

        // Second stride through and to the "right join"

        // Finally write bn_subjects that are not yet written
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

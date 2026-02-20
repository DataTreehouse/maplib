use super::{encode_triples, re_encode, CatEncs, CatTriples, CatType, Cats};
use crate::dataset::NamedGraph;
use crate::solution_mapping::BaseCatState;
use crate::BaseRDFNodeType;
use oxrdf::NamedNode;
use polars::frame::DataFrame;
use std::collections::HashSet;
use std::time::Instant;
use tracing::instrument;

impl Cats {
    pub fn globalize(&mut self, mut cat_triples: Vec<CatTriples>) -> Vec<CatTriples> {
        let local_cats: Vec<_> = cat_triples
            .iter_mut()
            .map(|x| x.local_cats.drain(..))
            .flatten()
            .collect();
        let re_enc_map = self.merge(local_cats);
        let global_cat_triples = re_encode(cat_triples, re_enc_map);
        global_cat_triples
    }

    pub fn encode_predicates(&mut self, cat_triples: &Vec<CatTriples>) {
        if cat_triples.is_empty() {
            return;
        }
        let mut predicates = HashSet::new();
        for ct in cat_triples {
            let pred = ct.predicate.as_str().to_string();
            predicates.insert(pred);
        }
        let t = CatType::IRI;
        if !self.cat_map.contains_key(&t) {
            self.cat_map.insert(
                t.clone(),
                CatEncs::new_empty(
                    self.path.as_ref().map(|x| x.as_ref()),
                    &BaseRDFNodeType::IRI,
                ),
            );
        }
        let mut iri_counter = self.iri_counter;
        let enc = self.cat_map.get_mut(&t).unwrap();
        let predicates: Vec<_> = predicates.into_iter().collect();
        enc.maps
            .encode_all_new_non_duplicated_strings(predicates, &mut iri_counter);
        self.iri_counter = iri_counter;
    }
}

#[instrument(skip_all)]
pub fn cat_encode_triples(
    df: DataFrame,
    subject_type: BaseRDFNodeType,
    object_type: BaseRDFNodeType,
    predicate: NamedNode,
    graph: NamedGraph,
    subject_cat_state: BaseCatState,
    object_cat_state: BaseCatState,
    global_cats: &Cats,
) -> CatTriples {
    let (local_cats, encoded_triples) = encode_triples(
        df,
        &subject_type,
        &object_type,
        subject_cat_state,
        object_cat_state,
        global_cats,
    );

    CatTriples {
        encoded_triples,
        predicate,
        graph,
        subject_type,
        object_type,
        local_cats,
    }
}

use super::{encode_triples, re_encode, CatEncs, CatTriples, CatType, Cats};
use crate::dataset::NamedGraph;
use crate::solution_mapping::BaseCatState;
use crate::BaseRDFNodeType;
use oxrdf::{GraphName, NamedNode};
use polars::frame::DataFrame;
use std::path::Path;
use tracing::instrument;

impl Cats {
    pub fn globalize(
        &mut self,
        mut cat_triples: Vec<CatTriples>,
        path: Option<&Path>,
    ) -> Vec<CatTriples> {
        let local_cats: Vec<_> = cat_triples
            .iter_mut()
            .map(|x| x.local_cats.drain(..))
            .flatten()
            .collect();
        let re_enc_map = self.merge(local_cats, path);
        let global_cat_triples = re_encode(cat_triples, re_enc_map);
        global_cat_triples
    }

    pub fn encode_predicates(&mut self, cat_triples: &Vec<CatTriples>, path: Option<&Path>) {
        for ct in cat_triples {
            let t = CatType::IRI;
            if !self.cat_map.contains_key(&t) {
                self.cat_map.insert(t.clone(), CatEncs::new_empty(path));
            }
            let enc = self.cat_map.get_mut(&t).unwrap();
            let pred = ct.predicate.as_str().to_string();
            if !enc.maps.contains_str(&pred) {
                enc.encode_new_string(pred, self.iri_counter);
                self.iri_counter += 1;
            }
        }
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
    path: Option<&Path>,
) -> CatTriples {
    let (local_cats, encoded_triples) = encode_triples(
        df,
        &subject_type,
        &object_type,
        subject_cat_state,
        object_cat_state,
        global_cats,
        path,
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

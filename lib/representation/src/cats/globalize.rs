use super::{encode_triples, re_encode, CatTriples, Cats};
use crate::solution_mapping::BaseCatState;
use crate::BaseRDFNodeType;
use oxrdf::NamedNode;
use polars::frame::DataFrame;

impl Cats {
    pub fn globalize(&mut self, mut cat_triples: Vec<CatTriples>) -> Vec<CatTriples> {
        let local_cats: Vec<_> = cat_triples
            .iter_mut()
            .map(|x| x.local_cats.drain(..))
            .flatten()
            .collect();
        let re_enc_map = self.merge(local_cats);
        let global_cats = re_encode(cat_triples, re_enc_map);
        global_cats
    }
}

pub fn cat_encode_triples(
    df: DataFrame,
    subject_type: BaseRDFNodeType,
    object_type: BaseRDFNodeType,
    predicate: NamedNode,
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
        subject_type,
        object_type,
        local_cats,
    }
}

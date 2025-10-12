use super::{encode_triples, rdf_split_iri_str, re_encode, CatEncs, CatTriples, CatType, Cats};
use crate::solution_mapping::BaseCatState;
use crate::BaseRDFNodeType;
use oxrdf::NamedNode;
use polars::frame::DataFrame;
use tracing::instrument;

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

    pub fn encode_predicates(&mut self, cat_triples: &Vec<CatTriples>) {
        for ct in cat_triples {
            let (pre, suf) = rdf_split_iri_str(ct.predicate.as_str());
            let ct = CatType::Prefix(NamedNode::new_unchecked(pre));
            if !self.cat_map.contains_key(&ct) {
                self.cat_map.insert(ct.clone(), CatEncs::new_empty());
            }

            let enc = self.cat_map.get_mut(&ct).unwrap();
            if !enc.contains_key(suf) {
                enc.encode_new_str(&suf, self.iri_height);
                self.iri_height += 1;
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

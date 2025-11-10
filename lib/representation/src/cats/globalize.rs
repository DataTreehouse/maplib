use std::sync::Arc;
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
            let prefix = NamedNode::new_unchecked(pre);
            let ct = CatType::Prefix(prefix.clone());
            let prefix_u = if let Some(prefix_u) = self.prefix_rev_map.get(&prefix) {
                *prefix_u
            } else {
                self.cat_map.insert(ct.clone(), CatEncs::new_empty(true));
                let prefix_u = self.prefix_map.len() as u32;
                self.prefix_map
                    .insert(prefix_u, NamedNode::new_unchecked(pre));
                self.prefix_rev_map
                    .insert(NamedNode::new_unchecked(pre), prefix_u);
                prefix_u
            };

            let enc = self.cat_map.get_mut(&ct).unwrap();
            if !enc.contains_key(suf) {
                self.belongs_prefix_map.insert(self.iri_counter, prefix_u);
                let arc_suf = Arc::new(suf.to_string());
                 enc.encode_new_arc_string(arc_suf.clone(), self.iri_counter);
                self.rev_iri_suffix_map.insert(self.iri_counter, arc_suf);
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

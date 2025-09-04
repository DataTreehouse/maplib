use super::Triplestore;
use representation::cats::CatTriples;
use std::sync::Arc;

impl Triplestore {
    pub fn globalize(&mut self, cat_triples: Vec<CatTriples>) -> Vec<CatTriples> {
        let cat_triples = if let Some(mutcat) = Arc::get_mut(&mut self.cats) {
            let cat_triples = mutcat.globalize(cat_triples);
            mutcat.encode_predicates(&cat_triples);
            cat_triples
        } else {
            unreachable!("Should never happen");
        };
        cat_triples
    }
}

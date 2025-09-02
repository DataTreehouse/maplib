use super::Triplestore;
use representation::cats::CatTriples;
use std::sync::Arc;

impl Triplestore {
    pub fn globalize(&mut self, cat_triples: Vec<CatTriples>) -> Vec<CatTriples> {
        let mutcat = Arc::get_mut(&mut self.cats);
        mutcat.unwrap().globalize(cat_triples)
    }
}

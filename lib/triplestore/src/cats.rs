use super::Triplestore;
use representation::cats::CatTriples;

impl Triplestore {
    pub fn globalize(&mut self, cat_triples: Vec<CatTriples>) -> Vec<CatTriples> {
        let mut mutcat = self.global_cats.write().unwrap();
        let cat_triples = {
            let cat_triples = mutcat.globalize(cat_triples);
            mutcat.encode_predicates(&cat_triples);
            cat_triples
        };
        cat_triples
    }
}

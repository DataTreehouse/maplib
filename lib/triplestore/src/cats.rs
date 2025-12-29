use super::Triplestore;
use representation::cats::CatTriples;

impl Triplestore {
    pub fn globalize(&mut self, cat_triples: Vec<CatTriples>) -> Vec<CatTriples> {
        let mut mutcat = self.global_cats.write().unwrap();
        let cat_triples = {
            let cat_triples = mutcat.globalize(
                cat_triples,
                self.storage_folder.as_ref().map(|x| x.as_ref()),
            );
            mutcat.encode_predicates(
                &cat_triples,
                self.storage_folder.as_ref().map(|x| x.as_ref()),
            );
            cat_triples
        };
        cat_triples
    }
}

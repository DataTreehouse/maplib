use super::Triplestore;
use representation::cats::CatTriples;
use representation::constants::normalize_legacy_chrontext_named_node;

impl Triplestore {
    pub fn globalize(&mut self, mut cat_triples: Vec<CatTriples>) -> Vec<CatTriples> {
        // Deprecated predicates in the former chrontext namespace are stored as the predicates of
        // the maplib vocabulary that replaced them, so the stored predicate is the one to encode:
        // queries that bind the predicate (?s ?p ?o) look every stored predicate up in the
        // dictionary.
        for ct in &mut cat_triples {
            ct.predicate = normalize_legacy_chrontext_named_node(ct.predicate.clone());
        }
        let mut mutcat = self.global_cats.write().unwrap();
        {
            let cat_triples = mutcat.globalize(cat_triples);
            mutcat.encode_predicates_and_named_graphs(&cat_triples);
            cat_triples
        }
    }
}

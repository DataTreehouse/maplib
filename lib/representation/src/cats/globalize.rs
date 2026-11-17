use super::{encode_triples, re_encode, CatEncs, CatTriples, CatType, Cats};
use crate::dataset::NamedGraph;
use crate::solution_mapping::BaseCatState;
use crate::BaseRDFNodeType;
use oxrdf::NamedNode;
use polars::frame::DataFrame;
use std::collections::HashSet;
use tracing::instrument;

impl Cats {
    pub fn globalize(&mut self, mut cat_triples: Vec<CatTriples>) -> Vec<CatTriples> {
        let local_cats: Vec<_> = cat_triples
            .iter_mut()
            .flat_map(|x| x.local_cats.drain(..))
            .collect();
        let re_enc_map = self.merge(local_cats);
        let mut global_cat_triples = re_encode(cat_triples, re_enc_map);
        for c in &mut global_cat_triples {
            c.local_cats.clear();
            c.encoded_triples.subject_local_cat_uuid = None;
            c.encoded_triples.object_local_cat_uuid = None;
        }
        global_cat_triples
    }

    pub fn encode_predicates_and_named_graphs(&mut self, cat_triples: &Vec<CatTriples>) {
        if cat_triples.is_empty() {
            return;
        }
        let mut predicates_and_named_graphs = HashSet::new();
        for ct in cat_triples {
            let pred = ct.predicate.as_str().to_string();
            predicates_and_named_graphs.insert(pred);
            if let NamedGraph::NamedGraph(nn) = &ct.graph {
                predicates_and_named_graphs.insert(nn.as_str().to_string());
            }
        }
        self.encode_iris(predicates_and_named_graphs);
    }

    /// Adds the IRIs that are not yet in the IRI dictionary, e.g. predicates, which are stored as
    /// table keys rather than in encoded columns but are looked up when a query binds them.
    pub fn encode_iris(&mut self, iris: HashSet<String>) {
        if iris.is_empty() {
            return;
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
        let iris: Vec<_> = iris.into_iter().collect();
        enc.maps
            .encode_all_new_non_duplicated_strings(iris, &mut iri_counter);
        self.iri_counter = iri_counter;
    }
}

#[instrument(skip_all)]
#[allow(clippy::too_many_arguments)]
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

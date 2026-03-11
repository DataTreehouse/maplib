use oxrdf::{GraphName, Quad, Subject, Term};
use std::collections::HashMap;
type MapType = HashMap<String, HashMap<String, (Vec<Subject>, Vec<Term>)>>;

pub fn remap_predicate_datatype(
    _predicate_map: HashMap<GraphName, HashMap<String, MapType>>,
) -> HashMap<GraphName, HashMap<String, MapType>> {
    unimplemented!("Contact Data Treehouse to try!")
}

pub fn fix_cim_quad(_quad: Quad, _base_iri: Option<&String>) -> Quad {
    unimplemented!("Contact Data Treehouse to try!")
}

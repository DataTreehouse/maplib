use oxrdf::NamedNode;
use std::collections::HashMap;
use templates::constants::{
    OTTR_PREFIX, OTTR_PREFIX_IRI, OWL_PREFIX, OWL_PREFIX_IRI, RDFS_PREFIX, RDFS_PREFIX_IRI,
    RDF_PREFIX, RDF_PREFIX_IRI, SHACL_PREFIX, SHACL_PREFIX_IRI, XSD_PREFIX, XSD_PREFIX_IRI,
};

pub fn get_default_prefixes() -> HashMap<String, NamedNode> {
    let predefined = [
        (RDFS_PREFIX, RDFS_PREFIX_IRI),
        (RDF_PREFIX, RDF_PREFIX_IRI),
        (XSD_PREFIX, XSD_PREFIX_IRI),
        (OTTR_PREFIX, OTTR_PREFIX_IRI),
        (OWL_PREFIX, OWL_PREFIX_IRI),
        (SHACL_PREFIX, SHACL_PREFIX_IRI),
    ];
    HashMap::from_iter(
        predefined
            .into_iter()
            .map(|(x, y)| (x.to_string(), NamedNode::new_unchecked(y))),
    )
}

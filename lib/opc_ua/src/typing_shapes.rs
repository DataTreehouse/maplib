use maplib::errors::MaplibError;
use maplib::model::Model;
use oxrdf::NamedNode;
use representation::dataset::NamedGraph;

/// Which namespaces hold types.
pub enum TypeNamespaces<'a> {
    /// A namespace holds types when it has type nodes.
    Derived,
    /// These namespaces hold types, all others instances.
    Types(&'a [NamedNode]),
    /// These namespaces hold instances, all others types.
    Instances(&'a [NamedNode]),
}

pub fn add_typing_shapes(
    _m: &mut Model,
    _shapes_graph: &NamedGraph,
    _namespaces: TypeNamespaces,
) -> Result<(), MaplibError> {
    unimplemented!("Contact Data Treehouse to try");
}

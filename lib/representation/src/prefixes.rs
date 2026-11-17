use crate::constants::{
    DEFAULT_PREFIX, DEFAULT_PREFIX_IRI, FOAF_PREFIX, FOAF_PREFIX_IRI, FX_PREFIX, FX_PREFIX_IRI,
    MAPLIB_PREFIX, MAPLIB_PREFIX_IRI, OTTR_PREFIX, OTTR_PREFIX_IRI, OWL_PREFIX, OWL_PREFIX_IRI,
    RDFS_PREFIX, RDFS_PREFIX_IRI, RDF_PREFIX, RDF_PREFIX_IRI, SHACL_PREFIX, SHACL_PREFIX_IRI,
    SKOS_PREFIX, SKOS_PREFIX_IRI, XSD_PREFIX, XSD_PREFIX_IRI, XYZ_PREFIX, XYZ_PREFIX_IRI,
};
use oxrdf::NamedNode;
use std::collections::HashMap;

/// The prefixes every model knows without being given (when parsing SPARQL, Datalog and OTTR):
/// [`WELL_KNOWN_PREFIXES`], maplib's one table of default prefixes.
pub fn get_default_prefixes() -> HashMap<String, NamedNode> {
    well_known_prefixes()
}

/// The prefixes maplib knows by name: the usual prefix of each well-known vocabulary (name,
/// namespace). This is maplib's one table of default prefixes: every model knows them when parsing
/// ([`get_default_prefixes`]), template generation and the RDF/XML writer name namespaces with
/// them, and Python (`maplib.well_known_prefixes()`, `maplib.config`) and applications such as the
/// Graph portal take them from here. Nothing else defines default prefixes.
///
/// `cim` is always CIM100 (CGMES 3); CIM16 (CGMES 2.4.15) is `cim16`. Names and namespaces are
/// unique.
pub const WELL_KNOWN_PREFIXES: &[(&str, &str)] = &[
    // maplib's own and the core W3C vocabularies
    (RDF_PREFIX, RDF_PREFIX_IRI),
    (RDFS_PREFIX, RDFS_PREFIX_IRI),
    (XSD_PREFIX, XSD_PREFIX_IRI),
    (OWL_PREFIX, OWL_PREFIX_IRI),
    (SHACL_PREFIX, SHACL_PREFIX_IRI),
    (SKOS_PREFIX, SKOS_PREFIX_IRI),
    (FOAF_PREFIX, FOAF_PREFIX_IRI),
    (OTTR_PREFIX, OTTR_PREFIX_IRI),
    (MAPLIB_PREFIX, MAPLIB_PREFIX_IRI),
    (FX_PREFIX, FX_PREFIX_IRI),
    (XYZ_PREFIX, XYZ_PREFIX_IRI),
    (DEFAULT_PREFIX, DEFAULT_PREFIX_IRI),
    // W3C and Dublin Core vocabularies
    ("dcterms", "http://purl.org/dc/terms/"),
    ("dc", "http://purl.org/dc/elements/1.1/"),
    ("prov", "http://www.w3.org/ns/prov#"),
    ("dcat", "http://www.w3.org/ns/dcat#"),
    ("time", "http://www.w3.org/2006/time#"),
    ("sosa", "http://www.w3.org/ns/sosa/"),
    ("ssn", "http://www.w3.org/ns/ssn/"),
    ("vann", "http://purl.org/vocab/vann/"),
    ("void", "http://rdfs.org/ns/void#"),
    ("schema", "https://schema.org/"),
    // geography
    ("geo", "http://www.opengis.net/ont/geosparql#"),
    ("geof", "http://www.opengis.net/def/function/geosparql/"),
    ("sf", "http://www.opengis.net/ont/sf#"),
    ("uom", "http://www.opengis.net/def/uom/OGC/1.0/"),
    ("wgs", "http://www.w3.org/2003/01/geo/wgs84_pos#"),
    // units
    ("qudt", "http://qudt.org/schema/qudt/"),
    ("unit", "http://qudt.org/vocab/unit/"),
    ("quantitykind", "http://qudt.org/vocab/quantitykind/"),
    // CIM: CGMES 3 (CIM100) is cim, CGMES 2.4.15 (CIM16) cim16, with their extensions and headers
    ("cim", "http://iec.ch/TC57/CIM100#"),
    ("cim16", "http://iec.ch/TC57/2013/CIM-schema-cim16#"),
    ("eu", "http://iec.ch/TC57/CIM100-European#"),
    ("entsoe", "http://entsoe.eu/CIM/SchemaExtension/3/1#"),
    ("md", "http://iec.ch/TC57/61970-552/ModelDescription/1#"),
    ("dm", "http://iec.ch/TC57/61970-552/DifferenceModel/1#"),
    // OPC UA and common companion specifications
    ("opc", "http://opcfoundation.org/UA/"),
    ("di", "http://opcfoundation.org/UA/DI/#"),
    ("machinery", "http://opcfoundation.org/UA/Machinery/#"),
    ("robotics", "http://opcfoundation.org/UA/Robotics/#"),
    ("ia", "http://opcfoundation.org/UA/IA/#"),
    // railways (the European Union Agency for Railways)
    ("era", "http://data.europa.eu/949/"),
    // maplib's configuration (the config graph)
    ("mcfg", "https://datatreehouse.github.io/maplib/config#"),
];

/// [`WELL_KNOWN_PREFIXES`] as a map from name to namespace.
pub fn well_known_prefixes() -> HashMap<String, NamedNode> {
    WELL_KNOWN_PREFIXES
        .iter()
        .map(|(name, ns)| (name.to_string(), NamedNode::new_unchecked(*ns)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn well_known_prefixes_are_unique_and_are_the_defaults() {
        let names: HashSet<_> = WELL_KNOWN_PREFIXES.iter().map(|(n, _)| *n).collect();
        let namespaces: HashSet<_> = WELL_KNOWN_PREFIXES.iter().map(|(_, ns)| *ns).collect();
        assert_eq!(names.len(), WELL_KNOWN_PREFIXES.len());
        assert_eq!(namespaces.len(), WELL_KNOWN_PREFIXES.len());
        let known = well_known_prefixes();
        assert_eq!(get_default_prefixes(), known);
        assert_eq!(known["cim"].as_str(), "http://iec.ch/TC57/CIM100#");
        assert_eq!(known["cim16"].as_str(), "http://iec.ch/TC57/2013/CIM-schema-cim16#");
    }
}

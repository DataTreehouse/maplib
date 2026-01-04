use pyo3::{pyclass, pymethods};
use representation::python::PyIRI;

const OWL_PREFIX: &str = "http://www.w3.org/2002/07/owl#";

#[derive(Clone, Debug)]
#[pyclass(name = "owl")]
pub struct PyOWL {}

#[pymethods]
impl PyOWL {
    #[classattr]
    #[pyo3(name = "allValuesFrom")]
    fn all_values_from() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "allValuesFrom")).unwrap()
    }

    #[classattr]
    #[pyo3(name = "annotatedProperty")]
    fn annotated_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "annotatedProperty")).unwrap()
    }

    #[classattr]
    #[pyo3(name = "annotatedSource")]
    fn annotated_source() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "annotatedSource")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "annotatedTarget")]
    fn annotated_target() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "annotatedTarget")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "assertionProperty")]
    fn assertion_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "assertionProperty")).unwrap()
    }
    #[classattr]
    fn cardinality() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "cardinality")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "complementOf")]
    fn complement_of() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "complementOf")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "datatypeComplementOf")]
    fn datatype_complement_of() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "datatypeComplementOf")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "differentFrom")]
    fn different_from() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "differentFrom")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "disjointUnionOf")]
    fn disjoint_union_of() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "disjointUnionOf")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "disjointWith")]
    fn disjoint_with() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "disjointWith")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "distinctMembers")]
    fn distinct_members() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "distinctMembers")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "equivalentClass")]
    fn equivalent_class() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "equivalentClass")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "equivalentProperty")]
    fn equivalent_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "equivalentProperty")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "hasKey")]
    fn has_key() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "hasKey")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "hasSelf")]
    fn has_self() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "hasSelf")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "hasValue")]
    fn has_value() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "hasValue")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "intersectionOf")]
    fn intersection_of() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "intersectionOf")).unwrap()
    }

    #[classattr]
    #[pyo3(name = "inverseOf")]
    fn inverse_of() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "inverseOf")).unwrap()
    }

    #[classattr]
    #[pyo3(name = "maxCardinality")]
    fn max_cardinality() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "maxCardinality")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "maxQualifiedCardinality")]
    fn max_qualified_cardinality() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "maxQualifiedCardinality")).unwrap()
    }
    #[classattr]
    fn members() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "members")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "minCardinality")]
    fn min_cardinality() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "minCardinality")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "minQualifiedCardinality")]
    fn min_qualified_cardinality() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "minQualifiedCardinality")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "onClass")]
    fn on_class() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "onClass")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "onDataRange")]
    fn on_data_range() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "onDataRange")).unwrap()
    }

    #[classattr]
    #[pyo3(name = "onDatatype")]
    fn on_datatype() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "onDatatype")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "onProperties")]
    fn on_properties() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "onProperties")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "onProperty")]
    fn on_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "onProperty")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "oneOf")]
    fn one_of() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "oneOf")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "propertyChainAxiom")]
    fn property_chain_axiom() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "propertyChainAxiom")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "propertyDisjointWith")]
    fn property_disjoint_with() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "propertyDisjointWith")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "qualifiedCardinality")]
    fn qualified_cardinality() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "qualifiedCardinality")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "sameAs")]
    fn same_as() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "sameAs")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "someValuesFrom")]
    fn some_values_from() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "someValuesFrom")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "sourceIndividual")]
    fn source_individual() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "sourceIndividual")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "targetIndividual")]
    fn target_individual() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "targetIndividual")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "targetValue")]
    fn target_value() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "targetValue")).unwrap()
    }

    #[classattr]
    #[pyo3(name = "unionOf")]
    fn union_of() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "unionOf")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "withRestrictions")]
    fn with_restrictions() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "withRestrictions")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "AllDifferent")]
    fn all_different() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "AllDifferent")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "AllDisjointClasses")]
    fn all_disjoint_classes() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "AllDisjointClasses")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "AllDisjointProperties")]
    fn all_disjoint_properties() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "AllDisjointProperties")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "Annotation")]
    fn annotation() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "Annotation")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "AnnotationProperty")]
    fn annotation_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "AnnotationProperty")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "Axiom")]
    fn axiom() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "Axiom")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "Class")]
    fn class() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "Class")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "DataRange")]
    fn data_range() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "DataRange")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "DatatypeProperty")]
    fn datatype_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "DatatypeProperty")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "DeprecatedClass")]
    fn deprecated_class() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "DeprecatedClass")).unwrap()
    }

    #[classattr]
    #[pyo3(name = "DeprecatedProperty")]
    fn deprecated_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "DeprecatedProperty")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "FunctionalProperty")]
    fn functional_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "FunctionalProperty")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "InverseFunctionalProperty")]
    fn inverse_functional_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "InverseFunctionalProperty")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "IrreflexiveProperty")]
    fn irreflexive_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "IrreflexiveProperty")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "NamedIndividual")]
    fn named_individual() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "NamedIndividual")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "NegativePropertyAssertion")]
    fn negative_property_assertion() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "NegativePropertyAssertion")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "ObjectProperty")]
    fn object_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "ObjectProperty")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "Ontology")]
    fn ontology() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "Ontology")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "OntologyProperty")]
    fn ontology_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "OntologyProperty")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "ReflexiveProperty")]
    fn reflexive_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "ReflexiveProperty")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "Restriction")]
    fn restriction() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "Restriction")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "SymmetricProperty")]
    fn symmetric_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "SymmetricProperty")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "TransitiveProperty")]
    fn transitive_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "TransitiveProperty")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "backwardCompatibleWith")]
    fn backward_compatible_with() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "backwardCompatibleWith")).unwrap()
    }
    #[classattr]
    fn deprecated() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "deprecated")).unwrap()
    }

    #[classattr]
    #[pyo3(name = "incompatibleWith")]
    fn incompatible_with() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "incompatibleWith")).unwrap()
    }

    #[classattr]
    #[pyo3(name = "priorVersion")]
    fn prior_version() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "priorVersion")).unwrap()
    }

    #[classattr]
    #[pyo3(name = "versionInfo")]
    fn version_info() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "versionInfo")).unwrap()
    }

    #[classattr]
    #[pyo3(name = "Nothing")]
    fn nothing() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "Nothing")).unwrap()
    }

    #[classattr]
    #[pyo3(name = "Thing")]
    fn thing() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "Thing")).unwrap()
    }

    #[classattr]
    #[pyo3(name = "bottomDataProperty")]
    fn bottom_data_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "bottomDataProperty")).unwrap()
    }

    #[classattr]
    #[pyo3(name = "topDataProperty")]
    fn top_data_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "topDataProperty")).unwrap()
    }

    #[classattr]
    #[pyo3(name = "bottomObjectProperty")]
    fn bottom_object_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "bottomObjectProperty")).unwrap()
    }

    #[classattr]
    #[pyo3(name = "topObjectProperty")]
    fn top_object_property() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "topObjectProperty")).unwrap()
    }

    #[classattr]
    fn imports() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "imports")).unwrap()
    }
    #[classattr]
    #[pyo3(name = "versionIRI")]
    fn version_iri() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "versionIRI")).unwrap()
    }
    #[classattr]
    fn rational() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "rational")).unwrap()
    }
    #[classattr]
    fn real() -> PyIRI {
        PyIRI::new(format!("{}{}", OWL_PREFIX, "real")).unwrap()
    }
}

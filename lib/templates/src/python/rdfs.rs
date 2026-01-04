use oxrdf::vocab::rdfs;
use pyo3::{pyclass, pymethods};
use representation::python::PyIRI;

#[derive(Clone, Debug)]
#[pyclass(name = "rdfs")]
pub struct PyRDFS {}

#[pymethods]
impl PyRDFS {
    #[classattr]
    #[pyo3(name = "Class")]
    fn class() -> PyIRI {
        PyIRI::new(rdfs::CLASS.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn comment() -> PyIRI {
        PyIRI::new(rdfs::COMMENT.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "Container")]
    fn container() -> PyIRI {
        PyIRI::new(rdfs::CONTAINER.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "Datatype")]
    fn datatype() -> PyIRI {
        PyIRI::new(rdfs::DATATYPE.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn domain() -> PyIRI {
        PyIRI::new(rdfs::DOMAIN.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "ContainerMembershipProperty")]
    fn container_membership_property() -> PyIRI {
        PyIRI::new(rdfs::CONTAINER_MEMBERSHIP_PROPERTY.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "isDefinedBy")]
    fn is_defined_by() -> PyIRI {
        PyIRI::new(rdfs::IS_DEFINED_BY.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn label() -> PyIRI {
        PyIRI::new(rdfs::LABEL.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "Literal")]
    fn literal() -> PyIRI {
        PyIRI::new(rdfs::LITERAL.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn member() -> PyIRI {
        PyIRI::new(rdfs::MEMBER.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn range() -> PyIRI {
        PyIRI::new(rdfs::RANGE.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "seeAlso")]
    fn see_also() -> PyIRI {
        PyIRI::new(rdfs::SEE_ALSO.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "subClassOf")]
    fn sub_class_of() -> PyIRI {
        PyIRI::new(rdfs::SUB_CLASS_OF.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "subPropertyOf")]
    fn sub_property_of() -> PyIRI {
        PyIRI::new(rdfs::SUB_PROPERTY_OF.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "Resource")]
    fn resource() -> PyIRI {
        PyIRI::new(rdfs::RESOURCE.as_str().to_string()).unwrap()
    }
}

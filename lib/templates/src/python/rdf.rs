use oxrdf::vocab::rdf;
use pyo3::{pyclass, pymethods};
use representation::python::PyIRI;

#[derive(Clone, Debug)]
#[pyclass(name = "rdf")]
pub struct PyRDF {}

#[pymethods]
impl PyRDF {
    #[classattr]
    #[pyo3(name = "type")]
    fn type_() -> PyIRI {
        PyIRI::new(rdf::TYPE.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "Alt")]
    fn alt() -> PyIRI {
        PyIRI::new(rdf::ALT.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "Bag")]
    fn bag() -> PyIRI {
        PyIRI::new(rdf::BAG.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn first() -> PyIRI {
        PyIRI::new(rdf::FIRST.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "HTML")]
    fn html() -> PyIRI {
        PyIRI::new(rdf::HTML.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "langString")]
    fn lang_string() -> PyIRI {
        PyIRI::new(rdf::LANG_STRING.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "List")]
    fn list() -> PyIRI {
        PyIRI::new(rdf::LIST.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn nil() -> PyIRI {
        PyIRI::new(rdf::NIL.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn object() -> PyIRI {
        PyIRI::new(rdf::OBJECT.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn predicate() -> PyIRI {
        PyIRI::new(rdf::PREDICATE.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "Property")]
    fn property() -> PyIRI {
        PyIRI::new(rdf::PROPERTY.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn rest() -> PyIRI {
        PyIRI::new(rdf::REST.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "Seq")]
    fn seq() -> PyIRI {
        PyIRI::new(rdf::SEQ.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "Statement")]
    fn statement() -> PyIRI {
        PyIRI::new(rdf::STATEMENT.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn subject() -> PyIRI {
        PyIRI::new(rdf::SUBJECT.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn value() -> PyIRI {
        PyIRI::new(rdf::VALUE.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "XMLLiteral")]
    fn xml_literal() -> PyIRI {
        PyIRI::new(rdf::XML_LITERAL.as_str().to_string()).unwrap()
    }
}

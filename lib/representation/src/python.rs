use crate::rdf_to_polars::rdf_literal_to_polars_literal_value;
use crate::RDFNodeType;
use chrono::{NaiveDateTime, TimeDelta, TimeZone};
use chrono_tz::Tz;
use oxrdf::{IriParseError, Literal, NamedNode, Variable, VariableNameParseError};
use polars::datatypes::TimeUnit;
use polars::prelude::LiteralValue;
use pyo3::exceptions::PyException;
use pyo3::prelude::PyAnyMethods;
use pyo3::{
    create_exception, pyclass, pymethods, Bound, IntoPy, Py, PyAny, PyErr, PyObject, PyResult,
    Python,
};
use thiserror::*;

#[derive(Error, Debug)]
pub enum PyRepresentationError {
    #[error(transparent)]
    IriParseError(#[from] IriParseError),
    #[error(transparent)]
    VariableNameParseError(#[from] VariableNameParseError),
    #[error("Bad arguments: `{0}`")]
    BadArgumentError(String),
}

impl From<PyRepresentationError> for PyErr {
    fn from(err: PyRepresentationError) -> PyErr {
        match &err {
            PyRepresentationError::IriParseError(err) => {
                IriParseErrorException::new_err(format!("{}", err))
            }
            PyRepresentationError::BadArgumentError(err) => {
                BadArgumentErrorException::new_err(format!("{}", err))
            }
            PyRepresentationError::VariableNameParseError(err) => {
                VariableNameParseErrorException::new_err(format!("{}", err))
            }
        }
    }
}

create_exception!(exceptions, IriParseErrorException, PyException);
create_exception!(exceptions, BadArgumentErrorException, PyException);
create_exception!(exceptions, VariableNameParseErrorException, PyException);

#[derive(Debug, Clone)]
#[pyclass(name = "RDFType")]
pub struct PyRDFType {
    pub flat: Option<RDFNodeType>,
    pub nested: Option<Py<PyRDFType>>,
}
#[pymethods]
impl PyRDFType {
    #[staticmethod]
    fn Literal<'py>(iri: Bound<'py, PyAny>) -> PyResult<PyRDFType> {
        if let Ok(pyiri) = iri.extract::<PyIRI>() {
            Ok(PyRDFType {
                flat: Some(RDFNodeType::Literal(pyiri.iri)),
                nested: None,
            })
        } else if let Ok(s) = iri.extract::<String>() {
            Ok(PyRDFType {
                flat: Some(RDFNodeType::Literal(
                    NamedNode::new(s).map_err(PyRepresentationError::from)?,
                )),
                nested: None,
            })
        } else {
            Err(
                PyRepresentationError::BadArgumentError("Literal expected IRI or str".to_string())
                    .into(),
            )
        }
    }
    #[staticmethod]
    fn IRI() -> PyRDFType {
        PyRDFType {
            flat: Some(RDFNodeType::IRI),
            nested: None,
        }
    }

    #[staticmethod]
    fn Blank() -> PyRDFType {
        PyRDFType {
            flat: Some(RDFNodeType::BlankNode),
            nested: None,
        }
    }

    #[staticmethod]
    fn Unknown() -> PyRDFType {
        PyRDFType {
            flat: Some(RDFNodeType::None),
            nested: None,
        }
    }

    fn Nested(rdf_type: Py<PyRDFType>) -> PyRDFType {
        PyRDFType {
            flat: None,
            nested: Some(rdf_type),
        }
    }
}

impl PyRDFType {
    pub fn as_rdf_node_type(&self) -> RDFNodeType {
        if let Some(rdf_node_type) = &self.flat {
            rdf_node_type.clone()
        } else {
            todo!()
        }
    }
}

#[derive(Clone)]
#[pyclass(name = "IRI")]
pub struct PyIRI {
    pub iri: NamedNode,
}

#[pymethods]
impl PyIRI {
    #[new]
    pub fn new(iri: String) -> PyResult<Self> {
        let iri = NamedNode::new(iri).map_err(PyRepresentationError::from)?;
        Ok(PyIRI { iri })
    }
}

impl PyIRI {
    pub fn into_inner(self) -> NamedNode {
        self.iri
    }
}

#[derive(Clone, Debug)]
#[pyclass(name = "Prefix")]
pub struct PyPrefix {
    pub prefix: String,
    pub iri: NamedNode,
}

#[pymethods]
impl PyPrefix {
    #[new]
    pub fn new(prefix: String, iri: String) -> PyResult<Self> {
        let iri = NamedNode::new(iri).map_err(PyRepresentationError::from)?;
        Ok(PyPrefix { prefix, iri })
    }

    pub fn suf(&self, suffix: String) -> PyResult<PyIRI> {
        PyIRI::new(format!("{}{}", self.iri.as_str(), suffix))
    }
}

#[pyclass(name = "Variable")]
#[derive(Clone)]
pub struct PyVariable {
    pub variable: Variable,
}

#[pymethods]
impl PyVariable {
    #[new]
    pub fn new(name: String) -> PyResult<Self> {
        let variable = Variable::new(name).map_err(PyRepresentationError::from)?;
        Ok(PyVariable { variable })
    }

    #[getter]
    pub fn name(&self) -> &str {
        self.variable.as_str()
    }
}

impl PyVariable {
    pub fn into_inner(self) -> Variable {
        self.variable
    }
}

#[derive(Clone)]
#[pyclass(name = "Literal")]
pub struct PyLiteral {
    pub literal: Literal,
}

#[pymethods]
impl PyLiteral {
    #[new]
    pub fn new(value: String, data_type: Option<PyIRI>, language: Option<String>) -> Self {
        let data_type_iri = if let Some(data_type) = data_type {
            Some(data_type.iri)
        } else {
            None
        };
        let literal = if let Some(language) = language {
            Literal::new_language_tagged_literal_unchecked(value, language)
        } else if let Some(data_type_iri) = data_type_iri {
            Literal::new_typed_literal(value, data_type_iri)
        } else {
            Literal::new_simple_literal(value)
        };
        PyLiteral { literal }
    }

    pub fn to_native<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        Ok(match rdf_literal_to_polars_literal_value(&self.literal) {
            LiteralValue::Boolean(b) => b.into_py(py),
            LiteralValue::String(s) => s.into_py(py),
            LiteralValue::UInt8(u) => u.into_py(py),
            LiteralValue::UInt16(u) => u.into_py(py),
            LiteralValue::UInt32(u) => u.into_py(py),
            LiteralValue::UInt64(u) => u.into_py(py),
            LiteralValue::Int8(i) => i.into_py(py),
            LiteralValue::Int16(i) => i.into_py(py),
            LiteralValue::Int32(i) => i.into_py(py),
            LiteralValue::Int64(i) => i.into_py(py),
            LiteralValue::Float32(f) => f.into_py(py),
            LiteralValue::Float64(f) => f.into_py(py),
            LiteralValue::Date(d) => {
                todo!()
            }
            LiteralValue::DateTime(i, tu, tz) => {
                //From temporal conversion in polars
                let delta = match tu {
                    TimeUnit::Nanoseconds => TimeDelta::nanoseconds(i),
                    TimeUnit::Microseconds => TimeDelta::microseconds(i),
                    TimeUnit::Milliseconds => TimeDelta::milliseconds(i),
                };
                let dt = NaiveDateTime::UNIX_EPOCH.checked_add_signed(delta).unwrap();
                if let Some(tz) = tz {
                    let tz = tz.parse::<Tz>().unwrap();
                    let dt = tz.from_utc_datetime(&dt);
                    dt.into_py(py)
                } else {
                    dt.into_py(py)
                }
            }
            _ => todo!(),
        })
    }
}

impl PyLiteral {
    pub fn from_literal(literal: Literal) -> PyLiteral {
        PyLiteral { literal }
    }
}

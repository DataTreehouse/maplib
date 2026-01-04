use crate::debug::DebugOutputs;
use crate::query_context::Context;
use crate::rdf_to_polars::rdf_literal_to_polars_literal_value;
use crate::{BaseRDFNodeType, RDFNodeState};
use chrono::{DateTime, TimeDelta, TimeZone};
use chrono_tz::Tz;
use oxrdf::vocab::xsd;
use oxrdf::{
    BlankNode, BlankNodeIdParseError, IriParseError, Literal, NamedNode, Variable,
    VariableNameParseError,
};
use oxsdatatypes::Duration;
use polars::datatypes::{AnyValue, TimeUnit};
use polars::prelude::LiteralValue;
use pyo3::basic::CompareOp;
use pyo3::exceptions::PyException;
use pyo3::prelude::PyAnyMethods;
use pyo3::IntoPyObjectExt;
use pyo3::{
    create_exception, pyclass, pymethods, Bound, Py, PyAny, PyErr, PyObject, PyResult, Python,
};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use thiserror::*;

#[derive(Error, Debug)]
pub enum PyRepresentationError {
    #[error(transparent)]
    IriParseError(#[from] IriParseError),
    #[error(transparent)]
    BlankNodeIdParseError(#[from] BlankNodeIdParseError),
    #[error(transparent)]
    VariableNameParseError(#[from] VariableNameParseError),
    #[error("Bad arguments: `{0}`")]
    BadArgumentError(String),
}

impl From<PyRepresentationError> for PyErr {
    fn from(err: PyRepresentationError) -> PyErr {
        match &err {
            PyRepresentationError::IriParseError(err) => {
                IriParseErrorException::new_err(format!("{err}"))
            }
            PyRepresentationError::BlankNodeIdParseError(err) => {
                BlankNodeIdParseErrorException::new_err(format!("{err}"))
            }
            PyRepresentationError::BadArgumentError(err) => {
                BadArgumentErrorException::new_err(err.to_string())
            }
            PyRepresentationError::VariableNameParseError(err) => {
                VariableNameParseErrorException::new_err(format!("{err}"))
            }
        }
    }
}

create_exception!(exceptions, IriParseErrorException, PyException);
create_exception!(exceptions, BlankNodeIdParseErrorException, PyException);
create_exception!(exceptions, BadArgumentErrorException, PyException);
create_exception!(exceptions, VariableNameParseErrorException, PyException);

#[derive(Debug, Clone)]
#[pyclass(name = "RDFType")]
pub struct PyRDFType {
    pub flat: Option<RDFNodeState>,
    pub nested: Option<Py<PyRDFType>>,
}

impl Display for PyRDFType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(flat) = &self.flat {
            if flat.is_multi() {
                write!(f, "RDFType.Multi([").unwrap();
                let sorted_types = flat.get_sorted_types();
                let n_types = sorted_types.len();
                for (i, t) in sorted_types.iter().enumerate() {
                    write_base(f, *t)?;
                    if i != n_types - 1 {
                        write!(f, ", ").unwrap();
                    }
                }
                write!(f, "])")
            } else {
                write_base(f, flat.get_base_type().unwrap())
            }
        } else if let Some(nested) = &self.nested {
            write!(f, "RDFType.Nested({nested})")
        } else {
            panic!()
        }
    }
}

fn write_base(f: &mut Formatter<'_>, t: &BaseRDFNodeType) -> std::fmt::Result {
    match t {
        BaseRDFNodeType::IRI => {
            write!(f, "RDFType.IRI()")
        }
        BaseRDFNodeType::BlankNode => {
            write!(f, "RDFType.BlankNode()")
        }
        BaseRDFNodeType::Literal(l) => {
            write!(f, "RDFType.Literal(\"{}\")", l.as_str())
        }
        BaseRDFNodeType::None => {
            write!(f, "RDFType.None()")
        }
    }
}

#[pymethods]
impl PyRDFType {
    fn __repr__(&self) -> String {
        format!("{self}")
    }

    fn __richcmp__(&self, other: PyRDFType, op: CompareOp, py: Python) -> PyResult<bool> {
        if matches!(op, CompareOp::Eq) {
            if let (Some(self_flat), Some(other_flat)) = (&self.flat, &other.flat) {
                Ok(self_flat.types_equal(other_flat))
            } else if let (Some(self_nested), Some(other_nested)) = (&self.nested, &other.nested) {
                let other_nested = other_nested.extract(py)?;
                self_nested.borrow(py).__richcmp__(other_nested, op, py)
            } else {
                Ok(false)
            }
        } else {
            Err(
                PyRepresentationError::BadArgumentError(format!("Only support for ==, not {op:?}"))
                    .into(),
            )
        }
    }

    #[staticmethod]
    #[pyo3(name = "Literal")]
    fn literal(iri: Bound<'_, PyAny>) -> PyResult<PyRDFType> {
        if let Ok(pyiri) = iri.extract::<PyIRI>() {
            Ok(PyRDFType {
                flat: Some(BaseRDFNodeType::Literal(pyiri.iri).into_default_input_rdf_node_state()),
                nested: None,
            })
        } else if let Ok(s) = iri.extract::<String>() {
            Ok(PyRDFType {
                flat: Some(
                    BaseRDFNodeType::Literal(
                        NamedNode::new(s).map_err(PyRepresentationError::IriParseError)?,
                    )
                    .into_default_input_rdf_node_state(),
                ),
                nested: None,
            })
        } else {
            Err(
                PyRepresentationError::BadArgumentError("Literal expected IRI or str".to_string())
                    .into(),
            )
        }
    }
    #[classattr]
    #[pyo3(name = "IRI")]
    fn iri() -> PyRDFType {
        PyRDFType {
            flat: Some(BaseRDFNodeType::IRI.into_default_input_rdf_node_state()),
            nested: None,
        }
    }

    #[classattr]
    #[pyo3(name = "BlankNode")]
    fn blank_node() -> PyRDFType {
        PyRDFType {
            flat: Some(BaseRDFNodeType::BlankNode.into_default_input_rdf_node_state()),
            nested: None,
        }
    }

    #[classattr]
    #[pyo3(name = "Unknown")]
    fn unknown() -> PyRDFType {
        PyRDFType {
            flat: Some(BaseRDFNodeType::None.into_default_input_rdf_node_state()),
            nested: None,
        }
    }

    #[staticmethod]
    #[pyo3(name = "Nested")]
    fn nested(rdf_type: Py<PyRDFType>) -> PyRDFType {
        PyRDFType {
            flat: None,
            nested: Some(rdf_type),
        }
    }

    #[staticmethod]
    #[pyo3(name = "Multi")]
    fn multi(rdf_types: Vec<Py<PyRDFType>>, py: Python) -> PyRDFType {
        let mut mapped = HashMap::new();
        for r in rdf_types {
            let state = r.borrow(py).as_rdf_node_state();
            mapped.extend(state.map);
        }
        let rdf_node_state = RDFNodeState::from_map(mapped);
        PyRDFType {
            flat: Some(rdf_node_state),
            nested: None,
        }
    }
}

impl PyRDFType {
    pub fn as_rdf_node_state(&self) -> RDFNodeState {
        if let Some(rdf_node_state) = &self.flat {
            rdf_node_state.clone()
        } else {
            todo!()
        }
    }
}

impl From<RDFNodeState> for PyRDFType {
    fn from(rdf_node_state: RDFNodeState) -> Self {
        PyRDFType {
            flat: Some(rdf_node_state),
            nested: None,
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
        let iri = NamedNode::new(iri).map_err(PyRepresentationError::IriParseError)?;
        Ok(PyIRI { iri })
    }

    #[getter]
    fn get_iri(&self) -> &str {
        self.iri.as_str()
    }

    fn __str__(&self) -> PyResult<String> {
        Ok(self.iri.as_str().to_string())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("IRI({})", self.iri.as_str()))
    }
}

impl PyIRI {
    pub fn into_inner(self) -> NamedNode {
        self.iri
    }
}

impl From<NamedNode> for PyIRI {
    fn from(iri: NamedNode) -> Self {
        PyIRI { iri }
    }
}

#[derive(Clone, Debug)]
#[pyclass(name = "Prefix")]
pub struct PyPrefix {
    pub prefix_name: Option<String>,
    pub iri: NamedNode,
}

#[pymethods]
impl PyPrefix {
    #[new]
    #[pyo3(signature = (iri, prefix_name=None))]
    pub fn new(iri: String, prefix_name: Option<String>) -> PyResult<Self> {
        let iri = NamedNode::new(iri).map_err(PyRepresentationError::IriParseError)?;
        Ok(PyPrefix { prefix_name, iri })
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
        let variable =
            Variable::new(name).map_err(PyRepresentationError::VariableNameParseError)?;
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
    #[pyo3(signature = (value, data_type=None, language=None))]
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

    #[getter]
    pub fn value(&self) -> &str {
        self.literal.value()
    }

    #[getter]
    pub fn datatype(&self) -> PyResult<PyIRI> {
        PyIRI::new(self.literal.datatype().as_str().to_string())
    }

    #[getter]
    pub fn language(&self) -> Option<&str> {
        self.literal.language()
    }

    pub fn to_native(&self, py: Python<'_>) -> PyResult<PyObject> {
        if self.literal.datatype() == xsd::DURATION {
            let duration = Duration::from_str(self.literal.value()).unwrap();
            return PyXSDDuration { duration }.into_py_any(py);
        }

        match rdf_literal_to_polars_literal_value(&self.literal) {
            LiteralValue::Scalar(s) => {
                match s.into_value() {
                    AnyValue::Boolean(b) => b.into_py_any(py),
                    AnyValue::String(s) => s.into_py_any(py),
                    AnyValue::StringOwned(s) => s.as_str().into_py_any(py),
                    AnyValue::UInt8(u) => u.into_py_any(py),
                    AnyValue::UInt16(u) => u.into_py_any(py),
                    AnyValue::UInt32(u) => u.into_py_any(py),
                    AnyValue::UInt64(u) => u.into_py_any(py),
                    AnyValue::Int8(i) => i.into_py_any(py),
                    AnyValue::Int16(i) => i.into_py_any(py),
                    AnyValue::Int32(i) => i.into_py_any(py),
                    AnyValue::Int64(i) => i.into_py_any(py),
                    AnyValue::Float32(f) => f.into_py_any(py),
                    AnyValue::Float64(f) => f.into_py_any(py),
                    AnyValue::Date(_d) => {
                        todo!()
                    }
                    AnyValue::Datetime(i, tu, tz) => {
                        //From temporal conversion in polars
                        let delta = match tu {
                            TimeUnit::Nanoseconds => TimeDelta::nanoseconds(i),
                            TimeUnit::Microseconds => TimeDelta::microseconds(i),
                            TimeUnit::Milliseconds => TimeDelta::milliseconds(i),
                        };
                        let dt = DateTime::UNIX_EPOCH.checked_add_signed(delta).unwrap();
                        if let Some(tz) = tz {
                            let tz = tz.parse::<Tz>().unwrap();
                            let dt = tz.from_utc_datetime(&dt.naive_utc());
                            dt.into_py_any(py)
                        } else {
                            dt.into_py_any(py)
                        }
                    }
                    AnyValue::DatetimeOwned(i, tu, tz) => {
                        //From temporal conversion in polars
                        let delta = match tu {
                            TimeUnit::Nanoseconds => TimeDelta::nanoseconds(i),
                            TimeUnit::Microseconds => TimeDelta::microseconds(i),
                            TimeUnit::Milliseconds => TimeDelta::milliseconds(i),
                        };
                        let dt = DateTime::UNIX_EPOCH.checked_add_signed(delta).unwrap();
                        if let Some(tz) = &tz {
                            let tz = tz.parse::<Tz>().unwrap();
                            let dt = tz.from_utc_datetime(&dt.naive_utc());
                            dt.into_py_any(py)
                        } else {
                            dt.into_py_any(py)
                        }
                    }
                    _ => todo!(),
                }
            }
            _ => todo!(),
        }
    }
}

impl PyLiteral {
    pub fn from_literal(literal: Literal) -> PyLiteral {
        PyLiteral { literal }
    }
}

#[pyclass]
#[pyo3(name = "XSDDuration")]
pub struct PyXSDDuration {
    pub duration: Duration,
}

#[pymethods]
impl PyXSDDuration {
    #[getter]
    fn years(&self) -> i64 {
        self.duration.years()
    }

    #[getter]
    fn months(&self) -> i64 {
        self.duration.months()
    }

    #[getter]
    fn days(&self) -> i64 {
        self.duration.days()
    }

    #[getter]
    fn hours(&self) -> i64 {
        self.duration.hours()
    }

    #[getter]
    fn minutes(&self) -> i64 {
        self.duration.minutes()
    }

    #[getter]
    fn seconds(&self) -> (i64, i64) {
        let duration_seconds_string = self.duration.seconds().to_string();
        let mut split_dot = duration_seconds_string.split(".");
        let whole = split_dot.next().unwrap().parse::<i64>().unwrap();
        let fraction = if let Some(second) = split_dot.next() {
            second.parse::<i64>().unwrap()
        } else {
            0
        };
        (whole, fraction)
    }
}

#[derive(Clone)]
#[pyclass]
#[pyo3(name = "BlankNode")]
pub struct PyBlankNode {
    pub inner: BlankNode,
}

#[pymethods]
impl PyBlankNode {
    #[new]
    fn new(name: &str) -> PyResult<Self> {
        Ok(PyBlankNode {
            inner: BlankNode::new(name).map_err(PyRepresentationError::BlankNodeIdParseError)?,
        })
    }

    #[getter]
    fn name(&self) -> &str {
        self.inner.as_str()
    }
}

#[pyclass]
#[pyo3(name = "SolutionMappings")]
pub struct PySolutionMappings {
    pub mappings: Py<PyAny>,
    pub rdf_node_states: HashMap<String, RDFNodeState>,
    pub pushdown_paths: Option<Vec<Context>>,
    pub debug: Option<DebugOutputs>,
}

#[pymethods]
impl PySolutionMappings {
    #[getter]
    fn mappings(&self) -> &Py<PyAny> {
        &self.mappings
    }

    #[getter]
    fn rdf_types(&self) -> HashMap<String, PyRDFType> {
        let mut map = HashMap::new();
        for (k, v) in &self.rdf_node_states {
            map.insert(
                k.clone(),
                PyRDFType {
                    flat: Some(v.clone()),
                    nested: None,
                },
            );
        }
        map
    }

    #[getter]
    fn pushdown_paths(&self) -> Option<Vec<Vec<String>>> {
        if let Some(paths) = &self.pushdown_paths {
            let mut all_paths = vec![];
            for p in paths {
                let mut paths = vec![];
                for pe in &p.path {
                    paths.push(pe.to_string());
                }
                all_paths.push(paths);
            }
            Some(all_paths)
        } else {
            None
        }
    }

    #[getter]
    fn debug(&self) -> Option<String> {
        if let Some(debug) = &self.debug {
            Some(debug.to_string())
        } else {
            None
        }
    }
}

use crate::ast::{
    Argument, ConstantTerm, ConstantTermOrList, Instance, ListExpanderType, Parameter, Signature,
    StottrLiteral, StottrTerm, StottrVariable, Template,
};
use crate::constants::{OTTR_TRIPLE, XSD_PREFIX};
use crate::MappingColumnType;
use oxrdf::{IriParseError, NamedNode};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use representation::python::PyRDFType;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PyTemplateError {
    #[error(transparent)]
    IriParseError(#[from] IriParseError),
}

impl From<PyTemplateError> for PyErr {
    fn from(err: PyTemplateError) -> PyErr {
        match &err {
            PyTemplateError::IriParseError(err) => {
                IriParseErrorException::new_err(format!("{}", err))
            }
        }
    }
}

create_exception!(exceptions, IriParseErrorException, PyException);

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
        let iri = NamedNode::new(iri).map_err(PyTemplateError::from)?;
        Ok(PyPrefix { prefix, iri })
    }

    fn suf(&self, suffix: String) -> PyResult<PyIRI> {
        PyIRI::new(format!("{}{}", self.iri.as_str(), suffix))
    }
}

#[derive(Clone)]
#[pyclass(name = "Variable")]
pub struct PyVariable {
    variable: StottrVariable,
}

#[pymethods]
impl PyVariable {
    #[new]
    pub fn new(name: String) -> Self {
        let variable = StottrVariable { name };
        PyVariable { variable }
    }
}

impl PyVariable {
    pub fn into_inner(self) -> StottrVariable {
        self.variable
    }
}

#[derive(Clone)]
#[pyclass(name = "IRI")]
pub struct PyIRI {
    iri: NamedNode,
}

#[pymethods]
impl PyIRI {
    #[new]
    pub fn new(iri: String) -> PyResult<Self> {
        let iri = NamedNode::new(iri).map_err(PyTemplateError::from)?;
        Ok(PyIRI { iri })
    }
}

impl PyIRI {
    pub fn into_inner(self) -> NamedNode {
        self.iri
    }
}

#[derive(Clone)]
#[pyclass(name = "Literal")]
pub struct PyLiteral {
    literal: StottrLiteral,
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
        let literal = StottrLiteral {
            value,
            language,
            data_type_iri,
        };
        PyLiteral { literal }
    }
}

impl PyLiteral {
    pub fn into_inner(self) -> StottrLiteral {
        self.literal
    }
}

#[derive(Clone)]
#[pyclass(name = "Parameter")]
pub struct PyParameter {
    parameter: Parameter,
}

#[pymethods]
impl PyParameter {
    #[new]
    pub fn new<'py>(
        variable: PyVariable,
        optional: Option<bool>,
        allow_blank: Option<bool>,
        rdf_type: Option<&Bound<'py, PyAny>>,
        py: Python<'py>,
    ) -> PyResult<Self> {
        let data_type = if let Some(data_type) = rdf_type {
            if let Ok(r) = data_type.extract::<PyRDFType>() {
                Some(
                    py_rdf_type_to_mapping_column_type(&r, py)
                        .map_err(PyTemplateError::from)?
                        .as_ptype(),
                )
            } else {
                panic!("Handle error")
            }
        } else {
            None
        };
        let optional = optional.unwrap_or(false);
        let non_blank = !allow_blank.unwrap_or(true);
        let parameter = Parameter {
            optional,
            non_blank,
            ptype: data_type,
            stottr_variable: variable.into_inner(),
            default_value: None,
        };
        Ok(PyParameter { parameter })
    }
}

impl PyParameter {
    pub fn into_inner(self) -> Parameter {
        self.parameter
    }
}

#[derive(Clone)]
#[pyclass(name = "Argument")]
pub struct PyArgument {
    argument: Argument,
}

#[pymethods]
impl PyArgument {
    #[new]
    pub fn new<'py>(term: &Bound<'py, PyAny>, list_expand: Option<bool>) -> PyResult<Self> {
        let list_expand = list_expand.unwrap_or(false);
        let term = if let Ok(r) = term.extract::<PyVariable>() {
            StottrTerm::Variable(r.clone().into_inner())
        } else if let Ok(r) = term.extract::<PyIRI>() {
            StottrTerm::ConstantTerm(ConstantTermOrList::ConstantTerm(ConstantTerm::Iri(
                r.into_inner(),
            )))
        } else if let Ok(r) = term.extract::<PyLiteral>() {
            StottrTerm::ConstantTerm(ConstantTermOrList::ConstantTerm(ConstantTerm::Literal(
                r.into_inner(),
            )))
        } else if let Ok(a) = term.extract::<PyArgument>() {
            return Ok(a);
        } else {
            todo!("{:?}", term)
        };
        let argument = Argument { list_expand, term };
        Ok(PyArgument { argument })
    }
}

impl PyArgument {
    pub fn into_inner(self) -> Argument {
        self.argument
    }
}

#[derive(Clone)]
#[pyclass(name = "Instance")]
pub struct PyInstance {
    instance: Instance,
}

#[pymethods]
impl PyInstance {
    #[new]
    pub fn new<'py>(
        iri: PyIRI,
        arguments: Vec<Bound<'py, PyAny>>,
        list_expander: Option<String>,
    ) -> PyResult<Self> {
        let list_expander = if let Some(s) = list_expander {
            Some(ListExpanderType::from(&s))
        } else {
            None
        };
        let mut new_arguments = vec![];
        for a in arguments {
            new_arguments.push(PyArgument::new(&a, None)?.into_inner());
        }
        let instance = Instance {
            list_expander: list_expander,
            template_name: iri.iri,
            prefixed_template_name: None,
            argument_list: new_arguments,
        };
        Ok(PyInstance { instance })
    }
}

impl PyInstance {
    pub fn into_inner(self) -> Instance {
        self.instance
    }
}

#[derive(Clone)]
#[pyclass(name = "Template")]
pub struct PyTemplate {
    pub template: Template,
}

#[pymethods]
impl PyTemplate {
    #[new]
    pub fn new<'py>(
        iri: PyIRI,
        parameters: Vec<Bound<'py, PyParameter>>,
        instances: Vec<Bound<'py, PyInstance>>,
        prefixed_iri: Option<String>,
    ) -> PyTemplate {
        let parameters: Vec<_> = parameters
            .into_iter()
            .map(|x| x.borrow().clone().into_inner())
            .collect();
        let instances: Vec<_> = instances
            .into_iter()
            .map(|x| x.borrow().clone().into_inner())
            .collect();
        let template = Template {
            signature: Signature {
                template_name: iri.iri,
                template_prefixed_name: prefixed_iri,
                parameter_list: parameters,
                annotation_list: None,
            },
            pattern_list: instances,
        };
        PyTemplate { template }
    }

    pub fn instance<'py>(
        &self,
        arguments: Vec<Bound<'py, PyAny>>,
        list_expander: Option<String>,
    ) -> PyResult<PyInstance> {
        PyInstance::new(
            PyIRI::new(self.template.signature.template_name.as_str().to_string())?,
            arguments,
            list_expander,
        )
    }

    fn __repr__(&self) -> String {
        self.template.to_string()
    }

    fn __str__(&self) -> String {
        self.template.to_string()
    }
}

impl PyTemplate {
    pub fn into_inner(self) -> Template {
        self.template
    }
}

#[pyfunction(name = "triple")]
pub fn py_triple<'py>(
    subject: Bound<'py, PyAny>,
    predicate: Bound<'py, PyAny>,
    object: Bound<'py, PyAny>,
    list_expander: Option<String>,
) -> PyResult<PyInstance> {
    PyInstance::new(
        PyIRI::new(OTTR_TRIPLE.to_string())?,
        vec![subject, predicate, object],
        list_expander,
    )
}

pub fn py_rdf_type_to_mapping_column_type(
    py_rdf_type: &PyRDFType,
    py: Python,
) -> Result<MappingColumnType, IriParseError> {
    match py_rdf_type {
        PyRDFType::Nested { rdf_type } => Ok(MappingColumnType::Nested(Box::new(
            MappingColumnType::Flat(Py::borrow(rdf_type, py).as_rdf_node_type()?),
        ))),
        t => Ok(MappingColumnType::Flat(t.as_rdf_node_type()?)),
    }
}

#[derive(Clone, Debug)]
#[pyclass(name = "XSD")]
pub struct PyXSD {
    prefix: PyPrefix,
}

#[pymethods]
impl PyXSD {
    #[new]
    pub fn new() -> PyXSD {
        PyXSD {
            prefix: PyPrefix::new("xsd".to_string(), XSD_PREFIX.to_string()).unwrap(),
        }
    }

    #[getter]
    fn boolean(&self) -> PyIRI {
        self.prefix.suf("boolean".to_string()).unwrap()
    }

    #[getter]
    fn double(&self) -> PyIRI {
        self.prefix.suf("double".to_string()).unwrap()
    }
}

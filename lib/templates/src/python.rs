use crate::ast::{Argument, ConstantTerm, ConstantTermOrList, Instance, ListExpanderType, Parameter, PType, Signature, StottrTerm, Template};
use crate::constants::{OTTR_TRIPLE, XSD_PREFIX_IRI};
use crate::MappingColumnType;
use oxrdf::vocab::rdf;
use oxrdf::IriParseError;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use representation::python::{PyIRI, PyLiteral, PyPrefix, PyRDFType, PyVariable};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PyTemplateError {
    #[error(transparent)]
    IriParseError(#[from] IriParseError),
    #[error("Bad argument: `{0}`")]
    BadTemplateArgumentError(String),
}

impl From<PyTemplateError> for PyErr {
    fn from(err: PyTemplateError) -> PyErr {
        match &err {
            PyTemplateError::IriParseError(err) => {
                IriParseErrorException::new_err(format!("{}", err))
            }
            PyTemplateError::BadTemplateArgumentError(err) => {
                IriParseErrorException::new_err(format!("{}", err))
            }
        }
    }
}

create_exception!(exceptions, IriParseErrorException, PyException);
create_exception!(exceptions, BadTemplateArgumentErrorException, PyException);

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
            if let Ok(r) = data_type.extract::<Py<PyRDFType>>() {
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
            variable: variable.into_inner(),
            default_value: None,
        };
        Ok(PyParameter { parameter })
    }

    #[getter]
    fn get_rdf_type(&self) -> Option<PyRDFType> {
        if let Some(ptype) = &self.parameter.ptype {
            let py_rdf_type = ptype_to_py_rdf_type(ptype);
            Some(py_rdf_type)
        } else {
            None
        }
    }

    #[setter]
    fn set_rdf_type(&mut self, py: Python, rdf_type: Option<Py<PyRDFType>>) -> PyResult<()> {
        if let Some(rdf_type) = rdf_type {
            self.parameter.ptype = Some(py_rdf_type_to_mapping_column_type(&rdf_type, py)
                .map_err(PyTemplateError::from)?
                .as_ptype());
        } else {
            self.parameter.ptype = None;
        }
        Ok(())
    }

    #[getter]
    fn get_optional(&self) -> bool {
        self.parameter.optional
    }

    #[setter]
    fn set_optional(&mut self, optional:bool) {
        self.parameter.optional = optional;
    }

    #[getter]
    fn get_allow_blank(&self) -> bool {
        !self.parameter.non_blank
    }

    #[setter]
    fn set_allow_blank(&mut self, allow_blank:bool) {
        self.parameter.non_blank = !allow_blank;
    }

    #[getter]
    fn get_variable(&self) -> PyResult<PyVariable> {
        PyVariable::new(self.parameter.variable.as_str().to_string())
    }

    #[setter]
    fn set_variable(&mut self, variable:PyVariable) {
        self.parameter.variable = variable.variable;
    }
}

fn ptype_to_py_rdf_type(ptype:&PType) -> PyRDFType {
    match ptype {
        PType::Basic(b, s) => {
            PyRDFType { flat: Some(b.as_rdf_node_type()), nested: None }
        }
        PType::Lub(_) => { todo!() }
        PType::List(l) => { ptype_to_py_rdf_type(l) }
        PType::NEList(_) => { todo!() }
    }
}

impl PyParameter {
    pub fn into_inner(self) -> Parameter {
        self.parameter
    }
}

impl From<Parameter> for PyParameter {
    fn from(parameter: Parameter) -> Self {
        PyParameter { parameter }
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
                r.literal,
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

impl From<Instance> for PyInstance {
    fn from(instance: Instance) -> Self {
        PyInstance {
            instance
        }
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
        parameters: Vec<Bound<'py, PyAny>>,
        instances: Vec<Bound<'py, PyInstance>>,
        prefixed_iri: Option<String>,
        py: Python,
    ) -> PyResult<PyTemplate> {
        let mut parameter_list = vec![];
        for p in parameters {
            if let Ok(parameter) = p.extract::<PyParameter>() {
                parameter_list.push(parameter.into_inner());
            } else if let Ok(variable) = p.extract::<PyVariable>() {
                parameter_list.push(PyParameter::new(variable, None, None, None, py)?.into_inner());
            } else {
                return Err(PyTemplateError::BadTemplateArgumentError(
                    "Parameter list should be only parameters or variables".to_string(),
                )
                .into());
            }
        }
        let instances: Vec<_> = instances
            .into_iter()
            .map(|x| x.borrow().clone().into_inner())
            .collect();
        let template = Template {
            signature: Signature {
                template_name: iri.iri,
                template_prefixed_name: prefixed_iri,
                parameter_list,
                annotation_list: None,
            },
            pattern_list: instances,
        };
        Ok(PyTemplate { template })
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

    #[getter]
    fn iri(&self) -> PyIRI {
        PyIRI::from(self.template.signature.template_name.clone())
    }

    #[getter]
    fn get_instances(&self) -> Vec<PyInstance> {
        let mut instances = vec![];
        for i in &self.template.pattern_list {
            instances.push(PyInstance::from(i.clone()));
        }
        instances
    }

    #[setter]
    fn set_instances(&mut self, instances:Vec<PyInstance>) {
        let mut inner_instances = vec![];
        for i in instances {
            inner_instances.push(i.instance);
        }
        self.template.pattern_list = inner_instances;
    }

    #[getter]
    fn get_parameters(&self) -> Vec<PyParameter> {
        let mut parameters = vec![];
        for p in &self.template.signature.parameter_list {
            parameters.push(PyParameter::from(p.clone()))
        }
        parameters
    }

    #[setter]
    fn set_parameters(&mut self, parameters:Vec<PyParameter>) {
        let mut inner_parameters = vec![];
        for a in parameters {
            inner_parameters.push(a.parameter);
        }
        self.template.signature.parameter_list = inner_parameters;
    }
}

impl PyTemplate {
    pub fn into_inner(self) -> Template {
        self.template
    }
}

#[pyfunction(name = "Triple")]
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
    py_rdf_type: &Py<PyRDFType>,
    py: Python,
) -> Result<MappingColumnType, IriParseError> {
    if let Some(nested) = &py_rdf_type.borrow(py).nested {
        Ok(MappingColumnType::Nested(Box::new(
            py_rdf_type_to_mapping_column_type(nested, py)?,
        )))
    } else {
        Ok(MappingColumnType::Flat(
            py_rdf_type.borrow(py).as_rdf_node_type(),
        ))
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
            prefix: PyPrefix::new("xsd".to_string(), XSD_PREFIX_IRI.to_string()).unwrap(),
        }
    }

    #[getter]
    fn boolean(&self) -> PyIRI {
        self.prefix.suf("boolean".to_string()).unwrap()
    }

    #[getter]
    fn byte(&self) -> PyIRI {
        self.prefix.suf("byte".to_string()).unwrap()
    }

    #[getter]
    fn date(&self) -> PyIRI {
        self.prefix.suf("date".to_string()).unwrap()
    }

    #[getter]
    #[pyo3(name = "dateTime")]
    fn date_time(&self) -> PyIRI {
        self.prefix.suf("dateTime".to_string()).unwrap()
    }

    #[getter]
    #[pyo3(name = "dateTimeStamp")]
    fn date_time_stamp(&self) -> PyIRI {
        self.prefix.suf("dateTimeStamp".to_string()).unwrap()
    }

    #[getter]
    fn decimal(&self) -> PyIRI {
        self.prefix.suf("decimal".to_string()).unwrap()
    }

    #[getter]
    fn double(&self) -> PyIRI {
        self.prefix.suf("double".to_string()).unwrap()
    }

    #[getter]
    fn duration(&self) -> PyIRI {
        self.prefix.suf("duration".to_string()).unwrap()
    }

    #[getter]
    fn float(&self) -> PyIRI {
        self.prefix.suf("float".to_string()).unwrap()
    }

    #[getter]
    fn int_(&self) -> PyIRI {
        self.prefix.suf("int".to_string()).unwrap()
    }

    #[getter]
    fn integer(&self) -> PyIRI {
        self.prefix.suf("integer".to_string()).unwrap()
    }

    #[getter]
    fn language(&self) -> PyIRI {
        self.prefix.suf("language".to_string()).unwrap()
    }

    #[getter]
    fn long(&self) -> PyIRI {
        self.prefix.suf("long".to_string()).unwrap()
    }

    #[getter]
    fn short(&self) -> PyIRI {
        self.prefix.suf("short".to_string()).unwrap()
    }

    #[getter]
    fn string(&self) -> PyIRI {
        self.prefix.suf("string".to_string()).unwrap()
    }
}

#[pyfunction]
pub fn a() -> PyIRI {
    PyIRI::new(rdf::TYPE.as_str().to_string()).unwrap()
}

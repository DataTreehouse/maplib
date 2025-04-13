use crate::ast::{
    ptype_nn_to_rdf_node_type, Argument, ConstantTerm, ConstantTermOrList, DefaultValue, Instance,
    ListExpanderType, PType, Parameter, Signature, StottrTerm, Template,
};
use crate::constants::{OTTR_TRIPLE, XSD_PREFIX_IRI};
use crate::MappingColumnType;
use oxrdf::vocab::rdf;
use oxrdf::IriParseError;
use pyo3::prelude::*;
use representation::python::{
    PyBlankNode, PyIRI, PyLiteral, PyPrefix, PyRDFType, PyRepresentationError, PyVariable,
};
use representation::RDFNodeType;

#[derive(Clone)]
#[pyclass(name = "Parameter")]
pub struct PyParameter {
    parameter: Parameter,
}

#[pymethods]
impl PyParameter {
    #[new]
    #[pyo3(signature = (variable, optional=None, allow_blank=None, rdf_type=None, default_value=None))]
    pub fn new<'py>(
        variable: PyVariable,
        optional: Option<bool>,
        allow_blank: Option<bool>,
        rdf_type: Option<&Bound<'py, PyAny>>,
        default_value: Option<&Bound<'py, PyAny>>,
        py: Python<'py>,
    ) -> PyResult<Self> {
        let data_type = if let Some(data_type) = rdf_type {
            if let Ok(r) = data_type.extract::<Py<PyRDFType>>() {
                Some(
                    py_rdf_type_to_mapping_column_type(&r, py)
                        .map_err(PyRepresentationError::from)?
                        .as_ptype(),
                )
            } else {
                return Err(PyRepresentationError::BadArgumentError(
                    "rdf_type should be RDFType, e.g. RDFType.IRI()".to_string(),
                )
                .into());
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
        let mut p = PyParameter { parameter };
        p.set_default_value(default_value)?;
        Ok(p)
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
            self.parameter.ptype = Some(
                py_rdf_type_to_mapping_column_type(&rdf_type, py)
                    .map_err(PyRepresentationError::from)?
                    .as_ptype(),
            );
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
    fn set_optional(&mut self, optional: bool) {
        self.parameter.optional = optional;
    }

    #[getter]
    fn get_allow_blank(&self) -> bool {
        !self.parameter.non_blank
    }

    #[setter]
    fn set_allow_blank(&mut self, allow_blank: bool) {
        self.parameter.non_blank = !allow_blank;
    }

    #[getter]
    fn get_variable(&self) -> PyResult<PyVariable> {
        PyVariable::new(self.parameter.variable.as_str().to_string())
    }

    #[setter]
    fn set_variable(&mut self, variable: PyVariable) {
        self.parameter.variable = variable.variable;
    }

    #[getter]
    fn get_default_value(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        match &self.parameter.default_value {
            None => Ok(None),
            Some(def) => match &def.constant_term {
                ConstantTermOrList::ConstantTerm(ct) => match ct {
                    ConstantTerm::Iri(i) => {
                        Ok(Some(Py::new(py, PyIRI::from(i.clone()))?.as_any().clone()))
                    }
                    ConstantTerm::BlankNode(bl) => Ok(Some(
                        Py::new(py, PyBlankNode { inner: bl.clone() })?
                            .as_any()
                            .clone(),
                    )),
                    ConstantTerm::Literal(l) => Ok(Some(
                        Py::new(py, PyLiteral::from_literal(l.clone()))?
                            .as_any()
                            .clone(),
                    )),
                    ConstantTerm::None => Ok(None),
                },
                ConstantTermOrList::ConstantList(_) => {
                    todo!()
                }
            },
        }
    }

    #[setter]
    fn set_default_value(&mut self, default_value: Option<&Bound<PyAny>>) -> PyResult<()> {
        let default = if let Some(default) = default_value {
            if let Some(ct) = extract_constant_term(default) {
                Some(DefaultValue {
                    constant_term: ConstantTermOrList::ConstantTerm(ct),
                })
            } else {
                return Err(PyRepresentationError::BadArgumentError(
                    "default_value should be IRI, BlankNode, Literal or None".to_string(),
                )
                .into());
            }
        } else {
            None
        };
        self.parameter.default_value = default;
        Ok(())
    }
}

fn ptype_to_py_rdf_type(ptype: &PType) -> PyRDFType {
    match ptype {
        PType::Basic(b) => PyRDFType {
            flat: Some(ptype_nn_to_rdf_node_type(b.as_ref())),
            nested: None,
        },
        PType::None => PyRDFType {
            flat: Some(RDFNodeType::None),
            nested: None,
        },
        PType::Lub(_) => {
            todo!()
        }
        PType::List(l) => ptype_to_py_rdf_type(l),
        PType::NEList(_) => {
            todo!()
        }
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
    #[pyo3(signature = (term, list_expand=None))]
    pub fn new(term: &Bound<'_, PyAny>, list_expand: Option<bool>) -> PyResult<Self> {
        let list_expand = list_expand.unwrap_or(false);
        let term = if let Ok(r) = term.extract::<PyVariable>() {
            StottrTerm::Variable(r.clone().into_inner())
        } else if let Some(ct) = extract_constant_term(term) {
            StottrTerm::ConstantTerm(ConstantTermOrList::ConstantTerm(ct))
        } else if let Ok(a) = term.extract::<PyArgument>() {
            return Ok(a);
        } else {
            return Err(PyRepresentationError::BadArgumentError(
                "rdf_type should be RDFType, e.g. RDFType.IRI()".to_string(),
            )
            .into());
        };
        let argument = Argument { list_expand, term };
        Ok(PyArgument { argument })
    }

    #[getter]
    pub fn variable(&self) -> PyResult<Option<PyVariable>> {
        match &self.argument.term {
            StottrTerm::Variable(v) => Ok(Some(PyVariable::new(v.as_str().to_string())?)),
            _ => Ok(None),
        }
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
    #[pyo3(signature = (iri, arguments, list_expander=None))]
    pub fn new(
        iri: PyIRI,
        arguments: Vec<Bound<'_, PyAny>>,
        list_expander: Option<String>,
    ) -> PyResult<Self> {
        let list_expander = list_expander.map(|x| ListExpanderType::from(&x));
        let mut new_arguments = vec![];
        for a in arguments {
            new_arguments.push(PyArgument::new(&a, None)?.into_inner());
        }
        let instance = Instance {
            list_expander,
            template_name: iri.iri,
            prefixed_template_name: None,
            argument_list: new_arguments,
        };
        Ok(PyInstance { instance })
    }

    #[getter]
    fn iri(&self) -> &str {
        self.instance.template_name.as_str()
    }
}

impl PyInstance {
    pub fn into_inner(self) -> Instance {
        self.instance
    }
}

impl From<Instance> for PyInstance {
    fn from(instance: Instance) -> Self {
        PyInstance { instance }
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
    #[pyo3(signature = (iri, parameters, instances, prefixed_iri=None))]
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
                parameter_list
                    .push(PyParameter::new(variable, None, None, None, None, py)?.into_inner());
            } else {
                return Err(PyRepresentationError::BadArgumentError(
                    "parameters should be a list of Parameter or Variable".to_string(),
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

    #[pyo3(signature = (arguments, list_expander=None))]
    fn instance(
        &self,
        arguments: Vec<Bound<'_, PyAny>>,
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
    fn set_instances(&mut self, instances: Vec<PyInstance>) {
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
    fn set_parameters(&mut self, parameters: Vec<PyParameter>) {
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
#[pyo3(signature = (subject, predicate, object, list_expander=None))]
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
    #[allow(clippy::new_without_default)]
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

fn extract_constant_term(term: &Bound<PyAny>) -> Option<ConstantTerm> {
    if let Ok(b) = term.extract::<PyBlankNode>() {
        Some(ConstantTerm::BlankNode(b.inner.clone()))
    } else if let Ok(r) = term.extract::<PyIRI>() {
        Some(ConstantTerm::Iri(r.into_inner()))
    } else if let Ok(r) = term.extract::<PyLiteral>() {
        Some(ConstantTerm::Literal(r.literal))
    } else if term.is_none() {
        Some(ConstantTerm::None)
    } else {
        None
    }
}

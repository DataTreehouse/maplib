use crate::VirtualizedQuery;
use polars::prelude::AnyValue;
use pyo3::prelude::*;
use representation::python::{PyIRI, PyLiteral, PyVariable};
use spargebra::algebra::{
    AggregateExpression, AggregateFunction, Expression, Function, OrderExpression,
};
use spargebra::term::TermPattern;
use std::collections::HashMap;

#[derive(Clone)]
#[pyclass(name = "VirtualizedQuery", from_py_object)]
pub enum PyVirtualizedQuery {
    Basic {
        identifier_name: String,
        column_mapping: HashMap<String, String>,
        resource: String,
        ids: Vec<String>,
        grouping_column_name: Option<String>,
        id_grouping_tuples: Option<Vec<(String, u32)>>,
    },
    Filtered {
        filter: Py<PyExpression>,
        query: Py<PyVirtualizedQuery>,
    },
    Grouped {
        query: Py<PyVirtualizedQuery>,
        by: Vec<PyVariable>,
        aggregations: Vec<(Py<PyVariable>, Py<PyAggregateExpression>)>,
    },
    ExpressionAs {
        query: Py<PyVirtualizedQuery>,
        variable: Py<PyVariable>,
        expression: Py<PyExpression>,
    },
    InnerJoin {
        queries: Vec<Py<PyVirtualizedQuery>>,
    },
    Ordered {
        query: Py<PyVirtualizedQuery>,
        ordering: Vec<Py<PyOrderExpression>>,
    },
    Sliced {
        query: Py<PyVirtualizedQuery>,
        offset: usize,
        limit: Option<usize>,
    },
}

#[pymethods]
impl PyVirtualizedQuery {
    fn type_name(&self) -> &str {
        match self {
            PyVirtualizedQuery::Basic { .. } => "Basic",
            PyVirtualizedQuery::Filtered { .. } => "Filtered",
            PyVirtualizedQuery::Grouped { .. } => "Grouped",
            PyVirtualizedQuery::ExpressionAs { .. } => "ExpressionAs",
            PyVirtualizedQuery::InnerJoin { .. } => "InnerJoin",
            PyVirtualizedQuery::Ordered { .. } => "Ordered",
            PyVirtualizedQuery::Sliced { .. } => "Sliced",
        }
    }

    #[getter]
    fn filter(&self, py: Python) -> Option<Py<PyExpression>> {
        match self {
            PyVirtualizedQuery::Filtered { filter, .. } => Some(filter.clone_ref(py)),
            _ => None,
        }
    }

    #[getter]
    fn identifier_name(&self) -> Option<String> {
        match self {
            PyVirtualizedQuery::Basic {
                identifier_name, ..
            } => Some(identifier_name.clone()),
            _ => None,
        }
    }
    #[getter]
    fn resource(&self) -> Option<String> {
        match self {
            PyVirtualizedQuery::Basic { resource, .. } => Some(resource.clone()),
            _ => None,
        }
    }
    #[getter]
    fn ids(&self) -> Option<Vec<String>> {
        match self {
            PyVirtualizedQuery::Basic { ids, .. } => Some(ids.clone()),
            _ => None,
        }
    }
    #[getter]
    fn column_mapping(&self) -> Option<HashMap<String, String>> {
        match self {
            PyVirtualizedQuery::Basic { column_mapping, .. } => Some(column_mapping.clone()),
            _ => None,
        }
    }
    #[getter]
    fn id_grouping_tuples(&self) -> Option<Vec<(String, u32)>> {
        match self {
            PyVirtualizedQuery::Basic {
                id_grouping_tuples, ..
            } => id_grouping_tuples.clone(),
            _ => None,
        }
    }
    #[getter]
    fn grouping_column_name(&self) -> Option<String> {
        match self {
            PyVirtualizedQuery::Basic {
                grouping_column_name,
                ..
            } => grouping_column_name.clone(),
            _ => None,
        }
    }
    #[getter]
    fn query(&self, py: Python) -> Option<Py<PyVirtualizedQuery>> {
        match self {
            PyVirtualizedQuery::Filtered { query, .. }
            | PyVirtualizedQuery::ExpressionAs { query, .. }
            | PyVirtualizedQuery::Ordered { query, .. }
            | PyVirtualizedQuery::Grouped { query, .. }
            | PyVirtualizedQuery::Sliced { query, .. } => Some(query.clone_ref(py)),
            _ => None,
        }
    }

    #[getter]
    fn queries(&self) -> Option<Vec<Py<PyVirtualizedQuery>>> {
        match self {
            PyVirtualizedQuery::InnerJoin { queries, .. } => Some(queries.clone()),
            _ => None,
        }
    }

    #[getter]
    fn by(&self) -> Option<Vec<PyVariable>> {
        match self {
            PyVirtualizedQuery::Grouped { by, .. } => Some(by.clone()),
            _ => None,
        }
    }

    #[getter]
    fn ordering(&self) -> Option<Vec<Py<PyOrderExpression>>> {
        match self {
            PyVirtualizedQuery::Ordered { ordering, .. } => Some(ordering.clone()),
            _ => None,
        }
    }

    #[getter]
    fn aggregations(&self) -> Option<Vec<(Py<PyVariable>, Py<PyAggregateExpression>)>> {
        match self {
            PyVirtualizedQuery::Grouped { aggregations, .. } => Some(aggregations.clone()),
            _ => None,
        }
    }

    #[getter]
    fn variable(&self, py: Python) -> Option<Py<PyVariable>> {
        match self {
            PyVirtualizedQuery::ExpressionAs { variable, .. } => Some(variable.clone_ref(py)),
            _ => None,
        }
    }

    #[getter]
    fn expression(&self, py: Python) -> Option<Py<PyExpression>> {
        match self {
            PyVirtualizedQuery::ExpressionAs { expression, .. } => Some(expression.clone_ref(py)),
            _ => None,
        }
    }

    #[getter]
    fn limit(&self) -> Option<usize> {
        match self {
            PyVirtualizedQuery::Sliced { limit, .. } => limit.as_ref().map(|limit| *limit),
            _ => None,
        }
    }

    #[getter]
    fn offset(&self) -> Option<usize> {
        match self {
            PyVirtualizedQuery::Sliced { offset, .. } => Some(*offset),
            _ => None,
        }
    }
}

impl PyVirtualizedQuery {
    pub fn new(vq: VirtualizedQuery, py: Python) -> PyResult<PyVirtualizedQuery> {
        Ok(match vq {
            VirtualizedQuery::Basic(basic) => {
                let mut column_mapping = HashMap::new();
                for (k, v) in &basic.column_mapping {
                    if let TermPattern::Variable(v) = v {
                        column_mapping.insert(k.as_str().to_string(), v.as_str().to_string());
                    }
                }
                let id_grouping_tuples = if let Some(df) = basic.grouping_mapping {
                    let mut id_grouping_tuples = vec![];
                    let id_iter = df
                        .column(basic.identifier_variable.as_str())
                        .unwrap()
                        .as_materialized_series()
                        .iter();
                    let group_iter = df
                        .column(basic.grouping_col.as_ref().unwrap())
                        .unwrap()
                        .as_materialized_series()
                        .iter();
                    for (id, group) in id_iter.zip(group_iter) {
                        if let (AnyValue::String(id), AnyValue::UInt32(group)) = (id, group) {
                            id_grouping_tuples.push((id.to_string(), group));
                        } else {
                            panic!("Should never happen")
                        }
                    }
                    Some(id_grouping_tuples)
                } else {
                    None
                };
                PyVirtualizedQuery::Basic {
                    identifier_name: basic.identifier_variable.as_str().to_string(),
                    column_mapping,
                    resource: basic.resource.unwrap(),
                    ids: basic.ids.unwrap(),
                    grouping_column_name: basic.grouping_col,
                    id_grouping_tuples,
                }
            }
            VirtualizedQuery::Filtered(inner, expression) => PyVirtualizedQuery::Filtered {
                query: Py::new(py, PyVirtualizedQuery::new(*inner, py)?)?,
                filter: Py::new(py, PyExpression::new(&expression, py)?)?,
            },
            VirtualizedQuery::Grouped(grouped) => {
                let mut by = vec![];
                for v in &grouped.by {
                    by.push(PyVariable::new(v.as_str().to_string())?);
                }
                let mut aggregations = vec![];
                for (v, a) in &grouped.aggregations {
                    aggregations.push((
                        Py::new(py, PyVariable::new(v.as_str().to_string())?)?,
                        Py::new(py, PyAggregateExpression::new(a, py)?)?,
                    ))
                }
                PyVirtualizedQuery::Grouped {
                    query: Py::new(py, PyVirtualizedQuery::new(*grouped.vq, py)?)?,
                    by,
                    aggregations,
                }
            }
            VirtualizedQuery::ExpressionAs(query, variable, expression) => {
                let py_query = PyVirtualizedQuery::new(*query, py)?;
                let py_var = PyVariable::new(variable.as_str().to_string())?;
                let py_expression = PyExpression::new(&expression, py)?;
                PyVirtualizedQuery::ExpressionAs {
                    query: Py::new(py, py_query)?,
                    variable: Py::new(py, py_var)?,
                    expression: Py::new(py, py_expression)?,
                }
            }
            VirtualizedQuery::InnerJoin(queries, _on) => {
                let mut py_qs = vec![];
                for q in queries {
                    py_qs.push(Py::new(py, PyVirtualizedQuery::new(q, py)?)?);
                }
                PyVirtualizedQuery::InnerJoin { queries: py_qs }
            }
            VirtualizedQuery::Ordered(vq, ordering) => {
                let mut py_orderings = vec![];
                for o in ordering {
                    let p = PyOrderExpression::new(&o, py)?;
                    py_orderings.push(Py::new(py, p)?);
                }
                PyVirtualizedQuery::Ordered {
                    query: Py::new(py, PyVirtualizedQuery::new(*vq, py)?)?,
                    ordering: py_orderings,
                }
            }
            VirtualizedQuery::Sliced(vq, offset, limit) => PyVirtualizedQuery::Sliced {
                query: Py::new(py, PyVirtualizedQuery::new(*vq, py)?)?,
                limit,
                offset,
            },
        })
    }
}

#[derive(Clone, Debug)]
#[pyclass(name = "Expression", from_py_object)]
pub enum PyExpression {
    Greater {
        left: Py<PyExpression>,
        right: Py<PyExpression>,
    },
    Less {
        left: Py<PyExpression>,
        right: Py<PyExpression>,
    },
    GreaterOrEqual {
        left: Py<PyExpression>,
        right: Py<PyExpression>,
    },
    LessOrEqual {
        left: Py<PyExpression>,
        right: Py<PyExpression>,
    },
    Equal {
        left: Py<PyExpression>,
        right: Py<PyExpression>,
    },
    And {
        left: Py<PyExpression>,
        right: Py<PyExpression>,
    },
    Or {
        left: Py<PyExpression>,
        right: Py<PyExpression>,
    },
    Not {
        expression: Py<PyExpression>,
    },
    Bound {
        variable: Py<PyVariable>,
    },
    If {
        left: Py<PyExpression>,
        middle: Py<PyExpression>,
        right: Py<PyExpression>,
    },
    Variable {
        variable: Py<PyVariable>,
    },
    IRI {
        iri: Py<PyIRI>,
    },
    Literal {
        literal: Py<PyLiteral>,
    },
    FunctionCall {
        function: String,
        arguments: Vec<Py<PyExpression>>,
    },
    Divide {
        left: Py<PyExpression>,
        right: Py<PyExpression>,
    },
    Add {
        left: Py<PyExpression>,
        right: Py<PyExpression>,
    },
    Subtract {
        left: Py<PyExpression>,
        right: Py<PyExpression>,
    },
    Multiply {
        left: Py<PyExpression>,
        right: Py<PyExpression>,
    },
    In {
        expression: Py<PyExpression>,
        expressions: Vec<Py<PyExpression>>,
    },
    Coalesce {
        expressions: Vec<Py<PyExpression>>,
    },
}

#[pymethods]
impl PyExpression {
    fn expression_type(&self) -> &str {
        match self {
            PyExpression::Greater { .. } => "Greater",
            PyExpression::Less { .. } => "Less",
            PyExpression::And { .. } => "And",
            PyExpression::Variable { .. } => "Variable",
            PyExpression::IRI { .. } => "IRI",
            PyExpression::Literal { .. } => "Literal",
            PyExpression::FunctionCall { .. } => "FunctionCall",
            PyExpression::Divide { .. } => "Divide",
            PyExpression::In { .. } => "In",
            PyExpression::GreaterOrEqual { .. } => "GreaterOrEqual",
            PyExpression::LessOrEqual { .. } => "LessOrEqual",
            PyExpression::Or { .. } => "Or",
            PyExpression::Not { .. } => "Not",
            PyExpression::If { .. } => "If",
            PyExpression::Add { .. } => "Add",
            PyExpression::Subtract { .. } => "Subtract",
            PyExpression::Multiply { .. } => "Multiply",
            PyExpression::Coalesce { .. } => "Coalesce",
            PyExpression::Bound { .. } => "Bound",
            PyExpression::Equal { .. } => "Equal",
        }
    }

    #[getter]
    fn left(&self, py: Python) -> Option<Py<PyExpression>> {
        match self {
            PyExpression::Greater { left, .. }
            | PyExpression::Less { left, .. }
            | PyExpression::GreaterOrEqual { left, .. }
            | PyExpression::LessOrEqual { left, .. }
            | PyExpression::Equal { left, .. }
            | PyExpression::And { left, .. }
            | PyExpression::Or { left, .. }
            | PyExpression::Divide { left, .. }
            | PyExpression::Multiply { left, .. }
            | PyExpression::Add { left, .. }
            | PyExpression::Subtract { left, .. }
            | PyExpression::If { left, .. } => Some(left.clone_ref(py)),
            _ => None,
        }
    }

    #[getter]
    fn middle(&self, py: Python) -> Option<Py<PyExpression>> {
        match self {
            PyExpression::If { middle, .. } => Some(middle.clone_ref(py)),
            _ => None,
        }
    }

    #[getter]
    fn right(&self, py: Python) -> Option<Py<PyExpression>> {
        match self {
            PyExpression::Greater { right, .. }
            | PyExpression::Less { right, .. }
            | PyExpression::GreaterOrEqual { right, .. }
            | PyExpression::LessOrEqual { right, .. }
            | PyExpression::Equal { right, .. }
            | PyExpression::And { right, .. }
            | PyExpression::Or { right, .. }
            | PyExpression::Divide { right, .. }
            | PyExpression::Multiply { right, .. }
            | PyExpression::Add { right, .. }
            | PyExpression::Subtract { right, .. }
            | PyExpression::If { right, .. } => Some(right.clone_ref(py)),
            _ => None,
        }
    }

    #[getter]
    fn variable(&self, py: Python) -> Option<Py<PyVariable>> {
        match self {
            PyExpression::Variable { variable } | PyExpression::Bound { variable } => {
                Some(variable.clone_ref(py))
            }
            _ => None,
        }
    }

    #[getter]
    fn literal(&self, py: Python) -> Option<Py<PyLiteral>> {
        match self {
            PyExpression::Literal { literal } => Some(literal.clone_ref(py)),
            _ => None,
        }
    }

    #[getter]
    fn function(&self) -> Option<String> {
        match self {
            PyExpression::FunctionCall { function, .. } => Some(function.clone()),
            _ => None,
        }
    }

    #[getter]
    fn arguments(&self) -> Option<Vec<Py<PyExpression>>> {
        match self {
            PyExpression::FunctionCall { arguments, .. } => Some(arguments.clone()),
            _ => None,
        }
    }

    #[getter]
    fn expression(&self) -> Option<Py<PyExpression>> {
        match self {
            PyExpression::In { expression, .. } => Some(expression.clone()),
            _ => None,
        }
    }

    #[getter]
    fn expressions(&self) -> Option<Vec<Py<PyExpression>>> {
        match self {
            PyExpression::In { expressions, .. } | PyExpression::Coalesce { expressions, .. } => {
                Some(expressions.clone())
            }
            _ => None,
        }
    }
}

impl PyExpression {
    pub fn new(expression: &Expression, py: Python) -> PyResult<PyExpression> {
        Ok(match expression {
            Expression::And(left, right) => PyExpression::And {
                left: Py::new(py, PyExpression::new(left, py)?)?,
                right: Py::new(py, PyExpression::new(right, py)?)?,
            },
            Expression::Or(left, right) => PyExpression::Or {
                left: Py::new(py, PyExpression::new(left, py)?)?,
                right: Py::new(py, PyExpression::new(right, py)?)?,
            },
            Expression::Not(expression) => PyExpression::Not {
                expression: Py::new(py, PyExpression::new(expression, py)?)?,
            },
            Expression::Coalesce(expressions) => {
                let mut py_expressions = vec![];
                for e in expressions {
                    py_expressions.push(Py::new(py, PyExpression::new(e, py)?)?);
                }
                PyExpression::Coalesce {
                    expressions: py_expressions,
                }
            }
            Expression::Bound(variable) => PyExpression::Bound {
                variable: Py::new(
                    py,
                    PyVariable {
                        variable: variable.clone(),
                    },
                )?,
            },
            Expression::If(left, middle, right) => PyExpression::If {
                left: Py::new(py, PyExpression::new(left, py)?)?,
                middle: Py::new(py, PyExpression::new(middle, py)?)?,
                right: Py::new(py, PyExpression::new(right, py)?)?,
            },
            Expression::Divide(left, right) => PyExpression::Divide {
                left: Py::new(py, PyExpression::new(left, py)?)?,
                right: Py::new(py, PyExpression::new(right, py)?)?,
            },
            Expression::Multiply(left, right) => PyExpression::Multiply {
                left: Py::new(py, PyExpression::new(left, py)?)?,
                right: Py::new(py, PyExpression::new(right, py)?)?,
            },
            Expression::Subtract(left, right) => PyExpression::Subtract {
                left: Py::new(py, PyExpression::new(left, py)?)?,
                right: Py::new(py, PyExpression::new(right, py)?)?,
            },
            Expression::Add(left, right) => PyExpression::Add {
                left: Py::new(py, PyExpression::new(left, py)?)?,
                right: Py::new(py, PyExpression::new(right, py)?)?,
            },
            Expression::Less(left, right) => PyExpression::Less {
                left: Py::new(py, PyExpression::new(left, py)?)?,
                right: Py::new(py, PyExpression::new(right, py)?)?,
            },
            Expression::LessOrEqual(left, right) => PyExpression::LessOrEqual {
                left: Py::new(py, PyExpression::new(left, py)?)?,
                right: Py::new(py, PyExpression::new(right, py)?)?,
            },
            Expression::Greater(left, right) => PyExpression::Greater {
                left: Py::new(py, PyExpression::new(left, py)?)?,
                right: Py::new(py, PyExpression::new(right, py)?)?,
            },
            Expression::GreaterOrEqual(left, right) => PyExpression::GreaterOrEqual {
                left: Py::new(py, PyExpression::new(left, py)?)?,
                right: Py::new(py, PyExpression::new(right, py)?)?,
            },
            Expression::Equal(left, right) => PyExpression::Equal {
                left: Py::new(py, PyExpression::new(left, py)?)?,
                right: Py::new(py, PyExpression::new(right, py)?)?,
            },
            Expression::Variable(variable) => PyExpression::Variable {
                variable: Py::new(
                    py,
                    PyVariable {
                        variable: variable.clone(),
                    },
                )?,
            },
            Expression::Literal(l) => PyExpression::Literal {
                literal: Py::new(py, PyLiteral::from_literal(l.clone()))?,
            },
            Expression::FunctionCall(function, args) => {
                let mut py_expressions = vec![];
                for a in args {
                    py_expressions.push(Py::new(py, PyExpression::new(a, py)?)?);
                }
                PyExpression::FunctionCall {
                    function: match function {
                        Function::Custom(c) => c.as_str().to_string(),
                        n => n.to_string(),
                    },
                    arguments: py_expressions,
                }
            }
            Expression::In(expression, expressions) => {
                let mut py_expressions = vec![];
                for c in expressions {
                    py_expressions.push(Py::new(py, PyExpression::new(c, py)?)?)
                }
                PyExpression::In {
                    expression: Py::new(py, PyExpression::new(expression, py)?)?,
                    expressions: py_expressions,
                }
            }
            _ => todo!(),
        })
    }
}

#[pyclass(name = "OrderExpression", from_py_object)]
#[derive(Clone)]
pub struct PyOrderExpression {
    pub expression: Py<PyExpression>,
    pub ascending: bool,
}

impl PyOrderExpression {
    pub fn new(order_expression: &OrderExpression, py: Python) -> PyResult<Self> {
        let (e, ascending) = match order_expression {
            OrderExpression::Asc(e) => (e, true),
            OrderExpression::Desc(e) => (e, false),
        };
        Ok(PyOrderExpression {
            expression: Py::new(py, PyExpression::new(e, py)?)?,
            ascending,
        })
    }
}

#[pymethods]
impl PyOrderExpression {
    #[getter]
    fn expression(&self, py: Python) -> Py<PyExpression> {
        self.expression.clone_ref(py)
    }

    #[getter]
    fn ascending(&self) -> bool {
        self.ascending
    }
}

#[pyclass(name = "AggregateExpression", from_py_object)]
#[derive(Clone)]
pub struct PyAggregateExpression {
    pub function: AggregateFunction,
    pub expression: Option<Py<PyExpression>>,
}

impl PyAggregateExpression {
    pub fn new(
        aggregate_expression: &AggregateExpression,
        py: Python,
    ) -> PyResult<PyAggregateExpression> {
        Ok(match aggregate_expression {
            AggregateExpression::CountSolutions { .. } => PyAggregateExpression {
                function: AggregateFunction::Count,
                expression: None,
            },
            AggregateExpression::FunctionCall { name, expr, .. } => PyAggregateExpression {
                function: name.clone(),
                expression: Some(Py::new(py, PyExpression::new(expr, py)?)?),
            },
        })
    }
}

#[pymethods]
impl PyAggregateExpression {
    #[getter]
    fn name(&self) -> String {
        self.function.to_string()
    }

    #[getter]
    fn expression(&self) -> Option<Py<PyExpression>> {
        self.expression.clone()
    }

    #[getter]
    fn separator(&self) -> Option<String> {
        match &self.function {
            AggregateFunction::GroupConcat { separator } => separator.clone(),
            _ => None,
        }
    }
}

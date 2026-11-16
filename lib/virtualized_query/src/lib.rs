pub mod pushdown_setting;
pub mod python;

use polars::frame::DataFrame;
use query_processing::find_query_variables::find_all_used_variables_in_expression;
use representation::query_context::{Context, VariableInContext};
use spargebra::algebra::{AggregateExpression, Expression, OrderExpression};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern, Variable};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use templates::ast::{ConstantTerm, ConstantTermOrList, FunctionCall, StottrTerm, Template};
use templates::functions::{function_call_to_expression, FunctionLibrary};

pub const ID_VARIABLE_NAME: &str = "id";

#[derive(Debug, Clone, PartialEq)]
pub enum VirtualizedQuery {
    Basic(Box<BasicVirtualizedQuery>),
    Filtered(Box<VirtualizedQuery>, Expression),
    InnerJoin(Vec<VirtualizedQuery>, Vec<Synchronizer>),
    ExpressionAs(Box<VirtualizedQuery>, Variable, Expression),
    Grouped(GroupedVirtualizedQuery),
    Sliced(Box<VirtualizedQuery>, usize, Option<usize>),
    Ordered(Box<VirtualizedQuery>, Vec<OrderExpression>),
}

impl VirtualizedQuery {
    pub fn add_sorting_pushdown(mut self, join_cols: &Vec<String>) -> VirtualizedQuery {
        if !self.try_modify_existing_sort(join_cols) {
            let orderings = create_orderings(join_cols);
            VirtualizedQuery::Ordered(Box::new(self), orderings)
        } else {
            self
        }
    }

    pub fn try_modify_existing_sort(&mut self, join_cols: &Vec<String>) -> bool {
        match self {
            VirtualizedQuery::Basic(_) => false,
            VirtualizedQuery::Filtered(inner, _) => inner.try_modify_existing_sort(join_cols),
            VirtualizedQuery::InnerJoin(_, _) => false,
            VirtualizedQuery::ExpressionAs(inner, _, _) => {
                inner.try_modify_existing_sort(join_cols)
            }
            VirtualizedQuery::Grouped(_) => false,
            VirtualizedQuery::Sliced(inner, ..) => inner.try_modify_existing_sort(join_cols),
            VirtualizedQuery::Ordered(_, orderings) => {
                let new_orderings = create_orderings(join_cols);
                for (i, c) in new_orderings.into_iter().enumerate() {
                    orderings.insert(i, c);
                }
                true
            }
        }
    }
}

fn create_orderings(join_cols: &Vec<String>) -> Vec<OrderExpression> {
    let mut orderings = vec![];
    for c in join_cols {
        orderings.push(OrderExpression::Asc(Expression::Variable(
            Variable::new_unchecked(c),
        )));
    }
    orderings
}

#[derive(Debug, Clone, PartialEq)]
pub enum Synchronizer {
    Identity(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupedVirtualizedQuery {
    pub context: Context, //TODO: Fix this workaround properly
    pub vq: Box<VirtualizedQuery>,
    pub by: Vec<Variable>,
    pub aggregations: Vec<(Variable, AggregateExpression)>,
}

/// A query term matched to a function call in a template.
#[derive(Debug, Clone, PartialEq)]
pub struct ComputedVariable {
    /// The query term: a variable that is bound to the result, or a constant that the result
    /// must equal.
    pub target: TermPattern,
    /// The function call, over the query variables (and constants) of the template parameters.
    pub expression: Expression,
    /// Variables only introduced to get the template parameters the function uses from the
    /// virtualized database. They are removed when the function has been evaluated.
    pub helper_variables: Vec<Variable>,
}

/// The prefix of the helper variables of computed variables.
pub const FUNCTION_ARGUMENT_VARIABLE_PREFIX: &str = "_ottr_fn_arg_";

#[derive(Debug, Clone, PartialEq)]
pub struct BasicVirtualizedQuery {
    pub identifier_variable: Variable,
    pub column_mapping: HashMap<Variable, TermPattern>,
    pub expected_unknown_cols: HashSet<Variable>,
    /// Query variables matched to template variables that are not parameters, i.e. blank nodes
    /// created by the template. They have no column in the virtualized database, and are bound
    /// to a fresh blank node per row when the results are attached.
    pub blank_node_variables: HashSet<Variable>,
    /// Query variables matched to function calls in the template, e.g. the object of
    /// `ottr:Triple(?point, ex:value, (* ?value 1000))`. The virtualized database returns the
    /// template parameters the functions use, and the functions are evaluated when the results
    /// are attached.
    pub computed_variables: Vec<ComputedVariable>,
    pub resource_variable: Variable,
    pub query_source_context: Context,
    pub query_source_variable: Variable,
    pub resource: Option<String>,
    pub ids: Option<Vec<String>>,
    pub grouping_mapping: Option<DataFrame>,
    pub grouping_col: Option<String>,
}

impl BasicVirtualizedQuery {
    pub fn finish_column_mapping(
        &mut self,
        patterns: &Vec<TriplePattern>,
        template: Option<&Template>,
    ) {
        let id_var = Variable::new_unchecked(ID_VARIABLE_NAME);
        let mut queue = vec![(&self.query_source_variable, &id_var)];
        let param_vars: HashSet<_> = if let Some(template) = template {
            template
                .signature
                .parameter_list
                .iter()
                .map(|x| &x.variable)
                .collect()
        } else {
            HashSet::new()
        };
        let mut new_mappings = vec![];
        let mut function_objects: Vec<(&FunctionCall, TermPattern)> = vec![];
        let mut visited_query_vars = HashSet::new();
        while let Some((current_query_var, current_template_var)) = queue.pop() {
            if !visited_query_vars.contains(&current_query_var) {
                visited_query_vars.insert(current_query_var);
                for p in patterns {
                    match &p.predicate {
                        NamedNodePattern::NamedNode(nn) => {
                            if let TermPattern::Variable(v) = &p.subject {
                                if current_query_var == v {
                                    if let Some(template) = template {
                                        for tp in &template.pattern_list {
                                            if let StottrTerm::ConstantTerm(
                                                ConstantTermOrList::ConstantTerm(
                                                    ConstantTerm::Iri(template_nn),
                                                ),
                                            ) = &tp.argument_list.get(1).unwrap().term
                                            {
                                                if nn == template_nn {
                                                    match &tp.argument_list.first().unwrap().term {
                                                        StottrTerm::Variable(tv) => {
                                                            if tv == current_template_var {
                                                                match &tp
                                                                    .argument_list
                                                                    .get(2)
                                                                    .unwrap()
                                                                    .term
                                                                {
                                                                    StottrTerm::Variable(tobj) => {
                                                                        if param_vars.contains(tobj)
                                                                        {
                                                                            new_mappings.push((
                                                                                tobj.clone(),
                                                                                p.object.clone(),
                                                                            ));
                                                                        }
                                                                        if let TermPattern::Variable(
                                                                            obj,
                                                                        ) = &p.object
                                                                        {
                                                                            if !param_vars
                                                                                .contains(tobj)
                                                                            {
                                                                                self.blank_node_variables
                                                                                    .insert(
                                                                                        obj.clone(),
                                                                                    );
                                                                            }
                                                                            queue.push((obj, tobj));
                                                                        }
                                                                    }
                                                                    StottrTerm::FunctionCall(fc) => {
                                                                        function_objects.push((
                                                                            fc,
                                                                            p.object.clone(),
                                                                        ));
                                                                    }
                                                                    StottrTerm::ConstantTerm(_)
                                                                    | StottrTerm::List(_) => {}
                                                                }
                                                            }
                                                        }
                                                        StottrTerm::ConstantTerm(_)
                                                        | StottrTerm::List(_)
                                                        | StottrTerm::FunctionCall(_) => {}
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        if let TermPattern::Variable(v) = &p.object {
                                            self.expected_unknown_cols.insert(v.clone());
                                            queue.push((v, v));
                                        }
                                    }
                                }
                            }
                        }
                        NamedNodePattern::Variable(_) => {
                            //Need to check if subject is external.. and then do something..
                        }
                    }
                }
            }
        }
        self.column_mapping.extend(new_mappings);
        for (fc, target) in function_objects {
            self.add_computed_variable(fc, target, &param_vars);
        }
    }

    /// Binds a query term to a function call of the template. The template parameters that the
    /// function uses are taken from the query variables they are matched to, or else fetched from
    /// the virtualized database as helper variables.
    fn add_computed_variable(
        &mut self,
        function_call: &FunctionCall,
        target: TermPattern,
        param_vars: &HashSet<&Variable>,
    ) {
        if self.computed_variables.iter().any(|c| c.target == target) {
            return;
        }
        // Functions defined with def are inlined, and function calls are checked, when the
        // virtualization is added, so the translation does not fail here.
        let Ok(expression) = function_call_to_expression(function_call, &FunctionLibrary::new())
        else {
            return;
        };
        let mut used = HashSet::new();
        find_all_used_variables_in_expression(&expression, &mut used, true, true);
        let mut substitution = HashMap::new();
        let mut helper_variables = vec![];
        let mut used: Vec<_> = used.into_iter().collect();
        used.sort();
        for v in used {
            let e = if let Some(tp) = self.column_mapping.get(&v) {
                match tp {
                    TermPattern::Variable(q) => Some(Expression::Variable(q.clone())),
                    TermPattern::NamedNode(nn) => Some(Expression::NamedNode(nn.clone())),
                    TermPattern::Literal(l) => Some(Expression::Literal(l.clone())),
                    _ => None,
                }
            } else if param_vars.contains(&v) {
                let n = self
                    .column_mapping
                    .values()
                    .filter(|x| {
                        matches!(x, TermPattern::Variable(q) if q.as_str().starts_with(FUNCTION_ARGUMENT_VARIABLE_PREFIX))
                    })
                    .count();
                let helper = Variable::new_unchecked(format!(
                    "{FUNCTION_ARGUMENT_VARIABLE_PREFIX}{}_{n}",
                    self.identifier_variable.as_str()
                ));
                self.column_mapping
                    .insert(v.clone(), TermPattern::Variable(helper.clone()));
                helper_variables.push(helper.clone());
                Some(Expression::Variable(helper))
            } else {
                None
            };
            substitution.insert(v, e);
        }
        self.computed_variables.push(ComputedVariable {
            target,
            expression: substitute_variables(&expression, &substitution),
            helper_variables,
        });
    }
}

/// Replaces the variables of an expression. Variables mapped to None are unbound.
fn substitute_variables(
    expression: &Expression,
    substitution: &HashMap<Variable, Option<Expression>>,
) -> Expression {
    let s = |e: &Expression| Box::new(substitute_variables(e, substitution));
    match expression {
        Expression::Variable(v) => match substitution.get(v) {
            Some(Some(e)) => e.clone(),
            Some(None) => Expression::Variable(Variable::new_unchecked(
                templates::functions::NONE_VARIABLE,
            )),
            None => expression.clone(),
        },
        Expression::NamedNode(_) | Expression::Literal(_) | Expression::Exists(_) => {
            expression.clone()
        }
        Expression::Bound(v) => match substitution.get(v) {
            Some(Some(Expression::Variable(q))) => Expression::Bound(q.clone()),
            Some(Some(_)) => Expression::Literal(spargebra::term::Literal::from(true)),
            Some(None) => Expression::Literal(spargebra::term::Literal::from(false)),
            None => expression.clone(),
        },
        Expression::Or(a, b) => Expression::Or(s(a), s(b)),
        Expression::And(a, b) => Expression::And(s(a), s(b)),
        Expression::Equal(a, b) => Expression::Equal(s(a), s(b)),
        Expression::SameTerm(a, b) => Expression::SameTerm(s(a), s(b)),
        Expression::Greater(a, b) => Expression::Greater(s(a), s(b)),
        Expression::GreaterOrEqual(a, b) => Expression::GreaterOrEqual(s(a), s(b)),
        Expression::Less(a, b) => Expression::Less(s(a), s(b)),
        Expression::LessOrEqual(a, b) => Expression::LessOrEqual(s(a), s(b)),
        Expression::Add(a, b) => Expression::Add(s(a), s(b)),
        Expression::Subtract(a, b) => Expression::Subtract(s(a), s(b)),
        Expression::Multiply(a, b) => Expression::Multiply(s(a), s(b)),
        Expression::Divide(a, b) => Expression::Divide(s(a), s(b)),
        Expression::In(a, l) => Expression::In(
            s(a),
            l.iter()
                .map(|x| substitute_variables(x, substitution))
                .collect(),
        ),
        Expression::UnaryPlus(a) => Expression::UnaryPlus(s(a)),
        Expression::UnaryMinus(a) => Expression::UnaryMinus(s(a)),
        Expression::Not(a) => Expression::Not(s(a)),
        Expression::If(a, b, c) => Expression::If(s(a), s(b), s(c)),
        Expression::Coalesce(l) => Expression::Coalesce(
            l.iter()
                .map(|x| substitute_variables(x, substitution))
                .collect(),
        ),
        Expression::FunctionCall(f, l) => Expression::FunctionCall(
            f.clone(),
            l.iter()
                .map(|x| substitute_variables(x, substitution))
                .collect(),
        ),
    }
}

impl BasicVirtualizedQuery {
    fn expected_columns(&self) -> HashSet<&str> {
        let mut s = HashSet::new();
        for tp in self.column_mapping.values() {
            match tp {
                TermPattern::BlankNode(b) => {
                    todo!("For the time being, replace {} with a variable", b)
                }
                TermPattern::Literal(_) | TermPattern::NamedNode(_) => {
                    //No action required
                }
                TermPattern::Variable(v) => {
                    s.insert(v.as_str());
                }
            }
        }
        if let Some(grouping_var) = &self.grouping_col {
            s.insert(grouping_var.as_str());
        } else {
            s.insert(self.identifier_variable.as_str());
        }
        s.extend(self.expected_unknown_cols.iter().map(|x| x.as_str()));
        s
    }

    pub fn get_virtualized_variables(&self) -> Vec<VariableInContext> {
        let mut virt = vec![];
        for vc in self.column_mapping.values() {
            if let TermPattern::Variable(vt) = vc {
                virt.push(VariableInContext::new(
                    vt.clone(),
                    self.query_source_context.clone(),
                ));
            }
        }
        virt
    }
}

#[derive(Debug)]
pub struct VirtualizedResultValidationError {
    missing_columns: Vec<String>,
    extra_columns: Vec<String>,
}

impl Display for VirtualizedResultValidationError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Missing columns: {}, Extra columns: {}",
            self.missing_columns.join(","),
            self.extra_columns.join(",")
        )
    }
}

impl Error for VirtualizedResultValidationError {}

//TODO: Redo these recursions in one method..
impl VirtualizedQuery {
    pub fn has_identifiers(&self) -> bool {
        match self {
            VirtualizedQuery::Basic(b) => {
                if let Some(i) = &b.ids {
                    !i.is_empty()
                } else {
                    false
                }
            }
            VirtualizedQuery::Filtered(i, _)
            | VirtualizedQuery::Sliced(i, ..)
            | VirtualizedQuery::Ordered(i, _) => i.has_identifiers(),
            VirtualizedQuery::InnerJoin(i, _) => i.iter().any(|x| x.has_identifiers()),
            VirtualizedQuery::ExpressionAs(t, _, _) => t.has_identifiers(),
            VirtualizedQuery::Grouped(g) => g.vq.has_identifiers(),
        }
    }

    pub fn has_resources(&self) -> bool {
        match self {
            VirtualizedQuery::Basic(b) => b.resource.is_some(),
            VirtualizedQuery::Filtered(i, _)
            | VirtualizedQuery::Sliced(i, ..)
            | VirtualizedQuery::Ordered(i, _) => i.has_resources(),
            VirtualizedQuery::InnerJoin(i, _) => i.iter().any(|x| x.has_resources()),
            VirtualizedQuery::ExpressionAs(t, _, _) => t.has_resources(),
            VirtualizedQuery::Grouped(g) => g.vq.has_resources(),
        }
    }

    pub fn validate(&self, df: &DataFrame) -> Result<(), VirtualizedResultValidationError> {
        let expected_columns = self.expected_columns();
        let df_columns: HashSet<&str> = df
            .get_column_names()
            .into_iter()
            .map(|x| x.as_str())
            .collect();
        if expected_columns != df_columns {
            let err = VirtualizedResultValidationError {
                missing_columns: expected_columns
                    .difference(&df_columns)
                    .map(|x| x.to_string())
                    .collect(),
                extra_columns: df_columns
                    .difference(&expected_columns)
                    .map(|x| x.to_string())
                    .collect(),
            };
            Err(err)
        } else {
            Ok(())
        }
    }

    pub fn expected_columns(&self) -> HashSet<&str> {
        match self {
            VirtualizedQuery::Basic(b) => b.expected_columns(),
            VirtualizedQuery::Filtered(inner, ..)
            | VirtualizedQuery::Sliced(inner, ..)
            | VirtualizedQuery::Ordered(inner, ..) => inner.expected_columns(),
            VirtualizedQuery::InnerJoin(inners, _synchronizers) => {
                inners.iter().fold(HashSet::new(), |mut exp, vq| {
                    exp.extend(vq.expected_columns());
                    exp
                })
            }
            VirtualizedQuery::Grouped(g) => {
                let mut expected_columns = HashSet::new();
                for (v, _) in &g.aggregations {
                    expected_columns.insert(v.as_str());
                }
                // The group by variables are the grouping column and the variables that the
                // virtualized query binds (computed or virtualized values), all in the result.
                for b in &g.by {
                    expected_columns.insert(b.as_str());
                }
                let grouping_col = self.get_groupby_columns();
                expected_columns.extend(grouping_col.iter().map(|x| x.as_str()));
                expected_columns
            }
            VirtualizedQuery::ExpressionAs(t, ..) => t.expected_columns(),
        }
    }

    pub fn get_ids(&self) -> Vec<&String> {
        match self {
            VirtualizedQuery::Basic(b) => {
                if let Some(ids) = &b.ids {
                    ids.iter().collect()
                } else {
                    vec![]
                }
            }
            VirtualizedQuery::Filtered(inner, _)
            | VirtualizedQuery::Sliced(inner, ..)
            | VirtualizedQuery::Ordered(inner, ..)
            | VirtualizedQuery::ExpressionAs(inner, ..) => inner.get_ids(),
            VirtualizedQuery::InnerJoin(inners, _) => {
                let mut ss = vec![];
                for inner in inners {
                    ss.extend(inner.get_ids())
                }
                ss
            }
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_ids(),
        }
    }

    pub fn get_virtualized_variables(&self) -> Vec<VariableInContext> {
        match self {
            VirtualizedQuery::Basic(b) => b.get_virtualized_variables(),
            VirtualizedQuery::Filtered(inner, _)
            | VirtualizedQuery::Sliced(inner, ..)
            | VirtualizedQuery::Ordered(inner, ..)
            | VirtualizedQuery::ExpressionAs(inner, ..) => inner.get_virtualized_variables(),
            VirtualizedQuery::InnerJoin(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_virtualized_variables())
                }
                vs
            }
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_virtualized_variables(),
        }
    }

    pub fn get_extend_functions(&self) -> Vec<(&Variable, &Expression)> {
        match self {
            VirtualizedQuery::Basic(_) => vec![],
            VirtualizedQuery::Filtered(inner, _)
            | VirtualizedQuery::Sliced(inner, ..)
            | VirtualizedQuery::Ordered(inner, ..) => inner.get_extend_functions(),
            VirtualizedQuery::InnerJoin(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_extend_functions())
                }
                vs
            }
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_extend_functions(),
            VirtualizedQuery::ExpressionAs(t, v, e) => {
                let mut extfuncs = t.get_extend_functions();
                extfuncs.push((v, e));
                extfuncs
            }
        }
    }

    pub fn get_identifier_variables(&self) -> Vec<&Variable> {
        match self {
            VirtualizedQuery::Basic(b) => {
                vec![&b.identifier_variable]
            }
            VirtualizedQuery::Filtered(inner, _)
            | VirtualizedQuery::Sliced(inner, ..)
            | VirtualizedQuery::Ordered(inner, ..) => inner.get_identifier_variables(),
            VirtualizedQuery::InnerJoin(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_identifier_variables())
                }
                vs
            }
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_identifier_variables(),
            VirtualizedQuery::ExpressionAs(t, ..) => t.get_identifier_variables(),
        }
    }

    /// The query variables bound to blank nodes created by the templates, see
    /// BasicVirtualizedQuery::blank_node_variables. Grouped queries have none, since the rows are
    /// aggregated in the virtualized database.
    pub fn get_blank_node_variables(&self) -> Vec<&Variable> {
        match self {
            VirtualizedQuery::Basic(b) => {
                let mut vs: Vec<_> = b.blank_node_variables.iter().collect();
                vs.sort();
                vs
            }
            VirtualizedQuery::Filtered(inner, _)
            | VirtualizedQuery::Sliced(inner, ..)
            | VirtualizedQuery::Ordered(inner, ..) => inner.get_blank_node_variables(),
            VirtualizedQuery::InnerJoin(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_blank_node_variables())
                }
                vs
            }
            VirtualizedQuery::Grouped(_) => vec![],
            VirtualizedQuery::ExpressionAs(t, ..) => t.get_blank_node_variables(),
        }
    }

    /// The query terms matched to function calls in templates, see
    /// BasicVirtualizedQuery::computed_variables. Grouped queries have none, since the rows are
    /// aggregated in the virtualized database.
    pub fn get_computed_variables(&self) -> Vec<&ComputedVariable> {
        match self {
            VirtualizedQuery::Basic(b) => b.computed_variables.iter().collect(),
            VirtualizedQuery::Filtered(inner, _)
            | VirtualizedQuery::Sliced(inner, ..)
            | VirtualizedQuery::Ordered(inner, ..) => inner.get_computed_variables(),
            VirtualizedQuery::InnerJoin(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_computed_variables())
                }
                vs
            }
            VirtualizedQuery::Grouped(_) => vec![],
            VirtualizedQuery::ExpressionAs(t, ..) => t.get_computed_variables(),
        }
    }

    pub fn get_resource_variables(&self) -> Vec<&Variable> {
        match self {
            VirtualizedQuery::Basic(b) => {
                vec![&b.resource_variable]
            }
            VirtualizedQuery::Filtered(inner, _)
            | VirtualizedQuery::Sliced(inner, ..)
            | VirtualizedQuery::Ordered(inner, ..) => inner.get_resource_variables(),
            VirtualizedQuery::InnerJoin(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_resource_variables())
                }
                vs
            }
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_resource_variables(),
            VirtualizedQuery::ExpressionAs(t, ..) => t.get_resource_variables(),
        }
    }

    pub fn get_basics(&self) -> Vec<&BasicVirtualizedQuery> {
        match self {
            VirtualizedQuery::Basic(b) => vec![b.as_ref()],
            VirtualizedQuery::Filtered(inner, _)
            | VirtualizedQuery::Sliced(inner, ..)
            | VirtualizedQuery::Ordered(inner, ..)
            | VirtualizedQuery::ExpressionAs(inner, ..) => inner.get_basics(),
            VirtualizedQuery::InnerJoin(inners, _) => {
                inners.iter().flat_map(|x| x.get_basics()).collect()
            }
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_basics(),
        }
    }

    /// The basic queries, in the same order as get_basics()
    pub fn get_basics_mut(&mut self) -> Vec<&mut BasicVirtualizedQuery> {
        match self {
            VirtualizedQuery::Basic(b) => vec![b.as_mut()],
            VirtualizedQuery::Filtered(inner, _)
            | VirtualizedQuery::Sliced(inner, ..)
            | VirtualizedQuery::Ordered(inner, ..)
            | VirtualizedQuery::ExpressionAs(inner, ..) => inner.get_basics_mut(),
            VirtualizedQuery::InnerJoin(inners, _) => {
                inners.iter_mut().flat_map(|x| x.get_basics_mut()).collect()
            }
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_basics_mut(),
        }
    }

    pub fn has_equivalent_variable(&self, variable: &Variable, context: &Context) -> bool {
        for ts in self.get_virtualized_variables() {
            if ts.equivalent(variable, context) {
                return true;
            }
        }
        false
    }
}

impl BasicVirtualizedQuery {
    pub fn new(
        query_source_context: Context,
        query_source_variable: Variable,
        identifier_variable: Variable,
        resource_variable: Variable,
    ) -> BasicVirtualizedQuery {
        BasicVirtualizedQuery {
            identifier_variable,
            column_mapping: Default::default(),
            resource_variable,
            query_source_context,
            query_source_variable,
            resource: None,
            ids: None,
            grouping_mapping: None,
            grouping_col: None,
            expected_unknown_cols: Default::default(),
            blank_node_variables: Default::default(),
            computed_variables: Default::default(),
        }
    }
}

impl VirtualizedQuery {
    pub fn get_groupby_columns(&self) -> HashSet<&String> {
        match self {
            VirtualizedQuery::Basic(b) => {
                if let Some(c) = &b.grouping_col {
                    HashSet::from([c])
                } else {
                    HashSet::new()
                }
            }
            VirtualizedQuery::Filtered(inner, _)
            | VirtualizedQuery::Sliced(inner, ..)
            | VirtualizedQuery::Ordered(inner, ..)
            | VirtualizedQuery::ExpressionAs(inner, ..) => inner.get_groupby_columns(),
            VirtualizedQuery::InnerJoin(vqs, _) => {
                let mut colnames = HashSet::new();
                for vq in vqs {
                    let new_colname = vq.get_groupby_columns();
                    colnames.extend(new_colname);
                }
                colnames
            }
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_groupby_columns(),
        }
    }

    pub fn get_groupby_mapping_df(&self) -> Option<&DataFrame> {
        match self {
            VirtualizedQuery::Basic(b) => {
                if let Some(df) = &b.grouping_mapping {
                    Some(df)
                } else {
                    None
                }
            }
            VirtualizedQuery::Filtered(vq, _)
            | VirtualizedQuery::ExpressionAs(vq, ..)
            | VirtualizedQuery::Sliced(vq, ..)
            | VirtualizedQuery::Ordered(vq, ..) => vq.get_groupby_mapping_df(),
            VirtualizedQuery::InnerJoin(vqs, _) => {
                let mut colname = None;
                for vq in vqs {
                    let new_colname = vq.get_groupby_mapping_df();
                    if new_colname.is_some() {
                        if colname.is_some() {
                            panic!("Should never happen")
                        }
                        colname = new_colname;
                    }
                }
                colname
            }
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_groupby_mapping_df(),
        }
    }

    pub fn get_virtualized_functions(&self, context: &Context) -> Vec<(&Variable, &Expression)> {
        match self {
            VirtualizedQuery::Basic(..) => {
                vec![]
            }
            VirtualizedQuery::Filtered(vq, _)
            | VirtualizedQuery::Sliced(vq, ..)
            | VirtualizedQuery::Ordered(vq, ..) => vq.get_virtualized_functions(context),
            VirtualizedQuery::InnerJoin(vqs, _) => {
                let mut out_tsfs = vec![];
                for vq in vqs {
                    out_tsfs.extend(vq.get_virtualized_functions(context))
                }
                out_tsfs
            }
            VirtualizedQuery::ExpressionAs(vq, v, e) => {
                let mut tsfs = vec![];
                let mut used_vars = HashSet::new();
                find_all_used_variables_in_expression(e, &mut used_vars, true, true);
                let mut exists_virtalized_var = false;
                let mut all_are_virtualized_var = true;
                for v in &used_vars {
                    if vq.has_equivalent_variable(v, context) {
                        exists_virtalized_var = true;
                    } else {
                        all_are_virtualized_var = false;
                        break;
                    }
                }
                if exists_virtalized_var && all_are_virtualized_var {
                    tsfs.push((v, e))
                }
                tsfs.extend(vq.get_virtualized_functions(context));
                tsfs
            }
            VirtualizedQuery::Grouped(vq, ..) => vq.vq.get_virtualized_functions(context),
        }
    }
}

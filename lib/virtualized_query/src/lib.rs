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
use templates::ast::{ConstantTerm, ConstantTermOrList, StottrTerm, Template};

pub const ID_VARIABLE_NAME: &str = "id";

#[derive(Debug, Clone, PartialEq)]
pub enum VirtualizedQuery {
    Basic(BasicVirtualizedQuery),
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

#[derive(Debug, Clone, PartialEq)]
pub struct BasicVirtualizedQuery {
    pub identifier_variable: Variable,
    pub column_mapping: HashMap<Variable, TermPattern>,
    pub resource_variable: Variable,
    pub query_source_context: Context,
    pub query_source_variable: Variable,
    pub resource: Option<String>,
    pub ids: Option<Vec<String>>,
    pub grouping_mapping: Option<DataFrame>,
    pub grouping_col: Option<String>,
}

impl BasicVirtualizedQuery {
    pub fn finish_column_mapping(&mut self, patterns: &Vec<TriplePattern>, template: &Template) {
        let param_vars: HashSet<_> = template
            .signature
            .parameter_list
            .iter()
            .map(|x| &x.variable)
            .collect();
        let mut new_mappings = vec![];
        let mut visited_query_vars = HashSet::new();
        let id_var = Variable::new_unchecked(ID_VARIABLE_NAME);
        let mut queue = vec![(&self.query_source_variable, &id_var)];
        while let Some((current_query_var, current_template_var)) = queue.pop() {
            if !visited_query_vars.contains(&current_query_var) {
                visited_query_vars.insert(current_query_var);
                for p in patterns {
                    match &p.predicate {
                        NamedNodePattern::NamedNode(nn) => {
                            if let TermPattern::Variable(v) = &p.subject {
                                if current_query_var == v {
                                    for tp in &template.pattern_list {
                                        if let StottrTerm::ConstantTerm(
                                            ConstantTermOrList::ConstantTerm(ConstantTerm::Iri(
                                                template_nn,
                                            )),
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
                                                                    if param_vars.contains(tobj) {
                                                                        new_mappings.push((
                                                                            tobj.clone(),
                                                                            p.object.clone(),
                                                                        ));
                                                                    }
                                                                    if let TermPattern::Variable(
                                                                        obj,
                                                                    ) = &p.object
                                                                    {
                                                                        queue.push((obj, tobj));
                                                                    }
                                                                }
                                                                StottrTerm::ConstantTerm(_) => {}
                                                                StottrTerm::List(_) => {}
                                                            }
                                                        }
                                                    }
                                                    StottrTerm::ConstantTerm(_) => {}
                                                    StottrTerm::List(_) => {}
                                                }
                                            }
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
            &self.missing_columns.join(","),
            &self.extra_columns.join(",")
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
                let tsfuncs = g.vq.get_virtualized_functions(&g.context);
                for b in &g.by {
                    for (v, _) in &tsfuncs {
                        if b == *v {
                            expected_columns.insert(v.as_str());
                            break;
                        }
                    }
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

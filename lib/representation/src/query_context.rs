use oxrdf::Variable;
use spargebra::algebra::{AggregateExpression, Expression};
use std::cmp::min;
use std::fmt;
use std::fmt::Formatter;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PathEntry {
    BGP,
    UnionLeftSide,
    UnionRightSide,
    JoinLeftSide,
    JoinRightSide,
    LeftJoinLeftSide,
    LeftJoinRightSide,
    LeftJoinExpression,
    MinusLeftSide,
    MinusRightSide,
    FilterInner,
    FilterExpression,
    GraphInner,
    ExtendInner,
    ExtendExpression,
    OrderByInner,
    OrderByExpression(u16),
    ProjectInner,
    DistinctInner,
    ReducedInner,
    SliceInner,
    ServiceInner,
    GroupInner,
    GroupAggregation(u16),
    IfLeft,
    IfMiddle,
    IfRight,
    OrLeft,
    OrRight,
    PathRewrite,
    AndLeft,
    AndRight,
    EqualLeft,
    EqualRight,
    SameTermLeft,
    SameTermRight,
    GreaterLeft,
    GreaterRight,
    GreaterOrEqualLeft,
    GreaterOrEqualRight,
    LessLeft,
    LessRight,
    LessOrEqualLeft,
    LessOrEqualRight,
    InLeft,
    InRight(u16),
    MultiplyLeft,
    MultiplyRight,
    AddLeft,
    AddRight,
    SubtractLeft,
    SubtractRight,
    DivideLeft,
    DivideRight,
    UnaryPlus,
    UnaryMinus,
    Not,
    Exists,
    Coalesce(u16),
    FunctionCall(u16),
    AggregationOperation,
    OrderingOperation,
}

impl fmt::Display for PathEntry {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            PathEntry::BGP => {
                write!(f, "BGP")
            }
            PathEntry::UnionLeftSide => {
                write!(f, "UnionLeftSide")
            }
            PathEntry::UnionRightSide => {
                write!(f, "UnionRightSide")
            }
            PathEntry::JoinLeftSide => {
                write!(f, "JoinLeftSide")
            }
            PathEntry::JoinRightSide => {
                write!(f, "JoinRightSide")
            }
            PathEntry::LeftJoinLeftSide => {
                write!(f, "LeftJoinLeftSide")
            }
            PathEntry::LeftJoinRightSide => {
                write!(f, "LeftJoinRightSide")
            }
            PathEntry::LeftJoinExpression => {
                write!(f, "LeftJoinExpression")
            }
            PathEntry::MinusLeftSide => {
                write!(f, "MinusLeftSide")
            }
            PathEntry::MinusRightSide => {
                write!(f, "MinusRightSide")
            }
            PathEntry::FilterInner => {
                write!(f, "FilterInner")
            }
            PathEntry::FilterExpression => {
                write!(f, "FilterExpression")
            }
            PathEntry::GraphInner => {
                write!(f, "GraphInner")
            }
            PathEntry::ExtendInner => {
                write!(f, "ExtendInner")
            }
            PathEntry::ExtendExpression => {
                write!(f, "ExtendExpression")
            }
            PathEntry::OrderByInner => {
                write!(f, "OrderByInner")
            }
            PathEntry::OrderByExpression(i) => {
                write!(f, "OrderByExpression({})", i)
            }
            PathEntry::ProjectInner => {
                write!(f, "ProjectInner")
            }
            PathEntry::DistinctInner => {
                write!(f, "DistinctInner")
            }
            PathEntry::ReducedInner => {
                write!(f, "ReducedInner")
            }
            PathEntry::SliceInner => {
                write!(f, "SliceInner")
            }
            PathEntry::ServiceInner => {
                write!(f, "ServiceInner")
            }
            PathEntry::GroupInner => {
                write!(f, "GroupInner")
            }
            PathEntry::GroupAggregation(i) => {
                write!(f, "GroupAggregation({})", i)
            }
            PathEntry::IfLeft => {
                write!(f, "IfLeft")
            }
            PathEntry::IfMiddle => {
                write!(f, "IfMiddle")
            }
            PathEntry::IfRight => {
                write!(f, "IfRight")
            }
            PathEntry::OrLeft => {
                write!(f, "OrLeft")
            }
            PathEntry::OrRight => {
                write!(f, "OrRight")
            }
            PathEntry::AndLeft => {
                write!(f, "AndLeft")
            }
            PathEntry::AndRight => {
                write!(f, "AndRight")
            }
            PathEntry::EqualLeft => {
                write!(f, "EqualLeft")
            }
            PathEntry::EqualRight => {
                write!(f, "EqualRight")
            }
            PathEntry::SameTermLeft => {
                write!(f, "SameTermLeft")
            }
            PathEntry::SameTermRight => {
                write!(f, "SameTermRight")
            }
            PathEntry::GreaterLeft => {
                write!(f, "GreaterLeft")
            }
            PathEntry::GreaterRight => {
                write!(f, "GreaterRight")
            }
            PathEntry::GreaterOrEqualLeft => {
                write!(f, "GreaterOrEqualLeft")
            }
            PathEntry::GreaterOrEqualRight => {
                write!(f, "GreaterOrEqualRight")
            }
            PathEntry::LessLeft => {
                write!(f, "LessLeft")
            }
            PathEntry::LessRight => {
                write!(f, "LessRight")
            }
            PathEntry::LessOrEqualLeft => {
                write!(f, "LessOrEqualLeft")
            }
            PathEntry::LessOrEqualRight => {
                write!(f, "LessOrEqualRight")
            }
            PathEntry::InLeft => {
                write!(f, "InLeft")
            }
            PathEntry::InRight(i) => {
                write!(f, "InRight({})", i)
            }
            PathEntry::MultiplyLeft => {
                write!(f, "MultiplyLeft")
            }
            PathEntry::MultiplyRight => {
                write!(f, "MultiplyRight")
            }
            PathEntry::AddLeft => {
                write!(f, "AddLeft")
            }
            PathEntry::AddRight => {
                write!(f, "AddRight")
            }
            PathEntry::SubtractLeft => {
                write!(f, "SubtractLeft")
            }
            PathEntry::SubtractRight => {
                write!(f, "SubtractRight")
            }
            PathEntry::DivideLeft => {
                write!(f, "DivideLeft")
            }
            PathEntry::DivideRight => {
                write!(f, "DivideRight")
            }
            PathEntry::UnaryPlus => {
                write!(f, "UnaryPlus")
            }
            PathEntry::UnaryMinus => {
                write!(f, "UnaryMinus")
            }
            PathEntry::Not => {
                write!(f, "Not")
            }
            PathEntry::Exists => {
                write!(f, "Exists")
            }
            PathEntry::Coalesce(i) => {
                write!(f, "Coalesce({})", i)
            }
            PathEntry::FunctionCall(i) => {
                write!(f, "FunctionCall({})", i)
            }
            PathEntry::AggregationOperation => {
                write!(f, "AggregationOperation")
            }
            PathEntry::OrderingOperation => {
                write!(f, "OrderingOperation")
            }
            PathEntry::PathRewrite => {
                write!(f, "PathRewrite")
            }
        }
    }
}

#[derive(Clone, PartialEq, Debug, Eq, Hash)]
pub struct Context {
    string_rep: String,
    pub path: Vec<PathEntry>,
}

impl Context {
    pub fn in_scope(&self, other: &Context, partial_scope: bool) -> bool {
        let min_i = min(self.path.len(), other.path.len());
        let mut self_divergence = vec![];
        let mut other_divergence = vec![];

        for i in 0..min_i {
            let other_entry = other.path.get(i).unwrap();
            let my_entry = self.path.get(i).unwrap();
            if other_entry != my_entry {
                self_divergence = self.path[i..self.path.len()].iter().collect();
                other_divergence = other.path[i..other.path.len()].iter().collect();
                break;
            }
        }

        for my_entry in self_divergence {
            if !exposes_variables(my_entry) {
                return false;
            }
        }
        if !partial_scope {
            for other_entry in other_divergence {
                if !maintains_full_downward_scope(other_entry) {
                    return false;
                }
            }
        }
        true
    }

    pub fn contains(&self, path_entry: &PathEntry) -> bool {
        self.path.contains(path_entry)
    }
}

fn exposes_variables(path_entry: &PathEntry) -> bool {
    match path_entry {
        PathEntry::BGP => true,
        PathEntry::UnionLeftSide => true,
        PathEntry::UnionRightSide => true,
        PathEntry::JoinLeftSide => true,
        PathEntry::JoinRightSide => true,
        PathEntry::LeftJoinLeftSide => true,
        PathEntry::LeftJoinRightSide => true,
        PathEntry::LeftJoinExpression => false,
        PathEntry::MinusLeftSide => true,
        PathEntry::MinusRightSide => false,
        PathEntry::FilterInner => true,
        PathEntry::FilterExpression => false,
        PathEntry::GraphInner => true,
        PathEntry::ExtendInner => true,
        PathEntry::ExtendExpression => false,
        PathEntry::OrderByInner => true,
        PathEntry::OrderByExpression(_) => false,
        PathEntry::ProjectInner => true, //TODO: Depends on projection! Extend later..
        PathEntry::DistinctInner => true,
        PathEntry::ReducedInner => true,
        PathEntry::SliceInner => true,
        PathEntry::ServiceInner => true,
        PathEntry::GroupInner => true,
        PathEntry::GroupAggregation(_) => false,
        PathEntry::IfLeft => false,
        PathEntry::IfMiddle => false,
        PathEntry::IfRight => false,
        PathEntry::OrLeft => false,
        PathEntry::OrRight => false,
        PathEntry::AndLeft => false,
        PathEntry::AndRight => false,
        PathEntry::EqualLeft => false,
        PathEntry::EqualRight => false,
        PathEntry::SameTermLeft => false,
        PathEntry::SameTermRight => false,
        PathEntry::GreaterLeft => false,
        PathEntry::GreaterRight => false,
        PathEntry::GreaterOrEqualLeft => false,
        PathEntry::GreaterOrEqualRight => false,
        PathEntry::LessLeft => false,
        PathEntry::LessRight => false,
        PathEntry::LessOrEqualLeft => false,
        PathEntry::LessOrEqualRight => false,
        PathEntry::InLeft => false,
        PathEntry::InRight(_) => false,
        PathEntry::MultiplyLeft => false,
        PathEntry::MultiplyRight => false,
        PathEntry::AddLeft => false,
        PathEntry::AddRight => false,
        PathEntry::SubtractLeft => false,
        PathEntry::SubtractRight => false,
        PathEntry::DivideLeft => false,
        PathEntry::DivideRight => false,
        PathEntry::UnaryPlus => false,
        PathEntry::UnaryMinus => false,
        PathEntry::Not => false,
        PathEntry::Exists => false,
        PathEntry::Coalesce(_) => false,
        PathEntry::FunctionCall(_) => false,
        PathEntry::AggregationOperation => false,
        PathEntry::OrderingOperation => false,
        PathEntry::PathRewrite => false,
    }
}

fn maintains_full_downward_scope(path_entry: &PathEntry) -> bool {
    match path_entry {
        PathEntry::BGP => false,
        PathEntry::UnionLeftSide => false,
        PathEntry::UnionRightSide => false,
        PathEntry::JoinLeftSide => false,
        PathEntry::JoinRightSide => false,
        PathEntry::LeftJoinLeftSide => false,
        PathEntry::LeftJoinRightSide => false,
        PathEntry::LeftJoinExpression => false,
        PathEntry::MinusLeftSide => false,
        PathEntry::MinusRightSide => false,
        PathEntry::FilterInner => false,
        PathEntry::FilterExpression => true,
        PathEntry::GraphInner => false,
        PathEntry::ExtendInner => false,
        PathEntry::ExtendExpression => true,
        PathEntry::OrderByInner => false,
        PathEntry::OrderByExpression(_) => true,
        PathEntry::ProjectInner => false,
        PathEntry::DistinctInner => false,
        PathEntry::ReducedInner => false,
        PathEntry::SliceInner => false,
        PathEntry::ServiceInner => false,
        PathEntry::GroupInner => false,
        PathEntry::GroupAggregation(_) => true,
        PathEntry::IfLeft => true,
        PathEntry::IfMiddle => true,
        PathEntry::IfRight => true,
        PathEntry::OrLeft => true,
        PathEntry::OrRight => true,
        PathEntry::AndLeft => true,
        PathEntry::AndRight => true,
        PathEntry::EqualLeft => true,
        PathEntry::EqualRight => true,
        PathEntry::SameTermLeft => true,
        PathEntry::SameTermRight => true,
        PathEntry::GreaterLeft => true,
        PathEntry::GreaterRight => true,
        PathEntry::GreaterOrEqualLeft => true,
        PathEntry::GreaterOrEqualRight => true,
        PathEntry::LessLeft => true,
        PathEntry::LessRight => true,
        PathEntry::LessOrEqualLeft => true,
        PathEntry::LessOrEqualRight => true,
        PathEntry::InLeft => true,
        PathEntry::InRight(_) => true,
        PathEntry::MultiplyLeft => true,
        PathEntry::MultiplyRight => true,
        PathEntry::AddLeft => true,
        PathEntry::AddRight => true,
        PathEntry::SubtractLeft => true,
        PathEntry::SubtractRight => true,
        PathEntry::DivideLeft => true,
        PathEntry::DivideRight => true,
        PathEntry::UnaryPlus => true,
        PathEntry::UnaryMinus => true,
        PathEntry::Not => true,
        PathEntry::Exists => true,
        PathEntry::Coalesce(_) => true,
        PathEntry::FunctionCall(_) => true,
        PathEntry::AggregationOperation => true,
        PathEntry::OrderingOperation => true,
        PathEntry::PathRewrite => false,
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl Context {
    pub fn new() -> Context {
        Context {
            string_rep: "".to_string(),
            path: vec![],
        }
    }

    pub fn from_path(path: Vec<PathEntry>) -> Context {
        let mut ctx = Context::new();
        for p in path {
            ctx = ctx.extension_with(p);
        }
        ctx
    }

    pub fn as_str(&self) -> &str {
        &self.string_rep
    }

    pub fn extension_with(&self, p: PathEntry) -> Context {
        let mut path = self.path.clone();
        let mut string_rep = self.string_rep.clone();
        if !path.is_empty() {
            string_rep += "-";
        }
        let entry_rep = p.to_string();
        string_rep += entry_rep.as_str();
        path.push(p);
        Context { path, string_rep }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct VariableInContext {
    pub variable: Variable,
    pub context: Context,
}

impl VariableInContext {
    pub fn same_name(&self, v: &Variable) -> bool {
        self.variable.as_str() == v.as_str()
    }

    pub fn in_scope(&self, context: &Context, partial_scope: bool) -> bool {
        self.context.in_scope(context, partial_scope)
    }

    pub fn equivalent(&self, variable: &Variable, context: &Context) -> bool {
        self.same_name(variable) && self.in_scope(context, false)
    }

    pub fn partial(&self, variable: &Variable, context: &Context) -> bool {
        self.same_name(variable) && self.in_scope(context, true)
    }
}

impl VariableInContext {
    pub fn new(variable: Variable, context: Context) -> VariableInContext {
        VariableInContext { variable, context }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct ExpressionInContext {
    pub expression: Expression,
    pub context: Context,
}

impl ExpressionInContext {
    pub fn new(expression: Expression, context: Context) -> ExpressionInContext {
        ExpressionInContext {
            expression,
            context,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct AggregateExpressionInContext {
    pub aggregate_expression: AggregateExpression,
    pub context: Context,
}

impl AggregateExpressionInContext {
    pub fn new(
        aggregate_expression: AggregateExpression,
        context: Context,
    ) -> AggregateExpressionInContext {
        AggregateExpressionInContext {
            aggregate_expression,
            context,
        }
    }
}

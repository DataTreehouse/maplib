// From https://github.com/DataTreehouse/chrontext - Apache 2.0 licensed
use std::fmt;
use std::fmt::Formatter;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[allow(dead_code)]
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
        }
    }
}

#[derive(Clone, PartialEq, Debug, Eq, Hash)]
pub struct Context {
    string_rep: String,
    pub path: Vec<PathEntry>,
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

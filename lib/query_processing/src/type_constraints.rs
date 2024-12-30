use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Variable};
use representation::subtypes::{is_literal_subtype, OWL_REAL};
use representation::BaseRDFNodeType;
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ConstraintExpr {
    Bottom,
    Top,
    Constraint(Box<BaseRDFNodeType>),
    And(Box<ConstraintExpr>, Box<ConstraintExpr>),
    Or(Box<ConstraintExpr>, Box<ConstraintExpr>),
}

impl ConstraintExpr {
    pub(crate) fn compatible_with(&self, t: &BaseRDFNodeType) -> bool {
        match self {
            ConstraintExpr::Bottom => false,
            ConstraintExpr::Top => true,
            ConstraintExpr::Constraint(c) => match c.as_ref() {
                BaseRDFNodeType::IRI => t == &BaseRDFNodeType::IRI,
                BaseRDFNodeType::BlankNode => t == &BaseRDFNodeType::BlankNode,
                BaseRDFNodeType::Literal(l_ctr) => {
                    if let BaseRDFNodeType::Literal(l) = t {
                        is_literal_subtype(l.as_ref(), l_ctr.as_ref())
                    } else {
                        false
                    }
                }
                BaseRDFNodeType::None => {
                    panic!("Invalid state")
                }
            },
            ConstraintExpr::And(left, right) => left.compatible_with(t) && right.compatible_with(t),
            ConstraintExpr::Or(left, right) => left.compatible_with(t) || right.compatible_with(t),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PossibleTypes {
    e: Option<ConstraintExpr>,
}

impl PossibleTypes {
    pub fn get_witness(&self) -> BaseRDFNodeType {
        match self.e.as_ref().unwrap() {
            ConstraintExpr::Bottom | ConstraintExpr::Top => BaseRDFNodeType::None,
            ConstraintExpr::Constraint(c) => c.as_ref().clone(),
            _ => BaseRDFNodeType::None, //Todo improve this..
        }
    }

    pub fn compatible_with(&self, t: &BaseRDFNodeType) -> bool {
        self.e.as_ref().unwrap().compatible_with(t)
    }

    pub fn singular(t: BaseRDFNodeType) -> Self {
        PossibleTypes::singular_ctr(ConstraintExpr::Constraint(Box::new(t)))
    }

    pub fn singular_ctr(ctr: ConstraintExpr) -> Self {
        PossibleTypes { e: Some(ctr) }
    }

    pub fn and(&mut self, t: BaseRDFNodeType) {
        self.and_ctr(ConstraintExpr::Constraint(Box::new(t)));
    }

    pub fn and_ctr(&mut self, ctr: ConstraintExpr) {
        let old_e = self.e.replace(ConstraintExpr::Bottom).unwrap();
        self.e = Some(ConstraintExpr::And(Box::new(old_e), Box::new(ctr)));
    }

    pub fn or(&mut self, t: BaseRDFNodeType) {
        let old_e = self.e.replace(ConstraintExpr::Bottom).unwrap();
        self.e = Some(ConstraintExpr::Or(
            Box::new(old_e),
            Box::new(ConstraintExpr::Constraint(Box::new(t))),
        ));
    }

    pub fn and_other(&mut self, other: PossibleTypes) {
        let old_e = self.e.replace(ConstraintExpr::Bottom).unwrap();
        self.e = Some(ConstraintExpr::And(
            Box::new(old_e),
            Box::new(other.e.unwrap()),
        ));
    }

    pub fn or_other(&mut self, other: PossibleTypes) {
        let old_e = self.e.replace(ConstraintExpr::Bottom).unwrap();
        self.e = Some(ConstraintExpr::Or(
            Box::new(old_e),
            Box::new(other.e.unwrap()),
        ));
    }
}

pub fn conjunction_variable_type(
    left: &mut HashMap<String, PossibleTypes>,
    mut right: HashMap<String, PossibleTypes>,
) -> HashMap<String, PossibleTypes> {
    let mut new_map = HashMap::new();
    for (k, mut v) in left.drain() {
        if let Some(vr) = right.remove(&k) {
            v.and_other(vr);
        } else {
            new_map.insert(k, v);
        }
    }
    new_map.extend(right);
    new_map
}

pub fn equal_variable_type(a: &Expression, b: &Expression) -> Option<(Variable, BaseRDFNodeType)> {
    if let Expression::Variable(v) = a {
        if let Some(t) = get_expression_rdf_type(b) {
            return Some((v.clone(), t));
        }
    }
    None
}

pub fn get_expression_rdf_type(e: &Expression) -> Option<BaseRDFNodeType> {
    match e {
        Expression::NamedNode(_) => Some(BaseRDFNodeType::IRI),
        Expression::Literal(l) => Some(BaseRDFNodeType::Literal(l.datatype().into_owned())),
        Expression::Or(_, _)
        | Expression::And(_, _)
        | Expression::Equal(_, _)
        | Expression::SameTerm(_, _)
        | Expression::Greater(_, _)
        | Expression::GreaterOrEqual(_, _)
        | Expression::Less(_, _)
        | Expression::LessOrEqual(_, _)
        | Expression::In(_, _)
        | Expression::Not(_)
        | Expression::Exists(_)
        | Expression::Bound(_) => Some(BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned())),
        Expression::Add(_, _)
        | Expression::Subtract(_, _)
        | Expression::Multiply(_, _)
        | Expression::Divide(_, _)
        | Expression::UnaryPlus(_)
        | Expression::UnaryMinus(_) => {
            Some(BaseRDFNodeType::Literal(NamedNode::new_unchecked(OWL_REAL)))
        }
        //Expression::If(_, _, _) => {} todo..
        //Expression::Coalesce(_) => {} todo..
        Expression::FunctionCall(f, _args) => match f {
            Function::Str | Function::StrBefore | Function::StrAfter => {
                Some(BaseRDFNodeType::Literal(xsd::STRING.into_owned()))
            }
            Function::Datatype | Function::Iri => Some(BaseRDFNodeType::IRI),
            Function::BNode => Some(BaseRDFNodeType::BlankNode),
            Function::Contains
            | Function::Regex
            | Function::LangMatches
            | Function::StrStarts
            | Function::StrEnds
            | Function::IsIri
            | Function::IsBlank
            | Function::IsLiteral
            | Function::IsNumeric => Some(BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned())),
            Function::Year
            | Function::Month
            | Function::Day
            | Function::Hours
            | Function::Minutes
            | Function::Seconds => Some(BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned())),
            Function::Custom(nn) => {
                if matches!(
                    nn.as_ref(),
                    xsd::INT
                        | xsd::LONG
                        | xsd::INTEGER
                        | xsd::BOOLEAN
                        | xsd::UNSIGNED_LONG
                        | xsd::UNSIGNED_INT
                        | xsd::UNSIGNED_SHORT
                        | xsd::UNSIGNED_BYTE
                        | xsd::DECIMAL
                        | xsd::DOUBLE
                        | xsd::FLOAT
                        | xsd::STRING
                ) {
                    Some(BaseRDFNodeType::Literal(nn.to_owned()))
                } else {
                    None
                }
            }
            _ => None,
        },
        _ => None,
    }
}

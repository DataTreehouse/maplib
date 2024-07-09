use crate::ast::{Directive, ListExpanderType};
use oxrdf::{BlankNode, NamedNode, Variable};

#[derive(PartialEq, Debug)]
pub enum UnresolvedStatement {
    Signature(UnresolvedSignature),
    Template(UnresolvedTemplate),
    BaseTemplate(UnresolvedBaseTemplate),
    Instance(UnresolvedInstance),
}

#[derive(PartialEq, Debug)]
pub struct UnresolvedTemplate {
    pub signature: UnresolvedSignature,
    pub pattern_list: Vec<UnresolvedInstance>,
}

#[derive(PartialEq, Debug)]
pub struct UnresolvedBaseTemplate {
    pub signature: UnresolvedSignature,
}

#[derive(PartialEq, Debug)]
pub struct UnresolvedSignature {
    pub template_name: ResolvesToNamedNode,
    pub parameter_list: Vec<UnresolvedParameter>,
    pub annotation_list: Option<Vec<UnresolvedAnnotation>>,
}

#[derive(PartialEq, Debug)]
pub struct UnresolvedParameter {
    pub optional: bool,
    pub non_blank: bool,
    pub ptype: Option<UnresolvedPType>,
    pub variable: Variable,
    pub default_value: Option<UnresolvedDefaultValue>,
}

#[derive(PartialEq, Debug)]
pub enum UnresolvedPType {
    Basic(ResolvesToNamedNode),
    Lub(Box<UnresolvedPType>),
    List(Box<UnresolvedPType>),
    NEList(Box<UnresolvedPType>),
}

#[derive(PartialEq, Debug)]
pub struct UnresolvedDefaultValue {
    pub constant_term: UnresolvedConstantTerm,
}

#[derive(PartialEq, Debug)]
pub enum UnresolvedConstantTerm {
    Constant(UnresolvedConstantLiteral),
    ConstantList(Vec<UnresolvedConstantTerm>),
}

#[derive(PartialEq, Debug)]
pub enum UnresolvedConstantLiteral {
    Iri(ResolvesToNamedNode),
    BlankNode(BlankNode),
    Literal(UnresolvedStottrLiteral),
    None,
}

#[derive(PartialEq, Debug)]
pub struct UnresolvedStottrLiteral {
    pub value: String,
    pub language: Option<String>,
    pub data_type_iri: Option<ResolvesToNamedNode>,
}

#[derive(PartialEq, Debug)]
pub struct PrefixedName {
    pub prefix: String,
    pub name: String,
}

#[derive(PartialEq, Debug)]
pub enum ResolvesToNamedNode {
    PrefixedName(PrefixedName),
    NamedNode(NamedNode),
}

#[derive(PartialEq, Debug)]
pub struct UnresolvedInstance {
    pub list_expander: Option<ListExpanderType>,
    pub template_name: ResolvesToNamedNode,
    pub argument_list: Vec<UnresolvedArgument>,
}

#[derive(PartialEq, Debug)]
pub struct UnresolvedAnnotation {
    pub instance: UnresolvedInstance,
}

#[derive(PartialEq, Debug)]
pub struct UnresolvedArgument {
    pub list_expand: bool,
    pub term: UnresolvedStottrTerm,
}

#[derive(PartialEq, Debug)]
pub enum UnresolvedStottrTerm {
    Variable(Variable),
    ConstantTerm(UnresolvedConstantTerm),
    List(Vec<UnresolvedStottrTerm>),
}

#[derive(PartialEq, Debug)]
pub struct UnresolvedStottrDocument {
    pub directives: Vec<Directive>,
    pub statements: Vec<UnresolvedStatement>,
}

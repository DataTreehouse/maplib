#[cfg(test)]
use crate::constants::OTTR_TRIPLE;
use crate::constants::{OTTR_BLANK_NODE, OTTR_IRI};
use oxrdf::vocab::rdfs;
#[cfg(test)]
use oxrdf::vocab::xsd;
use oxrdf::{BlankNode, Literal, NamedNode, NamedNodeRef, Variable};
use representation::RDFNodeType;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};

const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";
const OWL_NAMED_INDIVIDUAL: &str = "http://www.w3.org/2002/07/owl#NamedIndividual";
const OWL_OBJECT_PROPERTY: &str = "http://www.w3.org/2002/07/owl#ObjectProperty";
const OWL_DATATYPE_PROPERTY: &str = "http://www.w3.org/2002/07/owl#DatatypeProperty";
const OWL_ANNOTATION_PROPERTY: &str = "http://www.w3.org/2002/07/owl#AnnotationProperty";

#[derive(PartialEq, Debug, Clone)]
pub struct Prefix {
    pub name: String,
    pub iri: NamedNode,
}

#[derive(PartialEq, Debug, Clone)]
pub enum Directive {
    Prefix(Prefix),
    Base(NamedNode),
}

#[derive(PartialEq, Debug, Clone)]
pub enum Statement {
    Template(Template),
    Instance(Instance),
}

#[derive(PartialEq, Debug, Clone)]
pub struct Template {
    pub signature: Signature,
    pub pattern_list: Vec<Instance>,
}

impl Display for Template {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.signature, f)?;
        writeln!(f, " :: {{")?;
        for (idx, inst) in self.pattern_list.iter().enumerate() {
            write!(f, "  ")?;
            std::fmt::Display::fmt(inst, f)?;
            if idx + 1 != self.pattern_list.len() {
                writeln!(f, " ,")?;
            }
        }
        writeln!(f, "\n}} . ")
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Signature {
    pub template_name: NamedNode,
    pub template_prefixed_name: Option<String>,
    pub parameter_list: Vec<Parameter>,
    pub annotation_list: Option<Vec<Annotation>>,
}

impl Display for Signature {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(template_prefixed_name) = &self.template_prefixed_name {
            write!(f, "{}", template_prefixed_name)?;
        } else {
            write!(f, "{}", &self.template_name)?;
        }
        write!(f, " [")?;
        for (idx, p) in self.parameter_list.iter().enumerate() {
            std::fmt::Display::fmt(p, f)?;
            if idx + 1 != self.parameter_list.len() {
                write!(f, ", ")?;
            }
        }
        if self.annotation_list.is_some() {
            todo!();
        }
        write!(f, " ]")
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Parameter {
    pub optional: bool,
    pub non_blank: bool,
    pub ptype: Option<PType>,
    pub variable: Variable,
    pub default_value: Option<DefaultValue>,
}

impl Display for Parameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.optional {
            write!(f, "?")?;
        }
        if self.non_blank {
            write!(f, "!")?;
        }

        if let Some(ptype) = &self.ptype {
            write!(f, " ")?;
            std::fmt::Display::fmt(ptype, f)?;
            write!(f, " ")?;
        }

        std::fmt::Display::fmt(&self.variable, f)?;

        if let Some(def) = &self.default_value {
            write!(f, " = ")?;
            std::fmt::Display::fmt(def, f)?;
        }
        Ok(())
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum PType {
    None,
    Basic(NamedNode),
    Lub(Box<PType>),
    List(Box<PType>),
    NEList(Box<PType>),
}

impl PType {
    pub fn is_blank_node(&self) -> bool {
        if let PType::Basic(t) = &self {
            ptype_is_blank(t.as_ref())
        } else {
            false
        }
    }

    pub fn is_iri(&self) -> bool {
        if let PType::Basic(t) = self {
            ptype_is_iri(t.as_ref())
        } else {
            false
        }
    }
}

impl From<&RDFNodeType> for PType {
    fn from(value: &RDFNodeType) -> Self {
        match value {
            RDFNodeType::IRI => PType::Basic(NamedNode::new_unchecked(OTTR_IRI)),
            RDFNodeType::BlankNode => PType::Basic(NamedNode::new_unchecked(OTTR_BLANK_NODE)),
            RDFNodeType::Literal(l) => PType::Basic(l.to_owned()),
            RDFNodeType::None => PType::None,
            RDFNodeType::MultiType(_) => PType::Basic(rdfs::RESOURCE.into_owned()),
        }
    }
}

// Implements part of https://spec.ottr.xyz/rOTTR/0.2.0/#2.1_Basic_types
pub fn ptype_is_iri(nn: NamedNodeRef) -> bool {
    if matches!(
        nn.as_str(),
        OTTR_IRI
            | OWL_CLASS
            | OWL_NAMED_INDIVIDUAL
            | OWL_OBJECT_PROPERTY
            | OWL_DATATYPE_PROPERTY
            | OWL_ANNOTATION_PROPERTY
    ) {
        true
    } else {
        matches!(nn, rdfs::CLASS | rdfs::DATATYPE)
    }
}

pub fn ptype_is_possibly_literal(nn: NamedNodeRef) -> bool {
    !ptype_is_blank(nn) && !ptype_is_iri(nn)
}

pub fn ptype_is_blank(nn: NamedNodeRef) -> bool {
    nn.as_str() == OTTR_BLANK_NODE
}

pub fn ptype_nn_to_rdf_node_type(nn: NamedNodeRef) -> RDFNodeType {
    if ptype_is_blank(nn) {
        RDFNodeType::BlankNode
    } else if ptype_is_iri(nn) {
        RDFNodeType::IRI
    } else {
        RDFNodeType::Literal(nn.into_owned())
    }
}

impl Display for PType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PType::Basic(t) => write!(f, "{}", t),
            PType::Lub(lt) => {
                let s = lt.to_string();
                write!(f, "LUB<{}>", s)
            }
            PType::List(lt) => {
                let s = lt.to_string();
                write!(f, "List<{}>", s)
            }
            PType::NEList(lt) => {
                let s = lt.to_string();
                write!(f, "NEList<{}>", s)
            }
            PType::None => {
                write!(f, "")
            }
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct DefaultValue {
    pub constant_term: ConstantTermOrList,
}

impl Display for DefaultValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.constant_term, f)
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum ConstantTermOrList {
    ConstantTerm(ConstantTerm),
    ConstantList(Vec<ConstantTermOrList>),
}

impl ConstantTermOrList {
    pub fn has_blank_node(&self) -> bool {
        match self {
            ConstantTermOrList::ConstantTerm(c) => c.is_blank_node(),
            ConstantTermOrList::ConstantList(l) => {
                for c in l {
                    if c.has_blank_node() {
                        return true;
                    }
                }
                false
            }
        }
    }

    pub fn ptype(&self) -> PType {
        match self {
            ConstantTermOrList::ConstantTerm(ct) => ct.ptype(),
            ConstantTermOrList::ConstantList(cl) => {
                let mut inner_type = None;
                for p in cl {
                    let p_ptype = p.ptype();
                    if let Some(inner_type) = &inner_type {
                        assert_eq!(inner_type, &p_ptype);
                    } else {
                        inner_type = Some(p_ptype);
                    }
                }
                PType::List(Box::new(inner_type.unwrap_or(PType::None)))
            }
        }
    }
}

impl Display for ConstantTermOrList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConstantTermOrList::ConstantTerm(c) => std::fmt::Display::fmt(c, f),
            ConstantTermOrList::ConstantList(l) => {
                write!(f, "(")?;
                for (idx, ct) in l.iter().enumerate() {
                    std::fmt::Display::fmt(ct, f)?;
                    if idx + 1 != l.len() {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")
            }
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum ConstantTerm {
    Iri(NamedNode),
    BlankNode(BlankNode),
    Literal(Literal),
    None,
}

impl ConstantTerm {
    pub fn is_blank_node(&self) -> bool {
        matches!(self, ConstantTerm::BlankNode(_))
    }
    pub fn ptype(&self) -> PType {
        match self {
            ConstantTerm::Iri(_) => PType::Basic(NamedNode::new_unchecked(OTTR_IRI)),
            ConstantTerm::BlankNode(_) => PType::Basic(NamedNode::new_unchecked(OTTR_BLANK_NODE)),
            ConstantTerm::Literal(l) => PType::Basic(l.datatype().into_owned()),
            ConstantTerm::None => PType::None,
        }
    }
}

impl Display for ConstantTerm {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConstantTerm::Iri(i) => std::fmt::Display::fmt(i, f),
            ConstantTerm::BlankNode(bn) => std::fmt::Display::fmt(bn, f),
            ConstantTerm::Literal(lit) => std::fmt::Display::fmt(lit, f),
            ConstantTerm::None => {
                write!(f, "none")
            }
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Instance {
    pub list_expander: Option<ListExpanderType>,
    pub template_name: NamedNode,
    pub prefixed_template_name: Option<String>,
    pub argument_list: Vec<Argument>,
}

impl Display for Instance {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(le) = &self.list_expander {
            std::fmt::Display::fmt(le, f)?;
            write!(f, " | ")?;
        }
        if let Some(prefixed_template_name) = &self.prefixed_template_name {
            write!(f, "{}", prefixed_template_name)?;
        } else {
            write!(f, "{}", &self.template_name)?;
        }
        write!(f, "(")?;
        for (idx, a) in self.argument_list.iter().enumerate() {
            std::fmt::Display::fmt(a, f)?;
            if idx + 1 != self.argument_list.len() {
                write!(f, ",")?;
            }
        }
        write!(f, ")")
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Annotation {
    pub instance: Instance,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Argument {
    pub list_expand: bool,
    pub term: StottrTerm,
}

impl Display for Argument {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.list_expand {
            write!(f, "++ ")?;
        }
        std::fmt::Display::fmt(&self.term, f)
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum ListExpanderType {
    Cross,
    ZipMin,
    ZipMax,
}

impl ListExpanderType {
    pub fn from(l: &str) -> ListExpanderType {
        match l {
            "cross" => ListExpanderType::Cross,
            "zipMin" => ListExpanderType::ZipMin,
            "zipMax" => ListExpanderType::ZipMax,
            _ => panic!("Did not recognize list expander type"),
        }
    }
}

impl Display for ListExpanderType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ListExpanderType::Cross => {
                write!(f, "cross")
            }
            ListExpanderType::ZipMin => {
                write!(f, "zipMin")
            }
            ListExpanderType::ZipMax => {
                write!(f, "zipMax")
            }
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum StottrTerm {
    Variable(Variable),
    ConstantTerm(ConstantTermOrList),
    List(Vec<StottrTerm>),
}

impl Display for StottrTerm {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StottrTerm::Variable(var) => std::fmt::Display::fmt(var, f),
            StottrTerm::ConstantTerm(ct) => std::fmt::Display::fmt(ct, f),
            StottrTerm::List(li) => {
                write!(f, "(")?;
                for (idx, el) in li.iter().enumerate() {
                    std::fmt::Display::fmt(el, f)?;
                    if idx + 1 != li.len() {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")
            }
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct StottrDocument {
    pub directives: Vec<Directive>,
    pub statements: Vec<Statement>,
    pub prefix_map: HashMap<String, NamedNode>,
}

#[test]
fn test_display_easy_template() {
    let template = Template {
        signature: Signature {
            template_name: NamedNode::new("http://MYTEMPLATEURI#Templ").unwrap(),
            template_prefixed_name: Some("prefix:Templ".to_string()),
            parameter_list: vec![Parameter {
                optional: true,
                non_blank: true,
                ptype: Some(PType::Basic(xsd::DOUBLE.into_owned())),
                variable: Variable::new_unchecked("myVar"),
                default_value: Some(DefaultValue {
                    constant_term: ConstantTermOrList::ConstantTerm(ConstantTerm::Literal(
                        Literal::new_typed_literal("0.1", xsd::DOUBLE),
                    )),
                }),
            }],
            annotation_list: None,
        },
        pattern_list: vec![Instance {
            list_expander: None,
            template_name: NamedNode::new(OTTR_TRIPLE).unwrap(),
            prefixed_template_name: Some("ottr:Triple".to_string()),
            argument_list: vec![Argument {
                list_expand: false,
                term: StottrTerm::Variable(Variable::new_unchecked("myVar")),
            }],
        }],
    };

    assert_eq!(format!("{}", template), "prefix:Templ [?! <http://www.w3.org/2001/XMLSchema#double> ?myVar = \"0.1\"^^<http://www.w3.org/2001/XMLSchema#double> ] :: {\n  ottr:Triple(?myVar)\n} . \n".to_string());
}

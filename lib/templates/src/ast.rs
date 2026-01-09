use crate::dataset::TemplateDataset;
use oxrdf::vocab::rdfs;
use oxrdf::{BlankNode, Literal, NamedNode, NamedNodeRef, Variable};
use representation::constants::{OTTR_BLANK_NODE, OTTR_IRI};
use representation::prefixes::get_default_prefixes;
use representation::{BaseRDFNodeType, RDFNodeState};
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

impl Template {
    pub fn find_named_nodes(&self, nns: &mut Vec<NamedNode>) {
        self.signature.find_named_nodes(nns);
        for i in &self.pattern_list {
            i.find_named_nodes(nns);
        }
    }
}

impl Template {
    pub fn fmt_prefix(
        &self,
        f: &mut Formatter<'_>,
        prefixes: &HashMap<String, NamedNode>,
    ) -> std::fmt::Result {
        self.signature.fmt_prefix(f, prefixes)?;
        writeln!(f, " :: {{")?;
        for (idx, inst) in self.pattern_list.iter().enumerate() {
            write!(f, "  ")?;
            inst.fmt_prefixes(f, prefixes)?;
            if idx + 1 != self.pattern_list.len() {
                writeln!(f, " ,")?;
            }
        }
        writeln!(f, "\n}} . ")
    }
}

impl Display for Template {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut dataset = TemplateDataset::new_empty().unwrap();
        dataset.templates.push(self.clone());
        dataset.prefix_map = get_default_prefixes();
        write!(f, "{}", dataset)
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Signature {
    pub iri: NamedNode,
    pub parameter_list: Vec<Parameter>,
    pub annotation_list: Option<Vec<Annotation>>,
}

impl Signature {
    pub fn find_named_nodes(&self, nns: &mut Vec<NamedNode>) {
        nns.push(self.iri.clone());
        for p in &self.parameter_list {
            p.find_named_nodes(nns);
        }
    }
}

impl Signature {
    pub fn fmt_prefix(
        &self,
        f: &mut Formatter<'_>,
        prefixes: &HashMap<String, NamedNode>,
    ) -> std::fmt::Result {
        write_prefixed_namednode(f, &self.iri, prefixes)?;
        write!(f, " [")?;
        for (idx, p) in self.parameter_list.iter().enumerate() {
            write!(f, "\n    ")?;
            p.fmt_prefix(f, prefixes)?;
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

impl Display for Signature {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_prefix(f, &HashMap::new())
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Parameter {
    pub optional: bool,
    pub non_blank: bool,
    pub ptype: Option<PType>,
    pub variable: Variable,
    pub default_value: Option<ConstantTermOrList>,
}

impl Parameter {
    pub(crate) fn find_named_nodes(&self, nns: &mut Vec<NamedNode>) {
        if let Some(d) = &self.default_value {
            d.find_named_nodes(nns);
        }
        if let Some(t) = &self.ptype {
            t.find_named_nodes(nns);
        }
    }
}

impl Parameter {
    pub fn fmt_prefix(
        &self,
        f: &mut Formatter<'_>,
        prefixes: &HashMap<String, NamedNode>,
    ) -> std::fmt::Result {
        if self.optional {
            write!(f, "?")?;
        }
        if self.non_blank {
            write!(f, "!")?;
        }

        if let Some(ptype) = &self.ptype {
            write!(f, " ")?;
            ptype.fmt_prefix(f, prefixes)?;
            write!(f, " ")?;
        }

        std::fmt::Display::fmt(&self.variable, f)?;

        if let Some(def) = &self.default_value {
            write!(f, " = ")?;
            def.fmt_prefix(f, prefixes)?;
        }
        Ok(())
    }
}

#[derive(PartialEq, Debug, Clone, Eq, PartialOrd, Ord)]
pub enum PType {
    None,
    Basic(NamedNode),
    Lub(Box<PType>),
    List(Box<PType>),
    NEList(Box<PType>),
}

impl PType {
    pub(crate) fn find_named_nodes(&self, nns: &mut Vec<NamedNode>) {
        match self {
            PType::None => {}
            PType::Basic(b) => {
                nns.push(b.clone());
            }
            PType::Lub(l) | PType::List(l) | PType::NEList(l) => {
                l.find_named_nodes(nns);
            }
        }
    }
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

impl From<&RDFNodeState> for PType {
    fn from(value: &RDFNodeState) -> Self {
        if !value.is_multi() {
            let b = value.get_base_type().unwrap();
            match b {
                BaseRDFNodeType::IRI => PType::Basic(NamedNode::new_unchecked(OTTR_IRI)),
                BaseRDFNodeType::BlankNode => {
                    PType::Basic(NamedNode::new_unchecked(OTTR_BLANK_NODE))
                }
                BaseRDFNodeType::Literal(l) => PType::Basic(l.to_owned()),
                BaseRDFNodeType::None => PType::None,
            }
        } else {
            PType::Basic(rdfs::RESOURCE.into_owned())
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

pub fn ptype_nn_to_rdf_node_type(nn: NamedNodeRef) -> RDFNodeState {
    if ptype_is_blank(nn) {
        BaseRDFNodeType::BlankNode.into_default_input_rdf_node_state()
    } else if ptype_is_iri(nn) {
        BaseRDFNodeType::IRI.into_default_input_rdf_node_state()
    } else {
        BaseRDFNodeType::Literal(nn.into_owned()).into_default_input_rdf_node_state()
    }
}

impl PType {
    pub fn fmt_prefix(
        &self,
        f: &mut Formatter<'_>,
        prefixes: &HashMap<String, NamedNode>,
    ) -> std::fmt::Result {
        match self {
            PType::Basic(t) => write_prefixed_namednode(f, t, prefixes),
            PType::Lub(lt) => {
                write!(f, "LUB<")?;
                lt.fmt_prefix(f, prefixes)?;
                write!(f, ">")
            }
            PType::List(lt) => {
                write!(f, "List<")?;
                lt.fmt_prefix(f, prefixes)?;
                write!(f, ">")
            }
            PType::NEList(lt) => {
                write!(f, "NEList<")?;
                lt.fmt_prefix(f, prefixes)?;
                write!(f, ">")
            }
            PType::None => {
                write!(f, "")
            }
        }
    }
}

impl Display for PType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_prefix(f, &HashMap::new())
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum ConstantTermOrList {
    ConstantTerm(ConstantTerm),
    ConstantList(Vec<ConstantTermOrList>),
}

impl ConstantTermOrList {
    pub(crate) fn find_named_nodes(&self, nns: &mut Vec<NamedNode>) {
        match self {
            ConstantTermOrList::ConstantTerm(ct) => ct.find_named_nodes(nns),
            ConstantTermOrList::ConstantList(l) => {
                for c in l {
                    c.find_named_nodes(nns)
                }
            }
        }
    }
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

impl ConstantTermOrList {
    pub fn fmt_prefix(
        &self,
        f: &mut Formatter<'_>,
        prefixes: &HashMap<String, NamedNode>,
    ) -> std::fmt::Result {
        match self {
            ConstantTermOrList::ConstantTerm(c) => c.fmt_prefix(f, prefixes),
            ConstantTermOrList::ConstantList(l) => {
                write!(f, "(")?;
                for (idx, ct) in l.iter().enumerate() {
                    ct.fmt_prefix(f, prefixes)?;
                    if idx + 1 != l.len() {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")
            }
        }
    }
}

impl Display for ConstantTermOrList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_prefix(f, &HashMap::new())
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
    pub(crate) fn find_named_nodes(&self, nns: &mut Vec<NamedNode>) {
        if let ConstantTerm::Iri(i) = self {
            nns.push(i.clone());
        }
    }
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

impl ConstantTerm {
    pub fn fmt_prefix(
        &self,
        f: &mut Formatter<'_>,
        prefixes: &HashMap<String, NamedNode>,
    ) -> std::fmt::Result {
        match self {
            ConstantTerm::Iri(i) => write_prefixed_namednode(f, i, prefixes),
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
    pub template_iri: NamedNode,
    pub argument_list: Vec<Argument>,
}

impl Instance {
    pub(crate) fn find_named_nodes(&self, nns: &mut Vec<NamedNode>) {
        nns.push(self.template_iri.clone());
        for arg in self.argument_list.iter() {
            arg.find_named_nodes(nns);
        }
    }
}

impl Instance {
    pub fn fmt_prefixes(
        &self,
        f: &mut Formatter<'_>,
        prefixes: &HashMap<String, NamedNode>,
    ) -> std::fmt::Result {
        if let Some(le) = &self.list_expander {
            std::fmt::Display::fmt(le, f)?;
            write!(f, " | ")?;
        }
        write_prefixed_namednode(f, &self.template_iri, prefixes)?;
        write!(f, "(")?;
        for (idx, a) in self.argument_list.iter().enumerate() {
            a.fmt_prefixes(f, prefixes)?;
            if idx + 1 != self.argument_list.len() {
                write!(f, ", ")?;
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

impl Argument {
    pub fn find_named_nodes(&self, nns: &mut Vec<NamedNode>) {
        self.term.find_named_nodes(nns);
    }
}

impl Argument {
    pub fn fmt_prefixes(
        &self,
        f: &mut Formatter<'_>,
        prefixes: &HashMap<String, NamedNode>,
    ) -> std::fmt::Result {
        if self.list_expand {
            write!(f, "++ ")?;
        }
        self.term.fmt_prefixes(f, prefixes)
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

impl StottrTerm {
    pub(crate) fn find_named_nodes(&self, nns: &mut Vec<NamedNode>) {
        match self {
            StottrTerm::Variable(_) => {}
            StottrTerm::ConstantTerm(ct) => {
                ct.find_named_nodes(nns);
            }
            StottrTerm::List(l) => {
                for st in l {
                    st.find_named_nodes(nns);
                }
            }
        }
    }
}

impl StottrTerm {
    pub fn fmt_prefixes(
        &self,
        f: &mut Formatter<'_>,
        prefixes: &HashMap<String, NamedNode>,
    ) -> std::fmt::Result {
        match self {
            StottrTerm::Variable(var) => std::fmt::Display::fmt(var, f),
            StottrTerm::ConstantTerm(ct) => ct.fmt_prefix(f, prefixes),
            StottrTerm::List(li) => {
                write!(f, "(")?;
                for (idx, el) in li.iter().enumerate() {
                    el.fmt_prefixes(f, prefixes)?;
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

fn write_prefixed_namednode(
    f: &mut Formatter<'_>,
    nn: &NamedNode,
    prefixes: &HashMap<String, NamedNode>,
) -> std::fmt::Result {
    for (k, v) in prefixes.iter() {
        if nn.as_str().starts_with(v.as_str()) {
            write!(f, "{}:{}", k, nn.as_str().strip_prefix(v.as_str()).unwrap())?;
            return Ok(());
        }
    }
    write!(f, "{}", nn)
}

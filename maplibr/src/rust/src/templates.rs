use oxrdf::{BlankNode, Literal as OxLiteral, NamedNode, Variable};
use representation::constants::OTTR_TRIPLE;
use savvy::{savvy, ListSexp};
use templates::ast::{
    Argument, ConstantTerm, ConstantTermOrList, Instance, ListExpanderType, PType, Parameter,
    Signature, StottrTerm, Template,
};

fn parse_list_expander(s: Option<&str>) -> savvy::Result<Option<ListExpanderType>> {
    match s {
        None => Ok(None),
        Some("cross") => Ok(Some(ListExpanderType::Cross)),
        Some("zipMin") => Ok(Some(ListExpanderType::ZipMin)),
        Some("zipMax") => Ok(Some(ListExpanderType::ZipMax)),
        Some(other) => Err(crate::errors::argument_error(format!(
            "Unknown list_expander \"{other}\", expected one of \"cross\", \"zipMin\", \"zipMax\""
        ))),
    }
}

/// An OTTR constant term: an IRI, blank node, literal, or explicit "none".
/// Mirrors py_maplib's `extract_constant_term` (lib/templates/src/python.rs:434-446).
///
/// @export
#[savvy]
pub struct RConstantTerm {
    pub inner: ConstantTerm,
}

#[savvy]
impl RConstantTerm {
    fn iri(iri: &str) -> savvy::Result<Self> {
        let nn = NamedNode::new(iri).map_err(crate::errors::argument_error)?;
        Ok(RConstantTerm {
            inner: ConstantTerm::Iri(nn),
        })
    }

    fn blank_node(id: &str) -> savvy::Result<Self> {
        let bn = BlankNode::new(id).map_err(crate::errors::argument_error)?;
        Ok(RConstantTerm {
            inner: ConstantTerm::BlankNode(bn),
        })
    }

    fn literal(value: &str, datatype_iri: &str, language: Option<&str>) -> savvy::Result<Self> {
        let literal = if let Some(language) = language {
            OxLiteral::new_language_tagged_literal_unchecked(value, language)
        } else {
            let dt =
                NamedNode::new(datatype_iri).map_err(crate::errors::argument_error)?;
            OxLiteral::new_typed_literal(value, dt)
        };
        Ok(RConstantTerm {
            inner: ConstantTerm::Literal(literal),
        })
    }

    fn none() -> Self {
        RConstantTerm {
            inner: ConstantTerm::None,
        }
    }
}

/// An OTTR argument: a Variable, or a constant term (IRI/BlankNode/Literal/none).
/// Mirrors py_maplib's `Argument` (lib/templates/src/python.rs:203-236).
///
/// @export
#[savvy]
pub struct RArgument {
    pub inner: Argument,
}

#[savvy]
impl RArgument {
    fn from_variable(name: &str, list_expand: bool) -> savvy::Result<Self> {
        let v = Variable::new(name).map_err(crate::errors::argument_error)?;
        Ok(RArgument {
            inner: Argument {
                list_expand,
                term: StottrTerm::Variable(v),
            },
        })
    }

    fn from_constant_term(term: &RConstantTerm, list_expand: bool) -> Self {
        RArgument {
            inner: Argument {
                list_expand,
                term: StottrTerm::ConstantTerm(ConstantTermOrList::ConstantTerm(
                    term.inner.clone(),
                )),
            },
        }
    }
}

/// An OTTR template parameter. Mirrors py_maplib's `Parameter`
/// (lib/templates/src/python.rs:21-167) -- `rdf_type` here only supports a
/// basic (non-nested, non-list) RDF type, given as a datatype/class IRI
/// string; PType::Lub/List/NEList and RDFType.Nested()/Multi() are not yet
/// ported (they need the RDFType port, maplib-snz's remaining scope).
///
/// @export
#[savvy]
pub struct RParameter {
    pub inner: Parameter,
}

#[savvy]
impl RParameter {
    fn new(
        variable_name: &str,
        optional: bool,
        allow_blank: bool,
        rdf_type_iri: Option<&str>,
        default_value: Option<&RConstantTerm>,
    ) -> savvy::Result<Self> {
        let variable =
            Variable::new(variable_name).map_err(crate::errors::argument_error)?;
        let ptype = rdf_type_iri
            .map(|iri| {
                NamedNode::new(iri)
                    .map(PType::Basic)
                    .map_err(crate::errors::argument_error)
            })
            .transpose()?;
        let default_value =
            default_value.map(|d| ConstantTermOrList::ConstantTerm(d.inner.clone()));
        Ok(RParameter {
            inner: Parameter {
                optional,
                non_blank: !allow_blank,
                ptype,
                variable,
                default_value,
            },
        })
    }
}

/// An OTTR template instance: a call to a template with concrete arguments.
/// Mirrors py_maplib's `Instance` (lib/templates/src/python.rs:245-288).
///
/// @export
#[savvy]
pub struct RInstance {
    pub inner: Instance,
}

#[savvy]
impl RInstance {
    fn new(
        template_iri: &str,
        arguments: ListSexp,
        list_expander: Option<&str>,
    ) -> savvy::Result<Self> {
        let template_iri =
            NamedNode::new(template_iri).map_err(crate::errors::argument_error)?;
        let list_expander = parse_list_expander(list_expander)?;
        let mut argument_list = Vec::new();
        for (_, value) in arguments.iter() {
            let a = <&RArgument>::try_from(value)?;
            argument_list.push(a.inner.clone());
        }
        Ok(RInstance {
            inner: Instance {
                list_expander,
                template_iri,
                argument_list,
            },
        })
    }

    fn iri(&self) -> savvy::Result<savvy::Sexp> {
        self.inner.template_iri.as_str().to_string().try_into()
    }
}

/// An OTTR template: a named, parameterized set of instance patterns.
/// Mirrors py_maplib's `Template` (lib/templates/src/python.rs:291-402).
///
/// @export
#[savvy]
pub struct RTemplate {
    pub inner: Template,
}

#[savvy]
impl RTemplate {
    fn new(iri: &str, parameters: ListSexp, instances: ListSexp) -> savvy::Result<Self> {
        let iri = NamedNode::new(iri).map_err(crate::errors::argument_error)?;
        let mut parameter_list = Vec::new();
        for (_, value) in parameters.iter() {
            let p = <&RParameter>::try_from(value)?;
            parameter_list.push(p.inner.clone());
        }
        let mut pattern_list = Vec::new();
        for (_, value) in instances.iter() {
            let i = <&RInstance>::try_from(value)?;
            pattern_list.push(i.inner.clone());
        }
        Ok(RTemplate {
            inner: Template {
                signature: Signature {
                    iri,
                    parameter_list,
                    annotation_list: None,
                },
                pattern_list,
            },
        })
    }

    fn iri(&self) -> savvy::Result<savvy::Sexp> {
        self.inner.signature.iri.as_str().to_string().try_into()
    }

    fn print_string(&self) -> savvy::Result<savvy::Sexp> {
        self.inner.to_string().try_into()
    }

    /// Build a new Instance calling this Template (mirrors
    /// `PyTemplate::instance`, lib/templates/src/python.rs:335-346).
    fn instance(
        &self,
        arguments: ListSexp,
        list_expander: Option<&str>,
    ) -> savvy::Result<RInstance> {
        RInstance::new(
            self.inner.signature.iri.as_str(),
            arguments,
            list_expander,
        )
    }
}

/// Build an rdf:type-style Triple instance (mirrors py_maplib's `Triple()`
/// helper, lib/templates/src/python.rs:404-417).
///
/// @export
#[savvy]
fn make_triple(
    subject: &RArgument,
    predicate: &RArgument,
    object: &RArgument,
    list_expander: Option<&str>,
) -> savvy::Result<RInstance> {
    let list_expander = parse_list_expander(list_expander)?;
    let template_iri = NamedNode::new(OTTR_TRIPLE).unwrap();
    Ok(RInstance {
        inner: Instance {
            list_expander,
            template_iri,
            argument_list: vec![
                subject.inner.clone(),
                predicate.inner.clone(),
                object.inner.clone(),
            ],
        },
    })
}

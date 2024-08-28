use crate::ast::{
    Annotation, Argument, ConstantTerm, ConstantTermOrList, DefaultValue, Directive, Instance,
    PType, Parameter, Signature, Statement, StottrDocument, StottrTerm, Template,
};
use crate::constants::{
    OTTR_PREFIX, OTTR_PREFIX_IRI, RDFS_PREFIX, RDFS_PREFIX_IRI, RDF_PREFIX, RDF_PREFIX_IRI,
    XSD_PREFIX, XSD_PREFIX_IRI,
};
use crate::parsing::parsing_ast::{
    ResolvesToNamedNode, UnresolvedAnnotation, UnresolvedArgument, UnresolvedBaseTemplate,
    UnresolvedConstantLiteral, UnresolvedConstantTerm, UnresolvedDefaultValue, UnresolvedInstance,
    UnresolvedPType, UnresolvedParameter, UnresolvedSignature, UnresolvedStatement,
    UnresolvedStottrDocument, UnresolvedStottrLiteral, UnresolvedStottrTerm, UnresolvedTemplate,
};
use log::warn;
use oxrdf::{IriParseError, Literal, NamedNode};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

#[derive(Debug)]
pub enum ResolutionError {
    DuplicatedPrefixDefinition(String, String, String),
    BadCompositeIRIError(IriParseError),
    MissingPrefixError(String),
}

impl Display for ResolutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolutionError::DuplicatedPrefixDefinition(prefix, def1, def2) => {
                write!(
                    f,
                    "Prefix {} has two defintions: {} and {}",
                    prefix, def1, def2
                )
            }
            ResolutionError::BadCompositeIRIError(iri_err) => {
                write!(f, "Bad composite IRI {}", iri_err)
            }
            ResolutionError::MissingPrefixError(prefix) => {
                write!(f, "Prefix {} is not defined", prefix)
            }
        }
    }
}

impl Error for ResolutionError {}

pub(crate) fn resolve_document(
    unresolved_document: UnresolvedStottrDocument,
) -> Result<StottrDocument, ResolutionError> {
    let directives = unresolved_document.directives.clone();
    let mut prefix_map = build_prefix_map(&directives)?;
    let mut statements = vec![];
    for us in &unresolved_document.statements {
        statements.push(resolve_statement(us, &mut prefix_map)?);
    }
    Ok(StottrDocument {
        directives,
        statements,
        prefix_map,
    })
}

fn resolve_statement(
    unresolved_statement: &UnresolvedStatement,
    prefix_map: &mut HashMap<String, NamedNode>,
) -> Result<Statement, ResolutionError> {
    Ok(match unresolved_statement {
        UnresolvedStatement::Signature(s) => Statement::Template(Template {
            signature: resolve_signature(s, prefix_map)?,
            pattern_list: vec![],
        }),
        UnresolvedStatement::Template(t) => Statement::Template(resolve_template(t, prefix_map)?),
        UnresolvedStatement::BaseTemplate(b) => {
            Statement::Template(resolve_base_template(b, prefix_map)?)
        }
        UnresolvedStatement::Instance(i) => Statement::Instance(resolve_instance(i, prefix_map)?),
    })
}

fn resolve_base_template(
    unresolved_base_template: &UnresolvedBaseTemplate,
    prefix_map: &mut HashMap<String, NamedNode>,
) -> Result<Template, ResolutionError> {
    Ok(Template {
        signature: resolve_signature(&unresolved_base_template.signature, prefix_map)?,
        pattern_list: vec![],
    })
}

fn resolve_template(
    unresolved_template: &UnresolvedTemplate,
    prefix_map: &mut HashMap<String, NamedNode>,
) -> Result<Template, ResolutionError> {
    let mut pattern_list = vec![];
    for ui in &unresolved_template.pattern_list {
        pattern_list.push(resolve_instance(ui, prefix_map)?);
    }
    Ok(Template {
        signature: resolve_signature(&unresolved_template.signature, prefix_map)?,
        pattern_list,
    })
}

fn resolve_signature(
    unresolved_signature: &UnresolvedSignature,
    prefix_map: &mut HashMap<String, NamedNode>,
) -> Result<Signature, ResolutionError> {
    let mut parameter_list = vec![];
    for up in &unresolved_signature.parameter_list {
        parameter_list.push(resolve_parameter(up, prefix_map)?);
    }
    let annotation_list;
    if let Some(unresolved_annotation_list) = &unresolved_signature.annotation_list {
        let mut some_annotation_list = vec![];
        for ua in unresolved_annotation_list {
            some_annotation_list.push(resolve_annotation(ua, prefix_map)?);
        }
        annotation_list = Some(some_annotation_list);
    } else {
        annotation_list = None;
    }
    let prefixed_name = get_name(&unresolved_signature.template_name);

    Ok(Signature {
        template_name: resolve(&unresolved_signature.template_name, prefix_map)?,
        template_prefixed_name: prefixed_name,
        parameter_list,
        annotation_list,
    })
}

fn get_name(resolves_to_name: &ResolvesToNamedNode) -> Option<String> {
    match &resolves_to_name {
        ResolvesToNamedNode::PrefixedName(pname) => {
            Some(format!("{}:{}", pname.prefix, pname.name))
        }
        ResolvesToNamedNode::NamedNode(_) => None,
    }
}

fn resolve_annotation(
    unresolved_annotation: &UnresolvedAnnotation,
    prefix_map: &mut HashMap<String, NamedNode>,
) -> Result<Annotation, ResolutionError> {
    Ok(Annotation {
        instance: resolve_instance(&unresolved_annotation.instance, prefix_map)?,
    })
}

fn resolve_instance(
    unresolved_instance: &UnresolvedInstance,
    prefix_map: &mut HashMap<String, NamedNode>,
) -> Result<Instance, ResolutionError> {
    let mut argument_list = vec![];
    for ua in &unresolved_instance.argument_list {
        argument_list.push(resolve_argument(ua, prefix_map)?);
    }
    Ok(Instance {
        list_expander: unresolved_instance.list_expander.clone(),
        template_name: resolve(&unresolved_instance.template_name, prefix_map)?,
        prefixed_template_name: get_name(&unresolved_instance.template_name),
        argument_list,
    })
}

fn resolve_argument(
    unresolved_argument: &UnresolvedArgument,
    prefix_map: &mut HashMap<String, NamedNode>,
) -> Result<Argument, ResolutionError> {
    Ok(Argument {
        list_expand: unresolved_argument.list_expand,
        term: resolve_stottr_term(&unresolved_argument.term, prefix_map)?,
    })
}

fn resolve_stottr_term(
    unresolved_stottr_term: &UnresolvedStottrTerm,
    prefix_map: &mut HashMap<String, NamedNode>,
) -> Result<StottrTerm, ResolutionError> {
    Ok(match unresolved_stottr_term {
        UnresolvedStottrTerm::Variable(v) => StottrTerm::Variable(v.clone()),
        UnresolvedStottrTerm::ConstantTerm(t) => {
            StottrTerm::ConstantTerm(resolve_constant_term(t, prefix_map)?)
        }
        UnresolvedStottrTerm::List(l) => {
            let mut terms = vec![];
            for ust in l {
                terms.push(resolve_stottr_term(ust, prefix_map)?);
            }
            StottrTerm::List(terms)
        }
    })
}

fn resolve_constant_term(
    unresolved_constant_term: &UnresolvedConstantTerm,
    prefix_map: &mut HashMap<String, NamedNode>,
) -> Result<ConstantTermOrList, ResolutionError> {
    Ok(match unresolved_constant_term {
        UnresolvedConstantTerm::Constant(c) => {
            ConstantTermOrList::ConstantTerm(resolve_constant_literal(c, prefix_map)?)
        }
        UnresolvedConstantTerm::ConstantList(cl) => {
            let mut constant_terms = vec![];
            for uct in cl {
                constant_terms.push(resolve_constant_term(uct, prefix_map)?);
            }
            ConstantTermOrList::ConstantList(constant_terms)
        }
    })
}

fn resolve_constant_literal(
    unresolved_constant_literal: &UnresolvedConstantLiteral,
    prefix_map: &HashMap<String, NamedNode>,
) -> Result<ConstantTerm, ResolutionError> {
    Ok(match unresolved_constant_literal {
        UnresolvedConstantLiteral::Iri(iri) => ConstantTerm::Iri(resolve(iri, prefix_map)?),
        UnresolvedConstantLiteral::BlankNode(bn) => ConstantTerm::BlankNode(bn.clone()),
        UnresolvedConstantLiteral::Literal(lit) => {
            let resolved_lit = resolve_stottr_literal(lit, prefix_map)?;
            ConstantTerm::Literal(resolved_lit)
        }
        UnresolvedConstantLiteral::None => ConstantTerm::None,
    })
}

fn resolve_stottr_literal(
    unresolved_stottr_literal: &UnresolvedStottrLiteral,
    prefix_map: &HashMap<String, NamedNode>,
) -> Result<Literal, ResolutionError> {
    let value = unresolved_stottr_literal.value.clone();
    let language = unresolved_stottr_literal.language.clone();
    let data_type_iri =
        if let Some(unresolved_data_type_uri) = &unresolved_stottr_literal.data_type_iri {
            Some(resolve(unresolved_data_type_uri, prefix_map)?)
        } else {
            None
        };

    let literal = if let Some(language) = language {
        Literal::new_language_tagged_literal_unchecked(value, language)
    } else if let Some(data_type_iri) = data_type_iri {
        Literal::new_typed_literal(value, data_type_iri)
    } else {
        Literal::new_simple_literal(value)
    };
    Ok(literal)
}

fn resolve_parameter(
    unresolved_parameter: &UnresolvedParameter,
    prefix_map: &mut HashMap<String, NamedNode>,
) -> Result<Parameter, ResolutionError> {
    Ok(Parameter {
        optional: unresolved_parameter.optional,
        non_blank: unresolved_parameter.non_blank,
        ptype: if let Some(uptype) = &unresolved_parameter.ptype {
            Some(resolve_ptype(uptype, prefix_map)?)
        } else {
            None
        },
        variable: unresolved_parameter.variable.clone(),
        default_value: if let Some(udefault_value) = &unresolved_parameter.default_value {
            Some(resolve_default_value(udefault_value, prefix_map)?)
        } else {
            None
        },
    })
}

fn resolve_default_value(
    unresolved_default_value: &UnresolvedDefaultValue,
    prefix_map: &mut HashMap<String, NamedNode>,
) -> Result<DefaultValue, ResolutionError> {
    Ok(DefaultValue {
        constant_term: resolve_constant_term(&unresolved_default_value.constant_term, prefix_map)?,
    })
}

fn resolve_ptype(
    unresolved_ptype: &UnresolvedPType,
    prefix_map: &mut HashMap<String, NamedNode>,
) -> Result<PType, ResolutionError> {
    Ok(match unresolved_ptype {
        UnresolvedPType::Basic(b) => {
            let resolved = resolve(b, prefix_map)?;
            PType::Basic(resolved)
        }
        UnresolvedPType::Lub(l) => PType::Lub(Box::new(resolve_ptype(l, prefix_map)?)),
        UnresolvedPType::List(l) => PType::List(Box::new(resolve_ptype(l, prefix_map)?)),
        UnresolvedPType::NEList(l) => PType::NEList(Box::new(resolve_ptype(l, prefix_map)?)),
    })
}

fn resolve(
    resolves_to_named_node: &ResolvesToNamedNode,
    prefix_map: &HashMap<String, NamedNode>,
) -> Result<NamedNode, ResolutionError> {
    Ok(match resolves_to_named_node {
        ResolvesToNamedNode::PrefixedName(pn) => {
            if let Some(nn) = prefix_map.get(&pn.prefix) {
                let new_node_result = NamedNode::new(format!("{}{}", nn.as_str(), &pn.name));
                match new_node_result {
                    Ok(new_node) => new_node,
                    Err(err) => return Err(ResolutionError::BadCompositeIRIError(err)),
                }
            } else {
                return Err(ResolutionError::MissingPrefixError(format!(
                    "{}:{}",
                    &pn.prefix, &pn.name
                )));
            }
        }
        ResolvesToNamedNode::NamedNode(nn) => nn.clone(),
    })
}

fn build_prefix_map(
    directives: &Vec<Directive>,
) -> Result<HashMap<String, NamedNode>, ResolutionError> {
    let mut map = HashMap::new();
    for d in directives {
        match d {
            Directive::Prefix(p) => {
                insert_or_raise(&p.name, &p.iri, &mut map)?;
            }
            Directive::Base(b) => {
                insert_or_raise("", b, &mut map)?;
            }
        }
    }
    let predefined = [
        (RDFS_PREFIX, RDFS_PREFIX_IRI),
        (RDF_PREFIX, RDF_PREFIX_IRI),
        (XSD_PREFIX, XSD_PREFIX_IRI),
        (OTTR_PREFIX, OTTR_PREFIX_IRI),
    ];
    for (pre, iri) in predefined {
        if !map.contains_key(pre) {
            map.insert(pre.to_string(), NamedNode::new_unchecked(iri));
        }
    }
    Ok(map)
}

fn insert_or_raise(
    key: &str,
    value: &NamedNode,
    map: &mut HashMap<String, NamedNode>,
) -> Result<(), ResolutionError> {
    if let Some(n) = map.insert(key.to_string(), value.clone()) {
        if &n != value {
            return Err(ResolutionError::DuplicatedPrefixDefinition(
                key.to_string(),
                value.as_str().to_string(),
                n.as_str().to_string(),
            ));
        } else {
            warn!("Prefix {} was defined as {} two times", key, value.as_str());
        }
    }
    Ok(())
}

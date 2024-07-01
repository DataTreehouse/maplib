pub mod errors;

use crate::ast::{
    Instance, PType, Parameter, Signature, Statement, StottrDocument, StottrTerm, StottrVariable,
    Template,
};
use crate::constants::OTTR_TRIPLE;
use crate::document::document_from_file;
use errors::TemplateError;
use log::warn;

use oxrdf::NamedNode;
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME, VERB_COL_NAME};
use std::collections::{HashMap, HashSet};
use std::fs::read_dir;
use std::path::Path;

#[derive(Clone, Debug)]
pub struct TemplateDataset {
    pub templates: Vec<Template>,
    pub ground_instances: Vec<Instance>,
    pub prefix_map: HashMap<String, NamedNode>,
}

impl TemplateDataset {
    pub fn from_documents(
        mut documents: Vec<StottrDocument>,
    ) -> Result<TemplateDataset, TemplateError> {
        let mut templates = vec![];
        let mut ground_instances = vec![];
        let mut prefix_map = HashMap::new();
        let mut defined_prefixes = HashSet::new();
        for d in &mut documents {
            for (k, v) in d.prefix_map.drain() {
                if defined_prefixes.contains(&k) {
                    let mut remove = false;
                    if let Some(v_prime) = prefix_map.get(&k) {
                        if &v != v_prime {
                            remove = true;
                        }
                    }
                    if remove {
                        prefix_map.remove(&k);
                        warn!("Prefix {} has conflicting definitions across documents, consider harmonizing", k);
                    }
                } else {
                    defined_prefixes.insert(k.clone());
                    prefix_map.insert(k, v);
                }
            }
            for i in d.statements.drain(0..d.statements.len()) {
                match i {
                    Statement::Template(t) => {
                        templates.push(t);
                    }
                    Statement::Instance(i) => {
                        ground_instances.push(i);
                    }
                }
            }
        }
        let mut td = TemplateDataset {
            templates,
            ground_instances,
            prefix_map,
        };
        //TODO: Put in function, check not exists and consistent...
        let ottr_triple_subject = Parameter {
            optional: false,
            non_blank: false,
            ptype: Some(PType::Basic(
                BaseRDFNodeType::IRI,
                Some("ottr:IRI".to_string()),
            )),
            stottr_variable: StottrVariable {
                name: SUBJECT_COL_NAME.to_string(),
            },
            default_value: None,
        };
        let ottr_triple_verb = Parameter {
            optional: false,
            non_blank: false,
            ptype: Some(PType::Basic(
                BaseRDFNodeType::IRI,
                Some("ottr:IRI".to_string()),
            )),
            stottr_variable: StottrVariable {
                name: VERB_COL_NAME.to_string(),
            },
            default_value: None,
        };
        let ottr_triple_object = Parameter {
            optional: false,
            non_blank: false,
            ptype: None,
            stottr_variable: StottrVariable {
                name: OBJECT_COL_NAME.to_string(),
            },
            default_value: None,
        };

        let ottr_template = Template {
            signature: Signature {
                template_name: NamedNode::new_unchecked(OTTR_TRIPLE),
                template_prefixed_name: Some("ottr:Triple".to_string()),
                parameter_list: vec![ottr_triple_subject, ottr_triple_verb, ottr_triple_object],
                annotation_list: None,
            },
            pattern_list: vec![],
        };
        td.templates.push(ottr_template);
        //Todo: variable safe, no cycles, referential integrity, no duplicates, well founded
        //Check ground instances also!!
        td.infer_types()?;
        Ok(td)
    }

    pub fn from_folder<P: AsRef<Path>>(path: P) -> Result<TemplateDataset, TemplateError> {
        let mut docs = vec![];
        let files_result = read_dir(path).map_err(TemplateError::ReadTemplateDirectoryError)?;
        for f in files_result {
            let f = f.map_err(TemplateError::ResolveDirectoryEntryError)?;
            if let Some(e) = f.path().extension() {
                if let Some(s) = e.to_str() {
                    let extension = s.to_lowercase();
                    if "stottr" == &extension {
                        let doc = document_from_file(f.path())?;
                        docs.push(doc);
                    }
                }
            }
        }
        TemplateDataset::from_documents(docs)
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<TemplateDataset, TemplateError> {
        let doc = document_from_file(path)?;
        TemplateDataset::from_documents(vec![doc])
    }

    pub fn get(&self, template: &str) -> Option<&Template> {
        self.templates
            .iter()
            .find(|&t| t.signature.template_name.as_str() == template)
    }

    fn infer_types(&mut self) -> Result<(), TemplateError> {
        let mut changed = true;
        while changed {
            let mut inner_changed = false;
            for i in 0..self.templates.len() {
                let (left, right) = self.templates.split_at_mut(i);
                let (element, right) = right.split_at_mut(1);
                inner_changed = inner_changed
                    || infer_template_types(
                        element.first_mut().unwrap(),
                        left.iter().chain(right.iter()).collect(),
                    )?;
            }
            if !inner_changed {
                changed = false;
            }
        }
        Ok(())
    }

    pub fn add_template(&mut self, template: Template) -> Result<(), TemplateError> {
        if let Some(prefix) = &template.signature.template_prefixed_name {
            self.prefix_map
                .insert(prefix.clone(), template.signature.template_name.clone());
        }
        if let Some(pos) = self
            .templates
            .iter()
            .position(|x| x.signature.template_name == template.signature.template_name)
        {
            self.templates.remove(pos);
        }
        self.templates.push(template);
        Ok(())
    }
}

fn infer_template_types(
    template: &mut Template,
    templates: Vec<&Template>,
) -> Result<bool, TemplateError> {
    let mut changed = false;
    for i in &mut template.pattern_list {
        let other = *templates
            .iter()
            .find(|t| t.signature.template_name == i.template_name)
            .unwrap();
        if i.argument_list.len() != other.signature.parameter_list.len() {
            return Err(TemplateError::InconsistentNumberOfArguments(
                template.signature.template_name.as_str().to_string(),
                other.signature.template_name.as_str().to_string(),
                i.argument_list.len(),
                other.signature.parameter_list.len(),
            ));
        }
        for (argument, other_parameter) in i
            .argument_list
            .iter()
            .zip(other.signature.parameter_list.iter())
        {
            match &argument.term {
                StottrTerm::Variable(v) => {
                    for my_parameter in &mut template.signature.parameter_list {
                        if &my_parameter.stottr_variable == v {
                            if let Some(other_ptype) = &other_parameter.ptype {
                                if argument.list_expand {
                                    if !other_parameter.optional {
                                        changed = changed
                                            || lub_update(
                                                &template.signature.template_name,
                                                v,
                                                my_parameter,
                                                &PType::NEList(Box::new(other_ptype.clone())),
                                            )?;
                                    } else {
                                        changed = changed
                                            || lub_update(
                                                &template.signature.template_name,
                                                v,
                                                my_parameter,
                                                &PType::List(Box::new(other_ptype.clone())),
                                            )?;
                                    }
                                } else {
                                    changed = changed
                                        || lub_update(
                                            &template.signature.template_name,
                                            v,
                                            my_parameter,
                                            other_ptype,
                                        )?;
                                }
                            }
                        }
                    }
                }
                StottrTerm::ConstantTerm(_) => {}
                StottrTerm::List(_) => {}
            }
        }
    }
    Ok(changed)
}

fn lub_update(
    template_name: &NamedNode,
    variable: &StottrVariable,
    my_parameter: &mut Parameter,
    right: &PType,
) -> Result<bool, TemplateError> {
    if my_parameter.ptype.is_none() {
        my_parameter.ptype = Some(right.clone());
        Ok(true)
    } else if my_parameter.ptype.as_ref().unwrap() != right {
        let ptype = lub(
            template_name,
            variable,
            my_parameter.ptype.as_ref().unwrap(),
            right,
        )?;
        if my_parameter.ptype.as_ref().unwrap() != &ptype {
            my_parameter.ptype = Some(ptype);
            Ok(true)
        } else {
            Ok(false)
        }
    } else {
        Ok(false)
    }
}

//TODO: LUB ptype...
fn lub(
    template_name: &NamedNode,
    variable: &StottrVariable,
    left: &PType,
    right: &PType,
) -> Result<PType, TemplateError> {
    if left == right || left.is_iri() && right.is_iri() {
        return Ok(left.clone());
    } else if let PType::NEList(left_inner) = left {
        if let PType::List(right_inner) = right {
            return Ok(PType::NEList(Box::new(lub(
                template_name,
                variable,
                left_inner,
                right_inner,
            )?)));
        } else if let PType::NEList(right_inner) = right {
            return Ok(PType::NEList(Box::new(lub(
                template_name,
                variable,
                left_inner,
                right_inner,
            )?)));
        }
    } else if let PType::List(left_inner) = left {
        if let PType::NEList(right_inner) = right {
            return Ok(PType::NEList(Box::new(lub(
                template_name,
                variable,
                left_inner,
                right_inner,
            )?)));
        } else if let PType::List(right_inner) = right {
            return Ok(PType::List(Box::new(lub(
                template_name,
                variable,
                left_inner,
                right_inner,
            )?)));
        }
    }
    Err(TemplateError::IncompatibleTypes(
        template_name.as_str().to_string(),
        variable.clone(),
        left.to_string(),
        right.to_string(),
    ))
}

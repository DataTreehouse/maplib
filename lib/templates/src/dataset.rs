pub mod errors;

use crate::ast::{
    Instance, PType, Parameter, Signature, Statement, StottrDocument, StottrTerm, Template,
};
use crate::document::document_from_file;
use errors::TemplateError;
use representation::constants::{OTTR_IRI, OTTR_TRIPLE};
use std::cmp::min;
use tracing::warn;

use crate::subtypes_ext::is_literal_subtype_ext;
use oxrdf::vocab::rdfs;
use oxrdf::{NamedNode, Variable};
use representation::{OBJECT_COL_NAME, PREDICATE_COL_NAME, SUBJECT_COL_NAME};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::path::Path;
use walkdir::WalkDir;

#[derive(Clone, Debug)]
pub struct TemplateDataset {
    pub templates: Vec<Template>,
    pub ground_instances: Vec<Instance>,
    pub prefix_map: HashMap<String, NamedNode>,
    pub inferred_types: bool,
}

impl TemplateDataset {
    pub fn new_empty() -> Result<TemplateDataset, TemplateError> {
        Self::from_documents(vec![])
    }

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
                        warn!("Prefix {k} has conflicting definitions across documents, consider harmonizing");
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
            inferred_types: false,
        };
        //TODO: Put in function, check not exists and consistent...
        let ottr_triple_subject = Parameter {
            optional: false,
            non_blank: false,
            ptype: Some(PType::Basic(NamedNode::new_unchecked(OTTR_IRI))),
            variable: Variable::new_unchecked(SUBJECT_COL_NAME),
            default_value: None,
        };
        let ottr_triple_predicate = Parameter {
            optional: false,
            non_blank: false,
            ptype: Some(PType::Basic(NamedNode::new_unchecked(OTTR_IRI))),
            variable: Variable::new_unchecked(PREDICATE_COL_NAME),
            default_value: None,
        };
        let ottr_triple_object = Parameter {
            optional: false,
            non_blank: false,
            ptype: None,
            variable: Variable::new_unchecked(OBJECT_COL_NAME),
            default_value: None,
        };

        let ottr_template = Template {
            signature: Signature {
                iri: NamedNode::new_unchecked(OTTR_TRIPLE),
                parameter_list: vec![
                    ottr_triple_subject,
                    ottr_triple_predicate,
                    ottr_triple_object,
                ],
                annotation_list: None,
            },
            pattern_list: vec![],
        };
        td.templates.push(ottr_template);
        Ok(td)
    }

    pub fn from_folder<P: AsRef<Path>>(
        path: P,
        recursive: bool,
        prefixes: Option<&HashMap<String, NamedNode>>,
    ) -> Result<TemplateDataset, TemplateError> {
        let mut docs = vec![];
        let mut walk_dir = WalkDir::new(path);
        walk_dir = walk_dir.min_depth(0);
        if !recursive {
            walk_dir = walk_dir.max_depth(1);
        }
        for f in walk_dir {
            let f = f.map_err(TemplateError::ResolveDirectoryEntryError)?;
            if let Some(e) = f.path().extension() {
                if let Some(s) = e.to_str() {
                    let extension = s.to_lowercase();
                    if "stottr" == &extension {
                        let doc = document_from_file(f.path(), prefixes)?;
                        docs.push(doc);
                    }
                }
            }
        }
        TemplateDataset::from_documents(docs)
    }

    pub fn from_file<P: AsRef<Path>>(
        path: P,
        prefixes: Option<&HashMap<String, NamedNode>>,
    ) -> Result<TemplateDataset, TemplateError> {
        let doc = document_from_file(path, prefixes)?;
        TemplateDataset::from_documents(vec![doc])
    }

    pub fn get(&self, template: &str) -> Option<&Template> {
        self.templates
            .iter()
            .find(|&t| t.signature.iri.as_str() == template)
    }

    pub fn infer_types(&mut self) -> Result<(), TemplateError> {
        let template_names: Vec<_> = self
            .templates
            .iter()
            .map(|x| x.signature.iri.as_str().to_string())
            .collect();
        let mut template_map: HashMap<String, Template> = self
            .templates
            .drain(..)
            .map(|x| (x.signature.iri.as_str().to_string(), x))
            .collect();
        let mut affects_map: HashMap<String, Vec<String>> = HashMap::new();
        for t in template_map.values() {
            for i in &t.pattern_list {
                if let Some(v) = affects_map.get_mut(i.template_iri.as_str()) {
                    v.push(t.signature.iri.as_str().to_string());
                } else {
                    affects_map.insert(
                        i.template_iri.as_str().to_string(),
                        vec![t.signature.iri.as_str().to_string()],
                    );
                }
            }
        }

        let mut changed = HashSet::new();

        for n in &template_names {
            let (template_name, mut template) = template_map.remove_entry(n).unwrap();
            let template_changed = infer_template_types(&mut template, &template_map)?;
            if template_changed {
                changed.insert(template_name.clone());
            }
            template_map.insert(template_name, template);
        }
        let mut iters = 0usize;
        while !changed.is_empty() {
            let mut new_changed = HashSet::new();
            for c in &changed {
                let (template_name, mut template) = template_map.remove_entry(c).unwrap();
                let template_changed = infer_template_types(&mut template, &template_map)?;
                if template_changed {
                    if let Some(affected) = affects_map.get(&template_name) {
                        for a in affected {
                            new_changed.insert(a.clone());
                        }
                    }
                }
                template_map.insert(template_name, template);
            }
            changed = new_changed;
            if iters > 10000 {
                warn!(
                    "Reach maximum inference iterations (# templates: {})",
                    self.templates.len()
                );
                changed = HashSet::new();
            }
            iters += 1;
        }

        self.templates = template_map.into_values().collect();
        self.inferred_types = true;
        Ok(())
    }

    pub fn add_template(&mut self, template: Template) -> Result<(), TemplateError> {
        if let Some(pos) = self
            .templates
            .iter()
            .position(|x| x.signature.iri == template.signature.iri)
        {
            self.templates.remove(pos);
        }
        self.templates.push(template);
        self.inferred_types = false;
        Ok(())
    }
}

impl Display for TemplateDataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let mut nns = vec![];
        for t in &self.templates {
            t.find_named_nodes(&mut nns);
        }
        let mut new_prefixes: HashMap<String, NamedNode> = HashMap::new();
        let mut p = 0usize;
        for nn in &nns {
            let mut found = false;
            'inner: for prefix in new_prefixes.values() {
                if nn.as_str().starts_with(prefix.as_str()) {
                    found = true;
                    break 'inner;
                }
            }
            if !found {
                'inner: for (prefix, val) in &self.prefix_map {
                    if nn.as_str().starts_with(val.as_str()) {
                        found = true;
                        new_prefixes.insert(prefix.clone(), val.clone());
                        break 'inner;
                    }
                }
            }
            if !found {
                let f = nn.as_str().rfind(|x| x == '#' || x == '/' || x == ':');
                if let Some(f) = f {
                    let s = nn.as_str()[0..f + 1].to_string();
                    let possible_prefix_value = NamedNode::new(s);
                    if let Ok(poss) = possible_prefix_value {
                        let prefix = format!("p{}", p);
                        p = p + 1;
                        new_prefixes.insert(prefix, poss);
                    }
                }
            }
        }
        let mut prefix_keys: Vec<_> = new_prefixes.keys().collect();
        prefix_keys.sort();
        for p in prefix_keys {
            let nn = new_prefixes.get(p).unwrap();
            writeln!(f, "@prefix {}: {} .", p, nn)?;
        }
        if !new_prefixes.is_empty() {
            writeln!(f, "")?;
        }
        for t in &self.templates {
            if t.signature.iri.as_str() != OTTR_TRIPLE {
                t.fmt_prefix(f, &new_prefixes)?;
                writeln!(f)?;
            }
        }
        for i in &self.ground_instances {
            i.fmt_prefixes(f, &new_prefixes)?;
            writeln!(f)?;
        }
        Ok(())
    }
}

fn infer_template_types(
    template: &mut Template,
    templates: &HashMap<String, Template>,
) -> Result<bool, TemplateError> {
    let mut changed = false;
    for i in &mut template.pattern_list {
        let other = if let Some(t) = templates.get(i.template_iri.as_str()) {
            t
        } else {
            return Err(TemplateError::TemplateNotFound(
                template.signature.iri.to_string().clone(),
                i.template_iri.to_string().clone(),
            ));
        };
        if i.argument_list.len() != other.signature.parameter_list.len() {
            return Err(TemplateError::InconsistentNumberOfArguments(
                template.signature.iri.as_str().to_string(),
                other.signature.iri.as_str().to_string(),
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
                        if &my_parameter.variable == v {
                            if let Some(other_ptype) = &other_parameter.ptype {
                                if argument.list_expand {
                                    if !other_parameter.optional {
                                        changed = changed
                                            || lub_update(
                                                &template.signature.iri,
                                                v,
                                                my_parameter,
                                                &PType::NEList(Box::new(other_ptype.clone())),
                                            )?;
                                    } else {
                                        changed = changed
                                            || lub_update(
                                                &template.signature.iri,
                                                v,
                                                my_parameter,
                                                &PType::List(Box::new(other_ptype.clone())),
                                            )?;
                                    }
                                } else {
                                    changed = changed
                                        || lub_update(
                                            &template.signature.iri,
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
    variable: &Variable,
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
    variable: &Variable,
    left: &PType,
    right: &PType,
) -> Result<PType, TemplateError> {
    if left == right || left.is_iri() && right.is_iri() {
        return Ok(min(left, right).clone());
    } else if let (PType::Basic(left_basic), PType::Basic(right_basic)) = (left, right) {
        if left_basic.as_ref() == rdfs::RESOURCE
            || right_basic.as_ref() == rdfs::RESOURCE
            || is_literal_subtype_ext(left_basic.as_ref(), right_basic.as_ref())
            || is_literal_subtype_ext(right_basic.as_ref(), left_basic.as_ref())
        {
            return Ok(min(left, right).clone());
        } else {
            // Returns error
        }
    } else if let PType::NEList(left_inner) = left {
        if let PType::Basic(nn) = right {
            if nn.as_ref() == rdfs::RESOURCE {
                return Ok(left.clone());
            }
        } else if let PType::List(right_inner) = right {
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
        if let PType::Basic(nn) = right {
            if nn.as_ref() == rdfs::RESOURCE {
                return Ok(left.clone());
            }
        } else if let PType::NEList(right_inner) = right {
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

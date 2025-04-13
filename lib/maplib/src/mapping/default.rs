use super::Mapping;
use crate::mapping::errors::MappingError;
use crate::mapping::ExpandOptions;
use log::warn;
use std::collections::HashMap;
use templates::ast::{
    Argument, ConstantTerm, ConstantTermOrList, Instance, ListExpanderType, PType, Parameter,
    Signature, StottrTerm, Template,
};
use templates::constants::{DEFAULT_PREFIX, OTTR_IRI, OTTR_TRIPLE};

use crate::errors::MaplibError;
use crate::mapping::expansion::validation::infer_type_from_column;
use oxrdf::{NamedNode, Variable};
use polars::prelude::{col, DataFrame, DataType, IntoLazy};
use templates::MappingColumnType;

impl Mapping {
    pub fn expand_default(
        &mut self,
        mut df: DataFrame,
        pk_col: String,
        fk_cols: Vec<String>,
        dry_run: bool,
        mapping_column_types: Option<HashMap<String, MappingColumnType>>,
        options: ExpandOptions,
    ) -> Result<Template, MaplibError> {
        let mut params = vec![];
        let columns: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|x| x.to_string())
            .collect();
        for c in &columns {
            let dt = df.column(c).unwrap().dtype().clone();
            let has_null = df.column(c).unwrap().is_null().any();
            if c == &pk_col {
                if let DataType::List(..) = dt {
                    todo!()
                }
                if dt != DataType::String {
                    warn!(
                        "Primary key column {} is not String but instead {}. Will be cast",
                        &pk_col, dt
                    );
                    df = df
                        .lazy()
                        .with_column(col(c).cast(DataType::String))
                        .collect()
                        .unwrap();
                }

                params.push(Parameter {
                    optional: has_null,
                    non_blank: false,
                    ptype: Some(PType::Basic(NamedNode::new_unchecked(OTTR_IRI))),
                    variable: Variable::new_unchecked(c),
                    default_value: None,
                })
            } else if fk_cols.contains(c) {
                if let DataType::List(..) = dt {
                    todo!()
                }

                if dt != DataType::String {
                    warn!(
                        "Foreign key column {} is not String but instead {}. Will be cast",
                        &c, dt
                    );
                    df = df
                        .lazy()
                        .with_column(col(c).cast(DataType::String))
                        .collect()
                        .unwrap();
                }

                params.push(Parameter {
                    optional: has_null,
                    non_blank: false,
                    ptype: Some(PType::Basic(NamedNode::new_unchecked(OTTR_IRI))),
                    variable: Variable::new_unchecked(c),
                    default_value: None,
                })
            } else {
                let t = if let Some(map) = &mapping_column_types {
                    map.get(c.as_str()).cloned()
                } else {
                    None
                };
                let t = t.unwrap_or(infer_type_from_column(df.column(c).unwrap())?);
                let pt = t.as_ptype();

                params.push(Parameter {
                    optional: has_null,
                    non_blank: false,
                    ptype: Some(pt),
                    variable: Variable::new_unchecked(c),
                    default_value: None,
                });
            }
        }

        let mut patterns = vec![];
        for c in columns {
            if c != pk_col {
                let list_expander = if let DataType::List(..) = df.column(&c).unwrap().dtype() {
                    Some(ListExpanderType::Cross)
                } else {
                    None
                };

                patterns.push(Instance {
                    list_expander: list_expander.clone(),
                    template_name: NamedNode::new_unchecked(OTTR_TRIPLE),
                    prefixed_template_name: Some("ottr:Triple".to_string()),
                    argument_list: vec![
                        Argument {
                            list_expand: false,
                            term: StottrTerm::Variable(Variable::new_unchecked(&pk_col)),
                        },
                        Argument {
                            list_expand: false,
                            term: StottrTerm::ConstantTerm(ConstantTermOrList::ConstantTerm(
                                ConstantTerm::Iri(
                                    NamedNode::new(format!("{}{}", DEFAULT_PREFIX, c))
                                        .map_err(MappingError::IriParseError)?,
                                ),
                            )),
                        },
                        Argument {
                            list_expand: list_expander.is_some(),
                            term: StottrTerm::Variable(Variable::new_unchecked(c)),
                        },
                    ],
                })
            }
        }

        let template_name = format!(
            "{}default_template_{}",
            DEFAULT_PREFIX, &self.default_template_counter
        );
        self.default_template_counter += 1;
        let template = Template {
            signature: Signature {
                template_name: NamedNode::new(template_name.clone()).unwrap(),
                template_prefixed_name: None,
                parameter_list: params,
                annotation_list: None,
            },
            pattern_list: patterns,
        };
        if !dry_run {
            self.template_dataset.templates.push(template.clone());
            self.expand(
                template_name.as_str(),
                Some(df),
                mapping_column_types,
                options,
            )?;
        }
        Ok(template)
    }
}

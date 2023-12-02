use super::Mapping;
use crate::ast::{
    Argument, ConstantLiteral, ConstantTerm, Instance, ListExpanderType, PType, Parameter,
    Signature, StottrTerm, StottrVariable, Template,
};
use crate::constants::{
    DEFAULT_PREDICATE_URI_PREFIX, DEFAULT_TEMPLATE_PREFIX, OTTR_IRI, OTTR_TRIPLE,
};
use crate::mapping::errors::MappingError;
use crate::mapping::ExpandOptions;
use log::warn;

use oxrdf::NamedNode;
use polars::prelude::{col, IntoLazy};
use polars_core::frame::DataFrame;
use polars_core::prelude::DataType;
use uuid::Uuid;

impl Mapping {
    pub fn expand_default(
        &mut self,
        mut df: DataFrame,
        pk_col: String,
        fk_cols: Vec<String>,
        template_prefix: Option<String>,
        predicate_prefix_uri: Option<String>,
        options: ExpandOptions,
    ) -> Result<Template, MappingError> {
        let use_template_prefix = template_prefix.unwrap_or(DEFAULT_TEMPLATE_PREFIX.to_string());
        let use_predicate_uri_prefix =
            predicate_prefix_uri.unwrap_or(DEFAULT_PREDICATE_URI_PREFIX.to_string());
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
                if dt != DataType::Utf8 {
                    warn!(
                        "Primary key column {} is not Utf8 but instead {}. Will be cast",
                        &pk_col, dt
                    );
                    df = df
                        .lazy()
                        .with_column(col(c).cast(DataType::Utf8))
                        .collect()
                        .unwrap();
                }

                params.push(Parameter {
                    optional: has_null,
                    non_blank: false,
                    ptype: Some(PType::Basic(
                        NamedNode::new_unchecked(OTTR_IRI),
                        "ottr:IRI".to_string(),
                    )),
                    stottr_variable: StottrVariable {
                        name: c.to_string(),
                    },
                    default_value: None,
                })
            } else if fk_cols.contains(c) {
                if let DataType::List(..) = dt {
                    todo!()
                }

                if dt != DataType::Utf8 {
                    warn!(
                        "Foreign key column {} is not Utf8 but instead {}. Will be cast",
                        &c, dt
                    );
                    df = df
                        .lazy()
                        .with_column(col(c).cast(DataType::Utf8))
                        .collect()
                        .unwrap();
                }

                params.push(Parameter {
                    optional: has_null,
                    non_blank: false,
                    ptype: Some(PType::Basic(
                        NamedNode::new_unchecked(OTTR_IRI),
                        "ottr:IRI".to_string(),
                    )),
                    stottr_variable: StottrVariable {
                        name: c.to_string(),
                    },
                    default_value: None,
                })
            } else {
                params.push(Parameter {
                    optional: has_null,
                    non_blank: false,
                    ptype: None,
                    stottr_variable: StottrVariable {
                        name: c.to_string(),
                    },
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
                    template_name: NamedNode::new(OTTR_TRIPLE).unwrap(),
                    prefixed_template_name: "ottr:Triple".to_string(),
                    argument_list: vec![
                        Argument {
                            list_expand: false,
                            term: StottrTerm::Variable(StottrVariable {
                                name: pk_col.clone(),
                            }),
                        },
                        Argument {
                            list_expand: false,
                            term: StottrTerm::ConstantTerm(ConstantTerm::Constant(
                                ConstantLiteral::Iri(
                                    NamedNode::new(format!("{}{}", &use_predicate_uri_prefix, c))
                                        .unwrap(),
                                ),
                            )),
                        },
                        Argument {
                            list_expand: list_expander.is_some(),
                            term: StottrTerm::Variable(StottrVariable { name: c.clone() }),
                        },
                    ],
                })
            }
        }

        let template_uuid = Uuid::new_v4().to_string();
        let template_name = format!("{}{}", use_template_prefix, &template_uuid);
        let template = Template {
            signature: Signature {
                template_name: NamedNode::new(template_name.clone()).unwrap(),
                template_prefixed_name: format!("prefix:{}", template_uuid),
                parameter_list: params,
                annotation_list: None,
            },
            pattern_list: patterns,
        };
        self.template_dataset.templates.push(template.clone());
        self.expand(template_name.as_str(), Some(df), options)?;
        Ok(template)
    }
}

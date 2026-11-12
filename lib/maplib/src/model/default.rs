use super::Model;
use crate::model::errors::MappingError;
use crate::model::MapOptions;
use representation::constants::{DEFAULT_PREFIX_IRI, OTTR_IRI, OTTR_TRIPLE};
use std::collections::HashMap;
use templates::ast::{
    Argument, ConstantTerm, ConstantTermOrList, Instance, ListExpanderType, PType, Parameter,
    Signature, StottrTerm, Template,
};
use tracing::warn;

use crate::errors::MaplibError;
use crate::model::expansion::validation::{infer_type_from_column, normalize_lang_string_structs};
use oxrdf::{NamedNode, Variable};
use polars::prelude::{col, DataFrame, DataType, IntoLazy};
use templates::MappingColumnType;

impl Model {
    pub fn expand_default(
        &mut self,
        df: DataFrame,
        pk_col: String,
        fk_cols: Vec<String>,
        mapping_column_types: Option<HashMap<String, MappingColumnType>>,
        options: MapOptions,
    ) -> Result<Template, MaplibError> {
        let df = prepare_default_df(df, &pk_col, &fk_cols, true);
        let template =
            create_default_template(&df, &pk_col, &fk_cols, mapping_column_types.as_ref(), false)?;
        self.template_dataset.templates.push(template.clone());
        self.expand(
            template.signature.iri.as_str(),
            Some(df),
            mapping_column_types,
            options,
        )?;
        Ok(template)
    }

    /// Creates a default template from the first batch of a stream of batches with the same schema.
    /// All parameters are optional, since later batches may contain nulls where the first did not.
    /// The template is added to the model and the batch is mapped.
    /// Subsequent batches should be mapped with expand_default_batch.
    pub fn expand_default_first_batch(
        &mut self,
        df: DataFrame,
        pk_col: &str,
        fk_cols: &[String],
        mapping_column_types: Option<HashMap<String, MappingColumnType>>,
        options: MapOptions,
    ) -> Result<Template, MaplibError> {
        let df = prepare_default_df(df, pk_col, fk_cols, true);
        let template =
            create_default_template(&df, pk_col, fk_cols, mapping_column_types.as_ref(), true)?;
        self.template_dataset.templates.push(template.clone());
        if df.height() > 0 {
            self.expand(
                template.signature.iri.as_str(),
                Some(df),
                mapping_column_types,
                options,
            )?;
        }
        Ok(template)
    }

    /// Maps a subsequent batch using a default template created by expand_default_first_batch.
    pub fn expand_default_batch(
        &mut self,
        template: &Template,
        df: DataFrame,
        pk_col: &str,
        fk_cols: &[String],
        mapping_column_types: Option<HashMap<String, MappingColumnType>>,
        options: MapOptions,
    ) -> Result<(), MaplibError> {
        if df.height() == 0 {
            return Ok(());
        }
        let df = prepare_default_df(df, pk_col, fk_cols, false);
        self.expand(
            template.signature.iri.as_str(),
            Some(df),
            mapping_column_types,
            options,
        )?;
        Ok(())
    }
}

fn prepare_default_df(
    mut df: DataFrame,
    pk_col: &str,
    fk_cols: &[String],
    warn: bool,
) -> DataFrame {
    df = normalize_lang_string_structs(df);
    let columns: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|x| x.to_string())
        .collect();
    for c in &columns {
        let is_pk = c == pk_col;
        if !is_pk && !fk_cols.contains(c) {
            continue;
        }
        let dt = df.column(c).unwrap().dtype().clone();
        if let DataType::List(..) = dt {
            todo!()
        }
        if dt != DataType::String {
            if warn {
                if is_pk {
                    warn!(
                        "Primary key column {} is not String but instead {}. Will be cast",
                        c, dt
                    );
                } else {
                    warn!(
                        "Foreign key column {} is not String but instead {}. Will be cast",
                        c, dt
                    );
                }
            }
            df = df
                .lazy()
                .with_column(col(c).cast(DataType::String))
                .collect()
                .unwrap();
        }
    }
    df
}

fn create_default_template(
    df: &DataFrame,
    pk_col: &str,
    fk_cols: &[String],
    mapping_column_types: Option<&HashMap<String, MappingColumnType>>,
    all_optional: bool,
) -> Result<Template, MaplibError> {
    let mut params = vec![];
    let columns: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|x| x.to_string())
        .collect();
    for c in &columns {
        let has_null = all_optional || df.column(c).unwrap().is_null().any();
        let ptype = if c == pk_col || fk_cols.contains(c) {
            PType::Basic(NamedNode::new_unchecked(OTTR_IRI))
        } else {
            let t = mapping_column_types.and_then(|map| map.get(c.as_str()).cloned());
            let t = match t {
                Some(t) => t,
                None => infer_type_from_column(df.column(c).unwrap())?,
            };
            t.as_ptype()
        };
        params.push(Parameter {
            optional: has_null,
            non_blank: false,
            ptype: Some(ptype),
            variable: Variable::new_unchecked(c),
            default_value: None,
        });
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
                template_iri: NamedNode::new_unchecked(OTTR_TRIPLE),
                argument_list: vec![
                    Argument {
                        list_expand: false,
                        term: StottrTerm::Variable(Variable::new_unchecked(pk_col)),
                    },
                    Argument {
                        list_expand: false,
                        term: StottrTerm::ConstantTerm(ConstantTermOrList::ConstantTerm(
                            ConstantTerm::Iri(
                                NamedNode::new(format!("{DEFAULT_PREFIX_IRI}{c}"))
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
        DEFAULT_PREFIX_IRI,
        uuid::Uuid::new_v4().to_string().replace("-", "_")
    );
    Ok(Template {
        signature: Signature {
            iri: NamedNode::new(template_name).unwrap(),
            parameter_list: params,
            annotation_list: None,
        },
        pattern_list: patterns,
    })
}

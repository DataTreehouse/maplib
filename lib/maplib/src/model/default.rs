use crate::model::errors::MappingError;
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
use polars::prelude::{DataFrame, DataType};
use templates::MappingColumnType;

/// Generates a default template from a DataFrame without mapping anything.
/// The primary key column becomes the subject of one ottr:Triple per other column,
/// with predicates in the default namespace. Parameters are optional when the column has nulls.
pub fn generate_default_template(
    df: DataFrame,
    pk_col: &str,
    fk_cols: &[String],
    mapping_column_types: Option<&HashMap<String, MappingColumnType>>,
) -> Result<Template, MaplibError> {
    if !df.get_column_names().iter().any(|c| c.as_str() == pk_col) {
        return Err(MappingError::MissingParameterColumn(pk_col.to_string()).into());
    }
    let df = normalize_lang_string_structs(df);
    warn_non_string_keys(&df, pk_col, fk_cols);
    create_default_template(&df, pk_col, fk_cols, mapping_column_types)
}

/// Key columns are typed as ottr:IRI in the template, so mapping it requires String key columns.
fn warn_non_string_keys(df: &DataFrame, pk_col: &str, fk_cols: &[String]) {
    for c in df.columns() {
        let name = c.name().as_str();
        let kind = if name == pk_col {
            "Primary"
        } else if fk_cols.iter().any(|f| f == name) {
            "Foreign"
        } else {
            continue;
        };
        if c.dtype() != &DataType::String {
            warn!(
                "{kind} key column {name} is not String but instead {}. Cast it to String before mapping with the generated template",
                c.dtype()
            );
        }
    }
}

fn create_default_template(
    df: &DataFrame,
    pk_col: &str,
    fk_cols: &[String],
    mapping_column_types: Option<&HashMap<String, MappingColumnType>>,
) -> Result<Template, MaplibError> {
    let mut params = vec![];
    let columns: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|x| x.to_string())
        .collect();
    for c in &columns {
        let has_null = df.column(c).unwrap().is_null().any();
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

use crate::errors::QueryProcessingError;
use crate::expressions::functions::eval_expression_to_string::eval_expression_to_string;
use crate::expressions::functions::maybe_add_regex_feature_flags::maybe_add_regex_feature_flags;
use oxrdf::vocab::xsd;
use representation::RDFNodeState;
use spargebra::algebra::Expression;

pub fn create_regex_string(
    regex_sparql_expression: &Expression,
    regex_literal_type: &RDFNodeState,
    flags_expr: Option<(&Expression, &RDFNodeState)>,
) -> Result<String, QueryProcessingError> {
    if !regex_literal_type.is_lit_type(xsd::STRING) {
        return Err(QueryProcessingError::BadArgument(
            "Replace pattern was not a xsd:string".to_string(),
        ));
    }
    let flags = if let Some((flags_regex_expr, flags_type)) = flags_expr {
        if !flags_type.is_lit_type(xsd::STRING) {
            return Err(QueryProcessingError::BadArgument(format!(
                "Replace flags is was not a xsd:string"
            )));
        } else {
            Some(eval_expression_to_string(flags_regex_expr, true)?)
        }
    } else {
        None
    };
    let regex_str = eval_expression_to_string(regex_sparql_expression, true)?;
    let flags_str = if let Some(flags) = &flags {
        Some(flags.as_str())
    } else {
        None
    };
    let pattern = maybe_add_regex_feature_flags(&regex_str, flags_str);
    Ok(pattern)
}

use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use spargebra::algebra::{Expression, Function};

pub fn eval_expression_to_string(
    sparql_expression: &Expression,
    expect_string: bool,
) -> Result<String, QueryProcessingError> {
    if let Expression::Literal(l) = sparql_expression {
        if expect_string && l.datatype() != xsd::STRING {
            Err(QueryProcessingError::ExpectedConstantLiteralStringArgument(
                sparql_expression.clone(),
            ))
        } else {
            Ok(l.value().to_string())
        }
    } else if let Expression::NamedNode(nn) = sparql_expression {
        Ok(nn.as_str().to_string())
    } else if let Expression::FunctionCall(Function::Str, args) = sparql_expression {
        eval_expression_to_string(args.first().unwrap(), false)
    } else {
        Err(QueryProcessingError::ExpectedConstantLiteralArgument(
            sparql_expression.clone(),
        ))
    }
}

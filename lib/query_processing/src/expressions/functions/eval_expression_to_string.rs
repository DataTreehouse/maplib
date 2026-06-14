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
    } else if let Expression::FunctionCall(f, args) = sparql_expression {
        match f {
            Function::Str => eval_expression_to_string(args.get(0).unwrap(), false),
            _ => {
                return Err(QueryProcessingError::ExpectedConstantLiteralArgument(
                    sparql_expression.clone(),
                ))
            }
        }
    } else {
        return Err(QueryProcessingError::ExpectedConstantLiteralArgument(
            sparql_expression.clone(),
        ));
    }
}

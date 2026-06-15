use polars::prelude::Expr;
use spargebra::algebra::Function;

pub fn str_starts_ends_contains(expr_decoded: Expr, second_decoded: Expr, f: &Function) -> Expr {
    match f {
        Function::StrStarts => expr_decoded.str().starts_with(second_decoded),
        Function::StrEnds => expr_decoded.str().ends_with(second_decoded),
        Function::Contains => expr_decoded.str().contains_literal(second_decoded),
        _ => unreachable!("Should never happen"),
    }
}

use crate::errors::RepresentationError;
use oxrdf::{Literal, Term};

pub fn parse_literal_as_primitive<T: std::str::FromStr>(
    l: Literal,
) -> Result<T, RepresentationError> {
    let parsed = l.value().parse().map_err(|_x| {
        RepresentationError::InvalidLiteralError(format!("Could not parse as literal {}", l))
    })?;
    Ok(parsed)
}

pub fn parse_term_as_primitive<T: std::str::FromStr>(term: Term) -> Result<T, RepresentationError> {
    match term {
        Term::Literal(l) => parse_literal_as_primitive(l),
        _ => Err(RepresentationError::InvalidLiteralError(format!(
            "Wrong term type when trying to parse literal {}",
            term
        ))),
    }
}

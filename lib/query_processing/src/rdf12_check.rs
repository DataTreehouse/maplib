//! Finds the RDF 1.2 features of queries and updates that maplib does not support yet: triple
//! terms (`<<( s p o )>>`, also from the reifier syntax `<< s p o >>`, which is parsed into
//! `rdf:reifies` patterns of triple terms) and literals with a base direction (`"a"@ar--rtl`).
//!
//! The SPARQL parser is built with SPARQL 1.2, so these reach maplib, and the evaluation of
//! patterns would otherwise treat a triple term like an unbound variable in places, giving
//! wrong results instead of an error. The matches below are exhaustive on purpose: a new kind
//! of pattern or term then has to be added here.

use representation::rdf12::{
    has_base_direction, BASE_DIRECTION_NOT_SUPPORTED, TRIPLE_TERMS_NOT_SUPPORTED,
};
use spargebra::algebra::{
    AggregateExpression, Expression, Function, GraphPattern, OrderExpression,
    PropertyPathExpression,
};
use spargebra::term::{
    GroundQuad, GroundQuadPattern, GroundTerm, GroundTermPattern, Literal, Quad, QuadPattern, Term,
    TermPattern, TriplePattern,
};
use spargebra::{GraphUpdateOperation, Query, Update};

type Check = Result<(), &'static str>;

/// The message for the first RDF 1.2 feature of the query that maplib does not support yet.
pub fn check_query(query: &Query) -> Check {
    match query {
        Query::Select { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => graph_pattern(pattern),
        Query::Construct {
            template,
            graph_template,
            pattern,
            ..
        } => {
            template.iter().try_for_each(triple_pattern)?;
            graph_template.iter().try_for_each(quad_pattern)?;
            graph_pattern(pattern)
        }
    }
}

/// The message for the first RDF 1.2 feature of the update that maplib does not support yet.
pub fn check_update(update: &Update) -> Check {
    for op in &update.operations {
        match op {
            GraphUpdateOperation::InsertData { data } => data.iter().try_for_each(quad)?,
            GraphUpdateOperation::DeleteData { data } => data.iter().try_for_each(ground_quad)?,
            GraphUpdateOperation::DeleteInsert {
                delete,
                insert,
                pattern,
                ..
            } => {
                delete.iter().try_for_each(ground_quad_pattern)?;
                insert.iter().try_for_each(quad_pattern)?;
                graph_pattern(pattern)?;
            }
            GraphUpdateOperation::Load { .. }
            | GraphUpdateOperation::Clear { .. }
            | GraphUpdateOperation::Create { .. }
            | GraphUpdateOperation::Drop { .. } => {}
        }
    }
    Ok(())
}

fn literal(l: &Literal) -> Check {
    if has_base_direction(l) {
        Err(BASE_DIRECTION_NOT_SUPPORTED)
    } else {
        Ok(())
    }
}

fn term(t: &Term) -> Check {
    match t {
        Term::NamedNode(_) | Term::BlankNode(_) => Ok(()),
        Term::Literal(l) => literal(l),
        Term::Triple(_) => Err(TRIPLE_TERMS_NOT_SUPPORTED),
    }
}

fn ground_term(t: &GroundTerm) -> Check {
    match t {
        GroundTerm::NamedNode(_) => Ok(()),
        GroundTerm::Literal(l) => literal(l),
        GroundTerm::Triple(_) => Err(TRIPLE_TERMS_NOT_SUPPORTED),
    }
}

fn term_pattern(t: &TermPattern) -> Check {
    match t {
        TermPattern::NamedNode(_) | TermPattern::BlankNode(_) | TermPattern::Variable(_) => Ok(()),
        TermPattern::Literal(l) => literal(l),
        TermPattern::Triple(_) => Err(TRIPLE_TERMS_NOT_SUPPORTED),
    }
}

fn ground_term_pattern(t: &GroundTermPattern) -> Check {
    match t {
        GroundTermPattern::NamedNode(_) | GroundTermPattern::Variable(_) => Ok(()),
        GroundTermPattern::Literal(l) => literal(l),
        GroundTermPattern::Triple(_) => Err(TRIPLE_TERMS_NOT_SUPPORTED),
    }
}

fn triple_pattern(tp: &TriplePattern) -> Check {
    term_pattern(&tp.subject)?;
    term_pattern(&tp.object)
}

fn quad_pattern(qp: &QuadPattern) -> Check {
    term_pattern(&qp.subject)?;
    term_pattern(&qp.object)
}

fn ground_quad_pattern(qp: &GroundQuadPattern) -> Check {
    ground_term_pattern(&qp.subject)?;
    ground_term_pattern(&qp.object)
}

fn quad(q: &Quad) -> Check {
    term(&q.object)
}

fn ground_quad(q: &GroundQuad) -> Check {
    ground_term(&q.object)
}

/// A term pattern of a triple pattern of a basic graph pattern, where triple term patterns are
/// supported.
fn bgp_term_pattern(t: &TermPattern) -> Check {
    match t {
        TermPattern::Triple(tp) => {
            bgp_term_pattern(&tp.subject)?;
            bgp_term_pattern(&tp.object)
        }
        t => term_pattern(t),
    }
}

fn graph_pattern(gp: &GraphPattern) -> Check {
    match gp {
        GraphPattern::Bgp { patterns } => patterns.iter().try_for_each(|tp| {
            bgp_term_pattern(&tp.subject)?;
            bgp_term_pattern(&tp.object)
        }),
        GraphPattern::Path {
            subject,
            path,
            object,
        } => {
            term_pattern(subject)?;
            property_path(path)?;
            term_pattern(object)
        }
        GraphPattern::Join { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            graph_pattern(left)?;
            graph_pattern(right)
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            graph_pattern(left)?;
            graph_pattern(right)?;
            expression.as_ref().map_or(Ok(()), self::expression)
        }
        GraphPattern::Filter { expr, inner } => {
            expression(expr)?;
            graph_pattern(inner)
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            self::expression(expression)?;
            graph_pattern(inner)
        }
        GraphPattern::Values { bindings, .. } => bindings
            .iter()
            .flatten()
            .flatten()
            .try_for_each(ground_term),
        // The values of a PValues pattern are given as parameters, which are DataFrames.
        GraphPattern::PValues { .. } => Ok(()),
        GraphPattern::OrderBy { inner, expression } => {
            expression.iter().try_for_each(order_expression)?;
            graph_pattern(inner)
        }
        GraphPattern::Group {
            inner, aggregates, ..
        } => {
            aggregates
                .iter()
                .try_for_each(|(_, a)| aggregate_expression(a))?;
            graph_pattern(inner)
        }
        GraphPattern::Graph { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Service { inner, .. } => graph_pattern(inner),
    }
}

fn property_path(p: &PropertyPathExpression) -> Check {
    match p {
        PropertyPathExpression::NamedNode(_) | PropertyPathExpression::NegatedPropertySet(_) => {
            Ok(())
        }
        PropertyPathExpression::Reverse(p)
        | PropertyPathExpression::ZeroOrMore(p)
        | PropertyPathExpression::OneOrMore(p)
        | PropertyPathExpression::ZeroOrOne(p) => property_path(p),
        PropertyPathExpression::Sequence(a, b) | PropertyPathExpression::Alternative(a, b) => {
            property_path(a)?;
            property_path(b)
        }
    }
}

fn order_expression(o: &OrderExpression) -> Check {
    match o {
        OrderExpression::Asc(e) | OrderExpression::Desc(e) => expression(e),
    }
}

fn aggregate_expression(a: &AggregateExpression) -> Check {
    match a {
        AggregateExpression::CountSolutions { .. } => Ok(()),
        AggregateExpression::FunctionCall { expr, .. } => expression(expr),
    }
}

fn expression(e: &Expression) -> Check {
    match e {
        Expression::NamedNode(_) | Expression::Variable(_) | Expression::Bound(_) => Ok(()),
        Expression::Literal(l) => literal(l),
        Expression::Or(a, b)
        | Expression::And(a, b)
        | Expression::Equal(a, b)
        | Expression::SameTerm(a, b)
        | Expression::Greater(a, b)
        | Expression::GreaterOrEqual(a, b)
        | Expression::Less(a, b)
        | Expression::LessOrEqual(a, b)
        | Expression::Add(a, b)
        | Expression::Subtract(a, b)
        | Expression::Multiply(a, b)
        | Expression::Divide(a, b) => {
            expression(a)?;
            expression(b)
        }
        Expression::In(a, list) => {
            expression(a)?;
            list.iter().try_for_each(expression)
        }
        Expression::UnaryPlus(a) | Expression::UnaryMinus(a) | Expression::Not(a) => expression(a),
        Expression::Exists(gp) => graph_pattern(gp),
        Expression::If(a, b, c) => {
            expression(a)?;
            expression(b)?;
            expression(c)
        }
        Expression::Coalesce(list) => list.iter().try_for_each(expression),
        Expression::FunctionCall(f, args) => {
            match f {
                Function::Triple
                | Function::Subject
                | Function::Predicate
                | Function::Object
                | Function::IsTriple => return Err(TRIPLE_TERMS_NOT_SUPPORTED),
                Function::LangDir
                | Function::HasLang
                | Function::HasLangDir
                | Function::StrLangDir => return Err(BASE_DIRECTION_NOT_SUPPORTED),
                _ => {}
            }
            args.iter().try_for_each(expression)
        }
    }
}

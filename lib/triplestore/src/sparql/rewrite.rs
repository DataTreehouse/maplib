use crate::sparql::rewrite::rewrite_cse::rewrite_gp_cse;
use crate::sparql::rewrite::rewrite_pushdown::rewrite_gp_pushdown;
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::HashSet;

mod rewrite_cse;
mod rewrite_pushdown;

pub fn rewrite(q: Query) -> Query {
    let q = match q {
        Query::Select {
            dataset,
            pattern,
            base_iri,
        } => Query::Select {
            dataset,
            pattern: rewrite_gp(pattern),
            base_iri,
        },
        Query::Construct {
            template,
            dataset,
            pattern,
            base_iri,
        } => Query::Construct {
            template,
            dataset,
            pattern: rewrite_gp(pattern),
            base_iri,
        },
        Query::Describe {
            dataset,
            pattern,
            base_iri,
        } => Query::Describe {
            dataset,
            pattern: rewrite_gp(pattern),
            base_iri,
        },
        Query::Ask {
            dataset,
            pattern,
            base_iri,
        } => Query::Ask {
            dataset,
            pattern: rewrite_gp(pattern),
            base_iri,
        },
    };
    q
}

pub fn rewrite_gp(pattern: GraphPattern) -> GraphPattern {
    let mut gp = rewrite_gp_pushdown(pattern, vec![], HashSet::new());
    gp = rewrite_gp_cse(gp);
    gp
}

use oxrdf::{Term, Variable};
use polars::frame::UniqueKeepStrategy;
use polars::prelude::IntoLazy;
use representation::multitype::unique_workaround;

use crate::sparql::QueryResult;
use crate::Triplestore;
use representation::polars_to_rdf::{df_as_result, QuerySolutions};

pub fn query_select(query: &str, triplestore: &Triplestore, deduplicate: bool) -> QuerySolutions {
    let qres = triplestore.query_deduplicated(query, &None).unwrap();

    let (df, types) = if let QueryResult::Select(mut df, types) = qres {
        if deduplicate {
            let mut lf = df.lazy();
            lf = unique_workaround(lf, &types, None, false, UniqueKeepStrategy::Any);
            df = lf.collect().unwrap();
        }
        (df, types)
    } else {
        panic!("Should never happen")
    };
    df_as_result(df, &types)
}

pub fn get_seven_query_solutions(
    query_solution: Vec<Option<Term>>,
    variables: &Vec<Variable>,
    first: &str,
    second: &str,
    third: &str,
    fourth: &str,
    fifth: &str,
    sixth: &str,
    seven: &str,
) -> (
    Option<Term>,
    Option<Term>,
    Option<Term>,
    Option<Term>,
    Option<Term>,
    Option<Term>,
    Option<Term>,
) {
    let mut s1 = None;
    let mut s2 = None;
    let mut s3 = None;
    let mut s4 = None;
    let mut s5 = None;
    let mut s6 = None;
    let mut s7 = None;
    for (var, val) in variables.iter().zip(query_solution.into_iter()) {
        if var.as_str() == first {
            s1 = val;
        } else if var.as_str() == second {
            s2 = val;
        } else if var.as_str() == third {
            s3 = val;
        } else if var.as_str() == fourth {
            s4 = val;
        } else if var.as_str() == fifth {
            s5 = val;
        } else if var.as_str() == sixth {
            s6 = val;
        } else if var.as_str() == seven {
            s7 = val;
        } else {
            panic!("var {:?}, val {:?}", var, val);
        }
    }
    (s1, s2, s3, s4, s5, s6, s7)
}

pub fn get_four_query_solutions(
    query_solution: Vec<Option<Term>>,
    variables: &Vec<Variable>,
    first: &str,
    second: &str,
    third: &str,
    fourth: &str,
) -> (Option<Term>, Option<Term>, Option<Term>, Option<Term>) {
    let mut s1 = None;
    let mut s2 = None;
    let mut s3 = None;
    let mut s4 = None;
    for (var, val) in variables.iter().zip(query_solution.into_iter()) {
        if var.as_str() == first {
            s1 = val;
        } else if var.as_str() == second {
            s2 = val;
        } else if var.as_str() == third {
            s3 = val;
        } else if var.as_str() == fourth {
            s4 = val;
        } else {
            panic!("var {:?}, val {:?}", var, val);
        }
    }
    (s1, s2, s3, s4)
}

pub fn get_three_query_solutions(
    query_solution: Vec<Option<Term>>,
    variables: &Vec<Variable>,
    first: &str,
    second: &str,
    third: &str,
) -> (Option<Term>, Option<Term>, Option<Term>) {
    let mut s1 = None;
    let mut s2 = None;
    let mut s3 = None;
    for (var, val) in variables.iter().zip(query_solution.into_iter()) {
        if var.as_str() == first {
            s1 = val;
        } else if var.as_str() == second {
            s2 = val;
        } else if var.as_str() == third {
            s3 = val;
        } else {
            panic!("var {:?}, val {:?}", var, val);
        }
    }
    (s1, s2, s3)
}

pub fn get_two_query_solutions(
    query_solution: Vec<Option<Term>>,
    variables: &Vec<Variable>,
    first: &str,
    second: &str,
) -> (Option<Term>, Option<Term>) {
    let mut s1 = None;
    let mut s2 = None;
    for (var, val) in variables.iter().zip(query_solution.into_iter()) {
        if var.as_str() == first {
            s1 = val;
        } else if var.as_str() == second {
            s2 = val;
        } else {
            panic!("var {:?}, val {:?}", var, val);
        }
    }
    (s1, s2)
}

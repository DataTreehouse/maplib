use oxrdf::vocab::xsd;
use polars::datatypes::DataType;
use polars::prelude::{all, col, Expr};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::RDFNodeType;

pub struct AggregateReturn {
    pub solution_mappings: SolutionMappings,
    pub expr: Expr,
    pub context: Option<Context>,
    pub rdf_node_type: RDFNodeType,
}

pub fn count_with_expression(column_context: &Context, distinct: bool) -> (Expr, RDFNodeType) {
    let out_expr = if distinct {
        col(column_context.as_str()).n_unique()
    } else {
        col(column_context.as_str()).count()
    };
    (
        out_expr,
        RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
    )
}

pub fn count_without_expression(distinct: bool) -> (Expr, RDFNodeType) {
    let columns_expr = all();
    let out_expr = if distinct {
        columns_expr.n_unique()
    } else {
        columns_expr.len()
    };
    (
        out_expr,
        RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
    )
}

pub fn sum(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
    distinct: bool,
) -> (Expr, RDFNodeType) {
    let expr_rdf_node_type = rdf_node_type_from_context(column_context, solution_mappings);
    let out_rdf_node_type = if expr_rdf_node_type.is_bool() {
        RDFNodeType::Literal(xsd::UNSIGNED_LONG.into_owned())
    } else {
        expr_rdf_node_type.clone()
    };

    let out_expr = if distinct {
        col(column_context.as_str()).unique().sum()
    } else {
        col(column_context.as_str()).sum()
    };
    (out_expr, out_rdf_node_type)
}

pub fn avg(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
    distinct: bool,
) -> (Expr, RDFNodeType) {
    let expr_rdf_node_type = rdf_node_type_from_context(column_context, solution_mappings);
    let out_rdf_node_type = if expr_rdf_node_type.is_bool() {
        RDFNodeType::Literal(xsd::UNSIGNED_LONG.into_owned())
    } else {
        expr_rdf_node_type.clone()
    };

    let out_expr = if distinct {
        col(column_context.as_str()).unique().mean()
    } else {
        col(column_context.as_str()).mean()
    };
    (out_expr, out_rdf_node_type)
}

pub fn min(solution_mappings: &SolutionMappings, column_context: &Context) -> (Expr, RDFNodeType) {
    let expr_rdf_node_type = rdf_node_type_from_context(column_context, solution_mappings);
    let out_rdf_node_type = if expr_rdf_node_type.is_bool() {
        RDFNodeType::Literal(xsd::UNSIGNED_LONG.into_owned())
    } else {
        expr_rdf_node_type.clone()
    };

    let out_expr = col(column_context.as_str()).min();

    (out_expr, out_rdf_node_type)
}

pub fn max(solution_mappings: &SolutionMappings, column_context: &Context) -> (Expr, RDFNodeType) {
    let expr_rdf_node_type = rdf_node_type_from_context(column_context, solution_mappings);
    let out_rdf_node_type = if expr_rdf_node_type.is_bool() {
        RDFNodeType::Literal(xsd::UNSIGNED_LONG.into_owned())
    } else {
        expr_rdf_node_type.clone()
    };

    let out_expr = col(column_context.as_str()).max();

    (out_expr, out_rdf_node_type)
}

pub fn group_concat(
    column_context: &Context,
    separator: &Option<String>,
    distinct: bool,
) -> (Expr, RDFNodeType) {
    let out_rdf_node_type = RDFNodeType::Literal(xsd::STRING.into_owned());

    let use_sep = if let Some(sep) = separator {
        sep.to_string()
    } else {
        "".to_string()
    };
    let out_expr = if distinct {
        col(column_context.as_str())
            .cast(DataType::String)
            .unique_stable()
            .str()
            .join(use_sep.as_str(), true)
    } else {
        col(column_context.as_str())
            .cast(DataType::String)
            .str()
            .join(use_sep.as_str(), true)
    };
    (out_expr, out_rdf_node_type)
}

pub fn sample(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
) -> (Expr, RDFNodeType) {
    let out_rdf_node_type = rdf_node_type_from_context(column_context, solution_mappings).clone();

    let out_expr = col(column_context.as_str()).first();
    (out_expr, out_rdf_node_type)
}

fn rdf_node_type_from_context<'a>(
    context: &'_ Context,
    solution_mappings: &'a SolutionMappings,
) -> &'a RDFNodeType {
    let datatype = solution_mappings
        .rdf_node_types
        .get(context.as_str())
        .unwrap();
    datatype
}

pub fn list_aggregation(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
) -> (Expr, RDFNodeType) {
    //Todo: add list types..
    let out_rdf_node_type = rdf_node_type_from_context(column_context, solution_mappings).clone();
    let out_expr = col(column_context.as_str());
    (out_expr, out_rdf_node_type)
}

use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::query_context::Context;
use crate::sparql::solution_mapping::SolutionMappings;
use oxrdf::Variable;
use polars::prelude::{col, Expr, IntoLazy, JoinArgs, JoinType};
use polars_core::datatypes::AnyValue;
use polars_core::frame::DataFrame;
use polars_core::series::Series;
use representation::literals::sparql_literal_to_any_value;
use representation::RDFNodeType;
use spargebra::term::GroundTerm;
use std::collections::HashMap;

impl Triplestore {
    pub(crate) fn lazy_values(
        &self,
        solution_mappings: Option<SolutionMappings>,
        variables: &Vec<Variable>,
        bindings: &Vec<Vec<Option<GroundTerm>>>,
        _context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        let mut col_vecs = HashMap::new();
        for i in 0..variables.len() {
            col_vecs.insert(i, vec![]);
        }
        let mut datatypes = HashMap::new();
        for (i, row) in bindings.iter().enumerate() {
            for (j, col) in row.iter().enumerate() {
                if let Some(gt) = col {
                    #[allow(unreachable_patterns)]
                    match gt {
                        GroundTerm::NamedNode(nn) => {
                            if i == 0 {
                                datatypes.insert(j, RDFNodeType::IRI);
                            } else if datatypes.get(&j).unwrap() != &RDFNodeType::IRI {
                                todo!("No support yet for values of same variables having different types")
                            }
                            col_vecs
                                .get_mut(&j)
                                .unwrap()
                                .push(AnyValue::Utf8Owned(nn.as_str().into()));
                        }
                        GroundTerm::Literal(lit) => {
                            let dt = lit.datatype();
                            if i == 0 {
                                datatypes.insert(j, RDFNodeType::Literal(dt.into_owned()));
                            } else {
                                let existing = datatypes.get(&j).unwrap();
                                match existing {
                                    RDFNodeType::Literal(l) => {
                                        if l != &dt {
                                            todo!("No support for values of some varibales having different types")
                                        }
                                    }
                                    _ => {
                                        todo!("No support for values of some varibales having different types")
                                    }
                                }
                            }
                            let value = lit.value().to_string();
                            let (mut polarlit, _) = sparql_literal_to_any_value(&value, &Some(dt));
                            polarlit = polarlit.into_static().unwrap();
                            col_vecs.get_mut(&j).unwrap().push(polarlit);
                        }
                        _ => {
                            unimplemented!()
                        }
                    }
                    if i + 1 == bindings.len() {
                        datatypes.entry(j).or_insert(RDFNodeType::None);
                    }
                    col_vecs.get_mut(&j).unwrap().push(AnyValue::Null);
                }
            }
        }
        let mut all_series = vec![];
        for (i, var) in variables.iter().enumerate() {
            let series =
                Series::from_any_values(var.as_str(), col_vecs.get(&i).unwrap().as_slice(), false)
                    .unwrap();
            all_series.push(series);
        }
        let df = DataFrame::new(all_series).unwrap();
        if let Some(mut mappings) = solution_mappings {
            let join_on: Vec<String> = variables
                .iter()
                .map(|x| x.as_str().to_string())
                .filter(|v| mappings.columns.contains(v))
                .collect();
            let join_cols: Vec<Expr> = join_on.iter().map(|x| col(x)).collect();
            for v in variables {
                mappings.columns.insert(v.as_str().to_string());
            }
            for (k, v) in datatypes {
                let var = variables.get(k).unwrap();
                mappings.rdf_node_types.insert(var.as_str().to_string(), v);
            }
            if join_on.is_empty() {
                mappings.mappings = mappings.mappings.join(
                    df.lazy(),
                    join_cols.as_slice(),
                    join_cols.as_slice(),
                    JoinArgs::new(JoinType::Cross),
                );
            } else {
                mappings.mappings = mappings.mappings.join(
                    df.lazy(),
                    join_cols.as_slice(),
                    join_cols.as_slice(),
                    JoinArgs::new(JoinType::Inner),
                );
            }
            Ok(mappings)
        } else {
            let mut out_datatypes = HashMap::new();
            for (k, v) in datatypes {
                let var = variables.get(k).unwrap();
                out_datatypes.insert(var.as_str().to_string(), v);
            }
            Ok(SolutionMappings::new(
                df.lazy(),
                variables.iter().map(|x| x.as_str().to_string()).collect(),
                out_datatypes,
            ))
        }
    }
}

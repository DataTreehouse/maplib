use super::Triplestore;
use crate::sparql::errors::SparqlError;
use oxrdf::Variable;
use polars::prelude::{DataFrame, IntoLazy, JoinType};
use query_processing::graph_patterns::join;
use representation::query_context::Context;
use representation::rdf_to_polars::{
    polars_literal_values_to_series, rdf_literal_to_polars_literal_value,
    rdf_named_node_to_polars_literal_value,
};
use representation::solution_mapping::SolutionMappings;
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
                    let t = match gt {
                        GroundTerm::NamedNode(nn) => {
                            if i == 0 {
                                datatypes.insert(j, RDFNodeType::IRI);
                            } else if datatypes.get(&j).unwrap() != &RDFNodeType::IRI {
                                todo!("No support yet for values of same variables having different types")
                            }
                            rdf_named_node_to_polars_literal_value(nn)
                        }
                        GroundTerm::Literal(l) => {
                            let dt = l.datatype();
                            if i == 0 {
                                datatypes.insert(j, RDFNodeType::Literal(dt.into_owned()));
                            } else {
                                let existing = datatypes.get(&j).unwrap();
                                match existing {
                                    RDFNodeType::Literal(l) => {
                                        if l != &dt {
                                            todo!("No support for values of some variables having different types")
                                        }
                                    }
                                    _ => {
                                        todo!("No support for values of some variables having different types")
                                    }
                                }
                            }
                            rdf_literal_to_polars_literal_value(l)
                        }
                    };
                    col_vecs.get_mut(&j).unwrap().push(t);
                    if i + 1 == bindings.len() {
                        datatypes.entry(j).or_insert(RDFNodeType::None);
                    }
                }
            }
        }
        let mut all_series = vec![];
        for (i, var) in variables.iter().enumerate() {
            let series =
                polars_literal_values_to_series(col_vecs.remove(&i).unwrap(), var.as_str());
            all_series.push(series);
        }
        let df = DataFrame::new(all_series).unwrap();
        let mut rdf_node_types = HashMap::new();
        for (k, v) in datatypes {
            let var = variables.get(k).unwrap();
            rdf_node_types.insert(var.as_str().to_string(), v);
        }
        let sm = SolutionMappings {
            mappings: df.lazy(),
            rdf_node_types,
        };

        if let Some(mut mappings) = solution_mappings {
            mappings = join(mappings, sm, JoinType::Inner)?;
            Ok(mappings)
        } else {
            Ok(sm)
        }
    }
}

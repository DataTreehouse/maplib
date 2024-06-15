use crate::multitype::{MULTI_BLANK_DT, MULTI_IRI_DT};
use crate::{BaseRDFNodeType, RDFNodeType};
use polars::prelude::{col, lit, LazyFrame};
use std::collections::HashMap;

pub fn format_iris_and_blank_nodes(
    mut lf: LazyFrame,
    rdf_node_types: &HashMap<String, RDFNodeType>,
    include_multicolumns: bool,
) -> LazyFrame {
    for (c, dt) in rdf_node_types {
        match dt {
            RDFNodeType::IRI => {
                lf = lf.with_column((lit("<") + col(c) + lit(">")).alias(c));
            }
            RDFNodeType::BlankNode => {
                lf = lf.with_column((lit("_:") + col(c)).alias(c));
            }
            RDFNodeType::MultiType(ts) => {
                if include_multicolumns {
                    if ts.contains(&BaseRDFNodeType::IRI) {
                        lf = lf.with_column(
                            col(c)
                                .struct_()
                                .with_fields(vec![(lit("<")
                                    + col(c).struct_().field_by_name(MULTI_IRI_DT)
                                    + lit(">"))
                                .alias(MULTI_IRI_DT)])
                                .alias(c),
                        );
                    }
                    if ts.contains(&BaseRDFNodeType::BlankNode) {
                        lf = lf.with_column(
                            col(c)
                                .struct_()
                                .with_fields(vec![(lit("_:")
                                    + col(c).struct_().field_by_name(MULTI_BLANK_DT))
                                .alias(MULTI_BLANK_DT)])
                                .alias(c),
                        );
                    }
                }
            }
            _ => {}
        }
    }
    lf
}

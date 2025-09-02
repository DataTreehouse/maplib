use oxrdf::{Term, Variable};
use polars::datatypes::PlSmallStr;
use polars::frame::DataFrame;
use polars::prelude::{as_struct, col, IntoColumn, IntoLazy};
use representation::polars_to_rdf::particular_opt_term_vec_to_series;
use representation::solution_mapping::EagerSolutionMappings;
use representation::{
    get_ground_term_datatype_ref, BaseRDFNodeTypeRef, RDFNodeState, LANG_STRING_LANG_FIELD,
    LANG_STRING_VALUE_FIELD,
};
use spargebra::term::GroundTerm;
use std::collections::HashMap;

pub fn values_pattern(
    variables: &[Variable],
    bindings: &[Vec<Option<GroundTerm>>],
) -> EagerSolutionMappings {
    let mut variable_datatype_opt_term_vecs: HashMap<usize, HashMap<BaseRDFNodeTypeRef, Vec<_>>> =
        HashMap::new();
    // Todo: this could be parallel.. but we have very small data here..
    for i in 0..variables.len() {
        variable_datatype_opt_term_vecs.insert(i, HashMap::new());
    }
    for (i, row) in bindings.iter().enumerate() {
        for (j, col) in row.iter().enumerate() {
            let map = variable_datatype_opt_term_vecs.get_mut(&j).unwrap();
            if let Some(gt) = col {
                let dt = get_ground_term_datatype_ref(gt);
                {
                    let vector = if let Some(vector) = map.get_mut(&dt) {
                        vector
                    } else {
                        map.insert(dt.clone(), std::iter::repeat_n(None, i).collect());
                        map.get_mut(&dt).unwrap()
                    };
                    //TODO: Stop copying data here!!
                    #[allow(unreachable_patterns)]
                    let term = match gt {
                        GroundTerm::NamedNode(nn) => Term::NamedNode(nn.clone()),
                        GroundTerm::Literal(l) => Term::Literal(l.clone()),
                        _ => unimplemented!(),
                    };
                    vector.push(Some(term));
                }
                for (k, v) in &mut *map {
                    if k != &dt {
                        v.push(None);
                    }
                }
            } else {
                for v in (*map).values_mut() {
                    v.push(None);
                }
            }
        }
    }

    let mut all_columns = vec![];
    let mut all_datatypes = HashMap::new();
    for (i, m) in variable_datatype_opt_term_vecs {
        let mut types_columns = vec![];
        for (t, v) in m {
            types_columns.push((
                t.clone().into_owned(),
                particular_opt_term_vec_to_series(v, t, "c").into_column(),
            ));
        }
        types_columns.sort_unstable_by(|(x, _), (y, _)| x.cmp(y));

        let (dt, column) = if types_columns.len() > 1 {
            let mut struct_exprs = vec![];
            let mut columns = vec![];
            let mut types = vec![];
            for (i, (t, mut c)) in types_columns.into_iter().enumerate() {
                let name = format!("c{i}");
                c.rename(PlSmallStr::from_str(&name));
                let tname = t.field_col_name();
                if t.is_lang_string() {
                    struct_exprs.push(
                        col(&name)
                            .struct_()
                            .field_by_name(LANG_STRING_VALUE_FIELD)
                            .alias(LANG_STRING_VALUE_FIELD),
                    );
                    struct_exprs.push(
                        col(&name)
                            .struct_()
                            .field_by_name(LANG_STRING_LANG_FIELD)
                            .alias(LANG_STRING_LANG_FIELD),
                    );
                } else {
                    struct_exprs.push(col(&name).alias(tname));
                }

                columns.push(c);
                types.push(t);
            }
            let t = RDFNodeState::from_map(
                types
                    .into_iter()
                    .map(|x| {
                        let cat_state = x.default_input_cat_state();
                        (x, cat_state)
                    })
                    .collect(),
            );

            let mut df = DataFrame::new(columns)
                .unwrap()
                .lazy()
                .with_column(as_struct(struct_exprs).alias("struct"))
                .select([col("struct")])
                .rename(["struct"], [variables.get(i).unwrap().as_str()], true)
                .collect()
                .unwrap();
            let column = df
                .drop_in_place(variables.get(i).unwrap().as_str())
                .unwrap();
            (t, column)
        } else {
            let (t, mut column) = types_columns.pop().unwrap();
            column.rename(PlSmallStr::from_str(variables.get(i).unwrap().as_str()));
            (t.into_default_input_rdf_node_state(), column)
        };
        all_columns.push(column);
        all_datatypes.insert(variables.get(i).unwrap().as_str().to_string(), dt);
    }
    let varexpr: Vec<_> = variables.iter().map(|x| col(x.as_str())).collect();
    let df = DataFrame::new(all_columns)
        .unwrap()
        .lazy()
        .select(varexpr)
        .collect()
        .unwrap();
    EagerSolutionMappings::new(df, all_datatypes)
}

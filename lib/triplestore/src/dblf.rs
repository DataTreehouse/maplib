use super::{Triples, Triplestore};
use crate::sparql::errors::SparqlError;
use oxrdf::{NamedNode, Subject, Term};
use polars::prelude::{col, concat, lit, Expr, IntoLazy, LazyFrame, UnionArgs};
use polars_core::prelude::{Column, DataFrame};
use query_processing::graph_patterns::union;
use representation::multitype::convert_lf_col_to_multitype;
use representation::rdf_to_polars::{
    polars_literal_values_to_series, rdf_named_node_to_polars_literal_value,
    rdf_term_to_polars_expr,
};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashMap;

impl Triplestore {
    pub fn get_predicate_iris(&self) -> Vec<NamedNode> {
        let mut iris = vec![];
        for nn in self.triples_map.keys() {
            iris.push(nn.clone());
        }
        iris
    }

    pub fn get_predicate_eager_solution_mappings(
        &mut self,
        predicate: &NamedNode,
    ) -> Result<Vec<EagerSolutionMappings>, SparqlError> {
        let mut types = vec![];
        let mut out = vec![];
        if let Some(map) = self.triples_map.get(predicate) {
            for (subject_type, object_type) in map.keys() {
                types.push((subject_type.clone(), object_type.clone()));
            }
        }
        for (subject_type, object_type) in &types {
            let sm = self.get_deduplicated_predicate_lf(
                predicate,
                &Some(SUBJECT_COL_NAME.to_string()),
                &None,
                &Some(OBJECT_COL_NAME.to_string()),
                &None,
                None,
                Some(subject_type),
                Some(object_type),
            )?;
            let eager_sm = sm.as_eager();
            out.push(eager_sm);
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn get_deduplicated_predicate_lf(
        &mut self,
        verb_uri: &NamedNode,
        subject_keep_rename: &Option<String>,
        verb_keep_rename: &Option<String>,
        object_keep_rename: &Option<String>,
        subjects: &Option<Vec<Subject>>,
        object_filter: Option<Term>,
        subject_datatype_req: Option<&BaseRDFNodeType>,
        object_datatype_req: Option<&BaseRDFNodeType>,
    ) -> Result<SolutionMappings, SparqlError> {
        if let Some(m) = self.triples_map.get_mut(verb_uri) {
            if m.is_empty() {
                panic!("Empty map should never happen");
            }
            if let Some(SolutionMappings {
                mappings: mut lf,
                mut rdf_node_types,
                height_upper_bound,
            }) = multiple_tt_to_deduplicated_lf(
                m,
                self.transient_triples_map.get_mut(verb_uri),
                subject_datatype_req,
                object_datatype_req,
                subjects,
                object_filter,
                &self.caching_folder,
            )? {
                let mut out_datatypes = HashMap::new();
                let use_subject_col_name = uuid::Uuid::new_v4().to_string();
                let use_object_col_name = uuid::Uuid::new_v4().to_string();
                lf = lf.rename(
                    [SUBJECT_COL_NAME, OBJECT_COL_NAME],
                    [&use_subject_col_name, &use_object_col_name],
                    true,
                );

                let mut drop = vec![];
                if let Some(renamed) = subject_keep_rename {
                    lf = lf.rename([&use_subject_col_name], [renamed], true);
                    out_datatypes.insert(
                        renamed.to_string(),
                        rdf_node_types.remove(SUBJECT_COL_NAME).unwrap(),
                    );
                } else {
                    drop.push(use_subject_col_name);
                }
                if let Some(renamed) = object_keep_rename {
                    lf = lf.rename([&use_object_col_name], [renamed], true);
                    out_datatypes.insert(
                        renamed.to_string(),
                        rdf_node_types.remove(OBJECT_COL_NAME).unwrap(),
                    );
                } else {
                    drop.push(use_object_col_name)
                }
                if let Some(renamed) = verb_keep_rename {
                    lf = lf.with_column(
                        lit(rdf_named_node_to_polars_literal_value(verb_uri)).alias(renamed),
                    );
                    out_datatypes.insert(renamed.clone(), RDFNodeType::IRI);
                }
                lf = lf.drop(drop);
                Ok(SolutionMappings::new(lf, out_datatypes, height_upper_bound))
            } else {
                Ok(create_empty_lf_datatypes(
                    subject_keep_rename,
                    verb_keep_rename,
                    object_keep_rename,
                    object_datatype_req,
                ))
            }
        } else {
            Ok(create_empty_lf_datatypes(
                subject_keep_rename,
                verb_keep_rename,
                object_keep_rename,
                object_datatype_req,
            ))
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn get_predicates_lf(
        &mut self,
        predicate_uris: Option<Vec<NamedNode>>,
        subject_keep_rename: &Option<String>,
        verb_keep_rename: &Option<String>,
        object_keep_rename: &Option<String>,
        subjects: &Option<Vec<Subject>>,
        object_term: Option<Term>,
        object_datatype_req: Option<&BaseRDFNodeType>,
    ) -> Result<SolutionMappings, SparqlError> {
        let predicate_uris = predicate_uris.unwrap_or(self.all_predicates());
        let mut solution_mappings = vec![];

        let need_multi_subject =
            self.partial_check_need_multi(&predicate_uris, object_datatype_req, true);
        let need_multi_object =
            self.partial_check_need_multi(&predicate_uris, object_datatype_req, false);

        for v in predicate_uris {
            let SolutionMappings {
                    mappings: mut lf,
                    rdf_node_types: mut datatypes_map,
                    height_upper_bound,
                }
            = self.get_deduplicated_predicate_lf(
                &v,
                subject_keep_rename,
                verb_keep_rename,
                object_keep_rename,
                subjects,
                object_term.clone(),
                None, //TODO!
                object_datatype_req,
            )?;
            if let Some(subj_col) = subject_keep_rename {
                if height_upper_bound > 0
                    && need_multi_subject
                    && !matches!(
                        datatypes_map.get(subj_col).unwrap(),
                        &RDFNodeType::MultiType(..)
                    )
                {
                    lf = lf.with_column(convert_lf_col_to_multitype(
                        subj_col,
                        datatypes_map.get(subj_col).unwrap(),
                    ));
                    let existing_type =
                        BaseRDFNodeType::from_rdf_node_type(datatypes_map.get(subj_col).unwrap());
                    datatypes_map.insert(
                        subj_col.clone(),
                        RDFNodeType::MultiType(vec![existing_type]),
                    );
                }
            }

            if let Some(obj_col) = object_keep_rename {
                if height_upper_bound > 0
                    && need_multi_object
                    && !matches!(
                        datatypes_map.get(obj_col).unwrap(),
                        &RDFNodeType::MultiType(..)
                    )
                {
                    lf = lf.with_column(convert_lf_col_to_multitype(
                        obj_col,
                        datatypes_map.get(obj_col).unwrap(),
                    ));
                    let existing_type =
                        BaseRDFNodeType::from_rdf_node_type(datatypes_map.get(obj_col).unwrap());
                    datatypes_map
                        .insert(obj_col.clone(), RDFNodeType::MultiType(vec![existing_type]));
                }
            }

            if height_upper_bound > 0 {
                solution_mappings.push(SolutionMappings::new(lf, datatypes_map, height_upper_bound));
            }
        }
        Ok(if !solution_mappings.is_empty() {
            let sm = union(solution_mappings, false)?;
            sm
        } else {
            create_empty_lf_datatypes(
                subject_keep_rename,
                verb_keep_rename,
                object_keep_rename,
                object_datatype_req,
            )
        })
    }

    pub(crate) fn all_predicates(&self) -> Vec<NamedNode> {
        let mut predicates = Vec::new();
        for k in self.triples_map.keys() {
            predicates.push(k.clone());
        }
        predicates
    }

    fn partial_check_need_multi(
        &self,
        predicates: &Vec<NamedNode>,
        object_datatype_req: Option<&BaseRDFNodeType>,
        subject: bool,
    ) -> bool {
        if !subject && object_datatype_req.is_some() {
            return false;
        }

        let mut first_datatype = None;
        for p in predicates {
            if let Some(tt_map) = self.triples_map.get(p) {
                for (subject_dt, object_dt) in tt_map.keys() {
                    let use_dt = if subject { subject_dt } else { object_dt };
                    if let Some(first) = &first_datatype {
                        if first != &use_dt {
                            return true;
                        }
                    } else {
                        first_datatype = Some(use_dt);
                    }
                }
            }
        }
        false
    }
}

fn single_tt_to_deduplicated_lf(
    tt: &mut Triples,
    caching_folder: &Option<String>,
    subjects: &Option<Vec<Subject>>,
) -> Result<(LazyFrame, usize), SparqlError> {
    if !tt.unique {
        tt.deduplicate(caching_folder)
            .map_err(SparqlError::DeduplicationError)?;
    }
    assert!(tt.unique, "Should be deduplicated");
    let lfs_and_heights = tt.get_lazy_frames(subjects).map_err(SparqlError::TripleTableReadError)?;
    let mut lfs = vec![];
    let mut new_height = 0usize;
    for (lf, height) in lfs_and_heights {
        lfs.push(lf);
        new_height = new_height.saturating_add(height);
    }

    let lf = concat(
        lfs,
        UnionArgs {
            parallel: true,
            rechunk: false,
            to_supertypes: false,
            diagonal: false,
            from_partitioned_ds: false,
        },
    )
    .unwrap()
    .select(vec![col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)]);

    Ok((lf, new_height))
}

pub fn multiple_tt_to_deduplicated_lf(
    m1: &mut HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>,
    m2: Option<&mut HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
    subj_datatype_req: Option<&BaseRDFNodeType>,
    obj_datatype_req: Option<&BaseRDFNodeType>,
    subjects: &Option<Vec<Subject>>,
    object_term: Option<Term>,
    caching_folder: &Option<String>,
) -> Result<Option<SolutionMappings>, SparqlError> {
    let mut filtered = vec![];
    let m: Vec<_> = if let Some(m2) = m2 {
        m1.iter_mut().chain(m2).collect()
    } else {
        m1.iter_mut().collect()
    };
    for ((subj_type, obj_type), tt) in m {
        let mut keep = true;
        if let Some(subj_req) = subj_datatype_req {
            keep = subj_req == subj_type;
        }
        if let Some(obj_req) = obj_datatype_req {
            keep = keep && obj_req == obj_type;
        }
        if keep {
            let (mut lf, height) = single_tt_to_deduplicated_lf(tt, caching_folder, subjects)?;
            if let Some(subject_terms) = &subjects {
                if subject_terms.len() == 1 {
                    lf = lf.filter(
                        col(SUBJECT_COL_NAME).eq(rdf_term_to_polars_expr(&Term::from(
                            subject_terms.get(0).unwrap().as_ref(),
                        ))),
                    );
                } else {
                    let polars_subjects: Vec<_> = subject_terms
                        .iter()
                        .map(|x| {
                            if let Expr::Literal(l) = rdf_term_to_polars_expr(&Term::from(x.as_ref())) {
                                l
                            } else {
                                panic!()
                            }
                        })
                        .collect();
                    let ser = polars_literal_values_to_series(polars_subjects, "dummy");
                    lf = lf.filter(col(SUBJECT_COL_NAME).is_in(lit(ser)));
                }
            }
            if let Some(object_term) = &object_term {
                lf = lf.filter(col(OBJECT_COL_NAME).eq(rdf_term_to_polars_expr(object_term)));
            }
            let rdf_node_types = HashMap::from([
                (SUBJECT_COL_NAME.to_string(), subj_type.as_rdf_node_type()),
                (OBJECT_COL_NAME.to_string(), obj_type.as_rdf_node_type()),
            ]);
            let sm = SolutionMappings::new(lf, rdf_node_types, height);
            filtered.push(sm);
        }
    }
    if filtered.is_empty() {
        Ok(None)
    } else {
        let sm = union(filtered, false)?;
        Ok(Some(sm))
    }
}

pub fn create_empty_lf_datatypes(
    subject_keep_rename: &Option<String>,
    verb_keep_rename: &Option<String>,
    object_keep_rename: &Option<String>,
    object_datatype_req: Option<&BaseRDFNodeType>,
) -> SolutionMappings {
    let mut columns_vec = vec![];
    let mut out_datatypes = HashMap::new();

    if let Some(subject_rename) = subject_keep_rename {
        out_datatypes.insert(subject_rename.to_string(), RDFNodeType::None);
        columns_vec.push(Column::new_empty(
            subject_rename.into(),
            &BaseRDFNodeType::None.polars_data_type(),
        ))
    }
    if let Some(verb_rename) = verb_keep_rename {
        out_datatypes.insert(verb_rename.to_string(), RDFNodeType::None);
        columns_vec.push(Column::new_empty(
            verb_rename.into(),
            &BaseRDFNodeType::None.polars_data_type(),
        ))
    }
    if let Some(object_rename) = object_keep_rename {
        let (use_datatype, use_polars_datatype) = if let Some(dt) = object_datatype_req {
            let polars_dt = dt.polars_data_type();
            (dt.as_rdf_node_type(), polars_dt)
        } else {
            let dt = BaseRDFNodeType::None;
            let polars_dt = dt.polars_data_type();
            (dt.as_rdf_node_type(), polars_dt)
        };
        out_datatypes.insert(object_rename.to_string(), use_datatype);
        columns_vec.push(Column::new_empty(
            object_rename.into(),
            &use_polars_datatype,
        ))
    }
    SolutionMappings::new(DataFrame::new(columns_vec).unwrap().lazy(), out_datatypes, 0)
}

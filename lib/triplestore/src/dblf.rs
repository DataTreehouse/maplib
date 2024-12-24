use super::{Triples, Triplestore};
use crate::sparql::errors::SparqlError;
use crate::sparql::pushdowns::PossibleTypes;
use oxrdf::{NamedNode, Subject, Term};
use polars::prelude::{col, concat, lit, IntoLazy, LazyFrame, PlSmallStr, UnionArgs};
use polars_core::prelude::{Column, DataFrame, SortMultipleOptions};
use query_processing::graph_patterns::union;
use representation::multitype::convert_lf_col_to_multitype;
use representation::rdf_to_polars::{
    rdf_named_node_to_polars_literal_value, rdf_term_to_polars_expr,
};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::{HashMap, HashSet};

impl Triplestore {
    pub fn get_predicate_iris(&self, include_transient: bool) -> Vec<NamedNode> {
        let mut iris = vec![];
        for nn in self.triples_map.keys() {
            iris.push(nn.clone());
        }
        if include_transient {
            for nn in self.transient_triples_map.keys() {
                iris.push(nn.clone());
            }
        }
        iris
    }

    pub fn get_predicate_eager_solution_mappings(
        &mut self,
        predicate: &NamedNode,
        include_transient: bool,
    ) -> Result<Vec<EagerSolutionMappings>, SparqlError> {
        let mut types = vec![];
        if let Some(map) = self.triples_map.get(predicate) {
            for (s, o) in map.keys() {
                types.push((
                    PossibleTypes::singular(s.clone()),
                    PossibleTypes::singular(o.clone()),
                ));
            }
        }
        if include_transient {
            if let Some(map) = self.transient_triples_map.get(predicate) {
                for (s, o) in map.keys() {
                    types.push((
                        PossibleTypes::singular(s.clone()),
                        PossibleTypes::singular(o.clone()),
                    ));
                }
            }
        }
        let mut eager_sms = vec![];
        for (s, o) in types {
            let sm = self.get_deduplicated_predicate_lf(
                predicate,
                &Some(SUBJECT_COL_NAME.to_string()),
                &None,
                &Some(OBJECT_COL_NAME.to_string()),
                &None,
                &None,
                &Some(s),
                &Some(o),
            )?;
            let eager_sm = sm.as_eager();
            eager_sms.push(eager_sm);
        }
        Ok(eager_sms)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn get_deduplicated_predicate_lf(
        &mut self,
        verb_uri: &NamedNode,
        subject_keep_rename: &Option<String>,
        verb_keep_rename: &Option<String>,
        object_keep_rename: &Option<String>,
        subjects: &Option<Vec<Subject>>,
        objects: &Option<Vec<Term>>,
        subject_datatype_ctr: &Option<PossibleTypes>,
        object_datatype_ctr: &Option<PossibleTypes>,
    ) -> Result<SolutionMappings, SparqlError> {
        let mut all_sms = vec![];
        if self.triples_map.contains_key(verb_uri) {
            let compatible_types = self.all_compatible_types(
                verb_uri,
                false,
                subject_datatype_ctr,
                object_datatype_ctr,
            );
            if let Some(m) = self.triples_map.get_mut(verb_uri) {
                if let Some(sms) = multiple_tt_to_deduplicated_lf(
                    m,
                    compatible_types,
                    subjects,
                    objects,
                    &self.caching_folder,
                    subject_keep_rename.is_some(),
                )? {
                    all_sms.extend(sms);
                }
            }
        }
        if self.transient_triples_map.contains_key(verb_uri) {
            let compatible_types = self.all_compatible_types(
                verb_uri,
                true,
                subject_datatype_ctr,
                object_datatype_ctr,
            );
            if let Some(m) = self.transient_triples_map.get_mut(verb_uri) {
                if let Some(sms) = multiple_tt_to_deduplicated_lf(
                    m,
                    compatible_types,
                    subjects,
                    objects,
                    &self.caching_folder,
                    subject_keep_rename.is_some(),
                )? {
                    all_sms.extend(sms);
                }
            }
        }
        if !all_sms.is_empty() {
            let SolutionMappings {
                mut mappings,
                mut rdf_node_types,
                height_upper_bound,
            } = union(all_sms, true).map_err(SparqlError::QueryProcessingError)?;
            let mut out_datatypes = HashMap::new();
            let use_subject_col_name = uuid::Uuid::new_v4().to_string();
            let use_object_col_name = uuid::Uuid::new_v4().to_string();
            mappings = mappings.rename(
                [SUBJECT_COL_NAME, OBJECT_COL_NAME],
                [&use_subject_col_name, &use_object_col_name],
                true,
            );

            let mut drop = vec![];
            if let Some(renamed) = subject_keep_rename {
                mappings = mappings.rename([&use_subject_col_name], [renamed], true);
                out_datatypes.insert(
                    renamed.to_string(),
                    rdf_node_types.remove(SUBJECT_COL_NAME).unwrap(),
                );
            } else {
                drop.push(use_subject_col_name);
            }
            if let Some(renamed) = object_keep_rename {
                mappings = mappings.rename([&use_object_col_name], [renamed], true);
                out_datatypes.insert(
                    renamed.to_string(),
                    rdf_node_types.remove(OBJECT_COL_NAME).unwrap(),
                );
            } else {
                drop.push(use_object_col_name)
            }
            if let Some(renamed) = verb_keep_rename {
                mappings = mappings.with_column(
                    lit(rdf_named_node_to_polars_literal_value(verb_uri)).alias(renamed),
                );
                out_datatypes.insert(renamed.clone(), RDFNodeType::IRI);
            }
            mappings = mappings.drop(drop);
            Ok(SolutionMappings::new(
                mappings,
                out_datatypes,
                height_upper_bound,
            ))
        } else {
            Ok(create_empty_lf_datatypes(
                subject_keep_rename,
                verb_keep_rename,
                object_keep_rename,
                object_datatype_ctr,
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
        objects: &Option<Vec<Term>>,
        subject_datatype_ctr: &Option<PossibleTypes>,
        object_datatype_ctr: &Option<PossibleTypes>,
    ) -> Result<SolutionMappings, SparqlError> {
        let mut solution_mappings = vec![];

        let predicate_uris = predicate_uris.unwrap_or(self.all_predicates());

        let mut sms = vec![];
        for nn in predicate_uris {
            let sm = self.get_deduplicated_predicate_lf(
                &nn,
                subject_keep_rename,
                verb_keep_rename,
                object_keep_rename,
                subjects,
                objects,
                subject_datatype_ctr,
                object_datatype_ctr,
            )?;
            sms.push(sm);
        }
        let subject_need_multi = partial_check_need_multi(&sms, subject_keep_rename);
        let object_need_multi = partial_check_need_multi(&sms, object_keep_rename);

        for SolutionMappings {
            mappings: mut lf,
            rdf_node_types: mut datatypes_map,
            height_upper_bound,
        } in sms
        {
            if let Some(subj_col) = subject_keep_rename {
                if height_upper_bound > 0
                    && subject_need_multi
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
                    && object_need_multi
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
                solution_mappings.push(SolutionMappings::new(
                    lf,
                    datatypes_map,
                    height_upper_bound,
                ));
            }
        }
        Ok(if !solution_mappings.is_empty() {
            union(solution_mappings, true)?
        } else {
            create_empty_lf_datatypes(
                subject_keep_rename,
                verb_keep_rename,
                object_keep_rename,
                object_datatype_ctr,
            )
        })
    }

    pub(crate) fn all_compatible_types(
        &self,
        predicate_uri: &NamedNode,
        transient: bool,
        subject_datatype_ctr: &Option<PossibleTypes>,
        object_datatype_ctr: &Option<PossibleTypes>,
    ) -> Option<HashSet<(BaseRDFNodeType, BaseRDFNodeType)>> {
        if subject_datatype_ctr.is_none() && object_datatype_ctr.is_none() {
            return None;
        }
        let mut predicates = HashSet::new();
        let use_map = if transient {
            &self.transient_triples_map
        } else {
            &self.triples_map
        };

        if let Some(map) = use_map.get(predicate_uri) {
            for (subject_type, object_type) in map.keys() {
                if let Some(subject_datatype_ctr) = subject_datatype_ctr {
                    if !subject_datatype_ctr.compatible_with(subject_type) {
                        continue;
                    }
                }
                if let Some(object_datatype_ctr) = object_datatype_ctr {
                    if !object_datatype_ctr.compatible_with(object_type) {
                        continue;
                    }
                }
                predicates.insert((subject_type.clone(), object_type.clone()));
            }
        }
        Some(predicates)
    }

    fn all_predicates(&self) -> Vec<NamedNode> {
        let mut predicates = vec![];
        for nn in self.triples_map.keys() {
            predicates.push(nn.clone());
        }
        for nn in self.transient_triples_map.keys() {
            predicates.push(nn.clone());
        }
        predicates
    }
}

fn partial_check_need_multi(sms: &Vec<SolutionMappings>, col_name: &Option<String>) -> bool {
    if let Some(col_name) = col_name {
        let mut first_datatype = None;
        for sm in sms {
            let use_dt = sm.rdf_node_types.get(col_name).unwrap();
            if let Some(first) = &first_datatype {
                if first != &use_dt {
                    return true;
                }
            } else {
                first_datatype = Some(use_dt);
            }
        }
    }
    false
}

fn single_tt_to_deduplicated_lf(
    tt: &mut Triples,
    caching_folder: &Option<String>,
    subjects: &Option<Vec<Subject>>,
    objects: &Option<Vec<Term>>,
) -> Result<(LazyFrame, usize), SparqlError> {
    if !tt.unique {
        tt.deduplicate(caching_folder)
            .map_err(SparqlError::DeduplicationError)?;
    }
    assert!(tt.unique, "Should be deduplicated");
    let lfs_and_heights = tt
        .get_lazy_frames(subjects, objects)
        .map_err(SparqlError::TripleTableReadError)?;
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
    triples: &mut HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>,
    types: Option<HashSet<(BaseRDFNodeType, BaseRDFNodeType)>>,
    subjects: &Option<Vec<Subject>>,
    objects: &Option<Vec<Term>>,
    caching_folder: &Option<String>,
    keep_subject: bool,
) -> Result<Option<Vec<SolutionMappings>>, SparqlError> {
    let mut filtered = vec![];
    for ((subj_type, obj_type), tt) in triples.iter_mut() {
        let mut keep = true;
        if let Some(types) = &types {
            if !types.contains(&(subj_type.clone(), obj_type.clone())) {
                keep = false;
            }
        }
        if keep {
            let (mut lf, height) =
                single_tt_to_deduplicated_lf(tt, caching_folder, subjects, objects)?;
            if tt.output_sorted_subject() && keep_subject {
                lf = lf.sort(
                    vec![PlSmallStr::from_str(SUBJECT_COL_NAME)],
                    SortMultipleOptions {
                        descending: vec![true],
                        nulls_last: vec![false],
                        multithreaded: true,
                        maintain_order: false,
                        limit: None,
                    },
                );
            }
            if let Some(subject_terms) = &subjects {
                // Handles case where singular subject from triple pattern.
                if subject_terms.len() == 1 {
                    lf = lf.filter(
                        col(SUBJECT_COL_NAME).eq(rdf_term_to_polars_expr(&Term::from(
                            subject_terms.first().unwrap().as_ref(),
                        ))),
                    );
                }
            }
            if let Some(object_terms) = &objects {
                // Handles case where singular object from triple pattern.
                if object_terms.len() == 1 {
                    lf = lf.filter(
                        col(OBJECT_COL_NAME)
                            .eq(rdf_term_to_polars_expr(object_terms.first().unwrap())),
                    );
                }
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
        Ok(Some(filtered))
    }
}

pub fn create_empty_lf_datatypes(
    subject_keep_rename: &Option<String>,
    verb_keep_rename: &Option<String>,
    object_keep_rename: &Option<String>,
    object_datatype: &Option<PossibleTypes>,
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
        let dt = if let Some(object_datatype) = object_datatype {
            object_datatype.get_witness()
        } else {
            BaseRDFNodeType::None
        };
        let polars_dt = dt.polars_data_type();
        let use_datatype = dt.as_rdf_node_type();

        out_datatypes.insert(object_rename.to_string(), use_datatype);
        columns_vec.push(Column::new_empty(object_rename.into(), &polars_dt))
    }
    SolutionMappings::new(
        DataFrame::new(columns_vec).unwrap().lazy(),
        out_datatypes,
        0,
    )
}

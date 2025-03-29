use super::{Triples, Triplestore};
use crate::sparql::errors::SparqlError;
use oxrdf::{NamedNode, Subject, Term};
use polars::prelude::{as_struct, col, concat, lit, IntoLazy, LazyFrame, UnionArgs};
use polars_core::datatypes::CategoricalOrdering;
use polars_core::prelude::{Column, DataFrame, DataType};
use query_processing::type_constraints::PossibleTypes;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use representation::multitype::{all_multi_cols, base_col_name, lf_columns_to_categorical};
use representation::rdf_to_polars::{
    rdf_named_node_to_polars_literal_value, rdf_term_to_polars_expr,
};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::{
    BaseRDFNodeType, RDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME,
    SUBJECT_COL_NAME, VERB_COL_NAME,
};
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
            let sm = self.get_multi_predicates_solution_mappings(
                Some(vec![predicate.to_owned()]),
                &Some(SUBJECT_COL_NAME.to_string()),
                &None,
                &Some(OBJECT_COL_NAME.to_string()),
                &None,
                &None,
                &Some(s),
                &Some(o),
                include_transient,
            )?;
            let eager_sm = sm.as_eager(false);
            eager_sms.push(eager_sm);
        }
        Ok(eager_sms)
    }

    #[allow(clippy::too_many_arguments)]
    fn get_deduplicated_predicate_lf(
        &mut self,
        verb_uri: &NamedNode,
        keep_subject: bool,
        keep_verb: bool,
        _keep_object: bool,
        subjects: &Option<Vec<Subject>>,
        objects: &Option<Vec<Term>>,
        subject_datatype_ctr: &Option<PossibleTypes>,
        object_datatype_ctr: &Option<PossibleTypes>,
        include_transient: bool,
    ) -> Result<Option<Vec<HalfBakedSolutionMappings>>, SparqlError> {
        let mut all_sms = vec![];
        if self.triples_map.contains_key(verb_uri) {
            let compatible_types = self.all_compatible_types(
                verb_uri,
                false,
                subject_datatype_ctr,
                object_datatype_ctr,
            );
            if let Some(m) = self.triples_map.get_mut(verb_uri) {
                if let Some(sms) =
                    multiple_tt_to_lf(m, compatible_types, subjects, objects, keep_subject)?
                {
                    all_sms.extend(sms);
                }
            }
        }
        if include_transient && self.transient_triples_map.contains_key(verb_uri) {
            let compatible_types = self.all_compatible_types(
                verb_uri,
                true,
                subject_datatype_ctr,
                object_datatype_ctr,
            );
            if let Some(m) = self.transient_triples_map.get_mut(verb_uri) {
                if let Some(sms) =
                    multiple_tt_to_lf(m, compatible_types, subjects, objects, keep_subject)?
                {
                    all_sms.extend(sms);
                }
            }
        }
        if !all_sms.is_empty() {
            if keep_verb {
                for sm in &mut all_sms {
                    sm.verb = Some(verb_uri.to_owned());
                }
            }
            Ok(Some(all_sms))
        } else {
            Ok(None)
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn get_multi_predicates_solution_mappings(
        &mut self,
        predicate_uris: Option<Vec<NamedNode>>,
        subject_keep_rename: &Option<String>,
        verb_keep_rename: &Option<String>,
        object_keep_rename: &Option<String>,
        subjects: &Option<Vec<Subject>>,
        objects: &Option<Vec<Term>>,
        subject_datatype_ctr: &Option<PossibleTypes>,
        object_datatype_ctr: &Option<PossibleTypes>,
        include_transient: bool,
    ) -> Result<SolutionMappings, SparqlError> {
        let predicate_uris = predicate_uris.unwrap_or(self.all_predicates());
        let predicate_uris_len = predicate_uris.len();
        let mut sms = vec![];
        if !(objects.is_some() && objects.as_ref().unwrap().is_empty())
            || !(subjects.is_some() && subjects.as_ref().unwrap().is_empty())
        {
            for nn in predicate_uris {
                if let Some(sm) = self.get_deduplicated_predicate_lf(
                    &nn,
                    subject_keep_rename.is_some(),
                    verb_keep_rename.is_some(),
                    object_keep_rename.is_some(),
                    subjects,
                    objects,
                    subject_datatype_ctr,
                    object_datatype_ctr,
                    include_transient,
                )? {
                    sms.extend(sm);
                }
            }
        }

        if !sms.is_empty() {
            let subject_need_multi =
                partial_check_need_multi(&sms, true, subject_keep_rename.is_some());
            let object_need_multi =
                partial_check_need_multi(&sms, false, object_keep_rename.is_some());
            let mut all_mappings = vec![];
            let mut subject_types = HashSet::new();
            let mut object_types = HashSet::new();
            let mut accumulated_heights = 0usize;

            // This part is to work around a performance bug in Polars.
            // Still present..
            if predicate_uris_len > 1 && (subjects.is_some() || objects.is_some()) {
                sms = sms
                    .into_par_iter()
                    .map(|sm| {
                        let HalfBakedSolutionMappings {
                            mut mappings,
                            verb,
                            subject_type,
                            object_type,
                            height_upper_bound: _,
                        } = sm;
                        mappings =
                            mappings.with_column(col(SUBJECT_COL_NAME).cast(DataType::String));
                        if object_type.is_lang_string() {
                            mappings = mappings.with_column(
                                as_struct(vec![
                                    col(OBJECT_COL_NAME)
                                        .struct_()
                                        .field_by_name(LANG_STRING_VALUE_FIELD)
                                        .cast(DataType::String)
                                        .alias(LANG_STRING_VALUE_FIELD),
                                    col(OBJECT_COL_NAME)
                                        .struct_()
                                        .field_by_name(LANG_STRING_LANG_FIELD)
                                        .cast(DataType::String)
                                        .alias(LANG_STRING_LANG_FIELD),
                                ])
                                .alias(OBJECT_COL_NAME),
                            )
                        } else if object_type.polars_data_type() == DataType::String {
                            mappings =
                                mappings.with_column(col(OBJECT_COL_NAME).cast(DataType::String));
                        }
                        let df = mappings.collect().unwrap();
                        let height_upper_bound = df.height();
                        mappings = df.lazy();
                        let rdf_node_types = HashMap::from([
                            (
                                SUBJECT_COL_NAME.to_string(),
                                subject_type.as_rdf_node_type(),
                            ),
                            (OBJECT_COL_NAME.to_string(), object_type.as_rdf_node_type()),
                        ]);
                        mappings = lf_columns_to_categorical(
                            mappings,
                            &rdf_node_types,
                            CategoricalOrdering::Physical,
                        );

                        HalfBakedSolutionMappings {
                            mappings,
                            verb,
                            subject_type,
                            object_type,
                            height_upper_bound,
                        }
                    })
                    .collect();
            }

            for HalfBakedSolutionMappings {
                mut mappings,
                verb,
                subject_type,
                object_type,
                height_upper_bound,
            } in sms
            {
                if subject_need_multi {
                    mappings = unnest_non_multi_col(mappings, SUBJECT_COL_NAME, &subject_type);
                }
                if subject_keep_rename.is_some() {
                    subject_types.insert(subject_type);
                }

                if verb_keep_rename.is_some() {
                    mappings = mappings.with_column(
                        lit(rdf_named_node_to_polars_literal_value(&verb.unwrap()))
                            .cast(DataType::Categorical(None, CategoricalOrdering::Physical))
                            .alias(VERB_COL_NAME),
                    );
                }

                if object_need_multi {
                    mappings = unnest_non_multi_col(mappings, OBJECT_COL_NAME, &object_type);
                }
                if object_keep_rename.is_some() {
                    object_types.insert(object_type);
                }

                accumulated_heights = accumulated_heights.saturating_add(height_upper_bound);
                all_mappings.push(mappings);
            }

            let mut mappings = if all_mappings.len() > 1 {
                concat(
                    all_mappings,
                    UnionArgs {
                        parallel: true,
                        rechunk: false,
                        to_supertypes: false,
                        diagonal: true,
                        from_partitioned_ds: false,
                        maintain_order: false,
                    },
                )
                .unwrap()
            } else {
                all_mappings.pop().unwrap()
            };

            let mut sorted_subject_types: Vec<_> = subject_types.into_iter().collect();
            sorted_subject_types.sort();
            let mut sorted_object_types: Vec<_> = object_types.into_iter().collect();
            sorted_object_types.sort();

            if subject_need_multi {
                let multi_colnames = all_multi_cols(&sorted_subject_types);
                let mut struct_exprs = vec![];
                for inner_col in &multi_colnames {
                    let prefixed_col = create_prefixed_multi_colname(SUBJECT_COL_NAME, inner_col);
                    struct_exprs.push(col(prefixed_col).alias(inner_col));
                }
                mappings = mappings.with_column(as_struct(struct_exprs).alias(SUBJECT_COL_NAME));
            }
            if object_need_multi {
                let multi_colnames = all_multi_cols(&sorted_object_types);
                let mut struct_exprs = vec![];
                for inner_col in &multi_colnames {
                    let prefixed_col = create_prefixed_multi_colname(OBJECT_COL_NAME, inner_col);
                    struct_exprs.push(col(prefixed_col).alias(inner_col));
                }
                mappings = mappings.with_column(as_struct(struct_exprs).alias(OBJECT_COL_NAME));
            }
            let mut keep = vec![];
            let mut rename_src = vec![];
            let mut rename_trg = vec![];
            if let Some(s) = subject_keep_rename {
                keep.push(SUBJECT_COL_NAME);
                if s != SUBJECT_COL_NAME {
                    rename_src.push(SUBJECT_COL_NAME);
                    rename_trg.push(s);
                }
            }
            if let Some(v) = verb_keep_rename {
                keep.push(VERB_COL_NAME);
                if v != VERB_COL_NAME {
                    rename_src.push(VERB_COL_NAME);
                    rename_trg.push(v);
                }
            }
            if let Some(o) = object_keep_rename {
                keep.push(OBJECT_COL_NAME);
                if o != OBJECT_COL_NAME {
                    rename_src.push(OBJECT_COL_NAME);
                    rename_trg.push(o);
                }
            }
            let keep_exprs: Vec<_> = keep.iter().map(|x| col(*x)).collect();
            mappings = mappings.select(keep_exprs);
            if !rename_src.is_empty() {
                mappings = mappings.rename(&rename_src, &rename_trg, true);
            }

            let mut types = HashMap::new();
            if let Some(subject_col_name) = subject_keep_rename {
                let subject_type = if subject_need_multi {
                    RDFNodeType::MultiType(sorted_subject_types)
                } else {
                    sorted_subject_types.pop().unwrap().as_rdf_node_type()
                };
                types.insert(subject_col_name.clone(), subject_type);
            }
            if let Some(verb_col_name) = verb_keep_rename {
                types.insert(verb_col_name.clone(), RDFNodeType::IRI);
            }
            if let Some(object_col_name) = object_keep_rename {
                let object_type = if object_need_multi {
                    RDFNodeType::MultiType(sorted_object_types)
                } else {
                    sorted_object_types.pop().unwrap().as_rdf_node_type()
                };
                types.insert(object_col_name.clone(), object_type);
            };
            let sm = SolutionMappings::new(mappings, types, accumulated_heights);
            Ok(sm)
        } else {
            Ok(create_empty_lf_datatypes(
                subject_keep_rename,
                verb_keep_rename,
                object_keep_rename,
                object_datatype_ctr,
            ))
        }
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

#[allow(clippy::type_complexity)]
pub fn unnest_non_multi_col(mut mappings: LazyFrame, c: &str, dt: &BaseRDFNodeType) -> LazyFrame {
    let mut exprs = vec![];
    let mut drop_cols = vec![];

    if dt.is_lang_string() {
        let inner_cols = all_multi_cols(&vec![dt.clone()]);
        for inner in &inner_cols {
            let prefixed_inner = create_prefixed_multi_colname(c, inner);
            exprs.push(col(c).struct_().field_by_name(inner).alias(&prefixed_inner));
        }
    } else {
        let inner = base_col_name(dt);
        let prefixed_inner = create_prefixed_multi_colname(c, &inner);
        exprs.push(col(c).alias(&prefixed_inner));
    }
    drop_cols.push(col(c));

    mappings = mappings.with_columns(exprs);
    mappings = mappings.drop(drop_cols);
    mappings
}

fn create_prefixed_multi_colname(c: &str, inner: &str) -> String {
    format!("{c}{inner}")
}

fn partial_check_need_multi(
    sms: &Vec<HalfBakedSolutionMappings>,
    subject: bool,
    keep: bool,
) -> bool {
    if keep {
        let mut first_datatype = None;
        for sm in sms {
            let use_dt = if subject {
                &sm.subject_type
            } else {
                &sm.object_type
            };
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

fn single_tt_to_lf(
    tt: &Triples,
    subjects: &Option<Vec<&Subject>>,
    objects: &Option<Vec<&Term>>,
    _keep_subject: bool,
) -> Result<Option<(LazyFrame, usize)>, SparqlError> {
    let lfs_and_heights = tt
        .get_lazy_frames(subjects, objects)
        .map_err(SparqlError::TripleTableReadError)?;
    if lfs_and_heights.is_empty() {
        return Ok(None);
    }
    let mut lfs = vec![];
    let mut new_height = 0usize;
    for (lf, height) in lfs_and_heights {
        lfs.push(lf);
        new_height = new_height.saturating_add(height);
    }
    let mut lf = if lfs.len() > 1 {
        concat(
            lfs,
            UnionArgs {
                parallel: true,
                rechunk: true, //Todo: check if important for performance
                to_supertypes: false,
                diagonal: false,
                from_partitioned_ds: false,
                maintain_order: false,
            },
        )
        .unwrap()
    } else {
        lfs.pop().unwrap()
    };
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
                col(OBJECT_COL_NAME).eq(rdf_term_to_polars_expr(object_terms.first().unwrap())),
            );
        }
    }
    Ok(Some((lf, new_height)))
}

struct HalfBakedSolutionMappings {
    pub mappings: LazyFrame,
    pub verb: Option<NamedNode>,
    pub subject_type: BaseRDFNodeType,
    pub object_type: BaseRDFNodeType,
    pub height_upper_bound: usize,
}

fn multiple_tt_to_lf(
    triples: &HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>,
    types: Option<HashSet<(BaseRDFNodeType, BaseRDFNodeType)>>,
    subjects: &Option<Vec<Subject>>,
    objects: &Option<Vec<Term>>,
    keep_subject: bool,
) -> Result<Option<Vec<HalfBakedSolutionMappings>>, SparqlError> {
    let mut filtered = vec![];
    for ((subj_type, obj_type), tt) in triples.iter() {
        if let Some(types) = &types {
            if !types.contains(&(subj_type.clone(), obj_type.clone())) {
                continue;
            }
        }
        let filtered_subjects = if let Some(subjects) = subjects {
            let filtered = filter_subjects(subj_type, subjects);
            if filtered.is_empty() {
                continue;
            }
            Some(filtered)
        } else {
            None
        };

        let filtered_objects = if let Some(objects) = objects {
            let filtered = filter_objects(obj_type, objects);
            if filtered.is_empty() {
                continue;
            }
            Some(filtered)
        } else {
            None
        };

        if let Some((lf, height)) =
            single_tt_to_lf(tt, &filtered_subjects, &filtered_objects, keep_subject)?
        {
            if height > 0 {
                let half_baked = HalfBakedSolutionMappings {
                    mappings: lf,
                    verb: None,
                    subject_type: subj_type.clone(),
                    object_type: obj_type.clone(),
                    height_upper_bound: height,
                };
                filtered.push(half_baked);
            }
        }
    }
    if filtered.is_empty() {
        Ok(None)
    } else {
        Ok(Some(filtered))
    }
}

fn filter_objects<'a>(object_type: &BaseRDFNodeType, objects: &'a Vec<Term>) -> Vec<&'a Term> {
    let mut filtered = vec![];
    for o in objects {
        let ok = match object_type {
            BaseRDFNodeType::IRI => {
                matches!(o, Term::NamedNode(_))
            }
            BaseRDFNodeType::BlankNode => {
                matches!(o, Term::BlankNode(_))
            }
            BaseRDFNodeType::Literal(l) => {
                if let Term::Literal(tl) = o {
                    tl.datatype() == l.as_ref()
                } else {
                    false
                }
            }
            BaseRDFNodeType::None => {
                panic!("Triplestore in invalid state")
            }
        };
        if ok {
            filtered.push(o);
        }
    }
    filtered
}

fn filter_subjects<'a>(
    subject_type: &BaseRDFNodeType,
    subjects: &'a Vec<Subject>,
) -> Vec<&'a Subject> {
    let mut filtered = vec![];
    for s in subjects {
        #[allow(unreachable_patterns)]
        let ok = match s {
            Subject::NamedNode(_) => subject_type == &BaseRDFNodeType::IRI,
            Subject::BlankNode(_) => subject_type == &BaseRDFNodeType::BlankNode,
            _ => unimplemented!("Only blank node and iri subjects"),
        };
        if ok {
            filtered.push(s);
        }
    }
    filtered
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

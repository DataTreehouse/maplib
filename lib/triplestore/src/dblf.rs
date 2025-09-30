use super::{StoredBaseRDFNodeType, Triplestore};
use crate::sparql::errors::SparqlError;
use crate::storage::Triples;
use oxrdf::{NamedNode, Subject, Term};
use polars::prelude::{as_struct, by_name, col, concat, lit, IntoLazy, LazyFrame, UnionArgs};
use polars_core::prelude::{Column, DataFrame};
use query_processing::expressions::{blank_node_enc, maybe_literal_enc, named_node_enc};
use query_processing::type_constraints::PossibleTypes;
use representation::cats::{named_node_split_prefix, Cats};
use representation::multitype::all_multi_cols;
use representation::solution_mapping::{BaseCatState, EagerSolutionMappings, SolutionMappings};
use representation::{
    BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, PREDICATE_COL_NAME, SUBJECT_COL_NAME,
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
                    PossibleTypes::singular(s.as_constrained()),
                    PossibleTypes::singular(o.as_constrained()),
                ));
            }
        }
        if include_transient {
            if let Some(map) = self.transient_triples_map.get(predicate) {
                for (s, o) in map.keys() {
                    types.push((
                        PossibleTypes::singular(s.as_constrained()),
                        PossibleTypes::singular(o.as_constrained()),
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
    fn get_predicate_lf(
        &self,
        predicate_uri: &NamedNode,
        keep_subject: bool,
        keep_predicate: bool,
        keep_object: bool,
        subjects: &Option<Vec<Subject>>,
        objects: &Option<Vec<Term>>,
        subject_datatype_ctr: &Option<PossibleTypes>,
        object_datatype_ctr: &Option<PossibleTypes>,
        include_transient: bool,
    ) -> Result<Option<Vec<HalfBakedSolutionMappings>>, SparqlError> {
        let mut all_sms = vec![];
        let cats = self.global_cats.read()?;
        if self.triples_map.contains_key(predicate_uri) {
            let compatible_types = self.all_compatible_types(
                predicate_uri,
                false,
                subject_datatype_ctr,
                object_datatype_ctr,
            );
            if let Some(m) = self.triples_map.get(predicate_uri) {
                if let Some(sms) = multiple_tt_to_lf(
                    m,
                    compatible_types,
                    subjects,
                    objects,
                    keep_subject,
                    keep_object,
                    &cats,
                )? {
                    all_sms.extend(sms);
                }
            }
        }
        if include_transient && self.transient_triples_map.contains_key(predicate_uri) {
            let compatible_types = self.all_compatible_types(
                predicate_uri,
                true,
                subject_datatype_ctr,
                object_datatype_ctr,
            );
            if let Some(m) = self.transient_triples_map.get(predicate_uri) {
                if let Some(sms) = multiple_tt_to_lf(
                    m,
                    compatible_types,
                    subjects,
                    objects,
                    keep_subject,
                    keep_object,
                    &cats,
                )? {
                    all_sms.extend(sms);
                }
            }
        }
        if !all_sms.is_empty() {
            if keep_predicate {
                for sm in &mut all_sms {
                    sm.predicate = Some(predicate_uri.to_owned());
                }
            }
            Ok(Some(all_sms))
        } else {
            Ok(None)
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn get_multi_predicates_solution_mappings(
        &self,
        predicate_uris: Option<Vec<NamedNode>>,
        subject_keep_rename: &Option<String>,
        predicate_keep_rename: &Option<String>,
        object_keep_rename: &Option<String>,
        subjects: &Option<Vec<Subject>>,
        objects: &Option<Vec<Term>>,
        subject_datatype_ctr: &Option<PossibleTypes>,
        object_datatype_ctr: &Option<PossibleTypes>,
        include_transient: bool,
    ) -> Result<SolutionMappings, SparqlError> {
        let predicate_uris = predicate_uris.unwrap_or(self.all_predicates());
        let cats = self.global_cats.read()?;

        let mut sms = vec![];
        if !(objects.is_some() && objects.as_ref().unwrap().is_empty())
            || !(subjects.is_some() && subjects.as_ref().unwrap().is_empty())
        {
            for nn in predicate_uris {
                if let Some(sm) = self.get_predicate_lf(
                    &nn,
                    subject_keep_rename.is_some(),
                    predicate_keep_rename.is_some(),
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
            let mut subject_types = HashMap::new();
            let mut object_types = HashMap::new();
            let mut accumulated_heights = 0usize;

            for HalfBakedSolutionMappings {
                mut mappings,
                predicate,
                subject_type,
                object_type,
                height_upper_bound,
                subject_state,
                object_state,
            } in sms
            {
                if let Some(subject_type) = &subject_type {
                    if subject_need_multi {
                        mappings = unnest_non_multi_col(mappings, SUBJECT_COL_NAME, subject_type);
                    }
                    subject_types.insert(subject_type.clone(), subject_state.unwrap().clone());
                }
                if predicate_keep_rename.is_some() {
                    let enc = named_node_enc(predicate.as_ref().unwrap(), &cats).unwrap();
                    mappings = mappings.with_column(enc.alias(PREDICATE_COL_NAME));
                }

                if let Some(object_type) = &object_type {
                    if object_need_multi {
                        mappings = unnest_non_multi_col(mappings, OBJECT_COL_NAME, object_type);
                    }
                    object_types.insert(object_type.clone(), object_state.unwrap().clone());
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
            sorted_subject_types.sort_by_cached_key(|(x, _)| x.clone());
            let mut sorted_object_types: Vec<_> = object_types.into_iter().collect();
            sorted_object_types.sort_by_cached_key(|(x, _)| x.clone());

            if subject_need_multi {
                let multi_colnames =
                    all_multi_cols(sorted_subject_types.iter().map(|(x, _)| x).collect());
                let mut struct_exprs = vec![];
                for inner_col in &multi_colnames {
                    let prefixed_col = create_prefixed_multi_colname(SUBJECT_COL_NAME, inner_col);
                    struct_exprs.push(col(prefixed_col).alias(inner_col));
                }
                mappings = mappings.with_column(as_struct(struct_exprs).alias(SUBJECT_COL_NAME));
            }
            if object_need_multi {
                let multi_colnames =
                    all_multi_cols(sorted_object_types.iter().map(|(x, _)| x).collect());
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
            if let Some(v) = predicate_keep_rename {
                keep.push(PREDICATE_COL_NAME);
                if v != PREDICATE_COL_NAME {
                    rename_src.push(PREDICATE_COL_NAME);
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
                let subject_type_map: HashMap<_, _> = sorted_subject_types.into_iter().collect();
                types.insert(
                    subject_col_name.clone(),
                    RDFNodeState::from_map(subject_type_map),
                );
            }
            if let Some(predicate_col_name) = predicate_keep_rename {
                types.insert(
                    predicate_col_name.clone(),
                    RDFNodeState::from_bases(
                        BaseRDFNodeType::IRI,
                        BaseCatState::CategoricalNative(false, None),
                    ),
                );
            }
            if let Some(object_col_name) = object_keep_rename {
                let object_type_map: HashMap<_, _> = sorted_object_types.into_iter().collect();
                types.insert(
                    object_col_name.clone(),
                    RDFNodeState::from_map(object_type_map),
                );
            };
            let sm = SolutionMappings::new(mappings, types, accumulated_heights);
            Ok(sm)
        } else {
            Ok(create_empty_lf_datatypes(
                subject_keep_rename,
                predicate_keep_rename,
                object_keep_rename,
            ))
        }
    }

    pub(crate) fn all_compatible_types(
        &self,
        predicate_uri: &NamedNode,
        transient: bool,
        subject_datatype_ctr: &Option<PossibleTypes>,
        object_datatype_ctr: &Option<PossibleTypes>,
    ) -> Option<HashSet<(StoredBaseRDFNodeType, StoredBaseRDFNodeType)>> {
        if subject_datatype_ctr.is_none() && object_datatype_ctr.is_none() {
            return None;
        }
        let mut compat_types = HashSet::new();
        let use_map = if transient {
            &self.transient_triples_map
        } else {
            &self.triples_map
        };

        if let Some(map) = use_map.get(predicate_uri) {
            for (subject_type, object_type) in map.keys() {
                if let Some(subject_datatype_ctr) = subject_datatype_ctr {
                    if !subject_datatype_ctr.compatible_with(&subject_type.as_constrained()) {
                        continue;
                    }
                }
                if let Some(object_datatype_ctr) = object_datatype_ctr {
                    if !object_datatype_ctr.compatible_with(&object_type.as_constrained()) {
                        continue;
                    }
                }
                compat_types.insert((subject_type.clone(), object_type.clone()));
            }
        }
        Some(compat_types)
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
        let inner_cols = all_multi_cols(vec![dt]);
        for inner in &inner_cols {
            let prefixed_inner = create_prefixed_multi_colname(c, inner);
            exprs.push(col(c).struct_().field_by_name(inner).alias(&prefixed_inner));
        }
    } else {
        let inner = dt.field_col_name();
        let prefixed_inner = create_prefixed_multi_colname(c, &inner);
        exprs.push(col(c).alias(&prefixed_inner));
    }
    drop_cols.push(c);

    mappings = mappings.with_columns(exprs);
    mappings = mappings.drop(by_name(drop_cols, true));
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

fn single_triples_to_lf(
    triples: &Triples,
    subjects: &Option<Vec<&Subject>>,
    objects: &Option<Vec<&Term>>,
    global_cats: &Cats,
) -> Result<Option<(LazyFrame, usize)>, SparqlError> {
    let lfs_and_heights = triples
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
            let subj = *subject_terms.first().unwrap();
            let enc = match subj {
                Subject::NamedNode(nn) => named_node_enc(nn, global_cats),
                Subject::BlankNode(bl) => blank_node_enc(bl, global_cats),
            };
            if let Some(enc) = enc {
                lf = lf.filter(col(SUBJECT_COL_NAME).eq(enc));
            } else {
                lf = lf.filter(lit(false));
            }
        }
    }
    if let Some(object_terms) = &objects {
        // Handles case where singular object from triple pattern.
        if object_terms.len() == 1 {
            let obj = *object_terms.first().unwrap();
            let enc = match obj {
                Term::NamedNode(nn) => named_node_enc(nn, global_cats),
                Term::BlankNode(bl) => blank_node_enc(bl, global_cats),
                Term::Literal(l) => {
                    let (e, _, _) = maybe_literal_enc(l, global_cats);
                    Some(e)
                }
            };

            if let Some(enc) = enc {
                lf = lf.filter(col(OBJECT_COL_NAME).eq(enc));
            } else {
                lf = lf.filter(lit(false));
            }
        }
    }
    Ok(Some((lf, new_height)))
}

struct HalfBakedSolutionMappings {
    pub mappings: LazyFrame,
    pub predicate: Option<NamedNode>,
    pub subject_type: Option<BaseRDFNodeType>,
    pub object_type: Option<BaseRDFNodeType>,
    pub height_upper_bound: usize,
    pub subject_state: Option<BaseCatState>,
    pub object_state: Option<BaseCatState>,
}

fn multiple_tt_to_lf(
    triples: &HashMap<(StoredBaseRDFNodeType, StoredBaseRDFNodeType), Triples>,
    types: Option<HashSet<(StoredBaseRDFNodeType, StoredBaseRDFNodeType)>>,
    subjects: &Option<Vec<Subject>>,
    objects: &Option<Vec<Term>>,
    keep_subject: bool,
    keep_object: bool,
    global_cats: &Cats,
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
            single_triples_to_lf(tt, &filtered_subjects, &filtered_objects, global_cats)?
        {
            if height > 0 {
                let mut select = vec![];
                if keep_subject {
                    select.push(col(SUBJECT_COL_NAME));
                }
                if keep_object {
                    select.push(col(OBJECT_COL_NAME));
                }

                let base_subject = subj_type.as_base_rdf_node_type();
                let base_object = obj_type.as_base_rdf_node_type();

                let subject_state = base_subject.default_stored_cat_state();
                let object_state = base_object.default_stored_cat_state();

                let half_baked = HalfBakedSolutionMappings {
                    mappings: lf.select(select),
                    predicate: None,
                    subject_type: if keep_subject {
                        Some(base_subject)
                    } else {
                        None
                    },
                    object_type: if keep_object { Some(base_object) } else { None },
                    height_upper_bound: height,
                    subject_state: if keep_subject {
                        Some(subject_state)
                    } else {
                        None
                    },
                    object_state: if keep_object {
                        Some(object_state)
                    } else {
                        None
                    },
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

fn filter_objects<'a>(
    object_type: &StoredBaseRDFNodeType,
    objects: &'a Vec<Term>,
) -> Vec<&'a Term> {
    let mut filtered = vec![];
    for o in objects {
        let ok = match object_type {
            StoredBaseRDFNodeType::IRI(prefix) => {
                if let Term::NamedNode(nn) = o {
                    let o_prefix = named_node_split_prefix(nn);
                    prefix == &o_prefix
                } else {
                    false
                }
            }
            StoredBaseRDFNodeType::Blank => {
                matches!(o, Term::BlankNode(_))
            }
            StoredBaseRDFNodeType::Literal(l) => {
                if let Term::Literal(tl) = o {
                    tl.datatype() == l.as_ref()
                } else {
                    false
                }
            }
        };
        if ok {
            filtered.push(o);
        }
    }
    filtered
}

fn filter_subjects<'a>(
    subject_type: &StoredBaseRDFNodeType,
    subjects: &'a Vec<Subject>,
) -> Vec<&'a Subject> {
    let mut filtered = vec![];
    for s in subjects {
        #[allow(unreachable_patterns)]
        let ok = match s {
            Subject::NamedNode(nn) => {
                if let StoredBaseRDFNodeType::IRI(prefix) = subject_type {
                    let pre = named_node_split_prefix(nn);
                    &pre == prefix
                } else {
                    false
                }
            }
            Subject::BlankNode(_) => subject_type == &StoredBaseRDFNodeType::Blank,
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
    predicate_keep_rename: &Option<String>,
    object_keep_rename: &Option<String>,
) -> SolutionMappings {
    let mut columns_vec = vec![];
    let mut out_datatypes = HashMap::new();

    if let Some(subject_rename) = subject_keep_rename {
        out_datatypes.insert(
            subject_rename.to_string(),
            BaseRDFNodeType::None.into_default_stored_rdf_node_state(),
        );
        columns_vec.push(Column::new_empty(
            subject_rename.into(),
            &BaseRDFNodeType::None.default_stored_polars_data_type(),
        ))
    }
    if let Some(predicate_rename) = predicate_keep_rename {
        out_datatypes.insert(
            predicate_rename.to_string(),
            BaseRDFNodeType::None.into_default_stored_rdf_node_state(),
        );
        columns_vec.push(Column::new_empty(
            predicate_rename.into(),
            &BaseRDFNodeType::None.default_stored_polars_data_type(),
        ))
    }
    if let Some(object_rename) = object_keep_rename {
        let dt = BaseRDFNodeType::None;
        let polars_dt = dt.default_stored_polars_data_type();
        let use_datatype = dt.into_default_input_rdf_node_state();

        out_datatypes.insert(object_rename.to_string(), use_datatype);
        columns_vec.push(Column::new_empty(object_rename.into(), &polars_dt))
    }
    SolutionMappings::new(
        DataFrame::new(columns_vec).unwrap().lazy(),
        out_datatypes,
        0,
    )
}

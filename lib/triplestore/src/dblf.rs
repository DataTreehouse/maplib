use super::Triplestore;
use crate::errors::TriplestoreError;
use crate::sparql::QuerySettings;
use crate::storage::{LookupTarget, Triples};
use oxrdf::{NamedNode, NamedOrBlankNode, Term};
use polars::prelude::{
    as_struct, by_name, col, concat, lit, Expr, IntoLazy, LazyFrame, UnionArgs,
};
use polars_core::prelude::{Column, DataFrame};
use query_processing::expressions::{blank_node_enc, literal_enc, named_node_enc};
use query_processing::graph_patterns::{distinct, union};
use query_processing::pushdown_values::{is_code_type, term_base_type, PushdownValues};
use query_processing::type_constraints::{bt_as_constrained, PossibleTypes};
use representation::cats::Cats;
use representation::dataset::{NamedGraph, QueryGraph};
use representation::multitype::all_multi_cols;
use representation::solution_mapping::{BaseCatState, EagerSolutionMappings, SolutionMappings};
use representation::{
    BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, PREDICATE_COL_NAME, SUBJECT_COL_NAME,
};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use utils::fast_path::{self, FastPath};

impl Triplestore {
    /// The number of triples with the given predicate in the graph, or in the default graphs of
    /// the dataset.
    pub(crate) fn predicate_height(&self, predicate: &NamedNode, dataset: &QueryGraph) -> usize {
        let graphs: Vec<NamedGraph> = match dataset {
            QueryGraph::NamedGraph(ng) => vec![ng.clone()],
            QueryGraph::QueryDataset(qd) => qd
                .default
                .iter()
                .map(|nn| NamedGraph::from_maybe_named_node(Some(nn)))
                .collect(),
        };
        graphs
            .iter()
            .filter_map(|g| self.graph_triples_map.get(g))
            .filter_map(|m| m.get(predicate))
            .flat_map(|m| m.values())
            .map(|t| t.height())
            .sum()
    }

    /// The largest number of triples with the predicate that share a subject (`subject`) or an
    /// object, summed over the tables of the queried graphs: an upper bound on the matches of one
    /// solution on that position. None when a table does not know it cheaply.
    pub(crate) fn predicate_max_rows_per_key(
        &self,
        predicate: &NamedNode,
        subject: bool,
        dataset: &QueryGraph,
    ) -> Option<usize> {
        let graphs: Vec<NamedGraph> = match dataset {
            QueryGraph::NamedGraph(ng) => vec![ng.clone()],
            QueryGraph::QueryDataset(qd) => qd
                .default
                .iter()
                .map(|nn| NamedGraph::from_maybe_named_node(Some(nn)))
                .collect(),
        };
        let mut total = 0usize;
        for m in graphs
            .iter()
            .filter_map(|g| self.graph_triples_map.get(g))
            .filter_map(|m| m.get(predicate))
        {
            for t in m.values() {
                total = total.saturating_add(t.max_rows_per_key(subject)?);
            }
        }
        Some(total)
    }

    /// The predicates of the graph, together with the predicates of the included graphs.
    pub fn get_predicate_iris(
        &self,
        graph: &NamedGraph,
        include_graphs: &[NamedGraph],
    ) -> Result<Vec<NamedNode>, TriplestoreError> {
        self.check_graph_exists(graph)?;
        let mut iris = HashSet::new();
        for g in std::iter::once(graph).chain(include_graphs.iter()) {
            if let Some(m) = self.graph_triples_map.get(g) {
                for nn in m.keys() {
                    iris.insert(nn);
                }
            }
        }
        let mut iris: Vec<_> = iris.into_iter().cloned().collect();
        iris.sort();
        Ok(iris)
    }

    /// The triples with the predicate in the graph, merged with those in the included graphs.
    pub fn get_predicate_eager_solution_mappings(
        &self,
        predicate: &NamedNode,
        graph: &NamedGraph,
        include_graphs: &[NamedGraph],
    ) -> Result<Vec<EagerSolutionMappings>, TriplestoreError> {
        self.check_graph_exists(graph)?;
        let mut types = HashSet::new();
        for g in std::iter::once(graph).chain(include_graphs.iter()) {
            if let Some(map) = self.graph_triples_map.get(g).and_then(|m| m.get(predicate)) {
                for (s, o) in map.keys() {
                    types.insert((
                        PossibleTypes::singular(bt_as_constrained(s)),
                        PossibleTypes::singular(bt_as_constrained(o)),
                    ));
                }
            }
        }
        let query_settings = QuerySettings {
            include_graphs: include_graphs.to_vec(),
            max_rows: None,
            strict_project: false,
            federation: None,
            evaluation_hook: None,
        };
        let qg = self.query_graph(&None, Some(graph), &query_settings);
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
                &qg,
            )?;
            let eager_sm = sm.as_eager();
            eager_sms.push(eager_sm);
        }
        Ok(eager_sms)
    }

    #[allow(clippy::too_many_arguments)]
    fn get_predicate_lf<'c>(
        &self,
        predicate_uri: &NamedNode,
        keep_subject: bool,
        keep_predicate: bool,
        keep_object: bool,
        subjects: &Option<PushdownValues>,
        objects: &Option<PushdownValues>,
        subject_datatype_ctr: &Option<PossibleTypes>,
        object_datatype_ctr: &Option<PossibleTypes>,
        graph: &NamedGraph,
        cats: &'c Cats,
        encodings: &mut TermEncodings<'c>,
    ) -> Result<Option<Vec<HalfBakedSolutionMappings>>, TriplestoreError> {
        if !self.graph_triples_map.contains_key(graph) {
            return Ok(None);
        }
        let mut all_sms = vec![];
        if let Some(map) = self.graph_triples_map.get(graph) {
            if map.contains_key(predicate_uri) {
                let compatible_types = self.all_compatible_types(
                    predicate_uri,
                    subject_datatype_ctr,
                    object_datatype_ctr,
                    graph,
                );
                if let Some(m) = map.get(predicate_uri) {
                    if let Some(sms) = multiple_tt_to_lf(
                        m,
                        compatible_types,
                        subjects,
                        objects,
                        keep_subject,
                        keep_object,
                        cats,
                        encodings,
                    )? {
                        all_sms.extend(sms);
                    }
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

    /// The triples with the predicates (all predicates if None), whose subjects and objects are
    /// among the given terms (any if None), restricted to the types of the constraints.
    #[allow(clippy::too_many_arguments)]
    pub fn get_multi_predicates_solution_mappings(
        &self,
        predicate_uris: Option<Vec<NamedNode>>,
        subject_keep_rename: &Option<String>,
        predicate_keep_rename: &Option<String>,
        object_keep_rename: &Option<String>,
        subjects: &Option<Vec<NamedOrBlankNode>>,
        objects: &Option<Vec<Term>>,
        subject_datatype_ctr: &Option<PossibleTypes>,
        object_datatype_ctr: &Option<PossibleTypes>,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, TriplestoreError> {
        let subjects = subjects
            .as_ref()
            .map(|s| PushdownValues::from_terms(s.iter().map(|x| Term::from(x.clone())).collect()));
        let objects = objects
            .as_ref()
            .map(|o| PushdownValues::from_terms(o.iter().cloned().collect()));
        self.get_multi_predicates_solution_mappings_of_values(
            predicate_uris,
            subject_keep_rename,
            predicate_keep_rename,
            object_keep_rename,
            &subjects,
            &objects,
            subject_datatype_ctr,
            object_datatype_ctr,
            dataset,
        )
    }

    /// As [Self::get_multi_predicates_solution_mappings], with the subjects and objects given as
    /// pushed down values: their dictionary codes are looked up as they are.
    #[allow(clippy::too_many_arguments)]
    pub fn get_multi_predicates_solution_mappings_of_values(
        &self,
        predicate_uris: Option<Vec<NamedNode>>,
        subject_keep_rename: &Option<String>,
        predicate_keep_rename: &Option<String>,
        object_keep_rename: &Option<String>,
        subjects: &Option<PushdownValues>,
        objects: &Option<PushdownValues>,
        subject_datatype_ctr: &Option<PossibleTypes>,
        object_datatype_ctr: &Option<PossibleTypes>,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, TriplestoreError> {
        let named_graph = match dataset {
            QueryGraph::NamedGraph(ng) => ng,
            QueryGraph::QueryDataset(qd) => {
                // The default graph of the dataset is the RDF merge of the FROM graphs.
                let mut sms = vec![];
                for nn in &qd.default {
                    sms.push(self.get_multi_predicates_solution_mappings_of_values(
                        predicate_uris.clone(),
                        subject_keep_rename,
                        predicate_keep_rename,
                        object_keep_rename,
                        subjects,
                        objects,
                        subject_datatype_ctr,
                        object_datatype_ctr,
                        &QueryGraph::NamedGraph(NamedGraph::from_maybe_named_node(Some(nn))),
                    )?);
                }
                // A graph without matching triples gives an empty result with untyped (None)
                // columns. Merged with the typed results of the other graphs, they would make
                // every variable a multi-type that may be unbound, which joins with the same
                // variable elsewhere (e.g. a virtualized time series' external id) can not match.
                // They add no solutions, so they are left out, unless every graph is empty.
                if sms.len() > 1 {
                    let empty = |sm: &SolutionMappings| {
                        !sm.rdf_node_types.is_empty()
                            && sm.rdf_node_types.values().all(|t| t.is_none())
                    };
                    if sms.iter().any(|sm| !empty(sm)) {
                        sms.retain(|sm| !empty(sm));
                    } else {
                        sms.truncate(1);
                    }
                }
                return if sms.is_empty() {
                    Ok(create_empty_lf_datatypes(
                        subject_keep_rename,
                        predicate_keep_rename,
                        object_keep_rename,
                    ))
                } else if sms.len() == 1 {
                    Ok(sms.pop().unwrap())
                } else {
                    let sm = union(sms, false, self.global_cats.clone())
                        .map_err(|x| TriplestoreError::SparqlQueryError(x.to_string()))?;
                    distinct(sm).map_err(|x| TriplestoreError::SparqlQueryError(x.to_string()))
                };
            }
        };
        let predicate_uris = predicate_uris.unwrap_or(self.all_predicates(named_graph));
        let cats = self.global_cats.read()?;
        let mut encodings = TermEncodings::default();
        if predicate_keep_rename.is_some() {
            encodings.encode_iris(&predicate_uris, &cats);
        }

        let mut sms = vec![];
        if !(objects.as_ref().is_some_and(|o| o.is_empty()))
            || !(subjects.as_ref().is_some_and(|s| s.is_empty()))
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
                    named_graph,
                    &cats,
                    &mut encodings,
                )? {
                    sms.extend(sm);
                }
            }
        }

        // When the same variable occurs in several positions, slices where the types of those
        // positions cannot match yield no solutions. They must be dropped before deciding
        // whether a column needs to be multi-typed, or a column could be made a struct while
        // only one of its types remains.
        let same_subject_object =
            subject_keep_rename.is_some() && subject_keep_rename == object_keep_rename;
        let same_subject_predicate =
            subject_keep_rename.is_some() && subject_keep_rename == predicate_keep_rename;
        let same_object_predicate =
            object_keep_rename.is_some() && object_keep_rename == predicate_keep_rename;
        sms.retain(|sm| {
            // Unwraps are safe because kept columns have types
            !(same_subject_object
                && sm.subject_type.as_ref().unwrap() != sm.object_type.as_ref().unwrap())
                && !(same_subject_predicate
                    && sm.subject_type.as_ref().unwrap() != &BaseRDFNodeType::IRI)
                && !(same_object_predicate
                    && sm.object_type.as_ref().unwrap() != &BaseRDFNodeType::IRI)
        });

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
                // Handle when column names are identical but types are different
                if let Some(s) = &subject_keep_rename {
                    if let Some(o) = &object_keep_rename {
                        if s == o {
                            mappings =
                                mappings.filter(col(SUBJECT_COL_NAME).eq(col(OBJECT_COL_NAME)));
                        }
                    }
                    if let Some(p) = &predicate_keep_rename {
                        if s == p {
                            let expr = encodings.encode_iri(predicate.as_ref().unwrap(), &cats);
                            //Safety for unwrap: All predicates stored in triplestore are also in catmap
                            let expr = expr.unwrap();
                            mappings = mappings.filter(col(SUBJECT_COL_NAME).eq(expr));
                        }
                    }
                }
                if let Some(o) = &object_keep_rename {
                    if let Some(p) = &predicate_keep_rename {
                        if o == p {
                            let expr = encodings.encode_iri(predicate.as_ref().unwrap(), &cats);
                            //Safety for unwrap: All predicates stored in triplestore are also in catmap
                            let expr = expr.unwrap();
                            mappings = mappings.filter(col(OBJECT_COL_NAME).eq(expr));
                        }
                    }
                }

                if let Some(subject_type) = &subject_type {
                    if subject_need_multi {
                        mappings = unnest_non_multi_col(mappings, SUBJECT_COL_NAME, subject_type);
                    }
                    subject_types.insert(subject_type.clone(), subject_state.unwrap().clone());
                }
                if predicate_keep_rename.is_some() {
                    let enc = encodings
                        .encode_iri(predicate.as_ref().unwrap(), &cats)
                        .unwrap();
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
                        strict: false,
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
                if !rename_trg.contains(&v) {
                    keep.push(PREDICATE_COL_NAME);
                    if v != PREDICATE_COL_NAME {
                        rename_src.push(PREDICATE_COL_NAME);
                        rename_trg.push(v);
                    }
                }
            }
            if let Some(o) = object_keep_rename {
                if !rename_trg.contains(&o) {
                    keep.push(OBJECT_COL_NAME);
                    if o != OBJECT_COL_NAME {
                        rename_src.push(OBJECT_COL_NAME);
                        rename_trg.push(o);
                    }
                }
            }
            if keep.is_empty() {
                // A pattern without variables has a single empty solution if it matches.
                // A frame without columns must have its height set explicitly.
                let height = mappings
                    .limit(1)
                    .collect()
                    .map_err(|x| TriplestoreError::SparqlQueryError(x.to_string()))?
                    .height();
                mappings = DataFrame::empty_with_height(height).lazy();
            } else {
                let keep_exprs: Vec<_> = keep.iter().map(|x| col(*x)).collect();
                mappings = mappings.select(keep_exprs);
            }
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
                if !types.contains_key(predicate_col_name) {
                    types.insert(
                        predicate_col_name.clone(),
                        RDFNodeState::from_bases(
                            BaseRDFNodeType::IRI,
                            BaseCatState::CategoricalNative(None),
                        ),
                    );
                }
            }
            if let Some(object_col_name) = object_keep_rename {
                if !types.contains_key(object_col_name) {
                    let object_type_map: HashMap<_, _> = sorted_object_types.into_iter().collect();
                    types.insert(
                        object_col_name.clone(),
                        RDFNodeState::from_map(object_type_map),
                    );
                }
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
        subject_datatype_ctr: &Option<PossibleTypes>,
        object_datatype_ctr: &Option<PossibleTypes>,
        graph: &NamedGraph,
    ) -> Option<HashSet<(BaseRDFNodeType, BaseRDFNodeType)>> {
        if subject_datatype_ctr.is_none() && object_datatype_ctr.is_none() {
            return None;
        }
        let mut compat_types = HashSet::new();
        let use_map = self.graph_triples_map.get(graph).unwrap();

        if let Some(map) = use_map.get(predicate_uri) {
            for (subject_type, object_type) in map.keys() {
                if let Some(subject_datatype_ctr) = subject_datatype_ctr {
                    if !subject_datatype_ctr.compatible_with(&bt_as_constrained(subject_type)) {
                        continue;
                    }
                }
                if let Some(object_datatype_ctr) = object_datatype_ctr {
                    if !object_datatype_ctr.compatible_with(&bt_as_constrained(object_type)) {
                        continue;
                    }
                }
                compat_types.insert((subject_type.clone(), object_type.clone()));
            }
        }
        Some(compat_types)
    }

    pub(crate) fn all_predicates(&self, graph: &NamedGraph) -> Vec<NamedNode> {
        let mut predicates = HashSet::new();
        if let Some(g) = self.graph_triples_map.get(graph) {
            for nn in g.keys() {
                predicates.insert(nn);
            }
        }
        let predicates: Vec<_> = predicates.into_iter().cloned().collect();
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
    mappings = mappings.drop(by_name(drop_cols, true, false));
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

/// The subjects or objects a table is looked up by.
enum Targets {
    /// Global dictionary codes, sorted, for a column of codes.
    Codes(Arc<Vec<u32>>),
    /// Terms of the type of a column that does not hold codes.
    Terms(Vec<Term>),
}

/// An expression for the column value of a single target, None if there are several. The
/// inner None is for a term that is not in the dictionary.
fn single_target_expr(
    target: &Targets,
    global_cats: &Cats,
    encodings: &mut TermEncodings,
) -> Result<Option<Option<Expr>>, TriplestoreError> {
    Ok(match target {
        Targets::Codes(c) if c.len() == 1 => {
            Some(Some(lit(c[0]).cast(polars_core::prelude::DataType::UInt32)))
        }
        Targets::Terms(t) if t.len() == 1 => Some(encodings.encode(&t[0], global_cats)?),
        _ => None,
    })
}

fn single_triples_to_lf<'c>(
    triples: &Triples,
    subjects: Option<&Targets>,
    objects: Option<&Targets>,
    global_cats: &'c Cats,
    encodings: &mut TermEncodings<'c>,
) -> Result<Option<(LazyFrame, usize)>, TriplestoreError> {
    // A single IRI subject in triples held in memory: find its rows eagerly, so that tables
    // without the subject are left out of the query plan. Planning a union over every table of
    // a variable predicate costs far more than these lookups.
    if let (Some(Targets::Codes(codes)), true) = (subjects, triples.subject_type.is_iri()) {
        if let [code] = codes.as_slice() {
            if !triples.may_hold_code(*code, false) {
                return Ok(None);
            }
            encodings.decode_codes(&BaseRDFNodeType::IRI, &[*code], global_cats);
            let Some(string) = encodings.string(&BaseRDFNodeType::IRI, *code) else {
                return Ok(None);
            };
            let string = string.to_string();
            if let Some(dfs) = triples.get_in_memory_subject_rows(&string, *code)? {
                let mut dfs = dfs.into_iter();
                let Some(mut df) = dfs.next() else {
                    return Ok(None);
                };
                for other in dfs {
                    df.vstack_mut_owned(other)
                        .map_err(TriplestoreError::LazyLoadError)?;
                }
                let height = df.height();
                let mut lf = df.lazy();
                if let Some(objects) = objects {
                    if let Some(enc) = single_target_expr(objects, global_cats, encodings)? {
                        lf = match enc {
                            Some(enc) => lf.filter(col(OBJECT_COL_NAME).eq(enc)),
                            None => lf.filter(lit(false)),
                        };
                    }
                }
                return Ok(Some((lf, height)));
            }
        }
    }
    // The codes that some segment (or the write buffer) of the table may hold. Only those are
    // decoded, to the strings of the sparse indexes. Objects that are not codes are not looked
    // up by: their column has no sparse index.
    let may_hold = |targets: Option<&Targets>, by_object: bool| -> Option<Vec<u32>> {
        let Some(Targets::Codes(codes)) = targets else {
            return None;
        };
        // Every coded target is checked against the filters: checking a code costs no
        // dictionary lookup, so there is no cap on the number of targets.
        fast_path::hit_when(triples.has_code_filters(by_object), FastPath::FilterTargetCodes);
        let kept: Vec<u32> = codes
            .iter()
            .copied()
            .filter(|c| triples.may_hold_code(*c, by_object))
            .collect();
        fast_path::hit_when(kept.len() < codes.len(), FastPath::TableFilterSkip);
        Some(kept)
    };
    let subject_codes = may_hold(subjects, false);
    let object_codes = if subject_codes.is_none() {
        may_hold(objects, true)
    } else {
        None
    };
    if subject_codes.as_ref().is_some_and(|t| t.is_empty())
        || object_codes.as_ref().is_some_and(|t| t.is_empty())
    {
        return Ok(None);
    }
    if let Some(c) = &subject_codes {
        encodings.decode_codes(&triples.subject_type, c, global_cats);
    }
    if let Some(c) = &object_codes {
        encodings.decode_codes(&triples.object_type, c, global_cats);
    }
    let lfs_and_heights = {
        let lookup =
            |codes: &Option<Vec<u32>>, bt: &BaseRDFNodeType| -> Option<Vec<LookupTarget>> {
                codes.as_ref().map(|codes| {
                    codes
                        .iter()
                        .filter_map(|c| encodings.string(bt, *c).map(|s| (s, Some(*c))))
                        .collect()
                })
            };
        let subject_lookup = lookup(&subject_codes, &triples.subject_type);
        let object_lookup = lookup(&object_codes, &triples.object_type);
        if subject_lookup.as_ref().is_some_and(|t| t.is_empty())
            || object_lookup.as_ref().is_some_and(|t| t.is_empty())
        {
            return Ok(None);
        }
        triples.get_lazy_frames_of_targets(subject_lookup.as_deref(), object_lookup.as_deref())?
    };
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
                strict: false,
            },
        )
        .unwrap()
    } else {
        lfs.pop().unwrap()
    };
    // A single subject or object (e.g. a constant of the triple pattern) is filtered on here, as
    // the sparse index ranges hold other rows too.
    for (targets, c) in [(subjects, SUBJECT_COL_NAME), (objects, OBJECT_COL_NAME)] {
        if let Some(targets) = targets {
            if let Some(enc) = single_target_expr(targets, global_cats, encodings)? {
                lf = match enc {
                    Some(enc) => lf.filter(col(c).eq(enc)),
                    None => lf.filter(lit(false)),
                };
            }
        }
    }
    Ok(Some((lf, new_height)))
}

/// The encodings of the constant terms of a triple pattern, and the strings of the pushed down
/// codes. Each encoding or decoding looks the dictionaries up, which on disk is a database
/// query, so it is done once per pattern rather than once per predicate and type of table.
#[derive(Default)]
struct TermEncodings<'c> {
    encodings: HashMap<Term, Option<Expr>>,
    /// The global codes of the pushed down subjects (false) or objects (true) of each type of
    /// table, see [PushdownValues::codes_of_type].
    target_codes: HashMap<(bool, BaseRDFNodeType), Arc<Vec<u32>>>,
    /// The strings of the codes that were decoded or encoded, per type.
    strings: HashMap<BaseRDFNodeType, HashMap<u32, Cow<'c, str>>>,
}

impl<'c> TermEncodings<'c> {
    fn encode(
        &mut self,
        term: &Term,
        global_cats: &Cats,
    ) -> Result<Option<Expr>, TriplestoreError> {
        if let Some(enc) = self.encodings.get(term) {
            return Ok(enc.clone());
        }
        let enc = match term {
            Term::NamedNode(nn) => named_node_enc(nn, global_cats),
            Term::BlankNode(bl) => blank_node_enc(bl, global_cats),
            Term::Literal(l) => {
                let (e, _, _) = literal_enc(l, global_cats)
                    .map_err(|x| TriplestoreError::SparqlQueryError(x.to_string()))?;
                Some(e)
            }
        };
        self.encodings.insert(term.clone(), enc.clone());
        Ok(enc)
    }

    fn encode_iri(&mut self, nn: &NamedNode, global_cats: &Cats) -> Option<Expr> {
        let term = Term::NamedNode(nn.clone());
        if let Some(enc) = self.encodings.get(&term) {
            return enc.clone();
        }
        let enc = named_node_enc(nn, global_cats);
        self.encodings.insert(term, enc.clone());
        enc
    }

    /// The global codes of the values of type `bt` (sorted), found once per type: the codes
    /// pushed down as they are, and the codes of the terms (the constants of the query), whose
    /// strings are kept so that they need not be decoded.
    fn codes_of_type(
        &mut self,
        values: &PushdownValues,
        by_object: bool,
        bt: &BaseRDFNodeType,
        global_cats: &Cats,
    ) -> Arc<Vec<u32>> {
        let key = (by_object, bt.clone());
        if let Some(c) = self.target_codes.get(&key) {
            return c.clone();
        }
        let mut codes = values.codes.get(bt).cloned().unwrap_or_default();
        let terms: Vec<&str> = values
            .terms
            .iter()
            .filter(|t| &term_base_type(t) == bt)
            .map(|t| match t {
                Term::NamedNode(nn) => nn.as_str(),
                Term::BlankNode(bl) => bl.as_str(),
                Term::Literal(l) => l.value(),
            })
            .collect();
        if !terms.is_empty() {
            let strs: Vec<Option<&str>> = terms.iter().map(|s| Some(*s)).collect();
            let found = match bt {
                BaseRDFNodeType::IRI => global_cats.maybe_encode_iri_slice(&strs),
                BaseRDFNodeType::BlankNode => global_cats.maybe_encode_blanks(&strs),
                BaseRDFNodeType::Literal(dt, _) => {
                    global_cats.maybe_encode_literals(&strs, dt.clone())
                }
                BaseRDFNodeType::None => vec![None; strs.len()],
            };
            let strings = self.strings.entry(bt.clone()).or_default();
            for (s, c) in terms.into_iter().zip(found) {
                if let Some(c) = c {
                    strings.insert(c, Cow::Owned(s.to_string()));
                    codes.push(c);
                }
            }
            codes.sort_unstable();
            codes.dedup();
        }
        let codes = Arc::new(codes);
        self.target_codes.insert(key, codes.clone());
        codes
    }

    /// Finds the strings of the codes of type `bt` that were not decoded or encoded before,
    /// with one dictionary lookup.
    fn decode_codes(&mut self, bt: &BaseRDFNodeType, codes: &[u32], global_cats: &'c Cats) {
        let strings = self.strings.entry(bt.clone()).or_default();
        let missing: Vec<Option<u32>> = codes
            .iter()
            .filter(|c| !strings.contains_key(*c))
            .map(|c| Some(*c))
            .collect();
        if missing.is_empty() {
            return;
        }
        if let Some(enc) = global_cats.get_enc(bt) {
            for (c, s) in missing.iter().zip(enc.maps.decode_batch(&missing)) {
                if let Some(s) = s {
                    strings.insert(c.unwrap(), s);
                }
            }
        }
    }

    /// The string of a code of type `bt` found by [Self::decode_codes] or
    /// [Self::codes_of_type], None if it is not in the dictionary.
    fn string(&self, bt: &BaseRDFNodeType, code: u32) -> Option<&str> {
        self.strings.get(bt)?.get(&code).map(|s| s.as_ref())
    }

    /// Encodes the IRIs with a single dictionary lookup.
    fn encode_iris(&mut self, nns: &[NamedNode], global_cats: &Cats) {
        let strs: Vec<_> = nns.iter().map(|nn| Some(nn.as_str())).collect();
        let encs = global_cats.maybe_encode_iri_slice(&strs);
        for (nn, enc) in nns.iter().zip(encs) {
            self.encodings.insert(
                Term::NamedNode(nn.clone()),
                enc.map(|enc| lit(enc).cast(polars_core::prelude::DataType::UInt32)),
            );
        }
    }
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

/// Whether the column of a table of type `bt` holds global dictionary codes that pushed down
/// codes can be compared with.
fn holds_codes(bt: &BaseRDFNodeType) -> bool {
    is_code_type(bt)
        && matches!(
            bt.default_stored_cat_state(),
            BaseCatState::CategoricalNative(None)
        )
}

/// The targets of the pushed down values that a column of type `bt` can hold, None if there
/// are none.
fn targets_of_type(
    values: &PushdownValues,
    bt: &BaseRDFNodeType,
    by_object: bool,
    global_cats: &Cats,
    encodings: &mut TermEncodings,
) -> Option<Targets> {
    if holds_codes(bt) {
        let codes = encodings.codes_of_type(values, by_object, bt, global_cats);
        (!codes.is_empty()).then_some(Targets::Codes(codes))
    } else {
        let terms: Vec<Term> = values
            .terms
            .iter()
            .filter(|t| term_matches_type(t, bt))
            .cloned()
            .collect();
        (!terms.is_empty()).then_some(Targets::Terms(terms))
    }
}

#[allow(clippy::too_many_arguments)]
fn multiple_tt_to_lf<'c>(
    triples: &HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>,
    types: Option<HashSet<(BaseRDFNodeType, BaseRDFNodeType)>>,
    subjects: &Option<PushdownValues>,
    objects: &Option<PushdownValues>,
    keep_subject: bool,
    keep_object: bool,
    global_cats: &'c Cats,
    encodings: &mut TermEncodings<'c>,
) -> Result<Option<Vec<HalfBakedSolutionMappings>>, TriplestoreError> {
    let mut filtered = vec![];
    for ((subj_type, obj_type), tt) in triples.iter() {
        if let Some(types) = &types {
            if !types.contains(&(subj_type.clone(), obj_type.clone())) {
                continue;
            }
        }
        let subject_targets = match subjects {
            Some(s) => match targets_of_type(s, subj_type, false, global_cats, encodings) {
                Some(t) => Some(t),
                None => continue,
            },
            None => None,
        };
        let object_targets = match objects {
            Some(o) => match targets_of_type(o, obj_type, true, global_cats, encodings) {
                Some(t) => Some(t),
                None => continue,
            },
            None => None,
        };
        if let Some((lf, height)) = single_triples_to_lf(
            tt,
            subject_targets.as_ref(),
            object_targets.as_ref(),
            global_cats,
            encodings,
        )? {
            if height > 0 {
                let mut select = vec![];
                if keep_subject {
                    select.push(col(SUBJECT_COL_NAME));
                }
                if keep_object {
                    select.push(col(OBJECT_COL_NAME));
                }
                if select.is_empty() {
                    // Keep a column so that the number of matches is not lost
                    select.push(col(SUBJECT_COL_NAME));
                }

                let subject_state = subj_type.default_stored_cat_state();
                let object_state = obj_type.default_stored_cat_state();

                let half_baked = HalfBakedSolutionMappings {
                    mappings: lf.select(select),
                    predicate: None,
                    subject_type: if keep_subject {
                        Some(subj_type.clone())
                    } else {
                        None
                    },
                    object_type: if keep_object {
                        Some(obj_type.clone())
                    } else {
                        None
                    },
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

/// Whether the term can be a value of a column of type `bt`.
fn term_matches_type(t: &Term, bt: &BaseRDFNodeType) -> bool {
    match t {
        Term::NamedNode(..) => bt.is_iri(),
        Term::BlankNode(..) => bt.is_blank_node(),
        Term::Literal(l) => {
            if let BaseRDFNodeType::Literal(dt, _) = bt {
                l.datatype() == dt.as_ref()
            } else {
                false
            }
        }
    }
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
    // The same variable may occur in several positions, but needs only one column
    if let Some(predicate_rename) = predicate_keep_rename
        .as_ref()
        .filter(|x| !out_datatypes.contains_key(*x))
    {
        out_datatypes.insert(
            predicate_rename.to_string(),
            BaseRDFNodeType::None.into_default_stored_rdf_node_state(),
        );
        columns_vec.push(Column::new_empty(
            predicate_rename.into(),
            &BaseRDFNodeType::None.default_stored_polars_data_type(),
        ))
    }
    if let Some(object_rename) = object_keep_rename
        .as_ref()
        .filter(|x| !out_datatypes.contains_key(*x))
    {
        let dt = BaseRDFNodeType::None;
        let polars_dt = dt.default_stored_polars_data_type();
        let use_datatype = dt.into_default_input_rdf_node_state();

        out_datatypes.insert(object_rename.to_string(), use_datatype);
        columns_vec.push(Column::new_empty(object_rename.into(), &polars_dt))
    }

    SolutionMappings::new(
        DataFrame::new(0, columns_vec).unwrap().lazy(),
        out_datatypes,
        0,
    )
}

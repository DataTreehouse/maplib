use utils::polars::CollectAuto;
use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::QuerySettings;
use crate::storage::Triples;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Term, Variable};
use polars::prelude::{
    by_name, col, lit, ChunkAgg, DataFrame, IdxCa, IntoColumn, IntoLazy, JoinType, UInt32Chunked,
    UniqueKeepStrategy,
};
use polars_core::prelude::{Column, DataType, ExplodeOptions};
use query_processing::errors::QueryProcessingError;
use query_processing::expressions::{literal_enc, named_node_enc, named_node_local_enc};
use query_processing::graph_patterns::{distinct, group_by_workaround, join, union};
use query_processing::pushdowns::{Pushdowns, CHECK_SMALL_HEIGHT};
use rayon::prelude::*;
use representation::cats::{Cats, LockedCats};
use representation::dataset::{NamedGraph, QueryGraph};
use representation::multitype::{
    compress_actual_multitypes, force_convert_multicol_to_single_col, nest_multicolumns,
};
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{BaseCatState, EagerSolutionMappings, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeState};
use representation::{OBJECT_COL_NAME, SUBJECT_COL_NAME};
use spargebra::algebra::{Expression, GraphPattern, PropertyPathExpression};
use spargebra::term::{GroundTerm, NamedNodePattern, TermPattern, TriplePattern};
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use utils::fast_path::{self, FastPath};

const NAMED_NODE_INDEX_COL: &str = "named_node_index_column";
pub(crate) const VALUE_COLUMN: &str = "value";
pub(crate) const LOOKUP_COLUMN: &str = "key";

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum PathLeaf {
    NamedNode(NamedNode),
    NegatedSet(Vec<NamedNode>),
}

impl PathLeaf {
    fn negated(nns: &[NamedNode]) -> Self {
        let mut v = nns.to_vec();
        v.sort();
        v.dedup();
        PathLeaf::NegatedSet(v)
    }
}

impl Triplestore {
    #[allow(clippy::too_many_arguments)]
    pub fn lazy_path(
        &self,
        subject: &TermPattern,
        ppe: &PropertyPathExpression,
        object: &TermPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        // A sequence is a join of its steps (SPARQL 1.1, 18.4): it has a solution for each
        // intermediate node, also when a step is an arbitrary length path, whose own solutions
        // are distinct pairs. Steps that need the closure are evaluated as paths of their own.
        let normalised = push_reverse_down(ppe, false);
        if need_sparse_matrix(&normalised)
            && matches!(normalised, PropertyPathExpression::Sequence(..))
            && !is_repetition(&normalised)
        {
            let mut intermediaries = vec![];
            let gp = sequence_steps(&normalised, subject, object, &mut intermediaries);
            let mut sms = self.lazy_graph_pattern(
                &gp,
                None,
                &context.extension_with(PathEntry::PathRewrite),
                None,
                pushdowns,
                query_settings,
                dataset,
            )?;
            for i in &intermediaries {
                sms.rdf_node_types.remove(i);
            }
            sms.mappings = sms.mappings.drop(by_name(intermediaries, false, false));
            if let Some(solution_mappings) = solution_mappings {
                sms = join(
                    sms,
                    solution_mappings,
                    JoinType::Inner,
                    self.global_cats.clone(),
                    query_settings.max_rows,
                )?;
            }
            return Ok(sms);
        }
        let create_sparse = need_sparse_matrix(ppe);

        let mut var_cols = vec![];
        match subject {
            TermPattern::BlankNode(b) => {
                var_cols.push(b.to_string());
            }
            TermPattern::Variable(v) => {
                var_cols.push(v.as_str().to_string());
            }
            _ => {}
        }

        match object {
            TermPattern::BlankNode(b) => {
                var_cols.push(b.to_string());
            }
            TermPattern::Variable(v) => {
                var_cols.push(v.as_str().to_string());
            }
            _ => {}
        }

        if !create_sparse {
            let mut intermediaries = vec![];
            let mut gp = create_graph_pattern(ppe, subject, object, &mut intermediaries);
            gp = GraphPattern::Distinct {
                inner: Box::new(gp),
            };
            let mut sms = self.lazy_graph_pattern(
                &gp,
                None,
                &context.extension_with(PathEntry::PathRewrite),
                None,
                pushdowns,
                query_settings,
                dataset,
            )?;
            for i in &intermediaries {
                sms.rdf_node_types.remove(i).unwrap();
            }
            sms.mappings = sms.mappings.drop(by_name(intermediaries, true, false));
            if let Some(solution_mappings) = solution_mappings {
                sms = join(
                    sms,
                    solution_mappings,
                    JoinType::Inner,
                    self.global_cats.clone(),
                    query_settings.max_rows,
                )?;
            }
            return Ok(sms);
        }

        let mut out_df;

        // The nodes that an end of the path is bound to, as global dictionary codes. When an
        // end's column in the solution mappings so far holds IRIs with global codes, its codes
        // are read from the column, rather than pushed down as terms and encoded again.
        let mut pushdowns = pushdowns;
        let mut mapped_codes = [None, None];
        let solution_mappings = match solution_mappings {
            Some(sm)
                if sm.height_estimate <= CHECK_SMALL_HEIGHT
                    && [subject, object]
                        .into_iter()
                        .any(|t| global_iri_column(t, &sm.rdf_node_types).is_some()) =>
            {
                let eager = sm.as_eager();
                mapped_codes = [
                    mapped_iri_codes(subject, &eager),
                    mapped_iri_codes(object, &eager),
                ];
                Some(eager.as_lazy())
            }
            Some(sm) => {
                let focus: Vec<&str> = [subject, object]
                    .into_iter()
                    .filter_map(|t| match t {
                        TermPattern::Variable(v) => Some(v.as_str()),
                        _ => None,
                    })
                    .collect();
                // Paths walk outward from the pushed values, so no table size limits them.
                Some(pushdowns.add_from_solution_mappings(
                    sm,
                    self.global_cats.clone(),
                    &focus,
                    None,
                ))
            }
            None => None,
        };
        // With neither end bound, a path that matches with zero length pairs every node of the
        // graph with itself (see below): these pairs come from the node set, so they are left
        // out of the paths that are followed, and the two parts need no DISTINCT.
        let every_node = matches_zero_length(ppe)
            && [subject, object].into_iter().all(|t| {
                term_variable_name(t).is_some_and(|v| {
                    !pushdowns.variables_values.contains_key(&v)
                        && !solution_mappings
                            .as_ref()
                            .is_some_and(|sm| sm.rdf_node_types.contains_key(&v))
                })
            });
        let start_codes = {
            let cats = self.global_cats.read().unwrap();
            let [mapped_subject, mapped_object] = mapped_codes;
            fewer(
                mapped_subject,
                bound_iri_codes(subject, &pushdowns, &cats),
            )
            .map(|s| (s, false))
            .into_iter()
            .chain(
                fewer(mapped_object, bound_iri_codes(object, &pushdowns, &cats))
                    .map(|o| (o, true)),
            )
            // The end with fewer nodes, the subject on a tie.
            .min_by_key(|(c, reversed)| (c.len(), *reversed))
            .map(|(mut c, reversed)| {
                c.sort_unstable();
                c.dedup();
                (c, reversed)
            })
        };

        // From a few nodes, the paths are followed on dictionary codes, reading the edges of each
        // step from the sparse indexes of the triple tables. Past a budget of lookups, keying
        // the whole tables (below) is cheaper. A path that matches with zero length would match
        // each start to itself, also a start that is not a node of the graph: that is only
        // right for a constant start, which matches itself anyway (see below).
        let walked = match &start_codes {
            Some((starts, reversed))
                if starts.len() <= WALK_MAX_STARTS
                    && (!matches_zero_length(ppe)
                        || is_constant(if *reversed { object } else { subject })) =>
            {
                let cats = self.global_cats.read().unwrap();
                CodeWalk::new(self, ppe, *reversed, dataset, &cats)
                    .and_then(|walk| walk.paths_from(ppe, starts, *reversed).ok())
                    .inspect(|_| fast_path::hit(FastPath::PathCodeWalk))
                    .or_else(|| fast_path::hit_none(FastPath::PathCodeWalkAborted))
            }
            _ => None,
        };

        let value_state;
        let (subjects, objects) = if let Some((subject_vec, object_vec)) = walked {
            value_state = RDFNodeState::from_bases(
                BaseRDFNodeType::IRI,
                BaseCatState::CategoricalNative(None),
            );
            (
                UInt32Chunked::from_vec(SUBJECT_COL_NAME.into(), subject_vec).into_column(),
                UInt32Chunked::from_vec(OBJECT_COL_NAME.into(), object_vec).into_column(),
            )
        } else {
            let mut df_creator = U32DataFrameCreator::new(dataset.clone());
            df_creator.gather_namednode_dfs(ppe, self)?;
            let (lookup_df, lookup_dtypes, namednode_dfs) =
                df_creator.create_u32_dfs(self.global_cats.clone())?;
            value_state = lookup_dtypes.get(VALUE_COLUMN).unwrap().clone();
            // Row i of the lookup table holds the value with key i.
            let n_values = lookup_df.height();
            let bound = start_codes.and_then(|(codes, reversed)| {
                Some((
                    lookup_keys_of_iris(&lookup_df, &lookup_dtypes, codes)?,
                    reversed,
                ))
            });
            let (mut subject_vec, mut object_vec) = if let Some((starts, reversed)) = bound {
                fast_path::hit(FastPath::PathKeyedBound);
                // Only the paths from the nodes an end is bound to are followed.
                paths_from(ppe, starts, reversed, &namednode_dfs, n_values)
            } else {
                // Every path begins at a node of its first step, when that is a single predicate
                // (e.g. `rdf:rest+`), and otherwise at any node.
                fast_path::hit(FastPath::PathKeyedUnbound);
                let starts = first_step_starts(ppe, &namednode_dfs)
                    .unwrap_or_else(|| (0..n_values as u32).collect());
                paths_from(ppe, starts, false, &namednode_dfs, n_values)
            };
            if every_node {
                // Equal keys are equal nodes.
                let (s, o): (Vec<u32>, Vec<u32>) = subject_vec
                    .iter()
                    .zip(object_vec.iter())
                    .filter(|(s, o)| s != o)
                    .map(|(s, o)| (*s, *o))
                    .unzip();
                subject_vec = s;
                object_vec = o;
            }
            utils::cancel::check()?;
            // Keys without a value (none should occur) are left out rather than gathered.
            let n_values = n_values as u32;
            if subject_vec
                .iter()
                .chain(object_vec.iter())
                .any(|k| *k >= n_values)
            {
                let (s, o): (Vec<u32>, Vec<u32>) = subject_vec
                    .iter()
                    .zip(object_vec.iter())
                    .filter(|(s, o)| **s < n_values && **o < n_values)
                    .map(|(s, o)| (*s, *o))
                    .unzip();
                subject_vec = s;
                object_vec = o;
            }
            // Keys are mapped to values by gathering rows instead of joining.
            let values = lookup_df.column(VALUE_COLUMN).unwrap();
            (
                values
                    .take(&IdxCa::from_vec("".into(), subject_vec))
                    .unwrap()
                    .with_name(SUBJECT_COL_NAME.into()),
                values
                    .take(&IdxCa::from_vec("".into(), object_vec))
                    .unwrap()
                    .with_name(OBJECT_COL_NAME.into()),
            )
        };
        utils::cancel::check()?;
        let height = subjects.len();
        out_df = DataFrame::new(height, vec![subjects, objects]).unwrap();
        // With the same variable at both ends, the path must end where it starts.
        let same_variable = matches!(
            (term_variable_name(subject), term_variable_name(object)),
            (Some(s), Some(o)) if s == o
        );
        if same_variable {
            out_df = out_df
                .lazy()
                .filter(col(SUBJECT_COL_NAME).eq(col(OBJECT_COL_NAME)))
                .collect_auto()
                .unwrap();
        }
        let mut dtypes = HashMap::new();
        dtypes.insert(SUBJECT_COL_NAME.to_string(), value_state.clone());
        dtypes.insert(OBJECT_COL_NAME.to_string(), value_state.clone());

        if value_state.is_multi() {
            // A constant end keeps only the values of its type. When the values have no such
            // type there are no solutions, and the column is left as it is: converting it to a
            // type it does not have would fail.
            for (term, column) in [(subject, SUBJECT_COL_NAME), (object, OBJECT_COL_NAME)] {
                let wanted = match term {
                    TermPattern::NamedNode(_) => Some(BaseRDFNodeType::IRI),
                    TermPattern::Literal(l) => {
                        let exact = BaseRDFNodeType::from_literal(l);
                        Some(if value_state.map.contains_key(&exact) {
                            exact
                        } else {
                            // The same datatype with another time zone
                            value_state
                                .map
                                .keys()
                                .find(|t| matches!(t, BaseRDFNodeType::Literal(dt, _) if dt.as_ref() == l.datatype()))
                                .cloned()
                                .unwrap_or(exact)
                        })
                    }
                    _ => None,
                };
                let Some(wanted) = wanted else { continue };
                if value_state.map.contains_key(&wanted) {
                    out_df = force_convert_multicol_to_single_col(out_df.lazy(), column, &wanted)
                        .collect_auto()
                        .unwrap();
                    dtypes.insert(
                        column.to_string(),
                        wanted.into_default_stored_rdf_node_state(),
                    );
                } else {
                    out_df = out_df.clear();
                }
            }
        }
        (out_df, dtypes) = compress_actual_multitypes(out_df, dtypes);
        let out_dt_subj = dtypes.remove(SUBJECT_COL_NAME).unwrap();
        let out_dt_obj = dtypes.remove(OBJECT_COL_NAME).unwrap();

        let mut var_cols = vec![];
        let mut rename_src = vec![];
        let mut rename_trg = vec![];

        let mut constant_match_height = 0;
        let cats = self.global_cats.read().unwrap();
        match subject {
            TermPattern::NamedNode(nn) => {
                // A column of other types (e.g. only literals, or none at all when nothing
                // matched) has no IRIs, and cannot be compared with one.
                let e = if has_only_type(&out_dt_subj, &BaseRDFNodeType::IRI) {
                    named_node_enc(nn, &cats).map(|e| col(SUBJECT_COL_NAME).eq(e))
                } else {
                    None
                };
                out_df = out_df
                    .lazy()
                    .filter(e.unwrap_or(lit(false)))
                    .collect_auto()
                    .unwrap();
                out_df = out_df.drop(SUBJECT_COL_NAME).unwrap();
            }
            TermPattern::BlankNode(b) => {
                let bname = b.to_string();
                var_cols.push(bname.clone());
                rename_src.push(SUBJECT_COL_NAME);
                rename_trg.push(bname);
            }
            TermPattern::Literal(l) => {
                // Comparing a column of another type with the literal fails (e.g. the Boolean
                // column of a path without solutions with 1.0): such a column has no match.
                let e = if has_only_literal_datatype(&out_dt_subj, l) {
                    let (l, ..) = literal_enc(l, &cats)?;
                    col(SUBJECT_COL_NAME).eq(l)
                } else {
                    lit(false)
                };
                out_df = out_df.lazy().filter(e).collect_auto().unwrap();
                out_df = out_df.drop(SUBJECT_COL_NAME).unwrap();
            }
            TermPattern::Variable(v) => {
                var_cols.push(v.as_str().to_string());
                if v.as_str() != SUBJECT_COL_NAME {
                    rename_src.push(SUBJECT_COL_NAME);
                    rename_trg.push(v.as_str().into());
                }
            }
        }

        match object {
            TermPattern::NamedNode(nn) => {
                // A column of other types (e.g. only literals, or none at all when nothing
                // matched) has no IRIs, and cannot be compared with one.
                let e = if has_only_type(&out_dt_obj, &BaseRDFNodeType::IRI) {
                    named_node_enc(nn, &cats).map(|e| col(OBJECT_COL_NAME).eq(e))
                } else {
                    None
                };
                out_df = out_df
                    .lazy()
                    .filter(e.unwrap_or(lit(false)))
                    .collect_auto()
                    .unwrap();
                constant_match_height = out_df.height();
                out_df = out_df.drop(OBJECT_COL_NAME).unwrap();
            }
            // The subject column already holds the variable, and equals the object column.
            TermPattern::BlankNode(_) | TermPattern::Variable(_) if same_variable => {
                out_df = out_df.drop(OBJECT_COL_NAME).unwrap();
            }
            TermPattern::BlankNode(b) => {
                let bname = b.to_string();
                var_cols.push(bname.clone());
                rename_src.push(OBJECT_COL_NAME);
                rename_trg.push(bname);
            }
            TermPattern::Literal(l) => {
                // Comparing a column of another type with the literal fails (e.g. the Boolean
                // column of a path without solutions with 1.0): such a column has no match.
                let e = if has_only_literal_datatype(&out_dt_obj, l) {
                    let (l, ..) = literal_enc(l, &cats)?;
                    col(OBJECT_COL_NAME).eq(l)
                } else {
                    lit(false)
                };
                out_df = out_df.lazy().filter(e).collect_auto().unwrap();
                constant_match_height = out_df.height();
                out_df = out_df.drop(OBJECT_COL_NAME).unwrap();
            }
            TermPattern::Variable(v) => {
                var_cols.push(v.as_str().to_string());
                if v.as_str() != OBJECT_COL_NAME {
                    rename_src.push(OBJECT_COL_NAME);
                    rename_trg.push(v.as_str().into());
                }
            }
        }

        let mut datatypes = HashMap::new();
        if let TermPattern::Variable(v) = subject {
            datatypes.insert(v.as_str().to_string(), out_dt_subj);
        } else if let TermPattern::BlankNode(b) = subject {
            datatypes.insert(b.to_string(), out_dt_subj);
        }
        if same_variable {
            // Only the subject column is kept.
        } else if let TermPattern::Variable(v) = object {
            datatypes.insert(v.as_str().to_string(), out_dt_obj);
        } else if let TermPattern::BlankNode(b) = object {
            datatypes.insert(b.to_string(), out_dt_obj);
        }
        let subject_constant = is_constant(subject);
        let object_constant = is_constant(object);
        if subject_constant && object_constant {
            // A frame without columns must have its height set explicitly.
            let matches =
                constant_match_height > 0 || (matches_zero_length(ppe) && subject == object);
            out_df = DataFrame::empty_with_height(if matches { 1 } else { 0 });
        }
        let out_df_height = out_df.height();

        let mut out_lf = out_df.lazy();
        if !rename_src.is_empty() {
            out_lf = out_lf.rename(rename_src, rename_trg, true);
        }
        let mut path_solution_mappings = SolutionMappings {
            mappings: out_lf,
            rdf_node_types: datatypes,
            height_estimate: out_df_height,
        };
        // A zero length path from a constant term matches the term itself,
        // even if the term does not occur in the graph.
        if matches_zero_length(ppe) && subject_constant != object_constant {
            let (constant, variable) = if subject_constant {
                (subject, object)
            } else {
                (object, subject)
            };
            let variable = match variable {
                TermPattern::Variable(v) => v.as_str().to_string(),
                TermPattern::BlankNode(b) => b.to_string(),
                _ => unreachable!("Should never happen"),
            };
            let (expr, state) = match constant {
                TermPattern::NamedNode(nn) => {
                    let (e, local) = named_node_local_enc(nn, &cats);
                    (
                        e,
                        RDFNodeState::from_bases(
                            BaseRDFNodeType::IRI,
                            BaseCatState::CategoricalNative(local.map(LockedCats::new)),
                        ),
                    )
                }
                TermPattern::Literal(l) => {
                    let (e, bt, bs) = literal_enc(l, &cats)?;
                    (e, RDFNodeState::from_bases(bt, bs))
                }
                _ => unreachable!("Should never happen"),
            };
            drop(cats);
            let zero_length = SolutionMappings::new(
                DataFrame::empty_with_height(1)
                    .lazy()
                    .with_column(expr.alias(&variable)),
                HashMap::from([(variable, state)]),
                1,
            );
            path_solution_mappings = distinct(union(
                vec![path_solution_mappings, zero_length],
                false,
                self.global_cats.clone(),
            )?)?;
        } else {
            drop(cats);
        }
        // A zero length path from a variable matches each node of the graph (SPARQL 1.1 section
        // 18.4), also nodes without triples of the path. When the variable is bound by the
        // solution mappings so far, or its values are pushed down (as by VALUES), its values that
        // are nodes of the graph are added.
        let mut solution_mappings = solution_mappings;
        if matches_zero_length(ppe) && !subject_constant && !object_constant {
            let s = term_variable_name(subject).unwrap();
            let o = term_variable_name(object).unwrap();
            if let Some(sm) = solution_mappings.take() {
                // The mappings are read twice, so they are computed once; a cancelled or timed
                // out query stops here with its error instead of panicking.
                solution_mappings = Some(super::path_search::collect_once(sm)?);
            }
            let in_mappings = solution_mappings.as_ref().and_then(|sm| {
                [&s, &o]
                    .into_iter()
                    .find(|v| sm.rdf_node_types.contains_key(*v))
                    .cloned()
            });
            let bound = in_mappings.clone().or_else(|| {
                [&s, &o]
                    .into_iter()
                    .find(|v| pushdowns.variables_values.contains_key(*v))
                    .cloned()
            });
            // With neither end bound, every node of the graph (each subject and object) is a
            // zero length match, as in Oxigraph and Jena.
            let all_nodes = bound.is_none();
            let zero_length = if let Some(b) = bound {
                let sm = if in_mappings.is_some() {
                    solution_mappings.as_ref()
                } else {
                    None
                };
                self.zero_length_nodes(
                    &b,
                    &s,
                    &o,
                    sm,
                    Some(&pushdowns),
                    context,
                    query_settings,
                    dataset,
                )?
            } else {
                debug_assert!(every_node);
                self.graph_nodes(&s, &o, dataset)?
            };
            if let Some(zero_length) = zero_length {
                path_solution_mappings = union(
                    vec![path_solution_mappings, zero_length],
                    false,
                    self.global_cats.clone(),
                )?;
                // The paths followed from every node are distinct and have no node paired with
                // itself, and the nodes are distinct.
                if !all_nodes {
                    path_solution_mappings = distinct(path_solution_mappings)?;
                }
            }
        }
        if let Some(mappings) = solution_mappings {
            path_solution_mappings = join(
                path_solution_mappings,
                mappings,
                JoinType::Inner,
                self.global_cats.clone(),
                query_settings.max_rows,
            )?;
        }
        Ok(path_solution_mappings)
    }
}

impl Triplestore {
    /// The values of the variable `b` that are nodes of the queried graph, as (`s`, `o`) pairs of
    /// zero length paths. The values are those of `b` in `solution_mappings`, or else those pushed
    /// down for it, or else (without `pushdowns`) all nodes of the graph. None if there are none
    /// to check.
    #[allow(clippy::too_many_arguments)]
    fn zero_length_nodes(
        &self,
        b: &str,
        s: &str,
        o: &str,
        solution_mappings: Option<&SolutionMappings>,
        pushdowns: Option<&Pushdowns>,
        context: &Context,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<Option<SolutionMappings>, SparqlError> {
        let var = Variable::new_unchecked(b);
        let context = context.extension_with(PathEntry::PathRewrite);
        let mut node_pushdowns = Pushdowns::new();
        let candidates = if let Some(sm) = solution_mappings {
            let state = sm.rdf_node_types.get(b).unwrap().clone();
            let candidates = distinct(SolutionMappings::new(
                sm.mappings
                    .clone()
                    .filter(col(b).is_not_null())
                    .select([col(b)]),
                HashMap::from([(b.to_string(), state)]),
                sm.height_estimate,
            ))?;
            let df = candidates.mappings.collect_auto().unwrap();
            let height = df.height();
            let candidates = SolutionMappings::new(df.lazy(), candidates.rdf_node_types, height);
            // Small sets of values restrict the scan for nodes below.
            Some(node_pushdowns.add_from_solution_mappings(
                candidates,
                self.global_cats.clone(),
                &[b],
                None,
            ))
        } else if let Some(pushdowns) = pushdowns {
            let Some(values) = pushdowns.variables_values.get(b) else {
                return Ok(None);
            };
            let terms = values.to_terms(&self.global_cats.read().unwrap());
            let bindings: Vec<Vec<Option<GroundTerm>>> = terms
                .iter()
                .filter_map(|t| match t {
                    Term::NamedNode(nn) => Some(vec![Some(GroundTerm::NamedNode(nn.clone()))]),
                    Term::Literal(l) => Some(vec![Some(GroundTerm::Literal(l.clone()))]),
                    _ => None,
                })
                .collect();
            if bindings.is_empty() {
                return Ok(None);
            }
            node_pushdowns
                .variables_values
                .insert(b.to_string(), values.clone());
            Some(self.lazy_graph_pattern(
                &GraphPattern::Values {
                    variables: vec![var.clone()],
                    bindings,
                },
                None,
                &context,
                None,
                Pushdowns::new(),
                query_settings,
                dataset,
            )?)
        } else {
            None
        };
        // The values that occur as the subject or the object of a triple.
        let fresh = || {
            TermPattern::Variable(Variable::new_unchecked(format!(
                "v{}",
                uuid::Uuid::new_v4().simple()
            )))
        };
        let fresh_predicate = || {
            NamedNodePattern::Variable(Variable::new_unchecked(format!(
                "v{}",
                uuid::Uuid::new_v4().simple()
            )))
        };
        let bound = TermPattern::Variable(var.clone());
        let nodes_gp = GraphPattern::Distinct {
            inner: Box::new(GraphPattern::Project {
                inner: Box::new(GraphPattern::Union {
                    left: Box::new(GraphPattern::Bgp {
                        patterns: vec![TriplePattern {
                            subject: bound.clone(),
                            predicate: fresh_predicate(),
                            object: fresh(),
                        }],
                    }),
                    right: Box::new(GraphPattern::Bgp {
                        patterns: vec![TriplePattern {
                            subject: fresh(),
                            predicate: fresh_predicate(),
                            object: bound,
                        }],
                    }),
                }),
                variables: vec![var],
            }),
        };
        let nodes = self.lazy_graph_pattern(
            &nodes_gp,
            None,
            &context,
            None,
            node_pushdowns,
            query_settings,
            dataset,
        )?;
        let mut zero_length = if let Some(candidates) = candidates {
            join(
                nodes,
                candidates,
                JoinType::Inner,
                self.global_cats.clone(),
                query_settings.max_rows,
            )?
        } else {
            nodes
        };
        let state = zero_length.rdf_node_types.remove(b).unwrap();
        let mut exprs = vec![col(b).alias(s)];
        let mut types = HashMap::from([(s.to_string(), state.clone())]);
        if o != s {
            exprs.push(col(b).alias(o));
            types.insert(o.to_string(), state);
        }
        zero_length.mappings = zero_length.mappings.select(exprs);
        zero_length.rdf_node_types = types;
        Ok(Some(zero_length))
    }
}

impl Triplestore {
    /// Every node of the queried graph (each subject and each object of a triple), as (`s`, `o`)
    /// pairs of zero length paths; None if the graph has no triples.
    ///
    /// The nodes are gathered per stored type from the subject and object columns of the triple
    /// tables, as they are stored. Terms of different stored types are different terms, so the
    /// distinct values of each type are the distinct nodes. IRIs, blank nodes and strings are
    /// stored as dictionary codes, whose distinct values are the bits set in a bitset over the
    /// codes: the cost is a pass over the stored columns, without building rows of all triples.
    fn graph_nodes(
        &self,
        s: &str,
        o: &str,
        dataset: &QueryGraph,
    ) -> Result<Option<SolutionMappings>, SparqlError> {
        let graphs: Vec<NamedGraph> = match dataset {
            QueryGraph::NamedGraph(g) => vec![g.clone()],
            QueryGraph::QueryDataset(qd) => qd
                .default
                .iter()
                .map(|nn| NamedGraph::from_maybe_named_node(Some(nn)))
                .collect(),
        };
        let mut by_type: HashMap<&BaseRDFNodeType, Vec<Column>> = HashMap::new();
        for types in graphs
            .iter()
            .filter_map(|g| self.graph_triples_map.get(g))
            .flat_map(|predicates| predicates.values())
        {
            for ((subject_type, object_type), triples) in types {
                by_type
                    .entry(subject_type)
                    .or_default()
                    .extend(triples.stored_columns(SUBJECT_COL_NAME)?);
                by_type
                    .entry(object_type)
                    .or_default()
                    .extend(triples.stored_columns(OBJECT_COL_NAME)?);
            }
        }
        let mut by_type: Vec<_> = by_type.into_iter().collect();
        by_type.sort_by(|(a, _), (b, _)| a.cmp(b));
        let nodes = by_type
            .into_par_iter()
            .map(|(ty, columns)| Ok((ty, distinct_stored_values(ty, columns, s)?)))
            .collect::<Result<Vec<(&BaseRDFNodeType, Column)>, SparqlError>>()?;
        utils::cancel::check()?;
        let sms: Vec<SolutionMappings> = nodes
            .into_iter()
            .filter(|(_, c)| !c.is_empty())
            .map(|(ty, c)| {
                let height = c.len();
                SolutionMappings::new(
                    DataFrame::new(height, vec![c]).unwrap().lazy(),
                    HashMap::from([(
                        s.to_string(),
                        ty.clone().into_default_stored_rdf_node_state(),
                    )]),
                    height,
                )
            })
            .collect();
        if sms.is_empty() {
            return Ok(None);
        }
        fast_path::hit(FastPath::PathNodesFromStoredColumns);
        let mut nodes = union(sms, false, self.global_cats.clone())?;
        if o != s {
            let state = nodes.rdf_node_types.get(s).unwrap().clone();
            nodes.mappings = nodes.mappings.with_column(col(s).alias(o));
            nodes.rdf_node_types.insert(o.to_string(), state);
        }
        Ok(Some(nodes))
    }
}

/// The distinct values of the stored columns of one type, as a column named `name`.
fn distinct_stored_values(
    ty: &BaseRDFNodeType,
    columns: Vec<Column>,
    name: &str,
) -> Result<Column, SparqlError> {
    if ty.stored_cat()
        && !ty.is_lang_string()
        && columns.iter().all(|c| c.dtype() == &DataType::UInt32)
    {
        let chunked: Vec<&UInt32Chunked> = columns.iter().map(|c| c.u32().unwrap()).collect();
        return Ok(UInt32Chunked::from_vec(name.into(), distinct_codes(&chunked)).into_column());
    }
    let mut columns = columns.into_iter();
    let mut values = columns
        .next()
        .expect("A type has at least one column")
        .take_materialized_series();
    for c in columns {
        values
            .append(c.as_materialized_series())
            .map_err(crate::errors::TriplestoreError::LazyLoadError)?;
    }
    values.rename(name.into());
    let height = values.len();
    let df = DataFrame::new(height, vec![values.into_column()])
        .unwrap()
        .lazy()
        .unique(None, UniqueKeepStrategy::Any)
        .collect_auto()
        .map_err(crate::errors::TriplestoreError::LazyLoadError)?;
    Ok(df.column(name).unwrap().clone())
}

/// The distinct codes of the columns, sorted: the bits set in a bitset over the codes, or, for
/// codes too sparse for a bitset, the codes sorted.
fn distinct_codes(columns: &[&UInt32Chunked]) -> Vec<u32> {
    let rows: usize = columns.iter().map(|c| c.len() - c.null_count()).sum();
    let Some(max) = columns.iter().filter_map(|c| c.max()).max() else {
        return vec![];
    };
    // Arrays with nulls are not expected in stored columns.
    let for_each_code = |f: &mut dyn FnMut(u32)| {
        for c in columns {
            for arr in c.downcast_iter() {
                if arr.validity().is_none() {
                    arr.values().as_slice().iter().for_each(|u| f(*u));
                } else {
                    arr.iter().flatten().for_each(|u| f(*u));
                }
            }
        }
    };
    let words = (max as usize >> 6) + 1;
    // A bitset of at most 2 bytes per code (plus 512 KiB), else sort.
    if words > rows / 4 + (1 << 16) {
        let mut out: Vec<u32> = Vec::with_capacity(rows);
        for_each_code(&mut |u| out.push(u));
        out.par_sort_unstable();
        out.dedup();
        return out;
    }
    let mut bits = vec![0u64; words];
    for_each_code(&mut |u| bits[(u >> 6) as usize] |= 1u64 << (u & 63));
    let n: usize = bits.iter().map(|w| w.count_ones() as usize).sum();
    let mut out = Vec::with_capacity(n);
    for (i, mut word) in bits.into_iter().enumerate() {
        while word != 0 {
            out.push(((i as u32) << 6) | word.trailing_zeros());
            word &= word - 1;
        }
    }
    out
}

pub fn create_graph_pattern(
    ppe: &PropertyPathExpression,
    subject: &TermPattern,
    object: &TermPattern,
    intermediaries: &mut Vec<String>,
) -> GraphPattern {
    match ppe {
        PropertyPathExpression::NamedNode(nn) => GraphPattern::Bgp {
            patterns: vec![TriplePattern {
                subject: subject.clone(),
                predicate: NamedNodePattern::NamedNode(nn.to_owned()),
                object: object.clone(),
            }],
        },
        PropertyPathExpression::Reverse(r) => {
            create_graph_pattern(r, object, subject, intermediaries)
        }
        PropertyPathExpression::Sequence(l, r) => {
            let intermediary = format!("v{}", uuid::Uuid::new_v4());
            let intermediary_tp = TermPattern::Variable(Variable::new_unchecked(&intermediary));
            intermediaries.push(intermediary);
            GraphPattern::Join {
                left: Box::new(create_graph_pattern(
                    l,
                    subject,
                    &intermediary_tp,
                    intermediaries,
                )),
                right: Box::new(create_graph_pattern(
                    r,
                    &intermediary_tp,
                    object,
                    intermediaries,
                )),
            }
        }
        PropertyPathExpression::Alternative(a, b) => GraphPattern::Union {
            left: Box::new(create_graph_pattern(a, subject, object, intermediaries)),
            right: Box::new(create_graph_pattern(b, subject, object, intermediaries)),
        },
        PropertyPathExpression::ZeroOrMore(_)
        | PropertyPathExpression::OneOrMore(_)
        | PropertyPathExpression::ZeroOrOne(_) => {
            panic!("Should never happen")
        }
        PropertyPathExpression::NegatedPropertySet(nns) => {
            let pred_name = format!("v{}", uuid::Uuid::new_v4());
            let pred_var = Variable::new_unchecked(&pred_name);
            intermediaries.push(pred_name);
            let excluded = nns
                .iter()
                .map(|nn| Expression::NamedNode(nn.clone()))
                .collect();
            GraphPattern::Filter {
                expr: Expression::Not(Box::new(Expression::In(
                    Box::new(Expression::Variable(pred_var.clone())),
                    excluded,
                ))),
                inner: Box::new(GraphPattern::Bgp {
                    patterns: vec![TriplePattern {
                        subject: subject.clone(),
                        predicate: NamedNodePattern::Variable(pred_var),
                        object: object.clone(),
                    }],
                }),
            }
        }
    }
}

/// Moves inverses (`^`) through sequences and alternatives to the steps: `^(a/b)` is `^b/^a`.
fn push_reverse_down(ppe: &PropertyPathExpression, reversed: bool) -> PropertyPathExpression {
    use PropertyPathExpression as P;
    match ppe {
        P::Reverse(inner) => push_reverse_down(inner, !reversed),
        P::Sequence(a, b) if reversed => P::Sequence(
            Box::new(push_reverse_down(b, true)),
            Box::new(push_reverse_down(a, true)),
        ),
        P::Sequence(a, b) => P::Sequence(
            Box::new(push_reverse_down(a, false)),
            Box::new(push_reverse_down(b, false)),
        ),
        P::Alternative(a, b) => P::Alternative(
            Box::new(push_reverse_down(a, reversed)),
            Box::new(push_reverse_down(b, reversed)),
        ),
        other if reversed => P::Reverse(Box::new(other.clone())),
        other => other.clone(),
    }
}

/// A sequence path as the join of its steps through fresh intermediate variables; each step that
/// is not itself a sequence is a path pattern of its own.
fn sequence_steps(
    ppe: &PropertyPathExpression,
    subject: &TermPattern,
    object: &TermPattern,
    intermediaries: &mut Vec<String>,
) -> GraphPattern {
    match ppe {
        PropertyPathExpression::Sequence(l, r) => {
            let intermediary = format!("v{}", uuid::Uuid::new_v4());
            let intermediary_tp = TermPattern::Variable(Variable::new_unchecked(&intermediary));
            intermediaries.push(intermediary);
            GraphPattern::Join {
                left: Box::new(sequence_steps(l, subject, &intermediary_tp, intermediaries)),
                right: Box::new(sequence_steps(r, &intermediary_tp, object, intermediaries)),
            }
        }
        // A predicate or its inverse is a triple pattern, as in an explicit join.
        step if is_predicate_step(step) => {
            create_graph_pattern(step, subject, object, intermediaries)
        }
        // An inverse step is the path with its ends swapped, as the query rewriting does for an
        // inverse path pattern (the path evaluation is not used with an inverse at the top).
        PropertyPathExpression::Reverse(inner) => GraphPattern::Path {
            subject: object.clone(),
            path: inner.as_ref().clone(),
            object: subject.clone(),
        },
        step => GraphPattern::Path {
            subject: subject.clone(),
            path: step.clone(),
            object: object.clone(),
        },
    }
}

/// Whether a sequence is a length range of one step `x`: `x/.../x` followed by `x?/.../x?`, or by
/// a single `x+`. The parser writes `x{n,m}` and `x{n,}` so (see `repeat_path` in spargebra), and
/// length ranges have set semantics like the other arbitrary length paths, so they are not
/// evaluated as a join of their steps. The same sequences written out by hand are treated alike.
fn is_repetition(ppe: &PropertyPathExpression) -> bool {
    fn flatten<'a>(p: &'a PropertyPathExpression, out: &mut Vec<&'a PropertyPathExpression>) {
        if let PropertyPathExpression::Sequence(a, b) = p {
            flatten(a, out);
            flatten(b, out);
        } else {
            out.push(p);
        }
    }
    let mut steps = vec![];
    flatten(ppe, &mut steps);
    let (base, plus) = match steps.last() {
        Some(PropertyPathExpression::ZeroOrOne(x)) => (x.as_ref(), false),
        Some(PropertyPathExpression::OneOrMore(x)) => (x.as_ref(), true),
        _ => return false,
    };
    let rest = &steps[..steps.len() - 1];
    if plus {
        return rest.iter().all(|s| *s == base);
    }
    // Copies of x first, then copies of x?.
    let optional = |s: &&PropertyPathExpression| {
        matches!(s, PropertyPathExpression::ZeroOrOne(x) if x.as_ref() == base)
    };
    let first_optional = rest.iter().position(optional).unwrap_or(rest.len());
    rest[..first_optional].iter().all(|s| *s == base) && rest[first_optional..].iter().all(optional)
}

/// A single predicate, forwards or backwards.
fn is_predicate_step(ppe: &PropertyPathExpression) -> bool {
    match ppe {
        PropertyPathExpression::NamedNode(_) => true,
        PropertyPathExpression::Reverse(inner) => {
            matches!(inner.as_ref(), PropertyPathExpression::NamedNode(_))
        }
        _ => false,
    }
}

pub fn need_sparse_matrix(ppe: &PropertyPathExpression) -> bool {
    match ppe {
        PropertyPathExpression::NamedNode(_) => false,
        PropertyPathExpression::Reverse(inner) => need_sparse_matrix(inner),
        PropertyPathExpression::Sequence(left, right) => {
            need_sparse_matrix(left) || need_sparse_matrix(right)
        }
        PropertyPathExpression::Alternative(a, b) => need_sparse_matrix(a) || need_sparse_matrix(b),
        PropertyPathExpression::ZeroOrMore(_) => true,
        PropertyPathExpression::OneOrMore(_) => true,
        PropertyPathExpression::ZeroOrOne(_) => true,
        PropertyPathExpression::NegatedPropertySet(_) => false,
    }
}

pub(crate) struct U32DataFrameCreator {
    pub named_nodes: HashMap<PathLeaf, (DataFrame, RDFNodeState, RDFNodeState)>,
    dataset: QueryGraph,
}

impl U32DataFrameCreator {
    pub fn new(dataset: QueryGraph) -> Self {
        U32DataFrameCreator {
            named_nodes: Default::default(),
            dataset,
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn create_u32_dfs(
        self,
        global_cats: LockedCats,
    ) -> Result<
        (
            DataFrame,
            HashMap<String, RDFNodeState>,
            HashMap<PathLeaf, DataFrame>,
        ),
        QueryProcessingError,
    > {
        if let Some(out) = single_cat_type_keys(&self.named_nodes) {
            return Ok(out);
        }
        // TODO! Possible to constrain lookup to only nodes that may occur as subj/obj in path expr.
        // Can reduce size of a join
        let mut nns: Vec<_> = self.named_nodes.keys().cloned().collect();
        nns.sort();

        let mut soln_mappings = vec![];
        for (nn, (df, subject_dt, object_dt)) in self.named_nodes {
            let nn_idx = nns.iter().position(|x| x == &nn).unwrap();
            let df_height = df.height();
            let mut lf = df.lazy();
            lf = lf.with_column(lit(nn_idx as u32).alias(NAMED_NODE_INDEX_COL));
            let mut types = HashMap::new();
            types.insert(
                NAMED_NODE_INDEX_COL.to_string(),
                BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned(), None)
                    .into_default_stored_rdf_node_state(),
            );
            types.insert(SUBJECT_COL_NAME.to_string(), subject_dt);
            types.insert(OBJECT_COL_NAME.to_string(), object_dt);

            soln_mappings.push(SolutionMappings::new(lf, types, df_height));
        }
        let SolutionMappings {
            mut mappings,
            rdf_node_types,
            ..
        } = union(soln_mappings, false, global_cats.clone())?;

        let subject_row_index = uuid::Uuid::new_v4().to_string();
        let object_row_index = uuid::Uuid::new_v4().to_string();
        let mut df = mappings.collect_auto().unwrap();
        let df_height = df.height();
        mappings = df.lazy();
        mappings = mappings
            .with_row_index(&subject_row_index, None)
            .with_column(
                (col(&subject_row_index) + lit(df_height as u32)).alias(&object_row_index),
            );
        df = mappings.collect_auto().unwrap();

        let lf_subj = df.clone().lazy().select([
            col(SUBJECT_COL_NAME).alias(VALUE_COLUMN),
            col(&subject_row_index),
        ]);
        let lf_obj = df.clone().lazy().select([
            col(OBJECT_COL_NAME).alias(VALUE_COLUMN),
            col(&object_row_index),
        ]);

        let mut subj_types = HashMap::new();
        subj_types.insert(
            VALUE_COLUMN.to_string(),
            rdf_node_types.get(SUBJECT_COL_NAME).unwrap().clone(),
        );
        subj_types.insert(
            subject_row_index.clone(),
            BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned(), None)
                .into_default_stored_rdf_node_state(),
        );

        let mut obj_types = HashMap::new();
        obj_types.insert(
            VALUE_COLUMN.to_string(),
            rdf_node_types.get(OBJECT_COL_NAME).unwrap().clone(),
        );
        obj_types.insert(
            object_row_index.clone(),
            BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned(), None)
                .into_default_stored_rdf_node_state(),
        );

        let subj_soln_mappings = SolutionMappings::new(lf_subj, subj_types, df_height);
        let obj_soln_mappings = SolutionMappings::new(lf_obj, obj_types, df_height);
        let SolutionMappings {
            mut mappings,
            rdf_node_types: lookup_df_types,
            ..
        } = union(
            vec![subj_soln_mappings, obj_soln_mappings],
            false,
            global_cats.clone(),
        )?;
        let (mappings_grby, maps) =
            group_by_workaround(mappings, &lookup_df_types, vec![VALUE_COLUMN.to_string()]);
        mappings = mappings_grby.agg([
            col(&subject_row_index).alias(&subject_row_index),
            col(&object_row_index).alias(&object_row_index),
        ]);
        mappings = nest_multicolumns(mappings, maps);

        // One row per distinct value, so the key of a value is its row number.
        mappings = mappings.with_row_index(LOOKUP_COLUMN, None);
        let grouped = mappings.collect_auto().unwrap();

        // Each exploded row is one row of the union: exactly one of the two row indices is set,
        // and together they cover 0..2 * df_height. Scatter the keys into a dense array, so that
        // row i of df has subject key keys[i] and object key keys[df_height + i].
        let exploded = grouped
            .clone()
            .lazy()
            .select([
                col(LOOKUP_COLUMN),
                col(&subject_row_index),
                col(&object_row_index),
            ])
            .explode(
                by_name([&subject_row_index, &object_row_index], true, false),
                ExplodeOptions {
                    empty_as_null: false,
                    keep_nulls: true,
                },
            )
            .collect()
            .unwrap();
        let mut keys = vec![0u32; 2 * df_height];
        let key_ca = exploded.column(LOOKUP_COLUMN).unwrap().idx().unwrap();
        let subj_ca = exploded
            .column(&subject_row_index)
            .unwrap()
            .cast(&DataType::UInt32)
            .unwrap();
        let obj_ca = exploded
            .column(&object_row_index)
            .unwrap()
            .cast(&DataType::UInt32)
            .unwrap();
        for ((k, s), o) in key_ca
            .iter()
            .zip(subj_ca.u32().unwrap().iter())
            .zip(obj_ca.u32().unwrap().iter())
        {
            let pos = s.or(o).unwrap() as usize;
            keys[pos] = k.unwrap();
        }
        let object_keys = keys.split_off(df_height);
        let df = DataFrame::new(
            df_height,
            vec![
                df.column(NAMED_NODE_INDEX_COL).unwrap().clone(),
                UInt32Chunked::from_vec(SUBJECT_COL_NAME.into(), keys).into_column(),
                UInt32Chunked::from_vec(OBJECT_COL_NAME.into(), object_keys).into_column(),
            ],
        )
        .unwrap();

        let out_dfs = df.partition_by([NAMED_NODE_INDEX_COL], true).unwrap();
        let mut out_df_map = HashMap::new();
        for mut df in out_dfs {
            let nn_ser = df.drop_in_place(NAMED_NODE_INDEX_COL).unwrap();
            //TODO: Investigate why cast is needed..
            let nn_idx = nn_ser
                .cast(&DataType::UInt32)
                .unwrap()
                .u32()
                .unwrap()
                .get(0)
                .unwrap();
            out_df_map.insert(nns.get(nn_idx as usize).unwrap().clone(), df);
        }
        // Row i of the lookup table holds the value with key i.
        let lookup_df = grouped
            .drop(&subject_row_index)
            .unwrap()
            .drop(&object_row_index)
            .unwrap();
        Ok((lookup_df, lookup_df_types, out_df_map))
    }

    pub(crate) fn gather_namednode_dfs(
        &mut self,
        ppe: &PropertyPathExpression,
        triplestore: &Triplestore,
    ) -> Result<(), SparqlError> {
        match ppe {
            PropertyPathExpression::NamedNode(nn) => {
                let leaf = PathLeaf::NamedNode(nn.clone());
                let predicates = vec![nn.clone()];

                if self.named_nodes.contains_key(&leaf) {
                    return Ok(());
                }
                let SolutionMappings {
                    mappings,
                    mut rdf_node_types,
                    ..
                } = triplestore.get_multi_predicates_solution_mappings(
                    Some(predicates),
                    &Some(SUBJECT_COL_NAME.to_string()),
                    &None,
                    &Some(OBJECT_COL_NAME.to_string()),
                    &None,
                    &None,
                    &None,
                    &None,
                    &self.dataset,
                )?;
                let df = mappings.collect_auto().unwrap();
                self.named_nodes.insert(
                    leaf,
                    (
                        df,
                        rdf_node_types.remove(SUBJECT_COL_NAME).unwrap(),
                        rdf_node_types.remove(OBJECT_COL_NAME).unwrap(),
                    ),
                );
                Ok(())
            }
            PropertyPathExpression::Reverse(inner) => self.gather_namednode_dfs(inner, triplestore),
            PropertyPathExpression::Sequence(left, right) => {
                self.gather_namednode_dfs(left, triplestore)?;
                self.gather_namednode_dfs(right, triplestore)?;
                Ok(())
            }
            PropertyPathExpression::Alternative(left, right) => {
                self.gather_namednode_dfs(left, triplestore)?;
                self.gather_namednode_dfs(right, triplestore)?;
                Ok(())
            }
            PropertyPathExpression::ZeroOrMore(inner) => {
                self.gather_namednode_dfs(inner, triplestore)
            }
            PropertyPathExpression::OneOrMore(inner) => {
                self.gather_namednode_dfs(inner, triplestore)
            }
            PropertyPathExpression::ZeroOrOne(inner) => {
                self.gather_namednode_dfs(inner, triplestore)
            }
            PropertyPathExpression::NegatedPropertySet(nns) => {
                let graphs: Vec<NamedGraph> = match &self.dataset {
                    QueryGraph::NamedGraph(g) => vec![g.clone()],
                    QueryGraph::QueryDataset(qd) => qd
                        .default
                        .iter()
                        .map(|nn| NamedGraph::from_maybe_named_node(Some(nn)))
                        .collect(),
                };
                let mut allowed: Vec<NamedNode> = graphs
                    .iter()
                    .flat_map(|g| triplestore.all_predicates(g))
                    .filter(|p| !nns.contains(p))
                    .collect();
                allowed.sort();
                allowed.dedup();

                if self.named_nodes.contains_key(&PathLeaf::negated(nns)) {
                    return Ok(());
                }
                let SolutionMappings {
                    mappings,
                    mut rdf_node_types,
                    ..
                } = triplestore.get_multi_predicates_solution_mappings(
                    Some(allowed),
                    &Some(SUBJECT_COL_NAME.to_string()),
                    &None,
                    &Some(OBJECT_COL_NAME.to_string()),
                    &None,
                    &None,
                    &None,
                    &None,
                    &self.dataset,
                )?;
                let df = mappings.collect_auto().unwrap();
                self.named_nodes.insert(
                    PathLeaf::negated(nns),
                    (
                        df,
                        rdf_node_types.remove(SUBJECT_COL_NAME).unwrap(),
                        rdf_node_types.remove(OBJECT_COL_NAME).unwrap(),
                    ),
                );
                Ok(())
            }
        }
    }
}

/// The closure of a step that is not a single predicate (e.g. `(a/b)+`) is found by first
/// finding the step from every node, when the paths start from at least this fraction
/// (1 / factor) of all nodes. From fewer, the newly reached nodes are followed one step at a time.
const SELECTIVE_START_FACTOR: usize = 8;

/// Paths are followed on dictionary codes from at most this many starts (see [CodeWalk]).
const WALK_MAX_STARTS: usize = 64;
/// A walk on dictionary codes is given up for keying the whole tables after this many lookups of
/// a table segment, or after reading this many rows. Keying `rdf:rest` (48k triples) of RINF 0076
/// costs about as much as a few hundred lookups.
const WALK_MAX_LOOKUPS: usize = 256;
const WALK_MAX_ROWS: usize = 1 << 16;

/// The sorted keys of the nodes that every path matching `ppe` starts from, when its first step
/// is a single predicate (forwards or backwards); None when a path can start anywhere, e.g. with
/// a zero-length match or an alternative.
fn first_step_starts(
    ppe: &PropertyPathExpression,
    namednode_map: &HashMap<PathLeaf, DataFrame>,
) -> Option<Vec<u32>> {
    let (leaf, column) = match ppe {
        PropertyPathExpression::Sequence(first, _) => {
            return first_step_starts(first, namednode_map)
        }
        PropertyPathExpression::OneOrMore(inner) => return first_step_starts(inner, namednode_map),
        PropertyPathExpression::NamedNode(nn) => (PathLeaf::NamedNode(nn.clone()), SUBJECT_COL_NAME),
        PropertyPathExpression::Reverse(inner) => match inner.as_ref() {
            PropertyPathExpression::NamedNode(nn) => {
                (PathLeaf::NamedNode(nn.clone()), OBJECT_COL_NAME)
            }
            _ => return None,
        },
        _ => return None,
    };
    // A leaf without triples has no frame: no path can start.
    let Some(df) = namednode_map.get(&leaf) else {
        return Some(vec![]);
    };
    let mut keys: Vec<u32> = df.column(column).ok()?.u32().ok()?.iter().flatten().collect();
    keys.sort_unstable();
    keys.dedup();
    Some(keys)
}

/// Lookup keys for path leaves whose subjects and objects all have one and the same type with
/// global dictionary codes (e.g. IRIs only, as in `rdf:rest+`): the key of a value is the rank of
/// its code among the distinct codes, found by sorting the codes. This gives the same tables as
/// the general keying in [`U32DataFrameCreator::create_u32_dfs`] (a lookup table with one row per
/// value, and the leaves' edges as keys) without the union and group by, which dominated paths
/// from a bound start. None for anything else, e.g. several types or local codes.
#[allow(clippy::type_complexity)]
fn single_cat_type_keys(
    named_nodes: &HashMap<PathLeaf, (DataFrame, RDFNodeState, RDFNodeState)>,
) -> Option<(
    DataFrame,
    HashMap<String, RDFNodeState>,
    HashMap<PathLeaf, DataFrame>,
)> {
    let mut state: Option<&RDFNodeState> = None;
    let mut rows = 0usize;
    for (df, s, o) in named_nodes.values() {
        for st in [s, o] {
            let t = st.get_base_type()?;
            if st.is_multi() || !matches!(t, BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode) {
                return None;
            }
            if !matches!(st.map.get(t), Some(BaseCatState::CategoricalNative(None))) {
                return None;
            }
            match state {
                None => state = Some(st),
                Some(first) if first.get_base_type() == Some(t) => {}
                Some(_) => return None,
            }
        }
        rows += df.height();
    }
    let state = state?.clone();
    if rows == 0 {
        return None;
    }
    let mut columns = Vec::with_capacity(named_nodes.len());
    let mut values: Vec<u32> = Vec::with_capacity(2 * rows);
    for (leaf, (df, _, _)) in named_nodes {
        let s = df.column(SUBJECT_COL_NAME).ok()?.u32().ok()?.rechunk();
        let o = df.column(OBJECT_COL_NAME).ok()?.u32().ok()?.rechunk();
        if s.null_count() > 0 || o.null_count() > 0 {
            return None;
        }
        let s: Vec<u32> = s.into_no_null_iter().collect();
        let o: Vec<u32> = o.into_no_null_iter().collect();
        values.extend_from_slice(&s);
        values.extend_from_slice(&o);
        columns.push((leaf.clone(), s, o));
    }
    values.sort_unstable();
    values.dedup();
    let key = |c: &u32| values.binary_search(c).unwrap() as u32;
    let mut out = HashMap::with_capacity(columns.len());
    for (leaf, s, o) in columns {
        let height = s.len();
        let s: Vec<u32> = s.iter().map(key).collect();
        let o: Vec<u32> = o.iter().map(key).collect();
        let df = DataFrame::new(
            height,
            vec![
                UInt32Chunked::from_vec(SUBJECT_COL_NAME.into(), s).into_column(),
                UInt32Chunked::from_vec(OBJECT_COL_NAME.into(), o).into_column(),
            ],
        )
        .ok()?;
        out.insert(leaf, df);
    }
    let n = values.len();
    let lookup_df = DataFrame::new(
        n,
        vec![
            IdxCa::from_vec(LOOKUP_COLUMN.into(), (0..n as polars_core::prelude::IdxSize).collect()).into_column(),
            UInt32Chunked::from_vec(VALUE_COLUMN.into(), values).into_column(),
        ],
    )
    .ok()?;
    let types = HashMap::from([(VALUE_COLUMN.to_string(), state)]);
    Some((lookup_df, types, out))
}

fn is_constant(t: &TermPattern) -> bool {
    matches!(t, TermPattern::NamedNode(_) | TermPattern::Literal(_))
}

pub(super) fn matches_zero_length(ppe: &PropertyPathExpression) -> bool {
    match ppe {
        PropertyPathExpression::ZeroOrMore(_) | PropertyPathExpression::ZeroOrOne(_) => true,
        PropertyPathExpression::Sequence(a, b) => matches_zero_length(a) && matches_zero_length(b),
        PropertyPathExpression::Alternative(a, b) => {
            matches_zero_length(a) || matches_zero_length(b)
        }
        PropertyPathExpression::Reverse(a) => matches_zero_length(a),
        _ => false,
    }
}

pub(super) fn term_variable_name(t: &TermPattern) -> Option<String> {
    match t {
        TermPattern::Variable(v) => Some(v.as_str().to_string()),
        TermPattern::BlankNode(b) => Some(b.to_string()),
        _ => None,
    }
}

/// The lookup keys of the nodes that one end of the path is bound to, and whether that end is
/// the object (so the path is followed backwards), or None if neither end is bound to IRIs that
/// can be listed. The end with fewer values is chosen.
pub(super) fn bound_start_keys(
    subject: &TermPattern,
    object: &TermPattern,
    pushdowns: &Pushdowns,
    lookup_df: &DataFrame,
    lookup_dtypes: &HashMap<String, RDFNodeState>,
    cats: &Cats,
) -> Option<(Vec<u32>, bool)> {
    let subject_codes = bound_iri_codes(subject, pushdowns, cats);
    let object_codes = bound_iri_codes(object, pushdowns, cats);
    let (codes, reversed) = match (subject_codes, object_codes) {
        (Some(s), Some(o)) if o.len() < s.len() => (o, true),
        (Some(s), _) => (s, false),
        (None, Some(o)) => (o, true),
        (None, None) => return None,
    };
    let keys = lookup_keys_of_iris(lookup_df, lookup_dtypes, codes)?;
    Some((keys, reversed))
}

/// The global codes of the IRIs that an end of the path is bound to: a constant IRI, or a
/// variable whose possible values are pushed down and are all IRIs. IRIs that are not in the
/// dictionary are left out, since they do not occur in the graph.
fn bound_iri_codes(t: &TermPattern, pushdowns: &Pushdowns, cats: &Cats) -> Option<Vec<u32>> {
    let iris: Vec<Option<&str>> = match t {
        TermPattern::NamedNode(nn) => vec![Some(nn.as_str())],
        TermPattern::Variable(v) => {
            let values = pushdowns.variables_values.get(v.as_str())?;
            if !values.all_iris() {
                return None;
            }
            // Codes as they are, and the codes of the IRIs given as terms.
            return values.codes_of_type(&BaseRDFNodeType::IRI, cats);
        }
        _ => return None,
    };
    Some(
        cats.maybe_encode_iri_slice(&iris)
            .into_iter()
            .flatten()
            .collect(),
    )
}

/// The sorted lookup keys of the IRIs with the given global codes, or None if the IRIs of the
/// lookup are not encoded with global codes.
fn lookup_keys_of_iris(
    lookup_df: &DataFrame,
    lookup_dtypes: &HashMap<String, RDFNodeState>,
    codes: Vec<u32>,
) -> Option<Vec<u32>> {
    let state = lookup_dtypes.get(VALUE_COLUMN).unwrap();
    match state.map.get(&BaseRDFNodeType::IRI) {
        None => return Some(vec![]),
        Some(BaseCatState::CategoricalNative(None)) => {}
        Some(_) => return None,
    }
    if !state.is_multi() {
        // Row i holds the value with key i. Values keyed by sorting their codes (see
        // [single_cat_type_keys]) are found by binary search.
        let values = lookup_df.column(VALUE_COLUMN).unwrap().u32().unwrap();
        if let Ok(values) = values.cont_slice() {
            if values.is_sorted() {
                let mut out: Vec<u32> = codes
                    .iter()
                    .filter_map(|c| values.binary_search(c).ok().map(|k| k as u32))
                    .collect();
                out.sort_unstable();
                out.dedup();
                return Some(out);
            }
        }
    }
    let value = if state.is_multi() {
        col(VALUE_COLUMN)
            .struct_()
            .field_by_name(&BaseRDFNodeType::IRI.field_col_name())
    } else {
        col(VALUE_COLUMN)
    };
    let df = lookup_df
        .clone()
        .lazy()
        .select([
            value.alias(VALUE_COLUMN),
            col(LOOKUP_COLUMN).cast(DataType::UInt32),
        ])
        .collect_auto()
        .unwrap();
    let wanted: HashSet<u32> = codes.into_iter().collect();
    let values = df.column(VALUE_COLUMN).unwrap().u32().unwrap();
    let keys = df.column(LOOKUP_COLUMN).unwrap().u32().unwrap();
    let mut out: Vec<u32> = values
        .iter()
        .zip(keys.iter())
        .filter_map(|(v, k)| match (v, k) {
            (Some(v), Some(k)) if wanted.contains(&v) => Some(k),
            _ => None,
        })
        .collect();
    out.sort_unstable();
    out.dedup();
    Some(out)
}

/// The edges of a path leaf as adjacency lists over lookup keys (compressed sparse rows).
struct Adjacency {
    offsets: Vec<usize>,
    targets: Vec<u32>,
}

impl Adjacency {
    /// From a frame of subject and object keys, followed backwards if `reversed`.
    fn new(df: &DataFrame, n: usize, reversed: bool) -> Self {
        let (from, to) = if reversed {
            (OBJECT_COL_NAME, SUBJECT_COL_NAME)
        } else {
            (SUBJECT_COL_NAME, OBJECT_COL_NAME)
        };
        let from = df.column(from).unwrap().u32().unwrap();
        let to = df.column(to).unwrap().u32().unwrap();
        let edges: Vec<(u32, u32)> = from
            .iter()
            .zip(to.iter())
            .filter_map(|(f, t)| Some((f?, t?)))
            .collect();
        let mut offsets = vec![0usize; n + 1];
        for (f, _) in &edges {
            offsets[*f as usize + 1] += 1;
        }
        for i in 1..offsets.len() {
            offsets[i] += offsets[i - 1];
        }
        let mut next = offsets.clone();
        let mut targets = vec![0u32; edges.len()];
        for (f, t) in edges {
            targets[next[f as usize]] = t;
            next[f as usize] += 1;
        }
        Adjacency { offsets, targets }
    }

    /// From the nodes reached from each node 0..n.
    fn from_reached(reached: Reached) -> Self {
        let mut offsets = Vec::with_capacity(reached.len() + 1);
        offsets.push(0);
        let mut targets = Vec::with_capacity(reached.iter().map(|r| r.len()).sum());
        for r in reached {
            targets.extend_from_slice(&r);
            offsets.push(targets.len());
        }
        Adjacency { offsets, targets }
    }

    fn neighbours(&self, u: u32) -> &[u32] {
        let u = u as usize;
        if u + 1 < self.offsets.len() {
            &self.targets[self.offsets[u]..self.offsets[u + 1]]
        } else {
            &[]
        }
    }
}

/// For each start node, the sorted set of nodes reached from it.
type Reached = Vec<Vec<u32>>;

/// A walk on dictionary codes ran past its budget, or met a table it can not read.
#[derive(Debug)]
struct WalkAborted;

/// The graph that paths are followed in.
enum Walk<'a> {
    /// The edges of the path leaves over lookup keys `0..n`, with one adjacency per leaf and
    /// direction.
    Dense {
        adjacencies: HashMap<(PathLeaf, bool), Adjacency>,
        n: usize,
    },
    /// The edges read from the triple tables, one step at a time, on global dictionary codes.
    Codes(CodeWalk<'a>),
}

impl Walk<'_> {
    fn step(&self, key: (PathLeaf, bool), reached: &Reached) -> Result<Reached, WalkAborted> {
        match self {
            Walk::Dense { adjacencies, .. } => Ok(step(adjacencies.get(&key), reached)),
            Walk::Codes(walk) => walk.step(&key, reached),
        }
    }
}

/// The (subject, object) key pairs of the paths matching `ppe` that start (or, if `reversed`,
/// end) at one of `starts`, among the lookup keys `0..n`. The cost depends on the part of the
/// graph reachable from the starts rather than on the whole graph.
fn paths_from(
    ppe: &PropertyPathExpression,
    starts: Vec<u32>,
    reversed: bool,
    namednode_map: &HashMap<PathLeaf, DataFrame>,
    n: usize,
) -> (Vec<u32>, Vec<u32>) {
    let mut leaves = HashSet::new();
    path_leaves(ppe, reversed, &mut leaves);
    let adjacencies: HashMap<(PathLeaf, bool), Adjacency> = leaves
        .into_par_iter()
        .filter_map(|(leaf, rev)| {
            let df = namednode_map.get(&leaf)?;
            let adj = Adjacency::new(df, n, rev);
            Some(((leaf, rev), adj))
        })
        .collect();
    let walk = Walk::Dense { adjacencies, n };
    let initial: Reached = starts.iter().map(|s| vec![*s]).collect();
    let reached = follow(ppe, initial, reversed, &walk).expect("Dense walks are never aborted");
    reached_pairs(starts, reached, reversed)
}

/// The (subject, object) pairs of each start and the nodes reached from it.
fn reached_pairs(starts: Vec<u32>, reached: Reached, reversed: bool) -> (Vec<u32>, Vec<u32>) {
    let n: usize = reached.iter().map(|r| r.len()).sum();
    let mut from = Vec::with_capacity(n);
    let mut to = Vec::with_capacity(n);
    for (s, nodes) in starts.into_iter().zip(reached) {
        for o in nodes {
            from.push(s);
            to.push(o);
        }
    }
    if reversed {
        (to, from)
    } else {
        (from, to)
    }
}

/// Paths followed on the global dictionary codes of IRIs from a few starts. The edges of each
/// step are read from the triple tables of the path's predicates, through the sparse index of
/// their subject sort (or object sort, for a backward step): a lookup per table segment and step.
/// This avoids keying the whole tables, which costs time proportional to their size.
struct CodeWalk<'a> {
    /// The tables of each leaf and direction whose start nodes are IRIs.
    tables: HashMap<(PathLeaf, bool), Vec<&'a Triples>>,
    cats: &'a Cats,
    lookups: Cell<usize>,
    rows: Cell<usize>,
}

impl<'a> CodeWalk<'a> {
    /// None if a leaf is not a predicate (a negated property set needs every predicate), or if a
    /// step from an IRI may reach something else than an IRI, which has codes of its own.
    fn new(
        store: &'a Triplestore,
        ppe: &PropertyPathExpression,
        reversed: bool,
        dataset: &QueryGraph,
        cats: &'a Cats,
    ) -> Option<Self> {
        let graphs: Vec<NamedGraph> = match dataset {
            QueryGraph::NamedGraph(g) => vec![g.clone()],
            QueryGraph::QueryDataset(qd) => qd
                .default
                .iter()
                .map(|nn| NamedGraph::from_maybe_named_node(Some(nn)))
                .collect(),
        };
        let mut leaves = HashSet::new();
        path_leaves(ppe, reversed, &mut leaves);
        let mut tables = HashMap::new();
        for (leaf, rev) in leaves {
            let PathLeaf::NamedNode(nn) = &leaf else {
                return None;
            };
            let mut ts = vec![];
            for g in &graphs {
                let Some(types) = store.graph_triples_map.get(g).and_then(|m| m.get(nn)) else {
                    continue;
                };
                for ((subject_type, object_type), triples) in types {
                    let (from, to) = if rev {
                        (object_type, subject_type)
                    } else {
                        (subject_type, object_type)
                    };
                    if !from.is_iri() {
                        continue;
                    }
                    if !to.is_iri() {
                        return None;
                    }
                    ts.push(triples);
                }
            }
            tables.insert((leaf, rev), ts);
        }
        Some(CodeWalk {
            tables,
            cats,
            lookups: Cell::new(0),
            rows: Cell::new(0),
        })
    }

    /// The (subject, object) code pairs of the paths matching `ppe` that start (or, if
    /// `reversed`, end) at one of the sorted, distinct `starts`.
    fn paths_from(
        self,
        ppe: &PropertyPathExpression,
        starts: &[u32],
        reversed: bool,
    ) -> Result<(Vec<u32>, Vec<u32>), WalkAborted> {
        let walk = Walk::Codes(self);
        let initial: Reached = starts.iter().map(|s| vec![*s]).collect();
        let reached = follow(ppe, initial, reversed, &walk)?;
        Ok(reached_pairs(starts.to_vec(), reached, reversed))
    }

    fn spend(budget: &Cell<usize>, amount: usize, limit: usize) -> Result<(), WalkAborted> {
        let spent = budget.get() + amount;
        budget.set(spent);
        if spent > limit {
            Err(WalkAborted)
        } else {
            Ok(())
        }
    }

    fn step(&self, key: &(PathLeaf, bool), reached: &Reached) -> Result<Reached, WalkAborted> {
        let empty = || vec![vec![]; reached.len()];
        let Some(tables) = self.tables.get(key).filter(|t| !t.is_empty()) else {
            return Ok(empty());
        };
        let mut codes: Vec<u32> = reached.iter().flatten().copied().collect();
        codes.sort_unstable();
        codes.dedup();
        if codes.is_empty() {
            return Ok(empty());
        }
        // The frontier of each table, without the codes its filters reject, so that only codes
        // some table may hold are decoded.
        let frontiers: Vec<Vec<u32>> = tables
            .iter()
            .map(|t| {
                codes
                    .iter()
                    .copied()
                    .filter(|c| t.may_hold_code(*c, key.1))
                    .collect()
            })
            .collect();
        let mut names: HashMap<u32, _> = HashMap::new();
        for c in frontiers.iter().flatten() {
            if !names.contains_key(c) {
                let name = self
                    .cats
                    .maybe_decode_of_type(c, &BaseRDFNodeType::IRI)
                    .ok_or(WalkAborted)?;
                names.insert(*c, name);
            }
        }
        let mut edges = vec![];
        for (t, frontier) in tables.iter().zip(&frontiers) {
            if frontier.is_empty() {
                continue;
            }
            let mut targets: Vec<(&str, u32)> =
                frontier.iter().map(|c| (&*names[c], *c)).collect();
            targets.sort_unstable();
            Self::spend(&self.lookups, t.n_segments(), WALK_MAX_LOOKUPS)?;
            let rows = t
                .in_memory_edges_from(&targets, frontier, key.1, &mut edges)
                .ok()
                .flatten()
                .ok_or(WalkAborted)?;
            Self::spend(&self.rows, rows, WALK_MAX_ROWS)?;
        }
        edges.sort_unstable();
        edges.dedup();
        Ok(reached
            .iter()
            .map(|nodes| {
                let mut out = vec![];
                for u in nodes {
                    let i = edges.partition_point(|(f, _)| f < u);
                    out.extend(
                        edges[i..]
                            .iter()
                            .take_while(|(f, _)| f == u)
                            .map(|(_, t)| *t),
                    );
                }
                out.sort_unstable();
                out.dedup();
                out
            })
            .collect())
    }
}

/// The leaves of a path expression, with whether each is followed backwards.
pub(super) fn path_leaves(
    ppe: &PropertyPathExpression,
    reversed: bool,
    leaves: &mut HashSet<(PathLeaf, bool)>,
) {
    match ppe {
        PropertyPathExpression::NamedNode(nn) => {
            leaves.insert((PathLeaf::NamedNode(nn.clone()), reversed));
        }
        PropertyPathExpression::NegatedPropertySet(nns) => {
            leaves.insert((PathLeaf::negated(nns), reversed));
        }
        PropertyPathExpression::Reverse(inner) => path_leaves(inner, !reversed, leaves),
        PropertyPathExpression::Sequence(a, b) | PropertyPathExpression::Alternative(a, b) => {
            path_leaves(a, reversed, leaves);
            path_leaves(b, reversed, leaves);
        }
        PropertyPathExpression::ZeroOrMore(inner)
        | PropertyPathExpression::OneOrMore(inner)
        | PropertyPathExpression::ZeroOrOne(inner) => path_leaves(inner, reversed, leaves),
    }
}

/// The leaf and direction of a step that is a single predicate or negated property set, maybe
/// backwards.
fn single_leaf(ppe: &PropertyPathExpression, reversed: bool) -> Option<(PathLeaf, bool)> {
    match ppe {
        PropertyPathExpression::NamedNode(nn) => Some((PathLeaf::NamedNode(nn.clone()), reversed)),
        PropertyPathExpression::NegatedPropertySet(nns) => {
            Some((PathLeaf::negated(nns), reversed))
        }
        PropertyPathExpression::Reverse(inner) => single_leaf(inner, !reversed),
        _ => None,
    }
}

/// The nodes reached from each set of `reached` nodes by one match of `ppe` (backwards if
/// `reversed`).
fn follow(
    ppe: &PropertyPathExpression,
    reached: Reached,
    reversed: bool,
    walk: &Walk,
) -> Result<Reached, WalkAborted> {
    Ok(match ppe {
        PropertyPathExpression::NamedNode(nn) => {
            walk.step((PathLeaf::NamedNode(nn.clone()), reversed), &reached)?
        }
        PropertyPathExpression::NegatedPropertySet(nns) => {
            walk.step((PathLeaf::negated(nns), reversed), &reached)?
        }
        PropertyPathExpression::Reverse(inner) => follow(inner, reached, !reversed, walk)?,
        PropertyPathExpression::Sequence(a, b) => {
            let (first, second) = if reversed { (b, a) } else { (a, b) };
            let mid = follow(first, reached, reversed, walk)?;
            follow(second, mid, reversed, walk)?
        }
        PropertyPathExpression::Alternative(a, b) => {
            let left = follow(a, reached.clone(), reversed, walk)?;
            let right = follow(b, reached, reversed, walk)?;
            zip_reached(left, right, |l, r| union_sorted(&l, &r))
        }
        PropertyPathExpression::ZeroOrOne(inner) => {
            let once = follow(inner, reached.clone(), reversed, walk)?;
            zip_reached(reached, once, |l, r| union_sorted(&l, &r))
        }
        PropertyPathExpression::OneOrMore(inner) => {
            one_or_more_from(inner, reached, reversed, walk)?
        }
        PropertyPathExpression::ZeroOrMore(inner) => {
            let more = one_or_more_from(inner, reached.clone(), reversed, walk)?;
            zip_reached(reached, more, |l, r| union_sorted(&l, &r))
        }
    })
}

/// The nodes reached by one or more matches of `inner`. On lookup keys, a depth first search
/// from each set of nodes over the adjacency of `inner`; otherwise the newly reached nodes are
/// followed until no new nodes are reached.
fn one_or_more_from(
    inner: &PropertyPathExpression,
    reached: Reached,
    reversed: bool,
    walk: &Walk,
) -> Result<Reached, WalkAborted> {
    if let Walk::Dense { adjacencies, n } = walk {
        if let Some(key) = single_leaf(inner, reversed) {
            return Ok(closure(adjacencies.get(&key), reached, *n));
        }
        if reached.len().saturating_mul(SELECTIVE_START_FACTOR) >= *n {
            fast_path::hit(FastPath::PathEveryNodeStep);
            let every_node: Reached = (0..*n as u32).map(|u| vec![u]).collect();
            let adjacency = Adjacency::from_reached(follow(inner, every_node, reversed, walk)?);
            return Ok(closure(Some(&adjacency), reached, *n));
        }
    }
    let mut all = follow(inner, reached, reversed, walk)?;
    let mut frontier = all.clone();
    loop {
        // The query timed out or was cancelled: the caller discards the result.
        if utils::cancel::stopped().is_some() {
            return Ok(all);
        }
        let next = follow(inner, frontier, reversed, walk)?;
        let new: Reached = next
            .into_par_iter()
            .zip(all.par_iter())
            .map(|(n, a)| difference_sorted(&n, a))
            .collect();
        if new.iter().all(|x| x.is_empty()) {
            return Ok(all);
        }
        all = all
            .into_par_iter()
            .zip(new.par_iter())
            .map(|(a, n)| if n.is_empty() { a } else { union_sorted(&a, n) })
            .collect();
        frontier = new;
    }
}

/// The nodes reached from each set of nodes by one or more steps over `adjacency` (over the
/// nodes `0..n`), by a depth first search. Each search marks the nodes it reaches with its own
/// stamp, so a thread's marks are reset only when the stamps run out.
fn closure(adjacency: Option<&Adjacency>, reached: Reached, n: usize) -> Reached {
    let Some(adjacency) = adjacency else {
        return vec![vec![]; reached.len()];
    };
    reached
        .into_par_iter()
        .with_min_len(256)
        .map_init(
            || (vec![0u32; n], 0u32, Vec::new()),
            |(stamps, stamp, stack), nodes| {
                if utils::cancel::stopped().is_some() {
                    return vec![];
                }
                *stamp = stamp.wrapping_add(1);
                if *stamp == 0 {
                    stamps.fill(0);
                    *stamp = 1;
                }
                let mut out = vec![];
                stack.extend_from_slice(&nodes);
                while let Some(u) = stack.pop() {
                    for &v in adjacency.neighbours(u) {
                        let s = &mut stamps[v as usize];
                        if *s != *stamp {
                            *s = *stamp;
                            out.push(v);
                            stack.push(v);
                        }
                    }
                }
                out.sort_unstable();
                out
            },
        )
        .collect()
}

fn step(adjacency: Option<&Adjacency>, reached: &Reached) -> Reached {
    let Some(adjacency) = adjacency else {
        return vec![vec![]; reached.len()];
    };
    reached
        .par_iter()
        .map(|nodes| {
            let mut out = vec![];
            for n in nodes {
                out.extend_from_slice(adjacency.neighbours(*n));
            }
            out.sort_unstable();
            out.dedup();
            out
        })
        .collect()
}

fn zip_reached(
    left: Reached,
    right: Reached,
    f: impl Fn(Vec<u32>, Vec<u32>) -> Vec<u32> + Sync + Send,
) -> Reached {
    left.into_par_iter()
        .zip(right.into_par_iter())
        .map(|(l, r)| f(l, r))
        .collect()
}

fn union_sorted(a: &[u32], b: &[u32]) -> Vec<u32> {
    if a.is_empty() {
        return b.to_vec();
    }
    if b.is_empty() {
        return a.to_vec();
    }
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
}

/// The elements of `a` that are not in `b`.
fn difference_sorted(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = vec![];
    let mut j = 0;
    for x in a {
        while j < b.len() && b[j] < *x {
            j += 1;
        }
        if j >= b.len() || b[j] != *x {
            out.push(*x);
        }
    }
    out
}

/// The smaller of two sets of nodes, either of which may be unknown.
fn fewer(a: Option<Vec<u32>>, b: Option<Vec<u32>>) -> Option<Vec<u32>> {
    match (a, b) {
        (Some(a), Some(b)) => Some(if b.len() < a.len() { b } else { a }),
        (a, b) => a.or(b),
    }
}

/// The name of the variable of `t` when its column holds only IRIs with global codes.
fn global_iri_column(t: &TermPattern, types: &HashMap<String, RDFNodeState>) -> Option<String> {
    let name = term_variable_name(t)?;
    let state = types.get(&name)?;
    (has_only_type(state, &BaseRDFNodeType::IRI)
        && matches!(
            state.map.get(&BaseRDFNodeType::IRI),
            Some(BaseCatState::CategoricalNative(None))
        ))
    .then_some(name)
}

/// The distinct global codes of the IRIs that the variable of `t` is bound to in the solution
/// mappings, when its column holds only IRIs with global codes.
fn mapped_iri_codes(t: &TermPattern, sm: &EagerSolutionMappings) -> Option<Vec<u32>> {
    let name = global_iri_column(t, &sm.rdf_node_types)?;
    let column = sm.mappings.column(&name).ok()?;
    let codes = column.u32().ok()?;
    let mut out: Vec<u32> = codes.iter().flatten().collect();
    out.sort_unstable();
    out.dedup();
    Some(out)
}

/// True if every value of a column of this type is of the given type.
fn has_only_type(state: &RDFNodeState, t: &BaseRDFNodeType) -> bool {
    !state.is_multi() && state.get_base_type() == Some(t)
}

/// True if every value of a column of this type is a literal with the datatype of the literal
/// (the time zones of the values may differ).
fn has_only_literal_datatype(state: &RDFNodeState, l: &oxrdf::Literal) -> bool {
    !state.is_multi()
        && matches!(state.get_base_type(), Some(BaseRDFNodeType::Literal(dt, _)) if dt.as_ref() == l.datatype())
}

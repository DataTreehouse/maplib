use super::{TriplesToAdd, Triplestore};
use crate::errors::TriplestoreError;
use oxrdf::NamedNode;
use polars::prelude::{col, concat, concat_str, lit, UnionArgs};
use representation::dataset::NamedGraph;
use representation::cats::maybe_decode_complex_expr;
use representation::solution_mapping::BaseCatState;
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use tracing::instrument;
use utils::polars::collect_in_memory;

/// The triples are added in batches of about this many rows, so that a large graph is not held
/// in memory twice while it is copied.
const COPY_BATCH_ROWS: usize = 4_000_000;

impl Triplestore {
    /// Copies the triples of `source_graph` of `other` into `target_graph` of this triplestore.
    /// Blank nodes keep their labels (see `copy_triples` for copies that keep them apart).
    pub fn add_graph(
        &mut self,
        other: &Triplestore,
        source_graph: NamedGraph,
        target_graph: NamedGraph,
    ) -> Result<(), TriplestoreError> {
        self.copy_triples(Some(other), &source_graph, &target_graph, false)
    }

    /// Copies the triples of `source_graph` of `source` (this triplestore when None) into
    /// `target_graph`, one table (predicate, subject type and object type) at a time.
    /// With `fresh_blank_nodes`, the blank nodes get new labels, unique in this triplestore,
    /// so that they stay distinct from the blank nodes already here, as when a file is read.
    /// A copy within one triplestore keeps the blank nodes, as a graph shares them with the
    /// other graphs of its dataset.
    #[instrument(skip_all)]
    pub fn copy_triples(
        &mut self,
        source: Option<&Triplestore>,
        source_graph: &NamedGraph,
        target_graph: &NamedGraph,
        fresh_blank_nodes: bool,
    ) -> Result<(), TriplestoreError> {
        let same = source.is_none_or(|s| std::ptr::eq(s, self));
        if same && source_graph == target_graph {
            return Ok(());
        }
        let tables: Vec<(NamedNode, BaseRDFNodeType, BaseRDFNodeType)> = {
            let src = source.unwrap_or(self);
            let Some(graph) = src.graph_triples_map.get(source_graph) else {
                return Ok(());
            };
            graph
                .iter()
                .flat_map(|(p, m)| m.keys().map(|(s, o)| (p.clone(), s.clone(), o.clone())))
                .collect()
        };
        let blank_suffix = if fresh_blank_nodes && !same {
            let suffix = format!("_{}", self.parser_call);
            self.parser_call += 1;
            Some(suffix)
        } else {
            None
        };
        let mut batch = vec![];
        let mut batch_rows = 0;
        for (predicate, subject_type, object_type) in tables {
            let tta = {
                let src = source.unwrap_or(self);
                src.table_to_add(
                    &predicate,
                    subject_type,
                    object_type,
                    source_graph,
                    target_graph,
                    same,
                    blank_suffix.as_deref(),
                )?
            };
            let Some(tta) = tta else { continue };
            batch_rows += tta.df.height();
            batch.push(tta);
            if batch_rows >= COPY_BATCH_ROWS {
                self.add_triples_vec(std::mem::take(&mut batch))?;
                batch_rows = 0;
            }
        }
        if !batch.is_empty() {
            self.add_triples_vec(batch)?;
        }
        Ok(())
    }

    /// The triples of one table of this triplestore, to be added to `target_graph` of this
    /// triplestore (`same`) or of another. Codes of this triplestore's dictionaries are kept
    /// when adding to itself. For another triplestore, the columns are decoded to strings, which
    /// the target encodes with its dictionaries once for all tables of a batch: passing this
    /// triplestore's dictionaries along as local ones would merge all of them for every table,
    /// and is not possible when they are on disk. Blank nodes get `blank_suffix` appended, if
    /// given.
    #[allow(clippy::too_many_arguments)]
    fn table_to_add(
        &self,
        predicate: &NamedNode,
        subject_type: BaseRDFNodeType,
        object_type: BaseRDFNodeType,
        source_graph: &NamedGraph,
        target_graph: &NamedGraph,
        same: bool,
        blank_suffix: Option<&str>,
    ) -> Result<Option<TriplesToAdd>, TriplestoreError> {
        let Some(triples) = self
            .graph_triples_map
            .get(source_graph)
            .and_then(|g| g.get(predicate))
            .and_then(|m| m.get(&(subject_type.clone(), object_type.clone())))
        else {
            return Ok(None);
        };
        let lfs = triples.get_all_triples_lazy_frames(true)?;
        if lfs.is_empty() {
            return Ok(None);
        }
        let mut lf = concat(
            lfs,
            UnionArgs {
                parallel: true,
                rechunk: false,
                to_supertypes: false,
                diagonal: false,
                strict: false,
                from_partitioned_ds: false,
                maintain_order: true,
            },
        )
        .map_err(TriplestoreError::LazyLoadError)?;
        let mut cat_state = |c: &str, t: &BaseRDFNodeType| -> BaseCatState {
            if !t.stored_cat() {
                return t.default_stored_cat_state();
            }
            if same {
                // Already coded with the dictionaries of the target.
                return BaseCatState::CategoricalNative(None);
            }
            let mut e = maybe_decode_complex_expr(
                col(c),
                t,
                &BaseCatState::CategoricalNative(None),
                self.global_cats.clone(),
            );
            if let Some(suffix) = blank_suffix.filter(|_| t.is_blank_node()) {
                e = concat_str([e, lit(suffix.to_string())], "", false);
            }
            lf = std::mem::take(&mut lf).with_column(e.alias(c));
            BaseCatState::String
        };
        let subject_cat_state = cat_state(SUBJECT_COL_NAME, &subject_type);
        let object_cat_state = cat_state(OBJECT_COL_NAME, &object_type);
        let df = collect_in_memory(lf).map_err(TriplestoreError::LazyLoadError)?;
        if df.height() == 0 {
            return Ok(None);
        }
        Ok(Some(TriplesToAdd {
            df,
            subject_type,
            object_type,
            predicate: Some(predicate.clone()),
            graph: target_graph.clone(),
            subject_cat_state,
            object_cat_state,
            predicate_cat_state: None,
        }))
    }
}

//! What an update actually changed: the triples really inserted (not already present) and really
//! deleted (present before the deletion), per graph.

use crate::errors::TriplestoreError;
use crate::storage::Triples;
use crate::NewTriples;
use oxrdf::NamedNode;
use polars::prelude::{col, concat, LazyFrame, UnionArgs};
use representation::dataset::NamedGraph;
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashMap;

/// The changes of one graph. The counts are always kept, the triples only when asked for.
#[derive(Debug, Clone, Default)]
pub struct GraphChanges {
    pub inserted: usize,
    pub deleted: usize,
    pub inserted_triples: Vec<NewTriples>,
    pub deleted_triples: Vec<NewTriples>,
}

/// The changes of an update, per graph. Operations are applied in order and their changes are
/// summed, so a triple inserted by one operation and deleted by a later one counts in both.
#[derive(Debug, Clone, Default)]
pub struct Changes {
    pub keep_triples: bool,
    pub graphs: HashMap<NamedGraph, GraphChanges>,
}

impl Changes {
    pub fn new(keep_triples: bool) -> Changes {
        Changes {
            keep_triples,
            graphs: HashMap::new(),
        }
    }

    /// Records that the graph was the target of an operation, even if nothing changed.
    pub fn touch(&mut self, graph: &NamedGraph) -> &mut GraphChanges {
        self.graphs.entry(graph.clone()).or_default()
    }

    pub fn add_inserted(&mut self, new_triples: Vec<NewTriples>) {
        for nt in new_triples {
            let n = nt.df.as_ref().map(|x| x.height()).unwrap_or(0);
            let keep = self.keep_triples;
            let g = self.touch(&nt.graph);
            g.inserted += n;
            if keep && n > 0 {
                g.inserted_triples.push(nt);
            }
        }
    }

    pub fn add_deleted(&mut self, graph: &NamedGraph, deleted: Deleted) {
        let g = self.touch(graph);
        g.deleted += deleted.count;
        g.deleted_triples.extend(deleted.triples);
    }
}

/// Deleted triples: the exact count, and the triples themselves when asked for.
#[derive(Debug, Clone, Default)]
pub struct Deleted {
    pub count: usize,
    pub triples: Vec<NewTriples>,
}

impl Deleted {
    pub fn extend(&mut self, other: Deleted) {
        self.count += other.count;
        self.triples.extend(other.triples);
    }
}

pub(crate) fn concat_lfs(lfs: Vec<LazyFrame>) -> LazyFrame {
    concat(
        lfs,
        UnionArgs {
            parallel: true,
            rechunk: false,
            to_supertypes: false,
            diagonal: false,
            from_partitioned_ds: false,
            maintain_order: false,
            strict: false,
        },
    )
    .unwrap()
}

/// All triples of a table as subject/object columns.
pub(crate) fn table_as_new_triples(
    triples: &Triples,
    graph: &NamedGraph,
    predicate: &NamedNode,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
) -> Result<NewTriples, TriplestoreError> {
    let lfs: Vec<_> = triples
        .get_lazy_frames(&None, &None)?
        .into_iter()
        .map(|(lf, _)| lf)
        .collect();
    let df = if lfs.is_empty() {
        None
    } else {
        Some(
            concat_lfs(lfs)
                .select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)])
                .collect()
                .unwrap(),
        )
    };
    Ok(NewTriples {
        df,
        graph: graph.clone(),
        predicate: predicate.clone(),
        subject_type: subject_type.clone(),
        object_type: object_type.clone(),
    })
}

/// All triples of the tables of a predicate.
pub(crate) fn tables_as_new_triples(
    tables: &HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>,
    graph: &NamedGraph,
    predicate: &NamedNode,
) -> Result<Vec<NewTriples>, TriplestoreError> {
    let mut out = vec![];
    for ((s, o), t) in tables {
        out.push(table_as_new_triples(t, graph, predicate, s, o)?);
    }
    Ok(out)
}

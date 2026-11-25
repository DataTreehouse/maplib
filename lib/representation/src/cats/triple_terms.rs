//! The dictionary of RDF 1.2 triple terms: each distinct triple term `<<( s p o )>>` has a u32
//! id, which tables and query results store where the triple term is a value (the object of
//! `rdf:reifies`, for example), as IRIs are stored as codes of the IRI dictionary.
//!
//! The triple terms are partitioned as the triplestore partitions triples: by the predicate of
//! the triple term and the types of its subject and object. Each partition is a table of the
//! columns `id`, `subject` and `object`, holding the subject and object as the triplestore holds
//! them (codes of the string dictionaries, native values of other literals, ids of triple terms
//! for nested ones), so that a pattern `<<( ?s ex:p ?o )>>` reads only the partitions of `ex:p`.
//! A triple term is a value, not a statement in a graph, so the same triple term has one id
//! whatever graphs it is used in.
//!
//! Ids are given from a counter, in the order triple terms are first added, and are not reused.
//! The id of a triple term is found by joining with its partition on the subject and object,
//! which works alike for every object type.
//!
//! The partitions refer to codes of the string dictionaries of the same Cats, so whatever
//! re-encodes or removes those codes has to do the same here (not done yet: garbage collection
//! and compaction of the string dictionaries keep the codes of triple terms).

use crate::BaseRDFNodeType;
use oxrdf::NamedNode;
use polars::prelude::{
    col, concat, lit, ChunkFullNull, DataFrame, IntoColumn, IntoLazy, IntoSeries, JoinArgs,
    JoinType, LazyFrame, MaintainOrderJoin, PolarsResult, UInt32Chunked, UnionArgs,
    UniqueKeepStrategy,
};
use std::collections::HashMap;
use utils::polars::collect_in_memory;

pub const TRIPLE_TERM_ID_COL_NAME: &str = "id";
const SUBJECT: &str = crate::SUBJECT_COL_NAME;
const OBJECT: &str = crate::OBJECT_COL_NAME;
const ROW: &str = "__triple_term_row";

/// The partition of a triple term: its predicate and the types of its subject and object.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TripleTermPartitionKey {
    pub predicate: NamedNode,
    pub subject_type: BaseRDFNodeType,
    pub object_type: BaseRDFNodeType,
}

/// The triple terms of a partition, a table of the columns id, subject and object.
#[derive(Debug, Clone)]
pub struct TripleTermPartition {
    pub key: TripleTermPartitionKey,
    df: DataFrame,
}

impl TripleTermPartition {
    pub fn df(&self) -> &DataFrame {
        &self.df
    }

    pub fn lazy(&self) -> LazyFrame {
        self.df.clone().lazy()
    }

    pub fn height(&self) -> usize {
        self.df.height()
    }
}

#[derive(Debug, Clone, Default)]
pub struct TripleTerms {
    partitions: Vec<TripleTermPartition>,
    index: HashMap<TripleTermPartitionKey, usize>,
    /// The partition and row of each triple term, by id.
    locations: Vec<(u32, u32)>,
}

impl TripleTerms {
    /// The number of triple terms.
    pub fn len(&self) -> usize {
        self.locations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.locations.is_empty()
    }

    pub fn partitions(&self) -> &[TripleTermPartition] {
        &self.partitions
    }

    pub fn partitions_with_predicate<'a>(
        &'a self,
        predicate: &'a NamedNode,
    ) -> impl Iterator<Item = &'a TripleTermPartition> + 'a {
        self.partitions
            .iter()
            .filter(move |p| &p.key.predicate == predicate)
    }

    /// The partition and the row in it of the triple term with the id.
    pub fn location(&self, id: u32) -> Option<(&TripleTermPartition, usize)> {
        let (p, row) = self.locations.get(id as usize)?;
        Some((&self.partitions[*p as usize], *row as usize))
    }

    /// The tables of the partitions, with the codes of the columns of types that are stored as
    /// codes of the string dictionaries (see BaseRDFNodeType::stored_cat) mapped by `f`, for
    /// renumbering the codes of the dictionaries. Set them with set_partition_tables.
    pub fn mapped_codes(
        &self,
        f: &dyn Fn(&BaseRDFNodeType, &UInt32Chunked) -> PolarsResult<UInt32Chunked>,
    ) -> PolarsResult<Vec<DataFrame>> {
        let mut out = Vec::with_capacity(self.partitions.len());
        for p in &self.partitions {
            let mut df = p.df.clone();
            for (c, t) in [(SUBJECT, &p.key.subject_type), (OBJECT, &p.key.object_type)] {
                if !t.stored_cat() || df.height() == 0 {
                    continue;
                }
                let column = df.column(c)?.clone();
                let mapped = if t.is_lang_string() {
                    let st = column.struct_()?;
                    let mut fields = vec![];
                    for field in [
                        crate::LANG_STRING_VALUE_FIELD,
                        crate::LANG_STRING_LANG_FIELD,
                    ] {
                        let codes = st.field_by_name(field)?;
                        fields.push(f(t, codes.u32()?)?.with_name(field.into()).into_series());
                    }
                    polars::prelude::StructChunked::from_series(
                        c.into(),
                        df.height(),
                        fields.iter(),
                    )?
                    .into_series()
                    .into_column()
                } else {
                    f(t, column.u32()?)?.with_name(c.into()).into_column()
                };
                df.with_column(mapped)?;
            }
            out.push(df);
        }
        Ok(out)
    }

    /// Replaces the tables of the partitions by tables from mapped_codes, which have the same
    /// rows in the same order.
    pub fn set_partition_tables(&mut self, dfs: Vec<DataFrame>) {
        assert_eq!(dfs.len(), self.partitions.len());
        for (p, df) in self.partitions.iter_mut().zip(dfs) {
            assert_eq!(p.df.height(), df.height());
            p.df = df;
        }
    }

    /// The ids of the triple terms of the partition `key` with the subjects and objects of `df`
    /// (the columns subject and object, as the triplestore stores them), one per row of `df` in
    /// its order. Triple terms that are not in the dictionary are added with new ids when
    /// `insert`, and are null otherwise.
    pub fn ids(
        &mut self,
        key: &TripleTermPartitionKey,
        df: &DataFrame,
        insert: bool,
    ) -> PolarsResult<UInt32Chunked> {
        let input = df.select([SUBJECT, OBJECT])?;
        let height = input.height();
        if height == 0 {
            return Ok(UInt32Chunked::from_vec(
                TRIPLE_TERM_ID_COL_NAME.into(),
                vec![],
            ));
        }
        let Some(&p) = self.index.get(key) else {
            if !insert {
                return Ok(UInt32Chunked::full_null(
                    TRIPLE_TERM_ID_COL_NAME.into(),
                    height,
                ));
            }
            let empty = input.clear();
            let p = self.add_partition(key.clone(), &empty)?;
            return self.ids_in(p, input, insert);
        };
        self.ids_in(p, input, insert)
    }

    /// The ids of the triple terms that are in the dictionary, null for the others, without
    /// adding any (for triple terms made in queries, which can only equal stored ones if they
    /// are in the dictionary).
    pub fn lookup_ids(
        &self,
        key: &TripleTermPartitionKey,
        df: &DataFrame,
    ) -> PolarsResult<UInt32Chunked> {
        let input = df.select([SUBJECT, OBJECT])?;
        match self.index.get(key) {
            Some(&p) => found_ids(&self.partitions[p].df, input),
            None => Ok(UInt32Chunked::full_null(
                TRIPLE_TERM_ID_COL_NAME.into(),
                input.height(),
            )),
        }
    }

    fn add_partition(
        &mut self,
        key: TripleTermPartitionKey,
        empty: &DataFrame,
    ) -> PolarsResult<usize> {
        let id = UInt32Chunked::from_vec(TRIPLE_TERM_ID_COL_NAME.into(), vec![]).into_column();
        let mut columns = vec![id];
        columns.extend(empty.columns().iter().cloned());
        let df = DataFrame::new(0, columns)?;
        let p = self.partitions.len();
        self.index.insert(key.clone(), p);
        self.partitions.push(TripleTermPartition { key, df });
        Ok(p)
    }

    fn ids_in(&mut self, p: usize, input: DataFrame, insert: bool) -> PolarsResult<UInt32Chunked> {
        let found = found_ids(&self.partitions[p].df, input.clone())?;
        if !insert || found.null_count() == 0 {
            return Ok(found);
        }
        // The distinct triple terms that are not in the dictionary, in the order they first
        // occur, get the next ids.
        let missing = input
            .clone()
            .lazy()
            .with_column(lit(found.into_series()).alias(TRIPLE_TERM_ID_COL_NAME))
            .filter(col(TRIPLE_TERM_ID_COL_NAME).is_null())
            .select([col(SUBJECT), col(OBJECT)])
            .unique_stable(None, UniqueKeepStrategy::First);
        let mut missing = collect_in_memory(missing)?;
        let n = missing.height() as u32;
        let first_id = self.locations.len() as u32;
        let first_row = self.partitions[p].df.height() as u32;
        let ids: Vec<u32> = (first_id..first_id + n).collect();
        missing.insert_column(
            0,
            UInt32Chunked::from_vec(TRIPLE_TERM_ID_COL_NAME.into(), ids).into_column(),
        )?;
        self.locations
            .extend((first_row..first_row + n).map(|row| (p as u32, row)));
        let part = &mut self.partitions[p];
        part.df = if part.df.height() == 0 {
            missing
        } else {
            let mut df = part.df.vstack(&missing)?;
            df.rechunk_mut();
            df
        };
        // Every triple term of the input is now in the partition.
        found_ids(&self.partitions[p].df, input)
    }
}

/// The ids in `partition` of the triple terms of the rows of `input` (subject and object), in
/// the order of `input`, null for those that are not in it.
fn found_ids(partition: &DataFrame, input: DataFrame) -> PolarsResult<UInt32Chunked> {
    let height = input.height();
    if partition.height() == 0 {
        return Ok(UInt32Chunked::full_null(
            TRIPLE_TERM_ID_COL_NAME.into(),
            height,
        ));
    }
    let on = [col(SUBJECT), col(OBJECT)];
    let joined = input
        .lazy()
        .with_row_index(ROW, None)
        .join(
            partition.clone().lazy(),
            on.clone(),
            on,
            JoinArgs {
                how: JoinType::Left,
                validation: Default::default(),
                suffix: None,
                slice: None,
                nulls_equal: false,
                coalesce: Default::default(),
                maintain_order: MaintainOrderJoin::Left,
                build_side: None,
            },
        )?
        .sort([ROW], Default::default())
        .select([col(TRIPLE_TERM_ID_COL_NAME)]);
    let joined = collect_in_memory(joined)?;
    assert_eq!(
        joined.height(),
        height,
        "triple terms are distinct within a partition"
    );
    Ok(joined
        .column(TRIPLE_TERM_ID_COL_NAME)?
        .u32()?
        .clone()
        .with_name(TRIPLE_TERM_ID_COL_NAME.into()))
}

/// The tables of the partitions together, for whole-dictionary operations.
pub fn concat_partitions(lfs: Vec<LazyFrame>) -> PolarsResult<LazyFrame> {
    concat(lfs, UnionArgs::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::vocab::xsd;
    use polars::prelude::{Column, Float64Chunked};

    fn key(object_type: BaseRDFNodeType) -> TripleTermPartitionKey {
        TripleTermPartitionKey {
            predicate: NamedNode::new_unchecked("http://example.org/p"),
            subject_type: BaseRDFNodeType::IRI,
            object_type,
        }
    }

    fn codes(subjects: &[u32], objects: &[u32]) -> DataFrame {
        DataFrame::new(
            subjects.len(),
            vec![
                Column::new(SUBJECT.into(), subjects),
                Column::new(OBJECT.into(), objects),
            ],
        )
        .unwrap()
    }

    fn ids(c: &UInt32Chunked) -> Vec<Option<u32>> {
        c.iter().collect()
    }

    #[test]
    fn same_triple_term_same_id_within_and_across_batches() {
        let mut tt = TripleTerms::default();
        let k = key(BaseRDFNodeType::IRI);
        let first = tt.ids(&k, &codes(&[1, 2, 1], &[5, 5, 5]), true).unwrap();
        assert_eq!(ids(&first), vec![Some(0), Some(1), Some(0)]);
        let second = tt.ids(&k, &codes(&[3, 2, 1], &[5, 5, 5]), true).unwrap();
        assert_eq!(ids(&second), vec![Some(2), Some(1), Some(0)]);
        assert_eq!(tt.len(), 3);
        let (p, row) = tt.location(2).unwrap();
        assert_eq!(p.key, k);
        assert_eq!(row, 2);
    }

    #[test]
    fn lookup_does_not_add() {
        let mut tt = TripleTerms::default();
        let k = key(BaseRDFNodeType::IRI);
        tt.ids(&k, &codes(&[1], &[5]), true).unwrap();
        let found = tt.lookup_ids(&k, &codes(&[1, 9], &[5, 5])).unwrap();
        assert_eq!(ids(&found), vec![Some(0), None]);
        let not_inserted = tt.ids(&k, &codes(&[9], &[5]), false).unwrap();
        assert_eq!(ids(&not_inserted), vec![None]);
        let other = key(BaseRDFNodeType::BlankNode);
        assert_eq!(
            ids(&tt.lookup_ids(&other, &codes(&[1], &[5])).unwrap()),
            vec![None]
        );
        assert_eq!(tt.len(), 1);
    }

    #[test]
    fn partitions_by_types_share_one_id_counter() {
        let mut tt = TripleTerms::default();
        tt.ids(&key(BaseRDFNodeType::IRI), &codes(&[1], &[5]), true)
            .unwrap();
        let doubles = DataFrame::new(
            2,
            vec![
                Column::new(SUBJECT.into(), [1u32, 1u32]),
                Float64Chunked::from_vec(OBJECT.into(), vec![37.5, 37.5]).into_column(),
            ],
        )
        .unwrap();
        let k = key(BaseRDFNodeType::literal(xsd::DOUBLE.into_owned()));
        assert_eq!(
            ids(&tt.ids(&k, &doubles, true).unwrap()),
            vec![Some(1), Some(1)]
        );
        let p = NamedNode::new_unchecked("http://example.org/p");
        assert_eq!(tt.partitions_with_predicate(&p).count(), 2);
    }
}

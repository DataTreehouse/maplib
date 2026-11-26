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
//! re-encodes or removes those codes does the same here: garbage collection keeps the codes the
//! partitions use, compaction renumbers them (see mapped_codes), and serialization writes the
//! partitions with the string dictionaries (see cats::serialization).

use crate::BaseRDFNodeType;
use oxrdf::NamedNode;
use polars::prelude::{
    col, concat, lit, ChunkFullNull, DataFrame, IdxCa, IdxSize, IntoColumn, IntoLazy, IntoSeries,
    JoinArgs, JoinType, LazyFrame, MaintainOrderJoin, PolarsResult, UInt32Chunked, UnionArgs,
    UniqueKeepStrategy,
};
use std::collections::{BTreeMap, HashMap};
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

    /// The partitions that hold the triple terms with the ids, in the order of the partitions,
    /// each with the rows of those triple terms (the columns id, subject and object), so that
    /// triple terms with known ids are found without reading the other partitions. Ids that are
    /// not in the dictionary are left out, and an id given twice gives its row twice.
    pub fn rows_of_ids(&self, ids: &[u32]) -> PolarsResult<Vec<(&TripleTermPartition, DataFrame)>> {
        let mut rows: BTreeMap<u32, Vec<IdxSize>> = BTreeMap::new();
        for id in ids {
            if let Some((p, row)) = self.locations.get(*id as usize) {
                rows.entry(*p).or_default().push(*row as IdxSize);
            }
        }
        rows.into_iter()
            .map(|(p, rows)| {
                let part = &self.partitions[p as usize];
                let df = part.df.take(&IdxCa::from_vec("rows".into(), rows))?;
                Ok((part, df))
            })
            .collect()
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

    /// The triple terms of partitions read back from a serialization: each partition's key and
    /// table (the columns id, subject and object, as `partitions` gives them). The ids must be
    /// 0 to n-1 for n triple terms, each once, as the counter gives them and nothing removes
    /// them; the next triple term added gets the id n.
    pub fn from_partitions(
        parts: Vec<(TripleTermPartitionKey, DataFrame)>,
    ) -> Result<Self, String> {
        let n: usize = parts.iter().map(|(_, df)| df.height()).sum();
        let mut locations: Vec<Option<(u32, u32)>> = vec![None; n];
        let mut triple_terms = TripleTerms::default();
        for (key, df) in parts {
            let names: Vec<&str> = df.get_column_names().iter().map(|c| c.as_str()).collect();
            if names != [TRIPLE_TERM_ID_COL_NAME, SUBJECT, OBJECT] {
                return Err(format!(
                    "the triple terms of {} have the columns {names:?}, not id, subject and object",
                    key.predicate
                ));
            }
            if triple_terms.index.contains_key(&key) {
                return Err(format!("two partitions of triple terms are {key:?}"));
            }
            let p = triple_terms.partitions.len();
            let ids = df
                .column(TRIPLE_TERM_ID_COL_NAME)
                .and_then(|c| c.u32().cloned())
                .map_err(|e| e.to_string())?;
            for (row, id) in ids.iter().enumerate() {
                let id = id.ok_or("a triple term has no id")?;
                let slot = locations
                    .get_mut(id as usize)
                    .ok_or(format!("the triple term id {id} is not below {n}"))?;
                if slot.is_some() {
                    return Err(format!("two triple terms have the id {id}"));
                }
                *slot = Some((p as u32, row as u32));
            }
            triple_terms.index.insert(key.clone(), p);
            triple_terms
                .partitions
                .push(TripleTermPartition { key, df });
        }
        // n distinct ids below n fill every slot.
        triple_terms.locations = locations.into_iter().map(Option::unwrap).collect();
        Ok(triple_terms)
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
    fn from_partitions_rebuilds_the_ids_and_the_counter() {
        let mut tt = TripleTerms::default();
        let (a, b) = (key(BaseRDFNodeType::IRI), key(BaseRDFNodeType::BlankNode));
        tt.ids(&a, &codes(&[1, 2], &[5, 5]), true).unwrap();
        tt.ids(&b, &codes(&[1], &[7]), true).unwrap();
        tt.ids(&a, &codes(&[3], &[5]), true).unwrap();
        let parts: Vec<_> = tt
            .partitions()
            .iter()
            .map(|p| (p.key.clone(), p.df().clone()))
            .collect();
        let mut back = TripleTerms::from_partitions(parts.clone()).unwrap();
        assert_eq!(back.len(), 4);
        for id in 0..4 {
            let (p, row) = tt.location(id).unwrap();
            let (q, row_back) = back.location(id).unwrap();
            assert_eq!((&p.key, row), (&q.key, row_back));
        }
        // Stored triple terms keep their ids, and the next one gets the next id.
        let more = back.ids(&a, &codes(&[2, 4], &[5, 5]), true).unwrap();
        assert_eq!(ids(&more), vec![Some(1), Some(4)]);

        // Ids that are not 0 to n-1, each once, are refused.
        let mut twice = parts.clone();
        twice[1].1 = twice[0].1.head(Some(1));
        assert!(TripleTerms::from_partitions(twice).is_err());
        let same_key = vec![parts[0].clone(), (parts[0].0.clone(), parts[1].1.clone())];
        assert!(TripleTerms::from_partitions(same_key).is_err());
    }

    #[test]
    fn rows_of_ids_reads_only_the_partitions_that_hold_them() {
        let mut tt = TripleTerms::default();
        let (a, b, c) = (
            key(BaseRDFNodeType::IRI),
            key(BaseRDFNodeType::BlankNode),
            TripleTermPartitionKey {
                predicate: NamedNode::new_unchecked("http://example.org/q"),
                subject_type: BaseRDFNodeType::IRI,
                object_type: BaseRDFNodeType::IRI,
            },
        );
        tt.ids(&a, &codes(&[1, 2], &[5, 5]), true).unwrap(); // ids 0, 1
        tt.ids(&b, &codes(&[1], &[7]), true).unwrap(); // id 2
        tt.ids(&c, &codes(&[3], &[8]), true).unwrap(); // id 3
        let rows = tt.rows_of_ids(&[3, 1, 99]).unwrap();
        let found: Vec<_> = rows
            .iter()
            .map(|(p, df)| {
                let ids: Vec<_> = df
                    .column(TRIPLE_TERM_ID_COL_NAME)
                    .unwrap()
                    .u32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect();
                let subjects: Vec<_> = df
                    .column(SUBJECT)
                    .unwrap()
                    .u32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect();
                (p.key.clone(), ids, subjects)
            })
            .collect();
        // In the order of the partitions; partition b holds none of the ids, and 99 is unknown.
        assert_eq!(found, vec![(a, vec![1], vec![2]), (c, vec![3], vec![3])]);
        assert!(tt.rows_of_ids(&[]).unwrap().is_empty());
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

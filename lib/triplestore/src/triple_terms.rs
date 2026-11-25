//! Adding RDF 1.2 triple terms to the dictionary of triple terms (representation::cats::
//! triple_terms), which gives each distinct triple term a u32 id. Tables store these ids where a
//! triple term is a value, with the object type BaseRDFNodeType::TripleTerm.
//!
//! The subjects and objects of triple terms are encoded as the subjects and objects of triples
//! are (strings as codes of the global dictionaries, other literals as native values), by the
//! same functions, so that a triple term and the triple it quotes have the same values. A nested
//! triple term (the object of a triple term) gets its id first.

use crate::errors::TriplestoreError;
use crate::{can_encode_globally, prepare_add_triples_global, TriplesToAdd, Triplestore};
use oxrdf::{Term, Triple};
use polars::prelude::{DataFrame, IntoColumn, UInt32Chunked};
use polars_core::prelude::NewChunkedArray;
use representation::cats::triple_terms::TripleTermPartitionKey;
use representation::dataset::NamedGraph;
use representation::series_builder::SeriesBuilder;
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashMap;

/// A partition's triple terms: their positions in the input, and their subjects and objects.
struct Group {
    positions: Vec<usize>,
    subjects: SeriesBuilder,
    objects: SeriesBuilder,
}

impl Triplestore {
    /// The ids of the triple terms, adding the ones that are not in the dictionary yet, one per
    /// element in its order (null for None).
    pub(crate) fn add_triple_terms(
        &mut self,
        terms: &[Option<Triple>],
    ) -> Result<UInt32Chunked, TriplestoreError> {
        let mut ids: Vec<Option<u32>> = vec![None; terms.len()];
        if terms.iter().all(Option::is_none) {
            return Ok(UInt32Chunked::from_iter_options(
                OBJECT_COL_NAME.into(),
                ids.into_iter(),
            ));
        }
        // The triple terms that are objects of these get their ids first.
        let nested: Vec<Option<Triple>> = terms
            .iter()
            .map(|t| match t.as_ref().map(|t| &t.object) {
                Some(Term::Triple(o)) => Some(o.as_ref().clone()),
                _ => None,
            })
            .collect();
        let nested_ids = if nested.iter().any(Option::is_some) {
            Some(self.add_triple_terms(&nested)?)
        } else {
            None
        };

        let mut groups: HashMap<TripleTermPartitionKey, Group> = HashMap::new();
        for (i, t) in terms.iter().enumerate() {
            let Some(t) = t else { continue };
            let key = TripleTermPartitionKey {
                predicate: t.predicate.clone(),
                subject_type: BaseRDFNodeType::from_term(&t.subject.clone().into()),
                object_type: BaseRDFNodeType::from_term(&t.object),
            };
            let group = groups.entry(key).or_insert_with_key(|k| Group {
                positions: vec![],
                subjects: SeriesBuilder::new(&k.subject_type),
                objects: if k.object_type.is_triple_term() {
                    SeriesBuilder::U32(vec![])
                } else {
                    SeriesBuilder::new(&k.object_type)
                },
            });
            if let Some(nested_ids) = &nested_ids {
                if let Some(id) = nested_ids.get(i) {
                    group.objects.push_u32(id);
                } else {
                    group.objects.parse_term(&t.object).map_err(|e| {
                        TriplestoreError::UnparseableValue(format!(
                            "the object of the triple term {t} ({e})"
                        ))
                    })?;
                }
            } else {
                group.objects.parse_term(&t.object).map_err(|e| {
                    TriplestoreError::UnparseableValue(format!(
                        "the object of the triple term {t} ({e})"
                    ))
                })?;
            }
            group.subjects.push_named_or_blank(&t.subject);
            group.positions.push(i);
        }

        let mut keys = vec![];
        let mut positions = vec![];
        let mut ttas = vec![];
        for (key, group) in groups {
            let df = DataFrame::new(
                group.positions.len(),
                vec![
                    group.subjects.into_series(SUBJECT_COL_NAME).into_column(),
                    group.objects.into_series(OBJECT_COL_NAME).into_column(),
                ],
            )
            .unwrap();
            ttas.push(crate::add_triples::normalize_datetime_objects(
                TriplesToAdd {
                    df,
                    subject_cat_state: key.subject_type.default_input_cat_state(),
                    object_cat_state: key.object_type.default_input_cat_state(),
                    subject_type: key.subject_type.clone(),
                    object_type: key.object_type.clone(),
                    predicate: Some(key.predicate.clone()),
                    // Not stored in a graph; the encoding does not depend on it.
                    graph: NamedGraph::DefaultGraph,
                    predicate_cat_state: None,
                },
            ));
            keys.push(key);
            positions.push(group.positions);
        }
        if !can_encode_globally(&ttas, &self.global_cats) {
            return Err(TriplestoreError::Rdf12NotSupported(
                "RDF 1.2 triple terms are not supported yet with hybrid or disk storage"
                    .to_string(),
            ));
        }
        let encoded = prepare_add_triples_global(ttas, self.global_cats.clone())?;
        // Each partition is one part, in order, as the triple terms have a predicate and no
        // nulls.
        assert_eq!(encoded.len(), keys.len());
        let mut cats = self.global_cats.write()?;
        for ((key, positions), ct) in keys.iter().zip(positions).zip(encoded) {
            let df = ct.encoded_triples.df;
            assert_eq!(df.height(), positions.len());
            let part_ids = cats
                .triple_terms
                .ids(key, &df, true)
                .map_err(TriplestoreError::LazyLoadError)?;
            for (pos, id) in positions.into_iter().zip(part_ids.iter()) {
                ids[pos] = id;
            }
        }
        Ok(UInt32Chunked::from_iter_options(
            OBJECT_COL_NAME.into(),
            ids.into_iter(),
        ))
    }
}

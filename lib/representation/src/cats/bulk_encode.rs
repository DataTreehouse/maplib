//! Encoding string columns directly with global codes, for in-memory dictionaries.
//!
//! Every value is looked up in the global dictionary in parallel. The values that are not found
//! are deduplicated in parallel and added to the dictionary once, with consecutive codes, and
//! then looked up again. Compared with encoding each column with its own local dictionary and
//! merging those into the global one, this avoids building the local dictionaries, merging them
//! one by one and re-encoding every value from local to global codes.

use super::maps::CatMaps;
use super::{CatEncs, CatType, Cats};
use crate::BaseRDFNodeType;
use in_memory::{distinct_strs, CatMapsInMemory};
use polars::prelude::{IntoSeries, PlSmallStr, Series, UInt32Chunked};
use polars_arrow::array::{Array, PrimitiveArray, Utf8ViewArray};
use rayon::prelude::*;
use std::collections::HashMap;

/// Code of values not yet found in the dictionary (codes never get this large).
const MISS: u32 = u32::MAX;
const ROWS_PER_TASK: usize = 16_384;

impl Cats {
    /// Whether the dictionaries are kept in memory (no storage folder).
    pub fn is_in_memory(&self) -> bool {
        self.path.is_none()
    }

    /// Encodes String series of the given types with global codes, adding the strings that are
    /// not in the dictionary. Returns UInt32 series with the names and nulls of the inputs.
    /// Only for in-memory dictionaries (see `is_in_memory`).
    pub fn encode_strings_global(&mut self, series: Vec<(BaseRDFNodeType, Series)>) -> Vec<Series> {
        let mut by_type: HashMap<CatType, Vec<usize>> = HashMap::new();
        for (i, (t, _)) in series.iter().enumerate() {
            by_type
                .entry(CatType::from_base_rdf_node_type(t))
                .or_default()
                .push(i);
        }
        let mut out: Vec<Option<Series>> = vec![None; series.len()];
        for (ct, idxs) in by_type {
            let bt = ct.as_base_rdf_node_type();
            if !self.cat_map.contains_key(&ct) {
                self.cat_map
                    .insert(ct.clone(), CatEncs::new_empty(None, &bt));
            }
            let mut counter = self.get_counter(&ct);
            let enc = self.cat_map.get_mut(&ct).unwrap();
            let CatMaps::InMemory(m) = &mut enc.maps else {
                panic!("Global encoding of strings requires in-memory dictionaries");
            };
            let stored: Vec<Series>;
            let inputs: Vec<&Series> = if m.is_vector() {
                // Converted once, in parallel, rather than when looked up and when added.
                stored = idxs
                    .iter()
                    .map(|i| crate::vectors::lexical_series_to_stored(&series[*i].1))
                    .collect();
                stored.iter().collect()
            } else {
                idxs.iter().map(|i| &series[*i].1).collect()
            };
            let encoded = encode_with_map(m, &inputs, &mut counter);
            for (i, s) in idxs.into_iter().zip(encoded) {
                out[i] = Some(s);
            }
            self.set_counter(counter, &ct);
        }
        out.into_iter().map(|x| x.unwrap()).collect()
    }
}

/// The single string array of a series.
fn string_array(s: &Series) -> Utf8ViewArray {
    let s = s.rechunk();
    let ca = s.str().expect("Expected a String series");
    let arr = ca.downcast_iter().next().cloned();
    arr.unwrap_or_else(|| {
        Utf8ViewArray::new_empty(polars_arrow::datatypes::ArrowDataType::Utf8View)
    })
}

/// Looks up the values of `arr`, MISS where not found. Consecutive equal values are looked up
/// once (values are often grouped, e.g. by subject or by template constant).
fn lookup(m: &CatMapsInMemory, arr: &Utf8ViewArray) -> Vec<u32> {
    let n = arr.len();
    (0..n.div_ceil(ROWS_PER_TASK))
        .into_par_iter()
        .flat_map_iter(|t| {
            let from = t * ROWS_PER_TASK;
            let to = (from + ROWS_PER_TASK).min(n);
            let mut prev: Option<(&str, u32)> = None;
            (from..to).map(move |i| {
                if !arr.is_valid(i) {
                    return 0;
                }
                let s = arr.value(i);
                if let Some((p, u)) = prev {
                    if p == s {
                        return u;
                    }
                }
                let u = m.maybe_encode_in_memory_str(s).unwrap_or(MISS);
                prev = Some((s, u));
                u
            })
        })
        .collect()
}

fn encode_with_map(m: &mut CatMapsInMemory, inputs: &[&Series], counter: &mut u32) -> Vec<Series> {
    let arrays: Vec<Utf8ViewArray> = inputs.iter().map(|s| string_array(s)).collect();
    // Columns are processed in parallel as well as blocks within columns, as there may be many
    // small columns (e.g. one per predicate).
    let mut codes: Vec<Vec<u32>> = arrays.par_iter().map(|a| lookup(m, a)).collect();

    // The values that are new to the dictionary, each once.
    let missing: Vec<&str> = arrays
        .par_iter()
        .zip(codes.par_iter())
        .flat_map_iter(|(a, c)| {
            c.iter()
                .enumerate()
                .filter(|(_, u)| **u == MISS)
                .map(|(i, _)| a.value(i))
        })
        .collect();
    if !missing.is_empty() {
        let new = distinct_strs(&missing);
        drop(missing);
        m.insert_new_strs(&new, *counter);
        *counter += new.len() as u32;
        drop(new);
        let m = &*m;
        arrays
            .par_iter()
            .zip(codes.par_iter_mut())
            .for_each(|(a, c)| {
                c.par_iter_mut()
                    .enumerate()
                    .with_min_len(4096)
                    .filter(|(_, u)| **u == MISS)
                    .for_each(|(i, u)| *u = m.maybe_encode_in_memory_str(a.value(i)).unwrap());
            });
    }

    inputs
        .iter()
        .zip(arrays)
        .zip(codes)
        .map(|((s, a), c)| {
            let arr = PrimitiveArray::<u32>::from_vec(c).with_validity(a.validity().cloned());
            UInt32Chunked::with_chunk(PlSmallStr::from(s.name().as_str()), arr).into_series()
        })
        .collect()
}

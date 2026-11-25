use super::{CatEncs, CatType, Cats};
use crate::cats::LockedCats;
use crate::solution_mapping::BaseCatState;
use crate::{BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use in_memory::vectors::{is_packed, stored_to_lexical_into};
use in_memory::CatMapsInMemory;
use oxrdf::NamedNode;
use polars::datatypes::{DataType, Field, PlSmallStr};
use polars::frame::column::ScalarColumn;
use polars::frame::DataFrame;
use polars::prelude::{
    as_struct, col, Column, Expr, IntoColumn, IntoLazy, NamedFrom, PolarsResult, SeriesSealed,
};
use polars::prelude::{IntoSeries, StringChunked};
use polars::series::Series;
use polars_arrow::array::{Array, MutableBinaryViewArray, PrimitiveArray, Utf8ViewArray};
use rayon::prelude::*;
use std::borrow::Cow;
use utils::polars::collect_sized;

impl CatEncs {
    // Does not break apart lang strings, assumes only series of u32 in input, not struct.
    pub fn decode_series(
        &self,
        ser: &Series,
        bt: &BaseRDFNodeType,
        local_cats: Option<LockedCats>,
    ) -> PolarsResult<Series> {
        self.decode_series_wrapped(ser, bt, local_cats, "", "")
    }

    /// Decodes a series of codes, putting `prefix` and `suffix` around every value (e.g. "<" and
    /// ">" for IRIs).
    pub fn decode_series_wrapped(
        &self,
        ser: &Series,
        bt: &BaseRDFNodeType,
        local_cats: Option<LockedCats>,
        prefix: &str,
        suffix: &str,
    ) -> PolarsResult<Series> {
        let local_guard = local_cats.as_ref().map(|l| l.read().unwrap());
        let local_enc = local_guard.as_ref().and_then(|g| g.get_enc(bt));
        if let Some(global) = self.maps.as_in_memory() {
            let local = local_enc.map(|e| e.maps.as_in_memory());
            if !matches!(local, Some(None)) {
                // Local codes take precedence over global ones.
                let mut maps = vec![];
                if let Some(Some(l)) = local {
                    maps.push(l);
                }
                maps.push(global);
                return decode_codes(ser, &maps, prefix, suffix);
            }
        }
        drop(local_guard);
        let s = self.decode_series_with_batches(ser, bt, local_cats)?;
        if prefix.is_empty() && suffix.is_empty() {
            Ok(s)
        } else {
            Ok(s.str()?
                .apply_into_string_amortized(|v, out| {
                    out.push_str(prefix);
                    out.push_str(v);
                    out.push_str(suffix);
                })
                .into_series())
        }
    }

    fn decode_series_with_batches(
        &self,
        ser: &Series,
        bt: &BaseRDFNodeType,
        local_cats: Option<LockedCats>,
    ) -> PolarsResult<Series> {
        let original_name = ser.name().clone();
        let mut uch: Vec<_> = ser.u32()?.iter().collect();
        let mut need_global = true;
        //Just a trick for the unlocked local to live long enough
        #[allow(unused_assignments)]
        let mut unlocked_local = None;
        let decoded_local_vec = if let Some(local_cats) = &local_cats {
            unlocked_local = Some(local_cats.read().unwrap());

            if let Some(cat_encs) = unlocked_local.as_ref().unwrap().get_enc(bt) {
                let local_decoded = cat_encs.maps.decode_batch(uch.as_ref());

                let mut new_uch = Vec::with_capacity(uch.len());
                let mut any_global = false;
                for (u, d) in uch.into_iter().zip(&local_decoded) {
                    if u.is_some() {
                        if d.is_none() {
                            any_global = true;
                            new_uch.push(u)
                        } else {
                            new_uch.push(None);
                        }
                    } else {
                        new_uch.push(u);
                    }
                }
                need_global = any_global;
                uch = new_uch;
                Some(local_decoded)
            } else {
                None
            }
        } else {
            None
        };
        let decoded_vec = if let Some(decoded_local_vec) = decoded_local_vec {
            if need_global {
                let decoded_global_vec = self.maps.decode_batch(uch.as_ref());
                let mut decoded_vec = Vec::with_capacity(decoded_local_vec.len());
                for (l, g) in decoded_local_vec.into_iter().zip(decoded_global_vec) {
                    if l.is_some() {
                        decoded_vec.push(l);
                    } else {
                        decoded_vec.push(g);
                    }
                }
                decoded_vec
            } else {
                decoded_local_vec
            }
        } else {
            self.maps.decode_batch(uch.as_ref())
        };
        let new_ser = Series::new(original_name, decoded_vec);
        Ok(new_ser)
    }

    pub fn maybe_decode_string(&self, u: &u32) -> Option<Cow<'_, str>> {
        self.maps.maybe_decode(u)
    }
}

impl Cats {
    pub fn maybe_decode_of_type(&self, u: &u32, bt: &BaseRDFNodeType) -> Option<Cow<'_, str>> {
        let ct = CatType::from_base_rdf_node_type(bt);
        if let Some(cat_encs) = self.cat_map.get(&ct) {
            cat_encs.maybe_decode_string(u)
        } else {
            None
        }
    }

    pub fn decode_iri_u32s(&self, us: &[u32], local_cats: Option<LockedCats>) -> Vec<NamedNode> {
        us.iter()
            .map(|x| self.decode_iri_u32(x, local_cats.clone()))
            .collect()
    }

    pub fn decode_iri_u32(&self, u: &u32, local_cats: Option<LockedCats>) -> NamedNode {
        //Very important that we prefer the local encoding over the global encoding.
        let local = local_cats.as_ref().map(|x| x.read().unwrap());
        let local = local.as_ref();
        let mut maybe_s = if let Some(local) = local {
            if let Some(enc) = local.cat_map.get(&CatType::IRI) {
                enc.maybe_decode_string(u)
            } else {
                None
            }
        } else {
            None
        };
        if maybe_s.is_none() {
            maybe_s = if let Some(enc) = self.cat_map.get(&CatType::IRI) {
                enc.maybe_decode_string(u)
            } else {
                None
            }
        }
        NamedNode::new_unchecked(maybe_s.expect("Should be able to decode"))
    }

    // Does not break apart lang strings, assumes only series of u32 in input, not struct.
    pub fn decode_of_type(
        &self,
        ser: &Series,
        bt: &BaseRDFNodeType,
        local_cats: Option<LockedCats>,
    ) -> PolarsResult<Series> {
        let ct = CatType::from_base_rdf_node_type(bt);
        // It is possible in some cases for the global enc not to exist for a datatype.
        let dummy_enc = CatEncs::new_empty(None, bt);
        let enc = self.cat_map.get(&ct).unwrap_or(&dummy_enc);
        enc.decode_series(ser, bt, local_cats)
    }

    /// Like `decode_of_type`, with `prefix` and `suffix` around every value.
    pub fn decode_of_type_wrapped(
        &self,
        ser: &Series,
        bt: &BaseRDFNodeType,
        local_cats: Option<LockedCats>,
        prefix: &str,
        suffix: &str,
    ) -> PolarsResult<Series> {
        let ct = CatType::from_base_rdf_node_type(bt);
        let dummy_enc = CatEncs::new_empty(None, bt);
        let enc = self.cat_map.get(&ct).unwrap_or(&dummy_enc);
        enc.decode_series_wrapped(ser, bt, local_cats, prefix, suffix)
    }

    pub fn get_enc(&self, t: &BaseRDFNodeType) -> Option<&CatEncs> {
        let ct = CatType::from_base_rdf_node_type(t);
        self.cat_map.get(&ct)
    }
}

pub fn decode_column(
    column: &Column,
    base_type: &BaseRDFNodeType,
    base_state: &BaseCatState,
    global_cats: LockedCats,
) -> Column {
    let name = column.name().to_string();
    let mut df = DataFrame::new(column.len(), vec![column.clone()]).unwrap();
    let mut expr = col(&name);
    expr = maybe_decode_expr(expr, base_type, base_state, global_cats);
    let height = df.height();
    df = collect_sized(df.lazy().with_column(expr.alias(&name)), height).unwrap();
    df.drop_in_place(&name).unwrap()
}

// Also decodes structs (lang strings)
pub fn maybe_decode_complex_expr(
    expr: Expr,
    base_type: &BaseRDFNodeType,
    base_state: &BaseCatState,
    global_cats: LockedCats,
) -> Expr {
    // Triple terms are ids of the dictionary of triple terms, decoded to their N-Triples form.
    if base_type.is_triple_term() {
        return crate::polars_to_rdf::triple_term_expr_to_string(expr, global_cats);
    }
    match base_state {
        BaseCatState::CategoricalNative(local_cats) => {
            if base_type.is_lang_string() {
                let value_decode = decode_expr(
                    expr.clone()
                        .struct_()
                        .field_by_name(LANG_STRING_VALUE_FIELD),
                    base_type.clone(),
                    local_cats.as_ref().cloned(),
                    global_cats.clone(),
                );
                let lang_decode = decode_expr(
                    expr.struct_().field_by_name(LANG_STRING_LANG_FIELD),
                    base_type.clone(),
                    local_cats.as_ref().cloned(),
                    global_cats,
                );
                as_struct(vec![
                    value_decode.alias(LANG_STRING_VALUE_FIELD),
                    lang_decode.alias(LANG_STRING_LANG_FIELD),
                ])
            } else {
                decode_expr(
                    expr,
                    base_type.clone(),
                    local_cats.as_ref().cloned(),
                    global_cats,
                )
            }
        }
        BaseCatState::String | BaseCatState::NonString => expr,
    }
}

// Does not break apart lang strings, assumes only series of u32 in input, not struct.
pub fn maybe_decode_expr(
    expr: Expr,
    base_type: &BaseRDFNodeType,
    base_state: &BaseCatState,
    global_cats: LockedCats,
) -> Expr {
    match base_state {
        BaseCatState::CategoricalNative(local_cats) => decode_expr(
            expr,
            base_type.clone(),
            local_cats.as_ref().cloned(),
            global_cats,
        ),
        BaseCatState::String | BaseCatState::NonString => expr,
    }
}

// Does not break apart lang strings, assumes only series of u32 in input, not struct.
pub fn optional_maybe_decode_expr(
    expr: Expr,
    base_type: &BaseRDFNodeType,
    base_state: &BaseCatState,
    global_cats: LockedCats,
) -> Option<Expr> {
    match base_state {
        BaseCatState::CategoricalNative(local_cats) => Some(decode_expr(
            expr,
            base_type.clone(),
            local_cats.as_ref().cloned(),
            global_cats,
        )),
        BaseCatState::String | BaseCatState::NonString => None,
    }
}

// Does not break apart lang strings, assumes only series of u32 in input, not struct.
pub fn decode_expr(
    expr: Expr,
    base_rdf_node_type: BaseRDFNodeType,
    local_cats: Option<LockedCats>,
    global_cats: LockedCats,
) -> Expr {
    decode_expr_wrapped(expr, base_rdf_node_type, local_cats, global_cats, "", "")
}

/// Decodes codes, putting `prefix` and `suffix` around every value in the same pass.
pub fn decode_expr_wrapped(
    expr: Expr,
    base_rdf_node_type: BaseRDFNodeType,
    local_cats: Option<LockedCats>,
    global_cats: LockedCats,
    prefix: &'static str,
    suffix: &'static str,
) -> Expr {
    expr.map(
        move |x| {
            let original_name = x.name().to_string();
            let global_cats = global_cats.read().unwrap();
            let len = x.len();
            let mut s = match x {
                Column::Series(x) => {
                    let ser = global_cats.decode_of_type_wrapped(
                        x.as_series(),
                        &base_rdf_node_type,
                        local_cats.clone(),
                        prefix,
                        suffix,
                    )?;
                    ser.into_column()
                }
                Column::Scalar(s) => {
                    let s = global_cats.decode_of_type_wrapped(
                        &s.as_single_value_series(),
                        &base_rdf_node_type,
                        local_cats.clone(),
                        prefix,
                        suffix,
                    )?;
                    Column::Scalar(ScalarColumn::from_single_value_series(s, len))
                }
            };
            s.rename(PlSmallStr::from_string(original_name));
            Ok(s.into_column())
        },
        |_, f| Ok(Field::new(f.name().clone(), DataType::String)),
    )
}

const DECODE_ROWS_PER_TASK: usize = 16_384;
const DECODE_VECTOR_ROWS_PER_TASK: usize = 256;

/// Decodes a UInt32 series with in-memory dictionaries (the first that has a code wins), writing
/// each value once into a string array, in parallel blocks of rows.
fn decode_codes(
    ser: &Series,
    maps: &[&CatMapsInMemory],
    prefix: &str,
    suffix: &str,
) -> PolarsResult<Series> {
    let ser = ser.rechunk();
    let ch = ser.u32()?;
    let n = ch.len();
    let empty = PrimitiveArray::<u32>::from_vec(vec![]);
    let arr = ch.downcast_iter().next().unwrap_or(&empty);
    let wrap = !prefix.is_empty() || !suffix.is_empty();
    // Vector literals are formatted when decoded, which costs much more per row than copying a
    // string, so they are decoded in smaller blocks to use all threads.
    let rows_per_task = if maps.iter().any(|m| m.is_vector()) {
        DECODE_VECTOR_ROWS_PER_TASK
    } else {
        DECODE_ROWS_PER_TASK
    };
    let decode_block = |t: usize| {
        let from = t * rows_per_task;
        let to = (from + rows_per_task).min(n);
        let mut b = MutableBinaryViewArray::<str>::with_capacity(to - from);
        let mut scratch = String::new();
        for i in from..to {
            if !arr.is_valid(i) {
                b.push_null();
                continue;
            }
            let u = arr.value(i);
            match maps
                .iter()
                .find_map(|m| m.decode_parts(u).map(|pq| (m.is_vector(), pq)))
            {
                None => b.push_null(),
                Some((true, (_, q))) if is_packed(q) => {
                    scratch.clear();
                    scratch.push_str(prefix);
                    stored_to_lexical_into(q, &mut scratch);
                    scratch.push_str(suffix);
                    b.push_value(scratch.as_str());
                }
                Some((_, (p, q))) if p.is_empty() && !wrap => b.push_value(q),
                Some((_, (p, q))) => {
                    scratch.clear();
                    scratch.push_str(prefix);
                    scratch.push_str(p);
                    scratch.push_str(q);
                    scratch.push_str(suffix);
                    b.push_value(scratch.as_str());
                }
            }
        }
        b.freeze()
    };
    // A single block is decoded on the calling thread: handing it to the thread pool and waiting
    // for it costs more than decoding a few thousand rows, e.g. the 10 rows of a LIMIT 10.
    let n_blocks = n.div_ceil(rows_per_task);
    let chunks: Vec<Utf8ViewArray> = if n_blocks <= 1 {
        (0..n_blocks).map(decode_block).collect()
    } else {
        (0..n_blocks).into_par_iter().map(decode_block).collect()
    };
    let name = ser.name().clone();
    if chunks.is_empty() {
        return Ok(Series::new_empty(name, &DataType::String));
    }
    Ok(StringChunked::from_chunk_iter(name, chunks).into_series())
}

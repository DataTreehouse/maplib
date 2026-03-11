use super::{CatEncs, CatType, Cats};
use crate::cats::LockedCats;
use crate::solution_mapping::BaseCatState;
use crate::{BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use oxrdf::NamedNode;
use polars::datatypes::{DataType, Field, PlSmallStr};
use polars::frame::column::ScalarColumn;
use polars::frame::DataFrame;
use polars::prelude::{
    as_struct, col, Column, Expr, IntoColumn, IntoLazy, NamedFrom, PolarsResult, SeriesSealed,
};
use polars::series::Series;
use rayon::iter::ParallelIterator;
use std::borrow::Cow;

impl CatEncs {
    // Does not break apart lang strings, assumes only series of u32 in input, not struct.
    pub fn decode_series(
        &self,
        ser: &Series,
        bt: &BaseRDFNodeType,
        local_cats: Option<LockedCats>,
    ) -> PolarsResult<Series> {
        let original_name = ser.name().clone();
        let mut uch: Vec<_> = ser.u32()?.iter().collect();
        let mut need_global = true;
        //Just a trick for the unlocked local to live long enough
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
    df = df.lazy().with_column(expr.alias(&name)).collect().unwrap();
    df.drop_in_place(&name).unwrap()
}

// Also decodes structs (lang strings)
pub fn maybe_decode_complex_expr(
    expr: Expr,
    base_type: &BaseRDFNodeType,
    base_state: &BaseCatState,
    global_cats: LockedCats,
) -> Expr {
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
    expr.map(
        move |x| {
            let original_name = x.name().to_string();
            let global_cats = global_cats.read().unwrap();
            let len = x.len();
            let mut s = match x {
                Column::Series(x) => {
                    let ser = global_cats.decode_of_type(
                        x.as_series(),
                        &base_rdf_node_type,
                        local_cats.as_ref().map(|x| x.clone()),
                    )?;
                    ser.into_column()
                }
                Column::Scalar(s) => {
                    let s = global_cats.decode_of_type(
                        &s.as_single_value_series(),
                        &base_rdf_node_type,
                        local_cats.as_ref().map(|x| x.clone()),
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

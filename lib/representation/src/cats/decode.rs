use super::{CatEncs, CatType, Cats};
use crate::solution_mapping::BaseCatState;
use crate::BaseRDFNodeType;
use polars::datatypes::{DataType, Field, PlSmallStr};
use polars::frame::DataFrame;
use polars::prelude::{col, Column, Expr, IntoColumn, IntoLazy, IntoSeries, StringChunked, UInt32Chunked};
use polars::series::Series;
use std::sync::Arc;
use rayon::iter::{IntoParallelRefIterator, ParallelBridge, ParallelIterator};

impl CatEncs {
    pub fn decode(&self, ser: &Series, cat_type: &CatType) -> Series {
        let original_name = ser.name().clone();
        let uch = ser.u32().unwrap();
        let decoded_ser = uch.iter().map(|x| x.map(|x| self.rev_map.get(&x).unwrap()));
        let mut new_ser = if let CatType::Prefix(pre) = cat_type {
            Series::from_iter(decoded_ser.map(|x| x.map(|x| format!("{}{}", pre.as_str(), x))))
        } else {
            Series::from_iter(decoded_ser.map(|x| x.map(|x| x.as_str())))
        };
        new_ser.rename(original_name);
        new_ser
    }

    pub fn maybe_decode_string(&self, u: &u32, cat_type: &CatType) -> Option<String> {
        if let Some(s) = self.rev_map.get(u) {
            if let CatType::Prefix(pre) = cat_type {
                Some(format!("{}{}", pre.as_str(), s))
            } else {
                Some(s.to_string())
            }
        } else {
            None
        }
    }
}

impl Cats {
    pub fn decode_of_type(&self, ser: &Series, cat_type: &CatType) -> Option<Series> {
        if let Some(enc) = self.cat_map.get(cat_type) {
            Some(enc.decode(ser, cat_type))
        } else {
            unreachable!("Should never be called when type does not exist")
        }
    }

    pub fn get_encs(&self, t: &BaseRDFNodeType) -> Vec<(&CatType, &CatEncs)> {
        let encs = match t {
            BaseRDFNodeType::IRI => {
                let encs: Vec<_> = self
                    .cat_map
                    .iter()
                    .map(|(k, v)| {
                        if matches!(k, CatType::Prefix(..)) {
                            Some((k, v))
                        } else {
                            None
                        }
                    })
                    .filter(|x| x.is_some())
                    .map(|x| x.unwrap())
                    .collect();
                encs
            }
            BaseRDFNodeType::BlankNode => {
                if let Some(enc) = self.cat_map.get_key_value(&CatType::Blank) {
                    vec![enc]
                } else {
                    vec![]
                }
            }
            BaseRDFNodeType::Literal(nn) => {
                if let Some(enc) = self.cat_map.get_key_value(&CatType::Literal(nn.clone())) {
                    vec![enc]
                } else {
                    vec![]
                }
            }
            BaseRDFNodeType::None => {
                unreachable!("Should never happen")
            }
        };
        encs
    }

    pub fn decode(
        &self,
        ser: &Series,
        t: &BaseRDFNodeType,
        local_cats: Option<Arc<Cats>>,
    ) -> Series {
        let mut encs = self.get_encs(t);
        let local = local_cats.as_ref().map(|x| x.as_ref());
        if let Some(local_cats) = local {
            encs.extend(local_cats.get_encs(t))
        }
        let u32s = ser.u32().unwrap();
        let us:Vec<_> = u32s.iter().collect();
        let strings: Vec<_> = us.par_iter().map(|u| {
            let s = if let Some(u) = u {
                let mut s = None;
                for (t, e) in &encs {
                    if let Some(st) = e.maybe_decode_string(&u, t) {
                        s = Some(st);
                        break;
                    }
                }
                Some(s.expect("Expect all cats to resolve"))
            } else {
                None
            };
            s
        }).collect();
        Series::from_iter(strings)
    }
}

pub fn decode_column(
    column: &Column,
    base_type: &BaseRDFNodeType,
    base_state: &BaseCatState,
    global_cats: Arc<Cats>,
) -> Column {
    let name = column.name().to_string();
    let mut df = DataFrame::new(vec![column.clone()]).unwrap();
    let mut expr = col(&name);
    expr = maybe_decode_expr(expr, base_type, base_state, global_cats);
    df = df.lazy().with_column(expr.alias(&name)).collect().unwrap();
    df.drop_in_place(&name).unwrap()
}

pub fn maybe_decode_expr(
    expr: Expr,
    base_type: &BaseRDFNodeType,
    base_state: &BaseCatState,
    global_cats: Arc<Cats>,
) -> Expr {
    match base_state {
        BaseCatState::CategoricalNative(_, local_cats) => decode_expr(
            expr,
            base_type.clone(),
            local_cats.as_ref().cloned(),
            global_cats,
        ),
        BaseCatState::String | BaseCatState::NonString => expr,
    }
}

pub fn optional_maybe_decode_expr(
    expr: Expr,
    base_type: &BaseRDFNodeType,
    base_state: &BaseCatState,
    global_cats: Arc<Cats>,
) -> Option<Expr> {
    match base_state {
        BaseCatState::CategoricalNative(_, local_cats) => Some(decode_expr(
            expr,
            base_type.clone(),
            local_cats.as_ref().cloned(),
            global_cats,
        )),
        BaseCatState::String | BaseCatState::NonString => None,
    }
}

pub fn decode_expr(
    expr: Expr,
    base_rdf_node_type: BaseRDFNodeType,
    local_cats: Option<Arc<Cats>>,
    global_cats: Arc<Cats>,
) -> Expr {
    expr.map(
        move |x| {
            let original_name = x.name().to_string();
            let mut s = global_cats.decode(
                x.as_materialized_series(),
                &base_rdf_node_type,
                local_cats.as_ref().map(|x| x.clone()),
            );
            s.rename(PlSmallStr::from_string(original_name));
            Ok(s.into_column())
        },
        |_, f| Ok(Field::new(f.name().clone(), DataType::String)),
    )
}

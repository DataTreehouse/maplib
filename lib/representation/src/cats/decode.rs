use super::{CatEncs, CatType, Cats};
use crate::cats::LockedCats;
use crate::solution_mapping::BaseCatState;
use crate::BaseRDFNodeType;
use oxrdf::NamedNode;
use polars::datatypes::{DataType, Field, PlSmallStr};
use polars::frame::column::ScalarColumn;
use polars::frame::DataFrame;
use polars::prelude::{col, Column, Expr, IntoColumn, IntoLazy, NamedFrom, SeriesSealed};
use polars::series::Series;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::borrow::Cow;

impl CatEncs {
    pub fn decode_series(&self, ser: &Series) -> Series {
        let original_name = ser.name().clone();
        let uch: Vec<_> = ser.u32().unwrap().iter().collect();
        let decoded_vec = self.maps.decode_batch(uch.as_ref());
        let new_ser = Series::new(original_name, decoded_vec);
        new_ser
    }

    pub fn maybe_decode_string(&self, u: &u32) -> Option<&str> {
        self.maps.maybe_decode(u)
    }
}

impl Cats {
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

    pub fn decode_of_type(&self, ser: &Series, bt: &BaseRDFNodeType) -> Series {
        let ct = CatType::from_base_rdf_node_type(bt);
        if let Some(enc) = self.cat_map.get(&ct) {
            enc.decode_series(ser)
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
                        if matches!(k, CatType::IRI) {
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
        local_cats: Option<LockedCats>,
    ) -> Series {
        let local = local_cats.as_ref().map(|x| x.read().unwrap());
        let u32s = ser.u32().unwrap();
        let us: Vec<_> = u32s.iter().collect();

        //Very important that we prefer the local encoding over the global encoding.
        let mut encs = if let Some(local_cats) = &local {
            local_cats.get_encs(t)
        } else {
            vec![]
        };
        encs.extend(self.get_encs(t));

        let strings: Vec<_> = us
            .par_iter()
            .map(|u| {
                let s = if let Some(u) = u {
                    let mut s = None;
                    for (_, e) in &encs {
                        if let Some(st) = e.maybe_decode_string(&u) {
                            s = Some(st);
                            break;
                        }
                    }
                    Some(Cow::Borrowed(s.expect("Expect all cats to resolve")))
                } else {
                    None
                };
                s
            })
            .collect();

        let strs = strings.iter().map(|x| {
            if let Some(x) = x {
                Some(x.as_str())
            } else {
                None
            }
        });
        Series::from_iter(strs)
    }
}

pub fn decode_column(
    column: &Column,
    base_type: &BaseRDFNodeType,
    base_state: &BaseCatState,
    global_cats: LockedCats,
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
    global_cats: LockedCats,
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
    global_cats: LockedCats,
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
                    let ser = global_cats.decode(
                        x.as_series(),
                        &base_rdf_node_type,
                        local_cats.as_ref().map(|x| x.clone()),
                    );
                    ser.into_column()
                }
                Column::Partitioned(p) => p
                    .apply_unary_elementwise(|x| {
                        global_cats.decode(
                            x,
                            &base_rdf_node_type,
                            local_cats.as_ref().map(|x| x.clone()),
                        )
                    })
                    .into_column(),
                Column::Scalar(s) => {
                    let s = global_cats.decode(
                        &s.as_single_value_series(),
                        &base_rdf_node_type,
                        local_cats.as_ref().map(|x| x.clone()),
                    );
                    Column::Scalar(ScalarColumn::from_single_value_series(s, len))
                }
            };
            s.rename(PlSmallStr::from_string(original_name));
            Ok(s.into_column())
        },
        |_, f| Ok(Field::new(f.name().clone(), DataType::String)),
    )
}

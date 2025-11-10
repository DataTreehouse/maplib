use super::{CatEncs, CatType, Cats};
use crate::cats::LockedCats;
use crate::solution_mapping::BaseCatState;
use crate::BaseRDFNodeType;
use oxrdf::NamedNode;
use polars::datatypes::{DataType, Field, PlSmallStr};
use polars::frame::DataFrame;
use polars::prelude::{col, Column, Expr, IntoColumn, IntoLazy, NamedFrom};
use polars::series::Series;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::borrow::Cow;
use std::time::Instant;

impl CatEncs {
    pub fn decode(&self, ser: &Series, cat_type: &CatType) -> Series {
        if let CatType::Prefix(..) = cat_type {
            panic!("Should never happen, this happens on the cats level")
        }
        let original_name = ser.name().clone();
        let uch: Vec<_> = ser.u32().unwrap().iter().collect();
        let decoded_vec_iter = uch
            .into_par_iter()
            .map(|x| x.map(|x| self.rev_map.as_ref().unwrap().get(&x).unwrap()));

        let decoded_vec: Vec<_> = decoded_vec_iter.map(|x| x.map(|x| x.as_str())).collect();
        let new_ser = Series::new(original_name, decoded_vec);
        //
        // let mut new_ser = if let CatType::Prefix(pre) = cat_type {
        //     Series::from_iter(decoded_ser.map(|x| x.map(|x| format!("{}{}", pre.as_str(), x))))
        // } else {
        //     Series::from_iter(decoded_ser.map(|x| x.map(|x| x.as_str())))
        // };
        new_ser
    }

    pub fn maybe_decode_non_iri_string(&self, u: &u32) -> Option<&str> {
        self.rev_map.as_ref().unwrap().get(u).map(|x| x.as_str())
    }
}

impl Cats {
    pub fn decode_iri_u32s(&self, us: &[u32], local_cats: Option<LockedCats>) -> Vec<NamedNode> {
        us.par_iter()
            .map(|x| self.decode_iri_u32(x, local_cats.clone()))
            .collect()
    }

    pub fn decode_iri_u32(&self, u: &u32, local_cats: Option<LockedCats>) -> NamedNode {
        //Very important that we prefer the local encoding over the global encoding.
        let local = local_cats.as_ref().map(|x| x.read().unwrap());
        let local = local.as_ref();
        let mut maybe_s = if let Some(local) = local {
            local.maybe_decode_iri_string(u, true)
        } else {
            None
        };
        if maybe_s.is_none() {
            maybe_s = self.maybe_decode_iri_string(u, true);
        }
        NamedNode::new_unchecked(maybe_s.expect("Should be able to decode"))
    }

    pub fn maybe_decode_iri_string(&self, u: &u32, add_prefix: bool) -> Option<Cow<str>> {
        if let Some(s) = self.rev_iri_suffix_map.get(u) {
            if add_prefix {
                let prefix = self.prefix_map.get(self.belongs_prefix_map.get(u).unwrap()).unwrap();
                Some(Cow::Owned(format!("{}{}", prefix.as_str(), s)))
            } else {
                Some(Cow::Borrowed(s))
            }
        } else {
            None
        }
    }

    pub fn decode_of_type(&self, ser: &Series, cat_type: &CatType) -> Series {
        if matches!(cat_type, CatType::Prefix(..)) {
            let original_name = ser.name().clone();
            let uch: Vec<_> = ser.u32().unwrap().iter().collect();
            let decoded_vec: Vec<_> = uch
                .into_par_iter()
                .map(|x| x.map(|x| self.maybe_decode_iri_string(&x, true).unwrap())).collect();

            let new_ser = Series::new(original_name, decoded_vec);
            //
            // let mut new_ser = if let CatType::Prefix(pre) = cat_type {
            //     Series::from_iter(decoded_ser.map(|x| x.map(|x| format!("{}{}", pre.as_str(), x))))
            // } else {
            //     Series::from_iter(decoded_ser.map(|x| x.map(|x| x.as_str())))
            // };
            new_ser
        } else {
            if let Some(enc) = self.cat_map.get(cat_type) {
                enc.decode(ser, cat_type)
            } else {
                unreachable!("Should never be called when type does not exist")
            }
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
        local_cats: Option<LockedCats>,
        add_prefix: bool,
    ) -> Series {
        let local = local_cats.as_ref().map(|x| x.read().unwrap());
        let u32s = ser.u32().unwrap();
        let us: Vec<_> = u32s.iter().collect();
        let strings = if t.is_iri() {
            let strings: Vec<_> = us
                .par_iter()
                .map(|u| {

                    let s = if let Some(u) =u {
                        let mut s = None;
                        if let Some(local_cats) = &local {
                            s = local_cats.maybe_decode_iri_string(u, add_prefix);
                        }
                        if s.is_none() {
                            s = self.maybe_decode_iri_string(u, add_prefix);
                        }


                        Some(s.expect("Expect all cats to resolve"))
                    } else {
                    None
                    };
                    s
                })
                .collect();
            strings
        } else {
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
                            if let Some(st) = e.maybe_decode_non_iri_string(&u) {
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
            strings
        };
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
            true,
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
            true,
        )),
        BaseCatState::String | BaseCatState::NonString => None,
    }
}

pub fn decode_expr(
    expr: Expr,
    base_rdf_node_type: BaseRDFNodeType,
    local_cats: Option<LockedCats>,
    global_cats: LockedCats,
    add_prefix: bool,
) -> Expr {
    expr.map(
        move |x| {
            //println!("Start decode {}", x.name());
            let start_decode = Instant::now();
            let original_name = x.name().to_string();
            let global_cats = global_cats.read().unwrap();
            let mut s = global_cats.decode(
                x.as_materialized_series(),
                &base_rdf_node_type,
                local_cats.as_ref().map(|x| x.clone()),
                add_prefix,
            );
            s.rename(PlSmallStr::from_string(original_name));
            //println!("End decode {}", start_decode.elapsed().as_secs_f32());
            Ok(s.into_column())
        },
        |_, f| Ok(Field::new(f.name().clone(), DataType::String)),
    )
}

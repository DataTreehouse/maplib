use polars::prelude::Expr;
use representation::cats::{maybe_decode_expr, CatReEnc, CatType, Cats};
use representation::solution_mapping::BaseCatState;
use representation::{BaseRDFNodeType, RDFNodeState};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub fn create_compatible_cats(
    mut expressions: Vec<Option<Expr>>,
    mut states: Vec<Option<RDFNodeState>>,
    global_cats: Arc<Cats>,
) -> Vec<Option<HashMap<BaseRDFNodeType, (Vec<Expr>, BaseCatState)>>> {
    let mut base_types = HashSet::new();
    for s in &states {
        if let Some(s) = s {
            base_types.extend(s.map.keys())
        }
    }
    let mut base_types: Vec<_> = base_types.into_iter().cloned().collect();
    base_types.sort();

    let mut native_cat_map = HashMap::new();
    let mut string_cast = HashSet::new();

    for t in base_types {
        let need_native_cat_cast = check_need_native_cat_cast(&t, &states);
        if need_native_cat_cast {
            let locals = find_all_locals(&t, &states);
            let mut cats = Cats::new_empty();
            let renc_local = cats.merge(locals);
            let renc_local: HashMap<_, _> = renc_local
                .into_iter()
                .map(|(uu, map)| {
                    let mut blank_renc = None;
                    let mut iri_renc = vec![];
                    let mut literal_renc_map = HashMap::new();
                    for (p, renc) in map {
                        match p {
                            CatType::Prefix(_) => {
                                iri_renc.push(renc);
                            }
                            CatType::Blank => {
                                blank_renc = Some(renc);
                            }
                            CatType::Literal(l) => {
                                literal_renc_map.insert(l, renc);
                            }
                        }
                    }
                    let iri_renc = if iri_renc.is_empty() {
                        None
                    } else {
                        let mut renc_map = HashMap::new();
                        for renc in iri_renc {
                            renc_map.extend(renc.cat_map.iter().map(|(x, y)| (*x, *y)))
                        }
                        Some(CatReEnc {
                            cat_map: Arc::new(renc_map),
                        })
                    };
                    (uu, (blank_renc, iri_renc, literal_renc_map))
                })
                .collect();
            let state = BaseCatState::CategoricalNative(false, Some(Arc::new(cats)));
            native_cat_map.insert(t.clone(), (renc_local, state));
        }
        let need_string_cast = check_need_string_cast(&t, &states);
        if need_string_cast {
            string_cast.insert(t.clone());
        }
    }

    let mut exploded: Vec<_> = expressions
        .into_iter()
        .zip(states.into_iter())
        .map(|(e, s)| {
            if let (Some(e), Some(mut s)) = (e, s) {
                let exprs = if s.is_multi() {
                    let mut v = Vec::with_capacity(s.map.len());
                    for (bt, bs) in s.map.drain() {
                        let mut exprs = vec![];
                        for s in bt.multi_columns() {
                            exprs.push(e.clone().struct_().field_by_name(&s));
                        }
                        v.push((exprs, bt, bs));
                    }
                    v
                } else {
                    let (bt, bs) = s.map.drain().next().unwrap();
                    let exprs = if bt.is_lang_string() {
                        let mut exprs = vec![];
                        for s in bt.multi_columns() {
                            exprs.push(e.clone().struct_().field_by_name(&s));
                        }
                        exprs
                    } else {
                        vec![e.alias(bt.field_col_name())]
                    };
                    vec![(exprs, bt, bs)]
                };
                Some(exprs)
            } else {
                None
            }
        })
        .collect();

    for v in exploded.iter_mut() {
        if let Some(v) = v {
            for (e_vec, bt, bs) in v.iter_mut() {
                if let Some((m, state)) = native_cat_map.get(bt) {
                    let is_local = if let Some(local) = bs.get_local_cats() {
                        if let Some((blank_renc, iri_renc, literal_renc)) = m.get(&local.uuid) {
                            let renc = match bt {
                                BaseRDFNodeType::IRI => iri_renc.as_ref(),
                                BaseRDFNodeType::BlankNode => blank_renc.as_ref(),
                                BaseRDFNodeType::Literal(l) => literal_renc.get(l),
                                BaseRDFNodeType::None => {
                                    unreachable!()
                                }
                            };
                            if let Some(renc) = renc {
                                for e in e_vec.iter_mut() {
                                    let renc = renc.clone();
                                    let e_clone = e.clone();
                                    *e = e_clone.map(
                                        move |x| renc.clone().re_encode_column(x, false),
                                        |x, f| Ok(f.clone()),
                                    );
                                }
                            }
                        }
                        true
                    } else {
                        false
                    };

                    if is_local {
                        *bs = state.clone();
                    }
                }
                if string_cast.contains(bt) {
                    if matches!(bs, BaseCatState::CategoricalNative(..)) {
                        let mut new_e_vec = vec![];
                        for mut e in e_vec.drain(..) {
                            e = maybe_decode_expr(e, bt, bs, global_cats.clone());
                            new_e_vec.push(e);
                        }
                        *e_vec = new_e_vec;
                        *bs = BaseCatState::String;
                    }
                }
            }
        }
    }
    let mut out_vec = vec![];
    for e in exploded {
        if let Some(e) = e {
            let map: HashMap<_, _> = e.into_iter().map(|(e, t, s)| (t, (e, s))).collect();
            out_vec.push(Some(map));
        } else {
            out_vec.push(None);
        }
    }
    out_vec
}

fn check_need_native_cat_cast(t: &BaseRDFNodeType, types: &Vec<Option<RDFNodeState>>) -> bool {
    let mut found_non_global_witness = false;
    for s in types {
        if let Some(s) = s {
            if let Some(b) = s.map.get(t) {
                if matches!(b, BaseCatState::String | BaseCatState::NonString) {
                    return false;
                }
                let w = matches!(b, BaseCatState::CategoricalNative(_, Some(_)));
                if w {
                    if found_non_global_witness {
                        return true;
                    } else {
                        found_non_global_witness = true;
                    }
                }
            }
        }
    }
    false
}

fn check_need_string_cast(t: &BaseRDFNodeType, types: &Vec<Option<RDFNodeState>>) -> bool {
    let mut found_string = false;
    let mut found_cat = false;
    for s in types {
        if let Some(s) = s {
            if let Some(b) = s.map.get(t) {
                if matches!(b, BaseCatState::CategoricalNative(_, _)) {
                    if found_string {
                        return true;
                    } else {
                        found_cat = true;
                    }
                }
                if matches!(b, BaseCatState::String) {
                    if found_cat {
                        return true;
                    } else {
                        found_string = true;
                    }
                }
            }
        }
    }
    false
}

fn find_all_locals(t: &BaseRDFNodeType, states: &Vec<Option<RDFNodeState>>) -> Vec<Arc<Cats>> {
    let mut locals = vec![];
    for s in states {
        if let Some(s) = s {
            if let Some(bs) = s.map.get(t) {
                if let Some(lc) = bs.get_local_cats() {
                    locals.push(lc);
                }
            }
        }
    }
    locals
}

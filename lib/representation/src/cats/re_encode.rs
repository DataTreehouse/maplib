use super::{CatEncs, CatTriples, CatType, Cats};
use crate::cats::LockedCats;
use crate::solution_mapping::{BaseCatState, EagerSolutionMappings};
use crate::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use nohash_hasher::NoHashHasher;
use polars::datatypes::PlSmallStr;
use polars::error::PolarsResult;
use polars::prelude::{col, Column, IntoColumn, IntoLazy, LazyFrame, Series};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct CatReEnc {
    pub cat_map: Arc<HashMap<u32, u32, BuildHasherDefault<NoHashHasher<u32>>>>,
}

impl CatReEnc {
    pub fn re_encode(self, mut lf: LazyFrame, c: &str, forget_others: bool) -> LazyFrame {
        lf = lf.with_column(
            col(c)
                .map(
                    move |x| self.clone().re_encode_column(x, forget_others),
                    |_, f| Ok(f.clone()),
                )
                .alias(c),
        );
        lf
    }

    pub fn re_encode_column(self, c: Column, forget_others: bool) -> PolarsResult<Column> {
        let start_reenc = Instant::now();
        let uch = c.u32().unwrap();
        let mut v = Vec::with_capacity(uch.len());
        for u in uch {
            let u = if let Some(u) = u {
                if let Some(remap_u) = self.cat_map.get(&u) {
                    Some(*remap_u)
                } else if !forget_others {
                    Some(u)
                } else {
                    None
                }
            } else {
                None
            };
            v.push(u);
        }
        let s = Series::from_iter(v);
        Ok(s.into_column())
    }
}

impl CatEncs {
    pub fn inner_join_re_enc(&self, other: &CatEncs) -> Vec<(u32, u32)> {
        let renc: Vec<_> = self
            .map
            .par_iter()
            .map(|(x, l)| {
                if let Some(r) = other.maybe_encode_str(x) {
                    if l != r {
                        Some((*l, *r))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect();
        renc
    }
}

impl Cats {
    //Re enc is for right hand side
    pub fn join(left: LockedCats, right: LockedCats) -> CatReEnc {
        let left = left.read().unwrap();
        let right = right.read().unwrap();
        let rencs: Vec<_> = left
            .cat_map
            .par_iter()
            .map(|(t, left_enc)| {
                if let Some(right_enc) = right.cat_map.get(t) {
                    let renc = left_enc.inner_join_re_enc(right_enc);
                    Some(renc)
                } else {
                    None
                }
            })
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect();
        let renc_map: HashMap<_, _, BuildHasherDefault<NoHashHasher<u32>>> =
            rencs.into_iter().flatten().map(|x| x).collect();
        let cat_re_enc = CatReEnc {
            cat_map: Arc::new(renc_map),
        };
        cat_re_enc
    }

    pub fn merge(
        &mut self,
        other_cats: Vec<LockedCats>,
    ) -> HashMap<String, HashMap<CatType, CatReEnc>> {
        let mut map = HashMap::new();
        for c in other_cats {
            let c = c.read().unwrap();
            let mut other_map = HashMap::new();
            for (t, other_enc) in c.cat_map.iter() {
                let prefix_u = if let CatType::Prefix(nn) = t {
                    let i = if let Some(i) = self.prefix_rev_map.get(nn) {
                        *i
                    } else {
                        let i = self.prefix_map.len() as u32;
                        self.prefix_map.insert(i, nn.clone());
                        self.prefix_rev_map.insert(nn.clone(), i);
                        i
                    };
                    Some(i)
                } else {
                    None
                };
                let mut c = self.get_height(&t);
                if let Some(enc) = self.cat_map.get_mut(t) {
                    let (remap, insert): (Vec<_>, Vec<_>) = other_enc
                        .map
                        .par_iter()
                        .map(|(s, u)| {
                            if let Some(e) = enc.map.get(s) {
                                (Some((*u, *e)), None)
                            } else {
                                (None, Some((s.clone(), *u)))
                            }
                        })
                        .unzip();
                    let mut numbered_insert = Vec::new();
                    let mut new_remap = Vec::new();
                    for k in insert {
                        if let Some((s, u)) = k {
                            numbered_insert.push((s, c));
                            new_remap.push((u, c));
                            c += 1;
                        }
                    }
                    for (s, u) in numbered_insert {
                        enc.encode_new_arc_string(s.clone(), u);
                        if let Some(prefix_u) = &prefix_u {
                            self.belongs_prefix_map.insert(u, *prefix_u);
                            self.rev_iri_suffix_map.insert(u, s);
                        }
                    }
                    let remap: HashMap<_, _, BuildHasherDefault<NoHashHasher<u32>>> = remap
                        .into_iter()
                        .filter(|x| x.is_some())
                        .map(|x| x.unwrap())
                        .chain(new_remap.into_iter())
                        .collect();
                    let reenc = CatReEnc {
                        cat_map: Arc::new(remap),
                    };
                    other_map.insert(t.clone(), reenc);
                } else {
                    let mut remap = Vec::with_capacity(other_enc.map.len());
                    let mut new_enc = CatEncs::new_empty(matches!(t, CatType::Prefix(..)));
                    for (s, v) in other_enc.map.iter() {
                        remap.push((*v, c));
                        new_enc.encode_new_str(s, c);
                        if let Some(prefix_u) = &prefix_u {
                            self.belongs_prefix_map.insert(c, *prefix_u);
                            self.rev_iri_suffix_map.insert(c, s.clone());
                        }
                        c += 1;
                    }
                    self.cat_map.insert(t.clone(), new_enc);
                    let remap: HashMap<_, _, BuildHasherDefault<NoHashHasher<u32>>> =
                        remap.into_iter().collect();
                    other_map.insert(
                        t.clone(),
                        CatReEnc {
                            cat_map: Arc::new(remap),
                        },
                    );
                }
                self.set_height(c, t);
            }
            if !other_map.is_empty() {
                map.insert(c.uuid.clone(), other_map);
            }
        }
        map
    }
}

pub fn re_encode(
    encoded: Vec<CatTriples>,
    re_enc_map: HashMap<String, HashMap<CatType, CatReEnc>>,
) -> Vec<CatTriples> {
    let re_encoded: Vec<_> = encoded
        .into_par_iter()
        .map(|mut x| {
            let mut new_encoded = Vec::with_capacity(x.encoded_triples.len());
            for mut enc in x.encoded_triples {
                let subj_encs = if let Some(s_uuid) = &enc.subject_local_cat_uuid {
                    if let Some(s_map) = re_enc_map.get(s_uuid) {
                        if let Some(s) = &enc.subject {
                            s_map.get(s)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(subj_encs) = subj_encs {
                    let mut renc_col = subj_encs
                        .clone()
                        .re_encode_column(enc.df.column(SUBJECT_COL_NAME).unwrap().clone(), false)
                        .unwrap();
                    renc_col.rename(PlSmallStr::from_str(SUBJECT_COL_NAME));
                    enc.df.with_column(renc_col).unwrap();
                }
                let obj_encs = if let Some(o_uuid) = &enc.object_local_cat_uuid {
                    if let Some(o_map) = re_enc_map.get(o_uuid) {
                        if let Some(o) = &enc.object {
                            o_map.get(o)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(obj_encs) = obj_encs {
                    let mut renc_col = obj_encs
                        .clone()
                        .re_encode_column(enc.df.column(OBJECT_COL_NAME).unwrap().clone(), false)
                        .unwrap();
                    renc_col.rename(PlSmallStr::from_str(OBJECT_COL_NAME));
                    enc.df.with_column(renc_col).unwrap();
                }
                new_encoded.push(enc);
            }
            x.encoded_triples = new_encoded;
            x
        })
        .collect();
    re_encoded
}

pub fn reencode_solution_mappings(
    sm: EagerSolutionMappings,
    reencs: &HashMap<String, HashMap<BaseRDFNodeType, CatReEnc>>,
) -> EagerSolutionMappings {
    let EagerSolutionMappings {
        mappings,
        mut rdf_node_types,
    } = sm;
    let mut mappings = mappings.lazy();
    for (c, t) in &rdf_node_types {
        let mut reenc_exprs = vec![];
        for (bt, bs) in &t.map {
            if let BaseCatState::CategoricalNative(_, Some(local)) = bs {
                let local = local.read().unwrap();
                if let Some(rmap) = reencs.get(&local.uuid) {
                    let r = rmap.get(bt).unwrap();

                    let e = if t.is_multi() {
                        col(c).struct_().field_by_name(&bt.field_col_name())
                    } else {
                        col(c)
                    };
                    let r_cloned = r.clone();
                    reenc_exprs.push(e.map(
                        move |c| r_cloned.clone().re_encode_column(c, false),
                        |_, y| Ok(y.clone()),
                    ));
                }
            }
        }
        if !reenc_exprs.is_empty() {
            if t.is_multi() {
                mappings = mappings.with_column(col(c).struct_().with_fields(reenc_exprs).alias(c));
            } else {
                mappings = mappings.with_column(reenc_exprs.pop().unwrap().alias(c))
            }
        }
    }
    for v in rdf_node_types.values_mut() {
        for v2 in v.map.values_mut() {
            if matches!(v2, BaseCatState::CategoricalNative(..)) {
                *v2 = BaseCatState::CategoricalNative(false, None);
            }
        }
    }
    let sm = EagerSolutionMappings::new(mappings.collect().unwrap(), rdf_node_types);
    sm
}

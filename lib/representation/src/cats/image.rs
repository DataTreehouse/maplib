use super::{reencode_solution_mappings, CatEncs, CatType};
use super::{CatReEnc, Cats};
use crate::cats::LockedCats;
use crate::solution_mapping::{BaseCatState, EagerSolutionMappings};
use crate::{BaseRDFNodeType, RDFNodeState};
use nohash_hasher::NoHashHasher;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::BuildHasherDefault;
use std::ops::Deref;
use std::sync::Arc;

impl Cats {
    pub fn mappings_cat_image(&self, sms: &Vec<&EagerSolutionMappings>) -> Cats {
        let mut s: HashMap<_, HashSet<u32>> = HashMap::new();

        for sm in sms {
            let sm = *sm;
            let cols: Vec<_> = sm.rdf_node_types.keys().collect();
            for c in cols {
                let t = sm.rdf_node_types.get(c).unwrap();
                for (bt, bs) in &t.map {
                    if let BaseCatState::CategoricalNative(_, local) = bs {
                        let ser = if t.is_multi() {
                            sm.mappings
                                .column(c)
                                .unwrap()
                                .struct_()
                                .unwrap()
                                .field_by_name(&bt.field_col_name())
                                .unwrap()
                        } else {
                            sm.mappings
                                .column(c)
                                .unwrap()
                                .as_materialized_series()
                                .clone()
                        };
                        if !s.contains_key(bt) {
                            s.insert(bt.clone(), HashSet::new());
                        }
                        let local_read = local.as_ref().map(|x| x.read().unwrap());
                        let local_read_ref = local_read.as_ref().map(|x| x.deref());

                        let local_rev_map = if let Some(local) = local_read_ref {
                            Some(local.get_reverse_lookup(bt))
                        } else {
                            None
                        };
                        let ss = s.get_mut(bt).unwrap();
                        let new_ss = ser
                            .u32()
                            .unwrap()
                            .into_iter()
                            .filter(|x| x.is_some())
                            .map(|x| x.unwrap());
                        if let Some(local_rev_map) = local_rev_map {
                            ss.extend(new_ss.filter(|x| !local_rev_map.rev_map.contains_key(x)));
                        } else {
                            ss.extend(new_ss);
                        }
                    }
                }
            }
        }
        let cats = self.image(&s);
        cats
    }

    pub fn merge_solution_mappings_locals(
        &mut self,
        sms: &Vec<&EagerSolutionMappings>,
    ) -> HashMap<String, HashMap<BaseRDFNodeType, CatReEnc>> {
        let mut local_cats = vec![];

        for sm in sms {
            let cols: Vec<_> = sm.rdf_node_types.keys().collect();
            for c in cols {
                let t = sm.rdf_node_types.get(c).unwrap();
                for bs in t.map.values() {
                    if let BaseCatState::CategoricalNative(_, local) = bs {
                        if let Some(local) = local.as_ref() {
                            local_cats.push(local.clone());
                        }
                    }
                }
            }
        }
        let remap = self.merge(local_cats);
        let mut concat_reenc: HashMap<
            String,
            HashMap<BaseRDFNodeType, HashMap<u32, u32, BuildHasherDefault<NoHashHasher<u32>>>>,
        > = HashMap::new();
        for (uuid, reenc) in remap {
            for (ct, cat_reenc) in reenc {
                let bt = ct.as_base_rdf_node_type();
                let bt_map = if let Some(bt_map) = concat_reenc.get_mut(&uuid) {
                    bt_map
                } else {
                    concat_reenc.insert(uuid.clone(), HashMap::new());
                    concat_reenc.get_mut(&uuid).unwrap()
                };

                let e_reenc = if let Some(e_reenc) = bt_map.get_mut(&bt) {
                    e_reenc
                } else {
                    bt_map.insert(
                        bt.clone(),
                        HashMap::with_capacity_and_hasher(2, BuildHasherDefault::default()),
                    );
                    bt_map.get_mut(&bt).unwrap()
                };
                e_reenc.extend(cat_reenc.cat_map.iter());
            }
        }
        let mut concat_reenc_cats = HashMap::new();
        for (uuid, bt_map) in concat_reenc {
            let mut new_bt_map = HashMap::new();
            for (bt, map) in bt_map {
                new_bt_map.insert(
                    bt,
                    CatReEnc {
                        cat_map: Arc::new(map),
                    },
                );
            }
            concat_reenc_cats.insert(uuid, new_bt_map);
        }
        concat_reenc_cats
    }

    pub fn image(&self, s: &HashMap<BaseRDFNodeType, HashSet<u32>>) -> Cats {
        let mut encs = HashMap::new();
        for (t, set) in s {
            let ct = CatType::from_base_rdf_node_type(t);
            if let Some(enc) = self.cat_map.get(&ct) {
                if let Some(image_enc) = enc.image(set) {
                    encs.insert(ct, image_enc);
                }
            }
        }
        Cats::from_map(encs)
    }
}

impl CatEncs {
    pub fn image(&self, s: &HashSet<u32>) -> Option<CatEncs> {
        let new_map: BTreeMap<_, _> = s
            .par_iter()
            .map(|x| {
                if let Some(s) = self.reverse.get(x) {
                    Some((*x, s.clone()))
                } else {
                    None
                }
            })
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .map(|(x, y)| (y, x))
            .collect();
        let new_rev_map: HashMap<_, _, BuildHasherDefault<NoHashHasher<u32>>> =
            new_map.iter().map(|(x, y)| (*y, x.clone())).collect();

        if new_map.len() > 0 {
            Some(CatEncs {
                map: new_map,
                rev_map: new_rev_map,
            })
        } else {
            None
        }
    }
}

pub fn new_solution_mapping_cats(
    sms: Vec<EagerSolutionMappings>,
    global_cats: &Cats,
) -> (Vec<EagerSolutionMappings>, Cats) {
    let smsref: Vec<_> = sms.iter().collect();
    let mut cats = global_cats.mappings_cat_image(&smsref);
    let reenc = cats.merge_solution_mappings_locals(&smsref);
    let new_sms: Vec<_> = sms
        .into_iter()
        .map(|sm| reencode_solution_mappings(sm, &reenc))
        .collect();
    (new_sms, cats)
}

pub fn set_global_cats_as_local(
    rdf_node_types: &mut HashMap<String, RDFNodeState>,
    cats: LockedCats,
) {
    for (_, s) in rdf_node_types {
        for v in s.map.values_mut() {
            if matches!(v, BaseCatState::CategoricalNative(_, None)) {
                *v = BaseCatState::CategoricalNative(false, Some(cats.clone()));
            } else if matches!(v, BaseCatState::CategoricalNative(_, Some(..))) {
                panic!("Should never be called when locals exist")
            }
        }
    }
}

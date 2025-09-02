use super::{
    rdf_split_iri_str, split_iri_series, CatEncs, CatType, Cats, EncodedTriples,
    OBJECT_PREFIX_COL_NAME, SUBJECT_PREFIX_COL_NAME,
};
use crate::solution_mapping::{BaseCatState, EagerSolutionMappings};
use crate::{BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use oxrdf::vocab::xsd;
use oxrdf::NamedNode;
use polars::frame::DataFrame;
use polars::prelude::{col, lit, IntoLazy, Series};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::collections::HashMap;
use std::sync::Arc;

impl CatEncs {
    pub fn new_empty() -> CatEncs {
        CatEncs {
            map: Default::default(),
            rev_map: Default::default(),
        }
    }

    pub fn new_singular(value: &str, u: u32) -> CatEncs {
        let mut sing = Self::new_empty();
        sing.map.insert(value.to_string(), u);
        sing.rev_map.insert(u, value.to_string());
        sing
    }

    pub fn maybe_encode_str(&self, s: &str) -> Option<&u32> {
        self.map.get(s)
    }

    pub fn encode_new_str(&mut self, s: &str, u: u32) {
        self.encode_new_string(s.to_string(), u);
    }

    pub fn encode_new_string(&mut self, s: String, u: u32) {
        self.map.insert(s.clone(), u);
        self.rev_map.insert(u, s);
    }

    pub fn height(&self) -> u32 {
        self.map.len() as u32
    }
}

impl Cats {
    pub fn encode_solution_mappings(
        &self,
        sm: EagerSolutionMappings,
        include_cat_type_col: Option<HashMap<String, String>>,
    ) -> (EagerSolutionMappings, HashMap<String, HashMap<u32, String>>) {
        let EagerSolutionMappings {
            mappings,
            mut rdf_node_types,
        } = sm;
        let mut to_encode = vec![];
        let mut to_add_prefix_col = vec![];
        for (c, s) in &rdf_node_types {
            for (t, bs) in &s.map {
                if t.is_iri() || t.is_blank_node() || t.is_lit_type(xsd::STRING) {
                    if matches!(bs, BaseCatState::String) {
                        let ser = if s.is_multi() {
                            mappings
                                .column(c)
                                .unwrap()
                                .as_materialized_series()
                                .struct_()
                                .unwrap()
                                .field_by_name(&t.field_col_name())
                                .unwrap()
                        } else {
                            mappings
                                .column(c)
                                .unwrap()
                                .as_materialized_series().clone()
                        };
                        to_encode.push((c.clone(), t.clone(), ser));
                    } else if let BaseCatState::CategoricalNative(_,local) = bs {
                        if let Some(map) = &include_cat_type_col {
                            if map.contains_key(c) {
                                let ser = if s.is_multi() {
                                    mappings
                                        .column(c)
                                        .unwrap()
                                        .as_materialized_series()
                                        .struct_()
                                        .unwrap()
                                        .field_by_name(&t.field_col_name())
                                        .unwrap()
                                } else {
                                    mappings
                                        .column(c)
                                        .unwrap()
                                        .as_materialized_series().clone()
                                };
                                to_add_prefix_col.push((c.clone(), local.clone(), ser));
                            }
                        }
                    }
                }
            }
        }
        let encoded: Vec<_> = to_encode
            .into_par_iter()
            .map(|(c, t, ser)| {
                let t_col = if let Some(map) = &include_cat_type_col {
                    map.contains_key(&c)
                } else {
                    false
                };
                let (enc, local, cat_col) = self.encode_series(&ser, &t, t_col);
                (c, t, enc, local, cat_col)
            })
            .collect();
        let mut prefix_maps = HashMap::new();
        let mut mappings = mappings.lazy();
        let prefix_cols:Vec<_> = to_add_prefix_col.into_par_iter().map(|(c,local,ser)| {
            let (prefix_ser, maps) = self.get_prefix_column(ser, local);
            let maps: HashMap<_, _> = maps.into_iter().map(|(u, ct)| {
                if let CatType::Prefix(p) = ct {
                    Some((u, p.as_str().to_string()))
                } else {
                    None
                }
            }).filter(|x|x.is_some()).map(|x|x.unwrap()).collect();
            let name = include_cat_type_col.as_ref().unwrap().get(&c).unwrap();
            (c, prefix_ser, maps, name)
        }).collect();

        for (c, prefix_ser, maps, name) in prefix_cols {
            prefix_maps.insert(c, maps);
            mappings = mappings.with_column(lit(prefix_ser).explode().alias(name));
        }
        for (c, t, enc, local, cat_col) in encoded {
            let s = rdf_node_types.get_mut(&c).unwrap();
            if s.is_multi() {
                mappings =
                    mappings.with_column(col(&c).struct_().with_fields(vec![lit(enc).explode()]));
            } else {
                mappings = mappings.with_column(lit(enc).explode());
            }
            s.map.insert(
                t,
                BaseCatState::CategoricalNative(false, local.map(|x| Arc::new(x))),
            );
            if let Some((s, m)) = cat_col {
                let name = include_cat_type_col.as_ref().unwrap().get(&c).unwrap();
                prefix_maps.insert(c, m);
                mappings = mappings.with_column(lit(s).explode().alias(name));
            }
        }
        (
            EagerSolutionMappings::new(mappings.collect().unwrap(), rdf_node_types),
            prefix_maps,
        )
    }

    pub fn encode_series(
        &self,
        series: &Series,
        t: &BaseRDFNodeType,
        include_cat_type: bool,
    ) -> (Series, Option<Cats>, Option<(Series, HashMap<u32, String>)>) {
        let original_name = series.name().clone();
        let mut use_height = match t {
            BaseRDFNodeType::IRI => self.iri_height,
            BaseRDFNodeType::BlankNode => self.blank_height,
            BaseRDFNodeType::Literal(l) => self.literal_height_map.get(l).map(|x| *x).unwrap_or(0),
            BaseRDFNodeType::None => {
                unreachable!("Should never happen")
            }
        };
        let (mut ser, c, ct) = if t.is_iri() {
            let (pre, suf, map) = split_iri_series(series);
            let mut enc_map = HashMap::new();
            let mut new_enc_map = HashMap::new();
            let mut rev_map = HashMap::new();
            for (k, u) in map {
                let enc = CatEncs::new_empty();
                new_enc_map.insert(u, enc);
                if let Some(enc) = self
                    .cat_map
                    .get(&CatType::Prefix(NamedNode::new_unchecked(&k)))
                {
                    enc_map.insert(u, enc);
                }
                rev_map.insert(u, k);
            }
            let cat_type_series = if include_cat_type {
                Some(Series::from_iter(pre.iter().map(|x| x.clone())))
            } else {
                None
            };

            let to_iter: Vec<_> = pre.into_iter().zip(suf).collect();
            let encoded_global: Vec<_> = to_iter
                .into_par_iter()
                .map(|x| {
                    if let (Some(u), Some(s)) = x {
                        if let Some(enc) = enc_map.get(&u) {
                            if let Some(u) = enc.maybe_encode_str(s).map(|x| *x) {
                                (None, Some(u))
                            } else {
                                (Some((u,s)),None)
                            }
                        } else {
                            (Some((u, s)), None)
                        }
                    } else {
                        (None, None)
                    }
                })
                .collect();

            let mut encoded_global_local = Vec::with_capacity(encoded_global.len());
            for (unencoded, encoded) in encoded_global {
                let encoded = if let Some((u, s)) = unencoded {
                    let enc = new_enc_map.get_mut(&u).unwrap();
                    if let Some(su) = enc.maybe_encode_str(s) {
                        Some(*su)
                    } else {
                        use_height += 1;
                        let su = use_height;
                        enc.encode_new_str(s, su);
                        Some(su)
                    }
                } else {
                    encoded
                };
                encoded_global_local.push(encoded);
            }
            let ser = Series::from_iter(encoded_global_local);
            let cats = if !new_enc_map.is_empty() {
                let mut keep_new_enc_map = HashMap::new();
                for (u, enc) in new_enc_map {
                    if !enc.map.is_empty() {
                        keep_new_enc_map.insert(
                            CatType::Prefix(NamedNode::new_unchecked(rev_map.get(&u).unwrap())),
                            enc,
                        );
                    }
                }
                let cats = Cats::from_map(keep_new_enc_map);
                Some(cats)
            } else {
                None
            };
            let cat_type_things = if let Some(cat_type_series) = cat_type_series {
                Some((cat_type_series, rev_map))
            } else {
                None
            };
            (ser, cats, cat_type_things)
        } else {
            let enc = self.get_encs(t).pop();
            let mut new_enc = CatEncs::new_empty();
            let strch = series.str().unwrap();
            let encoded_global: Vec<_> = strch
                .par_iter()
                .map(|x| {
                    if let Some(x) = x {
                        if let Some((_, enc)) = enc {
                            if let Some(su) = enc.maybe_encode_str(x).map(|x| *x) {
                                (None, Some(su))
                            } else {
                                (Some(x), None)
                            }
                        } else {
                            (Some(x), None)
                        }
                    } else {
                        (None, None)
                    }
                })
                .collect();

            let mut encoded_global_local = Vec::with_capacity(encoded_global.len());
            for (unencoded, encoded) in encoded_global {
                let encoded = if let Some(s) = unencoded {
                    if let Some(u) = new_enc.maybe_encode_str(s) {
                        Some(*u)
                    } else {
                        use_height += 1;
                        let su = use_height;
                        new_enc.encode_new_str(s, su);
                        Some(su)
                    }
                } else {
                    encoded
                };
                encoded_global_local.push(encoded);
            }
            let local = if !new_enc.map.is_empty() {
                let cat_type = if let BaseRDFNodeType::Literal(nn) = t {
                    CatType::Literal(nn.clone())
                } else {
                    CatType::Blank
                };
                let mut map = HashMap::new();
                map.insert(cat_type, new_enc);
                Some(Cats::from_map(map))
            } else {
                None
            };
            let ser = Series::from_iter(encoded_global_local);
            (ser, local, None)
        };
        ser.rename(original_name);
        (ser, c, ct)
    }

    pub fn encode_blanks(&self, blanks: &[&str]) -> Vec<Option<u32>> {
        if let Some(encs) = self.cat_map.get(&CatType::Blank) {
            let u32s: Vec<_> = blanks
                .par_iter()
                .map(|x| encs.maybe_encode_str(*x).map(|x| *x))
                .collect();
            u32s
        } else {
            vec![None].repeat(blanks.len())
        }
    }

    pub fn encode_literals(&self, literals: &[&str], data_type: NamedNode) -> Vec<Option<u32>> {
        if let Some(encs) = self.cat_map.get(&CatType::Literal(data_type)) {
            let u32s: Vec<_> = literals
                .par_iter()
                .map(|x| encs.maybe_encode_str(*x).map(|x| *x))
                .collect();
            u32s
        } else {
            vec![None].repeat(literals.len())
        }
    }

    pub fn encode_iri_slice(&self, iris: &[&str]) -> Vec<Option<u32>> {
        let split: Vec<_> = iris.iter().map(|x| rdf_split_iri_str(x)).collect();

        let mut u32s = vec![];
        for (p, s) in split {
            if let Some(encs) = self
                .cat_map
                .get(&CatType::Prefix(NamedNode::new_unchecked(p)))
            {
                if let Some(suf) = encs.maybe_encode_str(s) {
                    u32s.push(Some(*suf));
                } else {
                    u32s.push(None)
                }
            } else {
                u32s.push(None)
            }
        }
        u32s
    }

    pub fn get_prefix_column(
        &self,
        ser: Series,
        local_cats: Option<Arc<Cats>>,
    ) -> (Series, HashMap<u32, CatType>) {
        let mut filtered_cat_types = self.filter_cat_types(&BaseRDFNodeType::IRI);
        let mut encs = Vec::with_capacity(filtered_cat_types.len());
        for c in &filtered_cat_types {
            let enc = self.cat_map.get(*c).unwrap();
            encs.push(enc);
        }
        let local_cats = local_cats.as_ref().map(|x| x.as_ref());
        if let Some(local_cats) = local_cats {
            let local_filtered_cat_types =
                local_cats.filter_cat_types(&BaseRDFNodeType::IRI);
            for c in &local_filtered_cat_types {
                let enc = local_cats.cat_map.get(*c).unwrap();
                encs.push(enc);
            }
            filtered_cat_types.extend(local_filtered_cat_types);
        }
        let uch = ser.u32().unwrap();
        let mut prefixes = Vec::with_capacity(ser.len());
        for u in uch {
            let p = if let Some(u) = u {
                let mut p = None;
                for (i, enc) in encs.iter().enumerate() {
                    if enc.rev_map.contains_key(&u) {
                        p = Some(i as u32);
                        break;
                    }
                }
                if p.is_none() {
                    panic!("Should never happen: {u}")
                }
                p
            } else {
                None
            };
            prefixes.push(p);
        }
        let ser = Series::from_iter(prefixes);
        let map: HashMap<u32, CatType> = filtered_cat_types
            .iter()
            .enumerate()
            .map(|(i, c)| (i as u32, (*c).clone()))
            .collect();
        (ser, map)
    }

    pub fn encode_iri_or_local_cat(&self, iri: &str) -> (u32, RDFNodeState) {
        let mut v = self.encode_iri_slice(&[iri]);
        if let Some(u) = v.pop().unwrap() {
            (
                u,
                RDFNodeState::from_bases(
                    BaseRDFNodeType::IRI,
                    BaseCatState::CategoricalNative(false, None),
                ),
            )
        } else {
            let (u, l) = Cats::new_singular_iri(iri, self.iri_height);
            (
                u,
                RDFNodeState::from_bases(
                    BaseRDFNodeType::IRI,
                    BaseCatState::CategoricalNative(false, Some(Arc::new(l))),
                ),
            )
        }
    }

    pub fn encode_blank_or_local_cat(&self, blank: &str) -> (u32, RDFNodeState) {
        let mut v = self.encode_blanks(&[blank]);
        if let Some(u) = v.pop().unwrap() {
            (
                u,
                RDFNodeState::from_bases(
                    BaseRDFNodeType::BlankNode,
                    BaseCatState::CategoricalNative(false, None),
                ),
            )
        } else {
            let (u, l) = Cats::new_singular_blank(blank, self.blank_height);
            (
                u,
                RDFNodeState::from_bases(
                    BaseRDFNodeType::BlankNode,
                    BaseCatState::CategoricalNative(false, Some(Arc::new(l))),
                ),
            )
        }
    }


    fn filter_cat_types<'a>(&'a self, bt: &BaseRDFNodeType) -> Vec<&'a CatType> {
        let mut keep_types = vec![];
        for t in self.cat_map.keys() {
            let keep = match t {
                CatType::Prefix(_) => {
                    matches!(bt, BaseRDFNodeType::IRI)
                }
                CatType::Blank => {
                    matches!(bt, BaseRDFNodeType::BlankNode)
                }
                CatType::Literal(nn) => {
                    if let BaseRDFNodeType::Literal(l) = bt {
                        nn == l
                    } else {
                        false
                    }
                }
            };
            if keep {
                keep_types.push(t);
            }
        }
        keep_types
    }
}


pub fn encode_triples(
    df: DataFrame,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
    subject_cat_state: BaseCatState,
    object_cat_state: BaseCatState,
    global_cats: &Cats,
) -> (Vec<Arc<Cats>>, Vec<EncodedTriples>) {
    let mut map = HashMap::new();
    map.insert(
        SUBJECT_COL_NAME.to_string(),
        RDFNodeState::from_bases(subject_type.clone(), subject_cat_state),
    );
    map.insert(
        OBJECT_COL_NAME.to_string(),
        RDFNodeState::from_bases(object_type.clone(), object_cat_state),
    );
    let mut prefix_name_map = HashMap::new();
    prefix_name_map.insert(
        SUBJECT_COL_NAME.to_string(),
        SUBJECT_PREFIX_COL_NAME.to_string(),
    );
    prefix_name_map.insert(
        OBJECT_COL_NAME.to_string(),
        OBJECT_PREFIX_COL_NAME.to_string(),
    );
    let (mut sm, prefix_maps) = global_cats
        .encode_solution_mappings(EagerSolutionMappings::new(df, map), Some(prefix_name_map));

    let mut subject_state = sm.rdf_node_types.remove(SUBJECT_COL_NAME).unwrap();
    let mut object_state = sm.rdf_node_types.remove(OBJECT_COL_NAME).unwrap();
    let (subject_type, subject_cat_state) = subject_state.map.drain().next().unwrap();
    let (object_type, object_cat_state) = object_state.map.drain().next().unwrap();

    let subject_local_cat_uuid = subject_cat_state.get_local_cats().map(|x| x.uuid.clone());
    let object_local_cat_uuid = object_cat_state.get_local_cats().map(|x| x.uuid.clone());

    let EagerSolutionMappings { mappings, .. } = sm;
    let mut partition_by = vec![];
    if mappings.column(SUBJECT_PREFIX_COL_NAME).is_ok() {
        partition_by.push(SUBJECT_PREFIX_COL_NAME)
    }
    if mappings.column(OBJECT_PREFIX_COL_NAME).is_ok() {
        partition_by.push(OBJECT_PREFIX_COL_NAME);
    }
    let out = if !partition_by.is_empty() {
        let dfs = mappings.partition_by(partition_by, true).unwrap();
        let out: Vec<_> = dfs
            .into_par_iter()
            .map(|mut df| {
                let subject_out =
                    if let Some(subject_prefix_map) = prefix_maps.get(SUBJECT_COL_NAME) {
                        let u = df
                            .drop_in_place(SUBJECT_PREFIX_COL_NAME)
                            .unwrap()
                            .as_materialized_series_maintain_scalar()
                            .u32()
                            .unwrap()
                            .first()
                            .unwrap();
                        Some(subject_prefix_map.get(&u).unwrap().clone())
                    } else {
                        None
                    };
                let object_out = if let Some(object_prefix_map) = prefix_maps.get(OBJECT_COL_NAME) {
                    let u = df
                        .drop_in_place(OBJECT_PREFIX_COL_NAME)
                        .unwrap()
                        .as_materialized_series_maintain_scalar()
                        .u32()
                        .unwrap()
                        .first()
                        .unwrap();
                    Some(object_prefix_map.get(&u).unwrap().clone())
                } else {
                    None
                };
                (df, subject_out, object_out)
            })
            .collect();
        out
    } else {
        vec![(mappings, None, None)]
    };
    let mut new_out = vec![];
    for (df, subject, object) in out {
        let subject = if matches!(subject_cat_state, BaseCatState::CategoricalNative(..)) {
            if let Some(subject) = subject {
                Some(CatType::Prefix(NamedNode::new_unchecked(subject)))
            } else if subject_type.is_blank_node() {
                Some(CatType::Blank)
            } else {
                None
            }
        } else {
            None
        };
        let object = if matches!(object_cat_state, BaseCatState::CategoricalNative(..)) {
            if let Some(object) = object {
                Some(CatType::Prefix(NamedNode::new_unchecked(object)))
            } else if object_type.is_blank_node() {
                Some(CatType::Blank)
            } else if let BaseRDFNodeType::Literal(nn) = &object_type {
                Some(CatType::Literal(nn.clone()))
            } else {
                None
            }
        } else {
            None
        };
        new_out.push(EncodedTriples {
            df,
            subject,
            subject_local_cat_uuid: subject_local_cat_uuid.clone(),
            object,
            object_local_cat_uuid: object_local_cat_uuid.clone(),
        });
    }
    let mut cats = vec![];
    if let BaseCatState::CategoricalNative(_, Some(c)) = subject_cat_state {
        cats.push(c);
    }
    if let BaseCatState::CategoricalNative(_, Some(c)) = object_cat_state {
        cats.push(c);
    }
    (cats, new_out)
}

use super::{
    CatEncs, CatType, Cats, EncodedTriples
};
use crate::cats::LockedCats;
use crate::solution_mapping::{BaseCatState, EagerSolutionMappings};
use crate::{BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use oxrdf::NamedNode;
use polars::frame::DataFrame;
use polars::prelude::{col, lit, IntoLazy, Series};
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use rayon::iter::ParallelIterator;

impl CatEncs {
    pub fn new_empty() -> CatEncs {
        let rev_map = HashMap::with_capacity_and_hasher(2, BuildHasherDefault::default());
        CatEncs {
            map: Default::default(),
            rev_map,
        }
    }

    pub fn contains_key(&self, s: &str) -> bool {
        let s = s.to_string();
        self.map.contains_key(&s)
    }

    pub fn new_singular(value: &str, u: u32) -> CatEncs {
        let mut sing = Self::new_empty();
        let s = Arc::new(value.to_string());
        sing.map.insert(s.clone(), u);
        sing.rev_map.insert(u, s);
        sing
    }

    pub fn maybe_encode_str(&self, s: &str) -> Option<&u32> {
        let s = Arc::new(s.to_string());
        self.map.get(&s)
    }

    pub fn encode_new_str(&mut self, s: &str, u: u32) {
        self.encode_new_string(s.to_string(), u);
    }

    pub fn encode_new_string(&mut self, s: String, u: u32) {
        let s = Arc::new(s.clone());
        self.map.insert(s.clone(), u);
        self.rev_map.insert(u, s);
    }

    pub fn encode_new_arc_string(&mut self, s: Arc<String>, u: u32) {
        self.map.insert(s.clone(), u);
        self.rev_map.insert(u, s);
    }

    pub fn height(&self) -> u32 {
        self.map.len() as u32
    }
}

impl Cats {
    pub fn encode_solution_mappings(&self, sm: EagerSolutionMappings) -> EagerSolutionMappings {
        let EagerSolutionMappings {
            mappings,
            mut rdf_node_types,
        } = sm;
        let mut to_encode = vec![];
        for (c, s) in &rdf_node_types {
            for (t, bs) in &s.map {
                if t.stored_cat() {
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
                            mappings.column(c).unwrap().as_materialized_series().clone()
                        };
                        to_encode.push((c.clone(), t.clone(), ser));
                    }
                }
            }
        }
        let encoded: Vec<_> = to_encode
            .into_iter()
            .map(|(c, t, ser)| {
                let (enc, local) = self.encode_series(&ser, &t);
                (c, t, enc, local)
            })
            .collect();
        let mut mappings = mappings.lazy();

        for (c, t, enc, local) in encoded {
            let s = rdf_node_types.get_mut(&c).unwrap();
            if s.is_multi() {
                mappings =
                    mappings.with_column(col(&c).struct_().with_fields(vec![lit(enc).explode()]));
            } else {
                mappings = mappings.with_column(lit(enc).explode());
            }
            s.map.insert(
                t,
                BaseCatState::CategoricalNative(false, local.map(|x| LockedCats::new(x))),
            );
        }
        EagerSolutionMappings::new(mappings.collect().unwrap(), rdf_node_types)
    }

    pub fn encode_series(&self, series: &Series, t: &BaseRDFNodeType) -> (Series, Option<Cats>) {
        let original_name = series.name().clone();
        let mut use_height = match t {
            BaseRDFNodeType::IRI => self.iri_counter,
            BaseRDFNodeType::BlankNode => self.blank_counter,
            BaseRDFNodeType::Literal(l) => self.literal_counter_map.get(l).map(|x| *x).unwrap_or(0),
            BaseRDFNodeType::None => {
                unreachable!("Should never happen")
            }
        };

        let enc = self.get_encs(t).pop();
        let mut new_enc = CatEncs::new_empty();
        let strch = series.str().unwrap();
        let encoded_global: Vec<_> = strch
            .iter()
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
            let cat_type = CatType::from_base_rdf_node_type(t);
            let mut map = HashMap::new();
            map.insert(cat_type, new_enc);
            Some(Cats::from_map(map))
        } else {
            None
        };
        let mut ser = Series::from_iter(encoded_global_local);
        ser.rename(original_name);
        (ser, local)
    }

    pub fn encode_blanks(&self, blanks: &[&str]) -> Vec<Option<u32>> {
        if let Some(encs) = self.cat_map.get(&CatType::Blank) {
            let u32s: Vec<_> = blanks
                .iter()
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
                .iter()
                .map(|x| encs.maybe_encode_str(*x).map(|x| *x))
                .collect();
            u32s
        } else {
            vec![None].repeat(literals.len())
        }
    }

    pub fn encode_iri_slice(&self, iris: &[&str]) -> Vec<Option<u32>> {
        let mut u32s = vec![];
        let encs = self.cat_map.get(&CatType::IRI);
        for s in iris {
            if let Some(encs) = encs {
                if let Some(u) = encs.maybe_encode_str(s) {
                    u32s.push(Some(*u));
                } else {
                    u32s.push(None)
                }
            } else {
                u32s.push(None)
            }
        }
        u32s
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
            let (u, l) = Cats::new_singular_iri(iri, self.iri_counter);
            (
                u,
                RDFNodeState::from_bases(
                    BaseRDFNodeType::IRI,
                    BaseCatState::CategoricalNative(false, Some(LockedCats::new(l))),
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
            let (u, l) = Cats::new_singular_blank(blank, self.blank_counter);
            (
                u,
                RDFNodeState::from_bases(
                    BaseRDFNodeType::BlankNode,
                    BaseCatState::CategoricalNative(false, Some(LockedCats::new(l))),
                ),
            )
        }
    }
}

pub fn encode_triples(
    df: DataFrame,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
    subject_cat_state: BaseCatState,
    object_cat_state: BaseCatState,
    global_cats: &Cats,
) -> (Vec<LockedCats>, EncodedTriples) {
    let mut map = HashMap::new();
    map.insert(
        SUBJECT_COL_NAME.to_string(),
        RDFNodeState::from_bases(subject_type.clone(), subject_cat_state),
    );
    map.insert(
        OBJECT_COL_NAME.to_string(),
        RDFNodeState::from_bases(object_type.clone(), object_cat_state),
    );
    let mut sm = global_cats.encode_solution_mappings(EagerSolutionMappings::new(df, map));

    let mut subject_state = sm.rdf_node_types.remove(SUBJECT_COL_NAME).unwrap();
    let mut object_state = sm.rdf_node_types.remove(OBJECT_COL_NAME).unwrap();
    let (subject_type, subject_cat_state) = subject_state.map.drain().next().unwrap();
    let (object_type, object_cat_state) = object_state.map.drain().next().unwrap();

    let subject_local_cat_uuid = subject_cat_state.get_local_cats().map(|x| {
        let x = x.read().unwrap();
        x.uuid.clone()
    });
    let object_local_cat_uuid = object_cat_state.get_local_cats().map(|x| {
        let x = x.read().unwrap();
        x.uuid.clone()
    });

    let subject = if matches!(subject_cat_state, BaseCatState::CategoricalNative(..)) {
        Some(CatType::from_base_rdf_node_type(&subject_type))
    } else {
        None
    };
    let object = if matches!(object_cat_state, BaseCatState::CategoricalNative(..)) {
        Some(CatType::from_base_rdf_node_type(&object_type))
    } else {
        None
    };

    let enc_trip = EncodedTriples {
        df:sm.mappings,
        subject,
        subject_local_cat_uuid: subject_local_cat_uuid.clone(),
        object,
        object_local_cat_uuid: object_local_cat_uuid.clone(),
    };
    let mut cats = vec![];
    if let BaseCatState::CategoricalNative(_, Some(c)) = subject_cat_state {
        cats.push(c);
    }
    if let BaseCatState::CategoricalNative(_, Some(c)) = object_cat_state {
        cats.push(c);
    }
    (cats, enc_trip)
}

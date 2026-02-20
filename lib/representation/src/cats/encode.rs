use super::{CatEncs, CatType, Cats, EncodedTriples};
use crate::cats::maps::in_memory::CatMapsInMemory;
use crate::cats::maps::CatMaps;
use crate::cats::LockedCats;
use crate::solution_mapping::{BaseCatState, EagerSolutionMappings};
use crate::{BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use oxrdf::NamedNode;
use polars::frame::DataFrame;
use polars::prelude::{col, lit, IntoLazy, Series};
use std::collections::HashMap;
use std::path::Path;

impl CatEncs {
    pub fn new_empty(path: Option<&Path>, bt: &BaseRDFNodeType) -> CatEncs {
        CatEncs {
            maps: CatMaps::new_empty(path, bt),
        }
    }

    pub fn new_local_singular(value: &str, u: u32, bt: &BaseRDFNodeType) -> CatEncs {
        let maps = CatMapsInMemory::new_singular(value, u, bt);
        CatEncs {
            maps: CatMaps::InMemory(maps),
        }
    }

    pub fn maybe_encode_strs(&self, s: &[Option<&str>]) -> Vec<Option<u32>> {
        self.maps.maybe_encode_strs(s)
    }

    pub fn counter(&self) -> u32 {
        self.maps.counter()
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
        // Local cat encs are always in memory
        let mut new_enc = CatEncs::new_empty(None, t);
        let maybe_encoded_global = if let Some((_, enc)) = enc {
            let strch = series.str().unwrap();
            let strvec: Vec<_> = strch.iter().collect();
            Some(enc.maybe_encode_strs(&strvec))
        } else {
            None
        };
        let mut encoded_global_local = Vec::with_capacity(series.len());

        if let Some(enc) = maybe_encoded_global {
            for (glob, s) in enc.into_iter().zip(series.str().unwrap().iter()) {
                if let Some(glob) = glob {
                    encoded_global_local.push(Some(glob));
                } else {
                    if let Some(s) = s {
                        if let Some(u) = new_enc.maps.maybe_encode_in_memory_str(s) {
                            encoded_global_local.push(Some(u))
                        } else {
                            use_height += 1;
                            let su = use_height;
                            new_enc.maps.encode_new_in_memory_string(s.to_string(), su);
                            encoded_global_local.push(Some(su))
                        }
                    } else {
                        encoded_global_local.push(None);
                    }
                }
            }
        } else {
            for s in series.str().unwrap().iter() {
                if let Some(s) = s {
                    if let Some(u) = new_enc.maps.maybe_encode_in_memory_str(s) {
                        encoded_global_local.push(Some(u))
                    } else {
                        use_height += 1;
                        let su = use_height;
                        new_enc.maps.encode_new_in_memory_string(s.to_string(), su);
                        encoded_global_local.push(Some(su))
                    }
                } else {
                    encoded_global_local.push(None);
                }
            }
        }

        let local = if !new_enc.maps.is_empty() {
            let cat_type = CatType::from_base_rdf_node_type(t);
            let mut map = HashMap::new();
            map.insert(cat_type, new_enc);
            Some(Cats::from_map(map, None))
        } else {
            None
        };
        let mut ser = Series::from_iter(encoded_global_local);
        ser.rename(original_name);
        (ser, local)
    }

    pub fn maybe_encode_blanks(&self, blanks: &[Option<&str>]) -> Vec<Option<u32>> {
        if let Some(encs) = self.cat_map.get(&CatType::Blank) {
            encs.maybe_encode_strs(blanks)
        } else {
            vec![None].repeat(blanks.len())
        }
    }

    pub fn maybe_encode_literals(
        &self,
        literals: &[Option<&str>],
        data_type: NamedNode,
    ) -> Vec<Option<u32>> {
        if let Some(encs) = self.cat_map.get(&CatType::Literal(data_type)) {
            encs.maybe_encode_strs(literals)
        } else {
            vec![None].repeat(literals.len())
        }
    }

    pub fn maybe_encode_iri_slice(&self, iris: &[Option<&str>]) -> Vec<Option<u32>> {
        let encs = self.cat_map.get(&CatType::IRI);
        if let Some(encs) = encs {
            encs.maybe_encode_strs(iris)
        } else {
            vec![None].repeat(iris.len())
        }
    }

    pub fn encode_iri_or_local_cat(&self, iri: &str) -> (u32, RDFNodeState) {
        let mut v = self.maybe_encode_iri_slice(&[Some(iri)]);
        if let Some(u) = v.pop().unwrap() {
            (
                u,
                RDFNodeState::from_bases(
                    BaseRDFNodeType::IRI,
                    BaseCatState::CategoricalNative(false, None),
                ),
            )
        } else {
            // Locals always in memory
            let (u, l) = Cats::new_local_singular_iri(iri, self.iri_counter);
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
        let mut v = self.maybe_encode_blanks(&[Some(blank)]);
        if let Some(u) = v.pop().unwrap() {
            (
                u,
                RDFNodeState::from_bases(
                    BaseRDFNodeType::BlankNode,
                    BaseCatState::CategoricalNative(false, None),
                ),
            )
        } else {
            let (u, l) = Cats::new_local_singular_blank(blank, self.blank_counter);
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
        df: sm.mappings,
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

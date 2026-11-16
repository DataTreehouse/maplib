use super::{CatEncs, CatType, Cats, EncodedTriples};
use crate::cats::maps::CatMaps;
use crate::cats::LockedCats;
use crate::solution_mapping::{BaseCatState, EagerSolutionMappings};
use crate::{
    BaseRDFNodeType, RDFNodeState, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD,
    OBJECT_COL_NAME, SUBJECT_COL_NAME,
};
use crate::vectors::{is_vector_literal_type, lexical_series_to_stored};
use in_memory::CatMapsInMemory;
use oxrdf::NamedNode;
use polars::frame::DataFrame;
use polars::prelude::{
    as_struct, col, lit, ExplodeOptions, IntoColumn, IntoLazy, NamedFrom, PlSmallStr, Series,
};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use utils::polars::collect_sized;

impl CatEncs {
    pub fn new_empty(path: Option<&Path>, bt: &BaseRDFNodeType) -> CatEncs {
        CatEncs {
            maps: CatMaps::new_empty(path, bt),
        }
    }

    pub fn new_local_singular(value: &str, u: u32, bt: &BaseRDFNodeType) -> CatEncs {
        let maps = CatMapsInMemory::new_singular_kind(
            value,
            u,
            bt.is_iri(),
            crate::vectors::is_vector_literal_type(bt),
        );
        CatEncs {
            maps: CatMaps::in_memory(maps),
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
                if t.stored_cat() && matches!(bs, BaseCatState::String) {
                    let ser = if t.is_lang_string() || !s.is_multi() {
                        mappings.column(c).unwrap().as_materialized_series().clone()
                    } else {
                        mappings
                            .column(c)
                            .unwrap()
                            .as_materialized_series()
                            .struct_()
                            .unwrap()
                            .field_by_name(&t.field_col_name())
                            .unwrap()
                    };
                    to_encode.push((c.clone(), t.clone(), ser));
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
        let height = mappings.height();
        let mut mappings = mappings.lazy();

        for (c, t, enc, local) in encoded {
            let s = rdf_node_types.get_mut(&c).unwrap();
            if t.is_lang_string() {
                let value_series = enc
                    .struct_()
                    .unwrap()
                    .field_by_name(LANG_STRING_VALUE_FIELD)
                    .unwrap();
                let lang_series = enc
                    .struct_()
                    .unwrap()
                    .field_by_name(LANG_STRING_LANG_FIELD)
                    .unwrap();
                mappings = mappings.with_column(
                    col(&c)
                        .struct_()
                        .with_fields(vec![lit(value_series), lit(lang_series)]),
                );
            } else if s.is_multi() {
                mappings = mappings.with_column(col(&c).struct_().with_fields(vec![lit(enc)]));
            } else {
                mappings = mappings.with_column(lit(enc).explode(ExplodeOptions {
                    empty_as_null: false,
                    keep_nulls: true,
                }));
            }
            s.map.insert(
                t,
                BaseCatState::CategoricalNative(local.map(LockedCats::new)),
            );
        }
        EagerSolutionMappings::new(collect_sized(mappings, height).unwrap(), rdf_node_types)
    }

    pub fn encode_series(&self, series: &Series, t: &BaseRDFNodeType) -> (Series, Option<Cats>) {
        let original_name = series.name().clone();
        let mut use_height = match t {
            BaseRDFNodeType::IRI => self.iri_counter,
            BaseRDFNodeType::BlankNode => self.blank_counter,
            BaseRDFNodeType::Literal(l, _) => self.literal_counter_map.get(l).copied().unwrap_or(0),
            BaseRDFNodeType::None => {
                unreachable!("Should never happen")
            }
        };

        let enc = self.get_enc(t);
        // Local cat encs are always in memory
        let mut new_enc = CatEncs::new_empty(None, t);

        let ser = if t.is_lang_string() {
            let value_ser = series
                .struct_()
                .unwrap()
                .field_by_name(LANG_STRING_VALUE_FIELD)
                .unwrap();
            let mut value_encoded =
                encode_single_string_series(&value_ser, &mut use_height, &mut new_enc, enc);
            value_encoded.rename(PlSmallStr::from_str(LANG_STRING_VALUE_FIELD));
            let lang_ser = series
                .struct_()
                .unwrap()
                .field_by_name(LANG_STRING_LANG_FIELD)
                .unwrap();
            let mut lang_encoded =
                encode_single_string_series(&lang_ser, &mut use_height, &mut new_enc, enc);
            lang_encoded.rename(PlSmallStr::from_str(LANG_STRING_LANG_FIELD));
            let mut df = DataFrame::new(
                lang_encoded.len(),
                vec![value_encoded.into_column(), lang_encoded.into_column()],
            )
            .unwrap();
            df = df
                .lazy()
                .with_column(
                    as_struct(vec![
                        col(LANG_STRING_VALUE_FIELD),
                        col(LANG_STRING_LANG_FIELD),
                    ])
                    .alias(original_name.as_str()),
                )
                .select(vec![col(original_name.as_str())])
                .collect()
                .unwrap();
            df.column(original_name.as_str())
                .unwrap()
                .as_materialized_series()
                .clone()
        } else if is_vector_literal_type(t) {
            let stored = lexical_series_to_stored(series);
            let mut ser = encode_single_string_series(&stored, &mut use_height, &mut new_enc, enc);
            ser.rename(original_name);
            ser
        } else {
            let mut ser = encode_single_string_series(series, &mut use_height, &mut new_enc, enc);
            ser.rename(original_name);
            ser
        };

        let local = if !new_enc.maps.is_empty() {
            let cat_type = CatType::from_base_rdf_node_type(t);
            let mut map = HashMap::new();
            map.insert(cat_type, new_enc);
            Some(Cats::from_map(map, None))
        } else {
            None
        };
        (ser, local)
    }

    /// Encodes strings of the given type directly into these (global) cats, adding the missing
    /// ones, and returns the encoding of each input string in order. Duplicates are allowed.
    /// Unlike [`Cats::encode_series`], no local cats are created, so the result needs no
    /// re-encoding when it is added to the triplestore.
    pub fn encode_strs_globally(&mut self, t: &CatType, strs: &[&str]) -> Vec<u32> {
        if self.is_in_memory() {
            let series = Series::new(PlSmallStr::EMPTY, strs);
            let encoded = self
                .encode_strings_global(vec![(t.as_base_rdf_node_type(), series)])
                .pop()
                .unwrap();
            return encoded.u32().unwrap().into_no_null_iter().collect();
        }
        if !self.cat_map.contains_key(t) {
            let enc = CatEncs::new_empty(
                self.path.as_ref().map(|x| x.as_ref()),
                &t.as_base_rdf_node_type(),
            );
            self.cat_map.insert(t.clone(), enc);
        }
        let opts: Vec<_> = strs.iter().map(|s| Some(*s)).collect();
        let mut found = self.cat_map.get(t).unwrap().maybe_encode_strs(&opts);
        let missing: Vec<usize> = found
            .iter()
            .enumerate()
            .filter_map(|(i, u)| if u.is_none() { Some(i) } else { None })
            .collect();
        if !missing.is_empty() {
            let mut seen = HashSet::with_capacity(missing.len());
            let new_strings: Vec<String> = missing
                .iter()
                .filter(|i| seen.insert(strs[**i]))
                .map(|i| strs[*i].to_string())
                .collect();
            let mut counter = self.get_counter(t);
            let enc = self.cat_map.get_mut(t).unwrap();
            enc.maps
                .encode_all_new_non_duplicated_strings(new_strings, &mut counter);
            self.set_counter(counter, t);
            let missing_strs: Vec<_> = missing.iter().map(|i| Some(strs[*i])).collect();
            let encoded = self
                .cat_map
                .get(t)
                .unwrap()
                .maybe_encode_strs(&missing_strs);
            for (i, u) in missing.into_iter().zip(encoded) {
                found[i] = u;
            }
        }
        found
            .into_iter()
            .map(|u| u.expect("string was just encoded"))
            .collect()
    }

    pub fn maybe_encode_blanks(&self, blanks: &[Option<&str>]) -> Vec<Option<u32>> {
        if let Some(encs) = self.cat_map.get(&CatType::Blank) {
            encs.maybe_encode_strs(blanks)
        } else {
            [None].repeat(blanks.len())
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
            [None].repeat(literals.len())
        }
    }

    pub fn maybe_encode_iri_slice(&self, iris: &[Option<&str>]) -> Vec<Option<u32>> {
        let encs = self.cat_map.get(&CatType::IRI);
        if let Some(encs) = encs {
            encs.maybe_encode_strs(iris)
        } else {
            [None].repeat(iris.len())
        }
    }

    pub fn encode_iri_or_local_cat(&self, iri: &str) -> (u32, RDFNodeState) {
        let mut v = self.maybe_encode_iri_slice(&[Some(iri)]);
        if let Some(u) = v.pop().unwrap() {
            (
                u,
                RDFNodeState::from_bases(
                    BaseRDFNodeType::IRI,
                    BaseCatState::CategoricalNative(None),
                ),
            )
        } else {
            // Locals always in memory
            let (u, l) = Cats::new_local_singular_iri(iri, self.iri_counter);
            (
                u,
                RDFNodeState::from_bases(
                    BaseRDFNodeType::IRI,
                    BaseCatState::CategoricalNative(Some(LockedCats::new(l))),
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
                    BaseCatState::CategoricalNative(None),
                ),
            )
        } else {
            let (u, l) = Cats::new_local_singular_blank(blank, self.blank_counter);
            (
                u,
                RDFNodeState::from_bases(
                    BaseRDFNodeType::BlankNode,
                    BaseCatState::CategoricalNative(Some(LockedCats::new(l))),
                ),
            )
        }
    }
}

fn encode_single_string_series(
    series: &Series,
    use_height: &mut u32,
    new_enc: &mut CatEncs,
    enc: Option<&CatEncs>,
) -> Series {
    let maybe_encoded_global = if let Some(enc) = enc {
        let strch = series.str().unwrap();
        let strvec: Vec<_> = strch.iter().collect();
        Some(enc.maybe_encode_strs(&strvec))
    } else {
        None
    };

    let mut encoded_global_local = Vec::with_capacity(series.len());
    // Codes of strings not in the global map, from the local map.
    let mut encode_local = |s: &str| -> u32 {
        if let Some(u) = new_enc.maps.maybe_encode_in_memory_str(s) {
            u
        } else {
            *use_height += 1;
            let su = *use_height;
            new_enc
                .maps
                .encode_new_in_memory_string(Cow::Borrowed(s), su);
            su
        }
    };
    // Consecutive repeats of the same string are looked up once.
    let mut prev: Option<(&str, u32)> = None;
    let strs = series.str().unwrap();
    let globals: Box<dyn Iterator<Item = Option<u32>>> = match maybe_encoded_global {
        Some(enc) => Box::new(enc.into_iter()),
        None => Box::new(std::iter::repeat(None)),
    };
    for (glob, s) in globals.zip(strs.iter()) {
        if glob.is_some() {
            encoded_global_local.push(glob);
            continue;
        }
        let Some(s) = s else {
            encoded_global_local.push(None);
            continue;
        };
        let u = match prev {
            Some((p, u)) if p == s => u,
            _ => {
                let u = encode_local(s);
                prev = Some((s, u));
                u
            }
        };
        encoded_global_local.push(Some(u));
    }

    Series::from_iter(encoded_global_local)
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
    if let BaseCatState::CategoricalNative(Some(c)) = subject_cat_state {
        cats.push(c);
    }
    if let BaseCatState::CategoricalNative(Some(c)) = object_cat_state {
        cats.push(c);
    }
    (cats, enc_trip)
}

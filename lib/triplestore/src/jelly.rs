mod eu;
use super::{TriplesToAdd, Triplestore};
use crate::errors::TriplestoreError;
use crate::jelly::eu::ostrzyciel::jelly::core::proto::v1::mod_RdfLiteral::OneOfliteralKind;
use crate::jelly::eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow;
use crate::jelly::eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::{
    OneOfobject, OneOfpredicate, OneOfsubject,
};
use crate::jelly::eu::ostrzyciel::jelly::core::proto::v1::{
    LogicalStreamType, PhysicalStreamType, RdfDatatypeEntry, RdfIri, RdfLiteral, RdfNameEntry,
    RdfPrefixEntry, RdfStreamFrame, RdfStreamOptions, RdfStreamRow, RdfTriple,
};
use fastbloom::BloomFilter;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::NamedNode;
use polars::prelude::{as_struct, col, IntoLazy, LiteralValue, PlSmallStr};
use polars_core::datatypes::UInt32Chunked;
use polars_core::frame::DataFrame;
use polars_core::prelude::{Column, IntoColumn, Scalar};
use polars_core::POOL;
use quick_protobuf::{serialize_into_vec, BytesReader, Writer};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use representation::cats::maps::in_memory::{
    CatMapsInMemory, PrefixCompressedCatMapsInMemory, PrefixCompressedString,
    UncompressedCatMapsInMemory,
};
use representation::cats::maps::CatMaps;
use representation::cats::{CatEncs, CatType, Cats, LockedCats};
use representation::dataset::NamedGraph;
use representation::formatting::base_literal_expression_to_string;
use representation::iri_split::split_iri;
use representation::rdf_to_polars::{
    polars_literal_values_to_series, rdf_literal_to_polars_literal_value_impl,
};
use representation::solution_mapping::BaseCatState;
use representation::{
    BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME,
    SUBJECT_COL_NAME,
};
use std::borrow::Cow;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;
use tracing::trace;

const JELLY_FRAME_SIZE: usize = 1024;
const IRI_U32: u32 = u32::MAX;
const BLANK_U32: u32 = u32::MAX - 1;
const STRING_U32: u32 = u32::MAX - 2;
const LANG_STRING_U32: u32 = u32::MAX - 3;

impl Triplestore {
    pub fn parse_jelly(
        &mut self,
        slice: &[u8],
        graph: &NamedGraph,
        triples_batch_size: Option<usize>,
    ) -> Result<(), TriplestoreError> {
        // we can build a bytes reader directly out of the bytes
        let mut reader = BytesReader::from_bytes(slice);
        let options: RdfStreamFrame = reader.read_message(slice).map_err(|x| {
            TriplestoreError::ReadJellyError(format!("Error reading initial options: {}", x))
        })?;
        let mut prefix_map: HashMap<u32, Arc<String>> = Default::default();
        let mut name_map: HashMap<u32, Arc<String>> = Default::default();
        let mut b_filter = BloomFilter::with_false_pos(0.001).expected_items(1000000);
        let mut iri_map: HashMap<(u32, u32), u32> = HashMap::new();
        let mut iri_rev_map: HashMap<u32, (u32, u32)> = HashMap::new();
        let mut blank_map: HashMap<String, u32> = HashMap::new();
        let mut datatype_map: HashMap<u32, NamedNode> = Default::default();
        let mut predicate_map: HashMap<
            u32,
            HashMap<bool, HashMap<u32, (Vec<LiteralValue>, Vec<LiteralValue>, Vec<LiteralValue>)>>,
        > = Default::default();
        let mut filter_total = std::time::Duration::ZERO;
        while !reader.is_eof() {
            let frame: RdfStreamFrame = reader.read_message(slice).map_err(|x| {
                TriplestoreError::ReadJellyError(format!("Error reading row: {}", x))
            })?;
            for r in frame.rows {
                match r.row {
                    OneOfrow::options(_) => {}
                    OneOfrow::triple(t) => {
                        let pred = match t.predicate {
                            OneOfpredicate::p_iri(i) => (i.prefix_id, i.name_id),
                            p => {
                                unimplemented!("Predicate {:?}", p)
                            }
                        };
                        let pred_iri_u32 = if let Some(pi) = iri_map.get(&pred) {
                            *pi
                        } else {
                            let pi = iri_map.len() as u32;
                            iri_map.insert(pred.clone(), pi);
                            pi
                        };
                        if !iri_rev_map.contains_key(&pred_iri_u32) {
                            iri_rev_map.insert(pred_iri_u32, pred);
                        }

                        let subject_type_map =
                            if let Some(sm) = predicate_map.get_mut(&pred_iri_u32) {
                                sm
                            } else {
                                predicate_map.insert(pred_iri_u32, Default::default());
                                predicate_map.get_mut(&pred_iri_u32).expect("Just inserted")
                            };
                        let (subject, object_map) = match t.subject {
                            OneOfsubject::s_iri(i) => {
                                let om = if let Some(om) = subject_type_map.get_mut(&true) {
                                    om
                                } else {
                                    subject_type_map.insert(true, Default::default());
                                    subject_type_map.get_mut(&true).expect("Just inserted")
                                };
                                let k = (i.prefix_id, i.name_id);
                                let iri_id = if let Some(iri_id) = iri_map.get(&k) {
                                    *iri_id
                                } else {
                                    let v = iri_map.len() as u32;
                                    iri_map.insert(k, v);
                                    v
                                };
                                (LiteralValue::Scalar(Scalar::from(iri_id)), om)
                            }
                            OneOfsubject::s_bnode(b) => {
                                let om = if let Some(om) = subject_type_map.get_mut(&true) {
                                    om
                                } else {
                                    subject_type_map.insert(false, Default::default());
                                    subject_type_map.get_mut(&false).expect("Just inserted")
                                };
                                let blank_id = if let Some(u) = blank_map.get(b.as_ref()) {
                                    *u
                                } else {
                                    let u = blank_map.len() as u32;
                                    blank_map.insert(b.into_owned(), u);
                                    u
                                };
                                (LiteralValue::Scalar(Scalar::from(blank_id)), om)
                            }
                            OneOfsubject::s_literal(_) => {
                                unreachable!()
                            }
                            OneOfsubject::s_triple_term(_) => {
                                unimplemented!()
                            }
                            OneOfsubject::None => {
                                unreachable!()
                            }
                        };
                        let (object, lang_tag, (subj_vec, obj_vec, lang_tag_vec)) = match t.object {
                            OneOfobject::o_iri(i) => {
                                let vecs = if let Some(vecs) = object_map.get_mut(&IRI_U32) {
                                    vecs
                                } else {
                                    object_map.insert(IRI_U32, Default::default());
                                    object_map.get_mut(&IRI_U32).expect("Just inserted")
                                };
                                let k = (i.prefix_id, i.name_id);
                                let filer_now = Instant::now();
                                let iri_id = if b_filter.contains(&k) {
                                    if let Some(iri_id) = iri_map.get(&k) {
                                        *iri_id
                                    } else {
                                        let v = iri_map.len() as u32;
                                        iri_map.insert(k, v);
                                        b_filter.insert(&k);
                                        v
                                    }
                                } else {
                                    let v = iri_map.len() as u32;
                                    iri_map.insert(k, v);
                                    b_filter.insert(&k);
                                    v
                                };
                                filter_total += filer_now.elapsed();
                                (LiteralValue::Scalar(Scalar::from(iri_id)), None, vecs)
                            }
                            OneOfobject::o_bnode(b) => {
                                let om = if let Some(om) = object_map.get_mut(&BLANK_U32) {
                                    om
                                } else {
                                    object_map.insert(BLANK_U32, Default::default());
                                    object_map.get_mut(&BLANK_U32).expect("Just inserted")
                                };
                                let blank_id = if let Some(u) = blank_map.get(b.as_ref()) {
                                    *u
                                } else {
                                    let u = blank_map.len() as u32;
                                    blank_map.insert(b.into_owned(), u);
                                    u
                                };
                                (LiteralValue::Scalar(Scalar::from(blank_id)), None, om)
                            }
                            OneOfobject::o_literal(l) => {
                                let value;
                                let mut lang_tag = None;
                                let dt_id = match &l.literalKind {
                                    OneOfliteralKind::langtag(t) => {
                                        lang_tag = Some(LiteralValue::Scalar(Scalar::from(
                                            PlSmallStr::from_string(t.to_string()),
                                        )));
                                        value = LiteralValue::Scalar(Scalar::from(
                                            PlSmallStr::from_string(l.lex.to_string()),
                                        ));
                                        LANG_STRING_U32
                                    }
                                    OneOfliteralKind::datatype(t) => {
                                        let dt = match datatype_map.get(t) {
                                            Some(dt) => dt,
                                            None => {
                                                return Err(TriplestoreError::ReadJellyError(
                                                    format!("Datatype id {} not found", t).into(),
                                                ))
                                            }
                                        };
                                        value = rdf_literal_to_polars_literal_value_impl(
                                            l.lex.as_str(),
                                            dt.as_ref(),
                                        );
                                        *t
                                    }
                                    OneOfliteralKind::None => {
                                        value = LiteralValue::Scalar(Scalar::from(
                                            PlSmallStr::from_string(l.lex.to_string()),
                                        ));
                                        STRING_U32
                                    }
                                };
                                let vecs = if let Some(vecs) = object_map.get_mut(&dt_id) {
                                    vecs
                                } else {
                                    object_map.insert(dt_id, Default::default());
                                    object_map.get_mut(&dt_id).expect("Just inserted")
                                };
                                (value, lang_tag, vecs)
                            }
                            OneOfobject::o_triple_term(_) => {
                                unimplemented!()
                            }
                            OneOfobject::None => {
                                unimplemented!()
                            }
                        };
                        subj_vec.push(subject);
                        obj_vec.push(object);
                        if let Some(lang_tag) = lang_tag {
                            lang_tag_vec.push(lang_tag);
                        }
                    }
                    OneOfrow::quad(_) => {
                        unimplemented!()
                    }
                    OneOfrow::graph_start(_) => {
                        unimplemented!()
                    }
                    OneOfrow::graph_end(_) => {
                        unimplemented!()
                    }
                    OneOfrow::namespace(_) => {
                        unimplemented!()
                    }
                    OneOfrow::name(n) => {
                        name_map.insert(n.id, Arc::new(n.value.to_string()));
                    }
                    OneOfrow::prefix(p) => {
                        prefix_map.insert(p.id, Arc::new(p.value.into_owned()));
                    }
                    OneOfrow::datatype(dt) => {
                        let nn = NamedNode::new(dt.value.as_str()).map_err(|e| {
                            TriplestoreError::ReadJellyError(format!(
                                "Invalid datatype IRI {}: {}",
                                dt.value, e
                            ))
                        })?;
                        datatype_map.insert(dt.id, nn);
                    }
                    OneOfrow::None => {
                        unimplemented!()
                    }
                }
            }
        }
        let mut iri_cat_enc = PrefixCompressedCatMapsInMemory::new_empty();

        for ((pre, suf), u) in iri_map {
            let prefix = match prefix_map.get(&pre) {
                Some(p) => p,
                None => {
                    return Err(TriplestoreError::ReadJellyError(
                        format!("Prefix id {} not found", pre).into(),
                    ))
                }
            };
            let suffix = match name_map.get(&suf) {
                Some(s) => s,
                None => {
                    return Err(TriplestoreError::ReadJellyError(
                        format!("Name id {} not found", suf).into(),
                    ))
                }
            };
            let (pre, suf) = split_iri(suffix.as_str());
            if pre.is_empty() {
                let (pre, suf) = split_iri(prefix.as_str());
                if suf.is_empty() {
                    iri_cat_enc.encode_new_prefix_suffix_str(
                        Cow::Borrowed(prefix),
                        suffix.to_string(),
                        u,
                    );
                } else {
                    iri_cat_enc.encode_new_prefix_suffix_str(
                        Cow::Borrowed(pre),
                        format!("{}{}", suf, suffix),
                        u,
                    );
                }
            } else {
                iri_cat_enc.encode_new_prefix_suffix_str(
                    Cow::Owned(format!("{}{}", prefix, pre)),
                    suf.to_string(),
                    u,
                );
            }
        }
        println!("filter total: {}", filter_total.as_secs_f32());
        let iri_cat_enc = CatEncs {
            maps: CatMaps::InMemory(CatMapsInMemory::Compressed(iri_cat_enc)),
        };

        let mut blank_cat_enc = UncompressedCatMapsInMemory::new_empty();
        for u in blank_map.values() {
            let uuid = uuid::Uuid::new_v4().to_string();
            blank_cat_enc.encode_new_string(uuid, *u);
        }
        let blank_cat_enc = CatEncs {
            maps: CatMaps::InMemory(CatMapsInMemory::Uncompressed(blank_cat_enc)),
        };
        let maps =
            HashMap::from_iter([(CatType::IRI, iri_cat_enc), (CatType::Blank, blank_cat_enc)]);
        let local = LockedCats::new(Cats::from_map(maps));

        let local_uuid = local.read().expect("Could not read LockedCat").uuid.clone();
        let mut reencs_map = {
            let mut global = self.global_cats.write().map_err(|e| {
                TriplestoreError::ReadJellyError(format!(
                    "Could not acquire write lock on global cats: {}",
                    e
                ))
            })?;
            global.merge(vec![local.clone()], None)
        };
        let reencs = reencs_map.remove(&local_uuid).unwrap_or_default();
        let iri_reenc = reencs.get(&CatType::IRI);
        let blank_reenc = reencs.get(&CatType::Blank);

        let mut triples_to_add = Vec::new();
        for (p, m) in predicate_map {
            let (pre, suf) = iri_rev_map.get(&p).ok_or_else(|| {
                TriplestoreError::ReadJellyError(format!(
                    "Missing IRI reverse mapping for predicate {}",
                    p
                ))
            })?;

            let prefix = prefix_map.get(pre).ok_or_else(|| {
                TriplestoreError::ReadJellyError(format!("Missing prefix for id {}", pre))
            })?;

            let name = name_map.get(suf).ok_or_else(|| {
                TriplestoreError::ReadJellyError(format!("Missing name for id {}", suf))
            })?;

            let predicate = NamedNode::new(format!("{}{}", prefix, name)).map_err(|e| {
                TriplestoreError::ReadJellyError(format!(
                    "Invalid predicate IRI {}{}: {}",
                    prefix, name, e
                ))
            })?;

            for (subject_is_iri, om) in m {
                let subject_type = if subject_is_iri {
                    BaseRDFNodeType::IRI
                } else {
                    BaseRDFNodeType::BlankNode
                };
                for (dt, (subject_vec, object_vec, lang_tag_vec)) in om {
                    let object_type = if dt == IRI_U32 {
                        BaseRDFNodeType::IRI
                    } else if dt == BLANK_U32 {
                        BaseRDFNodeType::BlankNode
                    } else if dt == LANG_STRING_U32 {
                        BaseRDFNodeType::Literal(rdf::LANG_STRING.into_owned())
                    } else if dt == STRING_U32 {
                        BaseRDFNodeType::Literal(xsd::STRING.into_owned())
                    } else {
                        let literal_iri = match datatype_map.get(&dt) {
                            Some(dt) => dt,
                            None => {
                                return Err(TriplestoreError::ReadJellyError(
                                    format!("Datatype id {} not found", dt).into(),
                                ))
                            }
                        };
                        BaseRDFNodeType::Literal(literal_iri.clone())
                    };
                    let subject_ser =
                        polars_literal_values_to_series(subject_vec, SUBJECT_COL_NAME);
                    let object_ser = match &object_type {
                        BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode => {
                            polars_literal_values_to_series(object_vec, OBJECT_COL_NAME)
                        }
                        BaseRDFNodeType::Literal(l) => {
                            if l.as_ref() == rdf::LANG_STRING {
                                let lex_ser = polars_literal_values_to_series(
                                    object_vec,
                                    LANG_STRING_VALUE_FIELD,
                                );
                                let lang_ser = polars_literal_values_to_series(
                                    lang_tag_vec,
                                    LANG_STRING_LANG_FIELD,
                                );
                                let mut df = DataFrame::new(vec![
                                    lex_ser.into_column(),
                                    lang_ser.into_column(),
                                ])
                                .map_err(|e| {
                                    TriplestoreError::ReadJellyError(format!(
                                        "Error creating DataFrame for language tagged string: {}",
                                        e
                                    ))
                                })?;
                                df = df
                                    .lazy()
                                    .with_column(
                                        as_struct(vec![
                                            col(LANG_STRING_VALUE_FIELD),
                                            col(LANG_STRING_LANG_FIELD),
                                        ])
                                        .alias(OBJECT_COL_NAME),
                                    )
                                    .select([col(OBJECT_COL_NAME)])
                                    .collect()
                                    .map_err(|e| {
                                        TriplestoreError::ReadJellyError(format!(
                                            "Error structuring DataFrame for language tagged string: {}",
                                            e
                                        ))
                                    })?;
                                df.column(OBJECT_COL_NAME)
                                    .map_err(|e| {
                                        TriplestoreError::ReadJellyError(format!(
                                            "Missing column {}: {}",
                                            OBJECT_COL_NAME, e
                                        ))
                                    })?
                                    .as_materialized_series()
                                    .clone()
                            } else {
                                polars_literal_values_to_series(object_vec, OBJECT_COL_NAME)
                            }
                        }
                        BaseRDFNodeType::None => {
                            unreachable!()
                        }
                    };
                    let mut df =
                        DataFrame::new(vec![subject_ser.into_column(), object_ser.into_column()])
                            .map_err(|e| {
                            TriplestoreError::ReadJellyError(format!(
                                "Error creating DataFrame for triple: {}",
                                e
                            ))
                        })?;

                    let sub_reenc = if subject_is_iri {
                        iri_reenc
                    } else {
                        blank_reenc
                    };
                    let mut lf = df.lazy();
                    if let Some(reenc) = sub_reenc {
                        lf = reenc.clone().re_encode(lf, SUBJECT_COL_NAME, false);
                    }

                    let obj_reenc = if object_type.is_iri() || object_type.is_blank_node() {
                        if object_type.is_iri() {
                            iri_reenc
                        } else {
                            blank_reenc
                        }
                    } else {
                        None
                    };

                    if let Some(reenc) = obj_reenc {
                        lf = reenc.clone().re_encode(lf, OBJECT_COL_NAME, false);
                    }

                    df = lf.collect().map_err(|e| {
                        TriplestoreError::ReadJellyError(format!("Error remapping: {}", e))
                    })?;

                    let object_cat_state = if object_type.is_iri() || object_type.is_blank_node() {
                        BaseCatState::CategoricalNative(false, None)
                    } else {
                        object_type.default_input_cat_state()
                    };
                    let trips = TriplesToAdd {
                        df,
                        subject_type: subject_type.clone(),
                        object_type,
                        predicate: Some(predicate.clone()),
                        graph: graph.clone(),
                        subject_cat_state: BaseCatState::CategoricalNative(false, None),
                        object_cat_state,
                        predicate_cat_state: None,
                    };
                    triples_to_add.push(trips);
                }
            }
        }
        let start_add_triples_vec = Instant::now();
        self.add_triples_vec(triples_to_add, false)?;
        trace!(
            "Adding triples vec took {}",
            start_add_triples_vec.elapsed().as_secs_f32()
        );

        Ok(())
    }
}

pub struct JellyEncoder {
    prefix_table: HashMap<u32, u32>,
    prefix_lookup: HashMap<String, u32>,
    next_prefix_id: u32,
    name_table: HashMap<u32, u32>,
    next_name_id: u32,
    datatype_table: HashMap<String, u32>,
    next_datatype_id: u32,
    pending_rows: Vec<RdfStreamRow<'static>>,
}

impl JellyEncoder {
    pub(crate) fn new() -> Self {
        Self {
            prefix_table: HashMap::new(),
            prefix_lookup: HashMap::new(),
            next_prefix_id: 1,
            name_table: HashMap::new(),
            next_name_id: 1,
            datatype_table: HashMap::new(),
            next_datatype_id: 1,
            pending_rows: Vec::new(),
        }
    }

    pub fn write_options<W: Write>(&mut self, buf: &mut W) -> Result<(), TriplestoreError> {
        let options_frame = RdfStreamFrame {
            rows: vec![RdfStreamRow {
                row: OneOfrow::options(RdfStreamOptions {
                    physical_type: PhysicalStreamType::PHYSICAL_STREAM_TYPE_TRIPLES,
                    logical_type: LogicalStreamType::LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
                    version: 1,
                    max_name_table_size: 4096,
                    max_prefix_table_size: 4096,
                    max_datatype_table_size: 4096,
                    ..Default::default()
                }),
            }],
            metadata: Default::default(),
        };
        let v = serialize_into_vec(&options_frame).map_err(|x| {
            TriplestoreError::WriteJellyError(format!("Error serializing options frame: {}", x))
        })?;
        buf.write(&v).map_err(|x| {
            TriplestoreError::WriteJellyError(format!("Error writing options to buffer: {}", x))
        })?;
        Ok(())
    }

    pub fn write_jelly<W: Write>(
        &mut self,
        buf: &mut W,
        df: DataFrame,
        predicate: &NamedNode,
        predicate_cat: u32,
        subject_type: &BaseRDFNodeType,
        object_type: &BaseRDFNodeType,
        global_cats: LockedCats,
    ) -> Result<(), TriplestoreError> {
        self.maybe_prepare_new_names_prefixes(
            df.column(SUBJECT_COL_NAME).map_err(|e| {
                TriplestoreError::WriteJellyError(format!("Missing subject column: {}", e))
            })?,
            subject_type,
            global_cats.clone(),
        )?;
        self.maybe_prepare_new_names_prefixes(
            df.column(OBJECT_COL_NAME).map_err(|e| {
                TriplestoreError::WriteJellyError(format!("Missing object column: {}", e))
            })?,
            object_type,
            global_cats.clone(),
        )?;
        let (pre, suf) = split_iri(predicate.as_str());
        let pre_u32 = self.get_or_insert_prefix(predicate_cat, pre);
        let name_u32 = self.get_or_insert_name(predicate_cat, suf);
        let predicate = OneOfpredicate::p_iri(RdfIri {
            prefix_id: pre_u32,
            name_id: name_u32,
        });

        let subject_u32s = df
            .column(SUBJECT_COL_NAME)
            .map_err(|e| {
                TriplestoreError::WriteJellyError(format!("Missing subject column: {}", e))
            })?
            .u32()
            .map_err(|e| {
                TriplestoreError::WriteJellyError(format!("Subject column is not u32: {}", e))
            })?;

        let subjects = if subject_type.is_iri() {
            self.create_iri_subjects(subject_u32s)?
        } else {
            create_blank_subjects(subject_u32s)?
        };

        //Todo: push datatype row and predicate row.
        if let BaseRDFNodeType::Literal(t) = object_type {
            let mut exprs = base_literal_expression_to_string(
                col(OBJECT_COL_NAME),
                object_type,
                &object_type.default_stored_cat_state(),
                global_cats,
            );
            let mut lf = df.clone().lazy().select([col(OBJECT_COL_NAME)]);
            let mut new_exprs = Vec::with_capacity(exprs.len());
            for (i, e) in exprs.into_iter().enumerate() {
                new_exprs.push(e.alias(format!("{i}")));
            }
            lf = lf.with_columns(new_exprs);
            let df = lf.collect().map_err(|e| {
                TriplestoreError::WriteJellyError(format!(
                    "Error evaluating literal expressions for object column: {}",
                    e
                ))
            })?;

            if object_type.is_lang_string() {
                for ((subject, o_lex), o_lang) in subjects
                    .into_iter()
                    .zip(df.column("0").map_err(|e| {
                        TriplestoreError::WriteJellyError(format!(
                            "Missing object column after evaluating literal expressions: {}",
                            e
                        ))
                    })?.str().map_err(|e| {
                        TriplestoreError::WriteJellyError(format!(
                            "Object column is not string after evaluating literal expressions: {}",
                            e
                        ))
                    })?)
                    .zip(df.column("1").map_err(|e| {
                        TriplestoreError::WriteJellyError(format!(
                            "Missing language tag column after evaluating literal expressions: {}",
                            e
                        ))
                    })?.str().map_err(|e| {
                        TriplestoreError::WriteJellyError(format!(
                            "Language tag column is not string after evaluating literal expressions: {}",
                            e
                        ))
                    })?)
                {
                    let o_lex = match o_lex {
                        Some(o) => o,
                        None => return Err(TriplestoreError::WriteJellyError(
                            "Missing lexical form for language tagged literal".into(),
                        )),
                    };
                    let o_lang = o_lang.ok_or_else(|| {
                        TriplestoreError::WriteJellyError(
                            "Missing language tag for language tagged literal".into(),
                        )
                    })?;
                    let object = OneOfobject::o_literal(RdfLiteral {
                        lex: Cow::Owned(o_lex.to_string()),
                        literalKind: OneOfliteralKind::langtag(Cow::Owned(
                            o_lang.to_string(),
                        )),
                    });
                    self.pending_rows.push(RdfStreamRow {
                        row: OneOfrow::triple(RdfTriple {
                            subject,
                            predicate: predicate.clone(),
                            object,
                        }),
                    });
                }
            } else {
                let dt_o = if let Some(dt_o) = self.datatype_table.get(t.as_str()) {
                    *dt_o
                } else {
                    let dt_o = self.next_datatype_id;
                    self.datatype_table.insert(t.as_str().to_string(), dt_o);
                    self.pending_rows.push(RdfStreamRow {
                        row: OneOfrow::datatype(RdfDatatypeEntry {
                            id: dt_o,
                            value: Cow::Owned(t.as_str().to_string()),
                        }),
                    });
                    self.next_datatype_id += 1;
                    dt_o
                };
                for (subject, o) in subjects.into_iter().zip(
                    df.column("0")
                        .map_err(|e| {
                            TriplestoreError::WriteJellyError(format!(
                                "Missing object column after evaluating literal expressions: {}",
                                e
                            ))
                        })?
                        .str()
                        .map_err(|e| {
                            TriplestoreError::WriteJellyError(format!(
                            "Object column is not string after evaluating literal expressions: {}",
                            e
                        ))
                        })?,
                ) {
                    let o = o.ok_or_else(|| {
                        TriplestoreError::WriteJellyError("Missing lexical form for literal".into())
                    })?;
                    let object = OneOfobject::o_literal(RdfLiteral {
                        lex: Cow::Owned(o.to_string()),
                        literalKind: OneOfliteralKind::datatype(dt_o),
                    });
                    self.pending_rows.push(RdfStreamRow {
                        row: OneOfrow::triple(RdfTriple {
                            subject,
                            predicate: predicate.clone(),
                            object,
                        }),
                    });
                }
            }
        } else {
            // subject and object are both either blank or iri: u32 cols..
            let object_u32s = df
                .column(OBJECT_COL_NAME)
                .map_err(|e| {
                    TriplestoreError::WriteJellyError(format!("Missing object column: {}", e))
                })?
                .u32()
                .map_err(|e| {
                    TriplestoreError::WriteJellyError(format!("Object column is not u32: {}", e))
                })?;
            let objects = if object_type.is_iri() {
                self.create_iri_objects(object_u32s)?
            } else {
                create_blank_objects(object_u32s)?
            };
            for (subject, object) in subjects.into_iter().zip(objects.into_iter()) {
                self.pending_rows.push(RdfStreamRow {
                    row: OneOfrow::triple(RdfTriple {
                        subject,
                        predicate: predicate.clone(),
                        object,
                    }),
                });
            }
        };
        self.write_rows(buf, false)?;
        Ok(())
    }

    fn create_iri(&self, u: &u32) -> Result<RdfIri, TriplestoreError> {
        Ok(RdfIri {
            prefix_id: *self.prefix_table.get(u).ok_or_else(|| {
                TriplestoreError::WriteJellyError(format!("Prefix id not found for u32 {}", u))
            })?,
            name_id: *self.name_table.get(u).ok_or_else(|| {
                TriplestoreError::WriteJellyError(format!("Name id not found for u32 {}", u))
            })?,
        })
    }

    fn create_iri_subjects(
        &self,
        u32s: &UInt32Chunked,
    ) -> Result<Vec<OneOfsubject<'static>>, TriplestoreError> {
        u32s.iter()
            .map(|x| {
                let u = x.ok_or_else(|| {
                    TriplestoreError::WriteJellyError("Null value in subject column".into())
                })?;
                Ok(OneOfsubject::s_iri(self.create_iri(&u)?))
            })
            .collect()
    }

    fn create_iri_objects(
        &self,
        u32s: &UInt32Chunked,
    ) -> Result<Vec<OneOfobject<'static>>, TriplestoreError> {
        u32s.iter()
            .map(|x| {
                let u = x.ok_or_else(|| {
                    TriplestoreError::WriteJellyError("Null value in object column".into())
                })?;
                Ok(OneOfobject::o_iri(self.create_iri(&u)?))
            })
            .collect()
    }

    pub fn write_rows<W: Write>(&mut self, buf: &mut W, all: bool) -> Result<(), TriplestoreError> {
        if !all && self.pending_rows.len() < JELLY_FRAME_SIZE {
            return Ok(());
        }
        let mut segments = Vec::new();
        let threads = POOL.current_num_threads();
        let threads = cmp::max(threads, 1);
        let frames_per_thread = self.pending_rows.len().div_ceil(JELLY_FRAME_SIZE);
        let mut pending_iter = self.pending_rows.drain(..);
        'outer: for _ in 0..(threads - 1) {
            let mut seg = Vec::with_capacity(frames_per_thread * JELLY_FRAME_SIZE);
            for _ in 0..(JELLY_FRAME_SIZE * frames_per_thread) {
                if let Some(n) = pending_iter.next() {
                    seg.push(n)
                }
            }
            if !seg.is_empty() {
                segments.push(seg);
            } else {
                break 'outer;
            }
        }
        if all {
            let seg: Vec<_> = pending_iter.collect();
            if !seg.is_empty() {
                segments.push(seg);
            }
        } else {
            let mut seg = Vec::with_capacity(frames_per_thread * JELLY_FRAME_SIZE);
            let mut frame = Vec::with_capacity(JELLY_FRAME_SIZE);
            let mut i = 0;
            loop {
                if let Some(n) = pending_iter.next() {
                    frame.push(n);
                    i += 1;
                } else {
                    break;
                }
                if i > 0 && i % JELLY_FRAME_SIZE == 0 {
                    seg.extend(frame.drain(..));
                }
            }
            if !seg.is_empty() {
                segments.push(seg);
            }
            self.pending_rows = frame.into_iter().chain(pending_iter).collect();
        }
        let mut segments_buffers: Vec<(_, Vec<u8>)> =
            segments.into_iter().map(|x| (x, Vec::new())).collect();

        let buffers: Result<Vec<_>, TriplestoreError> = POOL.install(|| {
            segments_buffers
                .into_iter()
                .map(|(mut rows, mut buffer)| {
                    let mut rows_iter = rows.drain(..);
                    loop {
                        let mut rows = Vec::with_capacity(JELLY_FRAME_SIZE);
                        for _ in 0..JELLY_FRAME_SIZE {
                            if let Some(n) = rows_iter.next() {
                                rows.push(n);
                            } else {
                                break;
                            }
                        }
                        if !rows.is_empty() {
                            let frame = RdfStreamFrame {
                                rows,
                                metadata: HashMap::new(),
                            };
                            let v = serialize_into_vec(&frame).map_err(|x| {
                                TriplestoreError::WriteJellyError(format!(
                                    "Error serializing to vec: {}",
                                    x
                                ))
                            })?;
                            buffer.extend(v);
                        } else {
                            break;
                        }
                    }
                    Ok(buffer)
                })
                .collect()
        });
        let buffers = buffers?;
        for part in buffers {
            buf.write(&part).map_err(|x| {
                TriplestoreError::WriteJellyError(format!("Error writing partial buffer {}", x))
            })?;
        }
        Ok(())
    }

    fn get_or_insert_prefix(&mut self, cat: u32, prefix: &str) -> u32 {
        if let Some(p) = self.prefix_table.get(&cat) {
            *p
        } else {
            let prefix_u = if let Some(prefix_u) = self.prefix_lookup.get(prefix) {
                *prefix_u
            } else {
                let prefix_u = self.next_prefix_id;
                self.pending_rows.push(RdfStreamRow {
                    row: OneOfrow::prefix(RdfPrefixEntry {
                        id: prefix_u,
                        value: Cow::Owned(prefix.to_string()),
                    }),
                });
                self.prefix_lookup.insert(prefix.to_string(), prefix_u);
                self.next_prefix_id += 1;
                prefix_u
            };
            self.prefix_table.insert(cat, prefix_u);
            prefix_u
        }
    }

    fn get_or_insert_name(&mut self, cat: u32, name: &str) -> u32 {
        if let Some(&id) = self.name_table.get(&cat) {
            return id;
        }
        let id = self.next_name_id;
        self.next_name_id += 1;
        self.name_table.insert(cat, id);
        self.pending_rows.push(RdfStreamRow {
            row: OneOfrow::name(RdfNameEntry {
                id,
                value: Cow::Owned(name.to_string()),
            }),
        });
        id
    }

    fn maybe_prepare_new_names_prefixes(
        &mut self,
        c: &Column,
        t: &BaseRDFNodeType,
        global_cats: LockedCats,
    ) -> Result<(), TriplestoreError> {
        let read_cats = global_cats.read().map_err(|e| {
            TriplestoreError::WriteJellyError(format!(
                "Error acquiring read lock on global categories: {}",
                e
            ))
        })?;
        let mut seen_iri_u32s = Vec::new();
        let mut seen_iri_out_u32s = Vec::new();
        match t {
            BaseRDFNodeType::IRI => {
                for u in c.u32().expect("IRI column should be u32") {
                    let u = u.expect("Null value in IRI column");
                    if !self.name_table.contains_key(&u) {
                        self.name_table.insert(u, self.next_name_id);
                        seen_iri_u32s.push(u);
                        seen_iri_out_u32s.push(u);
                        self.next_name_id += 1;
                    }
                }
            }
            _ => {}
        }
        if !seen_iri_u32s.is_empty() {
            //Deduplication in order to avoid duplicate names
            let seen_iri_out_u32s_set: HashSet<_> = seen_iri_out_u32s.into_iter().collect();
            let seen_iri_out_u32s: Vec<_> = seen_iri_out_u32s_set.into_iter().collect();
            let nns = read_cats.decode_iri_u32s(&seen_iri_u32s, None);
            let (pres, sufs): (Vec<_>, Vec<_>) = nns
                .par_iter()
                .map(|nn| {
                    let (pre, suf) = split_iri(nn.as_str());
                    (pre, suf)
                })
                .unzip();

            for (new_u, suf) in seen_iri_out_u32s.iter().zip(sufs) {
                let jelly_id = *self
                    .name_table
                    .get(new_u)
                    .expect("Just inserted name id should be present");
                self.pending_rows.push(RdfStreamRow {
                    row: OneOfrow::name(RdfNameEntry {
                        id: jelly_id,
                        value: Cow::Owned(suf.to_string()),
                    }),
                });
            }

            for (prefix, u) in pres.iter().zip(seen_iri_u32s) {
                self.get_or_insert_prefix(u, *prefix);
            }
        }
        Ok(())
    }
}

fn create_blank_subject(u: u32) -> OneOfsubject<'static> {
    OneOfsubject::s_bnode(create_blank_cow(u))
}

fn create_blank_object(u: u32) -> OneOfobject<'static> {
    OneOfobject::o_bnode(create_blank_cow(u))
}

fn create_blank_cow(u: u32) -> Cow<'static, str> {
    Cow::Owned(format!("b{}", u))
}
fn create_blank_subjects(
    u32s: &UInt32Chunked,
) -> Result<Vec<OneOfsubject<'static>>, TriplestoreError> {
    u32s.iter()
        .map(|x| {
            let u = x.ok_or_else(|| {
                TriplestoreError::WriteJellyError("Null value in subject column".into())
            })?;
            Ok(create_blank_subject(u))
        })
        .collect()
}

fn create_blank_objects(
    u32s: &UInt32Chunked,
) -> Result<Vec<OneOfobject<'static>>, TriplestoreError> {
    u32s.iter()
        .map(|x| {
            let u = x.ok_or_else(|| {
                TriplestoreError::WriteJellyError("Null value in object column".into())
            })?;
            Ok(create_blank_object(u))
        })
        .collect()
}

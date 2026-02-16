mod eu;
use std::borrow::Cow;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::io::Write;

use quick_protobuf::{serialize_into_vec, BytesWriter, MessageWrite, Writer};

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
use oxrdf::NamedNode;
use polars::polars_utils::parma::raw::Key;
use polars::polars_utils::pl_serialize::serialize_into_writer;
use polars::prelude::{col, IntoLazy};
use polars_core::datatypes::UInt32Chunked;
use polars_core::frame::DataFrame;
use polars_core::prelude::{Column, LhsNumOps};
use polars_core::POOL;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use representation::cats::LockedCats;
use representation::formatting::base_literal_expression_to_string;
use representation::iri_split::split_iri;
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};

const JELLY_FRAME_SIZE: usize = 1024;

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
                    max_name_table_size: 0,
                    max_prefix_table_size: 0,
                    max_datatype_table_size: 0,
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
            df.column(SUBJECT_COL_NAME).unwrap(),
            subject_type,
            global_cats.clone(),
        );
        self.maybe_prepare_new_names_prefixes(
            df.column(OBJECT_COL_NAME).unwrap(),
            object_type,
            global_cats.clone(),
        );
        let (pre, suf) = split_iri(predicate.as_str());
        let pre_u32 = self.get_or_insert_prefix(predicate_cat, pre);
        let name_u32 = self.get_or_insert_name(predicate_cat, suf);
        let predicate = OneOfpredicate::p_iri(RdfIri {
            prefix_id: pre_u32,
            name_id: name_u32,
        });

        let subject_u32s = df.column(SUBJECT_COL_NAME).unwrap().u32().unwrap();

        let subjects = if subject_type.is_iri() {
            self.create_iri_subjects(subject_u32s)
        } else {
            create_blank_subjects(subject_u32s)
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
            let df = lf.collect().unwrap();

            if object_type.is_lang_string() {
                for ((subject, o_lex), o_lang) in subjects
                    .into_iter()
                    .zip(df.column("0").unwrap().str().unwrap())
                    .zip(df.column("1").unwrap().str().unwrap())
                {
                    let o_lex = o_lex.unwrap();
                    let object = OneOfobject::o_literal(RdfLiteral {
                        lex: Cow::Owned(o_lex.to_string()),
                        literalKind: OneOfliteralKind::langtag(Cow::Owned(
                            o_lang.unwrap().to_string(),
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
                for (subject, o) in subjects
                    .into_iter()
                    .zip(df.column("0").unwrap().str().unwrap())
                {
                    let o = o.unwrap();
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
            let object_u32s = df.column(OBJECT_COL_NAME).unwrap().u32().unwrap();
            let objects = if object_type.is_iri() {
                self.create_iri_objects(object_u32s)
            } else {
                create_blank_objects(object_u32s)
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

    fn create_iri(&self, u: &u32) -> RdfIri {
        RdfIri {
            prefix_id: *self.prefix_table.get(u).unwrap(),
            name_id: *self.name_table.get(u).unwrap(),
        }
    }

    fn create_iri_subjects(&self, u32s: &UInt32Chunked) -> Vec<OneOfsubject<'static>> {
        u32s.iter()
            .map(|x| OneOfsubject::s_iri(self.create_iri(&x.unwrap())))
            .collect()
    }

    fn create_iri_objects(&self, u32s: &UInt32Chunked) -> Vec<OneOfobject<'static>> {
        u32s.iter()
            .map(|x| OneOfobject::o_iri(self.create_iri(&x.unwrap())))
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
    ) {
        let read_cats = global_cats.read().unwrap();
        let mut seen_iri_u32s = Vec::new();
        let mut seen_iri_out_u32s = Vec::new();
        match t {
            BaseRDFNodeType::IRI => {
                for u in c.u32().unwrap() {
                    let u = u.unwrap();
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
                self.pending_rows.push(RdfStreamRow {
                    row: OneOfrow::name(RdfNameEntry {
                        id: *new_u,
                        value: Cow::Owned(suf.to_string()),
                    }),
                });
            }

            for (prefix, u) in pres.iter().zip(seen_iri_u32s) {
                self.get_or_insert_prefix(u, *prefix);
            }
        }
        println!("PRefixes {:?}", self.pending_rows);
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
fn create_blank_subjects(u32s: &UInt32Chunked) -> Vec<OneOfsubject<'static>> {
    u32s.iter()
        .map(|x| create_blank_subject(x.unwrap()))
        .collect()
}

fn create_blank_objects(u32s: &UInt32Chunked) -> Vec<OneOfobject<'static>> {
    u32s.iter()
        .map(|x| create_blank_object(x.unwrap()))
        .collect()
}

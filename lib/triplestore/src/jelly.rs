mod eu;
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::Write;

use quick_protobuf::{MessageWrite, Writer};

use oxrdf::{NamedOrBlankNode, Term, Triple};
use crate::jelly::eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow;
use crate::jelly::eu::ostrzyciel::jelly::core::proto::v1::{LogicalStreamType, PhysicalStreamType, RdfDatatypeEntry, RdfIri, RdfLiteral, RdfNameEntry, RdfPrefixEntry, RdfStreamFrame, RdfStreamOptions, RdfStreamRow, RdfTriple};
use crate::jelly::eu::ostrzyciel::jelly::core::proto::v1::mod_RdfLiteral::OneOfliteralKind;
use crate::jelly::eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::{OneOfobject, OneOfpredicate, OneOfsubject};

const JELLY_FRAME_SIZE: usize = 1024;

struct JellyEncoder {
    prefix_table: HashMap<String, u32>,
    next_prefix_id: u32,
    name_table: HashMap<String, u32>,
    next_name_id: u32,
    datatype_table: HashMap<String, u32>,
    next_datatype_id: u32,
    pending_rows: Vec<RdfStreamRow<'static>>,
}

impl JellyEncoder {
    fn new() -> Self {
        Self {
            prefix_table: HashMap::new(),
            next_prefix_id: 1,
            name_table: HashMap::new(),
            next_name_id: 1,
            datatype_table: HashMap::new(),
            next_datatype_id: 1,
            pending_rows: Vec::new(),
        }
    }

    fn get_or_insert_prefix(&mut self, prefix: &str) -> u32 {
        if let Some(&id) = self.prefix_table.get(prefix) {
            return id;
        }
        let id = self.next_prefix_id;
        self.next_prefix_id += 1;
        self.prefix_table.insert(prefix.to_string(), id);
        self.pending_rows.push(RdfStreamRow {
            row: OneOfrow::prefix(RdfPrefixEntry {
                id,
                value: Cow::Owned(prefix.to_string()),
            }),
        });
        id
    }

    fn get_or_insert_name(&mut self, name: &str) -> u32 {
        if let Some(&id) = self.name_table.get(name) {
            return id;
        }
        let id = self.next_name_id;
        self.next_name_id += 1;
        self.name_table.insert(name.to_string(), id);
        self.pending_rows.push(RdfStreamRow {
            row: OneOfrow::name(RdfNameEntry {
                id,
                value: Cow::Owned(name.to_string()),
            }),
        });
        id
    }

    fn get_or_insert_datatype(&mut self, dt_iri: &str) -> u32 {
        if let Some(&id) = self.datatype_table.get(dt_iri) {
            return id;
        }
        let id = self.next_datatype_id;
        self.next_datatype_id += 1;
        self.datatype_table.insert(dt_iri.to_string(), id);
        self.pending_rows.push(RdfStreamRow {
            row: OneOfrow::datatype(RdfDatatypeEntry {
                id,
                value: Cow::Owned(dt_iri.to_string()),
            }),
        });
        id
    }

    fn encode_iri(&mut self, iri: &str) -> RdfIri {
        let (prefix, local) = split_iri(iri);
        let prefix_id = self.get_or_insert_prefix(prefix);
        let name_id = self.get_or_insert_name(local);
        RdfIri { prefix_id, name_id }
    }

    fn take_pending(&mut self) -> Vec<RdfStreamRow<'static>> {
        std::mem::take(&mut self.pending_rows)
    }

    fn encode_triple(&mut self, triple: &Triple) -> RdfStreamRow<'static> {
        let subject = match &triple.subject {
            NamedOrBlankNode::NamedNode(nn) => {
                OneOfsubject::s_iri(self.encode_iri(nn.as_str()))
            }
            NamedOrBlankNode::BlankNode(bn) => {
                OneOfsubject::s_bnode(Cow::Owned(bn.as_str().to_string()))
            }
            #[allow(unreachable_patterns)]
            _ => OneOfsubject::None,
        };

        let predicate = OneOfpredicate::p_iri(
            self.encode_iri(triple.predicate.as_str()),
        );

        let object = match &triple.object {
            Term::NamedNode(nn) => {
                OneOfobject::o_iri(self.encode_iri(nn.as_str()))
            }
            Term::BlankNode(bn) => {
                OneOfobject::o_bnode(Cow::Owned(bn.as_str().to_string()))
            }
            Term::Literal(lit) => {
                let literal_kind = if let Some(lang) = lit.language() {
                    OneOfliteralKind::langtag(Cow::Owned(lang.to_string()))
                } else {
                    let dt = lit.datatype().as_str();
                    if dt == "http://www.w3.org/2001/XMLSchema#string" {
                        OneOfliteralKind::None
                    } else {
                        let dt_id = self.get_or_insert_datatype(dt);
                        OneOfliteralKind::datatype(dt_id)
                    }
                };
                OneOfobject::o_literal(RdfLiteral {
                    lex: Cow::Owned(lit.value().to_string()),
                    literalKind: literal_kind,
                })
            }
            #[allow(unreachable_patterns)]
            _ => OneOfobject::None,
        };

        RdfStreamRow {
            row: OneOfrow::triple(RdfTriple {
                subject,
                predicate,
                object,
            }),
        }
    }
}

fn split_iri(iri: &str) -> (&str, &str) {
    if let Some(pos) = iri.rfind('#') {
        (&iri[..=pos], &iri[pos + 1..])
    } else if let Some(pos) = iri.rfind('/') {
        (&iri[..=pos], &iri[pos + 1..])
    } else {
        ("", iri)
    }
}

fn write_delimit_frame<W: Write>(
    buf: &mut W,
    frame: RdfStreamFrame,
) -> std::io::Result<()> {
    let size = frame.get_size();
    let mut temp = Vec::with_capacity(size + 10);
    {
        let mut writer = Writer::new(&mut temp);
        writer
            .write_varint(size as u64)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        frame
            .write_message(&mut writer)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    }
    buf.write_all(&temp)
}

pub fn write_jelly<W: Write>(
    buf: &mut W,
    triples: &[Triple],
) -> std::io::Result<()> {
    let mut encoder = JellyEncoder::new();

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
    write_delimit_frame(buf, options_frame)?;

    let mut current_rows: Vec<RdfStreamRow<'static>> = Vec::new();

    for t in triples {
        let triple_row = encoder.encode_triple(t);
        current_rows.extend(encoder.take_pending());
        current_rows.push(triple_row);

        if current_rows.len() >= JELLY_FRAME_SIZE {
            let frame = RdfStreamFrame {
                rows: std::mem::take(&mut current_rows),
                metadata: Default::default(),
            };
            write_delimit_frame(buf, frame)?;
        }
    }

    if !current_rows.is_empty() {
        let frame = RdfStreamFrame {
            rows: current_rows,
            metadata: Default::default(),
        };
        write_delimit_frame(buf, frame)?;
    }

    Ok(())
}
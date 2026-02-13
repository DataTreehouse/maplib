// Automatically generated rust module for 'rdf.proto' file

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_imports)]
#![allow(unknown_lints)]
#![allow(clippy::all)]
#![cfg_attr(rustfmt, rustfmt_skip)]


use std::borrow::Cow;
use std::collections::HashMap;
type KVMap<K, V> = HashMap<K, V>;
use quick_protobuf::{MessageInfo, MessageRead, MessageWrite, BytesReader, Writer, WriterBackend, Result};
use quick_protobuf::sizeofs::*;
use super::super::super::super::super::super::*;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PhysicalStreamType {
    PHYSICAL_STREAM_TYPE_UNSPECIFIED = 0,
    PHYSICAL_STREAM_TYPE_TRIPLES = 1,
    PHYSICAL_STREAM_TYPE_QUADS = 2,
    PHYSICAL_STREAM_TYPE_GRAPHS = 3,
}

impl Default for PhysicalStreamType {
    fn default() -> Self {
        PhysicalStreamType::PHYSICAL_STREAM_TYPE_UNSPECIFIED
    }
}

impl From<i32> for PhysicalStreamType {
    fn from(i: i32) -> Self {
        match i {
            0 => PhysicalStreamType::PHYSICAL_STREAM_TYPE_UNSPECIFIED,
            1 => PhysicalStreamType::PHYSICAL_STREAM_TYPE_TRIPLES,
            2 => PhysicalStreamType::PHYSICAL_STREAM_TYPE_QUADS,
            3 => PhysicalStreamType::PHYSICAL_STREAM_TYPE_GRAPHS,
            _ => Self::default(),
        }
    }
}

impl<'a> From<&'a str> for PhysicalStreamType {
    fn from(s: &'a str) -> Self {
        match s {
            "PHYSICAL_STREAM_TYPE_UNSPECIFIED" => PhysicalStreamType::PHYSICAL_STREAM_TYPE_UNSPECIFIED,
            "PHYSICAL_STREAM_TYPE_TRIPLES" => PhysicalStreamType::PHYSICAL_STREAM_TYPE_TRIPLES,
            "PHYSICAL_STREAM_TYPE_QUADS" => PhysicalStreamType::PHYSICAL_STREAM_TYPE_QUADS,
            "PHYSICAL_STREAM_TYPE_GRAPHS" => PhysicalStreamType::PHYSICAL_STREAM_TYPE_GRAPHS,
            _ => Self::default(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum LogicalStreamType {
    LOGICAL_STREAM_TYPE_UNSPECIFIED = 0,
    LOGICAL_STREAM_TYPE_FLAT_TRIPLES = 1,
    LOGICAL_STREAM_TYPE_FLAT_QUADS = 2,
    LOGICAL_STREAM_TYPE_GRAPHS = 3,
    LOGICAL_STREAM_TYPE_DATASETS = 4,
    LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS = 13,
    LOGICAL_STREAM_TYPE_NAMED_GRAPHS = 14,
    LOGICAL_STREAM_TYPE_TIMESTAMPED_NAMED_GRAPHS = 114,
}

impl Default for LogicalStreamType {
    fn default() -> Self {
        LogicalStreamType::LOGICAL_STREAM_TYPE_UNSPECIFIED
    }
}

impl From<i32> for LogicalStreamType {
    fn from(i: i32) -> Self {
        match i {
            0 => LogicalStreamType::LOGICAL_STREAM_TYPE_UNSPECIFIED,
            1 => LogicalStreamType::LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
            2 => LogicalStreamType::LOGICAL_STREAM_TYPE_FLAT_QUADS,
            3 => LogicalStreamType::LOGICAL_STREAM_TYPE_GRAPHS,
            4 => LogicalStreamType::LOGICAL_STREAM_TYPE_DATASETS,
            13 => LogicalStreamType::LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS,
            14 => LogicalStreamType::LOGICAL_STREAM_TYPE_NAMED_GRAPHS,
            114 => LogicalStreamType::LOGICAL_STREAM_TYPE_TIMESTAMPED_NAMED_GRAPHS,
            _ => Self::default(),
        }
    }
}

impl<'a> From<&'a str> for LogicalStreamType {
    fn from(s: &'a str) -> Self {
        match s {
            "LOGICAL_STREAM_TYPE_UNSPECIFIED" => LogicalStreamType::LOGICAL_STREAM_TYPE_UNSPECIFIED,
            "LOGICAL_STREAM_TYPE_FLAT_TRIPLES" => LogicalStreamType::LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
            "LOGICAL_STREAM_TYPE_FLAT_QUADS" => LogicalStreamType::LOGICAL_STREAM_TYPE_FLAT_QUADS,
            "LOGICAL_STREAM_TYPE_GRAPHS" => LogicalStreamType::LOGICAL_STREAM_TYPE_GRAPHS,
            "LOGICAL_STREAM_TYPE_DATASETS" => LogicalStreamType::LOGICAL_STREAM_TYPE_DATASETS,
            "LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS" => LogicalStreamType::LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS,
            "LOGICAL_STREAM_TYPE_NAMED_GRAPHS" => LogicalStreamType::LOGICAL_STREAM_TYPE_NAMED_GRAPHS,
            "LOGICAL_STREAM_TYPE_TIMESTAMPED_NAMED_GRAPHS" => LogicalStreamType::LOGICAL_STREAM_TYPE_TIMESTAMPED_NAMED_GRAPHS,
            _ => Self::default(),
        }
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct RdfIri {
    pub prefix_id: u32,
    pub name_id: u32,
}

impl<'a> MessageRead<'a> for RdfIri {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.prefix_id = r.read_uint32(bytes)?,
                Ok(16) => msg.name_id = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for RdfIri {
    fn get_size(&self) -> usize {
        0
        + if self.prefix_id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.prefix_id) as u64) }
        + if self.name_id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.name_id) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.prefix_id != 0u32 { w.write_with_tag(8, |w| w.write_uint32(*&self.prefix_id))?; }
        if self.name_id != 0u32 { w.write_with_tag(16, |w| w.write_uint32(*&self.name_id))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct RdfLiteral<'a> {
    pub lex: Cow<'a, str>,
    pub literalKind: eu::ostrzyciel::jelly::core::proto::v1::mod_RdfLiteral::OneOfliteralKind<'a>,
}

impl<'a> MessageRead<'a> for RdfLiteral<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.lex = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(18) => msg.literalKind = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfLiteral::OneOfliteralKind::langtag(r.read_string(bytes).map(Cow::Borrowed)?),
                Ok(24) => msg.literalKind = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfLiteral::OneOfliteralKind::datatype(r.read_uint32(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for RdfLiteral<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.lex == "" { 0 } else { 1 + sizeof_len((&self.lex).len()) }
        + match self.literalKind {
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfLiteral::OneOfliteralKind::langtag(ref m) => 1 + sizeof_len((m).len()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfLiteral::OneOfliteralKind::datatype(ref m) => 1 + sizeof_varint(*(m) as u64),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfLiteral::OneOfliteralKind::None => 0,
    }    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.lex != "" { w.write_with_tag(10, |w| w.write_string(&**&self.lex))?; }
        match self.literalKind {            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfLiteral::OneOfliteralKind::langtag(ref m) => { w.write_with_tag(18, |w| w.write_string(&**m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfLiteral::OneOfliteralKind::datatype(ref m) => { w.write_with_tag(24, |w| w.write_uint32(*m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfLiteral::OneOfliteralKind::None => {},
    }        Ok(())
    }
}

pub mod mod_RdfLiteral {

use super::*;

#[derive(Debug, PartialEq, Clone)]
pub enum OneOfliteralKind<'a> {
    langtag(Cow<'a, str>),
    datatype(u32),
    None,
}

impl<'a> Default for OneOfliteralKind<'a> {
    fn default() -> Self {
        OneOfliteralKind::None
    }
}

}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct RdfDefaultGraph { }

impl<'a> MessageRead<'a> for RdfDefaultGraph {
    fn from_reader(r: &mut BytesReader, _: &[u8]) -> Result<Self> {
        r.read_to_end();
        Ok(Self::default())
    }
}

impl MessageWrite for RdfDefaultGraph { }

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct RdfTriple<'a> {
    pub subject: eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfsubject<'a>,
    pub predicate: eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfpredicate<'a>,
    pub object: eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfobject<'a>,
}

impl<'a> MessageRead<'a> for RdfTriple<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.subject = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfsubject::s_iri(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfIri>(bytes)?),
                Ok(18) => msg.subject = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfsubject::s_bnode(r.read_string(bytes).map(Cow::Borrowed)?),
                Ok(26) => msg.subject = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfsubject::s_literal(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfLiteral>(bytes)?),
                Ok(34) => msg.subject = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfsubject::s_triple_term(Box::new(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfTriple>(bytes)?)),
                Ok(42) => msg.predicate = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfpredicate::p_iri(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfIri>(bytes)?),
                Ok(50) => msg.predicate = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfpredicate::p_bnode(r.read_string(bytes).map(Cow::Borrowed)?),
                Ok(58) => msg.predicate = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfpredicate::p_literal(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfLiteral>(bytes)?),
                Ok(66) => msg.predicate = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfpredicate::p_triple_term(Box::new(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfTriple>(bytes)?)),
                Ok(74) => msg.object = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfobject::o_iri(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfIri>(bytes)?),
                Ok(82) => msg.object = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfobject::o_bnode(r.read_string(bytes).map(Cow::Borrowed)?),
                Ok(90) => msg.object = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfobject::o_literal(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfLiteral>(bytes)?),
                Ok(98) => msg.object = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfobject::o_triple_term(Box::new(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfTriple>(bytes)?)),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for RdfTriple<'a> {
    fn get_size(&self) -> usize {
        0
        + match self.subject {
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfsubject::s_iri(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfsubject::s_bnode(ref m) => 1 + sizeof_len((m).len()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfsubject::s_literal(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfsubject::s_triple_term(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfsubject::None => 0,
    }        + match self.predicate {
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfpredicate::p_iri(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfpredicate::p_bnode(ref m) => 1 + sizeof_len((m).len()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfpredicate::p_literal(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfpredicate::p_triple_term(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfpredicate::None => 0,
    }        + match self.object {
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfobject::o_iri(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfobject::o_bnode(ref m) => 1 + sizeof_len((m).len()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfobject::o_literal(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfobject::o_triple_term(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfobject::None => 0,
    }    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        match self.subject {            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfsubject::s_iri(ref m) => { w.write_with_tag(10, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfsubject::s_bnode(ref m) => { w.write_with_tag(18, |w| w.write_string(&**m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfsubject::s_literal(ref m) => { w.write_with_tag(26, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfsubject::s_triple_term(ref m) => { w.write_with_tag(34, |w| w.write_message(&**m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfsubject::None => {},
    }        match self.predicate {            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfpredicate::p_iri(ref m) => { w.write_with_tag(42, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfpredicate::p_bnode(ref m) => { w.write_with_tag(50, |w| w.write_string(&**m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfpredicate::p_literal(ref m) => { w.write_with_tag(58, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfpredicate::p_triple_term(ref m) => { w.write_with_tag(66, |w| w.write_message(&**m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfpredicate::None => {},
    }        match self.object {            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfobject::o_iri(ref m) => { w.write_with_tag(74, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfobject::o_bnode(ref m) => { w.write_with_tag(82, |w| w.write_string(&**m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfobject::o_literal(ref m) => { w.write_with_tag(90, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfobject::o_triple_term(ref m) => { w.write_with_tag(98, |w| w.write_message(&**m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfTriple::OneOfobject::None => {},
    }        Ok(())
    }
}

pub mod mod_RdfTriple {

use super::*;

#[derive(Debug, PartialEq, Clone)]
pub enum OneOfsubject<'a> {
    s_iri(eu::ostrzyciel::jelly::core::proto::v1::RdfIri),
    s_bnode(Cow<'a, str>),
    s_literal(eu::ostrzyciel::jelly::core::proto::v1::RdfLiteral<'a>),
    s_triple_term(Box<eu::ostrzyciel::jelly::core::proto::v1::RdfTriple<'a>>),
    None,
}

impl<'a> Default for OneOfsubject<'a> {
    fn default() -> Self {
        OneOfsubject::None
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum OneOfpredicate<'a> {
    p_iri(eu::ostrzyciel::jelly::core::proto::v1::RdfIri),
    p_bnode(Cow<'a, str>),
    p_literal(eu::ostrzyciel::jelly::core::proto::v1::RdfLiteral<'a>),
    p_triple_term(Box<eu::ostrzyciel::jelly::core::proto::v1::RdfTriple<'a>>),
    None,
}

impl<'a> Default for OneOfpredicate<'a> {
    fn default() -> Self {
        OneOfpredicate::None
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum OneOfobject<'a> {
    o_iri(eu::ostrzyciel::jelly::core::proto::v1::RdfIri),
    o_bnode(Cow<'a, str>),
    o_literal(eu::ostrzyciel::jelly::core::proto::v1::RdfLiteral<'a>),
    o_triple_term(Box<eu::ostrzyciel::jelly::core::proto::v1::RdfTriple<'a>>),
    None,
}

impl<'a> Default for OneOfobject<'a> {
    fn default() -> Self {
        OneOfobject::None
    }
}

}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct RdfQuad<'a> {
    pub subject: eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfsubject<'a>,
    pub predicate: eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfpredicate<'a>,
    pub object: eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfobject<'a>,
    pub graph: eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfgraph<'a>,
}

impl<'a> MessageRead<'a> for RdfQuad<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.subject = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfsubject::s_iri(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfIri>(bytes)?),
                Ok(18) => msg.subject = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfsubject::s_bnode(r.read_string(bytes).map(Cow::Borrowed)?),
                Ok(26) => msg.subject = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfsubject::s_literal(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfLiteral>(bytes)?),
                Ok(34) => msg.subject = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfsubject::s_triple_term(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfTriple>(bytes)?),
                Ok(42) => msg.predicate = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfpredicate::p_iri(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfIri>(bytes)?),
                Ok(50) => msg.predicate = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfpredicate::p_bnode(r.read_string(bytes).map(Cow::Borrowed)?),
                Ok(58) => msg.predicate = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfpredicate::p_literal(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfLiteral>(bytes)?),
                Ok(66) => msg.predicate = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfpredicate::p_triple_term(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfTriple>(bytes)?),
                Ok(74) => msg.object = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfobject::o_iri(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfIri>(bytes)?),
                Ok(82) => msg.object = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfobject::o_bnode(r.read_string(bytes).map(Cow::Borrowed)?),
                Ok(90) => msg.object = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfobject::o_literal(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfLiteral>(bytes)?),
                Ok(98) => msg.object = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfobject::o_triple_term(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfTriple>(bytes)?),
                Ok(106) => msg.graph = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfgraph::g_iri(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfIri>(bytes)?),
                Ok(114) => msg.graph = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfgraph::g_bnode(r.read_string(bytes).map(Cow::Borrowed)?),
                Ok(122) => msg.graph = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfgraph::g_default_graph(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfDefaultGraph>(bytes)?),
                Ok(130) => msg.graph = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfgraph::g_literal(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfLiteral>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for RdfQuad<'a> {
    fn get_size(&self) -> usize {
        0
        + match self.subject {
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfsubject::s_iri(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfsubject::s_bnode(ref m) => 1 + sizeof_len((m).len()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfsubject::s_literal(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfsubject::s_triple_term(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfsubject::None => 0,
    }        + match self.predicate {
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfpredicate::p_iri(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfpredicate::p_bnode(ref m) => 1 + sizeof_len((m).len()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfpredicate::p_literal(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfpredicate::p_triple_term(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfpredicate::None => 0,
    }        + match self.object {
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfobject::o_iri(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfobject::o_bnode(ref m) => 1 + sizeof_len((m).len()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfobject::o_literal(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfobject::o_triple_term(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfobject::None => 0,
    }        + match self.graph {
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfgraph::g_iri(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfgraph::g_bnode(ref m) => 1 + sizeof_len((m).len()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfgraph::g_default_graph(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfgraph::g_literal(ref m) => 2 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfgraph::None => 0,
    }    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        match self.subject {            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfsubject::s_iri(ref m) => { w.write_with_tag(10, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfsubject::s_bnode(ref m) => { w.write_with_tag(18, |w| w.write_string(&**m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfsubject::s_literal(ref m) => { w.write_with_tag(26, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfsubject::s_triple_term(ref m) => { w.write_with_tag(34, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfsubject::None => {},
    }        match self.predicate {            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfpredicate::p_iri(ref m) => { w.write_with_tag(42, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfpredicate::p_bnode(ref m) => { w.write_with_tag(50, |w| w.write_string(&**m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfpredicate::p_literal(ref m) => { w.write_with_tag(58, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfpredicate::p_triple_term(ref m) => { w.write_with_tag(66, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfpredicate::None => {},
    }        match self.object {            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfobject::o_iri(ref m) => { w.write_with_tag(74, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfobject::o_bnode(ref m) => { w.write_with_tag(82, |w| w.write_string(&**m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfobject::o_literal(ref m) => { w.write_with_tag(90, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfobject::o_triple_term(ref m) => { w.write_with_tag(98, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfobject::None => {},
    }        match self.graph {            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfgraph::g_iri(ref m) => { w.write_with_tag(106, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfgraph::g_bnode(ref m) => { w.write_with_tag(114, |w| w.write_string(&**m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfgraph::g_default_graph(ref m) => { w.write_with_tag(122, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfgraph::g_literal(ref m) => { w.write_with_tag(130, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfQuad::OneOfgraph::None => {},
    }        Ok(())
    }
}

pub mod mod_RdfQuad {

use super::*;

#[derive(Debug, PartialEq, Clone)]
pub enum OneOfsubject<'a> {
    s_iri(eu::ostrzyciel::jelly::core::proto::v1::RdfIri),
    s_bnode(Cow<'a, str>),
    s_literal(eu::ostrzyciel::jelly::core::proto::v1::RdfLiteral<'a>),
    s_triple_term(eu::ostrzyciel::jelly::core::proto::v1::RdfTriple<'a>),
    None,
}

impl<'a> Default for OneOfsubject<'a> {
    fn default() -> Self {
        OneOfsubject::None
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum OneOfpredicate<'a> {
    p_iri(eu::ostrzyciel::jelly::core::proto::v1::RdfIri),
    p_bnode(Cow<'a, str>),
    p_literal(eu::ostrzyciel::jelly::core::proto::v1::RdfLiteral<'a>),
    p_triple_term(eu::ostrzyciel::jelly::core::proto::v1::RdfTriple<'a>),
    None,
}

impl<'a> Default for OneOfpredicate<'a> {
    fn default() -> Self {
        OneOfpredicate::None
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum OneOfobject<'a> {
    o_iri(eu::ostrzyciel::jelly::core::proto::v1::RdfIri),
    o_bnode(Cow<'a, str>),
    o_literal(eu::ostrzyciel::jelly::core::proto::v1::RdfLiteral<'a>),
    o_triple_term(eu::ostrzyciel::jelly::core::proto::v1::RdfTriple<'a>),
    None,
}

impl<'a> Default for OneOfobject<'a> {
    fn default() -> Self {
        OneOfobject::None
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum OneOfgraph<'a> {
    g_iri(eu::ostrzyciel::jelly::core::proto::v1::RdfIri),
    g_bnode(Cow<'a, str>),
    g_default_graph(eu::ostrzyciel::jelly::core::proto::v1::RdfDefaultGraph),
    g_literal(eu::ostrzyciel::jelly::core::proto::v1::RdfLiteral<'a>),
    None,
}

impl<'a> Default for OneOfgraph<'a> {
    fn default() -> Self {
        OneOfgraph::None
    }
}

}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct RdfGraphStart<'a> {
    pub graph: eu::ostrzyciel::jelly::core::proto::v1::mod_RdfGraphStart::OneOfgraph<'a>,
}

impl<'a> MessageRead<'a> for RdfGraphStart<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.graph = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfGraphStart::OneOfgraph::g_iri(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfIri>(bytes)?),
                Ok(18) => msg.graph = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfGraphStart::OneOfgraph::g_bnode(r.read_string(bytes).map(Cow::Borrowed)?),
                Ok(26) => msg.graph = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfGraphStart::OneOfgraph::g_default_graph(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfDefaultGraph>(bytes)?),
                Ok(34) => msg.graph = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfGraphStart::OneOfgraph::g_literal(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfLiteral>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for RdfGraphStart<'a> {
    fn get_size(&self) -> usize {
        0
        + match self.graph {
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfGraphStart::OneOfgraph::g_iri(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfGraphStart::OneOfgraph::g_bnode(ref m) => 1 + sizeof_len((m).len()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfGraphStart::OneOfgraph::g_default_graph(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfGraphStart::OneOfgraph::g_literal(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfGraphStart::OneOfgraph::None => 0,
    }    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        match self.graph {            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfGraphStart::OneOfgraph::g_iri(ref m) => { w.write_with_tag(10, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfGraphStart::OneOfgraph::g_bnode(ref m) => { w.write_with_tag(18, |w| w.write_string(&**m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfGraphStart::OneOfgraph::g_default_graph(ref m) => { w.write_with_tag(26, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfGraphStart::OneOfgraph::g_literal(ref m) => { w.write_with_tag(34, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfGraphStart::OneOfgraph::None => {},
    }        Ok(())
    }
}

pub mod mod_RdfGraphStart {

use super::*;

#[derive(Debug, PartialEq, Clone)]
pub enum OneOfgraph<'a> {
    g_iri(eu::ostrzyciel::jelly::core::proto::v1::RdfIri),
    g_bnode(Cow<'a, str>),
    g_default_graph(eu::ostrzyciel::jelly::core::proto::v1::RdfDefaultGraph),
    g_literal(eu::ostrzyciel::jelly::core::proto::v1::RdfLiteral<'a>),
    None,
}

impl<'a> Default for OneOfgraph<'a> {
    fn default() -> Self {
        OneOfgraph::None
    }
}

}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct RdfGraphEnd { }

impl<'a> MessageRead<'a> for RdfGraphEnd {
    fn from_reader(r: &mut BytesReader, _: &[u8]) -> Result<Self> {
        r.read_to_end();
        Ok(Self::default())
    }
}

impl MessageWrite for RdfGraphEnd { }

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct RdfNamespaceDeclaration<'a> {
    pub name: Cow<'a, str>,
    pub value: Option<eu::ostrzyciel::jelly::core::proto::v1::RdfIri>,
}

impl<'a> MessageRead<'a> for RdfNamespaceDeclaration<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.name = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(18) => msg.value = Some(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfIri>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for RdfNamespaceDeclaration<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.name == "" { 0 } else { 1 + sizeof_len((&self.name).len()) }
        + self.value.as_ref().map_or(0, |m| 1 + sizeof_len((m).get_size()))
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.name != "" { w.write_with_tag(10, |w| w.write_string(&**&self.name))?; }
        if let Some(ref s) = self.value { w.write_with_tag(18, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct RdfNameEntry<'a> {
    pub id: u32,
    pub value: Cow<'a, str>,
}

impl<'a> MessageRead<'a> for RdfNameEntry<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.id = r.read_uint32(bytes)?,
                Ok(18) => msg.value = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for RdfNameEntry<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.id) as u64) }
        + if self.value == "" { 0 } else { 1 + sizeof_len((&self.value).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u32 { w.write_with_tag(8, |w| w.write_uint32(*&self.id))?; }
        if self.value != "" { w.write_with_tag(18, |w| w.write_string(&**&self.value))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct RdfPrefixEntry<'a> {
    pub id: u32,
    pub value: Cow<'a, str>,
}

impl<'a> MessageRead<'a> for RdfPrefixEntry<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.id = r.read_uint32(bytes)?,
                Ok(18) => msg.value = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for RdfPrefixEntry<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.id) as u64) }
        + if self.value == "" { 0 } else { 1 + sizeof_len((&self.value).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u32 { w.write_with_tag(8, |w| w.write_uint32(*&self.id))?; }
        if self.value != "" { w.write_with_tag(18, |w| w.write_string(&**&self.value))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct RdfDatatypeEntry<'a> {
    pub id: u32,
    pub value: Cow<'a, str>,
}

impl<'a> MessageRead<'a> for RdfDatatypeEntry<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.id = r.read_uint32(bytes)?,
                Ok(18) => msg.value = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for RdfDatatypeEntry<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.id) as u64) }
        + if self.value == "" { 0 } else { 1 + sizeof_len((&self.value).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u32 { w.write_with_tag(8, |w| w.write_uint32(*&self.id))?; }
        if self.value != "" { w.write_with_tag(18, |w| w.write_string(&**&self.value))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct RdfStreamOptions<'a> {
    pub stream_name: Cow<'a, str>,
    pub physical_type: eu::ostrzyciel::jelly::core::proto::v1::PhysicalStreamType,
    pub generalized_statements: bool,
    pub rdf_star: bool,
    pub max_name_table_size: u32,
    pub max_prefix_table_size: u32,
    pub max_datatype_table_size: u32,
    pub logical_type: eu::ostrzyciel::jelly::core::proto::v1::LogicalStreamType,
    pub version: u32,
}

impl<'a> MessageRead<'a> for RdfStreamOptions<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.stream_name = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(16) => msg.physical_type = r.read_enum(bytes)?,
                Ok(24) => msg.generalized_statements = r.read_bool(bytes)?,
                Ok(32) => msg.rdf_star = r.read_bool(bytes)?,
                Ok(72) => msg.max_name_table_size = r.read_uint32(bytes)?,
                Ok(80) => msg.max_prefix_table_size = r.read_uint32(bytes)?,
                Ok(88) => msg.max_datatype_table_size = r.read_uint32(bytes)?,
                Ok(112) => msg.logical_type = r.read_enum(bytes)?,
                Ok(120) => msg.version = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for RdfStreamOptions<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.stream_name == "" { 0 } else { 1 + sizeof_len((&self.stream_name).len()) }
        + if self.physical_type == eu::ostrzyciel::jelly::core::proto::v1::PhysicalStreamType::PHYSICAL_STREAM_TYPE_UNSPECIFIED { 0 } else { 1 + sizeof_varint(*(&self.physical_type) as u64) }
        + if self.generalized_statements == false { 0 } else { 1 + sizeof_varint(*(&self.generalized_statements) as u64) }
        + if self.rdf_star == false { 0 } else { 1 + sizeof_varint(*(&self.rdf_star) as u64) }
        + if self.max_name_table_size == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.max_name_table_size) as u64) }
        + if self.max_prefix_table_size == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.max_prefix_table_size) as u64) }
        + if self.max_datatype_table_size == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.max_datatype_table_size) as u64) }
        + if self.logical_type == eu::ostrzyciel::jelly::core::proto::v1::LogicalStreamType::LOGICAL_STREAM_TYPE_UNSPECIFIED { 0 } else { 1 + sizeof_varint(*(&self.logical_type) as u64) }
        + if self.version == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.version) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.stream_name != "" { w.write_with_tag(10, |w| w.write_string(&**&self.stream_name))?; }
        if self.physical_type != eu::ostrzyciel::jelly::core::proto::v1::PhysicalStreamType::PHYSICAL_STREAM_TYPE_UNSPECIFIED { w.write_with_tag(16, |w| w.write_enum(*&self.physical_type as i32))?; }
        if self.generalized_statements != false { w.write_with_tag(24, |w| w.write_bool(*&self.generalized_statements))?; }
        if self.rdf_star != false { w.write_with_tag(32, |w| w.write_bool(*&self.rdf_star))?; }
        if self.max_name_table_size != 0u32 { w.write_with_tag(72, |w| w.write_uint32(*&self.max_name_table_size))?; }
        if self.max_prefix_table_size != 0u32 { w.write_with_tag(80, |w| w.write_uint32(*&self.max_prefix_table_size))?; }
        if self.max_datatype_table_size != 0u32 { w.write_with_tag(88, |w| w.write_uint32(*&self.max_datatype_table_size))?; }
        if self.logical_type != eu::ostrzyciel::jelly::core::proto::v1::LogicalStreamType::LOGICAL_STREAM_TYPE_UNSPECIFIED { w.write_with_tag(112, |w| w.write_enum(*&self.logical_type as i32))?; }
        if self.version != 0u32 { w.write_with_tag(120, |w| w.write_uint32(*&self.version))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct RdfStreamRow<'a> {
    pub row: eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow<'a>,
}

impl<'a> MessageRead<'a> for RdfStreamRow<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.row = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::options(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfStreamOptions>(bytes)?),
                Ok(18) => msg.row = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::triple(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfTriple>(bytes)?),
                Ok(26) => msg.row = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::quad(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfQuad>(bytes)?),
                Ok(34) => msg.row = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::graph_start(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfGraphStart>(bytes)?),
                Ok(42) => msg.row = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::graph_end(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfGraphEnd>(bytes)?),
                Ok(50) => msg.row = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::namespace(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfNamespaceDeclaration>(bytes)?),
                Ok(74) => msg.row = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::name(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfNameEntry>(bytes)?),
                Ok(82) => msg.row = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::prefix(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfPrefixEntry>(bytes)?),
                Ok(90) => msg.row = eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::datatype(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfDatatypeEntry>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for RdfStreamRow<'a> {
    fn get_size(&self) -> usize {
        0
        + match self.row {
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::options(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::triple(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::quad(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::graph_start(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::graph_end(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::namespace(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::name(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::prefix(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::datatype(ref m) => 1 + sizeof_len((m).get_size()),
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::None => 0,
    }    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        match self.row {            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::options(ref m) => { w.write_with_tag(10, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::triple(ref m) => { w.write_with_tag(18, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::quad(ref m) => { w.write_with_tag(26, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::graph_start(ref m) => { w.write_with_tag(34, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::graph_end(ref m) => { w.write_with_tag(42, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::namespace(ref m) => { w.write_with_tag(50, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::name(ref m) => { w.write_with_tag(74, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::prefix(ref m) => { w.write_with_tag(82, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::datatype(ref m) => { w.write_with_tag(90, |w| w.write_message(m))? },
            eu::ostrzyciel::jelly::core::proto::v1::mod_RdfStreamRow::OneOfrow::None => {},
    }        Ok(())
    }
}

pub mod mod_RdfStreamRow {

use super::*;

#[derive(Debug, PartialEq, Clone)]
pub enum OneOfrow<'a> {
    options(eu::ostrzyciel::jelly::core::proto::v1::RdfStreamOptions<'a>),
    triple(eu::ostrzyciel::jelly::core::proto::v1::RdfTriple<'a>),
    quad(eu::ostrzyciel::jelly::core::proto::v1::RdfQuad<'a>),
    graph_start(eu::ostrzyciel::jelly::core::proto::v1::RdfGraphStart<'a>),
    graph_end(eu::ostrzyciel::jelly::core::proto::v1::RdfGraphEnd),
    namespace(eu::ostrzyciel::jelly::core::proto::v1::RdfNamespaceDeclaration<'a>),
    name(eu::ostrzyciel::jelly::core::proto::v1::RdfNameEntry<'a>),
    prefix(eu::ostrzyciel::jelly::core::proto::v1::RdfPrefixEntry<'a>),
    datatype(eu::ostrzyciel::jelly::core::proto::v1::RdfDatatypeEntry<'a>),
    None,
}

impl<'a> Default for OneOfrow<'a> {
    fn default() -> Self {
        OneOfrow::None
    }
}

}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct RdfStreamFrame<'a> {
    pub rows: Vec<eu::ostrzyciel::jelly::core::proto::v1::RdfStreamRow<'a>>,
    pub metadata: KVMap<Cow<'a, str>, Cow<'a, [u8]>>,
}

impl<'a> MessageRead<'a> for RdfStreamFrame<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.rows.push(r.read_message::<eu::ostrzyciel::jelly::core::proto::v1::RdfStreamRow>(bytes)?),
                Ok(122) => {
                    let (key, value) = r.read_map(bytes, |r, bytes| Ok(r.read_string(bytes).map(Cow::Borrowed)?), |r, bytes| Ok(r.read_bytes(bytes).map(Cow::Borrowed)?))?;
                    msg.metadata.insert(key, value);
                }
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for RdfStreamFrame<'a> {
    fn get_size(&self) -> usize {
        0
        + self.rows.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
        + self.metadata.iter().map(|(k, v)| 1 + sizeof_len(2 + sizeof_len((k).len()) + sizeof_len((v).len()))).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.rows { w.write_with_tag(10, |w| w.write_message(s))?; }
        for (k, v) in self.metadata.iter() { w.write_with_tag(122, |w| w.write_map(2 + sizeof_len((k).len()) + sizeof_len((v).len()), 10, |w| w.write_string(&**k), 18, |w| w.write_bytes(&**v)))?; }
        Ok(())
    }
}


//! HDT (Header Dictionary Triples) reading and writing, vendored into maplib from the
//! [hdt crate](https://github.com/KonradHoeffner/hdt) 0.7.3 (MIT licensed).
//!
//! Maplib additions are scan-oriented: reading a file without building the query indexes
//! ([`hdt::HdtScan`]), decoding dictionary sections block by block
//! ([`dict_sect_pfc::DictSectPFC::decode_blocks`]), and writing the triples section without
//! building them ([`triples::write_bitmap_triples`]).
//#![warn(missing_docs)] //TODO: comment again after refactoring
#![warn(clippy::pedantic)]
#![warn(clippy::str_to_string)]
#![warn(clippy::print_stdout)]
#![warn(clippy::print_stderr)]
#![warn(clippy::missing_const_for_fn)]
#![allow(clippy::unnecessary_cast)]
#![allow(clippy::enum_glob_use)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::wildcard_imports)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::similar_names)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::if_not_else)]
#![allow(clippy::into_iter_without_iter)]
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::unsafe_derive_deserialize)]
// multiple versions of syn crate in transitive dependencies
#![allow(clippy::multiple_crate_versions)]
/// Types for storing and reading data.
pub mod containers;
/// Types for representing dictionaries.
pub mod dict_sect_pfc;
/// Types for representing a four section dictionary
pub mod four_sect_dict;
/// Types for representing triple sections.
pub mod hdt;
/// Types for representing the header.
pub mod header;
/// Types for representing and querying triples.
pub mod triples;
/// Constants for triple terms
pub mod vocab;

pub use crate::hdt::Hdt;
use containers::ControlInfo;
use dict_sect_pfc::DictSectPFC;
use four_sect_dict::FourSectDict;
pub use four_sect_dict::IdKind;

#![cfg_attr(doc, doc = include_str!("../README.md"))]
#![doc(test(attr(deny(warnings))))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc(html_favicon_url = "https://raw.githubusercontent.com/oxigraph/oxigraph/main/logo.svg")]
#![doc(html_logo_url = "https://raw.githubusercontent.com/oxigraph/oxigraph/main/logo.svg")]

mod error;
mod parallel;
mod parser;
mod serializer;
mod utils;

pub use error::{RdfXmlParseError, RdfXmlSyntaxError};
pub use parallel::split_slice_for_parallel_parsing;
#[cfg(feature = "async-tokio")]
pub use parser::TokioAsyncReaderRdfXmlParser;
pub use parser::{
    DEFAULT_ENTITY_AMPLIFICATION_FACTOR, DEFAULT_MAX_ENTITY_EXPANSION,
    DEFAULT_MAX_TOTAL_ENTITY_EXPANSION, RdfXmlParser, RdfXmlPrefixesIter, ReaderRdfXmlParser,
    SliceRdfXmlParser,
};
#[cfg(feature = "async-tokio")]
pub use serializer::TokioAsyncWriterRdfXmlSerializer;
pub use serializer::{RdfXmlSerializer, WriterRdfXmlSerializer};

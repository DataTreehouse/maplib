use crate::containers::ControlInfo;
use crate::containers::rdf::{Id, Literal, Term, Triple};
use ntriple::parser::triple_line;
use std::collections::BTreeSet;
use std::io::{BufRead, Write};
use std::str;

pub type Result<T> = core::result::Result<T, Error>;

/// Metadata about the dataset, see <https://www.rdfhdt.org/hdt-binary-format/#header>.
#[derive(Debug, Clone)]
pub struct Header {
    /// Header data format. Only "ntriples" is supported.
    pub format: String,
    /// The number of bytes of the header data.
    pub length: usize,
    /// Triples describing the dataset.
    pub body: BTreeSet<Triple>,
}

/// The error type for the `read` method.
#[derive(thiserror::Error, Debug)]
#[error("failed to read HDT header")]
pub enum Error {
    #[error("{0}")]
    Other(String),
    Io(#[from] std::io::Error),
    ControlInfo(#[from] crate::containers::control_info::Error),
    #[error("invalid header format {0}, only 'ntriples' is supported")]
    InvalidHeaderFormat(String),
    #[error("invalid header length '{0}'")]
    InvalidHeaderLength(String),
    #[error("missing header length")]
    MissingHeaderLength,
}

impl Header {
    /// Reader needs to be positioned directly after the global control information.
    pub fn read<R: BufRead>(reader: &mut R) -> Result<Self> {
        let header_ci = ControlInfo::read(reader)?;
        if header_ci.format != "ntriples" {
            return Err(Error::InvalidHeaderFormat(header_ci.format));
        }

        let ls = header_ci.get("length").ok_or(Error::MissingHeaderLength)?;
        let length = ls.parse::<usize>().map_err(|_| Error::InvalidHeaderLength(ls))?;

        let mut body_buffer: Vec<u8> = vec![0; length];
        reader.read_exact(&mut body_buffer)?;
        let mut body = BTreeSet::new();

        for line_slice in body_buffer.split(|b| b == &b'\n') {
            let line = str::from_utf8(line_slice).map_err(|_| Error::Other("Header is not UTF-8".to_owned()))?;
            if let Ok(Some(triple)) = triple_line(line) {
                let subject = match triple.subject {
                    ntriple::Subject::IriRef(iri) => Id::Named(iri),
                    ntriple::Subject::BNode(id) => Id::Blank(id),
                };

                let ntriple::Predicate::IriRef(predicate) = triple.predicate;

                let object = match triple.object {
                    ntriple::Object::IriRef(iri) => Term::Id(Id::Named(iri)),
                    ntriple::Object::BNode(id) => Term::Id(Id::Blank(id)),
                    ntriple::Object::Lit(lit) => Term::Literal(match lit.data_type {
                        ntriple::TypeLang::Lang(lan) => Literal::new_lang(lit.data, lan),
                        ntriple::TypeLang::Type(data_type) => {
                            // workaround incorrect https in xsd prefix in ntriples dependency
                            if data_type == "http://www.w3.org/2001/XMLSchema#string"
                                || data_type == "https://www.w3.org/2001/XMLSchema#string"
                            {
                                Literal::new(lit.data)
                            } else {
                                Literal::new_typed(lit.data, data_type)
                            }
                        }
                    }),
                };

                body.insert(Triple::new(subject, predicate, object));
            }
        }
        Ok(Header { format: header_ci.format, length, body })
    }

    pub fn write(&self, write: &mut impl Write) -> Result<()> {
        let mut body = Vec::<u8>::new();
        for triple in &self.body {
            writeln!(&mut body, "{triple}")?;
        }
        ControlInfo::header(body.len()).write(write)?;
        write.write_all(&body)?;
        Ok(())
    }
}

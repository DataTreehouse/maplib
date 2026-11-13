#![allow(missing_docs)]
// temporary while we figure out what should be public in the end
/// Four section dictionary.
use crate::dict_sect_pfc;
use crate::triples::Id;
use crate::{ControlInfo, DictSectPFC};
use std::io::BufRead;
#[cfg(not(any(target_arch = "wasm32", target_arch = "wasm64")))]
use std::thread::JoinHandle;
use thiserror::Error;

pub type Result<T> = core::result::Result<T, Error>;

/// Position in an RDF triple.
#[derive(Debug, Clone, Copy)]
pub enum IdKind {
    /// IRI or blank node in the first position of a triple.
    Subject,
    /// IRI in the second position of a triple.
    Predicate,
    /// IRI, blank node or literal in the third position of a triple.
    Object,
}

impl IdKind {
    pub const KINDS: [IdKind; 3] = [IdKind::Subject, IdKind::Predicate, IdKind::Object];
}

/// Four section dictionary with plain front coding.
/// Dictionary with shared, subject, predicate and object sections.
/// Types specified as <http://purl.org/HDT/hdt#dictionaryFour>.
/// See <https://www.rdfhdt.org/hdt-internals/#dictionary>.
#[cfg_attr(test, derive(PartialEq))]
#[derive(Debug)]
pub struct FourSectDict {
    /// The shared section contains URIs that occur both in subject and object position. Its IDs start at one.
    pub shared: DictSectPFC,
    /// URIs that only occur as subjects. Their IDs start at the last ID of the shared section + 1.
    pub subjects: DictSectPFC,
    /// The predicate section has its own separate numbering starting from 1.
    pub predicates: DictSectPFC,
    /// URIs and literals that only occur as objects . Their IDs start at the last ID of the shared section + 1.
    pub objects: DictSectPFC,
}

/// Designates one of the four sections.
#[derive(Debug)]
pub enum SectKind {
    /// section for terms that appear as both subject and object
    Shared,
    /// section for terms that only appear as subjects
    Subject,
    /// section for terms that only appear as predicates
    Predicate,
    /// sections for terms that only appear as objects
    Object,
}

/// Wraps an extraction error with additional information on which dictionary section it occurred in.
#[derive(Error, Debug)]
#[error("four sect dict error id_to_string({id},IdKind::{id_kind:?}) in the {sect_kind:?} section, caused by {e}")]
pub struct ExtractError {
    #[source]
    e: dict_sect_pfc::ExtractError,
    id: Id,
    id_kind: IdKind,
    sect_kind: SectKind,
}

#[derive(Error, Debug)]
#[error("four sect dict section error in the {sect_kind:?} section")]
pub struct DictSectError {
    #[source]
    e: dict_sect_pfc::Error,
    sect_kind: SectKind,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("failed to read FourSectDict control info")]
    ControlInfo(#[from] crate::containers::control_info::Error),
    #[error("failed to read FourSectDict section")]
    DictSect(#[from] DictSectError),
    #[error("failed to read FourSectDict: {0}")]
    Other(String),
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct EncodedTripleId {
    pub subject: Id,
    pub predicate: Id,
    pub object: Id,
}

impl FourSectDict {
    /// Get the string value of a given ID of a given type.
    /// String representation of URIs, literals and blank nodes is defined in <https://www.w3.org/Submission/2011/SUBM-HDT-20110330/#dictionaryEncoding>>..
    pub fn id_to_string(&self, id: Id, id_kind: IdKind) -> core::result::Result<String, ExtractError> {
        use SectKind::*;
        let shared_size = self.shared.num_strings() as Id;
        let d = id.saturating_sub(shared_size);
        match id_kind {
            IdKind::Subject => {
                if id <= shared_size {
                    self.shared.extract(id).map_err(|e| ExtractError { e, id, id_kind, sect_kind: Shared })
                } else {
                    self.subjects.extract(d).map_err(|e| ExtractError { e, id, id_kind, sect_kind: Subject })
                }
            }
            IdKind::Predicate => {
                self.predicates.extract(id).map_err(|e| ExtractError { e, id, id_kind, sect_kind: Predicate })
            }
            IdKind::Object => {
                if id <= shared_size {
                    self.shared.extract(id).map_err(|e| ExtractError { e, id, id_kind, sect_kind: Shared })
                } else {
                    self.objects.extract(d).map_err(|e| ExtractError { e, id, id_kind, sect_kind: Object })
                }
            }
        }
    }

    /// Get the ID for a given string or 0 if not found.
    /// String representation of URIs, literals and blank nodes is defined in <https://www.w3.org/Submission/2011/SUBM-HDT-20110330/#dictionaryEncoding>>..
    pub fn string_to_id(&self, s: &str, id_kind: IdKind) -> Id {
        let shared_size = self.shared.num_strings();
        match id_kind {
            IdKind::Subject => {
                let mut id = self.shared.string_to_id(s);
                if id == 0 {
                    id = self.subjects.string_to_id(s);
                    if id > 0 {
                        id += shared_size as Id;
                    }
                }
                id
            }
            IdKind::Predicate => self.predicates.string_to_id(s),
            IdKind::Object => {
                let mut id = self.shared.string_to_id(s);
                if id == 0 {
                    id = self.objects.string_to_id(s);
                    if id > 0 {
                        id += shared_size as Id;
                    }
                }
                id
            }
        }
    }

    /// read the whole dictionary section including control information
    pub fn read<R: BufRead>(reader: &mut R) -> Result<UnvalidatedFourSectDict> {
        use SectKind::*;
        let dict_ci = ControlInfo::read(reader)?;
        if dict_ci.format != "<http://purl.org/HDT/hdt#dictionaryFour>" {
            return Err(Error::Other("Implementation only supports four section dictionaries".to_owned()));
        }
        let mut f = |sect_kind| DictSectPFC::read(reader).map_err(|e| DictSectError { e, sect_kind });
        Ok(UnvalidatedFourSectDict([f(Shared)?, f(Subject)?, f(Predicate)?, f(Object)?]))
    }

    /// write the whole Dictionary including control info and all sections
    pub fn write(&self, write: &mut impl std::io::Write) -> Result<()> {
        use SectKind::*;
        ControlInfo::four_sect_dict().write(write)?;
        self.shared.write(write).map_err(|e| DictSectError { e, sect_kind: Shared })?;
        self.subjects.write(write).map_err(|e| DictSectError { e, sect_kind: Subject })?;
        self.predicates.write(write).map_err(|e| DictSectError { e, sect_kind: Predicate })?;
        self.objects.write(write).map_err(|e| DictSectError { e, sect_kind: Object })?;
        Ok(())
    }

    /// size in bytes of the in memory four section dictionary
    pub fn size_in_bytes(&self) -> usize {
        self.shared.size_in_bytes()
            + self.subjects.size_in_bytes()
            + self.predicates.size_in_bytes()
            + self.objects.size_in_bytes()
    }
}

/// A wrapper to ensure prevent using FourSectDict before its checksums have been validated
#[cfg(not(any(target_arch = "wasm32", target_arch = "wasm64")))]
pub struct UnvalidatedFourSectDict([JoinHandle<dict_sect_pfc::Result<DictSectPFC>>; 4]);

/// WASM version without JoinHandle
#[cfg(any(target_arch = "wasm32", target_arch = "wasm64"))]
pub struct UnvalidatedFourSectDict([DictSectPFC; 4]);

impl UnvalidatedFourSectDict {
    /// Validates the checksums of all dictionary sections in parallel.
    /// Dict validation takes around 1200 ms on a single thread with an 1.5 GB HDT file on an i9-12900k.
    #[cfg(not(any(target_arch = "wasm32", target_arch = "wasm64")))]
    pub fn validate(self) -> Result<FourSectDict> {
        use SectKind::*;
        let r: Vec<_> = [Shared, Subject, Predicate, Object]
            .into_iter()
            .zip(self.0)
            .map(|(sect_kind, handle)| handle.join().unwrap().map_err(|e| DictSectError { e, sect_kind }))
            .collect::<std::result::Result<Vec<DictSectPFC>, DictSectError>>()?;
        let [shared, subjects, predicates, objects]: [DictSectPFC; 4] = r.try_into().unwrap();
        Ok(FourSectDict { shared, subjects, predicates, objects })
    }

    /// WASM version - sections are already validated during read
    #[cfg(any(target_arch = "wasm32", target_arch = "wasm64"))]
    pub fn validate(self) -> Result<FourSectDict> {
        let [shared, subjects, predicates, objects] = self.0;
        Ok(FourSectDict { shared, subjects, predicates, objects })
    }
}

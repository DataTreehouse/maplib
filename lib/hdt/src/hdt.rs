use crate::containers::{ControlInfo, control_info};
use crate::four_sect_dict::{self, IdKind};
use crate::header::Header;
use crate::triples::{Id, ObjectIter, PredicateIter, PredicateObjectIter, SubjectIter, TripleId, TriplesBitmap};
use crate::{FourSectDict, header};
use bytesize::ByteSize;
use log::{debug, error};
use std::io::{BufRead, Write};
use std::iter;
use std::sync::Arc;

pub type Result<T> = core::result::Result<T, Error>;


/// In-memory representation of an RDF graph loaded from an HDT file.
/// Allows queries by triple patterns.
#[derive(Debug)]
pub struct Hdt {
    //global_ci: ControlInfo,
    // header is not necessary for querying but shouldn't waste too much space and we need it for writing in the future, may also make it optional
    pub header: Header,
    /// in-memory representation of dictionary
    pub dict: FourSectDict,
    /// in-memory representation of triples
    pub triples: TriplesBitmap,
}

/// An HDT file read for one full pass over its triples: the dictionary and the triples section
/// as stored, without the query indexes that [`Hdt::read`] builds.
/// Maplib addition.
pub struct HdtScan {
    pub header: Header,
    pub dict: FourSectDict,
    pub triples: crate::triples::TriplesScan,
}

impl HdtScan {
    /// Reads an HDT file. The dictionary checksums are verified while the triples are read.
    pub fn read<R: BufRead>(mut reader: R) -> Result<Self> {
        let header = Hdt::read_header(&mut reader)?;
        let unvalidated_dict = FourSectDict::read(&mut reader)?;
        let triples = crate::triples::TriplesScan::read_sect(&mut reader)?;
        let dict = unvalidated_dict.validate()?;
        Ok(HdtScan { header, dict, triples })
    }
}

type StringTriple = [Arc<str>; 3];

/// The error type for the `translate_id` method.
#[derive(thiserror::Error, Debug)]
#[error("cannot translate triple ID {t:?} to string triple: {e}")]
pub struct TranslateError {
    #[source]
    e: four_sect_dict::ExtractError,
    t: TripleId,
}

/// The error type for the `new` method.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to read HDT control info")]
    ControlInfo(#[from] control_info::Error),
    #[error("failed to read HDT header")]
    Header(#[from] header::Error),
    #[error("failed to read HDT four section dictionary")]
    FourSectDict(#[from] four_sect_dict::Error),
    #[error("failed to read HDT triples section")]
    Triples(#[from] crate::triples::Error),
    #[error("IO Error")]
    Io(#[from] std::io::Error),
}

impl Hdt {
    #[deprecated(since = "0.4.0", note = "please use `read` instead")]
    pub fn new(reader: impl BufRead) -> Result<Self> {
        Self::read(reader)
    }

    /// Validates the leading global control info chunk, then returns the header section.
    pub fn read_header<R: BufRead>(reader: &mut R) -> header::Result<Header> {
        ControlInfo::read(reader)?;
        Header::read(reader)
    }

    /// Creates an immutable HDT instance containing the dictionary and triples from the given reader.
    /// The reader must point to the beginning of the data of an HDT file.
    /// FourSectionDictionary with DictionarySectionPlainFrontCoding and SPO order is the only supported implementation.
    /// The format is specified at <https://www.rdfhdt.org/hdt-binary-format/>, however there are some deviations.
    /// The initial HDT specification at <http://www.w3.org/Submission/2011/03/> is outdated and not supported.
    /// # Example
    /// ```
    /// let file = std::fs::File::open("tests/resources/snikmeta.hdt").expect("error opening file");
    /// let hdt = hdt::Hdt::read(std::io::BufReader::new(file)).unwrap();
    /// ```
    pub fn read<R: BufRead>(mut reader: R) -> Result<Self> {
        let header = Self::read_header(&mut reader)?;
        let unvalidated_dict = FourSectDict::read(&mut reader)?;
        let triples = TriplesBitmap::read_sect(&mut reader)?;
        let dict = unvalidated_dict.validate()?;
        let hdt = Hdt { header, dict, triples };
        debug!("HDT size in memory {}, details:", ByteSize(hdt.size_in_bytes() as u64));
        debug!("{hdt:#?}");
        Ok(hdt)
    }






    pub fn write(&self, write: &mut impl Write) -> Result<()> {
        ControlInfo::global().write(write)?;
        self.header.write(write)?;
        self.dict.write(write)?;
        self.triples.write(write)?;
        write.flush()?;
        Ok(())
    }

    /// Recursive size in bytes on the heap.
    pub fn size_in_bytes(&self) -> usize {
        self.dict.size_in_bytes() + self.triples.size_in_bytes()
    }

    /// An iterator visiting *all* triples as strings in order.
    /// Using this method with a filter can be inefficient for large graphs,
    /// because the strings are stored in compressed form and must be decompressed and allocated.
    /// Whenever possible, use [`Hdt::triples_with_pattern`] instead.
    /// # Example
    /// ```
    /// fn print_first_triple(hdt: hdt::Hdt) {
    ///     println!("{:?}", hdt.triples_all().next().expect("no triple in the graph"));
    /// }
    /// ```
    pub fn triples_all(&self) -> impl Iterator<Item = StringTriple> + '_ {
        let mut triple_cache = TripleCache::new(self);
        self.triples.into_iter().map(move |ids| triple_cache.translate(ids).unwrap())
    }

    /// Get all subjects with the given property and object (?PO pattern).
    /// Use this over `triples_with_pattern(None,Some(p),Some(o))` if you don't need whole triples.
    /// # Example
    /// Who was born in Leipzig?
    /// ```
    /// fn query(dbpedia: hdt::Hdt) {
    ///     for person in dbpedia.subjects_with_po(
    ///       "http://dbpedia.org/ontology/birthPlace", "http://dbpedia.org/resource/Leipzig") {
    ///       println!("{person:?}");
    ///     }
    /// }
    /// ```
    pub fn subjects_with_po(&self, p: &str, o: &str) -> Box<dyn Iterator<Item = String> + '_> {
        let pid = self.dict.string_to_id(p, IdKind::Predicate);
        let oid = self.dict.string_to_id(o, IdKind::Object);
        // predicate or object not in dictionary, iterator would interpret 0 as variable
        if pid == 0 || oid == 0 {
            return Box::new(iter::empty());
        }
        // needed for extending the lifetime of the parameters into the iterator for error messages
        let p_owned = p.to_owned();
        let o_owned = o.to_owned();
        Box::new(
            PredicateObjectIter::new(&self.triples, pid, oid)
                .map(move |sid| self.dict.id_to_string(sid, IdKind::Subject))
                .filter_map(move |r| {
                    r.map_err(|e| error!("Error on triple with property {p_owned} and object {o_owned}: {e}")).ok()
                }),
        )
    }

    /// Get all triples that fit the given triple patterns, where `None` stands for a variable.
    /// For example, `triples_with_pattern(Some(s), Some(p), None)` answers an SP? pattern.
    /// # Example
    /// What is the capital of the United States of America?
    /// ```
    /// fn query(dbpedia: hdt::Hdt) {
    ///   println!("{:?}", dbpedia.triples_with_pattern(
    ///     Some("http://dbpedia.org/resource/United_States"), Some("http://dbpedia.org/ontology/capital"), None)
    ///     .next().expect("no capital found")[2]);
    /// }
    /// ```
    pub fn triples_with_pattern<'a>(
        &'a self, sp: Option<&'a str>, pp: Option<&'a str>, op: Option<&'a str>,
    ) -> Box<dyn Iterator<Item = StringTriple> + 'a> {
        let pattern: [Option<(Arc<str>, usize)>; 3] = [(0, sp), (1, pp), (2, op)]
            .map(|(i, x)| x.map(|x| (Arc::from(x), self.dict.string_to_id(x, IdKind::KINDS[i]))));
        // at least one term does not exist in the graph
        if pattern.iter().flatten().any(|x| x.1 == 0) {
            return Box::new(iter::empty());
        }
        // TODO: improve error handling
        let mut cache = TripleCache::new(self);
        match pattern {
            [Some(s), Some(p), Some(o)] => {
                if SubjectIter::with_pattern(&self.triples, [s.1, p.1, o.1]).next().is_some() {
                    Box::new(iter::once([s.0, p.0, o.0]))
                } else {
                    Box::new(iter::empty())
                }
            }
            [Some(s), Some(p), None] => {
                Box::new(SubjectIter::with_pattern(&self.triples, [s.1, p.1, 0]).map(move |t| {
                    [s.0.clone(), p.0.clone(), Arc::from(self.dict.id_to_string(t[2], IdKind::Object).unwrap())]
                }))
            }
            [Some(s), None, Some(o)] => {
                Box::new(SubjectIter::with_pattern(&self.triples, [s.1, 0, o.1]).map(move |t| {
                    [s.0.clone(), Arc::from(self.dict.id_to_string(t[1], IdKind::Predicate).unwrap()), o.0.clone()]
                }))
            }
            [Some(s), None, None] => Box::new(
                SubjectIter::with_pattern(&self.triples, [s.1, 0, 0])
                    .map(move |t| [s.0.clone(), cache.get(1, t[1]).unwrap(), cache.get(2, t[2]).unwrap()]),
            ),
            [None, Some(p), Some(o)] => {
                Box::new(PredicateObjectIter::new(&self.triples, p.1, o.1).map(move |sid| {
                    [Arc::from(self.dict.id_to_string(sid, IdKind::Subject).unwrap()), p.0.clone(), o.0.clone()]
                }))
            }
            [None, Some(p), None] => Box::new(
                PredicateIter::new(&self.triples, p.1)
                    .map(move |t| [cache.get(0, t[0]).unwrap(), p.0.clone(), cache.get(2, t[2]).unwrap()]),
            ),
            [None, None, Some(o)] => Box::new(
                ObjectIter::new(&self.triples, o.1)
                    .map(move |t| [cache.get(0, t[0]).unwrap(), cache.get(1, t[1]).unwrap(), o.0.clone()]),
            ),
            [None, None, None] => Box::new(self.triples_all()),
        }
    }

    /// Get all internal triple IDs that fit the given triple patterns, where `None` stands for a variable.
    /// Used for specific optimizations, call triples_with_pattern instead to get actual string results.
    pub fn triple_ids_with_pattern<'a>(
        &'a self, sp: Option<&'a str>, pp: Option<&'a str>, op: Option<&'a str>,
    ) -> Box<dyn Iterator<Item = TripleId> + 'a> {
        let pattern: [Option<usize>; 3] =
            [(0, sp), (1, pp), (2, op)].map(|(i, x)| x.map(|x| self.dict.string_to_id(x, IdKind::KINDS[i])));
        // fail fast if any constant is 0, meaning missing from the dict
        if pattern.contains(&Some(0)) {
            return Box::new(iter::empty());
        }
        // we did not fail, so all constants are >0 so we transform None values into 0 to signify variables
        let pattern: TripleId = pattern.map(|x| x.unwrap_or(0));
        self.triple_ids_with_id_pattern(pattern)
    }

    /// Get all internal triple IDs that fit the given triple patterns, where 0 stands for a variable.
    /// Used for specific optimizations, call triples_with_pattern instead to get actual string results.
    pub fn triple_ids_with_id_pattern<'a>(&'a self, pattern: TripleId) -> Box<dyn Iterator<Item = TripleId> + 'a> {
        let ts = &self.triples;
        let [s, p, o] = pattern;
        // can't use slice: half_open_range_patterns_in_slices is still unstable, see https://github.com/rust-lang/rust/issues/67264
        match (s, p, o) {
            (1.., _, _) => Box::new(SubjectIter::with_pattern(ts, [s, p, o]).map(move |t| [s, t[1], t[2]])),
            (0, 1.., 1..) => Box::new(PredicateObjectIter::new(ts, p, o).map(move |sid| [sid, p, o])),
            (0, 1.., 0) => Box::new(PredicateIter::new(ts, p).map(move |t| [t[0], p, t[2]])),
            (0, 0, 1..) => Box::new(ObjectIter::new(ts, o).map(move |t| [t[0], t[1], o])),
            (0, 0, 0) => Box::new(self.triples.into_iter()),
        }
    }
}

/// A TripleCache stores the `Arc<str>` of the last returned triple
#[derive(Clone, Debug)]
struct TripleCache<'a> {
    hdt: &'a Hdt,
    tid: TripleId,
    arc: [Option<Arc<str>>; 3],
}

impl<'a> TripleCache<'a> {
    /// Build a new [`TripleCache`] for the given [`Hdt`]
    const fn new(hdt: &'a super::Hdt) -> Self {
        TripleCache { hdt, tid: [0; 3], arc: [None, None, None] }
    }

    /// Translate a triple of indexes into a triple of strings.
    fn translate(&mut self, t: TripleId) -> core::result::Result<StringTriple, TranslateError> {
        // refactor when try_map for arrays becomes stable
        Ok([
            self.get(0, t[0]).map_err(|e| TranslateError { e, t })?,
            self.get(1, t[1]).map_err(|e| TranslateError { e, t })?,
            self.get(2, t[2]).map_err(|e| TranslateError { e, t })?,
        ])
    }

    fn get(&mut self, pos: usize, id: Id) -> core::result::Result<Arc<str>, four_sect_dict::ExtractError> {
        debug_assert!(id != 0);
        debug_assert!(pos < 3);
        if self.tid[pos] == id {
            Ok(self.arc[pos].as_ref().unwrap().clone())
        } else {
            let ret: Arc<str> = self.hdt.dict.id_to_string(id, IdKind::KINDS[pos])?.into();
            self.arc[pos] = Some(ret.clone());
            self.tid[pos] = id;
            Ok(ret)
        }
    }
}

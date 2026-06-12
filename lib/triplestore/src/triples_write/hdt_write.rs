use crate::errors::TriplestoreError;
use hdt::containers::rdf::{Id, Literal as HdtLiteral, Term as HdtTerm, Triple as HdtTriple};
use hdt::containers::ControlInfo;
use hdt::dict_sect_pfc::DictSectPFC;
use hdt::four_sect_dict::FourSectDict;
use hdt::header::Header;
use hdt::triples::{TripleId, TriplesBitmap};
use hdt::IdKind;
use oxrdf::{NamedOrBlankNode, Term, Triple};
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::io::{BufWriter, Write};

const BLOCK_SIZE: usize = 16;
const BASE_IRI: &str = "https://github.com/DataTreehouse/maplib";

pub(super) struct HdtBuilder {
    term_ids: HashMap<String, usize>,
    roles: Vec<TermRoles>,
    triples: Vec<[usize; 3]>,
}

#[derive(Default, Clone, Copy)]
struct TermRoles {
    subject: bool,
    predicate: bool,
    object: bool,
}

impl HdtBuilder {
    pub(super) fn new() -> Self {
        Self {
            term_ids: HashMap::new(),
            roles: Vec::new(),
            triples: Vec::new(),
        }
    }

    pub(super) fn add_triple(&mut self, t: &Triple) {
        let s = self.intern(subject_dict_string(&t.subject));
        let p = self.intern(t.predicate.as_str().to_owned());
        let o = self.intern(object_dict_string(&t.object));
        self.roles[s].subject = true;
        self.roles[p].predicate = true;
        self.roles[o].object = true;
        self.triples.push([s, p, o]);
    }

    fn intern(&mut self, term: String) -> usize {
        let next_id = self.term_ids.len();
        match self.term_ids.entry(term) {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                e.insert(next_id);
                self.roles.push(TermRoles::default());
                next_id
            }
        }
    }

    pub(super) fn finish<W: Write>(self, buf: &mut W) -> Result<(), TriplestoreError> {
        let dict = self.build_dict();
        let encoded = self.encode_triples(&dict)?;
        let triples = TriplesBitmap::from_triples(&encoded);
        let header = statistics_header(&dict, encoded.len());

        let mut writer = BufWriter::new(buf);
        ControlInfo::global()
            .write(&mut writer)
            .map_err(hdt_error)?;
        header.write(&mut writer).map_err(hdt_error)?;
        dict.write(&mut writer).map_err(hdt_error)?;
        triples.write(&mut writer).map_err(hdt_error)?;
        writer.flush().map_err(hdt_error)?;
        Ok(())
    }

    fn build_dict(&self) -> FourSectDict {
        let mut shared = BTreeSet::new();
        let mut subjects = BTreeSet::new();
        let mut predicates = BTreeSet::new();
        let mut objects = BTreeSet::new();
        for (term, &id) in &self.term_ids {
            let roles = &self.roles[id];
            if roles.predicate {
                predicates.insert(term.as_str());
            }
            if roles.subject && roles.object {
                shared.insert(term.as_str());
            } else if roles.subject {
                subjects.insert(term.as_str());
            } else if roles.object {
                objects.insert(term.as_str());
            }
        }
        FourSectDict {
            shared: DictSectPFC::compress(&shared, BLOCK_SIZE),
            subjects: DictSectPFC::compress(&subjects, BLOCK_SIZE),
            predicates: DictSectPFC::compress(&predicates, BLOCK_SIZE),
            objects: DictSectPFC::compress(&objects, BLOCK_SIZE),
        }
    }

    fn encode_triples(&self, dict: &FourSectDict) -> Result<Vec<TripleId>, TriplestoreError> {
        let mut term_by_id = vec![""; self.term_ids.len()];
        for (term, &id) in &self.term_ids {
            term_by_id[id] = term.as_str();
        }
        let mut encoded = Vec::with_capacity(self.triples.len());
        for [s, p, o] in &self.triples {
            let triple_id: TripleId = [
                dict.string_to_id(term_by_id[*s], IdKind::Subject),
                dict.string_to_id(term_by_id[*p], IdKind::Predicate),
                dict.string_to_id(term_by_id[*o], IdKind::Object),
            ];
            if triple_id.contains(&0) {
                return Err(TriplestoreError::HDTError(format!(
                    "term of ({}, {}, {}) missing from the HDT dictionary",
                    term_by_id[*s], term_by_id[*p], term_by_id[*o]
                )));
            }
            encoded.push(triple_id);
        }
        encoded.sort_unstable();
        encoded.dedup();
        Ok(encoded)
    }
}

fn subject_dict_string(subject: &NamedOrBlankNode) -> String {
    match subject {
        NamedOrBlankNode::NamedNode(nn) => nn.as_str().to_owned(),
        NamedOrBlankNode::BlankNode(bn) => bn.to_string(),
    }
}

fn object_dict_string(object: &Term) -> String {
    match object {
        Term::NamedNode(nn) => nn.as_str().to_owned(),
        Term::BlankNode(bn) => bn.to_string(),
        Term::Literal(lit) => lit.to_string(),
    }
}

fn statistics_header(dict: &FourSectDict, num_triples: usize) -> Header {
    use hdt::vocab::*;

    let mut body = BTreeSet::new();
    let base = Id::Named(BASE_IRI.to_owned());
    let stats_id = Id::Blank("statistics".to_owned());
    let pub_id = Id::Blank("publicationInformation".to_owned());
    let format_id = Id::Blank("format".to_owned());
    let dict_id = Id::Blank("dictionary".to_owned());
    let triples_id = Id::Blank("triples".to_owned());

    let distinct_subjects = dict.subjects.num_strings() + dict.shared.num_strings();
    let distinct_objects = dict.objects.num_strings() + dict.shared.num_strings();

    insert_literal(&mut body, &base, RDF_TYPE, HDT_CONTAINER);
    insert_literal(&mut body, &base, RDF_TYPE, VOID_DATASET);
    insert_literal(&mut body, &base, VOID_TRIPLES, num_triples);
    insert_literal(&mut body, &base, VOID_PROPERTIES, dict.predicates.num_strings());
    insert_literal(&mut body, &base, VOID_DISTINCT_SUBJECTS, distinct_subjects);
    insert_literal(&mut body, &base, VOID_DISTINCT_OBJECTS, distinct_objects);

    insert_id(&mut body, &base, HDT_STATISTICAL_INFORMATION, &stats_id);
    insert_id(&mut body, &base, HDT_STATISTICAL_INFORMATION, &pub_id);
    insert_id(&mut body, &base, HDT_FORMAT_INFORMATION, &format_id);
    insert_id(&mut body, &format_id, HDT_DICTIONARY, &dict_id);
    insert_id(&mut body, &format_id, HDT_TRIPLES, &triples_id);

    insert_literal(&mut body, &dict_id, HDT_DICT_SHARED_SO, dict.shared.num_strings());
    insert_literal(&mut body, &dict_id, HDT_DICT_MAPPING, "1");
    insert_literal(&mut body, &dict_id, HDT_DICT_SIZE_STRINGS, dict.size_in_bytes());
    insert_literal(&mut body, &dict_id, HDT_DICT_BLOCK_SIZE, BLOCK_SIZE);

    insert_literal(&mut body, &triples_id, DC_TERMS_FORMAT, HDT_TYPE_BITMAP);
    insert_literal(&mut body, &triples_id, HDT_NUM_TRIPLES, num_triples);
    insert_literal(&mut body, &triples_id, HDT_TRIPLES_ORDER, "SPO");

    let mut serialized_body = Vec::new();
    for triple in &body {
        writeln!(serialized_body, "{triple}").unwrap();
    }
    Header {
        format: "ntriples".to_owned(),
        length: serialized_body.len(),
        body,
    }
}

fn insert_literal(body: &mut BTreeSet<HdtTriple>, s: &Id, p: &str, o: impl ToString) {
    body.insert(HdtTriple::new(
        s.clone(),
        p.to_owned(),
        HdtTerm::Literal(HdtLiteral::new(o.to_string())),
    ));
}

fn insert_id(body: &mut BTreeSet<HdtTriple>, s: &Id, p: &str, o: &Id) {
    body.insert(HdtTriple::new(s.clone(), p.to_owned(), HdtTerm::Id(o.clone())));
}

fn hdt_error(e: impl std::fmt::Display) -> TriplestoreError {
    TriplestoreError::HDTError(e.to_string())
}

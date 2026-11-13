//! Reads HDT files straight into the triplestore's categorical encoding.
//!
//! Every dictionary entry is decoded once and mapped to its maplib encoding up front: IRIs, blank
//! nodes and string-like literals to their u32 in the global cats, other literals to a row of a
//! typed column. The triples are then scanned in parallel ranges as ID triples and translated
//! through these maps, so no term is parsed, hashed or allocated per triple, and no local cats
//! have to be merged and re-encoded afterwards.

use super::get_or_insert_dt;
use crate::errors::TriplestoreError;
use crate::Triplestore;
use hdt::dict_sect_pfc::DictSectPFC;
use hdt::hdt::HdtScan;
use hdt::triples::TriplesScan;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{Literal, NamedNode, Term};
use polars::prelude::{
    DataFrame, IdxCa, IdxSize, IntoColumn, IntoSeries, NamedFrom, PlSmallStr, Series, StructChunked,
};
use rayon::prelude::*;
use representation::cats::{CatTriples, CatType, EncodedTriples};
use representation::dataset::NamedGraph;
use representation::{
    get_term_datatype_ref, BaseRDFNodeType, SeriesBuilder, LANG_STRING_LANG_FIELD,
    LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME, SUBJECT_COL_NAME,
};
use std::borrow::Cow;
use std::cmp;
use std::collections::HashMap;
use std::io::Cursor;
use std::ops::Range;
use std::str::FromStr;
use std::time::Instant;
use tracing::debug;

/// Type index of dictionary entries that cannot be added (e.g. "abc"^^xsd:int), whose triples are
/// skipped like the other readers skip unparseable literals.
const INVALID: u16 = u16::MAX;
/// Dictionary blocks decoded per parallel job, at least.
const MIN_BLOCKS_PER_JOB: usize = 1024;
/// Triples scanned per parallel job, at least.
const MIN_TRIPLES_PER_JOB: usize = 1 << 16;

/// The maplib encoding of a dictionary entry: an index into the term types, and either the u32 in
/// the global cats (cat stored types), an index into the language string pairs (rdf:langString)
/// or a row of the typed values of its type (other literals).
#[derive(Clone, Copy)]
struct Enc {
    ty: u16,
    v: u32,
}

const INVALID_ENC: Enc = Enc { ty: INVALID, v: 0 };

impl Triplestore {
    pub(crate) fn read_hdt(
        &mut self,
        slice: &[u8],
        graph: &NamedGraph,
        triples_batch_size: usize,
    ) -> Result<(), TriplestoreError> {
        let start = Instant::now();
        let HdtScan { dict, triples, .. } = HdtScan::read(Cursor::new(slice))
            .map_err(|e| TriplestoreError::HDTError(e.to_string()))?;
        debug!(
            "Reading the HDT sections took {} seconds",
            start.elapsed().as_secs_f32()
        );

        let start_dict = Instant::now();
        let blank_suffix = self.parser_call.to_string();
        let sections = [&dict.shared, &dict.subjects, &dict.objects];
        let jobs = dictionary_jobs(&sections);
        let chunks: Vec<TermChunk> = jobs
            .par_iter()
            .map(|(section, blocks)| {
                let mut chunk = TermChunk::default();
                sections[*section]
                    .decode_blocks(blocks.clone(), |_, s| chunk.push_term(s, &blank_suffix));
                chunk
            })
            .collect();
        let predicates = decode_predicates(&dict.predicates);
        let terms = {
            let mut cats = self.global_cats.write().unwrap();
            DictionaryEncoding::new(chunks, &jobs, &sections, &mut cats)?
        };
        drop(dict);
        debug!(
            "Mapping the HDT dictionary to cats took {} seconds",
            start_dict.elapsed().as_secs_f32()
        );

        let num_triples = triples.len();
        let threads = rayon::current_num_threads();
        let batch_size = cmp::max(triples_batch_size, 1);
        let mut batch_start = 0;
        while batch_start < num_triples {
            let start_batch = Instant::now();
            let batch_end = cmp::min(num_triples, batch_start + batch_size);
            let per_job = cmp::max(
                (batch_end - batch_start).div_ceil(threads),
                MIN_TRIPLES_PER_JOB,
            );
            let ranges: Vec<Range<usize>> = (batch_start..batch_end)
                .step_by(per_job)
                .map(|a| a..cmp::min(batch_end, a + per_job))
                .collect();
            let parts: Vec<TripleGroups> = ranges
                .into_par_iter()
                .map(|r| TripleGroups::scan(&triples, r, &terms, predicates.len()))
                .collect();
            let cat_triples = merge_groups(parts)
                .into_par_iter()
                .map(|(key, subjects, objects)| {
                    terms.cat_triples(key, subjects, objects, &predicates, graph)
                })
                .collect::<Result<Vec<_>, _>>()?;
            debug!(
                "Translating {} HDT triples took {} seconds",
                batch_end - batch_start,
                start_batch.elapsed().as_secs_f32()
            );
            self.add_local_cat_triples(cat_triples)?;
            batch_start = batch_end;
        }
        self.parser_call += 1;
        Ok(())
    }
}

/// Splits the shared, subject and object sections into block ranges to decode in parallel.
fn dictionary_jobs(sections: &[&DictSectPFC; 3]) -> Vec<(usize, Range<usize>)> {
    let threads = rayon::current_num_threads();
    let mut jobs = vec![];
    for (i, section) in sections.iter().enumerate() {
        let num_blocks = section.num_blocks();
        let per_job = cmp::max(num_blocks.div_ceil(threads * 4), MIN_BLOCKS_PER_JOB);
        let mut b = 0;
        while b < num_blocks {
            let e = cmp::min(num_blocks, b + per_job);
            jobs.push((i, b..e));
            b = e;
        }
    }
    jobs
}

fn decode_predicates(section: &DictSectPFC) -> Vec<NamedNode> {
    let mut predicates = Vec::with_capacity(section.num_strings());
    section.decode_blocks(0..section.num_blocks(), |_, s| {
        predicates.push(NamedNode::new_unchecked(s))
    });
    predicates
}

/// Strings stored back to back, to avoid one allocation per term.
#[derive(Default)]
struct StrArena {
    data: String,
    ends: Vec<usize>,
}

impl StrArena {
    fn push(&mut self, parts: &[&str]) -> u32 {
        for p in parts {
            self.data.push_str(p);
        }
        self.ends.push(self.data.len());
        (self.ends.len() - 1) as u32
    }

    fn iter(&self) -> impl Iterator<Item = &str> {
        let mut start = 0;
        self.ends.iter().map(move |&end| {
            let s = &self.data[start..end];
            start = end;
            s
        })
    }
}

/// A dictionary entry classified by a [`TermChunk`], with type indices local to the chunk.
enum Slot {
    /// Cat stored type, index into the arena of the type.
    Cat(u16, u32),
    /// Language string, value and language tag indices into the rdf:langString arena.
    Lang(u16, u32, u32),
    /// Other literal, row of the typed values of the type.
    Typed(u16, u32),
    Invalid,
}

/// The dictionary entries of a range of blocks, classified by type. Strings of cat stored types
/// are kept to be encoded in the global cats, other literals are parsed into typed columns.
#[derive(Default)]
struct TermChunk {
    types: Vec<BaseRDFNodeType>,
    type_ids: HashMap<BaseRDFNodeType, u16>,
    arenas: Vec<StrArena>,
    builders: Vec<Option<SeriesBuilder>>,
    /// Datatype IRI to its local type, if literals of the datatype are cat stored.
    datatype_cats: HashMap<String, Option<u16>>,
    literal_types: HashMap<String, Vec<BaseRDFNodeType>>,
    slots: Vec<Slot>,
}

impl TermChunk {
    fn type_id(&mut self, t: BaseRDFNodeType) -> u16 {
        if let Some(i) = self.type_ids.get(&t) {
            return *i;
        }
        let i = self.types.len() as u16;
        self.builders.push(if t.stored_cat() {
            None
        } else {
            Some(SeriesBuilder::new(&t))
        });
        self.arenas.push(StrArena::default());
        self.types.push(t.clone());
        self.type_ids.insert(t, i);
        i
    }

    fn push_term(&mut self, s: &str, blank_suffix: &str) {
        let slot = if let Some(label) = s.strip_prefix("_:") {
            let t = self.type_id(BaseRDFNodeType::BlankNode);
            // Blank nodes are made unique to this read, like the other readers do.
            Slot::Cat(t, self.arenas[t as usize].push(&[label, "_", blank_suffix]))
        } else if s.starts_with('"') {
            self.literal_slot(s)
        } else {
            let t = self.type_id(BaseRDFNodeType::IRI);
            Slot::Cat(t, self.arenas[t as usize].push(&[s]))
        };
        self.slots.push(slot);
    }

    fn literal_slot(&mut self, s: &str) -> Slot {
        let (value, suffix) = split_hdt_literal(s);
        match suffix {
            LiteralSuffix::Language(lang) => {
                let t = self.type_id(BaseRDFNodeType::Literal(
                    rdf::LANG_STRING.into_owned(),
                    None,
                ));
                let arena = &mut self.arenas[t as usize];
                let v = arena.push(&[&value]);
                let l = arena.push(&[lang]);
                Slot::Lang(t, v, l)
            }
            LiteralSuffix::Datatype(dt) => {
                if dt == rdf::LANG_STRING.as_str() {
                    return Slot::Invalid;
                }
                let cat_type = if let Some(c) = self.datatype_cats.get(dt) {
                    *c
                } else {
                    let t = BaseRDFNodeType::Literal(NamedNode::new_unchecked(dt), None);
                    let c = if t.stored_cat() {
                        Some(self.type_id(t))
                    } else {
                        None
                    };
                    self.datatype_cats.insert(dt.to_string(), c);
                    c
                };
                if let Some(t) = cat_type {
                    return Slot::Cat(t, self.arenas[t as usize].push(&[&value]));
                }
                let term = Term::Literal(Literal::new_typed_literal(
                    value,
                    NamedNode::new_unchecked(dt),
                ));
                let t = get_or_insert_dt(get_term_datatype_ref(&term), &mut self.literal_types);
                let t = self.type_id(t);
                let builder = self.builders[t as usize].as_mut().unwrap();
                let row = builder.len() as u32;
                match builder.parse_term(&term) {
                    Ok(()) => Slot::Typed(t, row),
                    Err(_) => Slot::Invalid,
                }
            }
        }
    }
}

enum LiteralSuffix<'a> {
    Language(&'a str),
    Datatype(&'a str),
}

/// Splits an HDT dictionary literal (`"value"`, `"value"@lang` or `"value"^^<datatype>`) into its
/// value and its language tag or datatype. Values are stored unescaped by hdt-java and hdt-cpp,
/// but with N-Triples escapes by earlier maplib versions and the hdt crate, so escapes are
/// resolved when they parse.
fn split_hdt_literal(s: &str) -> (Cow<'_, str>, LiteralSuffix<'_>) {
    if let Some(close) = s.rfind('"').filter(|c| *c > 0) {
        let raw = &s[1..close];
        let rest = &s[close + 1..];
        let suffix = if rest.is_empty() {
            Some(LiteralSuffix::Datatype(xsd::STRING.as_str()))
        } else if let Some(lang) = rest.strip_prefix('@') {
            Some(LiteralSuffix::Language(lang))
        } else {
            rest.strip_prefix("^^<")
                .and_then(|r| r.strip_suffix('>'))
                .map(LiteralSuffix::Datatype)
        };
        if let Some(suffix) = suffix {
            return (unescape(raw), suffix);
        }
    }
    (
        Cow::Borrowed(s),
        LiteralSuffix::Datatype(xsd::STRING.as_str()),
    )
}

fn unescape(raw: &str) -> Cow<'_, str> {
    if !raw.contains('\\') {
        return Cow::Borrowed(raw);
    }
    match Literal::from_str(&format!("\"{raw}\"")) {
        Ok(l) => Cow::Owned(l.value().to_string()),
        Err(_) => Cow::Borrowed(raw),
    }
}

/// The maplib encoding of every entry of the shared, subject and object sections.
struct DictionaryEncoding {
    types: Vec<BaseRDFNodeType>,
    num_shared: usize,
    shared: Vec<Enc>,
    subjects: Vec<Enc>,
    objects: Vec<Enc>,
    lang_pairs: Vec<[u32; 2]>,
    /// The values of the literal types that are not cat stored, indexed by type.
    typed_values: Vec<Option<Series>>,
}

impl DictionaryEncoding {
    fn new(
        chunks: Vec<TermChunk>,
        jobs: &[(usize, Range<usize>)],
        sections: &[&DictSectPFC; 3],
        cats: &mut representation::cats::Cats,
    ) -> Result<Self, TriplestoreError> {
        // Unify the chunk local types.
        let mut types: Vec<BaseRDFNodeType> = vec![];
        let mut type_ids: HashMap<BaseRDFNodeType, u16> = HashMap::new();
        let mut local_to_global: Vec<Vec<u16>> = Vec::with_capacity(chunks.len());
        for chunk in &chunks {
            let mut m = Vec::with_capacity(chunk.types.len());
            for t in &chunk.types {
                let g = if let Some(g) = type_ids.get(t) {
                    *g
                } else {
                    if types.len() >= INVALID as usize {
                        return Err(TriplestoreError::HDTError(
                            "too many distinct literal types".to_string(),
                        ));
                    }
                    let g = types.len() as u16;
                    types.push(t.clone());
                    type_ids.insert(t.clone(), g);
                    g
                };
                m.push(g);
            }
            local_to_global.push(m);
        }

        // Encode the strings of each cat stored type in the global cats, and concatenate the
        // typed values, remembering where each chunk's part starts.
        let mut offsets: Vec<Vec<usize>> = chunks.iter().map(|c| vec![0; c.types.len()]).collect();
        let mut encoded: Vec<Vec<u32>> = vec![vec![]; types.len()];
        let mut typed_values: Vec<Option<Series>> = vec![None; types.len()];
        let mut chunks = chunks;
        for (g, t) in types.iter().enumerate() {
            if t.stored_cat() {
                let mut strs = vec![];
                for (c, chunk) in chunks.iter().enumerate() {
                    if let Some(l) = local_to_global[c].iter().position(|x| *x as usize == g) {
                        offsets[c][l] = strs.len();
                        strs.extend(chunk.arenas[l].iter());
                    }
                }
                encoded[g] = cats.encode_strs_globally(&CatType::from_base_rdf_node_type(t), &strs);
            } else {
                let mut values: Option<Series> = None;
                for (c, chunk) in chunks.iter_mut().enumerate() {
                    if let Some(l) = local_to_global[c].iter().position(|x| *x as usize == g) {
                        let series = chunk.builders[l]
                            .take()
                            .unwrap()
                            .into_series(OBJECT_COL_NAME);
                        if let Some(values) = &mut values {
                            offsets[c][l] = values.len();
                            values.append(&series).map_err(|e| {
                                TriplestoreError::HDTError(format!(
                                    "could not collect literals: {e}"
                                ))
                            })?;
                        } else {
                            values = Some(series);
                        }
                    }
                }
                typed_values[g] = values;
            }
        }
        for chunk in &mut chunks {
            chunk.arenas.clear();
        }

        // Translate the chunks' slots, in section order.
        let mut section_encs: [Vec<Enc>; 3] =
            std::array::from_fn(|i| Vec::with_capacity(sections[i].num_strings()));
        let mut lang_pairs = vec![];
        for (c, chunk) in chunks.into_iter().enumerate() {
            let out = &mut section_encs[jobs[c].0];
            let global = &local_to_global[c];
            for slot in chunk.slots {
                out.push(match slot {
                    Slot::Cat(l, i) => {
                        let g = global[l as usize];
                        Enc {
                            ty: g,
                            v: encoded[g as usize][offsets[c][l as usize] + i as usize],
                        }
                    }
                    Slot::Lang(l, vi, li) => {
                        let g = global[l as usize];
                        let enc = &encoded[g as usize];
                        let off = offsets[c][l as usize];
                        lang_pairs.push([enc[off + vi as usize], enc[off + li as usize]]);
                        Enc {
                            ty: g,
                            v: (lang_pairs.len() - 1) as u32,
                        }
                    }
                    Slot::Typed(l, row) => Enc {
                        ty: global[l as usize],
                        v: (offsets[c][l as usize] + row as usize) as u32,
                    },
                    Slot::Invalid => INVALID_ENC,
                });
            }
        }
        let [shared, subjects, objects] = section_encs;
        Ok(DictionaryEncoding {
            types,
            num_shared: sections[0].num_strings(),
            shared,
            subjects,
            objects,
            lang_pairs,
            typed_values,
        })
    }

    #[inline]
    fn subject(&self, id: usize) -> Enc {
        if id <= self.num_shared {
            id.checked_sub(1).and_then(|i| self.shared.get(i))
        } else {
            self.subjects.get(id - self.num_shared - 1)
        }
        .copied()
        .unwrap_or(INVALID_ENC)
    }

    #[inline]
    fn object(&self, id: usize) -> Enc {
        if id <= self.num_shared {
            id.checked_sub(1).and_then(|i| self.shared.get(i))
        } else {
            self.objects.get(id - self.num_shared - 1)
        }
        .copied()
        .unwrap_or(INVALID_ENC)
    }

    fn cat_triples(
        &self,
        (predicate, subject_type, object_type): GroupKey,
        subjects: Vec<u32>,
        objects: Vec<u32>,
        predicates: &[NamedNode],
        graph: &NamedGraph,
    ) -> Result<CatTriples, TriplestoreError> {
        let subject_type = self.types[subject_type as usize].clone();
        let object_index = object_type as usize;
        let object_type = self.types[object_index].clone();
        let len = subjects.len();
        let subjects = Series::new(SUBJECT_COL_NAME.into(), subjects);
        let objects = if object_type.is_lang_string() {
            let (values, langs): (Vec<u32>, Vec<u32>) = objects
                .into_iter()
                .map(|i| {
                    let [v, l] = self.lang_pairs[i as usize];
                    (v, l)
                })
                .unzip();
            let fields = [
                Series::new(LANG_STRING_VALUE_FIELD.into(), values),
                Series::new(LANG_STRING_LANG_FIELD.into(), langs),
            ];
            StructChunked::from_series(OBJECT_COL_NAME.into(), len, fields.iter())
                .map_err(polars_error)?
                .into_series()
        } else if object_type.stored_cat() {
            Series::new(OBJECT_COL_NAME.into(), objects)
        } else {
            let rows = IdxCa::from_vec(
                PlSmallStr::EMPTY,
                objects.into_iter().map(|i| i as IdxSize).collect(),
            );
            let mut s = self.typed_values[object_index]
                .as_ref()
                .unwrap()
                .take(&rows)
                .map_err(polars_error)?;
            s.rename(OBJECT_COL_NAME.into());
            s
        };
        let df = DataFrame::new(len, vec![subjects.into_column(), objects.into_column()])
            .map_err(polars_error)?;
        let object = if object_type.stored_cat() {
            Some(CatType::from_base_rdf_node_type(&object_type))
        } else {
            None
        };
        Ok(CatTriples {
            encoded_triples: EncodedTriples {
                df,
                subject: Some(CatType::from_base_rdf_node_type(&subject_type)),
                subject_local_cat_uuid: None,
                object,
                object_local_cat_uuid: None,
            },
            predicate: predicates[predicate as usize].clone(),
            graph: graph.clone(),
            subject_type,
            object_type,
            local_cats: vec![],
        })
    }
}

fn polars_error(e: impl std::fmt::Display) -> TriplestoreError {
    TriplestoreError::HDTError(e.to_string())
}

/// Zero based predicate index, subject type and object type.
type GroupKey = (u32, u16, u16);

/// The subject and object encodings of a range of triples, grouped by predicate and types.
#[derive(Default)]
struct TripleGroups {
    index: HashMap<GroupKey, usize>,
    groups: Vec<(GroupKey, Vec<u32>, Vec<u32>)>,
}

impl TripleGroups {
    fn scan(
        triples: &TriplesScan,
        range: Range<usize>,
        terms: &DictionaryEncoding,
        num_predicates: usize,
    ) -> Self {
        let mut out = TripleGroups::default();
        let mut last: Option<(GroupKey, usize)> = None;
        triples.for_each_in(range, |s, p, o| {
            let s = terms.subject(s);
            let o = terms.object(o);
            if s.ty == INVALID || o.ty == INVALID || p == 0 || p > num_predicates {
                return;
            }
            let key = ((p - 1) as u32, s.ty, o.ty);
            let i = match last {
                Some((k, i)) if k == key => i,
                _ => {
                    let groups = &mut out.groups;
                    let i = *out.index.entry(key).or_insert_with(|| {
                        groups.push((key, vec![], vec![]));
                        groups.len() - 1
                    });
                    last = Some((key, i));
                    i
                }
            };
            let (_, subjects, objects) = &mut out.groups[i];
            subjects.push(s.v);
            objects.push(o.v);
        });
        out
    }
}

/// Concatenates the groups of the parallel scans with the same key.
fn merge_groups(parts: Vec<TripleGroups>) -> Vec<(GroupKey, Vec<u32>, Vec<u32>)> {
    let mut parts = parts.into_iter();
    let Some(mut merged) = parts.next() else {
        return vec![];
    };
    for part in parts {
        for (key, subjects, objects) in part.groups {
            if let Some(i) = merged.index.get(&key) {
                let (_, s, o) = &mut merged.groups[*i];
                s.extend(subjects);
                o.extend(objects);
            } else {
                merged.index.insert(key, merged.groups.len());
                merged.groups.push((key, subjects, objects));
            }
        }
    }
    merged.groups
}

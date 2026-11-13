//! Writes a graph as HDT straight from the triplestore's categorical encoding.
//!
//! Terms are identified by their encoding (the u32 of IRIs, blank nodes and string-like literals in
//! the global cats), so the triples are deduplicated and mapped to HDT IDs as integers, and only
//! the distinct terms are decoded to the strings of the dictionary. Literals that are not cat
//! stored are formatted once per distinct value.

use crate::errors::TriplestoreError;
use crate::Triplestore;
use hdt::containers::rdf::{Id, Literal as HdtLiteral, Term as HdtTerm, Triple as HdtTriple};
use hdt::containers::ControlInfo;
use hdt::dict_sect_pfc::DictSectPFC;
use hdt::four_sect_dict::FourSectDict;
use hdt::header::Header;
use hdt::triples::write_bitmap_triples;
use oxrdf::vocab::rdf;
use oxrdf::{Literal, NamedNode};
use polars::prelude::{
    col, Column, DataFrame, IntoColumn, IntoLazy, JoinArgs, JoinType, NamedFrom, PlHashMap, Series,
};
use rayon::prelude::*;
use representation::cats::{CatType, Cats};
use representation::dataset::NamedGraph;
use representation::polars_to_rdf::column_as_terms;
use representation::{
    BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME,
    SUBJECT_COL_NAME,
};
use std::collections::{BTreeSet, HashMap};
use std::io::{BufWriter, Write};
use std::time::Instant;
use tracing::debug;

const BLOCK_SIZE: usize = 16;
const BASE_IRI: &str = "https://github.com/DataTreehouse/maplib";
const ROW_INDEX_COL: &str = "__hdt_row";

/// A term of the graph, identified by its encoding.
#[derive(Hash, Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
enum TermKey {
    Iri(u32),
    Blank(u32),
    /// Cat stored literal: index into the cat literal datatypes, and the u32 of the value.
    Cat(u16, u32),
    /// Language string: the u32s of the value and the language tag.
    Lang(u32, u32),
    /// Other literal: index into the distinct formatted literals.
    Typed(u32),
}

/// The triples of one predicate and subject/object type, as term keys.
struct KeyedTriples {
    predicate: usize,
    subjects: Vec<TermKey>,
    objects: Vec<TermKey>,
}

/// The object column of a table whose literals are formatted rather than cat stored: the distinct
/// formatted literals, and per row the index of its literal.
struct TypedObjects {
    strings: Vec<String>,
    subjects: Vec<u32>,
    rows: Vec<u32>,
}

pub(super) fn write_hdt<W: Write>(
    triplestore: &mut Triplestore,
    buf: &mut W,
    graph: &NamedGraph,
) -> Result<(), TriplestoreError> {
    let start = Instant::now();
    let mut predicates: Vec<NamedNode> = vec![];
    let mut tables: Vec<(usize, BaseRDFNodeType, BaseRDFNodeType, DataFrame)> = vec![];
    if triplestore.does_graph_exist(graph) {
        for (predicate, df_map) in triplestore.graph_triples_map.get(graph).unwrap() {
            let p = predicates.len();
            predicates.push(predicate.clone());
            for ((subject_type, object_type), tt) in df_map {
                for (lf, _) in tt.get_lazy_frames(&None, &None)? {
                    let df = lf
                        .select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)])
                        .collect()
                        .map_err(polars_error)?;
                    tables.push((p, subject_type.clone(), object_type.clone(), df));
                }
            }
        }
    }
    let global_cats = triplestore.global_cats.clone();
    let cats = global_cats.read().unwrap();

    let mut cat_datatypes: Vec<NamedNode> = vec![];
    for (_, _, object_type, _) in &tables {
        if let BaseRDFNodeType::Literal(dt, _) = object_type {
            if object_type.stored_cat()
                && !object_type.is_lang_string()
                && !cat_datatypes.contains(dt)
            {
                cat_datatypes.push(dt.clone());
            }
        }
    }

    // Translate every table to term keys. Formatted literals are deduplicated per table first,
    // then across tables.
    let keyed: Vec<Result<(KeyedTriples, Option<TypedObjects>), TriplestoreError>> = tables
        .into_par_iter()
        .map(|(predicate, subject_type, object_type, df)| {
            keyed_triples(
                predicate,
                &subject_type,
                &object_type,
                df,
                &cat_datatypes,
                &global_cats,
            )
        })
        .collect();
    let mut keyed_tables = Vec::with_capacity(keyed.len());
    let mut typed_ids: HashMap<String, u32> = HashMap::new();
    let mut typed_strings: Vec<String> = vec![];
    for k in keyed {
        let (mut kt, typed) = k?;
        if let Some(typed) = typed {
            let global: Vec<u32> = typed
                .strings
                .into_iter()
                .map(|s| {
                    let next = typed_strings.len() as u32;
                    *typed_ids.entry(s).or_insert_with_key(|s| {
                        typed_strings.push(s.clone());
                        next
                    })
                })
                .collect();
            kt.objects = typed
                .rows
                .into_iter()
                .map(|r| TermKey::Typed(global[r as usize]))
                .collect();
        }
        keyed_tables.push(kt);
    }
    drop(typed_ids);

    // The distinct subjects and objects, and from them the four dictionary sections.
    let subject_keys = sorted_distinct(keyed_tables.par_iter().map(|t| &t.subjects));
    let object_keys = sorted_distinct(keyed_tables.par_iter().map(|t| &t.objects));
    let (shared, subject_only, object_only) = split_shared(subject_keys, object_keys);
    let decoder = TermDecoder {
        cats: &cats,
        cat_datatypes: &cat_datatypes,
        typed_strings: &typed_strings,
    };
    let shared = decoder.section(shared);
    let subject_only = decoder.section(subject_only);
    let object_only = decoder.section(object_only);
    let mut predicate_strings: Vec<(&str, usize)> = predicates
        .iter()
        .enumerate()
        .map(|(i, p)| (p.as_str(), i))
        .collect();
    predicate_strings.par_sort_unstable();

    let num_shared = shared.len() as u32;
    let mut subject_ids: PlHashMap<TermKey, u32> = PlHashMap::default();
    let mut object_ids: PlHashMap<TermKey, u32> = PlHashMap::default();
    for (i, (_, k)) in shared.iter().enumerate() {
        subject_ids.insert(*k, i as u32 + 1);
        object_ids.insert(*k, i as u32 + 1);
    }
    for (i, (_, k)) in subject_only.iter().enumerate() {
        subject_ids.insert(*k, num_shared + i as u32 + 1);
    }
    for (i, (_, k)) in object_only.iter().enumerate() {
        object_ids.insert(*k, num_shared + i as u32 + 1);
    }
    let mut predicate_ids = vec![0u32; predicates.len()];
    for (i, (_, p)) in predicate_strings.iter().enumerate() {
        predicate_ids[*p] = i as u32 + 1;
    }

    let (subject_ids, object_ids) = (&subject_ids, &object_ids);
    let mut triples: Vec<[u32; 3]> = keyed_tables
        .par_iter()
        .flat_map_iter(|t| {
            let p = predicate_ids[t.predicate];
            t.subjects
                .iter()
                .zip(&t.objects)
                .map(move |(s, o)| [subject_ids[s], p, object_ids[o]])
        })
        .collect();
    drop(keyed_tables);
    triples.par_sort_unstable();
    triples.dedup();

    let dict = FourSectDict {
        shared: compress(&shared),
        subjects: compress(&subject_only),
        predicates: DictSectPFC::compress_iter(
            predicate_strings.iter().map(|(s, _)| *s),
            predicate_strings.len(),
            BLOCK_SIZE,
        ),
        objects: compress(&object_only),
    };
    drop(cats);
    let header = statistics_header(&dict, triples.len());
    debug!(
        "Preparing {} triples for HDT took {} seconds",
        triples.len(),
        start.elapsed().as_secs_f32()
    );

    let mut writer = BufWriter::new(buf);
    ControlInfo::global()
        .write(&mut writer)
        .map_err(hdt_error)?;
    header.write(&mut writer).map_err(hdt_error)?;
    dict.write(&mut writer).map_err(hdt_error)?;
    write_bitmap_triples(&triples, &mut writer).map_err(hdt_error)?;
    writer.flush().map_err(hdt_error)?;
    Ok(())
}

fn keyed_triples(
    predicate: usize,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
    df: DataFrame,
    cat_datatypes: &[NamedNode],
    global_cats: &representation::cats::LockedCats,
) -> Result<(KeyedTriples, Option<TypedObjects>), TriplestoreError> {
    let to_subject_key = if subject_type.is_blank_node() {
        TermKey::Blank
    } else {
        TermKey::Iri
    };
    if !object_type.stored_cat() {
        let typed = typed_objects(df, object_type, global_cats)?;
        let subjects = typed.subjects.iter().map(|u| to_subject_key(*u)).collect();
        return Ok((
            KeyedTriples {
                predicate,
                subjects,
                objects: vec![],
            },
            Some(typed),
        ));
    }
    let subjects: Vec<TermKey> = u32_column(df.column(SUBJECT_COL_NAME).unwrap())?
        .into_iter()
        .map(to_subject_key)
        .collect();
    let object_column = df.column(OBJECT_COL_NAME).unwrap();
    let objects: Vec<TermKey> = match object_type {
        BaseRDFNodeType::IRI => u32_column(object_column)?
            .into_iter()
            .map(TermKey::Iri)
            .collect(),
        BaseRDFNodeType::BlankNode => u32_column(object_column)?
            .into_iter()
            .map(TermKey::Blank)
            .collect(),
        BaseRDFNodeType::Literal(dt, _) => {
            if object_type.is_lang_string() {
                let s = object_column
                    .as_materialized_series()
                    .struct_()
                    .map_err(polars_error)?;
                let values = s
                    .field_by_name(LANG_STRING_VALUE_FIELD)
                    .map_err(polars_error)?;
                let langs = s
                    .field_by_name(LANG_STRING_LANG_FIELD)
                    .map_err(polars_error)?;
                u32_series(&values)?
                    .into_iter()
                    .zip(u32_series(&langs)?)
                    .map(|(v, l)| TermKey::Lang(v, l))
                    .collect()
            } else {
                let t = cat_datatypes.iter().position(|x| x == dt).unwrap() as u16;
                u32_column(object_column)?
                    .into_iter()
                    .map(|u| TermKey::Cat(t, u))
                    .collect()
            }
        }
        BaseRDFNodeType::None => vec![],
    };
    Ok((
        KeyedTriples {
            predicate,
            subjects,
            objects,
        },
        None,
    ))
}

/// Formats the distinct objects of a table whose literals are not cat stored.
fn typed_objects(
    df: DataFrame,
    object_type: &BaseRDFNodeType,
    global_cats: &representation::cats::LockedCats,
) -> Result<TypedObjects, TriplestoreError> {
    let distinct = df
        .column(OBJECT_COL_NAME)
        .unwrap()
        .unique()
        .map_err(polars_error)?;
    let n = distinct.len();
    let terms = column_as_terms(
        &distinct,
        &object_type.clone().into_default_stored_rdf_node_state(),
        global_cats.clone(),
        false,
    );
    let strings = terms
        .into_iter()
        .map(|t| t.map(|t| t.to_string()).unwrap_or_default())
        .collect();
    let rows = DataFrame::new(
        n,
        vec![
            distinct,
            Series::new(ROW_INDEX_COL.into(), (0..n as u32).collect::<Vec<_>>()).into_column(),
        ],
    )
    .map_err(polars_error)?;
    let height = df.height();
    let joined = df
        .clone()
        .lazy()
        .join(
            rows.lazy(),
            [col(OBJECT_COL_NAME)],
            [col(OBJECT_COL_NAME)],
            JoinArgs::new(JoinType::Inner),
        )
        .map_err(polars_error)?
        .select([col(SUBJECT_COL_NAME), col(ROW_INDEX_COL)])
        .collect()
        .map_err(polars_error)?;
    if joined.height() == height {
        return Ok(TypedObjects {
            strings,
            subjects: u32_column(joined.column(SUBJECT_COL_NAME).unwrap())?,
            rows: u32_column(joined.column(ROW_INDEX_COL).unwrap())?,
        });
    }
    // Some values do not equal themselves in a join (e.g. NaN), so format every row instead.
    let terms = column_as_terms(
        df.column(OBJECT_COL_NAME).unwrap(),
        &object_type.clone().into_default_stored_rdf_node_state(),
        global_cats.clone(),
        false,
    );
    let mut ids: HashMap<String, u32> = HashMap::new();
    let mut strings = vec![];
    let rows = terms
        .into_iter()
        .map(|t| {
            let s = t.map(|t| t.to_string()).unwrap_or_default();
            let next = strings.len() as u32;
            *ids.entry(s).or_insert_with_key(|s| {
                strings.push(s.clone());
                next
            })
        })
        .collect();
    Ok(TypedObjects {
        strings,
        subjects: u32_column(df.column(SUBJECT_COL_NAME).unwrap())?,
        rows,
    })
}

fn u32_column(c: &Column) -> Result<Vec<u32>, TriplestoreError> {
    u32_series(c.as_materialized_series())
}

fn u32_series(s: &Series) -> Result<Vec<u32>, TriplestoreError> {
    let ca = s.u32().map_err(polars_error)?;
    if ca.null_count() > 0 {
        return Err(TriplestoreError::HDTError(
            "cannot write null terms to HDT".to_string(),
        ));
    }
    Ok(ca.into_no_null_iter().collect())
}

fn sorted_distinct<'a>(keys: impl ParallelIterator<Item = &'a Vec<TermKey>>) -> Vec<TermKey> {
    let mut all: Vec<TermKey> = keys
        .map(|k| {
            let mut k = k.clone();
            k.sort_unstable();
            k.dedup();
            k
        })
        .flatten_iter()
        .collect();
    all.par_sort_unstable();
    all.dedup();
    all
}

/// Splits sorted, distinct subject and object keys into the shared, subject only and object only
/// terms.
fn split_shared(
    subjects: Vec<TermKey>,
    objects: Vec<TermKey>,
) -> (Vec<TermKey>, Vec<TermKey>, Vec<TermKey>) {
    let (mut shared, mut subject_only, mut object_only) = (vec![], vec![], vec![]);
    let (mut i, mut j) = (0, 0);
    while i < subjects.len() && j < objects.len() {
        match subjects[i].cmp(&objects[j]) {
            std::cmp::Ordering::Less => {
                subject_only.push(subjects[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                object_only.push(objects[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                shared.push(subjects[i]);
                i += 1;
                j += 1;
            }
        }
    }
    subject_only.extend_from_slice(&subjects[i..]);
    object_only.extend_from_slice(&objects[j..]);
    (shared, subject_only, object_only)
}

/// Decodes term keys to the strings of the HDT dictionary.
struct TermDecoder<'a> {
    cats: &'a Cats,
    cat_datatypes: &'a [NamedNode],
    typed_strings: &'a [String],
}

impl TermDecoder<'_> {
    /// The strings of a dictionary section with their keys, in dictionary (byte) order.
    fn section(&self, keys: Vec<TermKey>) -> Vec<(String, TermKey)> {
        let mut out: Vec<(String, TermKey)> = keys
            .into_par_iter()
            .with_min_len(4096)
            .map(|k| (self.decode(k), k))
            .collect();
        out.par_sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        out
    }

    fn decode(&self, k: TermKey) -> String {
        let decode = |t: &CatType, u: u32| -> String {
            self.cats
                .cat_map
                .get(t)
                .and_then(|e| e.maps.maybe_decode(&u))
                .map(|s| s.into_owned())
                .unwrap_or_default()
        };
        match k {
            TermKey::Iri(u) => decode(&CatType::IRI, u),
            TermKey::Blank(u) => format!("_:{}", decode(&CatType::Blank, u)),
            TermKey::Cat(t, u) => {
                let dt = &self.cat_datatypes[t as usize];
                let value = decode(&CatType::Literal(dt.clone()), u);
                Literal::new_typed_literal(value, dt.clone()).to_string()
            }
            TermKey::Lang(v, l) => {
                let t = CatType::Literal(rdf::LANG_STRING.into_owned());
                Literal::new_language_tagged_literal_unchecked(decode(&t, v), decode(&t, l))
                    .to_string()
            }
            TermKey::Typed(i) => self.typed_strings[i as usize].clone(),
        }
    }
}

fn compress(section: &[(String, TermKey)]) -> DictSectPFC {
    DictSectPFC::compress_iter(
        section.iter().map(|(s, _)| s.as_str()),
        section.len(),
        BLOCK_SIZE,
    )
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
    insert_literal(
        &mut body,
        &base,
        VOID_PROPERTIES,
        dict.predicates.num_strings(),
    );
    insert_literal(&mut body, &base, VOID_DISTINCT_SUBJECTS, distinct_subjects);
    insert_literal(&mut body, &base, VOID_DISTINCT_OBJECTS, distinct_objects);

    insert_id(&mut body, &base, HDT_STATISTICAL_INFORMATION, &stats_id);
    insert_id(&mut body, &base, HDT_STATISTICAL_INFORMATION, &pub_id);
    insert_id(&mut body, &base, HDT_FORMAT_INFORMATION, &format_id);
    insert_id(&mut body, &format_id, HDT_DICTIONARY, &dict_id);
    insert_id(&mut body, &format_id, HDT_TRIPLES, &triples_id);

    insert_literal(
        &mut body,
        &dict_id,
        HDT_DICT_SHARED_SO,
        dict.shared.num_strings(),
    );
    insert_literal(&mut body, &dict_id, HDT_DICT_MAPPING, "1");
    insert_literal(
        &mut body,
        &dict_id,
        HDT_DICT_SIZE_STRINGS,
        dict.size_in_bytes(),
    );
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
    body.insert(HdtTriple::new(
        s.clone(),
        p.to_owned(),
        HdtTerm::Id(o.clone()),
    ));
}

fn hdt_error(e: impl std::fmt::Display) -> TriplestoreError {
    TriplestoreError::HDTError(e.to_string())
}

fn polars_error(e: impl std::fmt::Display) -> TriplestoreError {
    TriplestoreError::HDTError(e.to_string())
}

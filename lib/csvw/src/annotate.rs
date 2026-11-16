//! Creating annotated tables from metadata and parsed tabular data, see
//! <https://www.w3.org/TR/tabular-metadata/#annotating-tables>.

use crate::datatypes::{parse_cell, CellParsing, CellValue};
use crate::errors::{meta_err, CsvwError};
use crate::jsonld::AnnotationTriple;
use crate::loader::{normalize_url, Loader};
use crate::metadata::{ColumnDesc, Datatype, ForeignKey, Inherited, TableDesc, TableGroupDesc};
use crate::parse::{parse_tabular, ParsedTable};
use crate::Warnings;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct AnnColumn {
    /// The column number, starting at 1.
    pub number: usize,
    pub source_number: usize,
    pub name: String,
    pub titles: Vec<(String, String)>,
    pub virtual_: bool,
    pub suppress_output: bool,
    pub about_url: Option<String>,
    pub property_url: Option<String>,
    pub value_url: Option<String>,
    pub parsing: CellParsing,
    pub lang: String,
    pub ordered: bool,
}

#[derive(Debug, Clone)]
pub struct AnnRow {
    pub number: usize,
    pub source_number: usize,
    /// One value per column, virtual columns are null.
    pub values: Vec<CellValue>,
}

#[derive(Debug, Clone)]
pub struct AnnTable {
    pub url: String,
    pub id: Option<String>,
    pub suppress_output: bool,
    pub columns: Vec<AnnColumn>,
    pub rows: Vec<AnnRow>,
    pub annotations: Vec<AnnotationTriple>,
    /// Indices of the columns holding the titles of rows.
    pub row_titles: Vec<usize>,
    pub foreign_keys: Vec<ForeignKey>,
    pub schema_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AnnGroup {
    pub id: Option<String>,
    pub tables: Vec<AnnTable>,
    pub annotations: Vec<AnnotationTriple>,
}

fn default_name(titles: &[(String, String)], number: usize) -> String {
    if let Some((_, t)) = titles.first() {
        crate::metadata::encode_name(t)
    } else {
        format!("_col.{number}")
    }
}

fn lang_compatible(a: &str, b: &str) -> bool {
    a == "und"
        || b == "und"
        || a == b
        || a.starts_with(&format!("{b}-"))
        || b.starts_with(&format!("{a}-"))
}

fn titles_intersect(a: &[(String, String)], b: &[(String, String)]) -> bool {
    a.iter()
        .any(|(la, ta)| b.iter().any(|(lb, tb)| ta == tb && lang_compatible(la, lb)))
}

/// Checks that a column description is compatible with the embedded metadata (the header).
fn check_compatibility(
    desc: &[ColumnDesc],
    parsed: &ParsedTable,
    url: &str,
    lang: &str,
    warnings: &mut Warnings,
) {
    let non_virtual: Vec<&ColumnDesc> = desc.iter().filter(|c| !c.virtual_).collect();
    if !parsed.titles.is_empty() && non_virtual.len() != parsed.titles.len() {
        warnings.push(format!(
            "the metadata of {url} describes {} columns, the file has {}",
            non_virtual.len(),
            parsed.titles.len()
        ));
        return;
    }
    for (i, c) in non_virtual.iter().enumerate() {
        let Some(em_titles) = parsed.titles.get(i) else {
            continue;
        };
        if em_titles.is_empty() || c.titles.is_empty() && c.name.is_none() {
            continue;
        }
        let em: Vec<(String, String)> = em_titles
            .iter()
            .map(|t| (lang.to_string(), t.clone()))
            .collect();
        let ok = if !c.titles.is_empty() {
            titles_intersect(&c.titles, &em)
        } else {
            em_titles.contains(c.name.as_ref().unwrap())
        };
        if !ok {
            warnings.push(format!(
                "column {} of {url} has the titles {:?} in the file, which are not compatible with the metadata",
                i + 1,
                em_titles
            ));
        }
    }
}

fn make_column(
    number: usize,
    source_number: usize,
    desc: Option<&ColumnDesc>,
    em_titles: &[String],
    parent: &Inherited,
) -> AnnColumn {
    let (name, titles, virtual_, suppress_output, inh) = match desc {
        Some(d) => (
            d.name.clone().or(d.default_name.clone()),
            d.titles.clone(),
            d.virtual_,
            d.suppress_output,
            d.inherited.inherit(parent),
        ),
        None => (
            None,
            em_titles
                .iter()
                .map(|t| ("und".to_string(), t.clone()))
                .collect(),
            false,
            false,
            parent.clone(),
        ),
    };
    let name = match (name, desc) {
        (Some(n), _) => n,
        (None, Some(_)) => format!("_col.{number}"),
        (None, None) => default_name(&titles, number),
    };
    AnnColumn {
        number,
        source_number,
        name,
        titles,
        virtual_,
        suppress_output,
        about_url: inh.about_url.clone(),
        property_url: inh.property_url.clone(),
        value_url: inh.value_url.clone(),
        parsing: CellParsing {
            datatype: inh.datatype.clone().unwrap_or(Datatype::builtin("string")),
            default: inh.default.clone().unwrap_or_default(),
            null: inh.null.clone().unwrap_or(vec![String::new()]),
            required: inh.required.unwrap_or(false),
            separator: inh.separator.clone().unwrap_or(None),
        },
        lang: inh.lang.clone().unwrap_or("und".to_string()),
        ordered: inh.ordered.unwrap_or(false),
    }
}

pub fn annotate_table(
    desc: &TableDesc,
    loader: &Loader,
    warnings: &mut Warnings,
) -> Result<AnnTable, CsvwError> {
    let bytes = loader.load_required(&desc.url)?;
    let dialect = desc.dialect.clone().unwrap_or_default();
    let parsed = parse_tabular(&bytes, &dialect, &desc.url)?;
    let mut columns = vec![];
    let schema_columns: Vec<ColumnDesc> = desc
        .schema
        .as_ref()
        .map(|s| s.columns.clone())
        .unwrap_or_default();
    let schema_inherited = desc
        .schema
        .as_ref()
        .map(|s| s.inherited.inherit(&desc.inherited))
        .unwrap_or(desc.inherited.clone());
    if desc.schema.is_some() {
        let lang = schema_inherited.lang.as_deref().unwrap_or("und");
        check_compatibility(&schema_columns, &parsed, &desc.url, lang, warnings);
    }
    let non_virtual: Vec<&ColumnDesc> = schema_columns.iter().filter(|c| !c.virtual_).collect();
    let n_data = parsed.n_columns.max(non_virtual.len());
    for i in 0..n_data {
        let empty = vec![];
        // Titles from the file only name the columns when there is no schema in the metadata
        let em_titles = if desc.schema.is_none() {
            parsed.titles.get(i).unwrap_or(&empty)
        } else {
            &empty
        };
        columns.push(make_column(
            i + 1,
            i + 1 + dialect.skip_columns,
            non_virtual.get(i).copied(),
            em_titles,
            &schema_inherited,
        ));
    }
    for c in schema_columns.iter().filter(|c| c.virtual_) {
        let n = columns.len() + 1;
        columns.push(make_column(n, n, Some(c), &[], &schema_inherited));
    }
    let mut names = HashSet::new();
    for c in &columns {
        if !names.insert(c.name.clone()) {
            return meta_err(format!("duplicate column name {} in {}", c.name, desc.url));
        }
    }
    let mut rows = vec![];
    for r in &parsed.rows {
        let mut values = vec![];
        for c in &columns {
            if c.virtual_ {
                values.push(CellValue::Null);
                continue;
            }
            let raw = r.cells.get(c.number - 1).map(|x| x.as_str()).unwrap_or("");
            let (v, errors) = parse_cell(raw, &c.parsing);
            for e in errors {
                warnings.push(format!(
                    "{} row {} column {}: {e}",
                    desc.url, r.source_number, c.name
                ));
            }
            values.push(v);
        }
        rows.push(AnnRow {
            number: r.number,
            source_number: r.source_number,
            values,
        });
    }
    let (row_titles, foreign_keys, schema_id, primary_key) = match &desc.schema {
        Some(s) => (
            s.row_titles
                .iter()
                .filter_map(|n| columns.iter().position(|c| &c.name == n))
                .collect(),
            s.foreign_keys.clone(),
            s.id.clone(),
            s.primary_key.clone(),
        ),
        None => (vec![], vec![], None, vec![]),
    };
    let mut annotations = desc.annotations.clone();
    if desc.schema.is_none() {
        // Comments are part of the embedded metadata
        for c in &parsed.comments {
            annotations.push(AnnotationTriple {
                subject: crate::jsonld::ATerm::This,
                predicate: "http://www.w3.org/2000/01/rdf-schema#comment".to_string(),
                object: crate::jsonld::ATerm::Literal {
                    lexical: c.clone(),
                    datatype: None,
                    language: None,
                },
            });
        }
    }
    let table = AnnTable {
        url: desc.url.clone(),
        id: desc.id.clone(),
        suppress_output: desc.suppress_output,
        columns,
        rows,
        annotations,
        row_titles,
        foreign_keys,
        schema_id,
    };
    if !primary_key.is_empty() {
        let idx = column_indices(&table, &primary_key)?;
        let mut seen = HashSet::new();
        for r in &table.rows {
            let key = key_of(r, &idx);
            if !seen.insert(key.clone()) {
                warnings.push(format!(
                    "{}: duplicate primary key {key:?} in row {}",
                    table.url, r.source_number
                ));
            }
        }
    }
    Ok(table)
}

fn column_indices(t: &AnnTable, names: &[String]) -> Result<Vec<usize>, CsvwError> {
    names
        .iter()
        .map(|n| {
            t.columns
                .iter()
                .position(|c| &c.name == n)
                .ok_or(CsvwError::MetadataError(format!(
                    "the column {n} does not exist in {}",
                    t.url
                )))
        })
        .collect()
}

fn key_of(r: &AnnRow, idx: &[usize]) -> Vec<String> {
    idx.iter()
        .map(|i| match &r.values[*i] {
            CellValue::Null => String::new(),
            CellValue::Single(v) => v.lexical.clone(),
            CellValue::List(l) => l
                .iter()
                .map(|x| x.as_ref().map(|x| x.lexical.clone()).unwrap_or_default())
                .collect::<Vec<_>>()
                .join(" "),
        })
        .collect()
}

/// Validates the foreign keys between the tables of the group.
fn check_foreign_keys(g: &AnnGroup) -> Result<(), CsvwError> {
    for t in &g.tables {
        for fk in &t.foreign_keys {
            let target = if let Some(r) = &fk.resource {
                let n = normalize_url(r);
                g.tables.iter().find(|x| normalize_url(&x.url) == n)
            } else {
                let s = fk.schema_reference.as_ref().unwrap();
                g.tables.iter().find(|x| x.schema_id.as_ref() == Some(s))
            };
            let Some(target) = target else {
                return meta_err("the table referenced by a foreign key does not exist");
            };
            let source_idx = column_indices(t, &fk.column_reference)?;
            let target_idx = column_indices(target, &fk.referenced_columns)?;
            let keys: HashSet<Vec<String>> =
                target.rows.iter().map(|r| key_of(r, &target_idx)).collect();
            for r in &t.rows {
                let k = key_of(r, &source_idx);
                if k.iter().all(|x| x.is_empty()) {
                    continue;
                }
                if !keys.contains(&k) {
                    return meta_err(format!(
                        "row {} of {} references {k:?} which does not exist in {}",
                        r.source_number, t.url, target.url
                    ));
                }
            }
        }
    }
    Ok(())
}

pub fn annotate_group(
    desc: &TableGroupDesc,
    loader: &Loader,
    warnings: &mut Warnings,
) -> Result<AnnGroup, CsvwError> {
    let mut tables = vec![];
    for t in &desc.tables {
        tables.push(annotate_table(t, loader, warnings)?);
    }
    let g = AnnGroup {
        id: desc.id.clone(),
        tables,
        annotations: desc.annotations.clone(),
    };
    check_foreign_keys(&g)?;
    Ok(g)
}

/// Annotated table from embedded metadata only (a CSV file without metadata).
pub fn embedded_table_desc(url: &str) -> TableDesc {
    TableDesc {
        url: url.to_string(),
        id: None,
        suppress_output: false,
        dialect: None,
        schema: None,
        inherited: Inherited::default(),
        annotations: vec![],
    }
}

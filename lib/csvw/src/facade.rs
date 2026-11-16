//! Writes annotated tables as Facade-X graphs, which the generated SPARQL Update maps to the
//! CSVW RDF output.
//!
//! The representation of table number k is in its own graph:
//!
//! ```text
//! <root> a fx:root ;
//!     fx:child <row> .
//! <row> fx:childNumber 0 ;                 # as in Model.map_df
//!     csvwx:row 1 ;                         # the row number (_row)
//!     csvwx:sourceRow 2 ;                   # the line number in the file (_sourceRow)
//!     xyz:<column name> "canonical value" ; # cells with a valid value
//!     csvwx:typed_<column name> 1 ;         # valid values, when the datatype is not string
//!     csvwx:invalid_<column name> "raw" ;   # cells with an invalid value
//!     xyz:<list column> <list> .            # cells with a list value
//! <list> rdf:_1 "value" ;                   # valid list items (typed unless strings)
//!     csvwx:invalidItem_2 "raw" ;           # invalid list items
//!     csvwx:joined "value,raw" .            # the value used in URI templates
//! ```
//!
//! where csvwx is the namespace urn:maplib:csvw:. Null cells have no triples.
//!
//! The typed values are created here with maplib's own literal parsing instead of with STRDT in
//! the update, so that the values that maplib can not store with their datatype are found when
//! the cells are parsed (they are output as strings with a warning) instead of failing the update.

use crate::annotate::{AnnColumn, AnnGroup};
use crate::datatypes::CellValue;
use crate::vocab::{MAPLIB_CSVW_NS, PREFIXES, RDF_NS};
use oxrdf::vocab::xsd;
use oxrdf::NamedNode;
use polars::prelude::{Column, DataFrame, NamedFrom, Series};
use representation::constants::{FX_CHILD, FX_CHILD_NUMBER, FX_ROOT, XYZ_PREFIX_IRI};
use representation::dataset::NamedGraph;
use representation::{
    literal_time_zone, BaseRDFNodeType, SeriesBuilder, OBJECT_COL_NAME, SUBJECT_COL_NAME,
};
use std::collections::HashMap;
use triplestore::TriplesToAdd;

pub struct FacadeIris {
    pub base: String,
}

impl FacadeIris {
    pub fn table_graph(&self, k: usize) -> String {
        format!("{}/table/{k}", self.base)
    }
    pub fn prefix_graph(&self) -> String {
        format!("{}/prefixes", self.base)
    }
    pub fn root(&self, k: usize) -> String {
        format!("{}/table/{k}/root", self.base)
    }
    fn row(&self, k: usize, n: usize) -> String {
        format!("{}/table/{k}/row/{n}", self.base)
    }
    fn list(&self, k: usize, n: usize, col: usize) -> String {
        format!("{}/table/{k}/row/{n}/list/{col}", self.base)
    }
}

pub fn cell_predicate(name: &str) -> String {
    format!("{XYZ_PREFIX_IRI}{name}")
}

pub fn invalid_predicate(name: &str) -> String {
    format!("{MAPLIB_CSVW_NS}invalid_{name}")
}

pub fn typed_predicate(name: &str) -> String {
    format!("{MAPLIB_CSVW_NS}typed_{name}")
}

/// The datatype of the valid values of a column, when they are not plain or language tagged
/// strings.
pub fn typed_datatype(c: &AnnColumn) -> Option<String> {
    let dt = &c.parsing.datatype;
    if dt.base == "string" && dt.id.is_none() {
        None
    } else {
        Some(dt.iri())
    }
}

pub fn helper(local: &str) -> String {
    format!("{MAPLIB_CSVW_NS}{local}")
}

/// Subjects and objects, in the same order.
type Pairs<T> = (Vec<String>, Vec<T>);

/// Collects (subject, object) pairs per (predicate, object type, graph).
#[derive(Default)]
struct Collector {
    strings: HashMap<(String, String, bool), Pairs<String>>,
    ints: HashMap<(String, String), Pairs<i64>>,
    typed: HashMap<(String, String, BaseRDFNodeType), Pairs<String>>,
}

impl Collector {
    fn string(&mut self, g: &str, s: &str, p: &str, o: &str) {
        let e = self
            .strings
            .entry((g.to_string(), p.to_string(), false))
            .or_default();
        e.0.push(s.to_string());
        e.1.push(o.to_string());
    }
    fn iri(&mut self, g: &str, s: &str, p: &str, o: &str) {
        let e = self
            .strings
            .entry((g.to_string(), p.to_string(), true))
            .or_default();
        e.0.push(s.to_string());
        e.1.push(o.to_string());
    }
    fn int(&mut self, g: &str, s: &str, p: &str, o: i64) {
        let e = self.ints.entry((g.to_string(), p.to_string())).or_default();
        e.0.push(s.to_string());
        e.1.push(o);
    }

    fn typed(&mut self, g: &str, s: &str, p: &str, o: &str, datatype: &str) {
        let dt = NamedNode::new_unchecked(datatype);
        let offset = literal_time_zone(o, dt.as_ref());
        let e = self
            .typed
            .entry((
                g.to_string(),
                p.to_string(),
                BaseRDFNodeType::Literal(dt, offset),
            ))
            .or_default();
        e.0.push(s.to_string());
        e.1.push(o.to_string());
    }

    fn into_triples(self) -> Vec<TriplesToAdd> {
        let mut out = vec![];
        let graph = |g: &str| NamedGraph::NamedGraph(NamedNode::new_unchecked(g));
        for ((g, p, is_iri), (s, o)) in self.strings {
            let object_type = if is_iri {
                BaseRDFNodeType::IRI
            } else {
                BaseRDFNodeType::Literal(xsd::STRING.into_owned(), None)
            };
            let height = s.len();
            let df = DataFrame::new(
                height,
                vec![
                    Column::new(SUBJECT_COL_NAME.into(), s),
                    Column::new(OBJECT_COL_NAME.into(), o),
                ],
            )
            .unwrap();
            out.push(TriplesToAdd {
                df,
                subject_type: BaseRDFNodeType::IRI,
                object_cat_state: object_type.default_input_cat_state(),
                object_type,
                predicate: Some(NamedNode::new_unchecked(p)),
                graph: graph(&g),
                subject_cat_state: BaseRDFNodeType::IRI.default_input_cat_state(),
                predicate_cat_state: None,
            });
        }
        for ((g, p, object_type), (s, o)) in self.typed {
            let height = s.len();
            let mut builder = SeriesBuilder::new_with_capacity(&object_type, height);
            for v in &o {
                // The values were checked with the same parsing when the cells were parsed.
                if builder.parse_literal(v, None).is_err() {
                    builder.push_none();
                }
            }
            let df = DataFrame::new(
                height,
                vec![
                    Column::new(SUBJECT_COL_NAME.into(), s),
                    builder.into_series(OBJECT_COL_NAME).into(),
                ],
            )
            .unwrap();
            out.push(TriplesToAdd {
                df,
                subject_type: BaseRDFNodeType::IRI,
                object_cat_state: object_type.default_input_cat_state(),
                object_type,
                predicate: Some(NamedNode::new_unchecked(&p)),
                graph: graph(&g),
                subject_cat_state: BaseRDFNodeType::IRI.default_input_cat_state(),
                predicate_cat_state: None,
            });
        }
        for ((g, p), (s, o)) in self.ints {
            let object_type = BaseRDFNodeType::Literal(xsd::INTEGER.into_owned(), None);
            let height = s.len();
            let df = DataFrame::new(
                height,
                vec![
                    Column::new(SUBJECT_COL_NAME.into(), s),
                    Series::new(OBJECT_COL_NAME.into(), o).into(),
                ],
            )
            .unwrap();
            out.push(TriplesToAdd {
                df,
                subject_type: BaseRDFNodeType::IRI,
                object_cat_state: object_type.default_input_cat_state(),
                object_type,
                predicate: Some(NamedNode::new_unchecked(p)),
                graph: graph(&g),
                subject_cat_state: BaseRDFNodeType::IRI.default_input_cat_state(),
                predicate_cat_state: None,
            });
        }
        out
    }
}

pub fn facade_triples(g: &AnnGroup, iris: &FacadeIris) -> Vec<TriplesToAdd> {
    let mut c = Collector::default();
    let rdf_type = format!("{RDF_NS}type");
    for (k, t) in g.tables.iter().enumerate() {
        let graph = iris.table_graph(k);
        let root = iris.root(k);
        c.iri(&graph, &root, &rdf_type, FX_ROOT);
        for (i, r) in t.rows.iter().enumerate() {
            let row = iris.row(k, r.number);
            c.iri(&graph, &root, FX_CHILD, &row);
            c.int(&graph, &row, FX_CHILD_NUMBER, i as i64);
            c.int(&graph, &row, &helper("row"), r.number as i64);
            c.int(&graph, &row, &helper("sourceRow"), r.source_number as i64);
            for (col, v) in t.columns.iter().zip(r.values.iter()) {
                match v {
                    CellValue::Null => {}
                    CellValue::Single(v) => {
                        if v.valid {
                            c.string(&graph, &row, &cell_predicate(&col.name), &v.lexical);
                            if let Some(dt) = typed_datatype(col) {
                                c.typed(&graph, &row, &typed_predicate(&col.name), &v.lexical, &dt);
                            }
                        } else {
                            c.string(&graph, &row, &invalid_predicate(&col.name), &v.lexical);
                        }
                    }
                    CellValue::List(items) => {
                        let list = iris.list(k, r.number, col.number);
                        c.iri(&graph, &row, &cell_predicate(&col.name), &list);
                        let mut joined = vec![];
                        for (j, item) in items.iter().enumerate() {
                            let Some(item) = item else {
                                continue;
                            };
                            if item.valid {
                                let p = format!("{RDF_NS}_{}", j + 1);
                                match typed_datatype(col) {
                                    Some(dt) => c.typed(&graph, &list, &p, &item.lexical, &dt),
                                    None => c.string(&graph, &list, &p, &item.lexical),
                                }
                            } else {
                                c.string(
                                    &graph,
                                    &list,
                                    &helper(&format!("invalidItem_{}", j + 1)),
                                    &item.lexical,
                                );
                            }
                            joined.push(item.lexical.clone());
                        }
                        c.string(&graph, &list, &helper("joined"), &joined.join(","));
                    }
                }
            }
        }
    }
    let pg = iris.prefix_graph();
    for (p, ns) in PREFIXES {
        let s = format!("{pg}/{p}");
        c.string(&pg, &s, &helper("prefix"), p);
        c.string(&pg, &s, &helper("namespace"), ns);
    }
    c.into_triples()
}

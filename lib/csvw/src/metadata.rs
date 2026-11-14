//! Parsing and normalization of CSVW metadata documents, see
//! <https://www.w3.org/TR/tabular-metadata/>.

use crate::errors::{meta_err, CsvwError};
use crate::jsonld::{common_property_triples, AnnotationTriple};
use crate::loader::{resolve, Loader};
use crate::vocab::*;
use crate::Warnings;
use regex::Regex;
use serde_json::{Map, Value};
use std::sync::LazyLock;

/// The context a description object is interpreted in.
#[derive(Debug, Clone)]
pub struct Ctx {
    /// Base IRI for resolving links.
    pub base: String,
    /// Default language from @context.
    pub lang: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Inherited {
    pub about_url: Option<String>,
    pub property_url: Option<String>,
    pub value_url: Option<String>,
    pub datatype: Option<Datatype>,
    pub default: Option<String>,
    pub lang: Option<String>,
    pub null: Option<Vec<String>>,
    pub ordered: Option<bool>,
    pub required: Option<bool>,
    pub separator: Option<Option<String>>,
    pub text_direction: Option<String>,
}

impl Inherited {
    /// Values set on self take precedence over those on the parent.
    pub fn inherit(&self, parent: &Inherited) -> Inherited {
        Inherited {
            about_url: self.about_url.clone().or(parent.about_url.clone()),
            property_url: self.property_url.clone().or(parent.property_url.clone()),
            value_url: self.value_url.clone().or(parent.value_url.clone()),
            datatype: self.datatype.clone().or(parent.datatype.clone()),
            default: self.default.clone().or(parent.default.clone()),
            lang: self.lang.clone().or(parent.lang.clone()),
            null: self.null.clone().or(parent.null.clone()),
            ordered: self.ordered.or(parent.ordered),
            required: self.required.or(parent.required),
            separator: self.separator.clone().or(parent.separator.clone()),
            text_direction: self
                .text_direction
                .clone()
                .or(parent.text_direction.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Datatype {
    /// Name of the built-in base datatype.
    pub base: String,
    /// The datatype IRI, if it is not the IRI of the base.
    pub id: Option<String>,
    pub format: Option<Value>,
    pub length: Option<usize>,
    pub min_length: Option<usize>,
    pub max_length: Option<usize>,
    pub min_inclusive: Option<String>,
    pub max_inclusive: Option<String>,
    pub min_exclusive: Option<String>,
    pub max_exclusive: Option<String>,
}

impl Datatype {
    pub fn builtin(base: &str) -> Datatype {
        Datatype {
            base: base.to_string(),
            id: None,
            format: None,
            length: None,
            min_length: None,
            max_length: None,
            min_inclusive: None,
            max_inclusive: None,
            min_exclusive: None,
            max_exclusive: None,
        }
    }

    pub fn iri(&self) -> String {
        if let Some(id) = &self.id {
            id.clone()
        } else {
            builtin_datatype_iri(&self.base).to_string()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Trim {
    True,
    False,
    Start,
    End,
}

#[derive(Debug, Clone)]
pub struct Dialect {
    pub comment_prefix: Option<String>,
    pub delimiter: String,
    pub double_quote: bool,
    pub encoding: String,
    pub header_row_count: usize,
    pub line_terminators: Vec<String>,
    pub quote_char: Option<String>,
    pub skip_blank_rows: bool,
    pub skip_columns: usize,
    pub skip_rows: usize,
    pub trim: Trim,
}

impl Default for Dialect {
    fn default() -> Self {
        Dialect {
            comment_prefix: None,
            delimiter: ",".to_string(),
            double_quote: true,
            encoding: "utf-8".to_string(),
            header_row_count: 1,
            line_terminators: vec!["\r\n".to_string(), "\n".to_string()],
            quote_char: Some("\"".to_string()),
            skip_blank_rows: false,
            skip_columns: 0,
            skip_rows: 0,
            trim: Trim::True,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnDesc {
    /// The name property, which column references refer to.
    pub name: Option<String>,
    /// The name from the titles, when there is no name property.
    pub default_name: Option<String>,
    /// (language, title)
    pub titles: Vec<(String, String)>,
    pub virtual_: bool,
    pub suppress_output: bool,
    pub inherited: Inherited,
}

#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub column_reference: Vec<String>,
    /// The url of the referenced table, or None if referenced through a schema.
    pub resource: Option<String>,
    pub schema_reference: Option<String>,
    pub referenced_columns: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SchemaDesc {
    pub id: Option<String>,
    pub columns: Vec<ColumnDesc>,
    pub inherited: Inherited,
    pub row_titles: Vec<String>,
    pub primary_key: Vec<String>,
    pub foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug, Clone)]
pub struct TableDesc {
    pub url: String,
    pub id: Option<String>,
    pub suppress_output: bool,
    pub dialect: Option<Dialect>,
    pub schema: Option<SchemaDesc>,
    pub inherited: Inherited,
    pub annotations: Vec<AnnotationTriple>,
}

#[derive(Debug, Clone)]
pub struct TableGroupDesc {
    pub id: Option<String>,
    pub tables: Vec<TableDesc>,
    pub annotations: Vec<AnnotationTriple>,
}

static NAME_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^([A-Za-z0-9]|%[0-9A-Fa-f]{2})([A-Za-z0-9._]|%[0-9A-Fa-f]{2})*$").unwrap()
});

pub fn parse_json(iri: &str, bytes: &[u8]) -> Result<Value, CsvwError> {
    let s = String::from_utf8_lossy(bytes);
    let s = s.trim_start_matches('\u{feff}');
    serde_json::from_str(s).map_err(|x| CsvwError::JsonError(iri.to_string(), x.to_string()))
}

/// Parses the @context of a top level metadata document.
fn parse_context(
    obj: &Map<String, Value>,
    doc_iri: &str,
    warnings: &mut Warnings,
) -> Result<Ctx, CsvwError> {
    let mut ctx = Ctx {
        base: doc_iri.to_string(),
        lang: None,
    };
    let Some(c) = obj.get("@context") else {
        return meta_err("the metadata document has no @context");
    };
    match c {
        Value::String(s) if s == CSVW_CONTEXT => {}
        Value::Array(a) if a.len() == 2 => {
            if a[0] != Value::String(CSVW_CONTEXT.to_string()) {
                return meta_err(format!(
                    "the first element of @context must be {CSVW_CONTEXT}"
                ));
            }
            let Value::Object(m) = &a[1] else {
                return meta_err("the second element of @context must be an object");
            };
            for (k, v) in m {
                match k.as_str() {
                    "@base" => {
                        let Value::String(b) = v else {
                            return meta_err("@base must be a string");
                        };
                        ctx.base = resolve(doc_iri, b)?;
                    }
                    "@language" => {
                        let Value::String(l) = v else {
                            return meta_err("@language must be a string");
                        };
                        if oxilangtag::LanguageTag::parse(l.as_str()).is_ok() {
                            ctx.lang = Some(l.clone());
                        } else {
                            warnings.push(format!("ignoring the invalid @language {l}"));
                        }
                    }
                    k => {
                        return meta_err(format!(
                            "@context objects may only contain @base and @language, found {k}"
                        ))
                    }
                }
            }
        }
        Value::Array(a) if a.len() == 1 && a[0] == Value::String(CSVW_CONTEXT.to_string()) => {}
        _ => {
            return meta_err(format!(
                "@context must be {CSVW_CONTEXT} or an array starting with it"
            ))
        }
    }
    Ok(ctx)
}

pub struct MetadataParser<'a> {
    pub loader: &'a Loader,
    pub warnings: &'a mut Warnings,
}

/// The kind of description object, used for checking properties.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    TableGroup,
    Table,
    Schema,
    Column,
    Dialect,
    Datatype,
    Transformation,
    ForeignKey,
    Reference,
}

const INHERITED_PROPS: &[&str] = &[
    "aboutUrl",
    "datatype",
    "default",
    "lang",
    "null",
    "ordered",
    "propertyUrl",
    "required",
    "separator",
    "textDirection",
    "valueUrl",
];

fn allowed_props(kind: Kind) -> &'static [&'static str] {
    match kind {
        Kind::TableGroup => &[
            "tables",
            "dialect",
            "notes",
            "tableDirection",
            "tableSchema",
            "transformations",
        ],
        Kind::Table => &[
            "url",
            "dialect",
            "notes",
            "suppressOutput",
            "tableDirection",
            "tableSchema",
            "transformations",
        ],
        Kind::Schema => &["columns", "foreignKeys", "primaryKey", "rowTitles"],
        Kind::Column => &["name", "suppressOutput", "titles", "virtual"],
        Kind::Dialect => &[
            "commentPrefix",
            "delimiter",
            "doubleQuote",
            "encoding",
            "header",
            "headerRowCount",
            "lineTerminators",
            "quoteChar",
            "skipBlankRows",
            "skipColumns",
            "skipInitialSpace",
            "skipRows",
            "trim",
        ],
        Kind::Datatype => &[
            "base",
            "format",
            "length",
            "minLength",
            "maxLength",
            "minimum",
            "maximum",
            "minInclusive",
            "maxInclusive",
            "minExclusive",
            "maxExclusive",
        ],
        Kind::Transformation => &["url", "scriptFormat", "targetFormat", "source", "titles"],
        Kind::ForeignKey => &["columnReference", "reference"],
        Kind::Reference => &["resource", "schemaReference", "columnReference"],
    }
}

fn type_name(kind: Kind) -> &'static str {
    match kind {
        Kind::TableGroup => "TableGroup",
        Kind::Table => "Table",
        Kind::Schema => "Schema",
        Kind::Column => "Column",
        Kind::Dialect => "Dialect",
        Kind::Datatype => "Datatype",
        Kind::Transformation => "Template",
        Kind::ForeignKey => "ForeignKey",
        Kind::Reference => "TableReference",
    }
}

fn is_common_property(k: &str) -> bool {
    k.contains(':')
}

impl MetadataParser<'_> {
    fn warn(&mut self, s: impl Into<String>) {
        self.warnings.push(s.into());
    }

    /// Checks @id, @type and the property names of a description object. Returns the
    /// common properties (annotations) of the object as triples.
    fn check_object(
        &mut self,
        obj: &Map<String, Value>,
        kind: Kind,
        ctx: &Ctx,
        top_level: bool,
    ) -> Result<(Option<String>, Vec<AnnotationTriple>), CsvwError> {
        let mut id = None;
        let mut annotations = vec![];
        let inherited_ok = matches!(
            kind,
            Kind::TableGroup | Kind::Table | Kind::Schema | Kind::Column
        );
        for (k, v) in obj {
            match k.as_str() {
                "@context" if top_level => {}
                "@context" => return meta_err("@context is only allowed in the top level object"),
                "@id" => {
                    let Value::String(s) = v else {
                        // Invalid link properties are treated as the empty string
                        self.warn(format!("@id must be a string, found {v}, using \"\""));
                        id = Some(resolve(&ctx.base, "")?);
                        continue;
                    };
                    if s.starts_with("_:") {
                        return meta_err(format!("@id must not be a blank node, found {s}"));
                    }
                    id = Some(resolve(&ctx.base, s)?);
                }
                "@type" => {
                    let expected = type_name(kind);
                    if v != &Value::String(expected.to_string()) {
                        return meta_err(format!("@type must be {expected}, found {v}"));
                    }
                }
                k if k.starts_with('@') => {
                    return meta_err(format!("invalid property {k}"));
                }
                k if allowed_props(kind).contains(&k) => {}
                k if inherited_ok && INHERITED_PROPS.contains(&k) => {}
                k if is_common_property(k) => {
                    if matches!(kind, Kind::ForeignKey | Kind::Reference) {
                        return meta_err(format!(
                            "foreign key definitions may not contain other properties, found {k}"
                        ));
                    }
                    annotations.extend(common_property_triples(k, v, ctx)?);
                }
                k => {
                    if matches!(kind, Kind::ForeignKey | Kind::Reference) {
                        return meta_err(format!(
                            "foreign key definitions may not contain other properties, found {k}"
                        ));
                    }
                    self.warn(format!(
                        "ignoring unknown property {k} on {}",
                        type_name(kind)
                    ))
                }
            }
        }
        Ok((id, annotations))
    }

    /// Parses a top level metadata document into a table group.
    pub fn parse_top_level(
        &mut self,
        v: &Value,
        doc_iri: &str,
    ) -> Result<TableGroupDesc, CsvwError> {
        let Value::Object(obj) = v else {
            return meta_err("the metadata document must be a JSON object");
        };
        let ctx = parse_context(obj, doc_iri, self.warnings)?;
        if obj.contains_key("tables") {
            self.parse_table_group(obj, &ctx)
        } else if obj.contains_key("url") {
            let t = self.parse_table(obj, &ctx, true, None, None, &Inherited::default())?;
            Ok(TableGroupDesc {
                id: None,
                tables: vec![t],
                annotations: vec![],
            })
        } else if obj.get("@type") == Some(&Value::String("Table".to_string())) {
            meta_err("the url property is required in a Table")
        } else {
            meta_err("the tables property is required in a TableGroup")
        }
    }

    fn parse_table_group(
        &mut self,
        obj: &Map<String, Value>,
        ctx: &Ctx,
    ) -> Result<TableGroupDesc, CsvwError> {
        let (id, mut annotations) = self.check_object(obj, Kind::TableGroup, ctx, true)?;
        annotations.extend(self.parse_notes(obj, ctx)?);
        let inherited = self.parse_inherited(obj, ctx)?;
        let dialect = self.parse_dialect_prop(obj, ctx)?;
        let schema = self.parse_schema_prop(obj, ctx, &inherited)?;
        self.parse_transformations(obj, ctx)?;
        self.check_direction(obj, "tableDirection");
        let tables = match obj.get("tables") {
            Some(Value::Array(a)) => a,
            _ => return meta_err("tables must be an array of table descriptions"),
        };
        let mut out_tables = vec![];
        for t in tables {
            if let Value::Object(tobj) = t {
                out_tables.push(self.parse_table(
                    tobj,
                    ctx,
                    false,
                    dialect.as_ref(),
                    schema.as_ref(),
                    &inherited,
                )?);
            } else {
                self.warn("ignoring a table description that is not an object");
            }
        }
        if out_tables.is_empty() {
            return meta_err("tables must contain one or more table descriptions");
        }
        Ok(TableGroupDesc {
            id,
            tables: out_tables,
            annotations,
        })
    }

    fn parse_notes(
        &mut self,
        obj: &Map<String, Value>,
        ctx: &Ctx,
    ) -> Result<Vec<AnnotationTriple>, CsvwError> {
        if let Some(n) = obj.get("notes") {
            match n {
                Value::Array(_) => common_property_triples("csvw:note", n, ctx),
                _ => {
                    self.warn("notes must be an array");
                    Ok(vec![])
                }
            }
        } else {
            Ok(vec![])
        }
    }

    fn parse_transformations(
        &mut self,
        obj: &Map<String, Value>,
        ctx: &Ctx,
    ) -> Result<(), CsvwError> {
        if let Some(t) = obj.get("transformations") {
            if let Value::Array(a) = t {
                for t in a {
                    if let Value::Object(o) = t {
                        self.check_object(o, Kind::Transformation, ctx, false)?;
                    } else {
                        self.warn(format!(
                            "ignoring a transformation that is not an object: {t}"
                        ));
                    }
                }
            } else {
                self.warn("transformations must be an array");
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_table(
        &mut self,
        obj: &Map<String, Value>,
        ctx: &Ctx,
        top_level: bool,
        group_dialect: Option<&Dialect>,
        group_schema: Option<&SchemaDesc>,
        group_inherited: &Inherited,
    ) -> Result<TableDesc, CsvwError> {
        let (id, mut annotations) = self.check_object(obj, Kind::Table, ctx, top_level)?;
        annotations.extend(self.parse_notes(obj, ctx)?);
        let url = match obj.get("url") {
            Some(Value::String(u)) => resolve(&ctx.base, u)?,
            Some(_) => return meta_err("the url of a table must be a string"),
            None => return meta_err("the url property is required in a Table"),
        };
        let inherited = self.parse_inherited(obj, ctx)?.inherit(group_inherited);
        let dialect = self
            .parse_dialect_prop(obj, ctx)?
            .or(group_dialect.cloned());
        let schema = match self.parse_schema_prop(obj, ctx, &inherited)? {
            Some(s) => Some(s),
            None => group_schema.cloned(),
        };
        let suppress_output = self.parse_bool(obj, "suppressOutput")?.unwrap_or(false);
        self.parse_transformations(obj, ctx)?;
        self.check_direction(obj, "tableDirection");
        Ok(TableDesc {
            url,
            id,
            suppress_output,
            dialect,
            schema,
            inherited,
            annotations,
        })
    }

    fn check_direction(&mut self, obj: &Map<String, Value>, k: &str) {
        match obj.get(k) {
            None => {}
            Some(Value::String(s)) if matches!(s.as_str(), "rtl" | "ltr" | "auto" | "inherit") => {}
            Some(v) => self.warn(format!("invalid {k} {v}")),
        }
    }

    fn parse_bool(&mut self, obj: &Map<String, Value>, k: &str) -> Result<Option<bool>, CsvwError> {
        match obj.get(k) {
            None => Ok(None),
            Some(Value::Bool(b)) => Ok(Some(*b)),
            Some(v) => {
                self.warn(format!("{k} must be a boolean, found {v}"));
                Ok(None)
            }
        }
    }

    fn parse_uint(&mut self, obj: &Map<String, Value>, k: &str) -> Option<usize> {
        match obj.get(k) {
            None => None,
            Some(Value::Number(n)) if n.as_u64().is_some() => Some(n.as_u64().unwrap() as usize),
            Some(v) => {
                self.warn(format!("{k} must be a non-negative integer, found {v}"));
                None
            }
        }
    }

    fn parse_string(&mut self, obj: &Map<String, Value>, k: &str) -> Option<String> {
        match obj.get(k) {
            None => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(v) => {
                self.warn(format!("{k} must be a string, found {v}"));
                None
            }
        }
    }

    /// Resolves an object property: an inline object or a link to a JSON document.
    fn object_prop(
        &mut self,
        obj: &Map<String, Value>,
        k: &str,
        ctx: &Ctx,
    ) -> Result<Option<(Map<String, Value>, Ctx, bool)>, CsvwError> {
        match obj.get(k) {
            None => Ok(None),
            Some(Value::Object(o)) => Ok(Some((o.clone(), ctx.clone(), false))),
            Some(Value::String(s)) => {
                let iri = resolve(&ctx.base, s)?;
                let bytes = self.loader.load_required(&iri)?;
                let v = parse_json(&iri, &bytes)?;
                let Value::Object(mut o) = v else {
                    return meta_err(format!("{iri} does not contain a JSON object"));
                };
                let mut new_ctx = Ctx {
                    base: iri.clone(),
                    lang: ctx.lang.clone(),
                };
                if o.contains_key("@context") {
                    new_ctx = parse_context(&o, &iri, self.warnings)?;
                    o.remove("@context");
                }
                if !o.contains_key("@id") {
                    o.insert("@id".to_string(), Value::String(iri));
                }
                Ok(Some((o, new_ctx, true)))
            }
            Some(v) => {
                self.warn(format!("{k} must be an object or a link, found {v}"));
                Ok(Some((Map::new(), ctx.clone(), false)))
            }
        }
    }

    fn parse_dialect_prop(
        &mut self,
        obj: &Map<String, Value>,
        ctx: &Ctx,
    ) -> Result<Option<Dialect>, CsvwError> {
        let Some((d, dctx, _)) = self.object_prop(obj, "dialect", ctx)? else {
            return Ok(None);
        };
        self.check_object(&d, Kind::Dialect, &dctx, false)?;
        let mut dialect = Dialect::default();
        if let Some(c) = self.parse_string(&d, "commentPrefix") {
            dialect.comment_prefix = Some(c);
        }
        if let Some(c) = self.parse_string(&d, "delimiter") {
            dialect.delimiter = c;
        }
        if let Some(b) = self.parse_bool(&d, "doubleQuote")? {
            dialect.double_quote = b;
        }
        if let Some(e) = self.parse_string(&d, "encoding") {
            if crate::parse::is_supported_encoding(&e) {
                dialect.encoding = e;
            } else {
                self.warn(format!("unsupported encoding {e}, using utf-8"));
            }
        }
        if let Some(h) = self.parse_bool(&d, "header")? {
            dialect.header_row_count = if h { 1 } else { 0 };
        }
        if let Some(h) = self.parse_uint(&d, "headerRowCount") {
            dialect.header_row_count = h;
        }
        match d.get("lineTerminators") {
            Some(Value::String(s)) => dialect.line_terminators = vec![s.clone()],
            Some(Value::Array(a)) => {
                dialect.line_terminators = a
                    .iter()
                    .filter_map(|x| x.as_str().map(|x| x.to_string()))
                    .collect()
            }
            Some(v) => self.warn(format!("invalid lineTerminators {v}")),
            None => {}
        }
        match d.get("quoteChar") {
            Some(Value::String(s)) => dialect.quote_char = Some(s.clone()),
            Some(Value::Null) => dialect.quote_char = None,
            Some(v) => self.warn(format!("invalid quoteChar {v}")),
            None => {}
        }
        if let Some(b) = self.parse_bool(&d, "skipBlankRows")? {
            dialect.skip_blank_rows = b;
        }
        if let Some(n) = self.parse_uint(&d, "skipColumns") {
            dialect.skip_columns = n;
        }
        if let Some(b) = self.parse_bool(&d, "skipInitialSpace")? {
            dialect.trim = if b { Trim::Start } else { Trim::False };
        }
        if let Some(n) = self.parse_uint(&d, "skipRows") {
            dialect.skip_rows = n;
        }
        match d.get("trim") {
            Some(Value::Bool(true)) => dialect.trim = Trim::True,
            Some(Value::Bool(false)) => dialect.trim = Trim::False,
            Some(Value::String(s)) if s == "true" => dialect.trim = Trim::True,
            Some(Value::String(s)) if s == "false" => dialect.trim = Trim::False,
            Some(Value::String(s)) if s == "start" => dialect.trim = Trim::Start,
            Some(Value::String(s)) if s == "end" => dialect.trim = Trim::End,
            Some(v) => self.warn(format!("invalid trim {v}")),
            None => {}
        }
        Ok(Some(dialect))
    }

    fn parse_schema_prop(
        &mut self,
        obj: &Map<String, Value>,
        ctx: &Ctx,
        _inherited: &Inherited,
    ) -> Result<Option<SchemaDesc>, CsvwError> {
        let Some((s, sctx, _)) = self.object_prop(obj, "tableSchema", ctx)? else {
            return Ok(None);
        };
        let (id, _annotations) = self.check_object(&s, Kind::Schema, &sctx, false)?;
        let inherited = self.parse_inherited(&s, &sctx)?;
        let mut columns = vec![];
        match s.get("columns") {
            None => {}
            Some(Value::Array(a)) => {
                for (i, c) in a.iter().enumerate() {
                    if let Value::Object(cobj) = c {
                        columns.push(self.parse_column(cobj, &sctx, i + 1)?);
                    } else {
                        self.warn("ignoring a column description that is not an object");
                    }
                }
            }
            Some(_) => self.warn("columns must be an array"),
        }
        // Validation of the columns
        let mut seen_virtual = false;
        let mut names = std::collections::HashSet::new();
        for c in &columns {
            if c.virtual_ {
                seen_virtual = true;
            } else if seen_virtual {
                return meta_err("virtual columns must come after all non-virtual columns");
            }
            if let Some(n) = &c.name {
                if !names.insert(n.clone()) {
                    return meta_err(format!("duplicate column name {n}"));
                }
            }
        }
        let column_names: Vec<String> = columns.iter().filter_map(|c| c.name.clone()).collect();
        let primary_key = match self.column_reference(&s, "primaryKey", &column_names) {
            Ok(pk) => pk.unwrap_or_default(),
            Err(w) => {
                self.warn(w);
                vec![]
            }
        };
        let row_titles = match self.column_reference(&s, "rowTitles", &column_names) {
            Ok(rt) => rt.unwrap_or_default(),
            Err(w) => {
                self.warn(w);
                vec![]
            }
        };
        let mut foreign_keys = vec![];
        match s.get("foreignKeys") {
            None => {}
            Some(Value::Array(a)) => {
                for fk in a {
                    let Value::Object(fko) = fk else {
                        self.warn(format!(
                            "ignoring a foreign key that is not an object: {fk}"
                        ));
                        continue;
                    };
                    foreign_keys.push(self.parse_foreign_key(fko, &sctx, &column_names)?);
                }
            }
            Some(_) => self.warn("foreignKeys must be an array"),
        }
        Ok(Some(SchemaDesc {
            id,
            columns,
            inherited,
            row_titles,
            primary_key,
            foreign_keys,
        }))
    }

    fn parse_foreign_key(
        &mut self,
        fk: &Map<String, Value>,
        ctx: &Ctx,
        column_names: &[String],
    ) -> Result<ForeignKey, CsvwError> {
        self.check_object(fk, Kind::ForeignKey, ctx, false)?;
        let column_reference = match self.column_reference(fk, "columnReference", column_names) {
            Ok(Some(c)) => c,
            Ok(None) => return meta_err("a foreign key must have a columnReference"),
            Err(e) => return meta_err(e),
        };
        let Some(Value::Object(r)) = fk.get("reference") else {
            return meta_err("the reference of a foreign key must be an object");
        };
        self.check_object(r, Kind::Reference, ctx, false)?;
        let resource = match r.get("resource") {
            Some(Value::String(s)) => Some(resolve(&ctx.base, s)?),
            Some(_) => return meta_err("resource must be a string"),
            None => None,
        };
        let schema_reference = match r.get("schemaReference") {
            Some(Value::String(s)) => Some(resolve(&ctx.base, s)?),
            Some(_) => return meta_err("schemaReference must be a string"),
            None => None,
        };
        if resource.is_some() == schema_reference.is_some() {
            return meta_err("a reference must have exactly one of resource and schemaReference");
        }
        let referenced_columns = match r.get("columnReference") {
            Some(Value::String(s)) => vec![s.clone()],
            Some(Value::Array(a)) if !a.is_empty() && a.iter().all(|x| x.is_string()) => {
                a.iter().map(|x| x.as_str().unwrap().to_string()).collect()
            }
            _ => return meta_err("a reference must have a columnReference"),
        };
        Ok(ForeignKey {
            column_reference,
            resource,
            schema_reference,
            referenced_columns,
        })
    }

    /// Parses a column reference property. Err contains a warning.
    fn column_reference(
        &mut self,
        obj: &Map<String, Value>,
        k: &str,
        column_names: &[String],
    ) -> Result<Option<Vec<String>>, String> {
        let refs = match obj.get(k) {
            None => return Ok(None),
            Some(Value::String(s)) => vec![s.clone()],
            Some(Value::Array(a)) if !a.is_empty() && a.iter().all(|x| x.is_string()) => {
                a.iter().map(|x| x.as_str().unwrap().to_string()).collect()
            }
            Some(v) => return Err(format!("invalid column reference {v} in {k}")),
        };
        for r in &refs {
            if !column_names.contains(r) {
                return Err(format!("{k} references the unknown column {r}"));
            }
        }
        Ok(Some(refs))
    }

    fn parse_column(
        &mut self,
        obj: &Map<String, Value>,
        ctx: &Ctx,
        _number: usize,
    ) -> Result<ColumnDesc, CsvwError> {
        let (_id, _annotations) = self.check_object(obj, Kind::Column, ctx, false)?;
        let name = match obj.get("name") {
            None => None,
            Some(Value::String(n)) => {
                if NAME_RE.is_match(n) && !n.starts_with('_') {
                    Some(n.clone())
                } else {
                    self.warn(format!("invalid column name {n}"));
                    None
                }
            }
            Some(v) => {
                self.warn(format!("column names must be strings, found {v}"));
                None
            }
        };
        let titles = self.natural_language(obj.get("titles"), ctx);
        // The default name is the first title in the default language, see
        // https://www.w3.org/TR/tabular-metadata/#column-name
        let default_lang = ctx.lang.as_deref().unwrap_or("und");
        let default_name = titles
            .iter()
            .find(|(l, _)| l == default_lang)
            .map(|(_, t)| encode_name(t));
        let virtual_ = self.parse_bool(obj, "virtual")?.unwrap_or(false);
        let suppress_output = self.parse_bool(obj, "suppressOutput")?.unwrap_or(false);
        let inherited = self.parse_inherited(obj, ctx)?;
        Ok(ColumnDesc {
            name,
            default_name,
            titles,
            virtual_,
            suppress_output,
            inherited,
        })
    }

    fn natural_language(&mut self, v: Option<&Value>, ctx: &Ctx) -> Vec<(String, String)> {
        let default_lang = ctx.lang.clone().unwrap_or("und".to_string());
        match v {
            None => vec![],
            Some(Value::String(s)) => vec![(default_lang, s.clone())],
            Some(Value::Array(a)) => {
                let mut out = vec![];
                for x in a {
                    match x {
                        Value::String(s) => out.push((default_lang.clone(), s.clone())),
                        _ => self.warn(format!("ignoring a non-string title {x}")),
                    }
                }
                out
            }
            Some(Value::Object(o)) => {
                let mut out = vec![];
                for (l, v) in o {
                    if oxilangtag::LanguageTag::parse(l.as_str()).is_err() {
                        self.warn(format!("invalid language tag {l}"));
                        continue;
                    }
                    match v {
                        Value::String(s) => out.push((l.clone(), s.clone())),
                        Value::Array(a) => {
                            for x in a {
                                if let Value::String(s) = x {
                                    out.push((l.clone(), s.clone()));
                                } else {
                                    self.warn(format!("ignoring a non-string title {x}"));
                                }
                            }
                        }
                        _ => self.warn(format!("ignoring a non-string title {v}")),
                    }
                }
                out
            }
            Some(v) => {
                self.warn(format!("invalid natural language property {v}"));
                vec![]
            }
        }
    }

    fn uri_template_prop(&mut self, obj: &Map<String, Value>, k: &str) -> Option<String> {
        match obj.get(k) {
            None => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(v) => {
                self.warn(format!("{k} must be a string, found {v}"));
                Some(String::new())
            }
        }
    }

    fn parse_inherited(
        &mut self,
        obj: &Map<String, Value>,
        ctx: &Ctx,
    ) -> Result<Inherited, CsvwError> {
        let mut inh = Inherited {
            about_url: self.uri_template_prop(obj, "aboutUrl"),
            property_url: self.uri_template_prop(obj, "propertyUrl"),
            value_url: self.uri_template_prop(obj, "valueUrl"),
            ..Default::default()
        };
        if let Some(d) = obj.get("datatype") {
            inh.datatype = self.parse_datatype(d, ctx)?;
        }
        match obj.get("default") {
            None => {}
            Some(Value::String(s)) => inh.default = Some(s.clone()),
            Some(v) => {
                self.warn(format!("default must be a string, found {v}"));
                inh.default = Some(String::new());
            }
        }
        match obj.get("lang") {
            None => {}
            Some(Value::String(s)) => {
                if oxilangtag::LanguageTag::parse(s.as_str()).is_ok() {
                    inh.lang = Some(s.clone())
                } else {
                    self.warn(format!("invalid language tag {s}"));
                }
            }
            Some(v) => self.warn(format!("lang must be a string, found {v}")),
        }
        match obj.get("null") {
            None => {}
            Some(Value::String(s)) => inh.null = Some(vec![s.clone()]),
            Some(Value::Array(a)) => {
                let mut nulls = vec![];
                for x in a {
                    if let Value::String(s) = x {
                        nulls.push(s.clone());
                    } else {
                        self.warn(format!("null values must be strings, found {x}"));
                    }
                }
                inh.null = Some(nulls);
            }
            Some(v) => {
                self.warn(format!("null must be a string or an array, found {v}"));
                inh.null = Some(vec![String::new()]);
            }
        }
        inh.ordered = self.parse_bool(obj, "ordered")?;
        inh.required = self.parse_bool(obj, "required")?;
        match obj.get("separator") {
            None => {}
            Some(Value::String(s)) => inh.separator = Some(Some(s.clone())),
            Some(Value::Null) => inh.separator = Some(None),
            Some(v) => {
                self.warn(format!("separator must be a string or null, found {v}"));
                inh.separator = Some(None);
            }
        }
        match obj.get("textDirection") {
            None => {}
            Some(Value::String(s)) if ["ltr", "rtl", "auto", "inherit"].contains(&s.as_str()) => {
                inh.text_direction = Some(s.clone())
            }
            Some(v) => self.warn(format!("invalid textDirection {v}")),
        }
        Ok(inh)
    }

    fn parse_datatype(&mut self, d: &Value, ctx: &Ctx) -> Result<Option<Datatype>, CsvwError> {
        match d {
            Value::String(s) => {
                if let Some(n) = builtin_datatype_name(s) {
                    Ok(Some(Datatype::builtin(n)))
                } else {
                    self.warn(format!("unknown datatype {s}, using string"));
                    Ok(Some(Datatype::builtin("string")))
                }
            }
            Value::Object(o) => {
                let mut id = None;
                if let Some(i) = o.get("@id") {
                    let Value::String(i) = i else {
                        return meta_err("the @id of a datatype must be a string");
                    };
                    if i.starts_with("_:") {
                        return meta_err(format!(
                            "the @id of a datatype must not be a blank node, found {i}"
                        ));
                    }
                    let iri = resolve(&ctx.base, i)?;
                    if is_builtin_datatype_iri(&iri) {
                        return meta_err(format!(
                            "the @id of a datatype must not be the IRI of a built-in datatype, found {iri}"
                        ));
                    }
                    id = Some(iri);
                }
                let mut o2 = o.clone();
                o2.remove("@id");
                self.check_object(&o2, Kind::Datatype, ctx, false)?;
                let base = match o.get("base") {
                    None => "string".to_string(),
                    Some(Value::String(b)) => match builtin_datatype_name(b) {
                        Some(n) => n.to_string(),
                        None => {
                            self.warn(format!("unknown datatype base {b}, using string"));
                            "string".to_string()
                        }
                    },
                    Some(v) => {
                        self.warn(format!("datatype base must be a string, found {v}"));
                        "string".to_string()
                    }
                };
                let mut dt = Datatype::builtin(&base);
                dt.id = id;
                dt.format = o.get("format").cloned();
                if let Some(problem) = crate::datatypes::format_problem(&dt) {
                    self.warn(format!("ignoring the format: {problem}"));
                    dt.format = None;
                }
                dt.length = self.parse_uint(o, "length");
                dt.min_length = self.parse_uint(o, "minLength");
                dt.max_length = self.parse_uint(o, "maxLength");
                let bound = |p: &mut Self, k: &str| -> Option<String> {
                    match o.get(k) {
                        None => None,
                        Some(Value::String(s)) => Some(s.clone()),
                        Some(Value::Number(n)) => Some(n.to_string()),
                        Some(v) => {
                            p.warn(format!("invalid {k} {v}"));
                            None
                        }
                    }
                };
                dt.min_inclusive = bound(self, "minInclusive").or(bound(self, "minimum"));
                dt.max_inclusive = bound(self, "maxInclusive").or(bound(self, "maximum"));
                dt.min_exclusive = bound(self, "minExclusive");
                dt.max_exclusive = bound(self, "maxExclusive");
                self.validate_datatype(&dt)?;
                Ok(Some(dt))
            }
            v => {
                self.warn(format!("invalid datatype {v}, using string"));
                Ok(Some(Datatype::builtin("string")))
            }
        }
    }

    fn validate_datatype(&mut self, dt: &Datatype) -> Result<(), CsvwError> {
        let has_length = dt.length.is_some() || dt.min_length.is_some() || dt.max_length.is_some();
        if has_length
            && !(is_string_type(&dt.base) || is_binary_type(&dt.base) || dt.base == "anyAtomicType")
        {
            return meta_err(format!(
                "length constraints are not allowed on the datatype {}",
                dt.base
            ));
        }
        if let (Some(l), Some(min)) = (dt.length, dt.min_length) {
            if l < min {
                return meta_err("length is less than minLength");
            }
        }
        if let (Some(l), Some(max)) = (dt.length, dt.max_length) {
            if l > max {
                return meta_err("length is greater than maxLength");
            }
        }
        if let (Some(min), Some(max)) = (dt.min_length, dt.max_length) {
            if min > max {
                return meta_err("minLength is greater than maxLength");
            }
        }
        let has_bounds = dt.min_inclusive.is_some()
            || dt.max_inclusive.is_some()
            || dt.min_exclusive.is_some()
            || dt.max_exclusive.is_some();
        if has_bounds
            && !(is_numeric_type(&dt.base)
                || is_date_time_type(&dt.base)
                || is_duration_type(&dt.base))
        {
            return meta_err(format!(
                "value constraints are not allowed on the datatype {}",
                dt.base
            ));
        }
        if dt.min_inclusive.is_some() && dt.min_exclusive.is_some() {
            return meta_err("both minInclusive and minExclusive are specified");
        }
        if dt.max_inclusive.is_some() && dt.max_exclusive.is_some() {
            return meta_err("both maxInclusive and maxExclusive are specified");
        }
        let cmp = |a: &str, b: &str| crate::datatypes::compare_bound(&dt.base, a, b);
        if let (Some(min), Some(max)) = (&dt.min_inclusive, &dt.max_inclusive) {
            if cmp(max, min) == Some(std::cmp::Ordering::Less) {
                return meta_err("maxInclusive is less than minInclusive");
            }
        }
        if let (Some(min), Some(max)) = (&dt.min_inclusive, &dt.max_exclusive) {
            if cmp(max, min) != Some(std::cmp::Ordering::Greater) {
                return meta_err("maxExclusive is less than or equal to minInclusive");
            }
        }
        if let (Some(min), Some(max)) = (&dt.min_exclusive, &dt.max_exclusive) {
            if cmp(max, min) == Some(std::cmp::Ordering::Less) {
                return meta_err("maxExclusive is less than minExclusive");
            }
        }
        if let (Some(min), Some(max)) = (&dt.min_exclusive, &dt.max_inclusive) {
            if cmp(max, min) != Some(std::cmp::Ordering::Greater) {
                return meta_err("maxInclusive is less than or equal to minExclusive");
            }
        }
        Ok(())
    }
}

/// Does the metadata describe the tabular data file at url?
pub fn describes(tg: &TableGroupDesc, url: &str) -> bool {
    let n = crate::loader::normalize_url(url);
    tg.tables
        .iter()
        .any(|t| crate::loader::normalize_url(&t.url) == n)
}

/// Percent-encodes a title into a column name, see the name production in
/// <https://www.w3.org/TR/tabular-metadata/#dfn-name>: ALPHA, DIGIT and "_" are kept, as is "."
/// after the first character, everything else is percent-encoded as UTF-8.
pub fn encode_name(title: &str) -> String {
    let mut out = String::new();
    for (i, c) in title.chars().enumerate() {
        if c.is_ascii_alphanumeric() || c == '_' || (c == '.' && i > 0) {
            out.push(c);
        } else {
            let mut buf = [0u8; 4];
            for b in c.encode_utf8(&mut buf).bytes() {
                out.push_str(&format!("%{b:02X}"));
            }
        }
    }
    out
}

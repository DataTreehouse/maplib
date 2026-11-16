//! Generates the SPARQL Update that maps the Facade-X representation of annotated tables to the
//! RDF defined by <https://www.w3.org/TR/csv2rdf/>.

use crate::annotate::{AnnColumn, AnnGroup, AnnTable};
use crate::errors::CsvwError;
use crate::facade::{
    cell_predicate, helper, invalid_predicate, typed_datatype, typed_predicate, FacadeIris,
};
use crate::jsonld::{ATerm, AnnotationTriple};
use crate::sparql_util::{aterm, iri, lit, Bindings};
use crate::uri_template::{compile_iri, IriExpr, VarResolver, VarValue};
use crate::vocab::{CSVW_NS, MAPLIB_CSVW_NS, RDF_NS};
use representation::constants::{FX_CHILD, FX_PREFIX_IRI, XYZ_PREFIX_IRI};
use std::cell::RefCell;
use std::collections::BTreeSet;

pub struct GenOptions {
    pub minimal: bool,
    /// The graph to insert the result into, None is the default graph.
    pub graph: Option<String>,
}

struct Gen<'a> {
    iris: &'a FacadeIris,
    opts: &'a GenOptions,
    ops: Vec<String>,
}

const HEADER: &str = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX csvw: <http://www.w3.org/ns/csvw#>
PREFIX fx: <http://sparql.xyz/facade-x/ns/>
PREFIX xyz: <http://sparql.xyz/facade-x/data/>
PREFIX csvwx: <urn:maplib:csvw:>
";

/// Abbreviates the IRIs of the namespaces in the header, when the local name allows it.
fn short(i: &str) -> String {
    for (p, ns) in [
        ("rdf", RDF_NS),
        ("csvw", CSVW_NS),
        ("fx", FX_PREFIX_IRI),
        ("xyz", XYZ_PREFIX_IRI),
        ("csvwx", MAPLIB_CSVW_NS),
        ("xsd", crate::vocab::XSD_NS),
    ] {
        if let Some(local) = i.strip_prefix(ns) {
            let ok = !local.is_empty()
                && local.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                && !local.starts_with(|c: char| c.is_ascii_digit() && p != "rdf");
            if ok {
                return format!("{p}:{local}");
            }
        }
    }
    iri(i)
}

/// Resolves template variables to the variables bound for the columns of a table, recording the
/// columns that are used.
struct TableVars<'a> {
    table: &'a AnnTable,
    column: Option<&'a AnnColumn>,
    used: RefCell<BTreeSet<usize>>,
    used_row: RefCell<bool>,
}

impl VarResolver for TableVars<'_> {
    fn resolve_var(&self, name: &str) -> VarValue {
        match name {
            "_row" => {
                *self.used_row.borrow_mut() = true;
                VarValue::Var {
                    var: "?_rowString".to_string(),
                    list: false,
                    always_bound: true,
                }
            }
            "_sourceRow" => {
                *self.used_row.borrow_mut() = true;
                VarValue::Var {
                    var: "?_sourceRowString".to_string(),
                    list: false,
                    always_bound: true,
                }
            }
            "_column" => VarValue::Const(self.column.map(|c| c.number.to_string())),
            "_sourceColumn" => VarValue::Const(self.column.map(|c| c.source_number.to_string())),
            "_name" => VarValue::Const(self.column.map(|c| crate::loader::percent_decode(&c.name))),
            n => {
                if let Some(c) = self.table.columns.iter().find(|c| c.name == n) {
                    if c.virtual_ {
                        return VarValue::Const(None);
                    }
                    self.used.borrow_mut().insert(c.number - 1);
                    VarValue::Var {
                        var: template_var(c),
                        list: c.parsing.separator.is_some(),
                        always_bound: false,
                    }
                } else {
                    VarValue::Const(None)
                }
            }
        }
    }
}

fn template_var(c: &AnnColumn) -> String {
    format!("?c{}", c.number)
}

/// Patterns binding the template variables of the used columns.
fn template_var_patterns(t: &TableVars) -> Vec<String> {
    let mut out = vec![];
    for i in t.used.borrow().iter() {
        let c = &t.table.columns[*i];
        let tv = template_var(c);
        if c.parsing.separator.is_some() {
            out.push(format!(
                "OPTIONAL {{ ?row {} ?list{n} . ?list{n} csvwx:joined {tv} }}",
                short(&cell_predicate(&c.name)),
                n = c.number
            ));
        } else {
            out.push(format!(
                "OPTIONAL {{ ?row {} ?value{} }}",
                short(&cell_predicate(&c.name)),
                c.number
            ));
            out.push(format!(
                "OPTIONAL {{ ?row {} ?invalid{} }}",
                short(&invalid_predicate(&c.name)),
                c.number
            ));
            out.push(format!(
                "BIND(COALESCE(?value{n}, ?invalid{n}) AS {tv})",
                n = c.number
            ));
        }
    }
    if *t.used_row.borrow() {
        out.push("BIND(STR(?_row) AS ?_rowString)".to_string());
        out.push("BIND(STR(?_sourceRow) AS ?_sourceRowString)".to_string());
    }
    out
}

fn no_fragment(u: &str) -> &str {
    u.split('#').next().unwrap()
}

impl Gen<'_> {
    fn out_graph(&self, triples: &str) -> String {
        match &self.opts.graph {
            Some(g) => format!("GRAPH {} {{\n{triples}  }}", iri(g)),
            None => triples.to_string(),
        }
    }

    fn insert(&mut self, comment: &str, template: &str, where_: &str) {
        self.ops.push(format!(
            "# {comment}\nINSERT {{\n  {}\n}}\nWHERE {{\n{where_}}}",
            self.out_graph(template)
        ));
    }

    fn row_patterns(&self) -> &'static str {
        "?row csvwx:row ?_row ;\n      csvwx:sourceRow ?_sourceRow ;\n      csvwx:defaultSubject ?_defaultSubject .\n"
    }

    /// Statement creating the blank nodes that are shared between statements.
    fn node_statements(&mut self, g: &AnnGroup, tables: &[usize]) {
        for k in tables {
            let graph = self.iris.table_graph(*k);
            let root = self.iris.root(*k);
            let row_node = if self.opts.minimal {
                ""
            } else {
                " ;\n         csvwx:rowNode _:rowNode"
            };
            self.ops.push(format!(
                "# Blank nodes for the default subject{} of each row of {}\nINSERT {{\n  GRAPH {} {{\n    ?row csvwx:defaultSubject _:defaultSubject{row_node} .\n  }}\n}}\nWHERE {{\n  GRAPH {} {{ {} {} ?row }}\n}}",
                if self.opts.minimal { "" } else { " and the row node" },
                g.tables[*k].url,
                iri(&graph),
                iri(&graph),
                iri(&root),
                short(FX_CHILD),
            ));
        }
        if self.opts.minimal {
            return;
        }
        // Each table node is stored in the graph of its table, where the statements for the
        // table look it up.
        let mut blanks = vec![];
        if g.id.is_none() {
            blanks.push((
                tables[0],
                format!("{} csvwx:node _:group", iri(&self.iris.root(tables[0]))),
            ));
        }
        for k in tables {
            if g.tables[*k].id.is_none() {
                blanks.push((
                    *k,
                    format!("{} csvwx:tableNode _:table{k}", iri(&self.iris.root(*k))),
                ));
            }
        }
        if !blanks.is_empty() {
            let graph = self.iris.table_graph(tables[0]);
            let template: String = blanks
                .iter()
                .map(|(k, t)| {
                    format!(
                        "  GRAPH {} {{\n    {t} .\n  }}\n",
                        iri(&self.iris.table_graph(*k))
                    )
                })
                .collect();
            self.ops.push(format!(
                "# Blank nodes for the group of tables and the tables\nINSERT {{\n{template}}}\nWHERE {{\n  GRAPH {} {{ {} a fx:root }}\n}}",
                iri(&graph),
                iri(&self.iris.root(tables[0])),
            ));
        }
    }

    fn group_node(&self, g: &AnnGroup) -> String {
        match &g.id {
            Some(id) => iri(id),
            None => "?group".to_string(),
        }
    }

    fn table_node(&self, t: &AnnTable, k: usize) -> String {
        match &t.id {
            Some(id) => iri(id),
            None => format!("?table{k}"),
        }
    }

    fn annotation_triples(&self, a: &[AnnotationTriple], this: &str, out: &mut Vec<String>) {
        for t in a {
            out.push(format!(
                "{} {} {} .",
                aterm(&t.subject, this),
                short(&t.predicate),
                match &t.object {
                    ATerm::Iri(i) => short(i),
                    o => aterm(o, this),
                }
            ));
        }
    }

    /// The group of tables and the tables (standard mode).
    fn group_statement(&mut self, g: &AnnGroup, tables: &[usize]) {
        let gn = self.group_node(g);
        let mut triples = vec![format!("{gn} a csvw:TableGroup .")];
        self.annotation_triples(&g.annotations, &gn, &mut triples);
        let mut wheres = vec![];
        let mut other_graphs = vec![];
        if g.id.is_none() {
            wheres.push(format!(
                "{} csvwx:node ?group .",
                iri(&self.iris.root(tables[0]))
            ));
        }
        for k in tables {
            let t = &g.tables[*k];
            let tn = self.table_node(t, *k);
            triples.push(format!("{gn} csvw:table {tn} ."));
            triples.push(format!(
                "{tn} a csvw:Table ;\n    csvw:url {} .",
                iri(&t.url)
            ));
            self.annotation_triples(&t.annotations, &tn, &mut triples);
            if t.id.is_none() {
                other_graphs.push(format!(
                    "  GRAPH {} {{ {} csvwx:tableNode {tn} . }}\n",
                    iri(&self.iris.table_graph(*k)),
                    iri(&self.iris.root(*k))
                ));
            }
        }
        let template = format!("  {}\n", triples.join("\n  "));
        let graph = self.iris.table_graph(tables[0]);
        let where_ = format!(
            "  GRAPH {} {{\n    {} a fx:root .\n    {}\n  }}\n{}",
            iri(&graph),
            iri(&self.iris.root(tables[0])),
            wheres.join("\n    "),
            other_graphs.concat()
        );
        self.insert("The group of tables and the tables", &template, &where_);
    }

    fn bindings(&self) -> Bindings {
        Bindings {
            prefix_graph: self.iris.prefix_graph(),
            ..Default::default()
        }
    }

    /// Compiles the about URL of a column to a term, adding patterns to bindings.
    fn subject_term(
        &self,
        t: &AnnTable,
        c: &AnnColumn,
        vars: &TableVars,
        b: &mut Bindings,
        binds: &mut Vec<String>,
        var_name: &str,
    ) -> Result<String, CsvwError> {
        match &c.about_url {
            None => Ok("?_defaultSubject".to_string()),
            Some(a) => Ok(match compile_iri(a, vars, &t.url, b)? {
                IriExpr::Const(i) => iri(&i),
                IriExpr::Sparql(e) => {
                    binds.push(format!("BIND({e} AS {var_name})"));
                    var_name.to_string()
                }
            }),
        }
    }

    /// The rows of a table (standard mode).
    fn rows_statement(&mut self, t: &AnnTable, k: usize) -> Result<(), CsvwError> {
        let vars = TableVars {
            table: t,
            column: None,
            used: RefCell::new(BTreeSet::new()),
            used_row: RefCell::new(false),
        };
        let tn = self.table_node(t, k);
        let mut b = self.bindings();
        let mut binds = vec![];
        let mut triples = vec![
            format!("{tn} csvw:row ?rowNode ."),
            "?rowNode a csvw:Row ;\n    csvw:rownum ?_row ;\n    csvw:url ?rowUrl .".to_string(),
        ];
        binds.push(format!(
            "BIND(IRI(CONCAT({}, STR(?_sourceRow))) AS ?rowUrl)",
            lit(&format!("{}#row=", no_fragment(&t.url)))
        ));
        let mut subjects = BTreeSet::new();
        for c in t.columns.iter().filter(|c| !c.suppress_output) {
            let v = format!("?subject{}", c.number);
            let s = self.subject_term(t, c, &vars, &mut b, &mut binds, &v)?;
            subjects.insert(s);
        }
        for s in subjects {
            triples.push(format!("?rowNode csvw:describes {s} ."));
        }
        let mut title_patterns = vec![];
        for i in &t.row_titles {
            let c = &t.columns[*i];
            let v = format!("?title{}", c.number);
            title_patterns.push(format!(
                "OPTIONAL {{ ?row {} ?titleValue{} }}",
                short(&cell_predicate(&c.name)),
                c.number
            ));
            if c.lang != "und" {
                binds.push(format!(
                    "BIND(STRLANG(?titleValue{}, {}) AS {v})",
                    c.number,
                    lit(&c.lang)
                ));
            } else {
                binds.push(format!("BIND(?titleValue{} AS {v})", c.number));
            }
            triples.push(format!("?rowNode csvw:title {v} ."));
        }
        let graph = self.iris.table_graph(k);
        let root = self.iris.root(k);
        let mut graph_patterns = vec![
            format!("{} fx:child ?row .", iri(&root)),
            "?row csvwx:rowNode ?rowNode .".to_string(),
            self.row_patterns().to_string(),
        ];
        if t.id.is_none() {
            graph_patterns.push(format!("{} csvwx:tableNode {tn} .", iri(&root)));
        }
        graph_patterns.extend(title_patterns);
        graph_patterns.extend(template_var_patterns(&vars));
        let where_ = format!(
            "  GRAPH {} {{\n    {}\n  }}\n{}",
            iri(&graph),
            graph_patterns.join("\n    "),
            b.patterns
                .iter()
                .chain(binds.iter())
                .map(|x| format!("  {x}\n"))
                .collect::<String>()
        );
        let template = format!("  {}\n", triples.join("\n  "));
        self.insert(&format!("The rows of {}", t.url), &template, &where_);
        Ok(())
    }

    /// The triples of the cells of a column.
    fn column_statements(
        &mut self,
        t: &AnnTable,
        k: usize,
        c: &AnnColumn,
    ) -> Result<(), CsvwError> {
        if c.virtual_ && c.value_url.is_none() {
            return Ok(());
        }
        let vars = TableVars {
            table: t,
            column: Some(c),
            used: RefCell::new(BTreeSet::new()),
            used_row: RefCell::new(false),
        };
        let mut b = self.bindings();
        let mut binds = vec![];
        let subject = self.subject_term(t, c, &vars, &mut b, &mut binds, "?subject")?;
        let predicate = match &c.property_url {
            None => iri(&format!("{}#{}", no_fragment(&t.url), c.name)),
            Some(p) => match compile_iri(p, &vars, &t.url, &mut b)? {
                IriExpr::Const(i) => short(&i),
                IriExpr::Sparql(e) => {
                    binds.push(format!("BIND({e} AS ?predicate)"));
                    "?predicate".to_string()
                }
            },
        };
        let graph = self.iris.table_graph(k);
        let root = self.iris.root(k);
        let mut graph_patterns = vec![
            format!("{} fx:child ?row .", iri(&root)),
            self.row_patterns().to_string(),
        ];
        let list = c.parsing.separator.is_some();
        // Valid values of other datatypes than string are typed in the Facade-X graph.
        let typed = typed_datatype(c).is_some();
        let lang = if c.lang != "und" && c.parsing.datatype.base == "string" {
            Some(c.lang.clone())
        } else {
            None
        };
        let valid_expr = |v: &str| -> String {
            if let Some(l) = &lang {
                format!("STRLANG({v}, {})", lit(l))
            } else {
                v.to_string()
            }
        };
        let invalid_expr = |v: &str| -> String {
            if let Some(l) = &lang {
                format!("STRLANG({v}, {})", lit(l))
            } else {
                v.to_string()
            }
        };
        let comment;
        let object;
        let mut extra_triples = vec![];
        if let Some(vu) = &c.value_url {
            if !c.virtual_ {
                // The value URL is null if the cell is null.
                vars.resolve_var(&c.name);
            }
            object = match compile_iri(vu, &vars, &t.url, &mut b)? {
                IriExpr::Const(i) => short(&i),
                IriExpr::Sparql(e) => {
                    binds.push(format!("BIND({e} AS ?object)"));
                    "?object".to_string()
                }
            };
            if !c.virtual_ {
                binds.push(format!("FILTER(BOUND({}))", template_var(c)));
            }
            comment = format!("The value URLs of the column {} of {}", c.name, t.url);
        } else if !list {
            graph_patterns.push(format!(
                "{{ ?row {} ?value BIND({} AS ?object) }}\n    UNION\n    {{ ?row {} ?invalid BIND({} AS ?object) }}",
                if typed {
                    short(&typed_predicate(&c.name))
                } else {
                    short(&cell_predicate(&c.name))
                },
                valid_expr("?value"),
                short(&invalid_predicate(&c.name)),
                invalid_expr("?invalid"),
            ));
            object = "?object".to_string();
            comment = format!("The cells of the column {} of {}", c.name, t.url);
        } else {
            let items = format!(
                "?row {} ?list .\n    {{ ?list ?member ?value FILTER(STRSTARTS(STR(?member), {})) BIND({} AS ?object) BIND(xsd:integer(STRAFTER(STR(?member), {})) AS ?index) }}\n    UNION\n    {{ ?list ?member ?invalid FILTER(STRSTARTS(STR(?member), {})) BIND({} AS ?object) BIND(xsd:integer(STRAFTER(STR(?member), {})) AS ?index) }}",
                short(&cell_predicate(&c.name)),
                lit(&format!("{RDF_NS}_")),
                valid_expr("?value"),
                lit(&format!("{RDF_NS}_")),
                lit(&helper("invalidItem_")),
                invalid_expr("?invalid"),
                lit(&helper("invalidItem_")),
            );
            if !c.ordered {
                graph_patterns.push(items);
                object = "?object".to_string();
                comment = format!("The list items of the column {} of {}", c.name, t.url);
            } else {
                // First a blank node for each item of the list, then the rdf:List.
                self.ops.push(format!(
                    "# A blank node for each item of the ordered lists in the column {} of {}\nINSERT {{\n  GRAPH {} {{\n    ?list csvwx:item _:item .\n    _:item csvwx:index ?index ;\n      csvwx:nextIndex ?nextIndex ;\n      csvwx:value ?object .\n  }}\n}}\nWHERE {{\n  GRAPH {} {{\n    {} fx:child ?row .\n    {items}\n  }}\n  BIND(?index + 1 AS ?nextIndex)\n}}",
                    c.name,
                    t.url,
                    iri(&graph),
                    iri(&graph),
                    iri(&root),
                ));
                graph_patterns.push(format!(
                    "?row {} ?list .\n    OPTIONAL {{ ?list csvwx:item ?head . ?head csvwx:index 1 }}\n    OPTIONAL {{\n      ?list csvwx:item ?item .\n      ?item csvwx:value ?object ;\n        csvwx:nextIndex ?nextIndex .\n      OPTIONAL {{ ?list csvwx:item ?next . ?next csvwx:index ?nextIndex }}\n    }}",
                    short(&cell_predicate(&c.name)),
                ));
                binds.push("BIND(COALESCE(?head, rdf:nil) AS ?listHead)".to_string());
                binds.push("BIND(COALESCE(?next, rdf:nil) AS ?rest)".to_string());
                extra_triples.push("?item rdf:first ?object ;\n    rdf:rest ?rest .".to_string());
                object = "?listHead".to_string();
                comment = format!("The ordered lists of the column {} of {}", c.name, t.url);
            }
        }
        graph_patterns.extend(template_var_patterns(&vars));
        let mut triples = vec![format!("{subject} {predicate} {object} .")];
        triples.extend(extra_triples);
        let template = format!("  {}\n", triples.join("\n  "));
        let where_ = format!(
            "  GRAPH {} {{\n    {}\n  }}\n{}",
            iri(&graph),
            graph_patterns.join("\n    "),
            b.patterns
                .iter()
                .chain(binds.iter())
                .map(|x| format!("  {x}\n"))
                .collect::<String>()
        );
        self.insert(&comment, &template, &where_);
        Ok(())
    }
}

/// Generates the SPARQL Update for a group of annotated tables, the Facade-X representation of
/// which is in the graphs given by iris.
pub fn generate_update(
    g: &AnnGroup,
    iris: &FacadeIris,
    opts: &GenOptions,
) -> Result<String, CsvwError> {
    let mut gen = Gen {
        iris,
        opts,
        ops: vec![],
    };
    let tables: Vec<usize> = (0..g.tables.len())
        .filter(|k| !g.tables[*k].suppress_output)
        .collect();
    if !tables.is_empty() {
        gen.node_statements(g, &tables);
        if !opts.minimal {
            gen.group_statement(g, &tables);
        }
        for k in &tables {
            let t = &g.tables[*k];
            if !opts.minimal {
                gen.rows_statement(t, *k)?;
            }
            for c in t.columns.iter().filter(|c| !c.suppress_output) {
                gen.column_statements(t, *k, c)?;
            }
        }
    }
    let mut drops = vec![];
    for k in 0..g.tables.len() {
        drops.push(format!("DROP SILENT GRAPH {}", iri(&iris.table_graph(k))));
    }
    drops.push(format!("DROP SILENT GRAPH {}", iri(&iris.prefix_graph())));
    gen.ops.push(format!(
        "# Remove the Facade-X representation of the tables\n{}",
        drops.join(" ;\n")
    ));
    Ok(format!("{HEADER}\n{}\n", gen.ops.join(" ;\n\n")))
}

use crate::jsonld::ATerm;
use crate::vocab::XSD_NS;

/// A SPARQL string literal.
pub fn lit(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// A SPARQL IRI reference.
pub fn iri(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('<');
    for c in s.chars() {
        match c {
            '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\' | ' ' => {
                let mut buf = [0u8; 4];
                for b in c.encode_utf8(&mut buf).bytes() {
                    out.push_str(&format!("%{b:02X}"));
                }
            }
            c if (c as u32) <= 0x20 => out.push_str(&format!("%{:02X}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('>');
    out
}

pub fn typed_lit(lexical: &str, datatype: Option<&str>, language: Option<&str>) -> String {
    if let Some(l) = language {
        format!("{}@{l}", lit(lexical))
    } else if let Some(d) = datatype {
        if d == format!("{XSD_NS}string") {
            lit(lexical)
        } else {
            format!("{}^^{}", lit(lexical), iri(d))
        }
    } else {
        lit(lexical)
    }
}

/// A term of an annotation triple in an INSERT template. `this` is the term of the annotated
/// node.
pub fn aterm(t: &ATerm, this: &str) -> String {
    match t {
        ATerm::This => this.to_string(),
        ATerm::Iri(i) => iri(i),
        ATerm::Blank(b) => format!("_:{b}"),
        ATerm::Literal {
            lexical,
            datatype,
            language,
        } => typed_lit(lexical, datatype.as_deref(), language.as_deref()),
    }
}

/// Intermediate variables and graph patterns needed by expressions in a WHERE clause.
#[derive(Debug, Default, Clone)]
pub struct Bindings {
    pub counter: usize,
    pub patterns: Vec<String>,
    /// The graph holding the table of the prefixes of the CSVW context.
    pub prefix_graph: String,
}

impl Bindings {
    pub fn new_var(&mut self, name: &str) -> String {
        self.counter += 1;
        format!("?{name}{}", self.counter)
    }

    /// Binds an expression to a new variable, returns the variable.
    pub fn bind(&mut self, expr: &str) -> String {
        if expr.starts_with('?') && expr[1..].chars().all(|c| c.is_alphanumeric() || c == '_') {
            return expr.to_string();
        }
        let v = self.new_var("e");
        self.patterns.push(format!("BIND({expr} AS {v})"));
        v
    }

    pub fn add_pattern(&mut self, p: String) {
        self.patterns.push(p);
    }

    /// Looks up the namespace of the prefix bound to pfx in the prefix table, binding it to ns.
    pub fn add_prefix_lookup(&mut self, pfx: &str, ns: &str) {
        let pn = self.new_var("p");
        self.patterns.push(format!(
            "OPTIONAL {{ GRAPH {} {{ {pn} {} {pfx} ; {} {ns} }} }}",
            iri(&self.prefix_graph),
            iri(&format!("{}prefix", crate::vocab::MAPLIB_CSVW_NS)),
            iri(&format!("{}namespace", crate::vocab::MAPLIB_CSVW_NS)),
        ));
    }
}

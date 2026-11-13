use std::fmt;

/// Represents an RDF triple.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct Triple {
    /// Named IRI or blank node.
    pub subject: Id,
    /// IRI
    pub predicate: String,
    /// Named IRI, blank node or literal.
    pub object: Term,
}

impl Triple {
    /// Triple with the given subject, predicate and object.
    pub const fn new(subject: Id, predicate: String, object: Term) -> Self {
        Triple { subject, predicate, object }
    }
}

/// ntriples format
impl fmt::Display for Triple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} <{}> {:?} .", self.subject, self.predicate, self.object)
    }
}

/// delegate to display
impl fmt::Debug for Triple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// RDF identifiers can either be Internationalized Resource Identifiers (IRIs) or blank node
/// identifiers. The latter are random identifiers which should be unique to the graph they are
/// contained in.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum Id {
    /// IRI
    Named(String),
    /// Blank node
    Blank(String),
}

// There's a custom debug implementation to hide the enum variant tag when printing,
// it saves some screen space that's not needed.
impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Id::Named(iri) => write!(f, "<{iri}>"),
            Id::Blank(id) => write!(f, "_:{id}"),
        }
    }
}

/// RDF Terms are either identifiers or literals.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum Term {
    /// Named IRI or blank node.
    Id(Id),
    /// Literal value.
    Literal(Literal),
}

// There's a custom debug implementation to hide the enum variant tag when printing,
// it saves some screen space that's not needed.
impl fmt::Debug for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Term::Id(id) => id.fmt(f),
            Term::Literal(lit) => lit.fmt(f),
        }
    }
}

/// RDF Literals always have a lexical 'form' as per
/// [RDF 1.1 Concepts And Abstract Syntax](https://www.w3.org/TR/rdf11-concepts/#dfn-literal).
///
/// They can optionally contain a datatype describing how the literal form maps to a literal value
/// (The default type is: [xs:string](http://www.w3.org/2001/XMLSchema#string), but we do not store
/// this).
/// If the datatype is [rdf:langString](http://www.w3.org/1999/02/22-rdf-syntax-ns#langString),
/// we can optionally supply a language tag ([BCP47](https://tools.ietf.org/html/bcp47)) such as
/// `"nl"` or `"fr"`.
///
/// # Examples
/// ```
/// // string
/// use hdt::containers::rdf::Literal;
/// let literal = Literal::new(String::from("hello"));
/// assert_eq!("\"hello\"", format!("{:?}", literal));
/// ```
/// ```
/// // typed literal
/// use hdt::containers::rdf::Literal;
/// let type_iri = String::from("http://www.w3.org/2001/XMLSchema#integer");
/// let typed_literal = Literal::new_typed(String::from("42"), type_iri);
/// assert_eq!("\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>", format!("{:?}", typed_literal));
/// ```
/// ```
/// // language tagged string
/// use hdt::containers::rdf::Literal;
/// let lang_tag = String::from("nl");
/// let lang_string = Literal::new_lang(String::from("hallo wereld"), lang_tag);
/// assert_eq!("\"hallo wereld\"@nl", format!("{:?}", lang_string));
/// ```
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct Literal {
    form: String,
    datatype: Option<String>,
    lang: Option<String>,
}

// There's a custom debug implementation to hide structure tags when printing,
// it saves some screen space that's not needed.
impl fmt::Debug for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(lang) = &self.lang {
            write!(f, "\"{}\"@{lang}", self.form)
        } else if let Some(dtype) = &self.datatype {
            write!(f, "\"{}\"^^<{dtype}>", self.form)
        } else {
            write!(f, "\"{}\"", self.form)
        }
    }
}

impl Literal {
    /// Create a new literal with type [xs:string](http://www.w3.org/2001/XMLSchema#string) (which
    /// we do not store since it is the default type).
    pub const fn new(form: String) -> Self {
        Literal { form, datatype: None, lang: None }
    }

    /// Create a new literal with a given form and datatype.
    pub const fn new_typed(form: String, datatype: String) -> Self {
        Literal { form, datatype: Some(datatype), lang: None }
    }

    /// Create a new literal with a given form and langauge. Automatically sets the type to
    /// [xs:langString](http://www.w3.org/2001/XMLSchema#langString)
    pub fn new_lang(form: String, lang: String) -> Self {
        let datatype = String::from("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString");

        Literal { form, datatype: Some(datatype), lang: Some(lang) }
    }
}

//! Generates OTTR templates for instantiating the classes of an OWL/RDFS ontology in a [`Model`].
//!
//! For each class there is a template with the class IRI, which also produces the rdf:type
//! triple, and one with the class IRI suffixed by `_notype`, which does not. The subject is the
//! parameter `id`, the other (optional) parameters are named by the prefixed IRIs of the
//! properties. Templates of subclasses instantiate the `_notype` templates of their superclasses.

mod ordering;

use crate::errors::MaplibError;
use crate::model::Model;
use oxrdf::vocab::rdf;
use oxrdf::{IriParseError, NamedNode, Term, Variable, VariableNameParseError};
use representation::constants::OTTR_TRIPLE;
use representation::dataset::NamedGraph;
use representation::polars_to_rdf::{df_as_result, QuerySolutions};
use representation::result::QueryResultKind;
use representation::variable_names::new_template_variable;
use representation::BaseRDFNodeType;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use templates::ast::{
    Argument, ConstantTerm, ConstantTermOrList, Instance, PType, Parameter, Signature, StottrTerm,
    Template,
};
use thiserror::Error;

const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const RDFS_LITERAL: &str = "http://www.w3.org/2000/01/rdf-schema#Literal";

/// Property types that imply that the object of the property is a resource (IRI or blank node).
const OBJECT_PROPERTY_TYPES: [&str; 7] = [
    "http://www.w3.org/2002/07/owl#ObjectProperty",
    "http://www.w3.org/2002/07/owl#TransitiveProperty",
    "http://www.w3.org/2002/07/owl#SymmetricProperty",
    "http://www.w3.org/2002/07/owl#AsymmetricProperty",
    "http://www.w3.org/2002/07/owl#ReflexiveProperty",
    "http://www.w3.org/2002/07/owl#IrreflexiveProperty",
    "http://www.w3.org/2002/07/owl#InverseFunctionalProperty",
];
const DATATYPE_PROPERTY_TYPE: &str = "http://www.w3.org/2002/07/owl#DatatypeProperty";

/// Datatypes that are not in the XSD namespace.
const NON_XSD_DATATYPES: [&str; 9] = [
    RDFS_LITERAL,
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#dirLangString",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#HTML",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#PlainLiteral",
    "http://www.w3.org/2002/07/owl#real",
    "http://www.w3.org/2002/07/owl#rational",
];

pub const SUBJECT_VARIABLE_NAME: &str = "id";
pub const NOTYPE_SUFFIX: &str = "_notype";

/// Prefixes that are known without being given.
pub const WELL_KNOWN_PREFIXES: [(&str, &str); 14] = [
    ("rdf", RDF),
    ("rdfs", RDFS),
    ("owl", OWL),
    ("xsd", XSD),
    ("skos", "http://www.w3.org/2004/02/skos/core#"),
    ("dcterms", "http://purl.org/dc/terms/"),
    ("dc", "http://purl.org/dc/elements/1.1/"),
    ("foaf", "http://xmlns.com/foaf/0.1/"),
    ("prov", "http://www.w3.org/ns/prov#"),
    ("sh", "http://www.w3.org/ns/shacl#"),
    ("dcat", "http://www.w3.org/ns/dcat#"),
    ("time", "http://www.w3.org/2006/time#"),
    ("geo", "http://www.opengis.net/ont/geosparql#"),
    ("schema", "https://schema.org/"),
];

#[derive(Debug, Error)]
pub enum TemplateGeneratorError {
    #[error("Invalid prefix name: {0:?}")]
    InvalidPrefixName(String),
    #[error(transparent)]
    Maplib(#[from] MaplibError),
    #[error(transparent)]
    IriParse(#[from] IriParseError),
    #[error(transparent)]
    VariableName(#[from] VariableNameParseError),
}

pub struct GeneratedTemplates {
    /// The templates keyed by their IRIs: first the `_notype` templates, then the typed ones,
    /// each in topological order of the class hierarchy.
    pub templates: Vec<(String, Template)>,
    /// Warnings, e.g. about cycles in the class hierarchy.
    pub warnings: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
enum Direction {
    // Out sorts before In.
    /// ?id <property> ?value
    Out,
    /// ?value <property> ?id
    In,
}

/// A property in a direction.
type Slot = (String, Direction);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Iri,
    Literal,
}

/// The type of a parameter: an IRI, a literal of a datatype or unknown.
type SlotType = Option<PType>;

/// Generate templates for instantiating the classes in the ontology in `graph` of the model.
/// `prefixes` (prefix name, namespace) are used to name the parameters, in the given order, and
/// take precedence over the well-known prefixes.
pub fn generate_templates(
    m: &Model,
    graph: Option<&NamedGraph>,
    prefixes: &[(String, String)],
) -> Result<GeneratedTemplates, TemplateGeneratorError> {
    let mut known_prefixes: HashMap<String, String> = WELL_KNOWN_PREFIXES
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    for (name, ns) in prefixes {
        if !is_pn_prefix(name) {
            return Err(TemplateGeneratorError::InvalidPrefixName(name.clone()));
        }
        // User prefixes take precedence for their namespace.
        known_prefixes.retain(|k, v| v.as_str() != ns.as_str() && k.as_str() != name.as_str());
        known_prefixes.insert(name.clone(), ns.clone());
    }

    let q = |query: &str| run_query(m, query, graph);

    let declared_classes: Vec<String> = q(CLASSES_QUERY)?
        .into_iter()
        .filter_map(|mut r| r.remove(0))
        .collect();
    let datatypes: HashSet<String> = q(DATATYPES_QUERY)?
        .into_iter()
        .filter_map(|mut r| r.remove(0))
        .collect();
    let subclass_edges: BTreeSet<(String, String)> =
        q(SUBCLASS_QUERY)?.into_iter().filter_map(pair).collect();

    let is_datatype = |iri: &str| {
        iri.starts_with(XSD) || NON_XSD_DATATYPES.contains(&iri) || datatypes.contains(iri)
    };

    let mut property_types: HashMap<String, HashSet<String>> = HashMap::new();
    for (p, t) in q(PROPERTY_TYPES_QUERY)?.into_iter().filter_map(pair) {
        property_types.entry(p).or_default().insert(t);
    }
    let mut property_domains: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (p, d) in q(DOMAINS_QUERY)?.into_iter().filter_map(pair) {
        property_domains.entry(p).or_default().insert(d);
    }
    let mut property_ranges: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (p, r) in q(RANGES_QUERY)?.into_iter().filter_map(pair) {
        property_ranges.entry(p).or_default().insert(r);
    }
    // (class, property) -> range hints from the restriction
    let mut restrictions: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    for mut r in q(RESTRICTIONS_QUERY)? {
        let (Some(d), Some(p)) = (r[0].take(), r[1].take()) else {
            continue;
        };
        let hints = restrictions.entry((d, p)).or_default();
        if let Some(range) = r[2].take() {
            hints.insert(range);
        }
    }

    let kind = |prop: &str, ranges: &BTreeSet<String>| -> Option<Kind> {
        let (is_object, is_datatype_prop) = match property_types.get(prop) {
            Some(types) => (
                OBJECT_PROPERTY_TYPES.iter().any(|t| types.contains(*t)),
                types.contains(DATATYPE_PROPERTY_TYPE),
            ),
            None => (false, false),
        };
        match (is_object, is_datatype_prop) {
            (true, false) => return Some(Kind::Iri),
            (false, true) => return Some(Kind::Literal),
            (true, true) => return None,
            (false, false) => {}
        }
        if !ranges.is_empty() {
            if ranges.iter().all(|r| is_datatype(r.as_str())) {
                return Some(Kind::Literal);
            }
            if !ranges.iter().any(|r| is_datatype(r.as_str())) {
                return Some(Kind::Iri);
            }
        }
        None
    };

    let out_type = |prop: &str, ranges: &BTreeSet<String>| -> SlotType {
        match kind(prop, ranges) {
            Some(Kind::Iri) => Some(iri_ptype()),
            Some(Kind::Literal) => {
                let dts: Vec<&String> = ranges
                    .iter()
                    .filter(|r| is_datatype(r.as_str()) && r.as_str() != RDFS_LITERAL)
                    .collect();
                let others = ranges
                    .iter()
                    .any(|r| !dts.contains(&r) && r.as_str() != RDFS_LITERAL);
                if dts.len() == 1 && !others {
                    Some(literal_ptype(dts[0]))
                } else {
                    None
                }
            }
            None => None,
        }
    };

    let empty = BTreeSet::new();
    let mut own_slots: BTreeMap<String, HashMap<Slot, SlotType>> = BTreeMap::new();
    for (prop, domains) in &property_domains {
        let ranges = property_ranges.get(prop).unwrap_or(&empty);
        let t = out_type(prop, ranges);
        for d in domains {
            own_slots
                .entry(d.clone())
                .or_default()
                .insert((prop.clone(), Direction::Out), t.clone());
        }
    }
    for ((d, prop), hints) in &restrictions {
        let slots = own_slots.entry(d.clone()).or_default();
        let key = (prop.clone(), Direction::Out);
        if !matches!(slots.get(&key), Some(Some(_))) {
            let ranges = match property_ranges.get(prop) {
                Some(r) if !r.is_empty() => r,
                _ => hints,
            };
            slots.insert(key, out_type(prop, ranges));
        }
    }
    for (prop, ranges) in &property_ranges {
        if kind(prop, ranges) != Some(Kind::Iri) {
            continue;
        }
        for r in ranges {
            if is_datatype(r) {
                continue;
            }
            let slots = own_slots.entry(r.clone()).or_default();
            // A property with the class as both domain and range is only included in the forward direction.
            if !slots.contains_key(&(prop.clone(), Direction::Out)) {
                slots.insert((prop.clone(), Direction::In), Some(iri_ptype()));
            }
        }
    }

    let mut classes: HashSet<String> = declared_classes.into_iter().collect();
    classes.extend(own_slots.keys().cloned());
    for (c, p) in &subclass_edges {
        classes.insert(c.clone());
        classes.insert(p.clone());
    }
    classes.retain(|c| !is_datatype(c));
    let edges: Vec<(String, String)> = subclass_edges
        .iter()
        .filter(|(c, p)| classes.contains(c) && classes.contains(p))
        .cloned()
        .collect();
    let ordering::Ordering {
        ordering,
        parents: parents_of,
        broken_cycle_members,
    } = ordering::topological_sort(&classes, &edges);
    let mut warnings = vec![];
    if !broken_cycle_members.is_empty() {
        warnings.push(format!(
            "Cyclic rdfs:subClassOf hierarchy, ignoring the subclass edges into: {}",
            broken_cycle_members.join(", ")
        ));
    }

    let all_properties: BTreeSet<&str> = own_slots
        .values()
        .flat_map(|slots| slots.keys().map(|(p, _)| p.as_str()))
        .collect();
    let mut property_names = PrefixedNamer::new(known_prefixes, &all_properties);

    let subj = Variable::new_unchecked(SUBJECT_VARIABLE_NAME);
    let mut templates_without_typing: Vec<(String, Template)> = Vec::with_capacity(ordering.len());
    let mut template_index: HashMap<String, usize> = HashMap::with_capacity(ordering.len());
    let mut slot_layouts: HashMap<String, Vec<(Slot, String, SlotType)>> = HashMap::new();
    let no_slots = HashMap::new();
    for c in &ordering {
        let own = own_slots.get(c).unwrap_or(&no_slots);
        let mut own_ordered: Vec<&Slot> = own.keys().collect();
        own_ordered.sort_by(|a, b| (a.1, &a.0).cmp(&(b.1, &b.0)));
        let parents = parents_of.get(c).map(|p| p.as_slice()).unwrap_or(&[]);
        // Own slots take precedence over inherited ones, and forward slots get names before
        // reverse slots, so that a plain property name always refers to the forward direction.
        let mut candidates: Vec<(&Slot, Vec<String>, SlotType)> = vec![];
        let mut seen: HashSet<&Slot> = HashSet::new();
        for slot in &own_ordered {
            seen.insert(slot);
            let base = property_names.name(&slot.0);
            let names_to_try = match slot.1 {
                Direction::Out => vec![base],
                Direction::In => {
                    let inverse = format!("{base}_inverse");
                    vec![base, inverse]
                }
            };
            candidates.push((slot, names_to_try, own[*slot].clone()));
        }
        for p in parents {
            for (slot, parent_name, t) in &slot_layouts[p] {
                if !seen.insert(slot) {
                    continue;
                }
                let mut names_to_try = vec![parent_name.clone()];
                if slot.1 == Direction::In {
                    names_to_try.push(format!("{}_inverse", property_names.name(&slot.0)));
                }
                candidates.push((slot, names_to_try, t.clone()));
            }
        }
        // Stable, so the order within each direction is kept.
        candidates.sort_by_key(|(slot, _, _)| slot.1);

        let mut used_names: HashSet<String> = HashSet::from([SUBJECT_VARIABLE_NAME.to_string()]);
        let mut layout: Vec<(Slot, String, SlotType)> = Vec::with_capacity(candidates.len());
        let mut names: HashMap<&Slot, String> = HashMap::with_capacity(candidates.len());
        for (slot, names_to_try, t) in candidates {
            let name = unique_name(&names_to_try, &mut used_names);
            names.insert(slot, name.clone());
            layout.push((slot.clone(), name, t));
        }

        let mut parameters = vec![Parameter {
            optional: false,
            non_blank: false,
            ptype: Some(iri_ptype()),
            variable: subj.clone(),
            default_value: None,
        }];
        for (_, name, t) in &layout {
            parameters.push(Parameter {
                optional: true,
                non_blank: false,
                ptype: t.clone(),
                variable: new_template_variable(name.clone())?,
                default_value: None,
            });
        }

        let mut instances = vec![];
        for slot in &own_ordered {
            let (prop, direction) = slot;
            let v = variable_term(new_template_variable(names[*slot].clone())?);
            let p = iri_term(NamedNode::new(prop.clone())?);
            let s = variable_term(subj.clone());
            instances.push(match direction {
                Direction::Out => triple(s, p, v),
                Direction::In => triple(v, p, s),
            });
        }

        // Each inherited slot is passed to the first superclass that has it,
        // so that triples are not produced more than once.
        let mut handled: HashSet<&Slot> = own.keys().collect();
        for p in parents {
            let mut arguments = vec![variable_term(subj.clone())];
            for (slot, _, _) in &slot_layouts[p] {
                if handled.contains(slot) {
                    arguments.push(StottrTerm::ConstantTerm(ConstantTermOrList::ConstantTerm(
                        ConstantTerm::None,
                    )));
                } else {
                    handled.insert(slot);
                    arguments.push(variable_term(new_template_variable(names[slot].clone())?));
                }
            }
            let parent_template = &templates_without_typing[template_index[p]].1;
            instances.push(instance(parent_template.signature.iri.clone(), arguments));
        }
        drop(handled);
        drop(names);

        let template = Template {
            signature: Signature {
                iri: NamedNode::new(format!("{c}{NOTYPE_SUFFIX}"))?,
                parameter_list: parameters,
                annotation_list: None,
            },
            pattern_list: instances,
        };
        slot_layouts.insert(c.clone(), layout);
        template_index.insert(c.clone(), templates_without_typing.len());
        templates_without_typing.push((c.clone(), template));
    }

    let mut templates = Vec::with_capacity(2 * templates_without_typing.len());
    for (c, template) in &templates_without_typing {
        templates.push((format!("{c}{NOTYPE_SUFFIX}"), template.clone()));
    }
    for (c, template) in templates_without_typing {
        let class = NamedNode::new(c.clone())?;
        let mut pattern_list = template.pattern_list;
        pattern_list.push(triple(
            variable_term(subj.clone()),
            iri_term(rdf::TYPE.into_owned()),
            iri_term(class.clone()),
        ));
        templates.push((
            c,
            Template {
                signature: Signature {
                    iri: class,
                    parameter_list: template.signature.parameter_list,
                    annotation_list: None,
                },
                pattern_list,
            },
        ));
    }
    Ok(GeneratedTemplates {
        templates,
        warnings,
    })
}

fn iri_ptype() -> PType {
    PType::from(&BaseRDFNodeType::IRI.into_default_input_rdf_node_state())
}

fn literal_ptype(datatype: &str) -> PType {
    PType::from(
        &BaseRDFNodeType::Literal(NamedNode::new_unchecked(datatype), None)
            .into_default_input_rdf_node_state(),
    )
}

fn variable_term(v: Variable) -> StottrTerm {
    StottrTerm::Variable(v)
}

fn iri_term(nn: NamedNode) -> StottrTerm {
    StottrTerm::ConstantTerm(ConstantTermOrList::ConstantTerm(ConstantTerm::Iri(nn)))
}

fn instance(template_iri: NamedNode, arguments: Vec<StottrTerm>) -> Instance {
    Instance {
        list_expander: None,
        template_iri,
        argument_list: arguments
            .into_iter()
            .map(|term| Argument {
                list_expand: false,
                term,
            })
            .collect(),
    }
}

fn triple(s: StottrTerm, p: StottrTerm, o: StottrTerm) -> Instance {
    instance(NamedNode::new_unchecked(OTTR_TRIPLE), vec![s, p, o])
}

fn pair(mut row: Vec<Option<String>>) -> Option<(String, String)> {
    Some((row[0].take()?, row[1].take()?))
}

/// Runs a SELECT query and returns the values of its projected variables, in order, as strings.
fn run_query(
    m: &Model,
    query: &str,
    graph: Option<&NamedGraph>,
) -> Result<Vec<Vec<Option<String>>>, MaplibError> {
    let res = m.query(
        query,
        None,
        graph,
        vec![NamedGraph::transient()],
        None,
        false,
        None,
    )?;
    let QueryResultKind::Select(sm) = res.kind else {
        unreachable!("Should never happen")
    };
    let projected = projected_variables(query);
    let QuerySolutions {
        variables,
        solutions,
    } = df_as_result(&sm, m.triplestore.global_cats.clone());
    let idx: Vec<Option<usize>> = projected
        .iter()
        .map(|name| variables.iter().position(|v| v.as_str() == *name))
        .collect();
    Ok(solutions
        .into_iter()
        .map(|row| {
            idx.iter()
                .map(|i| {
                    i.and_then(|i| row.get(i).cloned().flatten())
                        .map(|t| match t {
                            Term::NamedNode(nn) => nn.into_string(),
                            Term::BlankNode(b) => b.into_string(),
                            Term::Literal(l) => l.value().to_string(),
                        })
                })
                .collect()
        })
        .collect())
}

/// The variables of `SELECT DISTINCT ?a ?b WHERE`.
fn projected_variables(query: &str) -> Vec<&str> {
    let start = query.find("SELECT DISTINCT").unwrap() + "SELECT DISTINCT".len();
    let end = query[start..].find("WHERE").unwrap() + start;
    query[start..end]
        .split_whitespace()
        .map(|v| v.trim_start_matches('?'))
        .collect()
}

macro_rules! prefixed {
    ($q:literal) => {
        concat!(
            "\nPREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\nPREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\nPREFIX owl: <http://www.w3.org/2002/07/owl#>\n",
            $q
        )
    };
}

const CLASSES_QUERY: &str = prefixed!(
    r#"
SELECT DISTINCT ?c WHERE {
    { ?c a owl:Class } UNION { ?c a rdfs:Class }
    FILTER(ISIRI(?c))
}
"#
);

const DATATYPES_QUERY: &str = prefixed!(
    r#"
SELECT DISTINCT ?d WHERE {
    ?d a rdfs:Datatype .
    FILTER(ISIRI(?d))
}
"#
);

const SUBCLASS_QUERY: &str = prefixed!(
    r#"
SELECT DISTINCT ?child ?parent WHERE {
    { ?child rdfs:subClassOf ?parent }
    UNION
    { ?child rdfs:subClassOf/owl:intersectionOf/rdf:rest*/rdf:first ?parent }
    FILTER(ISIRI(?child) && ISIRI(?parent))
}
"#
);

const PROPERTY_TYPES_QUERY: &str = prefixed!(
    r#"
SELECT DISTINCT ?property ?property_type WHERE {
    ?property a ?property_type .
    { ?property rdfs:domain ?x } UNION { ?property rdfs:range ?x } UNION { ?r owl:onProperty ?property }
    FILTER(ISIRI(?property) && ISIRI(?property_type))
}
"#
);

const DOMAINS_QUERY: &str = prefixed!(
    r#"
SELECT DISTINCT ?property ?domain WHERE {
    { ?property rdfs:domain ?domain }
    UNION
    { ?property rdfs:domain/owl:unionOf/rdf:rest*/rdf:first ?domain }
    FILTER(ISIRI(?property) && ISIRI(?domain))
}
"#
);

const RANGES_QUERY: &str = prefixed!(
    r#"
SELECT DISTINCT ?property ?range WHERE {
    { ?property rdfs:range ?range }
    UNION
    { ?property rdfs:range/owl:unionOf/rdf:rest*/rdf:first ?range }
    FILTER(ISIRI(?property) && ISIRI(?range))
}
"#
);

const RESTRICTIONS_QUERY: &str = prefixed!(
    r#"
SELECT DISTINCT ?domain ?property ?range WHERE {
    {
        ?domain rdfs:subClassOf ?restr .
    } UNION {
        ?domain rdfs:subClassOf/owl:intersectionOf/rdf:rest*/rdf:first ?restr .
    } UNION {
        ?domain owl:equivalentClass/owl:intersectionOf/rdf:rest*/rdf:first ?restr .
    }
    ?restr a owl:Restriction .
    ?restr owl:onProperty ?property .
    OPTIONAL {
        ?restr owl:someValuesFrom|owl:allValuesFrom|owl:onClass|owl:onDataRange ?range .
        FILTER(ISIRI(?range))
    }
    FILTER(ISIRI(?domain) && ISIRI(?property))
}
"#
);

fn is_pn_chars_base(c: char) -> bool {
    matches!(c,
        'A'..='Z'
        | 'a'..='z'
        | '\u{00C0}'..='\u{00D6}'
        | '\u{00D8}'..='\u{00F6}'
        | '\u{00F8}'..='\u{02FF}'
        | '\u{0370}'..='\u{037D}'
        | '\u{037F}'..='\u{1FFF}'
        | '\u{200C}'..='\u{200D}'
        | '\u{2070}'..='\u{218F}'
        | '\u{2C00}'..='\u{2FEF}'
        | '\u{3001}'..='\u{D7FF}'
        | '\u{F900}'..='\u{FDCF}'
        | '\u{FDF0}'..='\u{FFFD}'
        | '\u{10000}'..='\u{EFFFF}')
}

fn is_pn_chars_u(c: char) -> bool {
    is_pn_chars_base(c) || c == '_'
}

fn is_pn_chars(c: char) -> bool {
    is_pn_chars_u(c)
        || matches!(c, '-' | '0'..='9' | '\u{00B7}' | '\u{0300}'..='\u{036F}' | '\u{203F}'..='\u{2040}')
}

/// Is `s` a valid prefix name (PN_PREFIX)?
pub fn is_pn_prefix(s: &str) -> bool {
    let chars: Vec<char> = s.chars().collect();
    let Some((first, rest)) = chars.split_first() else {
        return false;
    };
    if !is_pn_chars_base(*first) {
        return false;
    }
    match rest.split_last() {
        None => true,
        Some((last, middle)) => {
            is_pn_chars(*last) && middle.iter().all(|c| is_pn_chars(*c) || *c == '.')
        }
    }
}

/// Make a valid local name (PN_LOCAL without escapes) from the local part of an IRI.
fn sanitize_local(local: &str) -> String {
    let mut local: String = local
        .chars()
        .map(|c| {
            if is_pn_chars(c) || c == '.' || c == ':' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if local.is_empty() {
        return "property".to_string();
    }
    let first = local.chars().next().unwrap();
    if !(is_pn_chars_u(first) || first == ':' || first.is_ascii_digit()) {
        local.insert(0, '_');
    }
    if local.ends_with('.') {
        local.pop();
        local.push('_');
    }
    local
}

/// Split an IRI into a namespace and a local name at the last #, / or :.
fn split_iri(iri: &str) -> (&str, &str) {
    let stripped = iri.trim_end_matches(['/', '#']);
    for sep in ['#', '/', ':'] {
        if let Some(idx) = stripped.rfind(sep) {
            return (&iri[..idx + 1], &stripped[idx + 1..]);
        }
    }
    ("", stripped)
}

/// Names properties by prefixed IRIs, generating prefixes ns1, ns2, ... for unknown namespaces.
struct PrefixedNamer {
    /// (namespace, prefix), longest namespaces first, so that the most specific prefix is used.
    by_namespace: Vec<(String, String)>,
    names: HashMap<String, String>,
}

impl PrefixedNamer {
    fn new(mut prefixes: HashMap<String, String>, iris: &BTreeSet<&str>) -> PrefixedNamer {
        let mut namer = PrefixedNamer {
            by_namespace: prefixes
                .iter()
                .map(|(p, ns)| (ns.clone(), p.clone()))
                .collect(),
            names: HashMap::new(),
        };
        namer.sort();
        let unknown_namespaces: BTreeSet<&str> = iris
            .iter()
            .filter(|iri| namer.known(iri).is_none())
            .map(|iri| split_iri(iri).0)
            .collect();
        let mut i = 1;
        for ns in unknown_namespaces {
            while prefixes.contains_key(&format!("ns{i}")) {
                i += 1;
            }
            let p = format!("ns{i}");
            prefixes.insert(p.clone(), ns.to_string());
            namer.by_namespace.push((ns.to_string(), p));
            i += 1;
        }
        namer.sort();
        namer
    }

    fn sort(&mut self) {
        // Python orders by the length in code points, so we do too.
        self.by_namespace
            .sort_by(|a, b| (b.0.chars().count(), &a.1).cmp(&(a.0.chars().count(), &b.1)));
    }

    fn known(&self, iri: &str) -> Option<String> {
        for (ns, p) in &self.by_namespace {
            if iri.starts_with(ns.as_str()) && iri.len() > ns.len() {
                return Some(format!("{p}:{}", sanitize_local(&iri[ns.len()..])));
            }
        }
        None
    }

    fn name(&mut self, iri: &str) -> String {
        if let Some(name) = self.names.get(iri) {
            return name.clone();
        }
        let name = match self.known(iri) {
            Some(name) => name,
            None => {
                // Only happens for IRIs that are their own namespace.
                let (ns, local) = split_iri(iri);
                let p = self
                    .by_namespace
                    .iter()
                    .find(|(n, _)| n == ns)
                    .map(|(_, p)| p.as_str())
                    .unwrap_or_default();
                format!("{p}:{}", sanitize_local(local))
            }
        };
        self.names.insert(iri.to_string(), name.clone());
        name
    }
}

fn unique_name(candidates: &[String], used: &mut HashSet<String>) -> String {
    for c in candidates {
        if !used.contains(c) {
            used.insert(c.clone());
            return c.clone();
        }
    }
    let base = &candidates[0];
    let mut i = 2;
    while used.contains(&format!("{base}_{i}")) {
        i += 1;
    }
    let name = format!("{base}_{i}");
    used.insert(name.clone());
    name
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_iri() {
        assert_eq!(split_iri("http://ex.org/a"), ("http://ex.org/", "a"));
        assert_eq!(split_iri("http://ex.org/onto/"), ("http://ex.org/", "onto"));
        assert_eq!(split_iri("http://ex.org/o#a"), ("http://ex.org/o#", "a"));
        assert_eq!(split_iri("urn:x:y"), ("urn:x:", "y"));
        assert_eq!(split_iri("abc"), ("", "abc"));
    }

    #[test]
    fn test_sanitize_local() {
        assert_eq!(sanitize_local("has-part.v2"), "has-part.v2");
        assert_eq!(sanitize_local("onto/"), "onto_");
        assert_eq!(sanitize_local("høyde"), "høyde");
        assert_eq!(sanitize_local(""), "property");
        assert_eq!(sanitize_local("-a"), "_-a");
        assert_eq!(sanitize_local("a."), "a_");
        assert_eq!(sanitize_local("1a"), "1a");
        assert_eq!(sanitize_local("a b"), "a_b");
    }

    #[test]
    fn test_is_pn_prefix() {
        assert!(is_pn_prefix("ex"));
        assert!(is_pn_prefix("e"));
        assert!(is_pn_prefix("cmns-org"));
        assert!(is_pn_prefix("a.b"));
        assert!(!is_pn_prefix(""));
        assert!(!is_pn_prefix("1bad"));
        assert!(!is_pn_prefix("_a"));
        assert!(!is_pn_prefix("a."));
    }

    #[test]
    fn test_namer_generates_prefixes() {
        let iris = BTreeSet::from(["http://a.org/x", "http://b.org/y", "http://ex.org/z"]);
        let prefixes = HashMap::from([
            ("ex".to_string(), "http://ex.org/".to_string()),
            ("ns1".to_string(), "http://taken.org/".to_string()),
        ]);
        let mut namer = PrefixedNamer::new(prefixes, &iris);
        assert_eq!(namer.name("http://a.org/x"), "ns2:x");
        assert_eq!(namer.name("http://b.org/y"), "ns3:y");
        assert_eq!(namer.name("http://ex.org/z"), "ex:z");
    }

    #[test]
    fn test_unique_name() {
        let mut used = HashSet::from(["id".to_string(), "ex:a".to_string()]);
        assert_eq!(unique_name(&["ex:b".to_string()], &mut used), "ex:b");
        assert_eq!(
            unique_name(&["ex:a".to_string(), "ex:a_inverse".to_string()], &mut used),
            "ex:a_inverse"
        );
        assert_eq!(unique_name(&["ex:a".to_string()], &mut used), "ex:a_2");
        assert_eq!(unique_name(&["ex:a".to_string()], &mut used), "ex:a_3");
    }

    #[test]
    fn test_topological_sort_prunes_and_breaks_cycles() {
        let s = |x: &str| x.to_string();
        let classes = vec![s("A"), s("B"), s("D")];
        let edges = vec![(s("B"), s("A")), (s("D"), s("B")), (s("D"), s("A"))];
        let o = ordering::topological_sort(&classes, &edges);
        assert_eq!(o.ordering, vec![s("A"), s("B"), s("D")]);
        assert_eq!(o.parents["D"], vec![s("B")]);
        assert!(o.broken_cycle_members.is_empty());

        let edges = vec![(s("A"), s("B")), (s("B"), s("A")), (s("A"), s("A"))];
        let o = ordering::topological_sort(&classes, &edges);
        assert_eq!(o.ordering, vec![s("D"), s("A"), s("B")]);
        assert_eq!(o.broken_cycle_members, vec![s("A")]);
        assert!(o.parents["A"].is_empty());
        assert_eq!(o.parents["B"], vec![s("A")]);
    }
}

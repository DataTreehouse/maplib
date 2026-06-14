//! Serialize OTTR templates into a flattened, SPARQL-friendly RDF representation.
//!
//! This is not the standard OTTR RDF (rOTTR) list encoding but a denormalized vocabulary
//! (prefix `mtpl`, base `https://datatreehouse.github.io/maplib/vocab#`) so that template
//! structure and inter-template/IRI relationships can be queried with one-hop SPARQL and used
//! to derive SHACL shapes.

use crate::ast::{
    ConstantTerm, ConstantTermOrList, Instance, PType, Parameter, StottrTerm, Template,
};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{BlankNode, Literal, NamedNode, NamedOrBlankNode, Term, Triple};
use representation::constants::{MAPLIB_PREFIX_IRI, OTTR_TRIPLE};
use std::collections::HashSet;

/// Build a `NamedNode` in the maplib template vocabulary.
fn vocab(local: &str) -> NamedNode {
    NamedNode::new_unchecked(format!("{MAPLIB_PREFIX_IRI}{local}"))
}

fn bool_lit(b: bool) -> Literal {
    Literal::new_typed_literal(if b { "true" } else { "false" }, xsd::BOOLEAN.into_owned())
}

fn int_lit(i: usize) -> Literal {
    Literal::new_typed_literal(i.to_string(), xsd::INTEGER.into_owned())
}

fn string_lit(s: &str) -> Literal {
    Literal::new_simple_literal(s)
}

fn fresh_bnode(counter: &mut usize) -> BlankNode {
    let b = BlankNode::new_unchecked(format!("mtpl{counter}"));
    *counter += 1;
    b
}

fn constant_term_to_term(ct: &ConstantTerm) -> Option<Term> {
    match ct {
        ConstantTerm::Iri(i) => Some(Term::NamedNode(i.clone())),
        ConstantTerm::BlankNode(b) => Some(Term::BlankNode(b.clone())),
        ConstantTerm::Literal(l) => Some(Term::Literal(l.clone())),
        ConstantTerm::None => None,
    }
}

/// Reduce a (possibly nested) `PType` to (innermost base type, outermost cardinality, lub?).
/// Nested lists collapse to the outermost cardinality; `None` types yield `None`.
fn decompose_ptype(p: &PType) -> Option<(NamedNode, &'static str, bool)> {
    match p {
        PType::None => None,
        PType::Basic(nn) => Some((nn.clone(), "single", false)),
        PType::Lub(inner) => decompose_ptype(inner).map(|(b, c, _)| (b, c, true)),
        PType::List(inner) => decompose_ptype(inner).map(|(b, _, l)| (b, "list", l)),
        PType::NEList(inner) => decompose_ptype(inner).map(|(b, _, l)| (b, "nelist", l)),
    }
}

/// The predicate IRI of an `ottr:Triple` instance, if its predicate argument (index 1)
/// is a constant IRI.
fn ottr_triple_predicate_iri(inst: &Instance) -> Option<NamedNode> {
    let arg = inst.argument_list.get(1)?;
    if let StottrTerm::ConstantTerm(ConstantTermOrList::ConstantTerm(ConstantTerm::Iri(i))) =
        &arg.term
    {
        Some(i.clone())
    } else {
        None
    }
}

/// Serialize a slice of templates to triples in the flattened maplib vocabulary.
/// The built-in `ottr:Triple` template is skipped.
pub fn templates_to_triples(templates: &[Template]) -> Vec<Triple> {
    let mut triples = vec![];
    let mut counter = 0usize;
    for t in templates {
        if t.signature.iri.as_str() == OTTR_TRIPLE {
            continue;
        }
        template_to_triples(t, &mut triples, &mut counter);
    }
    triples
}

fn template_to_triples(t: &Template, triples: &mut Vec<Triple>, counter: &mut usize) {
    let subj = NamedOrBlankNode::NamedNode(t.signature.iri.clone());
    triples.push(Triple::new(
        subj.clone(),
        rdf::TYPE.into_owned(),
        vocab("Template"),
    ));

    for (idx, p) in t.signature.parameter_list.iter().enumerate() {
        let pb = fresh_bnode(counter);
        triples.push(Triple::new(subj.clone(), vocab("hasParameter"), pb.clone()));
        parameter_to_triples(p, idx, &pb, triples);
    }

    for (idx, inst) in t.pattern_list.iter().enumerate() {
        let ib = fresh_bnode(counter);
        triples.push(Triple::new(subj.clone(), vocab("hasInstance"), ib.clone()));
        instance_to_triples(inst, idx, &ib, triples, counter);
        if inst.template_iri.as_str() == OTTR_TRIPLE {
            if let Some(pred) = ottr_triple_predicate_iri(inst) {
                triples.push(Triple::new(subj.clone(), vocab("usesPredicate"), pred));
            }
        }
    }

    // Every distinct IRI referenced by the template, excluding the template's own IRI.
    let mut nns = vec![];
    t.find_named_nodes(&mut nns);
    let mut seen = HashSet::new();
    for nn in nns {
        if nn.as_str() == t.signature.iri.as_str() {
            continue;
        }
        if seen.insert(nn.as_str().to_string()) {
            triples.push(Triple::new(subj.clone(), vocab("referencesIri"), nn));
        }
    }
}

fn parameter_to_triples(p: &Parameter, idx: usize, pb: &BlankNode, triples: &mut Vec<Triple>) {
    let s = NamedOrBlankNode::BlankNode(pb.clone());
    triples.push(Triple::new(
        s.clone(),
        rdf::TYPE.into_owned(),
        vocab("Parameter"),
    ));
    triples.push(Triple::new(s.clone(), vocab("index"), int_lit(idx)));
    triples.push(Triple::new(
        s.clone(),
        vocab("variableName"),
        string_lit(p.variable.as_str()),
    ));
    triples.push(Triple::new(
        s.clone(),
        vocab("optional"),
        bool_lit(p.optional),
    ));
    triples.push(Triple::new(
        s.clone(),
        vocab("nonBlank"),
        bool_lit(p.non_blank),
    ));
    if let Some(pt) = &p.ptype {
        if let Some((base, card, lub)) = decompose_ptype(pt) {
            triples.push(Triple::new(s.clone(), vocab("type"), base));
            triples.push(Triple::new(
                s.clone(),
                vocab("cardinality"),
                string_lit(card),
            ));
            triples.push(Triple::new(s.clone(), vocab("lub"), bool_lit(lub)));
        }
    }
    if let Some(ConstantTermOrList::ConstantTerm(ct)) = &p.default_value {
        if let Some(term) = constant_term_to_term(ct) {
            triples.push(Triple::new(s.clone(), vocab("defaultValue"), term));
        }
    }
}

fn instance_to_triples(
    inst: &Instance,
    idx: usize,
    ib: &BlankNode,
    triples: &mut Vec<Triple>,
    counter: &mut usize,
) {
    let s = NamedOrBlankNode::BlankNode(ib.clone());
    triples.push(Triple::new(
        s.clone(),
        rdf::TYPE.into_owned(),
        vocab("Instance"),
    ));
    triples.push(Triple::new(s.clone(), vocab("index"), int_lit(idx)));
    triples.push(Triple::new(
        s.clone(),
        vocab("callsTemplate"),
        inst.template_iri.clone(),
    ));
    if let Some(le) = &inst.list_expander {
        triples.push(Triple::new(
            s.clone(),
            vocab("listExpander"),
            string_lit(&le.to_string()),
        ));
    }
    for (aidx, arg) in inst.argument_list.iter().enumerate() {
        let ab = fresh_bnode(counter);
        triples.push(Triple::new(s.clone(), vocab("hasArgument"), ab.clone()));
        let sa = NamedOrBlankNode::BlankNode(ab);
        triples.push(Triple::new(
            sa.clone(),
            rdf::TYPE.into_owned(),
            vocab("Argument"),
        ));
        triples.push(Triple::new(sa.clone(), vocab("index"), int_lit(aidx)));
        triples.push(Triple::new(
            sa.clone(),
            vocab("listExpand"),
            bool_lit(arg.list_expand),
        ));
        match &arg.term {
            StottrTerm::Variable(v) => {
                triples.push(Triple::new(
                    sa.clone(),
                    vocab("variableName"),
                    string_lit(v.as_str()),
                ));
            }
            StottrTerm::ConstantTerm(ConstantTermOrList::ConstantTerm(ct)) => {
                if let Some(term) = constant_term_to_term(ct) {
                    triples.push(Triple::new(sa.clone(), vocab("constantValue"), term));
                }
            }
            StottrTerm::ConstantTerm(ConstantTermOrList::ConstantList(_)) | StottrTerm::List(_) => {
                triples.push(Triple::new(sa.clone(), vocab("isList"), bool_lit(true)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::TemplateDataset;
    use crate::document::document_from_str;
    use representation::prefixes::get_default_prefixes;

    fn build(s: &str) -> Vec<Triple> {
        let doc = document_from_str(s, Some(&get_default_prefixes())).unwrap();
        let ds = TemplateDataset::from_documents(vec![doc]).unwrap();
        templates_to_triples(&ds.templates)
    }

    fn subject_str(s: &NamedOrBlankNode) -> String {
        match s {
            NamedOrBlankNode::NamedNode(n) => n.as_str().to_string(),
            NamedOrBlankNode::BlankNode(b) => format!("_:{}", b.as_str()),
            #[allow(unreachable_patterns)]
            _ => unreachable!(),
        }
    }

    fn term_str(t: &Term) -> String {
        match t {
            Term::NamedNode(n) => n.as_str().to_string(),
            Term::BlankNode(b) => format!("_:{}", b.as_str()),
            Term::Literal(l) => l.value().to_string(),
            #[allow(unreachable_patterns)]
            _ => unreachable!(),
        }
    }

    fn pred(local: &str) -> String {
        format!("{MAPLIB_PREFIX_IRI}{local}")
    }

    /// Objects of (subject, mtpl:local) triples.
    fn objs(ts: &[Triple], subj: &str, local: &str) -> Vec<String> {
        let p = pred(local);
        ts.iter()
            .filter(|t| subject_str(&t.subject) == subj && t.predicate.as_str() == p)
            .map(|t| term_str(&t.object))
            .collect()
    }

    /// All blank-node subjects reachable from `subj` via mtpl:local.
    fn child_bnodes(ts: &[Triple], subj: &str, local: &str) -> Vec<String> {
        objs(ts, subj, local)
    }

    const PERSON: &str = "\
@prefix ex: <http://example.org/> .
ex:Person [ ottr:IRI ?p, xsd:string ?name ] :: {
  ottr:Triple(?p, rdf:type, ex:Person) ,
  ottr:Triple(?p, ex:hasName, ?name)
} .";

    #[test]
    fn template_is_typed_and_skips_ottr_triple() {
        let ts = build(PERSON);
        // Person is typed mtpl:Template
        let types: Vec<_> = ts
            .iter()
            .filter(|t| {
                subject_str(&t.subject) == "http://example.org/Person"
                    && t.predicate.as_str() == rdf::TYPE.as_str()
            })
            .map(|t| term_str(&t.object))
            .collect();
        assert_eq!(types, vec![pred("Template")]);
        // ottr:Triple builtin is never emitted as a subject
        assert!(!ts.iter().any(|t| subject_str(&t.subject) == OTTR_TRIPLE));
    }

    #[test]
    fn parameters_have_index_type_and_flags() {
        let ts = build(PERSON);
        let params = child_bnodes(&ts, "http://example.org/Person", "hasParameter");
        assert_eq!(params.len(), 2);

        // Find the parameter named "name" and check its type is xsd:string, single, non-lub.
        let name_param = params
            .iter()
            .find(|b| objs(&ts, b, "variableName") == vec!["name".to_string()])
            .expect("name parameter");
        assert_eq!(
            objs(&ts, name_param, "type"),
            vec!["http://www.w3.org/2001/XMLSchema#string".to_string()]
        );
        assert_eq!(
            objs(&ts, name_param, "cardinality"),
            vec!["single".to_string()]
        );
        assert_eq!(objs(&ts, name_param, "optional"), vec!["false".to_string()]);

        let p_param = params
            .iter()
            .find(|b| objs(&ts, b, "variableName") == vec!["p".to_string()])
            .expect("p parameter");
        assert_eq!(
            objs(&ts, p_param, "type"),
            vec!["http://ns.ottr.xyz/0.4/IRI".to_string()]
        );
    }

    #[test]
    fn instances_call_ottr_triple() {
        let ts = build(PERSON);
        let instances = child_bnodes(&ts, "http://example.org/Person", "hasInstance");
        assert_eq!(instances.len(), 2);
        for i in &instances {
            assert_eq!(objs(&ts, i, "callsTemplate"), vec![OTTR_TRIPLE.to_string()]);
        }
    }

    #[test]
    fn uses_predicate_denormalized() {
        let ts = build(PERSON);
        let mut preds = objs(&ts, "http://example.org/Person", "usesPredicate");
        preds.sort();
        assert_eq!(
            preds,
            vec![
                "http://example.org/hasName".to_string(),
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_string(),
            ]
        );
    }

    #[test]
    fn references_iri_excludes_self() {
        let ts = build(PERSON);
        let refs = objs(&ts, "http://example.org/Person", "referencesIri");
        assert!(refs.contains(&"http://example.org/hasName".to_string()));
        // self is excluded
        assert!(!refs.contains(&"http://example.org/Person".to_string()));
    }

    #[test]
    fn list_expander_default_and_cardinality() {
        let s = "\
@prefix ex: <http://example.org/> .
ex:Tagged [ ? ! ottr:IRI ?c = ex:Default , NEList<xsd:string> ?xs ] :: {
  cross | ottr:Triple(?c, ex:tag, ++ ?xs)
} .";
        let ts = build(s);
        let params = child_bnodes(&ts, "http://example.org/Tagged", "hasParameter");

        let c = params
            .iter()
            .find(|b| objs(&ts, b, "variableName") == vec!["c".to_string()])
            .unwrap();
        assert_eq!(objs(&ts, c, "optional"), vec!["true".to_string()]);
        assert_eq!(objs(&ts, c, "nonBlank"), vec!["true".to_string()]);
        assert_eq!(
            objs(&ts, c, "defaultValue"),
            vec!["http://example.org/Default".to_string()]
        );

        let xs = params
            .iter()
            .find(|b| objs(&ts, b, "variableName") == vec!["xs".to_string()])
            .unwrap();
        assert_eq!(objs(&ts, xs, "cardinality"), vec!["nelist".to_string()]);

        // instance carries the list expander, and the ++ argument is flagged
        let instance = &child_bnodes(&ts, "http://example.org/Tagged", "hasInstance")[0];
        assert_eq!(
            objs(&ts, instance, "listExpander"),
            vec!["cross".to_string()]
        );
        let args = child_bnodes(&ts, instance, "hasArgument");
        let expanded: Vec<_> = args
            .iter()
            .filter(|a| objs(&ts, a, "listExpand") == vec!["true".to_string()])
            .collect();
        assert_eq!(expanded.len(), 1);
    }
}

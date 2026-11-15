//! Expansion of templates with function call terms, following Frog (Jarholt, "Frog: Functions
//! for ontologies -- An extension for the OTTR-framework", University of Oslo, 2022).

use maplib::model::{MapOptions, Model};
use oxrdfio::RdfFormat;
use polars::prelude::{DataFrame, NamedFrom, Series};
use representation::dataset::NamedGraph;
use std::collections::BTreeSet;
use triplestore::StorageOptions;

const PREFIXES: &str = r#"
@prefix ex:<http://example.net/ns#>.
@prefix fn:<http://ns.frog.ottr.xyz/0.1/function/>.
@prefix frog:<http://ns.frog.ottr.xyz/0.1#>.
"#;

fn model(doc: &str) -> Model {
    Model::from_str(&format!("{PREFIXES}{doc}"), StorageOptions::memory()).unwrap()
}

fn triples(m: &Model) -> BTreeSet<String> {
    let mut buf = vec![];
    m.write_triples(&mut buf, &NamedGraph::DefaultGraph, RdfFormat::NTriples, None)
        .unwrap();
    String::from_utf8(buf)
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.trim().trim_end_matches('.').trim().to_string())
        .collect()
}

fn expected(lines: &[&str]) -> BTreeSet<String> {
    lines.iter().map(|x| x.to_string()).collect()
}

fn df(columns: Vec<Series>) -> DataFrame {
    let height = columns.first().map(|c| c.len()).unwrap_or(0);
    DataFrame::new(height, columns.into_iter().map(|c| c.into()).collect()).unwrap()
}

#[test]
fn test_build_iri_and_string_functions() {
    let mut m = model(
        r#"
ex:Person [ xsd:string ?id, xsd:string ?name ] :: {
    ottr:Triple((IRI (CONCAT "http://example.net/person/" (ENCODE_FOR_URI ?id))), ex:name, (UCASE ?name)),
    ottr:Triple((IRI (CONCAT "http://example.net/person/" (ENCODE_FOR_URI ?id))), ex:nameLength, (STRLEN ?name))
} .
"#,
    );
    let d = df(vec![
        Series::new("id".into(), &["a 1", "b"]),
        Series::new("name".into(), &["Ann", "Bob"]),
    ]);
    m.expand(
        "http://example.net/ns#Person",
        Some(d),
        None,
        MapOptions::default(),
    )
    .unwrap();
    assert_eq!(
        triples(&m),
        expected(&[
            r#"<http://example.net/person/a%201> <http://example.net/ns#name> "ANN""#,
            r#"<http://example.net/person/b> <http://example.net/ns#name> "BOB""#,
            r#"<http://example.net/person/a%201> <http://example.net/ns#nameLength> "3"^^<http://www.w3.org/2001/XMLSchema#integer>"#,
            r#"<http://example.net/person/b> <http://example.net/ns#nameLength> "3"^^<http://www.w3.org/2001/XMLSchema#integer>"#,
        ])
    );
}

#[test]
fn test_frog_functions_and_arithmetic() {
    // Fahrenheit to Celsius, Figure 4.7 of the Frog thesis, with SPARQL operators and Frog's
    // base function names, and an xsd cast.
    let mut m = model(
        r#"
def ex:FtoC(xsd:decimal ?fahrenheit) -> xsd:decimal :: (
    fn:times<<xsd:decimal>> (fn:minus<<xsd:decimal>> ?fahrenheit 32) (/ 5 9)
) .
ex:Measurement [ ottr:IRI ?station, xsd:string ?f ] :: {
    ottr:Triple(?station, ex:celsius, (ROUND (ex:FtoC (xsd:decimal ?f)))),
    ottr:Triple(?station, ex:warm, (IF (> (xsd:decimal ?f) 70) true false))
} .
"#,
    );
    let d = df(vec![
        Series::new(
            "station".into(),
            &["http://example.net/s/1", "http://example.net/s/2"],
        ),
        Series::new("f".into(), &["212", "32"]),
    ]);
    m.expand(
        "http://example.net/ns#Measurement",
        Some(d),
        None,
        MapOptions::default(),
    )
    .unwrap();
    let t = triples(&m);
    // (212 - 32) * 5 / 9 = 100 and (32 - 32) * 5 / 9 = 0, as rounded decimals.
    let celsius: Vec<_> = t.iter().filter(|x| x.contains("#celsius>")).collect();
    assert_eq!(celsius.len(), 2, "{t:?}");
    assert!(
        celsius.iter().any(|x| x.starts_with("<http://example.net/s/1>")
            && x.contains("> \"100")
            && x.ends_with("XMLSchema#decimal>")),
        "{t:?}"
    );
    assert!(
        celsius
            .iter()
            .any(|x| x.starts_with("<http://example.net/s/2>") && x.contains("> \"0")),
        "{t:?}"
    );
    assert!(
        t.contains(r#"<http://example.net/s/1> <http://example.net/ns#warm> "true"^^<http://www.w3.org/2001/XMLSchema#boolean>"#),
        "{t:?}"
    );
    assert!(
        t.contains(r#"<http://example.net/s/2> <http://example.net/ns#warm> "false"^^<http://www.w3.org/2001/XMLSchema#boolean>"#),
        "{t:?}"
    );
    assert_eq!(t.len(), 4, "{t:?}");
}

#[test]
fn test_frog_planet_case_study_discards_instances_with_none() {
    // The planet case study of the Frog thesis (Templates 6.2.1 and 6.2.2, Figure 6.5).
    let mut m = model(
        r#"
def ex:toIRI(xsd:string ?namespace, xsd:string ?localName) -> ottr:IRI :: (
    fn:castToIRI (fn:concat ?namespace (REPLACE ?localName " " "_"))
) .
def ex:toIRIWNamespace(xsd:string ?localName) -> ottr:IRI :: (
    ex:toIRI "http://example.org/data/" ?localName
) .
ex:Planet [ ottr:IRI ?iri, xsd:string ?name, ottr:IRI ?star, ? xsd:string ?starName ] :: {
    ottr:Triple(?iri, rdf:type, ex:Planet),
    ottr:Triple(?iri, rdfs:label, ?name),
    ottr:Triple(?iri, ex:orbitsStar, ?star),
    ottr:Triple(?star, rdfs:label, ?starName)
} .
ex:PlanetFrog [ xsd:string ?name, ? xsd:string ?starName ] :: {
    ex:Planet((ex:toIRIWNamespace ?name), ?name, (ex:toIRIWNamespace ?starName), ?starName)
} .
"#,
    );
    let d = df(vec![
        Series::new("name".into(), &[Some("11 Com b"), Some("Rogue")]),
        Series::new("starName".into(), &[Some("11 Com"), None]),
    ]);
    m.expand(
        "http://example.net/ns#PlanetFrog",
        Some(d),
        None,
        MapOptions::default(),
    )
    .unwrap();
    // The star IRI of Rogue is none, and ?star is mandatory, so that instance is not expanded.
    assert_eq!(
        triples(&m),
        expected(&[
            r#"<http://example.org/data/11_Com_b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.net/ns#Planet>"#,
            r#"<http://example.org/data/11_Com_b> <http://www.w3.org/2000/01/rdf-schema#label> "11 Com b""#,
            r#"<http://example.org/data/11_Com_b> <http://example.net/ns#orbitsStar> <http://example.org/data/11_Com>"#,
            r#"<http://example.org/data/11_Com> <http://www.w3.org/2000/01/rdf-schema#label> "11 Com""#,
        ])
    );
}

#[test]
fn test_default_and_optional_function_results() {
    let mut m = model(
        r#"
ex:Inner [ ottr:IRI ?s, ? xsd:string ?opt, xsd:string ?def = "default" ] :: {
    ottr:Triple(?s, ex:opt, ?opt),
    ottr:Triple(?s, ex:def, ?def)
} .
ex:Outer [ ottr:IRI ?s, ? xsd:string ?v ] :: {
    ex:Inner(?s, (UCASE ?v), (LCASE ?v))
} .
"#,
    );
    let d = df(vec![
        Series::new(
            "s".into(),
            &["http://example.net/s/1", "http://example.net/s/2"],
        ),
        Series::new("v".into(), &[Some("Ab"), None]),
    ]);
    m.expand(
        "http://example.net/ns#Outer",
        Some(d),
        None,
        MapOptions::default(),
    )
    .unwrap();
    assert_eq!(
        triples(&m),
        expected(&[
            r#"<http://example.net/s/1> <http://example.net/ns#opt> "AB""#,
            r#"<http://example.net/s/1> <http://example.net/ns#def> "ab""#,
            r#"<http://example.net/s/2> <http://example.net/ns#def> "default""#,
        ])
    );
}

#[test]
fn test_constant_arguments_and_no_dataframe() {
    let mut m = model(
        r#"
ex:Inner [ ? xsd:string ?a, xsd:string ?b ] :: {
    ottr:Triple(ex:s, ex:p, (CONCAT ?a "-" ?b)),
    ottr:Triple(ex:s, ex:q, (COALESCE ?a "unbound"))
} .
ex:Outer [ ] :: {
    ex:Inner("x", "y"),
    ex:Inner(none, "z"),
    ottr:Triple(ex:s, ex:r, (frog:functionCall STRUUID))
} .
"#,
    );
    m.expand("http://example.net/ns#Outer", None, None, MapOptions::default())
        .unwrap();
    let t = triples(&m);
    assert!(
        t.contains(r#"<http://example.net/ns#s> <http://example.net/ns#p> "x-y""#),
        "{t:?}"
    );
    assert!(
        t.contains(r#"<http://example.net/ns#s> <http://example.net/ns#q> "x""#),
        "{t:?}"
    );
    assert!(
        t.contains(r#"<http://example.net/ns#s> <http://example.net/ns#q> "unbound""#),
        "{t:?}"
    );
    // CONCAT with an unbound argument is an error, i.e. none.
    assert!(!t.iter().any(|x| x.contains("\"-z\"")), "{t:?}");
    assert_eq!(
        t.iter()
            .filter(|x| x.contains("<http://example.net/ns#r>"))
            .count(),
        1,
        "{t:?}"
    );
}

#[test]
fn test_function_calls_with_list_expansion() {
    let mut m = model(
        r#"
ex:Inner [ ottr:IRI ?s, xsd:string ?tag ] :: {
    ottr:Triple(?s, ex:tag, ?tag)
} .
ex:Outer [ xsd:string ?id, List<xsd:string> ?tags ] :: {
    cross | ex:Inner((IRI (CONCAT "http://example.net/" ?id)), ++?tags)
} .
"#,
    );
    let tags = Series::new(
        "tags".into(),
        &[
            Series::new("".into(), &["a", "b"]),
            Series::new("".into(), &["c"]),
        ],
    );
    let d = df(vec![Series::new("id".into(), &["x", "y"]), tags]);
    m.expand(
        "http://example.net/ns#Outer",
        Some(d),
        None,
        MapOptions::default(),
    )
    .unwrap();
    assert_eq!(
        triples(&m),
        expected(&[
            r#"<http://example.net/x> <http://example.net/ns#tag> "a""#,
            r#"<http://example.net/x> <http://example.net/ns#tag> "b""#,
            r#"<http://example.net/y> <http://example.net/ns#tag> "c""#,
        ])
    );
}

#[test]
fn test_list_expanding_a_function_call_is_an_error() {
    let mut m = model(
        r#"
ex:Inner [ ottr:IRI ?s ] :: { ottr:Triple(?s, ex:p, ex:o) } .
ex:Outer [ xsd:string ?id ] :: {
    cross | ex:Inner(++(IRI ?id))
} .
"#,
    );
    let d = df(vec![Series::new("id".into(), &["http://example.net/x"])]);
    let r = m.expand(
        "http://example.net/ns#Outer",
        Some(d),
        None,
        MapOptions::default(),
    );
    assert!(r.is_err());
}

#[test]
fn test_existing_templates_are_unchanged() {
    let mut m = model(
        r#"
ex:T [ ottr:IRI ?s, List<xsd:string> ?l ] :: {
    ottr:Triple(?s, ex:p, ?l),
    ottr:Triple(?s, ex:q, ("a", "b"))
} .
"#,
    );
    let l = Series::new("l".into(), &[Series::new("".into(), &["x"])]);
    let d = df(vec![Series::new("s".into(), &["http://example.net/s"]), l]);
    m.expand("http://example.net/ns#T", Some(d), None, MapOptions::default())
        .unwrap();
    // Two lists of one and two elements: 2 + 2 * 2 triples plus the two list heads.
    assert_eq!(triples(&m).len(), 8);
}

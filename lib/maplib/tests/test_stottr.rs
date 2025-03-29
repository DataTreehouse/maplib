extern crate core;

use maplib::mapping::{ExpandOptions, Mapping};
use oxrdf::{Literal, NamedNode, Subject, Term, Triple};
use polars::prelude::{col, AnyValue, DataFrame, IntoLazy, PlSmallStr, Series, TimeUnit};
use representation::polars_to_rdf::df_as_result;
use representation::solution_mapping::EagerSolutionMappings;
use rstest::*;
use serial_test::serial;
use std::collections::HashSet;
use std::path::PathBuf;
use triplestore::sparql::QueryResult;

// TODO: Legacy functionality, these tests should move to Python.
fn get_triples(mapping: &mut Mapping) -> Vec<Triple> {
    let res = mapping
        .query(
            "SELECT ?subject ?verb ?object WHERE {?subject ?verb ?object}",
            &None,
            None,
            false,
            true,
        )
        .unwrap();
    let mut triples = vec![];
    if let QueryResult::Select(EagerSolutionMappings {
        mappings,
        rdf_node_types,
    }) = res
    {
        let solns = df_as_result(mappings, &rdf_node_types);
        for s in solns.solutions {
            let (subject, verb, object) = triplestore::query_solutions::get_three_query_solutions(
                s,
                &solns.variables,
                "subject",
                "verb",
                "object",
            );
            let subject = match subject.unwrap() {
                Term::NamedNode(nn) => Subject::NamedNode(nn),
                Term::BlankNode(bn) => Subject::BlankNode(bn),
                _ => panic!(),
            };
            let verb = match verb.unwrap() {
                Term::NamedNode(nn) => nn,
                _ => panic!(),
            };
            triples.push(Triple::new(subject, verb, object.unwrap()))
        }
    }
    triples
}

#[fixture]
fn testdata_path() -> PathBuf {
    let manidir = env!("CARGO_MANIFEST_DIR");
    let mut testdata_path = PathBuf::new();
    testdata_path.push(manidir);
    testdata_path.push("tests");
    testdata_path.push("stottr_testdata");
    testdata_path
}

#[rstest]
#[serial]
fn test_all_iri_case() {
    let t_str = r#"
    @prefix ex:<http://example.net/ns#>.

    ex:ExampleTemplate [ottr:IRI ?myVar1]
      :: {
        ottr:Triple(ex:anObject, ex:relatesTo, ?myVar1)
      } .
    "#;

    let mut v1 = Series::from_iter([
        "http://example.net/ns#OneThing",
        "http://example.net/ns#AnotherThing",
    ]);
    v1.rename("myVar1".into());
    let series = [v1];
    let df = DataFrame::from_iter(series);

    let mut mapping = Mapping::from_str(t_str, None).unwrap();
    let _report = mapping
        .expand(
            "http://example.net/ns#ExampleTemplate",
            Some(df),
            None,
            Default::default(),
        )
        .expect("");
    let triples = get_triples(&mut mapping);
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples);
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#relatesTo"),
            object: Term::NamedNode(NamedNode::new_unchecked("http://example.net/ns#OneThing")),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#relatesTo"),
            object: Term::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#AnotherThing",
            )),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}

#[rstest]
#[serial]
fn test_string_language_tag_cases() {
    let t_str = r#"
    @prefix ex:<http://example.net/ns#>.

    ex:ExampleTemplate [?myString]
      :: {
        ottr:Triple(ex:anObject, ex:hasString, ?myString) ,
        ottr:Triple(ex:anotherObject, ex:hasString, ""@ar-SA)
      } .
    "#;

    let mut my_string = Series::from_iter(["one", "two"]);
    my_string.rename("myString".into());
    let series = [my_string];
    let df = DataFrame::from_iter(series);

    let mut mapping = Mapping::from_str(t_str, None).unwrap();
    let _report = mapping
        .expand(
            "http://example.net/ns#ExampleTemplate",
            Some(df),
            None,
            ExpandOptions {
                ..Default::default()
            },
        )
        .expect("");
    let triples = get_triples(&mut mapping);
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples);
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasString"),
            object: Term::Literal(Literal::new_simple_literal("one")),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasString"),
            object: Term::Literal(Literal::new_simple_literal("two")),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#anotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasString"),
            object: Term::Literal(Literal::new_language_tagged_literal_unchecked("", "ar-SA")),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}

#[rstest]
#[serial]
fn test_const_list_case() {
    let t_str = r#"
    @prefix ex:<http://example.net/ns#>.

    ex:ExampleTemplate [ottr:IRI ?var1]
      :: {
        cross | ottr:Triple(?var1, ex:hasNumber, ++(1,2))
      } .
    "#;

    let mut v1 = Series::from_iter([
        "http://example.net/ns#OneThing",
        "http://example.net/ns#AnotherThing",
    ]);
    v1.rename("var1".into());
    let series = [v1];
    let df = DataFrame::from_iter(series);

    let mut mapping = Mapping::from_str(t_str, None).unwrap();
    let _report = mapping
        .expand(
            "http://example.net/ns#ExampleTemplate",
            Some(df),
            None,
            Default::default(),
        )
        .expect("");
    let triples = get_triples(&mut mapping);
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples);
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#OneThing")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "1",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#OneThing")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "2",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#AnotherThing",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "1",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#AnotherThing",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "2",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            )),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}

#[rstest]
#[serial]
fn test_nested_templates() {
    let stottr = r#"
@prefix ex:<http://example.net/ns#>.
ex:ExampleTemplate [?myVar1 , ?myVar2] :: {
    ex:Nested(?myVar1),  
    ottr:Triple(ex:anObject, ex:hasOtherNumber, ?myVar2)
  } .
ex:Nested [?myVar] :: {
    ottr:Triple(ex:anObject, ex:hasNumber, ?myVar)
} .
"#;
    let mut mapping = Mapping::from_str(stottr, None).unwrap();
    let mut v1 = Series::from_iter(&[1, 2i32]);
    v1.rename("myVar1".into());
    let mut v2 = Series::from_iter(&[3, 4i32]);
    v2.rename("myVar2".into());
    let series = [v1, v2];
    let df = DataFrame::from_iter(series);
    let _report = mapping
        .expand(
            "http://example.net/ns#ExampleTemplate",
            Some(df),
            None,
            Default::default(),
        )
        .unwrap();
    let triples = get_triples(&mut mapping);
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples);
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "1",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "2",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasOtherNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "3",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasOtherNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "4",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}

// ?Date
// ?Time
// ?Duration_sec
// ?List_String

#[rstest]
#[serial]
fn test_derived_datatypes() {
    let stottr = r#"
@prefix ex:<http://example.net/ns#>.
ex:ExampleTemplate [
?Boolean,
?UInt32,
?UInt64,
?Int32,
?Int64,
?Float32,
?Float64,
?String,
?Datetime_ms_tz,
?Datetime_ms
] :: {
    ottr:Triple(ex:anObject, ex:hasVal, ?Boolean),
    ottr:Triple(ex:anObject, ex:hasVal, ?UInt32),
    ottr:Triple(ex:anObject, ex:hasVal, ?UInt64),
    ottr:Triple(ex:anObject, ex:hasVal, ?Int32),
    ottr:Triple(ex:anObject, ex:hasVal, ?Int64),
    ottr:Triple(ex:anotherObject, ex:hasValVal, ?Float32),
    ottr:Triple(ex:anotherObject, ex:hasValVal, ?Float64),
    ottr:Triple(ex:yetAnotherObject, ex:hasString, ?String),
    ottr:Triple(ex:yetAnotherObject, ex:hasDateTime, ?Datetime_ms_tz),
    ottr:Triple(ex:yetAnotherObject, ex:hasDateTime, ?Datetime_ms)
  } .
"#;
    let mut mapping = Mapping::from_str(stottr, None).unwrap();
    let mut boolean = Series::from_iter(&[true, false]);
    boolean.rename("Boolean".into());
    let mut uint32 = Series::from_iter(&[5u32, 6u32]);
    uint32.rename("UInt32".into());
    let mut uint64 = Series::from_iter(&[7u64, 8u64]);
    uint64.rename("UInt64".into());
    let mut int32 = Series::from_iter(&[-13i32, -14i32]);
    int32.rename("Int32".into());
    let mut int64 = Series::from_iter(&[-15i64, -16i64]);
    int64.rename("Int64".into());
    let mut float32 = Series::from_iter(&[17.18f32, 19.20f32]);
    float32.rename("Float32".into());
    let mut float64 = Series::from_iter(&[21.22f64, 23.24f64]);
    float64.rename("Float64".into());
    let mut string = Series::from_iter(["abcde", "fghij"]);
    string.rename("String".into());
    let datetime_ms_tz = Series::from_any_values(
        "Datetime_ms_tz".into(),
        &[
            AnyValue::Datetime(
                1656842780123,
                TimeUnit::Milliseconds,
                Some(&PlSmallStr::from_str("Europe/Oslo")),
            ),
            AnyValue::Datetime(
                1656842781456,
                TimeUnit::Milliseconds,
                Some(&PlSmallStr::from_str("Europe/Oslo")),
            ),
        ],
        false,
    )
    .unwrap();
    let datetime_ms = Series::from_any_values(
        PlSmallStr::from_str("Datetime_ms"),
        &[
            AnyValue::Datetime(1656842790789, TimeUnit::Milliseconds, None),
            AnyValue::Datetime(1656842791101, TimeUnit::Milliseconds, None),
        ],
        false,
    )
    .unwrap();

    let series = [
        boolean,
        uint32,
        uint64,
        int32,
        int64,
        float32,
        float64,
        string,
        datetime_ms_tz,
        datetime_ms,
    ];
    let df = DataFrame::from_iter(series);
    let _report = mapping
        .expand(
            "http://example.net/ns#ExampleTemplate",
            Some(df),
            None,
            Default::default(),
        )
        .unwrap();
    let mut actual_triples = get_triples(&mut mapping);
    let mut expected_triples = vec![
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "true",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "false",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "5",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#unsignedInt"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "6",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#unsignedInt"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "7",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#unsignedLong"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "8",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#unsignedLong"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "-13",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "-14",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "-15",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#long"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#anObject")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "-16",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#long"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#anotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasValVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "17.18",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#float"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#anotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasValVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "19.2",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#float"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#anotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasValVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "21.22",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#double"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#anotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasValVal"),
            object: Term::Literal(Literal::new_typed_literal(
                "23.24",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#double"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#yetAnotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasString"),
            object: Term::Literal(Literal::new_typed_literal(
                "abcde",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#yetAnotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasString"),
            object: Term::Literal(Literal::new_typed_literal(
                "fghij",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#yetAnotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasDateTime"),
            object: Term::Literal(Literal::new_typed_literal(
                "2022-07-03T10:06:20.123+02:00",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#dateTimeStamp"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#yetAnotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasDateTime"),
            object: Term::Literal(Literal::new_typed_literal(
                "2022-07-03T10:06:21.456+02:00",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#dateTimeStamp"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#yetAnotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasDateTime"),
            object: Term::Literal(Literal::new_typed_literal(
                "2022-07-03T10:06:30.789",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#dateTime"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked(
                "http://example.net/ns#yetAnotherObject",
            )),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasDateTime"),
            object: Term::Literal(Literal::new_typed_literal(
                "2022-07-03T10:06:31.101",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#dateTime"),
            )),
        },
    ];
    expected_triples.sort_by_key(|x| x.to_string());
    actual_triples.sort_by_key(|x| x.to_string());

    assert_eq!(expected_triples, actual_triples);
}

#[rstest]
#[serial]
fn test_list_arguments() {
    let stottr = r#"
@prefix ex:<http://example.net/ns#>.
ex:AnotherExampleTemplate [?object, ?predicate, ?myList] :: {
    cross | ottr:Triple(?object, ?predicate, ++?myList)
  } .
"#;
    let mut mapping = Mapping::from_str(stottr, None).unwrap();
    let mut object = Series::from_iter([
        "http://example.net/ns#obj1",
        "http://example.net/ns#obj1",
        "http://example.net/ns#obj2",
        "http://example.net/ns#obj2",
    ]);
    object.rename("object".into());
    let mut predicate = Series::from_iter([
        "http://example.net/ns#hasNumberFromList1",
        "http://example.net/ns#hasNumberFromList1",
        "http://example.net/ns#hasNumberFromList2",
        "http://example.net/ns#hasNumberFromList2",
    ]);
    predicate.rename("predicate".into());
    let mut my_list = Series::from_iter([1i32, 2, 3, 4]);
    my_list.rename("myList".into());
    let series = [object, predicate, my_list];
    let mut df = DataFrame::from_iter(series);
    df = df
        .lazy()
        .group_by_stable([col("object"), col("predicate")])
        .agg([col("myList").list().0])
        .collect()
        .unwrap();
    //println!("{df}");
    let _report = mapping
        .expand(
            "http://example.net/ns#AnotherExampleTemplate",
            Some(df),
            None,
            Default::default(),
        )
        .unwrap();
    let triples = get_triples(&mut mapping);
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples);
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumberFromList1"),
            object: Term::Literal(Literal::new_typed_literal(
                "1",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumberFromList1"),
            object: Term::Literal(Literal::new_typed_literal(
                "2",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj2")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumberFromList2"),
            object: Term::Literal(Literal::new_typed_literal(
                "3",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj2")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumberFromList2"),
            object: Term::Literal(Literal::new_typed_literal(
                "4",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}

#[rstest]
#[serial]
fn test_two_list_arguments() {
    let stottr = r#"
@prefix ex:<http://example.net/ns#>.
ex:AnotherExampleTemplate [?subject, ?myList1, ?myList2] :: {
    cross | ex:Nested(?subject, ++?myList1, ++?myList2)
  } .
  ex:Nested [?subject, ?myVar1, ?myVar2] :: {
    ottr:Triple(?subject, ex:hasNumber, ?myVar1),
    ottr:Triple(?subject, ex:hasOtherNumber, ?myVar2)
} .
"#;
    let mut mapping = Mapping::from_str(stottr, None).unwrap();
    let mut subject = Series::from_iter([
        "http://example.net/ns#obj1",
        "http://example.net/ns#obj1",
        "http://example.net/ns#obj2",
        "http://example.net/ns#obj2",
        "http://example.net/ns#obj2",
    ]);
    subject.rename("subject".into());
    let mut my_list1 = Series::from_iter([Some(1i32), Some(2), Some(3), Some(4), None]);
    my_list1.rename("myList1".into());
    let mut my_list2 = Series::from_iter([5i32, 6, 7, 8, 9]);
    my_list2.rename("myList2".into());
    let series = [subject, my_list1, my_list2];
    let mut df = DataFrame::from_iter(series);
    df = df
        .lazy()
        .group_by_stable([col("subject")])
        .agg([col("myList1").list().0, col("myList2").list().0])
        .collect()
        .unwrap();

    //println!("{df}");
    let _report = mapping
        .expand(
            "http://example.net/ns#AnotherExampleTemplate",
            Some(df),
            None,
            Default::default(),
        )
        .unwrap();
    let triples = get_triples(&mut mapping);
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples);
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "1",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "2",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasOtherNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "5",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasOtherNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "6",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj2")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "3",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj2")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "4",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj2")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasOtherNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "7",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj2")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasOtherNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "8",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj2")),
            predicate: NamedNode::new_unchecked("http://example.net/ns#hasOtherNumber"),
            object: Term::Literal(Literal::new_typed_literal(
                "9",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}

#[rstest]
#[serial]
fn test_default() {
    let mut mapping = Mapping::from_str("", None).unwrap();
    let mut subject = Series::from_iter([
        "http://example.net/ns#obj1",
        "http://example.net/ns#obj1",
        "http://example.net/ns#obj2",
    ]);
    subject.rename("subject".into());
    let mut my_var1 = Series::from_iter([Some(1i32), Some(2), None]);
    my_var1.rename("myVar1".into());
    let mut my_var2 = Series::from_iter([5i32, 6, 7]);
    my_var2.rename("myVar2".into());
    let series = [subject, my_var1, my_var2];
    let df = DataFrame::from_iter(series);

    //println!("{df}");
    let _report = mapping
        .expand_default(
            df,
            "subject".to_string(),
            vec![],
            false,
            None,
            Default::default(),
        )
        .unwrap();
    let triples = get_triples(&mut mapping);
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples);
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("urn:maplib_default:myVar1"),
            object: Term::Literal(Literal::new_typed_literal(
                "1",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("urn:maplib_default:myVar1"),
            object: Term::Literal(Literal::new_typed_literal(
                "2",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("urn:maplib_default:myVar2"),
            object: Term::Literal(Literal::new_typed_literal(
                "5",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("urn:maplib_default:myVar2"),
            object: Term::Literal(Literal::new_typed_literal(
                "6",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj2")),
            predicate: NamedNode::new_unchecked("urn:maplib_default:myVar2"),
            object: Term::Literal(Literal::new_typed_literal(
                "7",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}

#[rstest]
#[serial]
fn test_default_list() {
    let mut mapping = Mapping::from_str("", None).unwrap();
    let mut subject = Series::from_iter([
        "http://example.net/ns#obj1",
        "http://example.net/ns#obj1",
        "http://example.net/ns#obj2",
    ]);
    subject.rename("subject".into());
    let mut my_list1 = Series::from_iter([Some(1i32), Some(2), None]);
    my_list1.rename("myList1".into());
    let mut my_list2 = Series::from_iter([5i32, 6, 7]);
    my_list2.rename("myList2".into());
    let series = [subject, my_list1, my_list2];
    let mut df = DataFrame::from_iter(series);
    df = df
        .lazy()
        .group_by_stable([col("subject")])
        .agg([
            col("myList1").drop_nulls().list().0,
            col("myList2").list().0,
        ])
        .collect()
        .unwrap();
    //println!("{df}");
    let _report = mapping
        .expand_default(
            df,
            "subject".to_string(),
            vec![],
            false,
            None,
            Default::default(),
        )
        .unwrap();
    let triples = get_triples(&mut mapping);
    //println!("{:?}", triples);
    let actual_triples_set: HashSet<Triple> = HashSet::from_iter(triples);
    let expected_triples_set = HashSet::from([
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("urn:maplib_default:myList1"),
            object: Term::Literal(Literal::new_typed_literal(
                "1",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("urn:maplib_default:myList1"),
            object: Term::Literal(Literal::new_typed_literal(
                "2",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("urn:maplib_default:myList2"),
            object: Term::Literal(Literal::new_typed_literal(
                "5",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj1")),
            predicate: NamedNode::new_unchecked("urn:maplib_default:myList2"),
            object: Term::Literal(Literal::new_typed_literal(
                "6",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
        Triple {
            subject: Subject::NamedNode(NamedNode::new_unchecked("http://example.net/ns#obj2")),
            predicate: NamedNode::new_unchecked("urn:maplib_default:myList2"),
            object: Term::Literal(Literal::new_typed_literal(
                "7",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#int"),
            )),
        },
    ]);
    assert_eq!(expected_triples_set, actual_triples_set);
}

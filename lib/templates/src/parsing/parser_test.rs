#[cfg(test)]
use crate::ast::{Directive, ListExpanderType, Prefix};
#[cfg(test)]
use crate::parsing::parsing_ast::{
    PrefixedName, ResolvesToNamedNode, UnresolvedAnnotation, UnresolvedArgument,
    UnresolvedConstantLiteral, UnresolvedConstantTerm, UnresolvedDefaultValue, UnresolvedInstance,
    UnresolvedPType, UnresolvedParameter, UnresolvedSignature, UnresolvedStatement,
    UnresolvedStottrDocument, UnresolvedStottrLiteral, UnresolvedStottrTerm, UnresolvedTemplate,
};
#[cfg(test)]
use crate::parsing::whole_stottr_doc;
#[cfg(test)]
use oxrdf::vocab::xsd;
#[cfg(test)]
use oxrdf::Variable;
#[cfg(test)]
use oxrdf::{BlankNode, NamedNode};

#[test]
fn test_easy_template() {
    //This test case is taken from:
    // https://github.com/Callidon/pyOTTR/blob/master/tests/stottr_test.py
    let stottr = r#"
        @prefix ex: <http://example.org#>.
        ex:Person[ ?firstName, ?lastName, ?email ] :: {
            o-rdf:Type (_:person, foaf:Person )
        } .
    "#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![Directive::Prefix(Prefix {
            name: "ex".to_string(),
            iri: NamedNode::new_unchecked("http://example.org#"),
        })],
        statements: vec![UnresolvedStatement::Template(UnresolvedTemplate {
            signature: UnresolvedSignature {
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "Person".to_string(),
                }),
                parameter_list: vec![
                    UnresolvedParameter {
                        optional: false,
                        non_blank: false,
                        ptype: None,
                        variable: Variable::new_unchecked("firstName"),
                        default_value: None,
                    },
                    UnresolvedParameter {
                        optional: false,
                        non_blank: false,
                        ptype: None,
                        variable: Variable::new_unchecked("lastName"),
                        default_value: None,
                    },
                    UnresolvedParameter {
                        optional: false,
                        non_blank: false,
                        ptype: None,
                        variable: Variable::new_unchecked("email"),
                        default_value: None,
                    },
                ],
                annotation_list: None,
            },
            pattern_list: vec![UnresolvedInstance {
                list_expander: None,
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "o-rdf".to_string(),
                    name: "Type".to_string(),
                }),
                argument_list: vec![
                    UnresolvedArgument {
                        list_expand: false,
                        term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(
                            UnresolvedConstantLiteral::BlankNode(BlankNode::new_unchecked(
                                "person",
                            )),
                        )),
                    },
                    UnresolvedArgument {
                        list_expand: false,
                        term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(
                            UnresolvedConstantLiteral::Iri(ResolvesToNamedNode::PrefixedName(
                                PrefixedName {
                                    prefix: "foaf".to_string(),
                                    name: "Person".to_string(),
                                },
                            )),
                        )),
                    },
                ],
            }],
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_easy_template_extra_comma() {
    //This test case is taken from:
    // https://github.com/Callidon/pyOTTR/blob/master/tests/stottr_test.py
    let stottr = r#"
        @prefix ex: <http://example.org#>.
        ex:Person[ ?firstName, ?lastName, ?email, ] :: {
            o-rdf:Type (_:person, foaf:Person )
        } .
    "#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![Directive::Prefix(Prefix {
            name: "ex".to_string(),
            iri: NamedNode::new_unchecked("http://example.org#"),
        })],
        statements: vec![UnresolvedStatement::Template(UnresolvedTemplate {
            signature: UnresolvedSignature {
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "Person".to_string(),
                }),
                parameter_list: vec![
                    UnresolvedParameter {
                        optional: false,
                        non_blank: false,
                        ptype: None,
                        variable: Variable::new_unchecked("firstName"),
                        default_value: None,
                    },
                    UnresolvedParameter {
                        optional: false,
                        non_blank: false,
                        ptype: None,
                        variable: Variable::new_unchecked("lastName"),
                        default_value: None,
                    },
                    UnresolvedParameter {
                        optional: false,
                        non_blank: false,
                        ptype: None,
                        variable: Variable::new_unchecked("email"),
                        default_value: None,
                    },
                ],
                annotation_list: None,
            },
            pattern_list: vec![UnresolvedInstance {
                list_expander: None,
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "o-rdf".to_string(),
                    name: "Type".to_string(),
                }),
                argument_list: vec![
                    UnresolvedArgument {
                        list_expand: false,
                        term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(
                            UnresolvedConstantLiteral::BlankNode(BlankNode::new_unchecked(
                                "person",
                            )),
                        )),
                    },
                    UnresolvedArgument {
                        list_expand: false,
                        term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(
                            UnresolvedConstantLiteral::Iri(ResolvesToNamedNode::PrefixedName(
                                PrefixedName {
                                    prefix: "foaf".to_string(),
                                    name: "Person".to_string(),
                                },
                            )),
                        )),
                    },
                ],
            }],
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_modifiers_1() {
    let stottr = r#"
        @prefix ex:<http://example.net/ns#>.

    ex:NamedPizza [ ??pizza  ] .
    "#;

    let doc = whole_stottr_doc(stottr).expect("Ok");
    let expected = UnresolvedStottrDocument {
        directives: vec![Directive::Prefix(Prefix {
            name: "ex".to_string(),
            iri: NamedNode::new_unchecked("http://example.net/ns#"),
        })],
        statements: vec![UnresolvedStatement::Signature(UnresolvedSignature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![UnresolvedParameter {
                optional: true,
                non_blank: false,
                ptype: None,
                variable: Variable::new_unchecked("pizza"),
                default_value: None,
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_modifiers_2() {
    let stottr = r#"
        @prefix ex:<http://example.net/ns#>.

    ex:NamedPizza [ !?pizza  ] .
    "#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![Directive::Prefix(Prefix {
            name: "ex".to_string(),
            iri: NamedNode::new_unchecked("http://example.net/ns#"),
        })],
        statements: vec![UnresolvedStatement::Signature(UnresolvedSignature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![UnresolvedParameter {
                optional: false,
                non_blank: true,
                ptype: None,
                variable: Variable::new_unchecked("pizza"),
                default_value: None,
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_modifiers_3() {
    let stottr = r#"
        @prefix ex:<http://example.net/ns#>.

    ex:NamedPizza [ ?!?pizza  ] .
    "#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![Directive::Prefix(Prefix {
            name: "ex".to_string(),
            iri: NamedNode::new_unchecked("http://example.net/ns#"),
        })],
        statements: vec![UnresolvedStatement::Signature(UnresolvedSignature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![UnresolvedParameter {
                optional: true,
                non_blank: true,
                ptype: None,
                variable: Variable::new_unchecked("pizza"),
                default_value: None,
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_modifiers_4() {
    let stottr = r#"
        @prefix ex:<http://example.net/ns#>.

    ex:NamedPizza [ !??pizza  ] .
    "#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![Directive::Prefix(Prefix {
            name: "ex".to_string(),
            iri: NamedNode::new_unchecked("http://example.net/ns#"),
        })],
        statements: vec![UnresolvedStatement::Signature(UnresolvedSignature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![UnresolvedParameter {
                optional: true,
                non_blank: true,
                ptype: None,
                variable: Variable::new_unchecked("pizza"),
                default_value: None,
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_type_1() {
    let stottr = r#"
        @prefix ex:<http://example.net/ns#>.

    ex:NamedPizza [ owl:Class ?pizza ].
    "#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![Directive::Prefix(Prefix {
            name: "ex".to_string(),
            iri: NamedNode::new_unchecked("http://example.net/ns#"),
        })],
        statements: vec![UnresolvedStatement::Signature(UnresolvedSignature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![UnresolvedParameter {
                optional: false,
                non_blank: false,
                ptype: Some(UnresolvedPType::Basic(ResolvesToNamedNode::PrefixedName(
                    PrefixedName {
                        prefix: "owl".to_string(),
                        name: "Class".to_string(),
                    },
                ))),
                variable: Variable::new_unchecked("pizza"),
                default_value: None,
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_type_2() {
    let stottr = r#"
        @prefix ex:<http://example.net/ns#>.

    ex:NamedPizza [ ? owl:Class ?pizza ].
    "#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![Directive::Prefix(Prefix {
            name: "ex".to_string(),
            iri: NamedNode::new_unchecked("http://example.net/ns#"),
        })],
        statements: vec![UnresolvedStatement::Signature(UnresolvedSignature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![UnresolvedParameter {
                optional: true,
                non_blank: false,
                ptype: Some(UnresolvedPType::Basic(ResolvesToNamedNode::PrefixedName(
                    PrefixedName {
                        prefix: "owl".to_string(),
                        name: "Class".to_string(),
                    },
                ))),
                variable: Variable::new_unchecked("pizza"),
                default_value: None,
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_type_3() {
    let stottr = r#"
        @prefix ex:<http://example.net/ns#>.

    ex:NamedPizza [ ?! owl:Class ?pizza ].
    "#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![Directive::Prefix(Prefix {
            name: "ex".to_string(),
            iri: NamedNode::new_unchecked("http://example.net/ns#"),
        })],
        statements: vec![UnresolvedStatement::Signature(UnresolvedSignature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![UnresolvedParameter {
                optional: true,
                non_blank: true,
                ptype: Some(UnresolvedPType::Basic(ResolvesToNamedNode::PrefixedName(
                    PrefixedName {
                        prefix: "owl".to_string(),
                        name: "Class".to_string(),
                    },
                ))),
                variable: Variable::new_unchecked("pizza"),
                default_value: None,
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_default_value_1() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:NamedPizza[ owl:Class ?pizza = p:pizza] ."#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
        statements: vec![UnresolvedStatement::Signature(UnresolvedSignature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![UnresolvedParameter {
                optional: false,
                non_blank: false,
                ptype: Some(UnresolvedPType::Basic(ResolvesToNamedNode::PrefixedName(
                    PrefixedName {
                        prefix: "owl".to_string(),
                        name: "Class".to_string(),
                    },
                ))),
                variable: Variable::new_unchecked("pizza"),
                default_value: Some(UnresolvedDefaultValue {
                    constant_term: UnresolvedConstantTerm::Constant(
                        UnresolvedConstantLiteral::Iri(ResolvesToNamedNode::PrefixedName(
                            PrefixedName {
                                prefix: "p".to_string(),
                                name: "pizza".to_string(),
                            },
                        )),
                    ),
                }),
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_default_value_2() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:NamedPizza[ owl:Class ?pizza = 2] ."#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
        statements: vec![UnresolvedStatement::Signature(UnresolvedSignature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![UnresolvedParameter {
                optional: false,
                non_blank: false,
                ptype: Some(UnresolvedPType::Basic(ResolvesToNamedNode::PrefixedName(
                    PrefixedName {
                        prefix: "owl".to_string(),
                        name: "Class".to_string(),
                    },
                ))),
                variable: Variable::new_unchecked("pizza"),
                default_value: Some(UnresolvedDefaultValue {
                    constant_term: UnresolvedConstantTerm::Constant(
                        UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                            value: "2".to_string(),
                            language: None,
                            data_type_iri: Some(ResolvesToNamedNode::NamedNode(
                                xsd::INTEGER.into_owned(),
                            )),
                        }),
                    ),
                }),
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_default_value_3() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:NamedPizza[ owl:Class ?pizza = "asdf"] ."#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
        statements: vec![UnresolvedStatement::Signature(UnresolvedSignature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![UnresolvedParameter {
                optional: false,
                non_blank: false,
                ptype: Some(UnresolvedPType::Basic(ResolvesToNamedNode::PrefixedName(
                    PrefixedName {
                        prefix: "owl".to_string(),
                        name: "Class".to_string(),
                    },
                ))),
                variable: Variable::new_unchecked("pizza"),
                default_value: Some(UnresolvedDefaultValue {
                    constant_term: UnresolvedConstantTerm::Constant(
                        UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                            value: "asdf".to_string(),
                            language: None,
                            data_type_iri: Some(ResolvesToNamedNode::NamedNode(
                                xsd::STRING.into_owned(),
                            )),
                        }),
                    ),
                }),
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_more_parameters() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:NamedPizza [  ?pizza ,  ?country  ,  ?toppings ] ."#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
        statements: vec![UnresolvedStatement::Signature(UnresolvedSignature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![
                UnresolvedParameter {
                    optional: false,
                    non_blank: false,
                    ptype: None,
                    variable: Variable::new_unchecked("pizza"),
                    default_value: None,
                },
                UnresolvedParameter {
                    optional: false,
                    non_blank: false,
                    ptype: None,
                    variable: Variable::new_unchecked("country"),
                    default_value: None,
                },
                UnresolvedParameter {
                    optional: false,
                    non_blank: false,
                    ptype: None,
                    variable: Variable::new_unchecked("toppings"),
                    default_value: None,
                },
            ],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_lists() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:NamedPizza [  ?pizza = "asdf" ,  ?country = ("asdf", "asdf") ,  ?toppings = ((())) ] ."#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
        statements: vec![UnresolvedStatement::Signature(UnresolvedSignature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![
                UnresolvedParameter {
                    optional: false,
                    non_blank: false,
                    ptype: None,
                    variable: Variable::new_unchecked("pizza"),
                    default_value: Some(UnresolvedDefaultValue {
                        constant_term: UnresolvedConstantTerm::Constant(
                            UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                                value: "asdf".to_string(),
                                language: None,
                                data_type_iri: Some(ResolvesToNamedNode::NamedNode(
                                    NamedNode::new_unchecked(
                                        "http://www.w3.org/2001/XMLSchema#string",
                                    ),
                                )),
                            }),
                        ),
                    }),
                },
                UnresolvedParameter {
                    optional: false,
                    non_blank: false,
                    ptype: None,
                    variable: Variable::new_unchecked("country"),
                    default_value: Some(UnresolvedDefaultValue {
                        constant_term: UnresolvedConstantTerm::ConstantList(vec![
                            UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(
                                UnresolvedStottrLiteral {
                                    value: "asdf".to_string(),
                                    language: None,
                                    data_type_iri: Some(ResolvesToNamedNode::NamedNode(
                                        NamedNode::new_unchecked(
                                            "http://www.w3.org/2001/XMLSchema#string",
                                        ),
                                    )),
                                },
                            )),
                            UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(
                                UnresolvedStottrLiteral {
                                    value: "asdf".to_string(),
                                    language: None,
                                    data_type_iri: Some(ResolvesToNamedNode::NamedNode(
                                        NamedNode::new_unchecked(
                                            "http://www.w3.org/2001/XMLSchema#string",
                                        ),
                                    )),
                                },
                            )),
                        ]),
                    }),
                },
                UnresolvedParameter {
                    optional: false,
                    non_blank: false,
                    ptype: None,
                    variable: Variable::new_unchecked("toppings"),
                    default_value: Some(UnresolvedDefaultValue {
                        constant_term: UnresolvedConstantTerm::ConstantList(vec![
                            UnresolvedConstantTerm::ConstantList(vec![
                                UnresolvedConstantTerm::ConstantList(vec![]),
                            ]),
                        ]),
                    }),
                },
            ],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_more_complex_types() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:NamedPizza [
      ! owl:Class ?pizza  ,
      ?! owl:NamedIndividual ?country  = ex:Class ,
      NEList<List<List<owl:Class>>> ?toppings
    ] ."#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
        statements: vec![UnresolvedStatement::Signature(UnresolvedSignature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![
                UnresolvedParameter {
                    optional: false,
                    non_blank: true,
                    ptype: Some(UnresolvedPType::Basic(ResolvesToNamedNode::PrefixedName(
                        PrefixedName {
                            prefix: "owl".to_string(),
                            name: "Class".to_string(),
                        },
                    ))),
                    variable: Variable::new_unchecked("pizza"),
                    default_value: None,
                },
                UnresolvedParameter {
                    optional: true,
                    non_blank: true,
                    ptype: Some(UnresolvedPType::Basic(ResolvesToNamedNode::PrefixedName(
                        PrefixedName {
                            prefix: "owl".to_string(),
                            name: "NamedIndividual".to_string(),
                        },
                    ))),
                    variable: Variable::new_unchecked("country"),
                    default_value: Some(UnresolvedDefaultValue {
                        constant_term: UnresolvedConstantTerm::Constant(
                            UnresolvedConstantLiteral::Iri(ResolvesToNamedNode::PrefixedName(
                                PrefixedName {
                                    prefix: "ex".to_string(),
                                    name: "Class".to_string(),
                                },
                            )),
                        ),
                    }),
                },
                UnresolvedParameter {
                    optional: false,
                    non_blank: false,
                    ptype: Some(UnresolvedPType::NEList(Box::new(UnresolvedPType::List(
                        Box::new(UnresolvedPType::List(Box::new(UnresolvedPType::Basic(
                            ResolvesToNamedNode::PrefixedName(PrefixedName {
                                prefix: "owl".to_string(),
                                name: "Class".to_string(),
                            }),
                        )))),
                    )))),
                    variable: Variable::new_unchecked("toppings"),
                    default_value: None,
                },
            ],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_example_1() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:template [ ] :: { ex:template((ex:template)) } ."#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
        statements: vec![UnresolvedStatement::Template(UnresolvedTemplate {
            signature: UnresolvedSignature {
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "template".to_string(),
                }),
                parameter_list: vec![],
                annotation_list: None,
            },
            pattern_list: vec![UnresolvedInstance {
                list_expander: None,
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "template".to_string(),
                }),
                argument_list: vec![UnresolvedArgument {
                    list_expand: false,
                    term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::ConstantList(
                        vec![UnresolvedConstantTerm::Constant(
                            UnresolvedConstantLiteral::Iri(ResolvesToNamedNode::PrefixedName(
                                PrefixedName {
                                    prefix: "ex".to_string(),
                                    name: "template".to_string(),
                                },
                            )),
                        )],
                    )),
                }],
            }],
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_example_2() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:template [?!?var ] :: { ex:template((((ex:template)))) } ."#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
        statements: vec![UnresolvedStatement::Template(UnresolvedTemplate {
            signature: UnresolvedSignature {
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "template".to_string(),
                }),
                parameter_list: vec![UnresolvedParameter {
                    optional: true,
                    non_blank: true,
                    ptype: None,
                    variable: Variable::new_unchecked("var"),
                    default_value: None,
                }],
                annotation_list: None,
            },
            pattern_list: vec![UnresolvedInstance {
                list_expander: None,
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "template".to_string(),
                }),
                argument_list: vec![UnresolvedArgument {
                    list_expand: false,
                    term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::ConstantList(
                        vec![UnresolvedConstantTerm::ConstantList(vec![
                            UnresolvedConstantTerm::ConstantList(vec![
                                UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Iri(
                                    ResolvesToNamedNode::PrefixedName(PrefixedName {
                                        prefix: "ex".to_string(),
                                        name: "template".to_string(),
                                    }),
                                )),
                            ]),
                        ])],
                    )),
                }],
            }],
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_example_3() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:template [ ] :: { ex:template(( ex:template )) } ."#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
        statements: vec![UnresolvedStatement::Template(UnresolvedTemplate {
            signature: UnresolvedSignature {
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "template".to_string(),
                }),
                parameter_list: vec![],
                annotation_list: None,
            },
            pattern_list: vec![UnresolvedInstance {
                list_expander: None,
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "template".to_string(),
                }),
                argument_list: vec![UnresolvedArgument {
                    list_expand: false,
                    term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::ConstantList(
                        vec![UnresolvedConstantTerm::Constant(
                            UnresolvedConstantLiteral::Iri(ResolvesToNamedNode::PrefixedName(
                                PrefixedName {
                                    prefix: "ex".to_string(),
                                    name: "template".to_string(),
                                },
                            )),
                        )],
                    )),
                }],
            }],
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_example_4() {
    let stottr = r#"
    @prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:NamedPizza [
      ! owl:Class ?pizza = p:Grandiosa , ?! LUB<owl:NamedIndividual> ?country  , List<owl:Class> ?toppings
      ]
      @@ cross | ex:SomeAnnotationTemplate("asdf", "asdf", "asdf" ),
      @@<http://asdf>("asdf", "asdf", ++("A", "B", "C") )
      :: {
         cross | ex:Template1 (?pizza, ++?toppings) ,
         ex:Template2 (1, 2,4,   5) ,
         <http://Template2.com> ("asdf"^^xsd:string) ,
         zipMax | ex:Template4 ("asdf"^^xsd:string, ?pizza, ++( "a", "B" )),
         zipMax | ex:Template4 ([], [], [], ++([], []))
      } ."#;

    let doc = whole_stottr_doc(stottr).expect("Ok");

    let expected = UnresolvedStottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),

            }),
        ],
        statements: vec![UnresolvedStatement::Template(UnresolvedTemplate {
            signature: UnresolvedSignature {
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "NamedPizza".to_string(),
                }),
                parameter_list: vec![
                    UnresolvedParameter {
                        optional: false,
                        non_blank: true,
                        ptype: Some(UnresolvedPType::Basic(ResolvesToNamedNode::PrefixedName(PrefixedName {
                            prefix: "owl".to_string(),
                            name: "Class".to_string(),
                        }))),
                        variable: Variable::new_unchecked("pizza"),
                        default_value: Some(UnresolvedDefaultValue {
                            constant_term: UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Iri(ResolvesToNamedNode::PrefixedName(PrefixedName {
                                prefix: "p".to_string(),
                                name: "Grandiosa".to_string(),
                            }))),
                        }),
                    },
                    UnresolvedParameter {
                        optional: true,
                        non_blank: true,
                        ptype: Some(UnresolvedPType::Lub(Box::new(UnresolvedPType::Basic(ResolvesToNamedNode::PrefixedName(PrefixedName {
                            prefix: "owl".to_string(),
                            name: "NamedIndividual".to_string(),
                        }))))),
                        variable: Variable::new_unchecked("country"),
                        default_value: None,
                    },
                    UnresolvedParameter {
                        optional: false,
                        non_blank: false,
                        ptype: Some(UnresolvedPType::List(Box::new(UnresolvedPType::Basic(ResolvesToNamedNode::PrefixedName(PrefixedName {
                            prefix: "owl".to_string(),
                            name: "Class".to_string(),
                        }))))),
                        variable: Variable::new_unchecked("toppings"),
                        default_value: None,
                    },
                ],
                annotation_list: Some(vec![
                    UnresolvedAnnotation {
                        instance: UnresolvedInstance {
                            list_expander: Some(ListExpanderType::Cross),
                            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                                prefix: "ex".to_string(),
                                name: "SomeAnnotationTemplate".to_string(),
                            }),
                            argument_list: vec![
                                UnresolvedArgument {
                                    list_expand: false,
                                    term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                                        value: "asdf".to_string(),
                                        language: None,
                                        data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"))),

                                    }))),
                                },
                                UnresolvedArgument {
                                    list_expand: false,
                                    term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                                        value: "asdf".to_string(),
                                        language: None,
                                        data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"))),

                                    }))),
                                },
                                UnresolvedArgument {
                                    list_expand: false,
                                    term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                                        value: "asdf".to_string(),
                                        language: None,
                                        data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"))),
                                    }))),
                                },
                            ],
                        },
                    },
                    UnresolvedAnnotation {
                        instance: UnresolvedInstance {
                            list_expander: None,
                            template_name: ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://asdf" )),
                            argument_list: vec![
                                UnresolvedArgument {
                                    list_expand: false,
                                    term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                                        value: "asdf".to_string(),
                                        language: None,
                                        data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"))),
                                    }))),
                                },
                                UnresolvedArgument {
                                    list_expand: false,
                                    term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                                        value: "asdf".to_string(),
                                        language: None,
                                        data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"))),
                                    }))),
                                },
                                UnresolvedArgument {
                                    list_expand: true,
                                    term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::ConstantList(vec![
                                        UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                                            value: "A".to_string(),
                                            language: None,
                                            data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"))),

                                        })),
                                        UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                                            value: "B".to_string(),
                                            language: None,
                                            data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"))),
                                        })),
                                        UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                                            value: "C".to_string(),
                                            language: None,
                                            data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"))),
                                        })),
                                    ])),
                                },
                            ],
                        },
                    },
                ]),
            },
            pattern_list: vec![
                UnresolvedInstance {
                    list_expander: Some(ListExpanderType::Cross),
                    template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                        prefix: "ex".to_string(),
                        name: "Template1".to_string(),
                    }),
                    argument_list: vec![
                        UnresolvedArgument {
                            list_expand: false,
                            term: UnresolvedStottrTerm::Variable(                        Variable::new_unchecked("pizza"), ),
                        },
                        UnresolvedArgument {
                            list_expand: true,
                            term: UnresolvedStottrTerm::Variable(                  Variable::new_unchecked("toppings"),
                            ),
                        },
                    ],
                },
                UnresolvedInstance {
                    list_expander: None,
                    template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                        prefix: "ex".to_string(),
                        name: "Template2".to_string(),
                    }),
                    argument_list: vec![
                        UnresolvedArgument {
                            list_expand: false,
                            term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                                value: "1".to_string(),
                                language: None,
                                data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"))),
                            }))),
                        },
                        UnresolvedArgument {
                            list_expand: false,
                            term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                                value: "2".to_string(),
                                language: None,
                                data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"))),
                            }))),
                        },
                        UnresolvedArgument {
                            list_expand: false,
                            term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                                value: "4".to_string(),
                                language: None,
                                data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"))),
                            }))),
                        },
                        UnresolvedArgument {
                            list_expand: false,
                            term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                                value: "5".to_string(),
                                language: None,
                                data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"))),
                            }))),
                        },
                    ],
                },
                UnresolvedInstance {
                    list_expander: None,
                    template_name: ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://Template2.com")),
                    argument_list: vec![UnresolvedArgument {
                        list_expand: false,
                        term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                            value: "asdf".to_string(),
                            language: None,
                            data_type_iri: Some(ResolvesToNamedNode::PrefixedName(PrefixedName {
                                prefix: "xsd".to_string(),
                                name: "string".to_string(),
                            })),
                        }))),
                    }],
                },
                UnresolvedInstance {
                    list_expander: Some(ListExpanderType::ZipMax),
                    template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                        prefix: "ex".to_string(),
                        name: "Template4".to_string(),
                    }),
                    argument_list: vec![
                        UnresolvedArgument {
                            list_expand: false,
                            term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                                value: "asdf".to_string(),
                                language: None,
                                data_type_iri: Some(ResolvesToNamedNode::PrefixedName(PrefixedName {
                                    prefix: "xsd".to_string(),
                                    name: "string".to_string(),
                                })),
                            }))),
                        },
                        UnresolvedArgument {
                            list_expand: false,
                            term: UnresolvedStottrTerm::Variable(Variable::new_unchecked("pizza")),
                        },
                        UnresolvedArgument {
                            list_expand: true,
                            term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::ConstantList(vec![
                                UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                                    value: "a".to_string(),
                                    language: None,
                                    data_type_iri: Some(ResolvesToNamedNode::NamedNode(xsd::STRING.into_owned())),
                                })),
                                UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::Literal(UnresolvedStottrLiteral {
                                    value: "B".to_string(),
                                    language: None,
                                    data_type_iri: Some(ResolvesToNamedNode::NamedNode(xsd::STRING.into_owned())),
                                })),
                            ])),
                        },
                    ],
                },
                UnresolvedInstance {
                    list_expander: Some(ListExpanderType::ZipMax),
                    template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                        prefix: "ex".to_string(),
                        name: "Template4".to_string(),
                    }),
                    argument_list: vec![
                        UnresolvedArgument {
                            list_expand: false,
                            term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::BlankNode(BlankNode::new_unchecked(
                                "AnonymousBlankNode",
                            )))),
                        },
                        UnresolvedArgument {
                            list_expand: false,
                            term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::BlankNode(BlankNode::new_unchecked(
                                "AnonymousBlankNode",
                            )))),
                        },
                        UnresolvedArgument {
                            list_expand: false,
                            term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::BlankNode(BlankNode::new_unchecked(
                                "AnonymousBlankNode",
                            )))),
                        },
                        UnresolvedArgument {
                            list_expand: true,
                            term: UnresolvedStottrTerm::ConstantTerm(UnresolvedConstantTerm::ConstantList(vec![
                                UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::BlankNode(BlankNode::new_unchecked("AnonymousBlankNode"))),
                                UnresolvedConstantTerm::Constant(UnresolvedConstantLiteral::BlankNode(BlankNode::new_unchecked("AnonymousBlankNode"))),
                            ])),
                        },
                    ],
                },
            ],
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_trailing_commas_and_rdftype_shorthand() {
    let s = r#"
@prefix rr:<http://www.w3.org/ns/r2rml#>.
@prefix foaf:<http://xmlns.com/foaf/0.1/>.
@prefix xsd:<http://www.w3.org/2001/XMLSchema#>.
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#>.
@prefix dc:<http://purl.org/dc/elements/1.1/>.
@prefix rev:<http://purl.org/stuff/rev#>.
@prefix gtfs:<http://vocab.gtfs.org/terms#>.
@prefix geo:<http://www.w3.org/2003/01/geo/wgs84_pos#>.
@prefix schema:<http://schema.org/>.
@prefix dct:<http://purl.org/dc/terms/>.
@prefix rml:<http://semweb.mmlab.be/ns/rml#>.
@prefix ql:<http://semweb.mmlab.be/ns/ql#>.
@prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
@prefix t:<https://github.com/magbak/maplib/benchmark/template#>.

t:Stops [ ottr:IRI ?stop_id, ?stop_code, ?stop_name, ?stop_desc, ?stop_lat, ?stop_lon, ??zone_id,
          ottr:IRI ?stop_url, ottr:IRI ?location_type, ? ottr:IRI ?parent_station, ??stop_timezone, ?wheelchair_boarding
          , ] :: {
  ottr:Triple(?stop_id, a, gtfs:Stop) ,
  ottr:Triple(?stop_id, gtfs:code, ?stop_code) ,
  ottr:Triple(?stop_id, dct:identifier, ?stop_id) ,
  ottr:Triple(?stop_id, foaf:name, ?stop_name) ,
  ottr:Triple(?stop_id, dct:description, ?stop_desc) ,
  ottr:Triple(?stop_id, geo:lat, ?stop_lat) ,
  ottr:Triple(?stop_id, geo:long, ?stop_lon) ,
  ottr:Triple(?stop_id, gtfs:zone, ?zone_id) ,
  ottr:Triple(?stop_id, foaf:page, ?stop_url) ,
  ottr:Triple(?stop_id, gtfs:locationType, ?location_type) ,
  ottr:Triple(?stop_id, gtfs:parentStation, ?parent_station) ,
  ottr:Triple(?stop_id, gtfs:timeZone, ?stop_timezone) ,
  ottr:Triple(?stop_id, gtfs:wheelchairAccessible, ?wheelchair_boarding)

  ,
} . "#;
    let _doc = whole_stottr_doc(s).expect("Ok");
    //No assert, enough that we are parsing it
}

#[test]
fn test_handles_space_before_prefix_dot() {
    let s = r#"
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

    ex:Person[ ?firstName, ?lastName, ?email ] :: {
      ottr:Triple(_:person, rdf:type, foaf:Person ),
      ottr:Triple(_:person, foaf:firstName, ?firstName ),
      ottr:Triple(_:person, foaf:lastName, ?lastName ),
      ottr:Triple(_:person, foaf:mbox, ?email )
    } .
    "#;

    let _doc = whole_stottr_doc(s).expect("Ok");
    // No assert, we are just testing that this parses
}

#[test]
fn test_optional_commas() {
    let s = r#"
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

    ex:Person[ ?firstName, ?lastName, ?email ] :: {
      ottr:Triple(_:person, rdf:type, foaf:Person )
      ottr:Triple(_:person, foaf:firstName, ?firstName ),
      ottr:Triple(_:person, foaf:lastName, ?lastName )
      ottr:Triple(_:person, foaf:mbox, ?email )
    } .
    "#;

    let _doc = whole_stottr_doc(s).expect("Ok");
    // No assert, we are just testing that this parses
}

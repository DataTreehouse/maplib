use super::Triplestore;

use crate::conversion::convert_to_string;
use crate::errors::TriplestoreError;
use oxrdf::{Literal, NamedNode, Subject, Term, Triple};
use polars_core::prelude::AnyValue;
use representation::{literal_iri_to_namednode, RDFNodeType, TripleType};

impl Triplestore {
    pub fn object_property_triples<F, T>(
        &mut self,
        func: F,
        out: &mut Vec<T>,
    ) -> Result<(), TriplestoreError>
    where
        F: Fn(&str, &str, &str) -> T,
    {
        for (verb, map) in &mut self.df_map {
            for ((_k1, k2), v) in map {
                if k2.find_triple_type() == TripleType::ObjectProperty {
                    for i in 0..v.len() {
                        let df = v.get_df(i)?;
                        if df.height() == 0 {
                            return Ok(());
                        }
                        let mut subject_iterator = df.column("subject").unwrap().iter();
                        let mut object_iterator = df.column("object").unwrap().iter();
                        for _ in 0..df.height() {
                            let s = anyutf8_to_str(subject_iterator.next().unwrap());
                            let o = anyutf8_to_str(object_iterator.next().unwrap());
                            out.push(func(s, verb.as_str(), o));
                        }
                    }
                }
                v.forget_tmp_df();
            }
        }
        Ok(())
    }

    pub fn string_data_property_triples<F, T>(
        &mut self,
        func: F,
        out: &mut Vec<T>,
    ) -> Result<(), TriplestoreError>
    where
        F: Fn(&str, &str, &str, Option<&str>) -> T,
    {
        //subject, verb, lexical_form, language_tag, datatype
        for (verb, map) in &mut self.df_map {
            for ((_k1, k2), v) in map {
                if k2.find_triple_type() == TripleType::StringProperty {
                    for i in 0..v.len() {
                        let df = v.get_df(i)?;
                        if df.height() == 0 {
                            return Ok(());
                        }
                        let mut subject_iterator = df.column("subject").unwrap().iter();
                        let mut data_iterator = df.column("object").unwrap().iter();
                        let mut language_tag_iterator = df.column("language_tag").unwrap().iter();
                        for _ in 0..df.height() {
                            let s = anyutf8_to_str(subject_iterator.next().unwrap());
                            let lex = anyutf8_to_str(data_iterator.next().unwrap());
                            let lang_opt = if let AnyValue::Utf8(lang) =
                                language_tag_iterator.next().unwrap()
                            {
                                Some(lang)
                            } else {
                                None
                            };
                            out.push(func(s, verb.as_str(), lex, lang_opt));
                        }
                        v.forget_tmp_df();
                    }
                }
            }
        }
        Ok(())
    }

    pub fn nonstring_data_property_triples<F, T>(
        &mut self,
        func: F,
        out: &mut Vec<T>,
    ) -> Result<(), TriplestoreError>
    where
        F: Fn(&str, &str, &str, &NamedNode) -> T,
    {
        //subject, verb, lexical_form, datatype
        for (verb, map) in &mut self.df_map {
            for ((_k1, k2), v) in map {
                if k2.find_triple_type() == TripleType::NonStringProperty {
                    let object_type = if let RDFNodeType::Literal(l) = k2 {
                        l
                    } else {
                        panic!("Should never happen")
                    };
                    for i in 0..v.len() {
                        let df = v.get_df(i)?;
                        if df.height() == 0 {
                            return Ok(());
                        }
                        let mut subject_iterator = df.column("subject").unwrap().iter();
                        let data_as_strings = convert_to_string(df.column("object").unwrap());
                        if let Some(s) = data_as_strings {
                            let mut data_iterator = s.iter();
                            for _ in 0..df.height() {
                                let s = anyutf8_to_str(subject_iterator.next().unwrap());
                                let lex = anyutf8_to_str(data_iterator.next().unwrap());
                                out.push(func(s, verb.as_str(), lex, object_type));
                            }
                        } else {
                            let mut data_iterator = df.column("object").unwrap().iter();
                            for _ in 0..df.height() {
                                let s = anyutf8_to_str(subject_iterator.next().unwrap());
                                let lex = anyutf8_to_str(data_iterator.next().unwrap());
                                out.push(func(s, verb.as_str(), lex, object_type));
                            }
                        };
                        v.forget_tmp_df();
                    }
                }
            }
        }
        Ok(())
    }

    pub fn export_oxrdf_triples(&mut self) -> Result<Vec<Triple>, TriplestoreError> {
        self.deduplicate()?;
        fn subject_from_str(s: &str) -> Subject {
            Subject::NamedNode(literal_iri_to_namednode(s))
        }
        fn object_term_from_str(s: &str) -> Term {
            Term::NamedNode(literal_iri_to_namednode(s))
        }

        fn object_triple_func(s: &str, v: &str, o: &str) -> Triple {
            let subject = subject_from_str(s);
            let verb = NamedNode::new_unchecked(v);
            let object = object_term_from_str(o);
            Triple::new(subject, verb, object)
        }

        fn string_data_triple_func(s: &str, v: &str, lex: &str, lang_opt: Option<&str>) -> Triple {
            let subject = subject_from_str(s);
            let verb = NamedNode::new_unchecked(v);
            let literal = if let Some(lang) = lang_opt {
                Literal::new_language_tagged_literal_unchecked(lex, lang)
            } else {
                Literal::new_simple_literal(lex)
            };
            Triple::new(subject, verb, Term::Literal(literal))
        }

        fn nonstring_data_triple_func(s: &str, v: &str, lex: &str, dt: &NamedNode) -> Triple {
            let subject = subject_from_str(s);
            let verb = NamedNode::new_unchecked(v);
            let literal = Literal::new_typed_literal(lex, dt.clone());
            Triple::new(subject, verb, Term::Literal(literal))
        }

        let mut triples = vec![];
        self.object_property_triples(object_triple_func, &mut triples)?;
        self.string_data_property_triples(string_data_triple_func, &mut triples)?;
        self.nonstring_data_property_triples(nonstring_data_triple_func, &mut triples)?;
        Ok(triples)
    }
}

fn anyutf8_to_str(a: AnyValue) -> &str {
    if let AnyValue::Utf8(s) = a {
        s
    } else {
        panic!("Should never happen {}", a)
    }
}

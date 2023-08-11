use oxrdf::{BlankNode, NamedNode, Subject, Term, Triple};
use rio_api::parser::TriplesParser;
use rio_turtle::TurtleError;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::str::FromStr;

pub fn triples_from_file(file: File) -> HashSet<Triple> {
    let mut reader = BufReader::new(file);
    let mut triples = vec![];

    rio_turtle::NTriplesParser::new(&mut reader)
        .parse_all(&mut |x| {
            let subject = match x.subject {
                rio_api::model::Subject::NamedNode(nn) => {
                    Subject::NamedNode(NamedNode::new_unchecked(nn.to_string()))
                }
                rio_api::model::Subject::BlankNode(bn) => {
                    Subject::BlankNode(BlankNode::new_unchecked(bn.to_string()))
                }
                rio_api::model::Subject::Triple(_) => {
                    unimplemented!("Not supported")
                }
            };
            let predicate = NamedNode::new_unchecked(x.predicate.to_string());
            let object = Term::from_str(&x.object.to_string()).unwrap();
            let t = Triple {
                subject,
                predicate,
                object,
            };
            triples.push(t);
            Ok(()) as Result<(), TurtleError>
        })
        .expect("No problems");
    triples.into_iter().collect()
}

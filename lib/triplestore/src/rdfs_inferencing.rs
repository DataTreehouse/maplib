use super::Triplestore;
use crate::errors::TriplestoreError;

const SUBCLASS_INFERENCING: &str = r#"
PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
CONSTRUCT { ?a a ?b }
WHERE {
    ?a a ?c .
    ?c rdfs:subClassOf+ ?b .
}
"#;

impl Triplestore {
    pub fn rdfs_class_inheritance(&mut self) -> Result<(), TriplestoreError> {
        self.insert(SUBCLASS_INFERENCING, &None, true)
            .map_err(|x| TriplestoreError::RDFSClassInheritanceError(x.to_string()))?;
        Ok(())
    }
}

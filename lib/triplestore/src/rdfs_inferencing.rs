use super::Triplestore;
use crate::errors::TriplestoreError;
use crate::sparql::QuerySettings;
use oxrdf::vocab::rdf;
use representation::dataset::NamedGraph;
use representation::result::QueryResultKind;

const RDFS_2: &str = r#"
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

CONSTRUCT {
    ?y rdf:type ?x .
}
WHERE {
    ?a rdfs:domain ?x .
    ?y ?a ?z .
}
"#;

const RDFS_3: &str = r#"
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

CONSTRUCT {
    ?z rdf:type ?x .
}
WHERE {
    ?a rdfs:range ?x .
    ?y ?a ?z .
}
"#;

const RDFS_5: &str = r#"
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

CONSTRUCT {
    ?x rdfs:subPropertyOf ?z .
}
WHERE {
    ?x rdfs:subPropertyOf ?y .
    ?y rdfs:subPropertyOf ?z .
}
"#;

const RDFS_6: &str = r#"
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

CONSTRUCT {
    ?x rdfs:subPropertyOf ?x .
}
WHERE {
    ?x rdf:type rdf:Property .
}
"#;

const RDFS_7: &str = r#"
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

CONSTRUCT {
    ?x ?b ?y .
}
WHERE {
    ?a rdfs:subPropertyOf ?b .
    ?x ?a ?y .
}
"#;

const RDFS_8: &str = r#"
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

CONSTRUCT {
    ?x rdfs:subClassOf rdfs:Resource .
}
WHERE {
    ?x rdf:type rdfs:Class .
}
"#;

const RDFS_9: &str = r#"
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

CONSTRUCT {
    ?z rdf:type ?y .
}
WHERE {
    ?z rdf:type ?x .
    ?x rdfs:subClassOf ?y .
}
"#;

const RDFS_10: &str = r#"
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

CONSTRUCT {
    ?x rdfs:subClassOf ?x .
}
WHERE {
    ?x rdf:type rdfs:Class .
}
"#;

const RDFS_11: &str = r#"
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

CONSTRUCT {
    ?x rdfs:subClassOf ?z .
}
WHERE {
    ?x rdfs:subClassOf ?y .
    ?y rdfs:subClassOf ?z .
}
"#;

impl Triplestore {
    pub fn rdfs_class_inheritance(&mut self, graph: &NamedGraph) -> Result<(), TriplestoreError> {
        let qs = QuerySettings {
            include_transient: false,
            max_rows: None,
            strict_project: false,
        };
        self.insert(RDFS_9, None, true, false, &qs, graph, graph, None, false)
            .map_err(|x| TriplestoreError::RDFSClassInheritanceError(x.to_string()))?;
        Ok(())
    }

    pub fn drop_rdfs_class_inheritance(
        &mut self,
        graph: &NamedGraph,
    ) -> Result<(), TriplestoreError> {
        if let Some(t) = self.graph_transient_triples_map.get_mut(graph) {
            t.remove(&rdf::TYPE.into_owned());
        }
        Ok(())
    }

    pub fn interesting_rdfs_rules(
        &mut self,
        graph: &NamedGraph,
    ) -> Result<usize, TriplestoreError> {
        let qs = QuerySettings {
            include_transient: true,
            max_rows: None,
            strict_project: false,
        };
        // Reflexivity of subproperty
        let c6 = self.insert_once(&[RDFS_6], graph, &qs)?;
        // Transitivity of subproperty
        let c5 = self.insert_until_fixed_point(&[RDFS_5], graph, &qs)?;

        // Inheriting subproperty
        let c7 = self.insert_once(&[RDFS_7], graph, &qs)?;

        // Typing from domain and range, universal class, reflexivity of subclass
        let c2_3_8 = self.insert_once(&[RDFS_2, RDFS_3, RDFS_8], graph, &qs)?;

        // Reflexivity and transitivity of subclass
        let c_10_11 = self.insert_until_fixed_point(&[RDFS_10, RDFS_11], graph, &qs)?;

        // Inheriting subclass
        let c_9 = self.insert_once(&[RDFS_9], graph, &qs)?;

        Ok(c6 + c5 + c7 + c2_3_8 + c_10_11 + c_9)
    }

    fn insert_until_fixed_point(
        &mut self,
        queries: &[&str],
        graph: &NamedGraph,
        query_settings: &QuerySettings,
    ) -> Result<usize, TriplestoreError> {
        let mut changed = true;
        let mut c = 0;
        while changed {
            let new_c = self.insert_once(queries, graph, query_settings)?;
            changed = new_c > 0;
            c += new_c;
        }
        Ok(c)
    }

    fn insert_once(
        &mut self,
        queries: &[&str],
        graph: &NamedGraph,
        query_settings: &QuerySettings,
    ) -> Result<usize, TriplestoreError> {
        let r: Result<Vec<_>, TriplestoreError> = queries
            .iter()
            .map(|x| {
                let r = self
                    .query(x, None, false, query_settings, Some(graph), None, false)
                    .map_err(|x| TriplestoreError::SparqlQueryError(x.to_string()))?;
                Ok(r)
            })
            .collect();
        let r = r?;
        let r_unpacked: Vec<_> = r
            .into_iter()
            .flat_map(|x| match x.kind {
                QueryResultKind::Select(_) => {
                    unreachable!()
                }
                QueryResultKind::Construct(r) => r,
            })
            .collect();
        let nt = self
            .insert_construct_result(r_unpacked, true, graph)
            .map_err(|x| TriplestoreError::SparqlQueryError(x.to_string()))?;
        let c = nt
            .into_iter()
            .map(|x| x.df.map(|x| x.height()).unwrap_or(0))
            .sum();
        Ok(c)
    }
}

//! Which (graph, predicate) pairs a query can read, and whether its result can be cached.

use super::ReadItem;
use oxrdf::NamedNode;
use representation::dataset::NamedGraph;
use spargebra::algebra::{
    AggregateExpression, AggregateFunction, Expression, Function, GraphPattern, OrderExpression,
    PropertyPathExpression, QueryDataset,
};
use spargebra::term::{GraphNamePattern, GroundTerm, NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use std::collections::HashSet;

/// Predicates of the full text search index (maplib vocab fts, ftsQuery, ...). They are
/// answered from the index of the graph, which covers all its predicates.
const FTS_PREDICATE_PREFIX: &str = "https://datatreehouse.github.io/maplib/vocab#fts";

/// What a query runs against, besides its text.
pub struct PlanContext<'a> {
    /// The graph queried when the query has no dataset, None is the default graph.
    pub graph: Option<&'a NamedGraph>,
    /// Graphs merged into the default graph.
    pub include_graphs: &'a [NamedGraph],
    /// IRIs of virtualized predicates and resources, when the model has virtualized databases.
    pub virtualized_iris: Option<&'a HashSet<NamedNode>>,
    /// IRIs of user defined functions, which may give a different answer each time.
    pub udfs: &'a [NamedNode],
}

#[derive(Debug, Clone, PartialEq)]
pub enum CachePlan {
    /// The result can be kept while nothing in `reads` is written. `virtualized` is set when
    /// the query reads virtualized data, which the cache can not see changing.
    Cache {
        reads: Vec<ReadItem>,
        virtualized: bool,
    },
    /// The result must not be kept, for the given reason.
    Bypass(&'static str),
}

/// Graphs a pattern reads from: a list, or any graph.
#[derive(Clone)]
enum Scope {
    Graphs(Vec<NamedGraph>),
    Any,
}

struct Planner<'a> {
    ctx: &'a PlanContext<'a>,
    named: Scope,
    reads: HashSet<ReadItem>,
    iris: HashSet<NamedNode>,
    bypass: Option<&'static str>,
}

/// The read set of a parsed query (with its bindings replaced), or why it can not be cached.
pub fn cache_plan(query: &Query, ctx: &PlanContext) -> CachePlan {
    let (dataset, pattern) = match query {
        Query::Select {
            dataset, pattern, ..
        }
        | Query::Construct {
            dataset, pattern, ..
        }
        | Query::Describe {
            dataset, pattern, ..
        }
        | Query::Ask {
            dataset, pattern, ..
        } => (dataset, pattern),
    };
    let (default, named) = scopes(dataset, ctx);
    let mut p = Planner {
        ctx,
        named,
        reads: HashSet::new(),
        iris: HashSet::new(),
        bypass: None,
    };
    p.reads.insert(ReadItem::Prefixes);
    p.pattern(pattern, &default);
    match query {
        Query::Construct {
            template,
            graph_template,
            ..
        } => {
            for t in template {
                p.collect_triple_iris(t);
            }
            for q in graph_template {
                p.term(&q.subject);
                p.term(&q.object);
                if let NamedNodePattern::NamedNode(nn) = &q.predicate {
                    p.iris.insert(nn.clone());
                }
                if let GraphNamePattern::NamedNode(nn) = &q.graph_name {
                    p.iris.insert(nn.clone());
                }
            }
        }
        // DESCRIBE fetches all triples of the described resources.
        Query::Describe { .. } => p.any_predicate(&default),
        _ => {}
    }
    if let Some(reason) = p.bypass {
        return CachePlan::Bypass(reason);
    }
    let virtualized = ctx
        .virtualized_iris
        .is_some_and(|v| p.iris.iter().any(|i| v.contains(i)));
    let mut reads: Vec<ReadItem> = if p.reads.contains(&ReadItem::All) {
        vec![ReadItem::All]
    } else {
        // A whole graph read covers the reads of its predicates.
        let graphs: HashSet<NamedGraph> = p
            .reads
            .iter()
            .filter_map(|r| match r {
                ReadItem::Graph(g) => Some(g.clone()),
                _ => None,
            })
            .collect();
        p.reads
            .into_iter()
            .filter(|r| !matches!(r, ReadItem::Cell(g, _) if graphs.contains(g)))
            .collect()
    };
    reads.sort_by_key(|r| format!("{r:?}"));
    CachePlan::Cache { reads, virtualized }
}

fn graph_of(nn: &NamedNode) -> NamedGraph {
    NamedGraph::from_maybe_named_node(Some(nn))
}

/// The scope of the default graph and of GRAPH ?g, as `Triplestore::query_graph` resolves them.
fn scopes(dataset: &Option<QueryDataset>, ctx: &PlanContext) -> (Scope, Scope) {
    match dataset {
        Some(d) => {
            let mut default: Vec<NamedGraph> = d.default.iter().map(graph_of).collect();
            if !default.is_empty() {
                default.extend(ctx.include_graphs.iter().cloned());
            }
            let named = match &d.named {
                Some(n) => Scope::Graphs(n.iter().map(graph_of).collect()),
                None => Scope::Any,
            };
            (Scope::Graphs(default), named)
        }
        None => {
            let mut default = vec![ctx.graph.cloned().unwrap_or(NamedGraph::DefaultGraph)];
            default.extend(ctx.include_graphs.iter().cloned());
            (Scope::Graphs(default), Scope::Any)
        }
    }
}

impl Planner<'_> {
    fn predicate(&mut self, p: &NamedNode, scope: &Scope) {
        self.iris.insert(p.clone());
        if p.as_str().starts_with(FTS_PREDICATE_PREFIX) {
            self.any_predicate(scope);
            return;
        }
        match scope {
            Scope::Graphs(gs) => {
                for g in gs {
                    self.reads.insert(ReadItem::Cell(g.clone(), p.clone()));
                }
            }
            Scope::Any => {
                self.reads.insert(ReadItem::Predicate(p.clone()));
            }
        }
    }

    fn any_predicate(&mut self, scope: &Scope) {
        match scope {
            Scope::Graphs(gs) => {
                for g in gs {
                    self.reads.insert(ReadItem::Graph(g.clone()));
                }
            }
            Scope::Any => {
                self.reads.insert(ReadItem::All);
            }
        }
    }

    fn term(&mut self, t: &TermPattern) {
        if let TermPattern::NamedNode(nn) = t {
            self.iris.insert(nn.clone());
        }
    }

    fn collect_triple_iris(&mut self, t: &TriplePattern) {
        self.term(&t.subject);
        self.term(&t.object);
        if let NamedNodePattern::NamedNode(nn) = &t.predicate {
            self.iris.insert(nn.clone());
        }
    }

    fn triple(&mut self, t: &TriplePattern, scope: &Scope) {
        self.term(&t.subject);
        self.term(&t.object);
        match &t.predicate {
            NamedNodePattern::NamedNode(p) => self.predicate(p, scope),
            NamedNodePattern::Variable(_) => self.any_predicate(scope),
        }
    }

    fn path(&mut self, path: &PropertyPathExpression, both_ends_variable: bool, scope: &Scope) {
        match path {
            PropertyPathExpression::NamedNode(p) => self.predicate(p, scope),
            PropertyPathExpression::Reverse(p) => self.path(p, both_ends_variable, scope),
            PropertyPathExpression::Sequence(a, b) | PropertyPathExpression::Alternative(a, b) => {
                self.path(a, both_ends_variable, scope);
                self.path(b, both_ends_variable, scope);
            }
            PropertyPathExpression::OneOrMore(p) => self.path(p, both_ends_variable, scope),
            PropertyPathExpression::ZeroOrMore(p) | PropertyPathExpression::ZeroOrOne(p) => {
                // The zero length match binds every term of the graph when both ends are
                // variables.
                if both_ends_variable {
                    self.any_predicate(scope);
                }
                self.path(p, both_ends_variable, scope);
            }
            PropertyPathExpression::NegatedPropertySet(ps) => {
                for p in ps {
                    self.iris.insert(p.clone());
                }
                self.any_predicate(scope);
            }
        }
    }

    fn pattern(&mut self, gp: &GraphPattern, scope: &Scope) {
        match gp {
            GraphPattern::Bgp { patterns } => {
                for t in patterns {
                    self.triple(t, scope);
                }
            }
            GraphPattern::Path {
                subject,
                path,
                object,
            } => {
                self.term(subject);
                self.term(object);
                let variable = |t: &TermPattern| {
                    matches!(t, TermPattern::Variable(_) | TermPattern::BlankNode(_))
                };
                self.path(path, variable(subject) && variable(object), scope);
            }
            GraphPattern::Join { left, right }
            | GraphPattern::Union { left, right }
            | GraphPattern::Minus { left, right }
            | GraphPattern::Lateral { left, right } => {
                self.pattern(left, scope);
                self.pattern(right, scope);
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                self.pattern(left, scope);
                self.pattern(right, scope);
                if let Some(e) = expression {
                    self.expression(e, scope);
                }
            }
            GraphPattern::Filter { expr, inner } => {
                self.expression(expr, scope);
                self.pattern(inner, scope);
            }
            GraphPattern::Graph { name, inner } => {
                let inner_scope = match name {
                    NamedNodePattern::NamedNode(nn) => Scope::Graphs(vec![graph_of(nn)]),
                    NamedNodePattern::Variable(_) => self.named.clone(),
                };
                // A GRAPH ?g without triple patterns lists the graphs.
                if matches!(inner.as_ref(), GraphPattern::Bgp { patterns } if patterns.is_empty())
                {
                    self.any_predicate(&inner_scope);
                }
                self.pattern(inner, &inner_scope);
            }
            GraphPattern::Extend {
                inner, expression, ..
            } => {
                self.pattern(inner, scope);
                self.expression(expression, scope);
            }
            GraphPattern::Values { bindings, .. } => {
                for row in bindings {
                    for t in row.iter().flatten() {
                        if let GroundTerm::NamedNode(nn) = t {
                            self.iris.insert(nn.clone());
                        }
                    }
                }
            }
            GraphPattern::PValues { .. } => self.bypass = Some("parameters"),
            GraphPattern::OrderBy { inner, expression } => {
                self.pattern(inner, scope);
                for o in expression {
                    match o {
                        OrderExpression::Asc(e) | OrderExpression::Desc(e) => {
                            self.expression(e, scope)
                        }
                    }
                }
            }
            GraphPattern::Project { inner, .. }
            | GraphPattern::Distinct { inner }
            | GraphPattern::Reduced { inner }
            | GraphPattern::Slice { inner, .. } => self.pattern(inner, scope),
            GraphPattern::Group {
                inner, aggregates, ..
            } => {
                self.pattern(inner, scope);
                for (_, a) in aggregates {
                    if let AggregateExpression::FunctionCall { name, expr, .. } = a {
                        if let AggregateFunction::Custom(nn) = name {
                            self.custom_function(nn);
                        }
                        self.expression(expr, scope);
                    }
                }
            }
            GraphPattern::Service { .. } => self.bypass = Some("service"),
        }
    }

    fn custom_function(&mut self, nn: &NamedNode) {
        if self.ctx.udfs.contains(nn) {
            self.bypass = Some("udf");
        }
    }

    fn expression(&mut self, e: &Expression, scope: &Scope) {
        match e {
            Expression::NamedNode(nn) => {
                self.iris.insert(nn.clone());
            }
            Expression::Literal(_) | Expression::Variable(_) | Expression::Bound(_) => {}
            Expression::Or(a, b)
            | Expression::And(a, b)
            | Expression::Equal(a, b)
            | Expression::SameTerm(a, b)
            | Expression::Greater(a, b)
            | Expression::GreaterOrEqual(a, b)
            | Expression::Less(a, b)
            | Expression::LessOrEqual(a, b)
            | Expression::Add(a, b)
            | Expression::Subtract(a, b)
            | Expression::Multiply(a, b)
            | Expression::Divide(a, b) => {
                self.expression(a, scope);
                self.expression(b, scope);
            }
            Expression::In(a, bs) => {
                self.expression(a, scope);
                for b in bs {
                    self.expression(b, scope);
                }
            }
            Expression::UnaryPlus(a) | Expression::UnaryMinus(a) | Expression::Not(a) => {
                self.expression(a, scope)
            }
            Expression::Exists(p) => self.pattern(p, scope),
            Expression::If(a, b, c) => {
                self.expression(a, scope);
                self.expression(b, scope);
                self.expression(c, scope);
            }
            Expression::Coalesce(es) => {
                for e in es {
                    self.expression(e, scope);
                }
            }
            Expression::FunctionCall(f, args) => {
                match f {
                    Function::Now => self.bypass = Some("now"),
                    Function::Rand => self.bypass = Some("rand"),
                    Function::Uuid | Function::StrUuid => self.bypass = Some("uuid"),
                    Function::BNode => self.bypass = Some("bnode"),
                    Function::Custom(nn) => self.custom_function(nn),
                    _ => {}
                }
                for a in args {
                    self.expression(a, scope);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EX: &str = "http://example.org/";

    fn nn(s: &str) -> NamedNode {
        NamedNode::new_unchecked(format!("{EX}{s}"))
    }

    fn g(s: &str) -> NamedGraph {
        NamedGraph::NamedGraph(nn(s))
    }

    fn plan_with(q: &str, include: &[NamedGraph], udfs: &[NamedNode]) -> CachePlan {
        let q = Query::parse(&format!("PREFIX : <{EX}> {q}"), None, None).unwrap();
        cache_plan(
            &q,
            &PlanContext {
                graph: None,
                include_graphs: include,
                virtualized_iris: None,
                udfs,
            },
        )
    }

    fn reads(q: &str) -> Vec<ReadItem> {
        match plan_with(q, &[], &[]) {
            CachePlan::Cache { reads, .. } => reads
                .into_iter()
                .filter(|r| *r != ReadItem::Prefixes)
                .collect(),
            CachePlan::Bypass(r) => panic!("bypassed: {r}"),
        }
    }

    fn cell(graph: NamedGraph, p: &str) -> ReadItem {
        ReadItem::Cell(graph, nn(p))
    }

    const D: NamedGraph = NamedGraph::DefaultGraph;

    #[test]
    fn constant_predicates_of_the_default_graph() {
        let r = reads("SELECT * WHERE { ?s :p ?o . ?o a :C }");
        assert_eq!(r.len(), 2);
        assert!(r.contains(&cell(D, "p")));
        assert!(r.contains(&ReadItem::Cell(D, oxrdf::vocab::rdf::TYPE.into_owned())));
    }

    #[test]
    fn variable_predicates_read_the_graph() {
        assert_eq!(reads("SELECT * WHERE { ?s ?p ?o }"), vec![ReadItem::Graph(D)]);
        assert_eq!(
            reads("SELECT * WHERE { GRAPH :g { ?s ?p ?o . ?s :p ?x } }"),
            vec![ReadItem::Graph(g("g"))]
        );
    }

    #[test]
    fn graph_variables_read_the_predicate_in_any_graph() {
        assert_eq!(
            reads("SELECT * WHERE { GRAPH ?g { ?s :p ?o } }"),
            vec![ReadItem::Predicate(nn("p"))]
        );
        assert_eq!(
            reads("SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }"),
            vec![ReadItem::All]
        );
        assert_eq!(reads("SELECT ?g WHERE { GRAPH ?g { } }"), vec![ReadItem::All]);
    }

    #[test]
    fn datasets() {
        let r = reads("SELECT * FROM :a FROM :b FROM NAMED :c WHERE { ?s :p ?o GRAPH ?g { ?s :q ?o } }");
        assert_eq!(r.len(), 3);
        assert!(r.contains(&cell(g("a"), "p")));
        assert!(r.contains(&cell(g("b"), "p")));
        assert!(r.contains(&cell(g("c"), "q")));
    }

    #[test]
    fn include_graphs_are_part_of_the_default_graph() {
        let t = NamedGraph::transient();
        match plan_with("SELECT * WHERE { ?s :p ?o }", std::slice::from_ref(&t), &[]) {
            CachePlan::Cache { reads, .. } => {
                assert!(reads.contains(&cell(D, "p")));
                assert!(reads.contains(&cell(t, "p")));
            }
            p => panic!("{p:?}"),
        }
    }

    #[test]
    fn property_paths() {
        let r = reads("SELECT * WHERE { ?s :p/^:q|:r+ ?o }");
        assert_eq!(r.len(), 3);
        assert!(r.contains(&cell(D, "q")));
        // Zero length paths between variables match every term of the graph.
        assert_eq!(reads("SELECT * WHERE { ?s :p* ?o }"), vec![ReadItem::Graph(D)]);
        assert_eq!(reads("SELECT * WHERE { :a :p* ?o }"), vec![cell(D, "p")]);
        assert_eq!(reads("SELECT * WHERE { ?s !(:p|^:q) ?o }"), vec![ReadItem::Graph(D)]);
    }

    #[test]
    fn exists_and_subqueries() {
        let r = reads(
            "SELECT * WHERE { ?s :p ?o FILTER NOT EXISTS { ?s :q ?x } { SELECT ?s WHERE { GRAPH :g { ?s :r ?y } } } }",
        );
        assert_eq!(r.len(), 3);
        assert!(r.contains(&cell(g("g"), "r")));
    }

    #[test]
    fn describe_reads_whole_graphs() {
        assert_eq!(reads("DESCRIBE ?s WHERE { ?s :p ?o }"), vec![ReadItem::Graph(D)]);
    }

    #[test]
    fn fts_reads_the_graph() {
        let q = "PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#> SELECT * WHERE { ?s maplib:fts ?x }";
        assert_eq!(reads(q), vec![ReadItem::Graph(D)]);
    }

    #[test]
    fn volatile_queries_are_bypassed() {
        for (q, why) in [
            ("SELECT (NOW() AS ?n) WHERE {}", "now"),
            ("SELECT * WHERE { ?s :p ?o FILTER(RAND() < 0.5) }", "rand"),
            ("SELECT (UUID() AS ?u) WHERE {}", "uuid"),
            ("SELECT (STRUUID() AS ?u) WHERE {}", "uuid"),
            ("SELECT ?b WHERE { ?s :p ?o BIND(BNODE() AS ?b) }", "bnode"),
            ("SELECT * WHERE { SERVICE <http://x> { ?s ?p ?o } }", "service"),
            ("SELECT (:f(?o) AS ?x) WHERE { ?s :p ?o }", "udf"),
        ] {
            assert_eq!(plan_with(q, &[], &[nn("f")]), CachePlan::Bypass(why), "{q}");
        }
        // A blank node in a CONSTRUCT template is not volatile.
        assert!(matches!(
            plan_with("CONSTRUCT { _:b :p ?o } WHERE { ?s :p ?o }", &[], &[]),
            CachePlan::Cache { .. }
        ));
    }

    #[test]
    fn virtualized_iris_are_flagged() {
        let q = Query::parse(&format!("PREFIX : <{EX}> SELECT * WHERE {{ ?s :v ?o }}"), None, None)
            .unwrap();
        let v: HashSet<NamedNode> = [nn("v")].into();
        let ctx = PlanContext {
            graph: None,
            include_graphs: &[],
            virtualized_iris: Some(&v),
            udfs: &[],
        };
        assert!(matches!(
            cache_plan(&q, &ctx),
            CachePlan::Cache {
                virtualized: true,
                ..
            }
        ));
    }
}

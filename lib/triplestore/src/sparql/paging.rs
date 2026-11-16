//! Keyset pagination of SELECT queries ([sparql-dev#49](https://github.com/w3c-cg/sparql-dev/issues/49)).
//!
//! A page is the result of the query rewritten so that it is ordered by a total order: the
//! ORDER BY of the query, then the projected variables. Each page after the first keeps only
//! the solutions after the last solution of the previous page in that order, as given by the
//! values of the sort keys of that solution (the cursor), instead of skipping the solutions
//! before it with OFFSET. So pages neither repeat nor skip solutions, and each page costs the
//! evaluation of the query and a partial sort, however far into the result it is.
//!
//! Pages are evaluated one at a time, so the model may change between pages. A page sees the
//! model as it is when the page is evaluated: solutions are never returned twice, solutions
//! that stay in the result are returned exactly once, solutions removed before their page are
//! not returned, and solutions added are returned if they sort after the cursor.

use super::errors::SparqlError;
use super::{QuerySettings, Triplestore};
use oxrdf::{BlankNode, Literal, NamedNode, Term, Variable};
use query_processing::find_query_variables::find_all_used_variables_in_expression;
use representation::dataset::NamedGraph;
use representation::polars_to_rdf::column_as_terms;
use representation::result::QueryResultKind;
use representation::solution_mapping::EagerSolutionMappings;
use spargebra::algebra::{Expression, Function, GraphPattern, OrderExpression};
use spargebra::term::GroundTerm;
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

/// The function marking the ORDER BY of a page after the first, see [`KeysetSpec`].
pub(crate) const KEYSET_FUNCTION: &str = "https://datatreehouse.github.io/maplib/vocab#keysetAfter";
/// Marks blank nodes among the cursor values, given as literals of this datatype.
const KEYSET_BLANK_NODE: &str = "https://datatreehouse.github.io/maplib/vocab#keysetBlankNode";
/// Marks unbound cursor values.
const KEYSET_UNBOUND: &str = "https://datatreehouse.github.io/maplib/vocab#keysetUnbound";
const KEY_VARIABLE_PREFIX: &str = "__maplib_page_key_";
const TOKEN_VERSION: &str = "mlp1";

/// Where the next page of a query starts: the sort key values of the last solution returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageCursor {
    /// Identifies the query and dataset, so that a cursor is not used with another query.
    pub fingerprint: u64,
    /// The values of the sort keys of the last solution returned.
    pub keys: Vec<Option<Term>>,
    /// How many of the solutions returned had exactly these sort key values. More than one only
    /// for results with duplicate solutions.
    pub ties: u64,
    /// The number of solutions returned so far, for a LIMIT of the query.
    pub returned: u64,
}

/// A page of the solutions of a query.
#[derive(Debug)]
pub struct QueryPage {
    /// The solutions, with the projected variables of the query.
    pub solutions: EagerSolutionMappings,
    /// Where the next page starts, None when this is the last page.
    pub next: Option<PageCursor>,
}

impl PageCursor {
    /// The cursor as a string that is safe in URLs, to be given back for the next page.
    pub fn to_token(&self) -> String {
        let keys: Vec<serde_json::Value> = self
            .keys
            .iter()
            .map(|k| match k {
                Some(t) => serde_json::Value::String(t.to_string()),
                None => serde_json::Value::Null,
            })
            .collect();
        let json = serde_json::json!({
            "q": format!("{:016x}", self.fingerprint),
            "k": keys,
            "t": self.ties,
            "n": self.returned,
        });
        format!(
            "{TOKEN_VERSION}.{}",
            base64url_encode(json.to_string().as_bytes())
        )
    }

    pub fn from_token(token: &str) -> Result<PageCursor, SparqlError> {
        let invalid = |why: &str| SparqlError::PagingError(format!("invalid page token: {why}"));
        let Some(body) = token.strip_prefix(&format!("{TOKEN_VERSION}.")) else {
            return Err(invalid("unknown version"));
        };
        let bytes = base64url_decode(body).ok_or_else(|| invalid("not base64url"))?;
        let json: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| invalid("not JSON"))?;
        let fingerprint = json
            .get("q")
            .and_then(|q| q.as_str())
            .and_then(|q| u64::from_str_radix(q, 16).ok())
            .ok_or_else(|| invalid("no query fingerprint"))?;
        let ties = json
            .get("t")
            .and_then(|t| t.as_u64())
            .ok_or_else(|| invalid("no ties"))?;
        let returned = json
            .get("n")
            .and_then(|t| t.as_u64())
            .ok_or_else(|| invalid("no count"))?;
        let mut keys = vec![];
        for k in json
            .get("k")
            .and_then(|k| k.as_array())
            .ok_or_else(|| invalid("no keys"))?
        {
            if k.is_null() {
                keys.push(None);
            } else {
                let s = k.as_str().ok_or_else(|| invalid("key is not a string"))?;
                keys.push(Some(
                    Term::from_str(s).map_err(|_| invalid("key is not an RDF term"))?,
                ));
            }
        }
        Ok(PageCursor {
            fingerprint,
            keys,
            ties,
            returned,
        })
    }
}

/// The cursor of a page after the first, as given to the ORDER BY of the rewritten query.
#[derive(Debug, Clone)]
pub(crate) struct KeysetSpec {
    pub keys: Vec<Option<Term>>,
    pub ties: u64,
    pub strict: bool,
}

impl KeysetSpec {
    fn to_order_expression(&self) -> OrderExpression {
        let mut args = vec![
            Expression::Literal(Literal::from(self.ties as i64)),
            Expression::Literal(Literal::from(self.strict)),
        ];
        for k in &self.keys {
            args.push(match k {
                None => Expression::NamedNode(NamedNode::new_unchecked(KEYSET_UNBOUND)),
                Some(Term::NamedNode(nn)) => Expression::NamedNode(nn.clone()),
                Some(Term::BlankNode(b)) => Expression::Literal(Literal::new_typed_literal(
                    b.as_str(),
                    NamedNode::new_unchecked(KEYSET_BLANK_NODE),
                )),
                Some(Term::Literal(l)) => Expression::Literal(l.clone()),
                #[allow(unreachable_patterns)]
                Some(_) => Expression::NamedNode(NamedNode::new_unchecked(KEYSET_UNBOUND)),
            });
        }
        OrderExpression::Asc(Expression::FunctionCall(
            Function::Custom(NamedNode::new_unchecked(KEYSET_FUNCTION)),
            args,
        ))
    }

    /// The cursor, when `oexpr` is the marker that [`KeysetSpec::to_order_expression`] makes.
    pub(crate) fn from_order_expression(
        oexpr: &OrderExpression,
    ) -> Option<Result<KeysetSpec, SparqlError>> {
        let OrderExpression::Asc(Expression::FunctionCall(Function::Custom(f), args)) = oexpr
        else {
            return None;
        };
        if f.as_str() != KEYSET_FUNCTION {
            return None;
        }
        let bad = || SparqlError::PagingError("invalid keyset ordering".to_string());
        if args.len() < 2 {
            return Some(Err(bad()));
        }
        let ties = match &args[0] {
            Expression::Literal(l) => l.value().parse::<u64>().ok(),
            _ => None,
        };
        let strict = match &args[1] {
            Expression::Literal(l) => l.value().parse::<bool>().ok(),
            _ => None,
        };
        let (Some(ties), Some(strict)) = (ties, strict) else {
            return Some(Err(bad()));
        };
        let mut keys = vec![];
        for a in &args[2..] {
            keys.push(match a {
                Expression::NamedNode(nn) if nn.as_str() == KEYSET_UNBOUND => None,
                Expression::NamedNode(nn) => Some(Term::NamedNode(nn.clone())),
                Expression::Literal(l) if l.datatype().as_str() == KEYSET_BLANK_NODE => {
                    match BlankNode::new(l.value()) {
                        Ok(b) => Some(Term::BlankNode(b)),
                        Err(_) => return Some(Err(bad())),
                    }
                }
                Expression::Literal(l) => Some(Term::Literal(l.clone())),
                _ => return Some(Err(bad())),
            });
        }
        Some(Ok(KeysetSpec { keys, ties, strict }))
    }
}

/// A SELECT query taken apart for paging.
struct PagingPlan {
    /// The query, whose pattern is replaced for each page.
    query: Query,
    /// The pattern below the ORDER BY, with the ORDER BY expressions that are not variables
    /// bound to key variables.
    inner: GraphPattern,
    /// The sort keys: variables with whether they are ascending.
    keys: Vec<(Variable, bool)>,
    /// The projected variables of the query.
    variables: Vec<Variable>,
    /// The projected variables and the key variables that are not projected.
    output_variables: Vec<Variable>,
    distinct: bool,
    reduced: bool,
    offset: usize,
    limit: Option<usize>,
}

fn plan_paging(query: &Query) -> Result<PagingPlan, SparqlError> {
    let Query::Select { pattern, .. } = query else {
        return Err(SparqlError::PagingError(
            "only SELECT queries can be paged".to_string(),
        ));
    };
    let mut pattern = pattern.clone();
    let (offset, limit) = if let GraphPattern::Slice {
        inner,
        start,
        length,
    } = pattern
    {
        pattern = *inner;
        (start, length)
    } else {
        (0, None)
    };
    let (distinct, reduced) = match pattern {
        GraphPattern::Distinct { inner } => {
            pattern = *inner;
            (true, false)
        }
        GraphPattern::Reduced { inner } => {
            pattern = *inner;
            (false, true)
        }
        p => {
            pattern = p;
            (false, false)
        }
    };
    let GraphPattern::Project { inner, variables } = pattern else {
        return Err(SparqlError::PagingError(
            "the query has no projection".to_string(),
        ));
    };
    let (mut inner, order) = match *inner {
        GraphPattern::OrderBy { inner, expression } => (*inner, expression),
        p => (p, vec![]),
    };
    let projected: HashSet<&Variable> = variables.iter().collect();
    let mut keys: Vec<(Variable, bool)> = vec![];
    let mut seen: HashSet<Variable> = HashSet::new();
    for (i, oe) in order.into_iter().enumerate() {
        let (expr, asc) = match oe {
            OrderExpression::Asc(e) => (e, true),
            OrderExpression::Desc(e) => (e, false),
        };
        if distinct {
            // The key values are output with the solutions, so with DISTINCT they must be
            // functions of the projected variables, or they would change which solutions are
            // distinct.
            let mut used = HashSet::new();
            find_all_used_variables_in_expression(&expr, &mut used, true, true);
            if let Some(v) = used.iter().find(|v| !projected.contains(v)) {
                return Err(SparqlError::PagingError(format!(
                    "SELECT DISTINCT can only be paged when the ORDER BY uses projected variables, not {v}"
                )));
            }
        }
        let v = if let Expression::Variable(v) = expr {
            v
        } else {
            let v = Variable::new_unchecked(format!("{KEY_VARIABLE_PREFIX}{i}"));
            inner = GraphPattern::Extend {
                inner: Box::new(inner),
                variable: v.clone(),
                expression: expr,
            };
            v
        };
        // A key that occurs again can not change the order.
        if seen.insert(v.clone()) {
            keys.push((v, asc));
        }
    }
    // The projected variables break ties, so that the order is total: solutions with the same
    // keys are equal.
    for v in &variables {
        if seen.insert(v.clone()) {
            keys.push((v.clone(), true));
        }
    }
    let mut output_variables = variables.clone();
    for (k, _) in &keys {
        if !projected.contains(k) {
            output_variables.push(k.clone());
        }
    }
    Ok(PagingPlan {
        query: query.clone(),
        inner,
        keys,
        variables,
        output_variables,
        distinct,
        reduced,
        offset,
        limit,
    })
}

impl PagingPlan {
    /// The query of a page of at most `length` solutions after `after`.
    fn page_query(&self, after: Option<&PageCursor>, start: usize, length: usize) -> Query {
        let mut expression = vec![];
        if let Some(after) = after {
            expression.push(
                KeysetSpec {
                    keys: after.keys.clone(),
                    ties: after.ties,
                    strict: self.distinct,
                }
                .to_order_expression(),
            );
        }
        for (v, asc) in &self.keys {
            let e = Expression::Variable(v.clone());
            expression.push(if *asc {
                OrderExpression::Asc(e)
            } else {
                OrderExpression::Desc(e)
            });
        }
        let mut pattern = GraphPattern::Project {
            inner: Box::new(GraphPattern::OrderBy {
                inner: Box::new(self.inner.clone()),
                expression,
            }),
            variables: self.output_variables.clone(),
        };
        if self.distinct {
            pattern = GraphPattern::Distinct {
                inner: Box::new(pattern),
            };
        } else if self.reduced {
            pattern = GraphPattern::Reduced {
                inner: Box::new(pattern),
            };
        }
        pattern = GraphPattern::Slice {
            inner: Box::new(pattern),
            start,
            length: Some(length),
        };
        let mut query = self.query.clone();
        if let Query::Select { pattern: p, .. } = &mut query {
            *p = pattern;
        }
        query
    }
}

impl Triplestore {
    /// A page of at most `page_size` solutions of a SELECT query, after `after` (None for the
    /// first page), see the [module documentation](self). The LIMIT and OFFSET of the query
    /// apply to the paged result as a whole.
    #[allow(clippy::too_many_arguments)]
    pub fn query_page(
        &self,
        query: &str,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
        prefixes: Option<&HashMap<String, NamedNode>>,
        bindings: Option<&HashMap<String, GroundTerm>>,
        page_size: usize,
        after: Option<&PageCursor>,
    ) -> Result<QueryPage, SparqlError> {
        let fingerprint = query_fingerprint(query, bindings, graph, query_settings);
        let query = Query::parse(query, None, prefixes).map_err(SparqlError::ParseError)?;
        let query = query_processing::bindings::maybe_replace_bindings(query, bindings)
            .map_err(SparqlError::ReplaceBindingsError)?;
        self.query_page_parsed(
            &query,
            parameters,
            query_settings,
            graph,
            page_size,
            after,
            fingerprint,
        )
    }

    /// As [`Triplestore::query_page`], for a parsed query. `fingerprint` identifies the query
    /// and its dataset, see [`query_fingerprint`]: the parsed query can not be used, since the
    /// parser names some variables (e.g. of aggregates) randomly.
    #[allow(clippy::too_many_arguments)]
    pub fn query_page_parsed(
        &self,
        query: &Query,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
        page_size: usize,
        after: Option<&PageCursor>,
        fingerprint: u64,
    ) -> Result<QueryPage, SparqlError> {
        if page_size == 0 {
            return Err(SparqlError::PagingError(
                "the page size must be at least 1".to_string(),
            ));
        }
        let plan = plan_paging(query)?;
        if let Some(after) = after {
            if after.fingerprint != fingerprint {
                return Err(SparqlError::PagingError(
                    "the page token belongs to another query or dataset".to_string(),
                ));
            }
            if after.keys.len() != plan.keys.len() {
                return Err(SparqlError::PagingError(
                    "the page token does not match the query".to_string(),
                ));
            }
        }
        let returned = after.map(|a| a.returned).unwrap_or(0);
        // The solutions that the LIMIT of the query leaves for this page.
        let remaining = plan
            .limit
            .map(|l| (l as u64).saturating_sub(returned) as usize);
        let (length, last_by_limit) = match remaining {
            Some(r) if r <= page_size => (r, true),
            _ => (page_size, false),
        };
        if length == 0 {
            return Ok(QueryPage {
                solutions: self.empty_page(&plan, parameters, query_settings, graph)?,
                next: None,
            });
        }
        // One more solution than the page tells whether there is a next page. Without DISTINCT,
        // the solutions equal to the cursor come first, and those already returned (ties) are
        // skipped: they are equal in every projected variable, so any of them can be.
        let ties = match after {
            Some(a) if !plan.distinct => a.ties as usize,
            _ => 0,
        };
        let fetch = ties + if last_by_limit { length } else { length + 1 };
        // The first page of a query with an OFFSET also fetches the solutions that the OFFSET
        // skips, when there may be duplicates: those equal to the last solution of the page are
        // counted as ties, so that the next page does not return them.
        let lead = if after.is_none() && !plan.distinct {
            plan.offset
        } else {
            0
        };
        let start = if after.is_none() { plan.offset - lead } else { 0 };
        let page_query = plan.page_query(after, start, fetch + lead);
        let res = self.query_parsed(&page_query, parameters, query_settings, graph, false)?;
        let QueryResultKind::Select(mut sm) = res.kind else {
            unreachable!("a SELECT query gives solutions")
        };
        let lead = lead.min(sm.mappings.height());
        let with_lead = sm.clone();
        sm.mappings = sm.mappings.slice(lead as i64, sm.mappings.height() - lead);
        if ties > 0 {
            let after = after.unwrap();
            let head = ties.min(sm.mappings.height());
            let skip = self
                .key_terms(&plan, &sm, 0, head)?
                .iter()
                .take_while(|k| **k == after.keys)
                .count();
            sm.mappings = sm.mappings.slice(skip as i64, sm.mappings.height() - skip);
            if sm.mappings.height() > length + usize::from(!last_by_limit) {
                sm.mappings = sm.mappings.slice(0, length + usize::from(!last_by_limit));
            }
        }
        let has_more = !last_by_limit && sm.mappings.height() > length;
        if sm.mappings.height() > length {
            sm.mappings = sm.mappings.slice(0, length);
        }
        let next = if has_more {
            let page_rows = sm.mappings.height();
            let returned = returned + page_rows as u64;
            if lead > 0 {
                let mut head = with_lead;
                head.mappings = head.mappings.slice(0, lead + page_rows);
                Some(self.next_cursor(&plan, &head, after, fingerprint, returned)?)
            } else {
                Some(self.next_cursor(&plan, &sm, after, fingerprint, returned)?)
            }
        } else {
            None
        };
        Ok(QueryPage {
            solutions: project_solutions(sm, &plan.variables),
            next,
        })
    }

    fn empty_page(
        &self,
        plan: &PagingPlan,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
    ) -> Result<EagerSolutionMappings, SparqlError> {
        let q = plan.page_query(None, 0, 0);
        let res = self.query_parsed(&q, parameters, query_settings, graph, false)?;
        let QueryResultKind::Select(sm) = res.kind else {
            unreachable!("a SELECT query gives solutions")
        };
        Ok(project_solutions(sm, &plan.variables))
    }

    fn next_cursor(
        &self,
        plan: &PagingPlan,
        sm: &EagerSolutionMappings,
        after: Option<&PageCursor>,
        fingerprint: u64,
        returned: u64,
    ) -> Result<PageCursor, SparqlError> {
        let height = sm.mappings.height();
        // The key values of the last rows, as many as needed to count the rows equal to the
        // last one (only results with duplicate solutions have such rows).
        let mut tail = if plan.distinct { 1 } else { 64.min(height) };
        loop {
            let rows = self.key_terms(plan, sm, height - tail, tail)?;
            let last = rows.last().unwrap().clone();
            let equal = rows.iter().rev().take_while(|r| **r == last).count();
            if plan.distinct || equal < tail || tail == height {
                let mut ties = equal as u64;
                if equal == height {
                    if let Some(after) = after {
                        if after.keys == last {
                            ties += after.ties;
                        }
                    }
                }
                return Ok(PageCursor {
                    fingerprint,
                    keys: last,
                    ties,
                    returned,
                });
            }
            tail = (tail * 4).min(height);
        }
    }

    /// The sort key values of `len` rows from `offset`.
    fn key_terms(
        &self,
        plan: &PagingPlan,
        sm: &EagerSolutionMappings,
        offset: usize,
        len: usize,
    ) -> Result<Vec<Vec<Option<Term>>>, SparqlError> {
        let tail = sm.mappings.slice(offset as i64, len);
        let mut columns = vec![];
        for (k, _) in &plan.keys {
            let terms = if let Ok(c) = tail.column(k.as_str()) {
                let t = sm.rdf_node_types.get(k.as_str()).ok_or_else(|| {
                    SparqlError::PagingError(format!("no type for the key {k}"))
                })?;
                column_as_terms(c, t, self.global_cats.clone(), false)
            } else {
                vec![None; tail.height()]
            };
            columns.push(terms);
        }
        Ok((0..tail.height())
            .map(|i| columns.iter().map(|c| c[i].clone()).collect())
            .collect())
    }
}

fn project_solutions(sm: EagerSolutionMappings, variables: &[Variable]) -> EagerSolutionMappings {
    let EagerSolutionMappings {
        mappings,
        mut rdf_node_types,
    } = sm;
    let names: Vec<&str> = variables
        .iter()
        .map(|v| v.as_str())
        .filter(|v| mappings.column(v).is_ok())
        .collect();
    let mappings = mappings.select(names.iter().copied()).unwrap();
    rdf_node_types.retain(|k, _| names.contains(&k.as_str()));
    EagerSolutionMappings::new(mappings, rdf_node_types)
}

/// Identifies a query text with its bindings and dataset (FNV-1a), stable across processes and
/// versions, for checking that a page cursor is used with the query it came from.
pub fn query_fingerprint(
    query: &str,
    bindings: Option<&HashMap<String, GroundTerm>>,
    graph: Option<&NamedGraph>,
    settings: &QuerySettings,
) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    let mut feed = |s: &str| {
        for b in s.as_bytes().iter().chain(std::iter::once(&0u8)) {
            h ^= *b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    };
    feed(query);
    if let Some(bindings) = bindings {
        let mut bs: Vec<_> = bindings.iter().collect();
        bs.sort_by(|a, b| a.0.cmp(b.0));
        for (k, v) in bs {
            feed(k);
            feed(&v.to_string());
        }
    }
    feed(&graph.map(|g| g.to_string()).unwrap_or_default());
    for g in &settings.include_graphs {
        feed(&g.to_string());
    }
    h
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn base64url_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = (chunk[0] as u32) << 16
            | (*chunk.get(1).unwrap_or(&0) as u32) << 8
            | *chunk.get(2).unwrap_or(&0) as u32;
        for i in 0..chunk.len() + 1 {
            out.push(B64[(n >> (18 - 6 * i) & 63) as usize] as char);
        }
    }
    out
}

fn base64url_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut n: u32 = 0;
    let mut bits = 0;
    for c in s.bytes() {
        let v = B64.iter().position(|b| *b == c)? as u32;
        n = n << 6 | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((n >> bits & 0xff) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64url_round_trips() {
        for s in ["", "a", "ab", "abc", "abcd", "{\"k\":[\"<http://ex/é>\"]}"] {
            assert_eq!(
                base64url_decode(&base64url_encode(s.as_bytes())).unwrap(),
                s.as_bytes()
            );
        }
    }

    #[test]
    fn tokens_round_trip() {
        let c = PageCursor {
            fingerprint: 42,
            keys: vec![
                Some(Term::NamedNode(NamedNode::new_unchecked("http://ex/a"))),
                None,
                Some(Term::Literal(Literal::new_language_tagged_literal_unchecked(
                    "x \"y\"", "en",
                ))),
                Some(Term::BlankNode(BlankNode::new_unchecked("b1"))),
            ],
            ties: 3,
            returned: 100,
        };
        assert_eq!(PageCursor::from_token(&c.to_token()).unwrap(), c);
        assert!(PageCursor::from_token("garbage").is_err());
    }
}

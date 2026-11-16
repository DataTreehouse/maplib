//! Keyset pagination of SELECT queries from Python: `Model.query_page` and `Model.query_pages`.

use crate::error::PyMaplibError;
use crate::py_model::PyModel;
use crate::{parse_include_graphs, query_to_result};
use pyo3::prelude::*;
use representation::dataset::NamedGraph;
use representation::result::{QueryResult, QueryResultKind};
use representation::solution_mapping::EagerSolutionMappings;
use spargebra::term::GroundTerm;
use std::collections::HashMap;
use std::sync::Mutex;
use triplestore::sparql::paging::PageCursor;

/// A query to page through, with its arguments.
#[derive(Clone)]
pub(crate) struct PagedQuery {
    pub query: String,
    pub parameters: Option<HashMap<String, EagerSolutionMappings>>,
    pub graph: Option<NamedGraph>,
    pub include_graphs: Option<Vec<String>>,
    pub max_rows: Option<usize>,
    pub bindings: Option<HashMap<String, GroundTerm>>,
    pub page_size: usize,
    pub solution_mappings: bool,
}

/// Runs one page, returning the page as a DataFrame (or SolutionMappings) and the next cursor.
pub(crate) fn run_page(
    model: &PyModel,
    q: &PagedQuery,
    after: Option<&PageCursor>,
    py: Python<'_>,
) -> PyResult<(Py<PyAny>, Option<PageCursor>)> {
    let (page, cats, prefixes) = py.detach(|| -> PyResult<_> {
        let inner = model.inner.read().unwrap();
        let page = inner
            .query_page(
                &q.query,
                q.parameters.as_ref(),
                q.graph.as_ref(),
                parse_include_graphs(q.include_graphs.clone(), true)?,
                q.max_rows,
                q.bindings.as_ref(),
                q.page_size,
                after,
                None,
            )
            .map_err(PyMaplibError::from)?;
        Ok((
            page,
            inner.triplestore.global_cats.clone(),
            inner.prefixes.clone(),
        ))
    })?;
    let res = QueryResult {
        kind: QueryResultKind::Select(page.solutions),
        debug: None,
        pushdown_paths: vec![],
    };
    let out = query_to_result(
        res,
        q.solution_mappings,
        q.solution_mappings,
        false,
        None,
        cats,
        &prefixes,
        py,
    )?;
    Ok((out, page.next))
}

pub(crate) fn parse_page_token(token: Option<&str>) -> PyResult<Option<PageCursor>> {
    token
        .map(|t| {
            PageCursor::from_token(t)
                .map_err(|e| PyMaplibError::FunctionArgumentError(e.to_string()).into())
        })
        .transpose()
}

/// The pages of a SELECT query, read one at a time as the iterator advances.
#[pyclass(name = "QueryPages", frozen)]
pub struct PyQueryPages {
    pub(crate) model: Py<PyModel>,
    pub(crate) query: PagedQuery,
    /// The cursor of the next page, and whether there are no more pages.
    pub(crate) state: Mutex<(Option<PageCursor>, bool)>,
}

#[pymethods]
impl PyQueryPages {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let (after, first) = {
            let state = self.state.lock().unwrap();
            if state.1 {
                return Ok(None);
            }
            (state.0.clone(), state.0.is_none())
        };
        let (page, next) = run_page(self.model.get(), &self.query, after.as_ref(), py)?;
        let mut state = self.state.lock().unwrap();
        state.1 = next.is_none();
        let empty = page
            .bind(py)
            .getattr("height")
            .and_then(|h| h.extract::<usize>())
            .map(|h| h == 0)
            .unwrap_or(false);
        state.0 = next;
        // The first page is returned even when it is empty, so that the columns are known. A
        // later page is only empty when the model changed, and ends the iteration.
        if empty && !first {
            state.1 = true;
            return Ok(None);
        }
        Ok(Some(page))
    }

    /// The token of the next page, to continue with `Model.query_pages(..., page_token=)` or
    /// `Model.query_page`, or None when there are no more pages.
    #[getter]
    fn token(&self) -> Option<String> {
        let state = self.state.lock().unwrap();
        if state.1 {
            None
        } else {
            state.0.as_ref().map(|c| c.to_token())
        }
    }

    #[getter]
    fn page_size(&self) -> usize {
        self.query.page_size
    }
}

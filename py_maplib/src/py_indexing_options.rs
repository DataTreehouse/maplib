use crate::DEFAULT_FTS;
use pyo3::{pyclass, pymethods};
use representation::python::PyIRI;
use std::collections::HashSet;
use std::path::Path;
use tracing::instrument;
use triplestore::IndexingOptions;

#[derive(Clone)]
#[pyclass(name = "IndexingOptions", from_py_object)]
pub struct PyIndexingOptions {
    pub inner: IndexingOptions,
}

#[pymethods]
impl PyIndexingOptions {
    #[new]
    #[instrument(skip_all)]
    #[pyo3(signature = (
        object_sort_all=None,
        object_sort_some=None,
        fts=None,
        fts_path=None,
        geo=None,
        fts_predicates=None,
        fts_local_names=None,
        fts_iris=None,
        vectors=None,
        vector_ann=None,
    ))]
    pub fn new(
        object_sort_all: Option<bool>,
        object_sort_some: Option<Vec<PyIRI>>,
        fts: Option<bool>,
        fts_path: Option<String>,
        geo: Option<bool>,
        fts_predicates: Option<Vec<PyIRI>>,
        fts_local_names: Option<bool>,
        fts_iris: Option<bool>,
        vectors: Option<bool>,
        vector_ann: Option<bool>,
    ) -> PyIndexingOptions {
        let fts = fts.unwrap_or(DEFAULT_FTS) || fts_path.is_some();
        let fts_path = fts_path.map(|fts_path| Path::new(&fts_path).to_owned());
        let inner = if object_sort_all.is_none() && object_sort_some.is_none() {
            IndexingOptions::new_default_object_sort(fts, fts_path)
        } else {
            let object_sort_all = object_sort_all.unwrap_or(false);
            let object_sort_some: Option<HashSet<_>> = object_sort_some.map(|object_sort_some| {
                object_sort_some
                    .into_iter()
                    .map(|x| x.into_inner())
                    .collect()
            });
            IndexingOptions::new(object_sort_all, object_sort_some, fts, fts_path)
        };
        let fts_predicates =
            fts_predicates.map(|ps| ps.into_iter().map(|x| x.into_inner()).collect());
        let inner = inner
            .with_geo(geo.unwrap_or(false))
            .with_fts_options(
            fts_predicates,
            fts_local_names.unwrap_or(false),
            fts_iris.unwrap_or(false),
        )
            .with_vectors(vectors.unwrap_or(false), vector_ann.unwrap_or(false));
        PyIndexingOptions { inner }
    }
}

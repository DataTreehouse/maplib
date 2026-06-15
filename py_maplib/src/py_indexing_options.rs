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
        subject_object_index=None,
    ))]
    pub fn new(
        object_sort_all: Option<bool>,
        object_sort_some: Option<Vec<PyIRI>>,
        fts: Option<bool>,
        fts_path: Option<String>,
        subject_object_index: Option<bool>,
    ) -> PyIndexingOptions {
        let fts = fts.unwrap_or(DEFAULT_FTS) || fts_path.is_some();
        let fts_path = fts_path.map(|fts_path| Path::new(&fts_path).to_owned());
        let subject_object_index =
            subject_object_index.unwrap_or(IndexingOptions::default_subject_object_index());
        let inner = if object_sort_all.is_none() && object_sort_some.is_none() {
            let opts =
                IndexingOptions::new_default_object_sort(fts, fts_path, subject_object_index);
            opts
        } else {
            let object_sort_all = object_sort_all.unwrap_or(false);
            if object_sort_all && object_sort_some.is_none() {
                IndexingOptions::default()
            } else {
                let object_sort_some: Option<HashSet<_>> =
                    object_sort_some.map(|object_sort_some| {
                        object_sort_some
                            .into_iter()
                            .map(|x| x.into_inner())
                            .collect()
                    });
                IndexingOptions::new(
                    object_sort_all,
                    object_sort_some,
                    fts,
                    fts_path,
                    subject_object_index,
                )
            }
        };
        PyIndexingOptions { inner }
    }
}

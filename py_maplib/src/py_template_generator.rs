use crate::error::PyMaplibError;
use crate::parse_optional_named_node;
use crate::py_model::PyModel;
use maplib::template_generator::{generate_templates, TemplateGeneratorError};
use pyo3::exceptions::{PyTypeError, PyUserWarning, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use representation::dataset::NamedGraph;
use templates::python::PyTemplate;

/// Generate templates for instantiating the classes in an ontology
///
/// :param m: The model where the ontology is stored. We rely on owl:Class/rdfs:Class declarations, rdfs:subClassOf, rdfs:domain, rdfs:range (also via owl:unionOf), owl:Restriction and the declared types of properties.
/// :param graph: The named graph where the ontology is stored. Defaults to the default graph.
/// :param prefixes: Prefixes (prefix name to namespace IRI) used to name the parameters, e.g. {"ex": "https://example.net/"}.
///     Common prefixes such as rdf, rdfs, owl, xsd, skos and dcterms are known already.
///     Namespaces without a prefix get the generated prefixes ns1, ns2, ...
///
/// :return A dictionary of templates for instantiating the classes in the ontology, where the keys are the class IRIs.
///     For each class there is a template with the class IRI, which also produces the rdf:type triple, and one with
///     the IRI of the class suffixed by "_notype", which does not. The subject is the parameter "id",
///     the other (optional) parameters are named by the prefixed IRIs of the properties, e.g. "ex:name",
///     so the columns of the data frame you map should be named like that.
///     Properties that have the class as their range are included in the reverse direction, with the suffix
///     "_inverse" on the local name (e.g. "ex:hasPart_inverse"). Remaining name clashes get the suffixes "_2", "_3", ...
///     Templates of subclasses instantiate the "_notype" templates of their superclasses,
///     so all templates must be added to the Model you want to populate.
///
/// Usage example - note that it is important to add the templates to the Model you want to populate,
/// and that the columns of the data frame are named by the prefixed IRIs of the properties.
/// >>> import polars as pl
/// >>> from maplib import Model, generate_templates
/// >>>
/// >>> m_ont = Model()
/// >>> m_ont.read("my_ontology.ttl")
/// >>> templates = generate_templates(m_ont, prefixes={"ex": "https://example.net/"})
/// >>> m = Model()
/// >>> for t in templates.values():
/// >>>     m.add_template(t)
/// >>> df = pl.DataFrame({"id": ["https://example.net/thing1"], "ex:name": ["Thing 1"]})
/// >>> m.map("https://example.net/MyClass", df)
#[pyfunction(name = "generate_templates")]
#[pyo3(signature = (m, graph=None, prefixes=None))]
pub fn py_generate_templates<'py>(
    py: Python<'py>,
    m: &Bound<'py, PyAny>,
    graph: Option<&Bound<'py, PyAny>>,
    prefixes: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyDict>> {
    let Ok(m) = m.cast::<PyModel>() else {
        return Err(PyTypeError::new_err(format!(
            "Expected a Model, got {}",
            m.get_type().name()?
        )));
    };
    let graph = match graph {
        None => None,
        Some(g) if g.is_none() => None,
        Some(g) => match g.extract::<String>() {
            Ok(g) => Some(g),
            Err(_) => {
                return Err(PyTypeError::new_err(format!(
                    "Expected graph to be a str or None, got {}",
                    g.get_type().name()?
                )))
            }
        },
    };
    let mut prefix_list = vec![];
    if let Some(prefixes) = prefixes.filter(|p| !p.is_none()) {
        let Ok(prefixes) = prefixes.cast::<PyDict>() else {
            return Err(PyTypeError::new_err(format!(
                "Expected prefixes to be a dict or None, got {}",
                prefixes.get_type().name()?
            )));
        };
        for (name, ns) in prefixes.iter() {
            match (name.extract::<String>(), ns.extract::<String>()) {
                (Ok(name), Ok(ns)) => prefix_list.push((name, ns)),
                _ => {
                    return Err(PyTypeError::new_err(
                        "Prefixes must map prefix names (str) to namespace IRIs (str)",
                    ))
                }
            }
        }
    }
    let graph =
        parse_optional_named_node(graph)?.map(|g| NamedGraph::from_maybe_named_node(Some(&g)));

    let model = m.borrow().inner.clone();
    let generated = py
        .detach(|| {
            let inner = model.read().unwrap_or_else(|e| e.into_inner());
            generate_templates(&inner, graph.as_ref(), &prefix_list)
        })
        .map_err(|e| match e {
            TemplateGeneratorError::InvalidPrefixName(_) => PyValueError::new_err(e.to_string()),
            TemplateGeneratorError::Maplib(e) => PyMaplibError::from(e).into(),
            e => PyMaplibError::RuntimeError(e.to_string()).into(),
        })?;
    let category = py.get_type::<PyUserWarning>();
    for w in &generated.warnings {
        let msg = std::ffi::CString::new(w.replace('\0', "")).unwrap_or_default();
        PyErr::warn(py, category.as_any(), &msg, 1)?;
    }
    let dict = PyDict::new(py);
    for (iri, template) in generated.templates {
        dict.set_item(iri, PyTemplate { template })?;
    }
    Ok(dict)
}

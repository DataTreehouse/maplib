from typing import Dict, Optional
from .ordering import topological_sort
import pathlib
import polars as pl

from maplib import Model, Variable, RDFType, Parameter, Triple, IRI, Instance, Template


def generate_templates(m: Model, graph: Optional[str]) -> Dict[str, Template]:
    """Generate templates for instantiating the classes in an ontology

    :param m: The model where the ontology is stored. We mainly rely on rdfs:subClassOf, rdfs:range and rdfs:domain.
    :param graph: The named graph where the ontology is stored.

    :return A dictionary of templates for instantiating the classes in the ontology, where the keys are the class URIs.

    Usage example - note that it is important to add the templates to the Model you want to populate.
    >>> from maplib import Model, create_templates
    >>>
    >>> m_ont = Model()
    >>> m_ont.read("my_ontology.ttl")
    >>> templates = generate_templates(m_ont)
    >>> m = Model()
    >>> for t in templates.values():
    >>>     m.add_template(t)
    >>> m.map("https://example.net/MyClass", df)
    """

    properties = get_properties(m, graph=graph)
    properties_by_domain = {}
    properties_by_range = {}
    for r in properties.iter_rows(named=True):
        dom = r["domain"]
        if dom in properties_by_domain:
            properties_by_domain[dom].append(r)
        else:
            properties_by_domain[dom] = [r]

        ran = r["range"]
        if ran in properties_by_range:
            properties_by_range[ran].append(r)
        else:
            properties_by_range[ran] = [r]

    subclasses = get_subclasses(m, graph=graph)

    subclass_of = {}
    for r in (
        subclasses.group_by("child")
        .agg(pl.col("parent").alias("parents"))
        .iter_rows(named=True)
    ):
        subclass_of[r["child"]] = r["parents"]

    class_ordering = topological_sort(subclasses)

    templates_without_typing = generate_templates_without_typing(
        properties_by_domain, properties_by_range, class_ordering, subclass_of
    )
    templates_with_typing = generate_templates_with_typing(templates_without_typing)
    templates = {}
    for t, template in templates_without_typing.items():

        templates[t + "_notype"] = template
    for t, template in templates_with_typing.items():
        templates[t] = template

    return templates


def get_properties(m: Model, graph: Optional[str]) -> pl.DataFrame:
    properties = m.query(
        """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
        PREFIX owl:  <http://www.w3.org/2002/07/owl#> 

        SELECT ?property ?property_type ?domain ?range WHERE { 
            ?property a ?property_type .
            ?property rdfs:domain ?domain .
            ?property rdfs:range ?range .
            FILTER(ISIRI(?domain) && ISIRI(?range))
        }
        """,
        native_dataframe=True,
        graph=graph,
    )
    pl.Config.set_fmt_str_lengths(100)
    properties_by_subclass_restriction = m.query(
        """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
        PREFIX owl:  <http://www.w3.org/2002/07/owl#> 

        SELECT ?property ?property_type ?domain WHERE { 
            ?domain rdfs:subClassOf ?restr .
            ?restr a owl:Restriction .
            ?restr owl:onProperty ?property .
            #?property a ?property_type .
        }
        """,
        native_dataframe=True,
        graph=graph,
    )
    properties_by_subclass_restriction = (
        properties_by_subclass_restriction.with_columns(
            pl.lit("http://www.w3.org/2000/01/rdf-schema#ObjectType").alias(
                "property_type"
            ),
            pl.lit(None).cast(pl.String).alias("range"),
        )
    )
    if properties.height == 0:
        return properties_by_subclass_restriction
    elif properties_by_subclass_restriction.height == 0:
        return properties
    else:
        return properties.vstack(properties_by_subclass_restriction)

def get_subclasses(m: Model, graph: Optional[str]) -> pl.DataFrame:
    subclasses = m.query(
        """
        PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#> 
        SELECT ?child ?parent WHERE { 
            ?child rdfs:subClassOf ?parent .
            FILTER(ISIRI(?child) && ISIRI(?parent))
        }
        """,
        native_dataframe=True,
        graph=graph,
    )
    return subclasses


def uri_to_variable(uri: str) -> Variable:
    split = uri.split("/")
    name = split[-1].split("#")[-1]
    cleaned_name = name.replace("-", "_").replace(".", "_")
    return Variable(cleaned_name)


def generate_templates_without_typing(
    properties_by_domain,
    properties_by_range,
    class_ordering,
    subclass_of: Dict[str, str],
) -> Dict[str, Template]:
    templates = {}
    i = 0
    subj = get_subj_var()

    for c in class_ordering:
        i += 1

        parameters = []
        instances = []
        existing_preds = set()
        existing_varnames = set()

        # Check dupe!!
        subj_parameter = Parameter(
            variable=subj, optional=False, rdf_type=RDFType.IRI()
        )
        parameters.append(subj_parameter)
        if c in properties_by_domain:
            for p in properties_by_domain[c]:
                if p["property"] in existing_preds:
                    # print("dupe: ", str(p))
                    continue
                existing_preds.add(p["property"])
                v = uri_to_variable(p["property"])
                existing_varnames.add(v.name)
                if p["property_type"] == "http://www.w3.org/2002/07/owl#ObjectProperty":
                    t = RDFType.IRI()
                elif p["range"]:
                    t = RDFType.Literal(p["range"])
                else:
                    t = None
                param = Parameter(variable=v, optional=True, rdf_type=t)
                parameters.append(param)
                predicate = IRI(p["property"])
                triple = Triple(subj, predicate, v)
                instances.append(triple)
        if c in properties_by_range:
            for p in properties_by_range[c]:
                if p["property"] in existing_preds:
                    # print("dupe: ", str(p))
                    continue
                existing_preds.add(p["property"])
                v = uri_to_variable(p["property"])
                existing_varnames.add(v.name)
                t = RDFType.IRI()
                param = Parameter(variable=v, optional=True, rdf_type=t)
                parameters.append(param)
                predicate = IRI(p["property"])
                triple = Triple(v, predicate, subj)
                instances.append(triple)
        c_tpl_iri = c + "_notype"
        if c in subclass_of:
            for sc in subclass_of[c]:
                if sc in templates:
                    sct = templates[sc]
                    variables = []
                    for p in sct.parameters:
                        if p.variable.name not in existing_varnames:
                            variables.append(p.variable)
                            if p.variable.name != "id":
                                parameters.append(p)
                        else:
                            variables.append(None)
                            # print(f"Duplicate variable: {str(p.variable.name)}")
                    instances.append(sct.instance(variables))

        tpl = Template(IRI(c_tpl_iri), parameters=parameters, instances=instances)
        templates[c] = tpl
    return templates


def generate_templates_with_typing(
    templates: Dict[str, Template],
) -> Dict[str, Template]:
    subj = get_subj_var()
    with_type_templates = {}
    for t, template in templates.items():
        instances = template.instances
        instances.append(
            Triple(subj, IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), IRI(t))
        )
        new_template = Template(
            iri=IRI(t), parameters=template.parameters, instances=instances
        )
        with_type_templates[t] = new_template
    return with_type_templates


def get_subj_var() -> Variable:
    return Variable("id")

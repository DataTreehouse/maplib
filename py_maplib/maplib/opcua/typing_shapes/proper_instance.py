from maplib import Model, Prefix, Variable, Template, Triple, Parameter, RDFType, xsd
import polars as pl
import logging

logger = logging.getLogger(__name__)


def add_has_proper_instance(m: Model) -> Model:
    logger.info("Start adding hasProperInstance helper")
    uamapper_prefix = Prefix("https://github.com/DataTreehouse/uamapper#")

    logger.info("Start has proper instance query")
    has_proper_instance = m.query(
        """
    PREFIX ua:<http://opcfoundation.org/UA/>
    PREFIX uamapper:<https://github.com/DataTreehouse/uamapper#>
    SELECT DISTINCT ?type ?node_in_type_namespace WHERE {         
        ?node a ?type .
        ?node ua:Namespace ?node_ns .
        ?node_ns uamapper:typeNamespace ?node_in_type_namespace .     
    }""",
        solution_mappings=True,
    )
    logger.info("Finish has proper instance query")
    has_proper_instance = has_proper_instance.mappings.group_by("type").agg(
        pl.col("node_in_type_namespace").not_().any().alias("has_proper_instance")
    )
    type = Variable("type")
    has_proper_instance_var = Variable("has_proper_instance")
    has_proper_instance_template = Template(
        uamapper_prefix.suf("HasProperInstanceTemplate"),
        # Todo check why this fails without RDF types specified..
        parameters=[
            Parameter(type, rdf_type=RDFType.IRI),
            Parameter(has_proper_instance_var, rdf_type=RDFType.Literal(xsd.boolean)),
        ],
        instances=[
            Triple(
                type, uamapper_prefix.suf("hasProperInstance"), has_proper_instance_var
            )
        ],
    )
    m.map(has_proper_instance_template, has_proper_instance)
    logger.info("Finish adding hasProperInstance helper")

    return m

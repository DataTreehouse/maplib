from maplib import (
    Model,
    Template,
    Variable,
    Parameter,
    RDFType,
    xsd,
    Prefix,
    Triple,
    a,
    BlankNode,
    Argument,
    Literal,
    IRI,
)
import polars as pl


def add_mandatory_cardinality_shapes(
    m: Model, shapes_graph: str, concept: str
) -> Model:
    uamapper_prefix = Prefix("https://github.com/DataTreehouse/uamapper#")
    ua_prefix = Prefix("http://opcfoundation.org/UA/")
    shacl_prefix = Prefix("http://www.w3.org/ns/shacl#")
    rdfs_prefix = Prefix("http://www.w3.org/2000/01/rdf-schema#")
    maplib_prefix = Prefix("https://datatreehouse.github.io/maplib/vocab#")

    parent_shape = Variable("parent_shape")
    cardinality_shape = Variable("cardinality_shape")
    reference_type = Variable("reference_type")
    message = Variable("message")
    browse_name = Variable("browse_name")
    browse_name_space = Variable("browse_name_space")
    label = Variable("label")
    description = Variable("description")
    node = Variable("node")

    qualified_shape = BlankNode("qs")
    qualified_browse_name_shape = BlankNode("qbns")
    qualified_browse_name_space_shape = BlankNode("qbnss")

    # The browse name property shape will check cardinality
    ua_mandatory_template = Template(
        iri=uamapper_prefix.suf("MandatoryShapeTemplate"),
        parameters=[
            Parameter(parent_shape, rdf_type=RDFType.Nested(RDFType.IRI)),
            Parameter(cardinality_shape, rdf_type=RDFType.IRI),
            Parameter(reference_type, rdf_type=RDFType.IRI),
            Parameter(browse_name, rdf_type=RDFType.Literal(xsd.string)),
            Parameter(browse_name_space, rdf_type=RDFType.IRI),
            Parameter(message, rdf_type=RDFType.Literal(xsd.string)),
            Parameter(label, rdf_type=RDFType.Literal(xsd.string)),
            Parameter(description, rdf_type=RDFType.Literal(xsd.string)),
            Parameter(node, rdf_type=RDFType.IRI),
        ],
        instances=[
            Triple(
                Argument(parent_shape, list_expand=True),
                shacl_prefix.suf("property"),
                cardinality_shape,
                list_expander="cross",
            ),
            Triple(cardinality_shape, maplib_prefix.suf("shapeCategory"), IRI(concept)),
            Triple(cardinality_shape, rdfs_prefix.suf("definedBy"), node),
            Triple(cardinality_shape, a, shacl_prefix.suf("PropertyShape")),
            Triple(cardinality_shape, shacl_prefix.suf("message"), message),
            Triple(cardinality_shape, shacl_prefix.suf("path"), reference_type),
            Triple(
                cardinality_shape,
                shacl_prefix.suf("minCount"),
                Literal("1", data_type=xsd.integer),
            ),
            Triple(cardinality_shape, rdfs_prefix.suf("label"), label),
            Triple(cardinality_shape, rdfs_prefix.suf("comment"), description),
            Triple(
                cardinality_shape,
                shacl_prefix.suf("qualifiedValueShape"),
                qualified_shape,
            ),
            Triple(
                cardinality_shape,
                shacl_prefix.suf("qualifiedMinCount"),
                Literal("1", data_type=xsd.integer),
            ),
            Triple(
                cardinality_shape,
                shacl_prefix.suf("qualifiedMaxCount"),
                Literal("1", data_type=xsd.integer),
            ),
            Triple(
                qualified_shape,
                shacl_prefix.suf("property"),
                qualified_browse_name_shape,
            ),
            Triple(
                qualified_browse_name_shape,
                shacl_prefix.suf("path"),
                ua_prefix.suf("BrowseName"),
            ),
            Triple(
                qualified_browse_name_shape, shacl_prefix.suf("hasValue"), browse_name
            ),
            Triple(
                qualified_shape,
                shacl_prefix.suf("property"),
                qualified_browse_name_space_shape,
            ),
            Triple(
                qualified_browse_name_space_shape,
                shacl_prefix.suf("path"),
                ua_prefix.suf("BrowseNamespace"),
            ),
            Triple(
                qualified_browse_name_space_shape,
                shacl_prefix.suf("hasValue"),
                browse_name_space,
            ),
        ],
    )

    mandatory_sms = m.query(
        """
    PREFIX ua:<http://opcfoundation.org/UA/>
    PREFIX uamapper:<https://github.com/DataTreehouse/uamapper#>
    SELECT ?type_browse_name ?parent ?reference_type ?node ?node_id ?browse_name ?browse_name_space ?browse_path WHERE {
        ?base_type uamapper:baseFiid ?node .
        ?base_type ua:BrowseName ?type_browse_name .
        ?parent uamapper:fiid ?node .
        ?node ua:NodeId ?node_id .
        ?node ua:Namespace ?namespace .
        ?parent ?reference_type ?node .
        ?node ua:BrowseName ?browse_name .
        ?node ua:BrowseNamespace ?browse_name_space .
        ?node uamapper:path ?browse_path . 
        ?node ua:HasModellingRule ?mandatory .
        ?mandatory ua:BrowseName "Mandatory" .  
        ua:HierarchicalReferences ua:HasSubtype+ ?reference_type .
        FILTER( 
            ?reference_type != ua:HasSubtype 
        )
    }""",
        solution_mappings=True,
    )
    mandatory = mandatory_sms.mappings
    mandatory = (
        mandatory.with_columns(
            (pl.col("parent") + pl.lit("_Shape")).alias("parent_shape"),
            (pl.col("node") + pl.lit("_CardinalityShape")).alias("cardinality_shape"),
            (pl.lit("Missing mandatory node on path: ") + pl.col("browse_path")).alias(
                "message"
            ),
            (
                pl.lit("Ensure instance of ")
                + pl.col("type_browse_name")
                + pl.lit(" has mandatory node on path: ")
                + pl.col("browse_path")
                + pl.lit(" (")
                + pl.col("node_id")
                + pl.lit(")")
            ).alias("description"),
            (
                pl.col("type_browse_name")
                + pl.lit(' browse path: "')
                + pl.col("browse_path")
                + pl.lit('" mandatory child exists')
            ).alias("label"),
        )
        .select(
            [
                "node",
                "parent_shape",
                "cardinality_shape",
                "reference_type",
                "browse_name",
                "browse_name_space",
                "message",
                "label",
                "description",
            ]
        )
        .group_by(
            "node",
            "cardinality_shape",
            "reference_type",
            "browse_name",
            "browse_name_space",
            "message",
            "label",
            "description",
        )
        .agg(pl.col("parent_shape"))
    )

    m.map(
        ua_mandatory_template,
        mandatory,
        graph=shapes_graph,
    )

    return m


def add_browse_path_shapes(m: Model, shapes_graph: str, concept: str):
    uamapper_prefix = Prefix("https://github.com/DataTreehouse/uamapper#")
    shacl_prefix = Prefix("http://www.w3.org/ns/shacl#")
    rdfs_prefix = Prefix("http://www.w3.org/2000/01/rdf-schema#")
    maplib_prefix = Prefix("https://datatreehouse.github.io/maplib/vocab#")

    browse_path_shapes_sms = m.query(
        """
        PREFIX ua:<http://opcfoundation.org/UA/>
        PREFIX uamapper:<https://github.com/DataTreehouse/uamapper#>
        SELECT ?base_type ?type_browse_name ?node_id ?node ?browse_path ?has_proper_instance WHERE {
            ?base_type uamapper:baseFiid ?node .
            ?base_type ua:BrowseName ?type_browse_name .
            ?node uamapper:path ?browse_path . 
            ?node ua:NodeId ?node_id .
            OPTIONAL { 
                ?node uamapper:hasProperInstance ?has_proper_instance . 
            }
            FILTER (
                ?browse_path != ""
            )
        }""",
        solution_mappings=True,
    )
    browse_path_shapes = browse_path_shapes_sms.mappings
    browse_path_shapes = browse_path_shapes.with_columns(
        (pl.col("base_type") + pl.lit("_Shape")).alias("type_shape"),
        (pl.col("node") + pl.lit("_Shape")).alias("shape"),
        pl.col("has_proper_instance")
        .fill_null(pl.lit(False))
        .not_()
        .alias("deactivated"),
        (
            pl.col("type_browse_name")
            + pl.lit(' browse path: "')
            + pl.col("browse_path")
            + pl.lit('" conforms')
        ).alias("label"),
        (
            pl.lit("Shape to check the existing instances on path: ")
            + pl.col("browse_path")
        ).alias("description"),
    ).select(["type_shape", "shape", "node", "label", "description", "deactivated"])

    shape = Variable("shape")
    node = Variable("node")
    deactivated = Variable("deactivated")
    label = Variable("label")
    description = Variable("description")
    type_shape = Variable("type_shape")

    browse_path_shape_template = Template(
        iri=uamapper_prefix.suf("BrowsePathShapeTemplate"),
        parameters=[
            Parameter(shape, rdf_type=RDFType.IRI),
            Parameter(node, rdf_type=RDFType.IRI),
            Parameter(deactivated, rdf_type=RDFType.Literal(xsd.boolean)),
            Parameter(label, rdf_type=RDFType.Literal(xsd.string)),
            Parameter(description, rdf_type=RDFType.Literal(xsd.string)),
            Parameter(type_shape, rdf_type=RDFType.IRI),
        ],
        instances=[
            Triple(shape, a, shacl_prefix.suf("NodeShape")),
            Triple(shape, maplib_prefix.suf("shapeCategory"), IRI(concept)),
            Triple(shape, rdfs_prefix.suf("isDefinedBy"), node),
            Triple(shape, shacl_prefix.suf("targetClass"), node),
            Triple(shape, shacl_prefix.suf("deactivated"), deactivated),
            Triple(shape, rdfs_prefix.suf("label"), label),
            Triple(shape, rdfs_prefix.suf("comment"), description),
        ],
    )
    m.map(
        browse_path_shape_template,
        browse_path_shapes,
        graph=shapes_graph,
    )

    return m


def add_type_shapes(m: Model, shapes_graph: str, concept: str) -> Model:
    uamapper_prefix = Prefix("https://github.com/DataTreehouse/uamapper#")
    shacl_prefix = Prefix("http://www.w3.org/ns/shacl#")
    rdfs_prefix = Prefix("http://www.w3.org/2000/01/rdf-schema#")
    maplib_prefix = Prefix("https://datatreehouse.github.io/maplib/vocab#")

    shape = Variable("shape")
    node = Variable("node")
    select = Variable("select")
    sparql_target = BlankNode("sparql_target")
    deactivated = Variable("deactivated")
    label = Variable("label")
    description = Variable("description")

    type_shapes_sms = m.query(
        """
        PREFIX ua:<http://opcfoundation.org/UA/>
        PREFIX uamapper:<https://github.com/DataTreehouse/uamapper#>
        SELECT ?type ?type_browse_name ?has_proper_instance WHERE {
            ?type ua:BrowseName ?type_browse_name .
            ?type ua:NodeClass ?type_node_class .
            OPTIONAL { 
                ?type uamapper:hasProperInstance ?has_proper_instance. 
            }
            FILTER (
                ?type_node_class in (ua:UAObjectType, ua:UAVariableType)
            )
        }""",
        solution_mappings=True,
    )
    type_shapes = type_shapes_sms.mappings
    type_shapes = (
        type_shapes.with_columns(
            (pl.col("type") + pl.lit("_Shape")).alias("shape"),
            (pl.col("type_browse_name") + pl.lit(" validation")).alias("label"),
            (pl.lit("Shape to check instances of ") + pl.col("type_browse_name")).alias(
                "description"
            ),
            pl.col("has_proper_instance")
            .fill_null(pl.lit(False))
            .not_()
            .alias("deactivated"),
            # We want to avoid using targetClass since this has inheritance
            (
                pl.lit(
                    """
    PREFIX ua:<http://opcfoundation.org/UA/>
    PREFIX uamapper:<https://github.com/DataTreehouse/uamapper#>
    SELECT ?this WHERE {
        ?this ua:Namespace ?ns .
        ?ns uamapper:typeNamespace false .
        ?this ua:HasTypeDefinition <"""
                )
                + pl.col("type")
                + pl.lit(
                    """> .
    }"""
                )
            ).alias("select"),
        )
        .rename({"type": "node"})
        .select(
            [
                "shape",
                "node",
                "select",
                "deactivated",
                "label",
                "description",
            ]
        )
    )

    type_shape_template = Template(
        iri=uamapper_prefix.suf("BrowsePathShapeTemplate"),
        parameters=[
            Parameter(shape, rdf_type=RDFType.IRI),
            Parameter(node, rdf_type=RDFType.IRI),
            Parameter(select, rdf_type=RDFType.Literal(xsd.string)),
            Parameter(deactivated, rdf_type=RDFType.Literal(xsd.boolean)),
            Parameter(label, rdf_type=RDFType.Literal(xsd.string)),
            Parameter(description, rdf_type=RDFType.Literal(xsd.string)),
        ],
        instances=[
            Triple(shape, a, shacl_prefix.suf("NodeShape")),
            Triple(
                shape,
                maplib_prefix.suf("shapeCategory"),
                IRI(concept),
            ),
            Triple(shape, rdfs_prefix.suf("isDefinedBy"), node),
            Triple(shape, shacl_prefix.suf("target"), sparql_target),
            Triple(sparql_target, a, shacl_prefix.suf("SPARQLTarget")),
            Triple(sparql_target, shacl_prefix.suf("select"), select),
            Triple(shape, shacl_prefix.suf("deactivated"), deactivated),
            Triple(shape, rdfs_prefix.suf("label"), label),
            Triple(shape, rdfs_prefix.suf("comment"), description),
        ],
    )
    m.map(type_shape_template, type_shapes, graph=shapes_graph)
    return m

from pathlib import Path
from typing import Union, List, Dict, Optional, Callable, Tuple, Literal as LiteralType
from polars import DataFrame
from datetime import datetime, date

class RDFType:
    """
    The type of a column containing a RDF variable.
    For instance, xsd:string is RDFType.Literal("http://www.w3.org/2001/XMLSchema#string")
    """

    IRI: Callable[[], "RDFType"]
    BlankNode: Callable[[], "RDFType"]
    Literal: Callable[[Union[str, "IRI"]], "RDFType"]
    Multi: Callable[[List["RDFType"]], "RDFType"]
    Nested: Callable[["RDFType"], "RDFType"]
    Unknown: Callable[[], "RDFType"]

class SolutionMappings:
    """
    Detailed information about the solution mappings and the types of the variables.
    """

    mappings: DataFrame
    rdf_types: Dict[str, RDFType]

class Variable:
    """
    A variable in a template.
    """

    name: str

    def __init__(self, name: str):
        """
        Create a new variable.
        :param name: The name of the variable.
        """
        ...

class IRI:
    iri: str
    """
    An IRI.
    """

    def __init__(self, iri: str):
        """
        Create a new IRI
        :param iri: IRI (without < and >).
        """

class BlankNode:
    """
    A Blank Node.
    """

    name: str

    def __init__(self, name: str):
        """
        Create a new Blank Node
        :param name: Name of blank node (without _:).
        """

class Prefix:
    """
    A prefix that can be used to ergonomically build iris.
    """

    def __init__(self, iri,  prefix_name=None):
        """
        Create a new prefix.
        :param iri: The prefix IRI.
        :param prefix_name: The name of the prefix
        """

    def suf(self, suffix: str) -> IRI:
        """
        Create an IRI by appending the suffix.
        :param suffix: The suffix to append.
        :return:
        """

class Literal:
    """
    An RDF literal.
    """

    value: str
    datatype: Optional[IRI]
    language: Optional[str]

    def __init__(self, value: str, data_type: IRI = None, language: str = None):
        """
        Create a new RDF Literal
        :param value: The lexical representation of the value.
        :param data_type: The data type of the value (an IRI).
        :param language: The language tag of the value.
        """

    def to_native(self) -> Union[int, float, bool, str, datetime, date]:
        """

        :return:
        """

class Parameter:
    variable: Variable
    optional: bool
    allow_blank: bool
    rdf_type: Optional[RDFType]
    default_value: Optional[Union[Literal, IRI, BlankNode]]
    """
    Parameters for template signatures.
    """

    def __init__(
        self,
        variable: Variable,
        optional: Optional[bool] = False,
        allow_blank: Optional[bool] = True,
        rdf_type: Optional[RDFType] = None,
        default_value: Optional[Union[Literal, IRI, BlankNode]] = None,
    ):
        """
        Create a new parameter for a Template.
        :param variable: The variable.
        :param optional: Can the variable be unbound?
        :param allow_blank: Can the variable be bound to a blank node?
        :param rdf_type: The type of the variable. Can be nested.
        :param default_value: Default value when no value provided.
        """

class Argument:
    def __init__(
        self, term: Union[Variable, IRI, Literal], list_expand: Optional[bool] = False
    ):
        """
        An argument for a template instance.
        :param term: The term.
        :param list_expand: Should the argument be expanded? Used with the list_expander argument of instance.
        """

class Instance:
    def __init__(
        self,
        iri: IRI,
        arguments: List[Union[Argument, Variable, IRI, Literal, BlankNode, None]],
        list_expander: Optional[LiteralType["cross", "zipMin", "zipMax"]] = None,
    ):
        """
        A template instance.
        :param iri: The IRI of the template to be instantiated.
        :param arguments: The arguments for template instantiation.
        :param list_expander: (How) should we do list expansion?
        """

class Template:
    iri: str
    parameters: List[Parameter]
    instances: List[Instance]
    """
    An OTTR Template.
    Note that accessing parameters- or instances-fields returns copies. 
    To change these fields, you must assign new lists of parameters or instances.  
    """

    def __init__(
        self,
        iri: IRI,
        parameters: List[Union[Parameter, Variable]],
        instances: List[Instance],
    ):
        """
        Create a new OTTR Template
        :param iri: The IRI of the template
        :param parameters:
        :param instances:
        """

    def instance(
        self,
        arguments: List[Union[Argument, Variable, IRI, Literal, None]],
        list_expander: LiteralType["cross", "zipMin", "zipMax"] = None,
    ) -> Instance:
        """

        :param arguments: The arguments to the template.
        :param list_expander: (How) should we list-expand?
        :return:
        """

def Triple(
    subject: Union["Argument", IRI, Variable, BlankNode],
    predicate: Union["Argument", IRI, Variable, BlankNode],
    object: Union["Argument", IRI, Variable, Literal, BlankNode],
    list_expander: Optional[LiteralType["cross", "zipMin", "zipMax"]] = None,
):
    """
    An OTTR Triple Pattern used for creating templates.
    This is the basis pattern which all template instances are rewritten into.
    Equivalent to:

    >>> ottr = Prefix("http://ns.ottr.xyz/0.4/")
    ... Instance(ottr.suf("Triple"), subject, predicate, object, list_expander)

    :param subject:
    :param predicate:
    :param object:
    :param list_expander:
    :return:
    """

class XSD:
    """
    The xsd namespace, for convenience.
    """

    boolean: IRI
    byte: IRI
    date: IRI
    dateTime: IRI
    dateTimeStamp: IRI
    decimal: IRI
    double: IRI
    duration: IRI
    float: IRI
    int_: IRI
    integer: IRI
    language: IRI
    long: IRI
    short: IRI
    string: IRI

    def __init__(self):
        """
        Create the xsd namespace helper.
        """

def a() -> IRI:
    """
    :return: IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
    """

# END COMMON WITH CHRONTEXT

class IndexingOptions:
    """
    Options for indexing
    """

    def __init__(
        self,
        object_sort_all: bool = None,
        object_sort_some: List["IRI"] = None,
        fts_path: str = None,
    ):
        """
        Defaults to indexing on subjects and objects for select types (e.g. rdf:type and rdfs:label)

        :param object_sort_all: Enable object-indexing for all suitable predicates (doubles memory requirement).
        :param object_sort_some: Enable object-indexing for a selected list of predicates.
        :param fts_path: Enable full text search, stored at the path
        """

ParametersType = Dict[str, Tuple[DataFrame, Dict[str, RDFType]]]

class ValidationReport:
    """
    SHACL Validation report.
    Only constructed by maplib.
    """

    conforms: bool
    "Whether or not the validation report conforms to the shapes"

    shape_targets: DataFrame
    "A DataFrame containing the counts of the targets of each shape and constraint"

    performance: DataFrame
    "Performance statistics for the validation process"

    def results(
        self,
        native_dataframe: bool = False,
        include_datatypes: bool = False,
        streaming: bool = False,
    ) -> Optional[Union[DataFrame, "SolutionMappings"]]:
        """
        Return the results of the validation report, if they exist.

        :param native_dataframe: Return columns with maplib-native formatting. Useful for round-trips.
        :param include_datatypes: Return datatypes of the results DataFrame (returns SolutionMappings instead of DataFrame).
        :param streaming: Use the Polars streaming functionality.
        :return: The SHACL validation report, as a DataFrame
        """

    def details(
        self,
        native_dataframe: bool = False,
        include_datatypes: bool = False,
        streaming: bool = False,
    ) -> Optional[DataFrame]:
        """
        Returns the details of the validation report.
        Only available if validation was called with include_details=True.

        :param native_dataframe: Return columns with maplib-native formatting. Useful for round-trips.
        :param include_datatypes: Return datatypes of the results DataFrame (returns SolutionMappings instead of DataFrame).
        :param streaming: Use the Polars streaming functionality.
        :return: Details of the SHACL validation report, as a DataFrame
        """

    def graph(self) -> "Mapping":
        """
        Creates a new mapping object where the base graph is the validation report with results.
        Includes the details of the validation report in the new graph if they exist.

        :return:
        """

class Model:
    """
    A mapping session allowing:

    * Iterative mapping using OTTR templates
    * Interactive SPARQL querying and enrichment
    * SHACL validation

    Usage:

    >>> from maplib import Model
    ... doc = '''
    ... :prefix ex:<http://example.net/ns#>.
    ... ex:ExampleTemplate [?MyValue] :: {
    ...    ottr:Triple(ex:myObject, ex:hasValue, ?MyValue)
    ... } .'''
    ... m = Model()
    ... m.add_template(doc)

    :param documents: a stOTTR document or a list of these
    :param indexing_options: options for indexing
    """

    def __init__(
        self,
        indexing_options: "IndexingOptions" = None,
    ) -> "Model": ...
    def add_template(self, template: Union["Template", str]):
        """
        Add a template to the model. Overwrites any existing template with the same IRI.
        :param template: The template to add, as a stOTTR string or as a programmatically constructed Template.
        :return:
        """

    def map(
        self,
        template: Union[str, "Template", IRI],
        df: DataFrame = None,
        graph: str = None,
        types: Dict[str, RDFType] = None,
        validate_iris: bool = True,
    ) -> None:
        """
        Map a template using a DataFrame
        Usage:

        >>> m.map("ex:ExampleTemplate", df)

        If the template has no arguments, the df argument is not necessary.

        :param template: Template, IRI, IRI string or prefixed template name.
        :param df: DataFrame where the columns have the same names as the template arguments
        :param graph: The IRI of the graph to add triples to.
        :param types: The types of the columns.
        :param validate_iris: Validate any IRI-columns.
        """

    def map_triples(
        self,
        df: DataFrame = None,
        predicate: str = None,
        graph: str = None,
        types: Dict[str, RDFType] = None,
        validate_iris: bool = True,
    ) -> None:
        """
        Map a template using a DataFrame with columns subject, object and predicate
        The predicate column can also be supplied as a string if it is the same for all rows.
        Usage:

        >>> m.map_triples(df)

        If the template has no arguments, the df argument is not necessary.

        :param df: DataFrame where the columns are named subject and object. May also contain a verb-column.
        :param verb: The uri of the verb.
        :param graph: The IRI of the graph to add triples to.
        :param types: The types of the columns.
        :param validate_iris: Validate any IRI-columns.
        """

    def map_default(
        self,
        df: DataFrame,
        primary_key_column: str,
        dry_run: bool = False,
        graph: str = None,
        types: Dict[str, RDFType] = None,
        validate_iris: bool = True,
    ) -> str:
        """
        Create a default template and map it based on a dataframe.
        Usage:

        >>> template_string = m.map_default(df, "myKeyCol")
        ... print(template_string)

        :param df: DataFrame where the columns have the same names as the template arguments
        :param primary_key_column: This column will be the subject of all triples in the generated template.
        :param dry_run: Do not map the template, only return the string.
        :param graph: The IRI of the graph to add triples to.
        :param types: The types of the columns.
        :param validate_iris: Validate any IRI-columns.
        :return: The generated template
        """

    def query(
        self,
        query: str,
        parameters: ParametersType = None,
        include_datatypes: bool = False,
        native_dataframe: bool = False,
        graph: str = None,
        streaming: bool = False,
        return_json: bool = False,
        include_transient: bool = True,
    ) -> Union[
        DataFrame, SolutionMappings, List[Union[DataFrame, SolutionMappings, str]], None
    ]:
        """
        Query the contained knowledge graph using SPARQL
        Currently, SELECT, CONSTRUCT and INSERT are supported.
        Usage:

        >>> df = model.query('''
        ... PREFIX ex:<http://example.net/ns#>
        ... SELECT ?obj1 ?obj2 WHERE {
        ...    ?obj1 ex:hasObj ?obj2
        ... }''')
        ... print(df)

        :param query: The SPARQL query string
        :param parameters: PVALUES Parameters, a DataFrame containing the value bindings in the custom PVALUES construction.
        :param native_dataframe: Return columns with maplib-native formatting. Useful for round-trips.
        :param include_datatypes: Datatypes are not returned by default, set to true to return a dict with the solution mappings and the datatypes.
        :param graph: The IRI of the graph to query.
        :param streaming: Use Polars streaming
        :param return_json: Return JSON string.
        :param include_transient: Include transient triples when querying.
        :return: DataFrame (Select), list of DataFrames (Construct) containing results, or None for Insert-queries

        """

    def update(
            self,
            update: str,
            parameters: ParametersType = None,
            streaming: bool = False,
            include_transient: bool = True,
    ):
        """
        Insert the results of a Construct query in the graph.
        Useful for being able to use the same query for inspecting what will be inserted and actually inserting.
        Usage:

        >>> m = Model(doc)
        ... # Omitted
        ... update_pizzas = '''
        ... ...'''
        ... m.update(update_pizzas)

        :param update: The SPARQL Update string
        :param parameters: PVALUES Parameters, a DataFrame containing the value bindings in the custom PVALUES construction.
        :param streaming: Use Polars streaming
        :param include_transient: Include transient triples when querying (but see "transient" above).
        :return: None
        """

    def insert(
        self,
        query: str,
        parameters: ParametersType = None,
        include_datatypes: bool = False,
        native_dataframe: bool = False,
        transient: bool = False,
        streaming: bool = False,
        source_graph: str = None,
        target_graph: str = None,
        include_transient: bool = True,
    ):
        """
        Insert the results of a Construct query in the graph.
        Useful for being able to use the same query for inspecting what will be inserted and actually inserting.
        Usage:

        >>> m = Model(doc)
        ... # Omitted
        ... hpizzas = '''
        ... PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
        ... PREFIX ing:<https://github.com/magbak/maplib/pizza/ingredients#>
        ... CONSTRUCT { ?p a pizza:HeterodoxPizza }
        ... WHERE {
        ... ?p a pizza:Pizza .
        ... ?p pizza:hasIngredient ing:Pineapple .
        ... }'''
        ... m.insert(hpizzas)

        :param query: The SPARQL Insert query string
        :param parameters: PVALUES Parameters, a DataFrame containing the value bindings in the custom PVALUES construction.
        :param native_dataframe: Return columns with maplib-native formatting. Useful for round-trips.
        :param include_datatypes: Datatypes are not returned by default, set to true to return a dict with the solution mappings and the datatypes.
        :param transient: Should the inserted triples be transient?
        :param source_graph: The IRI of the source graph to execute the construct query.
        :param target_graph: The IRI of the target graph to insert into.
        :param streaming: Use Polars streaming
        :param include_transient: Include transient triples when querying (but see "transient" above).
        :return: None
        """

    def validate(
        self,
        shape_graph: str,
        include_details: bool = False,
        include_conforms: bool = False,
        include_shape_graph: bool = True,
        streaming: bool = False,
        max_shape_constraint_results: int = None,
        only_shapes: List[str] = None,
        deactivate_shapes: List[str] = None,
        dry_run: bool = False,
    ) -> ValidationReport:
        """
        Validate the contained knowledge graph using SHACL
        Assumes that the contained knowledge graph also contains SHACL Shapes.

        :param shape_graph: The IRI of the Shape Graph.
        :param include_details: Include details of SHACL evaluation alongside the report. Currently uses a lot of memory.
        :param include_conforms: Include those results that conformed. Also applies to details.
        :param include_shape_graph: Include the shape graph in the report, useful when creating the graph from the report.
        :param include_datatypes: Return the datatypes of the validation report (and details).
        :param streaming: Use Polars streaming
        :param max_shape_constraint_results: Maximum number of results per shape and constraint. Reduces the size of the result set.
        :param only_shapes: Validate only these shapes, None means all shapes are validated (must be IRI, cannot be used with deactivate_shapes).
        :param deactivate_shapes: Disable validation of these shapes (must be IRI, cannot be used with deactivate_shapes).
        :param dry_run: Only find targets of shapes, but do not validate them.
        :return: Validation report containing a report (report.df) and whether the graph conforms (report.conforms)
        """

    def read(
        self,
        file_path: Union[str, Path],
        format: LiteralType["ntriples", "turtle", "rdf/xml", "xml", "rdfxml"] = None,
        base_iri: str = None,
        transient: bool = False,
        parallel: bool = None,
        checked: bool = True,
        graph: str = None,
        replace_graph: bool = False,
    ) -> None:
        """
        Reads triples from a file path.
        You can specify the format, or it will be derived using file extension, e.g. filename.ttl or filename.nt.
        Specify transient if you only want the triples to be available for further querying and validation,
        but not persisted using write-methods.

        Usage:

        >>> m.read("my_triples.ttl")

        :param file_path: The path of the file containing triples
        :param format: One of "ntriples", "turtle", "rdf/xml", otherwise it is inferred from the file extension.
        :param base_iri: Base iri
        :param transient: Should these triples be included when writing the graph to the file system?
        :param parallel: Parse triples in parallel, currently only NTRiples and Turtle. Assumes all prefixes are in the beginning of the document. Defaults to true only for NTriples.
        :param checked: Check IRIs etc.
        :param graph: The IRI of the graph to read the triples into, if None, it will be the default graph.
        :param replace_graph: Replace the graph with these triples? Will replace the default graph if no graph is specified.
        """

    def reads(
        self,
        s: str,
        format: LiteralType["ntriples", "turtle", "rdf/xml", "xml", "rdfxml"],
        base_iri: str = None,
        transient: bool = False,
        parallel: bool = None,
        checked: bool = True,
        graph: str = None,
        replace_graph: bool = False,
    ) -> None:
        """
        Reads triples from a string.
        Specify transient if you only want the triples to be available for further querying and validation,
        but not persisted using write-methods.

        Usage:

        >>> m.reads(my_ntriples_string, format="ntriples")

        :param s: String containing serialized triples.
        :param format: One of "ntriples", "turtle", "rdf/xml".
        :param base_iri: Base iri
        :param transient: Should these triples be included when writing the graph to the file system?
        :param parallel: Parse triples in parallel, currently only NTRiples and Turtle. Assumes all prefixes are in the beginning of the document. Defaults to true for NTriples.
        :param checked: Check IRIs etc.
        :param graph: The IRI of the graph to read the triples into.
        :param replace_graph: Replace the graph with these triples? Will replace the default graph if no graph is specified.
        """

    def write_cim_xml(
        self,
        file_path: Union[str, Path],
        profile_graph: str,
        model_iri: str = None,
        version: str = None,
        description: str = None,
        created: str = None,
        scenario_time: str = None,
        modeling_authority_set: str = None,
        prefixes: Dict[str, str] = None,
        graph: str = None,
    ) -> None:
        """
        Write the legacy CIM XML format.

        >>> PROFILE_GRAPH = "urn:graph:profiles"
        >>> m = Model()
        >>> m.read(model_path, base_iri=publicID, format="rdf/xml")
        >>> m.read("61970-600-2_Equipment-AP-Voc-RDFS2020_v3-0-0.rdf", graph=PROFILE_GRAPH, format="rdf/xml")
        >>> m.read("61970-600-2_Operation-AP-Voc-RDFS2020_v3-0-0.rdf", graph=PROFILE_GRAPH, format="rdf/xml")
        >>> m.write_cim_xml(
        >>>     "model.xml",
        >>>     profile_graph=PROFILE_GRAPH,
        >>>     description = "MyModel",
        >>>     created = "2023-09-14T20:27:41",
        >>>     scenario_time = "2023-09-14T02:44:43",
        >>>     modeling_authority_set="www.westernpower.co.uk",
        >>>     version="22",
        >>> )

        :param file_path: The path of the file containing triples
        :param profile_graph: The IRI of the graph containing the ontology of the CIM profile to write.
        :param model_iri: model_iri a md:FullModel. Is generated if not provided.
        :param version: model_iri md:Model.version version .
        :param description: model_iri md:Model.description description .
        :param created: model_iri md:Model.created created .
        :param scenario_time: model_iri md:Model.scenarioTime scenario_time .
        :param modeling_authority_set: model_iri md:Model.modelingAuthoritySet modeling_authority_set .
        :param prefixes: Prefixes to be used in XML export.
        :param graph: The graph to write, defaults to the default graph.
        """

    def write(
        self,
        file_path: Union[str, Path],
        format=LiteralType["ntriples", "turtle", "rdf/xml"],
        graph: str = None,
    ) -> None:
        """
        Write the non-transient triples to the file path specified in the NTriples format.

        Usage:

        >>> m.write("my_triples.nt", format="ntriples")

        :param file_path: The path of the file containing triples
        :param format: One of "ntriples", "turtle", "rdf/xml".
        :param graph: The IRI of the graph to write.
        """

    def writes(
        self, format=LiteralType["ntriples", "turtle", "rdf/xml"], graph: str = None
    ) -> str:
        """
        DEPRECATED: use writes with format="ntriples"
        Write the non-transient triples to a string in memory.

        Usage:

        >>> s = m.write_ntriples_string(format="turtle")

        :param format: One of "ntriples", "turtle", "rdf/xml".
        :param graph: The IRI of the graph to write.
        :return Triples in model in the NTriples format (potentially a large string)
        """

    def write_native_parquet(
        self, folder_path: Union[str, Path], graph: str = None
    ) -> None:
        """
        Write non-transient triples using the internal native Parquet format.

        Usage:

        >>> m.write_native_parquet("output_folder")

        :param folder_path: The path of the folder to write triples in the native format.
        :param graph: The IRI of the graph to write.
        """

    def create_sprout(self):
        """
        A sprout is a simplified way of dealing with multiple graphs.
        See also `Model.insert_sprout` and `Model.detach_sprout`

        :return:
        """

    def insert_sprout(
        self,
        query: str,
        parameters: ParametersType = None,
        include_datatypes: bool = False,
        native_dataframe: bool = False,
        transient: bool = False,
        streaming: bool = False,
        source_graph: str = None,
        target_graph: str = None,
        include_transient: bool = True,
    ):
        """
        Insert the results of a Construct query in a sprouted graph, which is created if no sprout is active.
        Sprouts are simplified way of dealing with multiple graphs.
        Useful for being able to use the same query for inspecting what will be inserted and actually inserting.
        See also `Model.detach_sprout`

        Usage:

        >>> m = Model()
        ... m.add_template(doc)
        ... m.create_sprout()
        ... # Omitted
        ... hpizzas = '''
        ... PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
        ... PREFIX ing:<https://github.com/magbak/maplib/pizza/ingredients#>
        ... CONSTRUCT { ?p a pizza:HeterodoxPizza }
        ... WHERE {
        ... ?p a pizza:Pizza .
        ... ?p pizza:hasIngredient ing:Pineapple .
        ... }'''
        ... m.insert_sprout(hpizzas)

        :param query: The SPARQL Insert query string
        :param parameters: PVALUES Parameters, a DataFrame containing the value bindings in the custom PVALUES construction.
        :param native_dataframe: Return columns with maplib-native formatting. Useful for round-trips.
        :param include_datatypes: Datatypes are not returned by default, set to true to return a dict with the solution mappings and the datatypes.
        :param transient: Should the inserted triples be included in exports?
        :param source_graph: The IRI of the source graph to execute the construct query.
        :param target_graph: The IRI of the target graph to insert into.
        :param streaming: Use Polars streaming
        :param include_transient: Include transient triples when querying (see also "transient" above).
        :return: None
        """

    def detach_sprout(self) -> "Model":
        """
        Detaches and returns the sprout from the model.

        :return: The sprout as its own Model.
        """

    def get_predicate_iris(
        self, graph: str = None, include_transient: bool = False
    ) -> List["IRI"]:
        """
        :param graph: The graph to get the predicate iris from.
        :param include_transient: Should we include predicates only between transient triples?
        :return: The IRIs of the predicates currently in the given graph.
        """

    def get_predicate(
        self, iri: "IRI", graph: str = None, include_transient: bool = False
    ) -> List["SolutionMappings"]:
        """
        :param iri: The predicate IRI
        :param graph: The graph to get the predicate from.
        :param include_transient: Should we include transient triples?
        :return: A list of the underlying tables that store a given predicate.
        """

    def create_index(
        self, options: "IndexingOptions" = None, all: bool = True, graph: str = None
    ):
        """
        :param options: Indexing options
        :param all: Apply to all existing and new graphs
        :param graph: The graph where indexes should be added
        :return:
        """

    def infer(
        self,
        ruleset: Union[str, List[str]],
        include_datatypes: bool = False,
        native_dataframe: bool = False,
    ) -> Optional[Dict[str, DataFrame]]:
        """
        Run the inference rules that are provided
        :param ruleset: The Datalog ruleset (a string).
        :param native_dataframe: Return columns with maplib-native formatting. Useful for round-trips.
        :param include_datatypes: Datatypes are not returned by default, set to true to return a dict with the solution mappings and the datatypes.
        :return: The inferred N-Tuples.
        """

class MaplibException(Exception): ...

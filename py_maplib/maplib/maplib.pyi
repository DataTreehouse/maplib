from pathlib import Path
from typing import Union, List, Dict, Optional, Callable, Tuple, Literal as LiteralType
from polars import DataFrame


class RDFType:
    """
    The type of a column containing a RDF variable.
    """
    IRI: Callable[[], "RDFType"]
    Blank: Callable[[], "RDFType"]
    Literal: Callable[[str], "RDFType"]
    Nested: Callable[["RDFType"], "RDFType"]
    Unknown: Callable[[], "RDFType"]


class Variable:
    """
    A variable in a template.
    """

    def __init__(self, name: str):
        """
        Create a new variable.
        :param name: The name of the variable.
        """
        ...


class IRI:
    """
    An IRI.
    """

    def __init__(self, iri: str):
        """
        Create a new IRI
        :param iri: IRI (without < and >).
        """


class Prefix:
    """
    A prefix that can be used to ergonomically build iris.
    """

    def __init__(self, prefix, iri):
        """
        Create a new prefix.
        :param prefix: The name of the prefix
        :param iri: The prefix IRI.
        """

    def suf(self, suffix: str) -> IRI:
        """
        Create a IRI by appending the suffix.
        :param suffix: The suffix to append.
        :return:
        """


class Literal:
    """
    An RDF literal.
    """

    def __init__(self, value: str, data_type: IRI = None, language: str = None):
        """
        Create a new RDF Literal
        :param value: The lexical representation of the value.
        :param data_type: The data type of the value (an IRI).
        :param language: The language tag of the value.
        """


class Parameter:
    def __init__(self,
                 variable: Variable,
                 optional: bool = False,
                 allow_blank: bool = True,
                 rdf_type: Union[RDFType] = None):
        """
        Create a new parameter.
        :param variable: The variable.
        :param optional: Can the variable be unbound?
        :param allow_blank: Can the variable be bound to a blank node?
        :param rdf_type: The type of the variable. Can be nested.
        """


class Argument:
    def __init__(self, term: Union[Variable, IRI, Literal], list_expand: bool):
        """
        An argument for a template instance.
        :param term: The term.
        :param list_expand: Should the argument be expanded? Used with the list_expander argument of instance.
        """


class Instance:
    def __init__(self,
                 iri: IRI,
                 arguments: List[Union[Argument, Variable, IRI, Literal]],
                 list_expander: LiteralType["cross", "zipMin", "zipMax"] = None):
        """
        A template instance.
        :param iri: The IRI of the template to be instantiated.
        :param arguments: The arguments for template instantiation.
        :param list_expander: (How) should we do list expansion?
        """


class Template:
    """
    An OTTR Template.
    """

    def __init__(self,
                 iri: IRI,
                 parameters: List[Parameter],
                 instances: List[Instance]):
        """
        Create a new OTTR Template
        :param iri: The IRI of the template
        :param parameters:
        :param instances:
        """

    def instance(self,
                 arguments: List[Union[Argument, Variable, IRI, Literal]],
                 list_expander: LiteralType["cross", "zipMin", "zipMax"] = None) -> Instance:
        """

        :param arguments: The arguments to the template.
        :param list_expander: (How) should we list-expand?
        :return:
        """


ParametersType = Dict[str, Tuple[DataFrame, Dict[str, RDFType]]]


class ValidationReport:
    """
    SHACL Validation report.
    Only constructed by maplib.
    """
    conforms: bool

    def results(self,
                native_dataframe: bool = False) -> Optional[DataFrame]:
        """
        Return the results of the validation report, if they exist.

        :param native_dataframe: Return columns with maplib-native formatting. Useful for round-trips.
        :return: The SHACL validation report, as a DataFrame
        """

    def details(self,
                native_dataframe: bool = False) -> Optional[DataFrame]:
        """
        Returns the details of the validation report.
        Only available if validation was called with include_details=True.

        :param native_dataframe: Return columns with maplib-native formatting. Useful for round-trips.
        :return: Details of the SHACL validation report, as a DataFrame
        """

    def graph(self) -> "Mapping":
        """
        Creates a new mapping object where the base graph is the validation report with results.
        Includes the details of the validation report in the new graph if they exist.

        :return:
        """


class Mapping:
    """
    A mapping session allowing:

    * Iterative mapping using OTTR templates
    * Interactive SPARQL querying and enrichment
    * SHACL validation

    Usage:

    >>> from maplib import Mapping
    ... doc = '''
    ... :prefix ex:<http://example.net/ns#>.
    ... ex:ExampleTemplate [?MyValue] :: {
    ...    ottr:Triple(ex:myObject, ex:hasValue, ?MyValue)
    ... } .'''
    ... m = Mapping(doc)

    :param documents: a stOTTR document or a list of these
    :param caching_folder: a folder to cache the triples as Parquet files
    """

    def __init__(self, documents: Union[str, List[str]] = None, caching_folder: str = None) -> Mapping: ...

    def add_template(self, template: Template):
        """
        Add a template to the mapping. Overwrites any existing template with the same IRI.
        :param template: The template to add.
        :return:
        """

    def expand(self, template: Union[str, Template, IRI],
               df: DataFrame = None, unique_subset: List[str] = None) -> None:
        """
        Expand a template using a DataFrame
        Usage:

        >>> m.expand("ex:ExampleTemplate", df)
        ... m.expand("ex:ExampleTemplate", df, unique_subsets=["MyValue"])

        If the template has no arguments, the df argument is not necessary.

        :param template: Template, IRI, IRI string or prefixed template name.
        :param df: DataFrame where the columns have the same names as the template arguments
        :param unique_subset: DataFrame column names known to be unique e.g. ["colA", "colB"], for a performance boost (reduce costly deduplication)
        """

    def expand_default(self, df: DataFrame, primary_key_column: str, template_prefix: str = None,
                       predicate_uri_prefix: str = None) -> str:
        """
        Create a default template and expand it based on a dataframe.
        Usage:

        >>> template_string = m.expand_default(df, "myKeyCol")
        ... print(template_string)

        :param df: DataFrame where the columns have the same names as the template arguments
        :param primary_key_column: This column will be the subject of all triples in the generated template.
        :param template_prefix: Prefix of the template - the name is auto-generated.
        :param predicate_uri_prefix: Prefix of the predicates/verbs in the generated template, names are derived from column names.
        :return: The generated template
        """

    def query(self, query: str,
              parameters: ParametersType = None,
              include_datatypes:bool = False,
              native_dataframe:bool = False,
              graph: str = None) -> Union[
        DataFrame,
        Dict[str, Union[DataFrame, Dict[str, str]]],
        List[Union[DataFrame, Dict[str, Union[DataFrame, Dict[str, str]]]]],
        None]:
        """
        Query the contained knowledge graph using SPARQL
        Currently, SELECT, CONSTRUCT and INSERT are supported.
        Usage:

        >>> df = mapping.query('''
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
        :return: DataFrame (Select), list of DataFrames (Construct) containing results, or None for Insert-queries

        """

    def insert(self, query: str,
               parameters: ParametersType = None,
               transient: bool = False,
               source_graph: str = None,
               target_graph: str = None):
        """
        Insert the results of a Construct query in the graph.
        Useful for being able to use the same query for inspecting what will be inserted and actually inserting.
        Usage:

        >>> m = Mapping(doc)
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
        :param transient: Should the inserted triples be included in exports?
        :param source_graph: The IRI of the source graph to execute the construct query.
        :param target_graph: The IRI of the target graph to insert into.
        :return: None
        """

    def validate(self,
                 shape_graph: str,
                 include_details: bool = False,
                 include_conforms: bool = False,
                 ) -> ValidationReport:
        """
        Validate the contained knowledge graph using SHACL
        Assumes that the contained knowledge graph also contains SHACL Shapes.

        :param shape_graph: The IRI of the Shape Graph.
        :param include_details: Include details of SHACL evaluation alongside the report. Currently uses a lot of memory.
        :param include_conforms: Include those results that conformed. Also applies to details.
        :return: Validation report containing a report (report.df) and whether the graph conforms (report.conforms)
        """

    def read_triples(self,
                     file_path: Union[str, Path],
                     format: LiteralType["ntriples", "turtle", "rdf/xml", "xml", "rdfxml"] = None,
                     base_iri: str = None,
                     transient: bool = False,
                     parallel: bool = False,
                     checked: bool = True,
                     deduplicate: bool = True,
                     graph: str = None) -> None:
        """
        Reads triples from a file path.
        You can specify the format, or it will be derived using file extension, e.g. filename.ttl or filename.nt.
        Specify transient if you only want the triples to be available for further querying and validation,
        but not persisted using write-methods.

        Usage:

        >>> m.read_triples("my_triples.ttl")

        :param file_path: The path of the file containing triples
        :param format: One of "ntriples", "turtle", "rdf/xml", otherwise it is inferred from the file extension.
        :param base_iri: Base iri
        :param transient: Should these triples be included when writing the graph to the file system?
        :param parallel: Parse triples in parallel, currently only NTRiples. Assumes all prefixes are in the beginning of the document.
        :param checked: Check IRIs etc.
        :param deduplicate: Set to true by default, disable to increase throughput for large files containing only unique triples.
        :param graph: The IRI of the graph to read the triples into.
        """

    def read_triples_string(self,
                            s: str,
                            format: LiteralType["ntriples", "turtle", "rdf/xml", "xml", "rdfxml"],
                            base_iri: str = None,
                            transient: bool = False,
                            parallel: bool = False,
                            checked: bool = True,
                            deduplicate: bool = True,
                            graph: str = None) -> None:
        """
        Reads triples from a string.
        Specify transient if you only want the triples to be available for further querying and validation,
        but not persisted using write-methods.

        Usage:

        >>> m.read_triples(my_ntriples_string, format="ntriples")

        :param s: String containing serialized triples.
        :param format: One of "ntriples", "turtle", "rdf/xml".
        :param base_iri: Base iri
        :param transient: Should these triples be included when writing the graph to the file system?
        :param parallel: Parse triples in parallel, currently only NTRiples. Assumes all prefixes are in the beginning of the document.
        :param checked: Check IRIs etc.
        :param deduplicate: Set to true by default, disable to increase throughput for large files containing only unique triples.
        :param graph: The IRI of the graph to read the triples into.
        """

    def write_ntriples(self, file_path: Union[str, Path], graph: str = None) -> None:
        """
        Write the non-transient triples to the file path specified in the NTriples format.

        Usage:

        >>> m.write_ntriples("my_triples.nt")

        :param file_path: The path of the file containing triples
        :param graph: The IRI of the graph to write.
        """

    def write_ntriples_string(self, graph: str = None) -> str:
        """
        Write the non-transient triples to a string in memory.

        Usage:

        >>> s = m.write_ntriples_string()

        :param graph: The IRI of the graph to write.
        :return Triples in mapping in the NTriples format (potentially a large string)
        """

    def write_native_parquet(self, folder_path: Union[str, Path], graph: str = None) -> None:
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
        See also `maplib.maplib.Mapping.insert_sprout` and `maplib.maplib.Mapping.detach_sprout`

        :return:
        """

    def insert_sprout(self, query: str,
                      parameters: ParametersType = None,
                      transient: bool = False,
                      source_graph: str = None,
                      target_graph: str = None):
        """
        Insert the results of a Construct query in a sprouted graph, which is created if no sprout is active.
        Sprouts are simplified way of dealing with multiple graphs.
        Useful for being able to use the same query for inspecting what will be inserted and actually inserting.
        See also `maplib.maplib.Mapping.detach_sprout`

        Usage:

        >>> m = Mapping(doc)
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
        :param transient: Should the inserted triples be included in exports?
        :param source_graph: The IRI of the source graph to execute the construct query.
        :param target_graph: The IRI of the target graph to insert into.
        :return: None
        """

    def detach_sprout(self) -> Mapping:
        """
        Detaches and returns the sprout from the mapping.

        @return: The sprout as its own Mapping.
        """


def triple(subject: Union[Argument, Variable, IRI],
           predicate: Union[Argument, Variable, IRI],
           object: Union[Argument, Variable, IRI, Literal],
           list_expander: LiteralType["cross", "zipMin", "zipMax"] = None) -> Instance:
    """
    Instantiate the ottr:Triple-template.
    :param subject: The predicate of the triple.
    :param predicate: The predicate of the triple.
    :param object: The object of the triple.
    :param list_expander: (How) should we do list-expansion?
    :return:
    """

def a() -> IRI:
    """
    :return: IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
    """
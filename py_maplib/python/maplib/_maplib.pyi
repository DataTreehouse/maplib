from pathlib import Path
from typing import Union, List

from polars import DataFrame

class QueryResult:
    """
    Class representing the result of SPARQL queries.
    Only constructed by maplib.
    Fields:
    df: DataFrame with result
    types: Dict where each key is a column name and each value is the name of the type of that column. 
    """

class ValidationReport:
    """
    SHACL Validation report.
    Only constructed by maplib.
    Fields:
    df: Report with constraints that were not met.
    conforms: True if no violations were found.
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
    def __init__(self, documents:Union[str,List[str]]=None, caching_folder:str=None) -> Mapping: ...

    def expand(self, template:str, df:DataFrame=None, unique_subset:List[str]=None) -> None:
        """
        Expand a template using a DataFrame 
        Usage:

        >>> m.expand("ex:ExampleTemplate", df)
        ... m.expand("ex:ExampleTemplate", df, unique_subsets=["MyValue"])

        If the template has no arguments, the df argument is not necessary.

        :param template: String identifying the template: prefix:name or full IRI 
        :param df: DataFrame where the columns have the same names as the template arguments
        :param unique_subset: DataFrame column names known to be unique e.g. ["colA", "colB"], for a performance boost (reduce costly deduplication)
        """

    def expand_default(self, df:DataFrame, primary_key_column:str, template_prefix:str=None, predicate_uri_prefix:str=None) -> str:
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

    def query(self, query:str) -> QueryResult:
        """
        Query the contained knowledge graph using SPARQL
        Currently, SELECT, CONSTRUCT and INSERT are supported.
        Usage:

        >>> res = mapping.query('''
        ... PREFIX ex:<http://example.net/ns#>
        ... SELECT ?obj1 ?obj2 WHERE {
        ...    ?obj1 ex:hasObj ?obj2
        ... }''')
        ... print(res.df)
        ... print(res.types)

        :param query: The SPARQL query string
        :return: QueryResult containing a DataFrame (types.df) and the types of each column (res.types) 
        """
    
    def validate(self) -> ValidationReport:
        """
        Validate the contained knowledge graph using SHACL
        Assumes that the contained knowledge graph also contains SHACL Shapes.

        :return: Validation report containing a report (report.df) and whether the graph conforms (report.conforms)
        """


    def read_triples(self, file_path:Union[str,Path], transient:bool=False) -> None:
        """
        Reads triples from a file path.
        File format is derived using file extension, e.g. filename.ttl or filename.nt.
        Specify transient if you only want the triples to be available for further querying and validation,
        but not persisted using write-methods.

        Usage:

        >>> m.read_triples("my_triples.ttl", transient=True)

        :param file_path: The path of the file containing triples
        :param transient: Should these triples be included when writing the graph to the file system?
        """

    def write_ntriples(self, file_path:Union[str,Path]) -> None:
        """
        Write the non-transient triples to the file path specified in the NTriples format.

        Usage:

        >>> m.write_ntriples("my_triples.nt")

        :param file_path: The path of the file containing triples
        """

    def write_native_parquet(self, folder_path:Union[str, Path]) -> None:
        """
        Write non-transient triples using the internal native Parquet format.

        Usage:

        >>> m.write_native_parquet("output_folder")

        :param folder_path: The path of the folder to write triples in the native format.
        """
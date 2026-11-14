from datetime import datetime, date
from pathlib import Path
from typing import Any, Union, List, Dict, Optional, Callable, Iterable, Literal as LiteralType, Tuple, BinaryIO, TextIO

import polars as pl
from maplib.maplib import rdf

DEFAULT_GRAPH: str
"""
The IRI that stands for the default graph wherever a graph IRI is given, e.g. in a list of graphs to write:
m.write("out.jsonld", format="json-ld", graph=[DEFAULT_GRAPH, "http://example.com/g1"]).
"""

TRANSIENT_GRAPH: str
"""
The IRI of the reserved graph for triples that should be available for querying, validation and reasoning,
but not written: map, read or insert into this graph, and include it in queries with include_graphs.
Queries, updates, inserts and inference include it by default.
"""
from polars import DataFrame, LazyFrame

a = rdf.type


class xsd:
    """
    The xsd namespace: http://www.w3.org/2001/XMLSchema#
    """
    NCName: "IRI"
    NMTOKEN: "IRI"
    Name: "IRI"
    anyURI: "IRI"
    base64Binary: "IRI"
    boolean: "IRI"
    byte: "IRI"
    date: "IRI"
    dateTime: "IRI"
    dateTimeStamp: "IRI"
    dayTimeDuration: "IRI"
    decimal: "IRI"
    double: "IRI"
    duration: "IRI"
    float: "IRI"
    gDay: "IRI"
    gMonth: "IRI"
    gMonthDay: "IRI"
    gYear: "IRI"
    gYearMonth: "IRI"
    hexBinary: "IRI"
    int_: "IRI"
    integer: "IRI"
    language: "IRI"
    long: "IRI"
    negativeInteger: "IRI"
    nonNegativeInteger: "IRI"
    nonPositiveInteger: "IRI"
    normalizedString: "IRI"
    short: "IRI"
    string: "IRI"
    token: "IRI"
    unsignedByte: "IRI"
    unsignedInt: "IRI"
    unsignedLong: "IRI"
    unsignedShort: "IRI"
    yearMonthDuration: "IRI"


class rdf:
    """
    The rdf namespace: http://www.w3.org/1999/02/22-rdf-syntax-ns#
    """
    Alt: "IRI"
    Bag: "IRI"
    HTML: "IRI"
    List: "IRI"
    Property: "IRI"
    Seq: "IRI"
    Statement: "IRI"
    XMLLiteral: "IRI"
    first: "IRI"
    langString: "IRI"
    nil: "IRI"
    object: "IRI"
    predicate: "IRI"
    rest: "IRI"
    subject: "IRI"
    type: "IRI"
    value: "IRI"


class rdfs:
    """
    The rdfs namespace: http://www.w3.org/1999/02/22-rdf-syntax-ns#
    """
    Class: "IRI"
    Container: "IRI"
    ContainerMembershipProperty: "IRI"
    Datatype: "IRI"
    Literal: "IRI"
    Resource: "IRI"
    comment: "IRI"
    domain: "IRI"
    isDefinedBy: "IRI"
    label: "IRI"
    member: "IRI"
    range: "IRI"
    seeAlso: "IRI"
    subClassOf: "IRI"
    subPropertyOf: "IRI"


class owl:
    """
    The owl namespace: http://www.w3.org/2002/07/owl#
    """
    AllDifferent: "IRI"
    AllDisjointClasses: "IRI"
    AllDisjointProperties: "IRI"
    Annotation: "IRI"
    AnnotationProperty: "IRI"
    Axiom: "IRI"
    Class: "IRI"
    DataRange: "IRI"
    DatatypeProperty: "IRI"
    DeprecatedClass: "IRI"
    DeprecatedProperty: "IRI"
    FunctionalProperty: "IRI"
    InverseFunctionalProperty: "IRI"
    IrreflexiveProperty: "IRI"
    NamedIndividual: "IRI"
    NegativePropertyAssertion: "IRI"
    Nothing: "IRI"
    ObjectProperty: "IRI"
    Ontology: "IRI"
    OntologyProperty: "IRI"
    ReflexiveProperty: "IRI"
    Restriction: "IRI"
    SymmetricProperty: "IRI"
    Thing: "IRI"
    TransitiveProperty: "IRI"
    allValuesFrom: "IRI"
    annotatedProperty: "IRI"
    annotatedSource: "IRI"
    annotatedTarget: "IRI"
    assertionProperty: "IRI"
    backwardCompatibleWith: "IRI"
    bottomDataProperty: "IRI"
    bottomObjectProperty: "IRI"
    cardinality: "IRI"
    complementOf: "IRI"
    datatypeComplementOf: "IRI"
    deprecated: "IRI"
    differentFrom: "IRI"
    disjointUnionOf: "IRI"
    disjointWith: "IRI"
    distinctMembers: "IRI"
    equivalentClass: "IRI"
    equivalentProperty: "IRI"
    hasKey: "IRI"
    hasSelf: "IRI"
    hasValue: "IRI"
    imports: "IRI"
    incompatibleWith: "IRI"
    intersectionOf: "IRI"
    inverseOf: "IRI"
    maxCardinality: "IRI"
    maxQualifiedCardinality: "IRI"
    members: "IRI"
    minCardinality: "IRI"
    minQualifiedCardinality: "IRI"
    onClass: "IRI"
    onDataRange: "IRI"
    onDatatype: "IRI"
    onProperties: "IRI"
    onProperty: "IRI"
    oneOf: "IRI"
    priorVersion: "IRI"
    propertyChainAxiom: "IRI"
    propertyDisjointWith: "IRI"
    qualifiedCardinality: "IRI"
    rational: "IRI"
    real: "IRI"
    sameAs: "IRI"
    someValuesFrom: "IRI"
    sourceIndividual: "IRI"
    targetIndividual: "IRI"
    targetValue: "IRI"
    topDataProperty: "IRI"
    topObjectProperty: "IRI"
    unionOf: "IRI"
    versionIRI: "IRI"
    versionInfo: "IRI"
    withRestrictions: "IRI"


class RDFType:
    """
    The type of a column containing a RDF variable.
    For instance, IRIs are RDFType.IRI and a string literal is RDFType.Literal("http://www.w3.org/2001/XMLSchema#string")
    """

    IRI: "RDFType"
    BlankNode: "RDFType"
    Literal: Callable[[Union[str, "IRI"]], "RDFType"]
    Multi: Callable[[List["RDFType"]], "RDFType"]
    Nested: Callable[["RDFType"], "RDFType"]
    Unknown: "RDFType"


class SolutionMappings:
    """
    Detailed information about the solution mappings, the types of the variables and debugging for queries.
    """

    mappings: DataFrame
    rdf_types: Dict[str, RDFType]
    debug: Optional[str]
    pushdown_paths: List[List[str]]

    def __init__(self, mappings: DataFrame, rdf_types: Dict[str, RDFType]):
        """
        Create new SolutionMappings object corresponding to solution mappings for variables in e.g. a query.
        :param mappings: A DataFrame
        :param rdf_types: For each column (variable), the RDFType of the variable.
        """
        ...


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

    def __init__(self, iri, prefix_name=None):
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
    datatype: IRI
    "The datatype IRI (xsd:string for plain literals, rdf:langString for language-tagged literals)."
    language: Optional[str]

    def __init__(self, value: str, data_type: "IRI" = None, language: str = None):
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
    variable: Optional[Variable]
    "The variable, if the term is a variable."

    def __init__(
            self,
            term: Union["Argument", Variable, IRI, Literal, BlankNode, None],
            list_expand: Optional[bool] = False,
    ):
        """
        An argument for a template instance.
        :param term: The term. None means the argument is unbound (ottr:none). An Argument is returned as-is.
        :param list_expand: Should the argument be expanded? Used with the list_expander argument of instance.
        """


class Instance:
    iri: str
    "The IRI of the instantiated template."

    def __init__(
            self,
            iri: "IRI",
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
    iri: IRI
    parameters: List[Parameter]
    instances: List[Instance]
    """
    An OTTR Template.
    Note that accessing parameters- or instances-fields returns copies.
    To change these fields, you must assign new lists of parameters or instances.
    """

    def __init__(
            self,
            iri: "IRI",
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
            arguments: List[Union[Argument, Variable, IRI, Literal, BlankNode, None]],
            list_expander: Optional[LiteralType["cross", "zipMin", "zipMax"]] = None,
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


# END COMMON WITH CHRONTEXT

class IndexingOptions:
    """
    Options for indexing
    """

    def __init__(
            self,
            object_sort_all: bool = None,
            object_sort_some: List["IRI"] = None,
            fts: bool = None,
            fts_path: str = None,
            geo: bool = None,
    ):
        """
        Defaults to indexing on subjects and objects for select types (e.g. rdf:type and rdfs:label)

        :param object_sort_all: Enable object-indexing for all suitable predicates (doubles memory requirement).
        :param object_sort_some: Enable object-indexing for a selected list of predicates.
        :param fts: Enable full text search. The index is stored with the categorical maps (strings): on disk
                    for the hybrid and disk storage modes, in memory otherwise.
        :param fts_path: Deprecated and ignored, except that it enables full text search.
        :param geo: Enable the GeoSPARQL index (maplib EE). Geometry literals (geo:wktLiteral,
                    geo:wkbLiteral, geo:geoJSONLiteral) are stored with a spatial index, and GeoSPARQL
                    filters use it. GeoSPARQL functions also work without the index, but slower.
        """


ParametersType = Dict[str, "SolutionMappings"]


class ValidationReport:
    """
    SHACL Validation report.
    Only constructed by maplib.
    """

    conforms: Optional[bool]
    "Whether or not the validation report conforms to the shapes (None if not determined)"

    shape_targets: DataFrame
    "A DataFrame containing the counts of the targets of each shape and constraint"

    performance: DataFrame
    "Performance statistics for the validation process"

    report_graph: Optional[str]
    "The named graph where the validation report is stored"

    rule_log: str
    "The log of SHACL rules execution"

    def results(
            self,
    ) -> Optional[DataFrame]:
        """
        Return the results of the validation report, if they exist.

        :return: The SHACL validation report, as a DataFrame
        """

    def details(
            self,
    ) -> Optional[DataFrame]:
        """
        Returns the details of the validation report.
        Only available if validation was called with include_details=True.

        :return: Details of the SHACL validation report, as a DataFrame
        """



class ModelStream:
    """
    A lazy pipeline over batched mapping input, created with stream=True in Model.map, Model.map_triples,
    Model.map_json and Model.map_xml. Each step returns a new ModelStream, and nothing
    runs until collect() is called.

    When collected, each batch of the input (a DataFrame of an iterable or LazyFrame, or documents_per_batch
    JSON/XML documents) is mapped into a fresh in-memory model with the templates, prefixes and UDFs of the
    Model. The steps then run on that batch model in the order they were added, each write step appends the
    batch to its file, and the batch model is dropped. Memory use depends on the batch size, not on the size
    of the input. The triples of the Model itself are not changed.

    Queries, updates and rules only see the triples of the current batch, so results that depend on
    triples in other batches (such as joins across batches) are not found.

    Usage:

    >>> m = Model()
    ... m.add_template(doc)
    ... (
    ...     m.map("ex:ExampleTemplate", pl.scan_parquet("big.parquet"), stream=True)
    ...     .update(update_query)
    ...     .insert(construct_query)
    ...     .write("out.nt")
    ...     .write("out.ttl", format="turtle")
    ...     .collect()
    ... )

    Mapping documents into the transient graph and writing only what is constructed from them:

    >>> from maplib import TRANSIENT_GRAPH
    ... with open("records.jsonl") as lines:
    ...     m.map_json(lines, graph=TRANSIENT_GRAPH, stream=True, documents_per_batch=1000) \
    ...         .insert(construct_query).write("out.nt").collect()
    """

    def update(
            self,
            update: str,
            parameters: ParametersType = None,
            graph: str = None,
            include_graphs: Optional[List[str]] = None,
            max_rows: int = None,
            debug: bool = False,
    ) -> "ModelStream":
        """
        Add a SPARQL Update step, see Model.update.
        """

    def insert(
            self,
            query: str,
            parameters: ParametersType = None,
            source_graph: str = None,
            target_graph: str = None,
            include_graphs: Optional[List[str]] = None,
            max_rows: int = None,
            debug: bool = False,
    ) -> "ModelStream":
        """
        Add a step that inserts the results of a Construct query into the batch, see Model.insert.
        """

    def infer(
            self,
            rulesets: Union[str, List[str]],
            graph: str = None,
            max_iterations: Optional[int] = 100_000,
            include_graphs: Optional[List[str]] = None,
            max_rows: Optional[int] = 100_000_000,
            debug: bool = False,
    ) -> "ModelStream":
        """
        Add a Datalog inference step, see Model.infer.
        """

    def write(
            self,
            file_path: Union[str, Path, BinaryIO],
            format: LiteralType["ntriples", "turtle", "nquads"] = None,
            graph: str = None,
            prefixes: Dict[str, str] = None,
    ) -> "ModelStream":
        """
        Add a step that appends the triples of each batch to a file or a binary file-like object. The file is created
        (or truncated) when the stream is collected. Turtle prefixes are declared once at the start of the file.

        :param file_path: The path of the file, or a binary file-like object (anything with a write method that
            accepts bytes, such as io.BytesIO or a file opened with "wb"). The object is flushed but not closed.
        :param format: One of "ntriples" (the default), "turtle" or "nquads".
        :param graph: The IRI of the graph to write. For "nquads", all graphs except the transient graph are written
            when this is None.
        :param prefixes: The prefixes that will be used in turtle serialization.
        """

    def collect(self) -> None:
        """
        Run the stream: map each batch, run the steps on it and write it. Iterators given as input are consumed.
        """


class Model:
    """
    A model session allowing:

    * Iterative model using OTTR templates
    * Interactive SPARQL querying and enrichment
    * SHACL validation

    Usage:

    >>> from maplib import Model
    ... doc = '''
    ... @prefix ex:<http://example.net/ns#>.
    ... ex:ExampleTemplate [?MyValue] :: {
    ...    ottr:Triple(ex:myObject, ex:hasValue, ?MyValue)
    ... } .'''
    ... m = Model()
    ... m.add_template(doc)

    Storage modes:

    * ``"memory"``: categorical maps (strings) and triple tables are kept in memory.
    * ``"hybrid"``: categorical maps are stored on disk, triple tables in memory.
    * ``"disk"``: categorical maps are stored on disk, and triple tables as parquet files on disk.

    >>> m = Model(storage="disk", storage_folder="maplib_disk")

    Object storage: read, read_template, write, write_cim_xml, serialize and deserialize also take URLs of
    Google Cloud Storage (gs://bucket/path), Azure Blob Storage (az://container/path, abfss://container@account.dfs.core.windows.net/path
    or https://account.blob.core.windows.net/container/path), Amazon S3 (s3://bucket/path), HTTP(S) and file://.
    Credentials are found as the cloud SDKs find them: from the environment (e.g. GOOGLE_APPLICATION_CREDENTIALS,
    AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY or AZURE_STORAGE_SAS_KEY, AWS_ACCESS_KEY_ID), and on cloud
    machines from the metadata server or managed identity. They can be given with storage_options, using the option names of
    Polars' storage_options (the object_store crate), e.g. {"google_service_account": "/path/key.json"} or
    {"azure_storage_account_name": "acc", "azure_storage_use_emulator": "true"}. Files that are read are downloaded to the
    system temporary folder first (set TMPDIR to change it), and files that are written are uploaded as they are written;
    the object appears when the write completes.

    >>> m.read("gs://my-bucket/data/triples.ttl")
    >>> m.write("az://my-container/out/triples.nt")

    :param indexing_options: options for indexing
    :param storage_folder: folder for on-disk storage, required for the hybrid and disk storage modes
    :param storage: "memory", "hybrid" or "disk". Defaults to "hybrid" when a storage folder is given,
                    and "memory" otherwise.
    """

    def __init__(
            self,
            indexing_options: "IndexingOptions" = None,
            storage_folder: Union[Path, str] = None,
            storage: LiteralType["memory", "hybrid", "disk"] = None,
    ) -> "Model": ...

    def serialize(self, path: Union[Path, str] = "./model.maplib", raw_db: bool = False,
                  single_file: bool = True, storage_options: Dict[str, str] = None):
        """
        Serialization of named graphs in the Model object.
        The triple tables and the dictionaries of IRIs and literals are written as parquet files,
        by default bundled into a single file.

        Usage:
        >>> m.read("triples.nt")
        >>> m.serialize("model.maplib")

        :param path: The path of the file (or with single_file=False, the folder) to write. Any file name or
                     extension can be used, .maplib is the convention.
        :param raw_db: Write the dictionaries as DuckDB databases instead of parquet files (maplib Enterprise Edition).
                       The files are larger and slower to write, but deserializing with hybrid or disk storage uses
                       them in place instead of building the databases, which is much faster.
        :param single_file: Write a single file. With False, a folder of parquet files (and DuckDB databases)
                            is written instead.
        :param storage_options: Options for object storage URLs, such as credentials, see the Model class documentation.

        The path can be a URL of object storage, see the Model class documentation, e.g. gs://bucket/models/model.maplib.
        A single file is written locally (next to the storage folder of the model, or in the system temporary folder)
        and uploaded; the object appears whole when the upload is complete. With single_file=False, the folder is written
        to a new generation folder under the URL (gen-<time>-<id>/), which is then published by replacing the object
        LATEST under the URL with its name. Readers see either the previous or the new model, never a mix. If another
        writer published a model to the URL after this serialization started, it fails instead of replacing it. The
        current and the previous generation are kept, older ones are deleted.

        >>> m.serialize("gs://my-bucket/models/model", single_file=False, raw_db=True)
        """

    @staticmethod
    def deserialize(path: Union[Path, str] = "./model.maplib",
                    storage_folder: Union[Path, str] = None,
                    storage: LiteralType["memory", "hybrid", "disk"] = None,
                    storage_options: Dict[str, str] = None,
                    cache_size: int = None) -> "Model":
        """
        Deserialization of serialized graphs

        Usage:
        >>> m = Model.deserialize("model.maplib", storage_folder="maplib_disk", storage="disk")

        :param path: The path to the file or folder written by Model.serialize, either kind is detected.
        :param storage_folder: The target path and folder name where the deserialized triples should be stored
        :param storage: "memory", "hybrid" or "disk", see Model. Defaults to "hybrid" when a storage folder
                        is given, and "memory" otherwise.
        :param storage_options: Options for object storage URLs, such as credentials, see the Model class documentation.
        :param cache_size: The most bytes of triple tables downloaded from object storage to keep in storage folders
                           with disk storage, for all models of the process. 0 keeps them all, and None leaves the
                           limit as it is (by default no limit, or the environment variable MAPLIB_REMOTE_CACHE_BYTES).
        :return: A model.

        The path can be a URL of object storage, see the Model class documentation: a single file, which is downloaded,
        or a folder written by serialize with single_file=False. With storage="disk", the triple tables of a folder are not
        downloaded when it is deserialized, but when they are first read, into the storage folder. With a cache_size, the
        least recently read tables are removed from the storage folder when the downloaded tables take more space, and
        downloaded again when they are read. Tables read in the last five minutes are kept, so the cache can be larger
        than cache_size for a while. Triples added to the model are kept locally until it is serialized again. The
        dictionaries of IRIs and literals are downloaded when deserializing; serialize with raw_db=True so that they are
        used as they are with hybrid and disk storage.

        >>> m = Model.deserialize("gs://my-bucket/models/model", storage="disk",
        ...                       storage_folder="/mnt/nvme/maplib", cache_size=200 * 2**30)
        """

    def add_template(self, template: Union["Template", str]):
        """
        Add a template to the model. Overwrites any existing template with the same IRI.
        :param template: The template to add, as a stOTTR string or as a programmatically constructed Template.
        :return:
        """

    def get_templates(self) -> List["Template"]:
        """
        Return the OTTR templates currently held by the model (whether added as stOTTR or
        programmatically). The built-in ``ottr:Triple`` primitive is not included.

        Usage:
        >>> for t in m.get_templates():
        ...     print(t)

        :return: A list of Template objects.
        """

    def templates_to_graph(self, graph: str = None) -> None:
        """
        Materialize the model's OTTR templates into a named graph as RDF, using the flattened
        maplib template vocabulary (prefix ``maplib``, base
        ``https://datatreehouse.github.io/maplib/vocab#``). This lets template structure and
        the interconnectedness of IRIs across templates be inspected with ordinary SPARQL, and
        used to derive SHACL shapes. The triples are added alongside any existing content of
        the target graph (the graph is not replaced).

        Usage:
        >>> m.templates_to_graph("https://example.org/templates")
        >>> m.query('''
        ... PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#>
        ... SELECT ?template ?iri WHERE {
        ...     GRAPH <https://example.org/templates> { ?template maplib:referencesIri ?iri }
        ... }''')

        :param graph: The IRI of the graph to add the template triples to. Defaults to the default graph.
        """

    def add_prefixes(self, prefixes: Dict[str, str]):
        """
        Add prefixes that will be used in parsing of SPARQL, Datalog and OTTR.

        Usage:
        >>> m.add_prefixes({"ex" : "http:://example.net/"})

        :param prefixes: Known prefixes
        :return:
        """

    def map(
            self,
            template: Union[str, "Template", IRI],
            data: Union[DataFrame, LazyFrame, "SolutionMappings", Iterable[Union[DataFrame, LazyFrame, "SolutionMappings"]]] = None,
            graph: str = None,
            validate_iris: bool = True,
            stream: bool = False,
    ) -> Optional["ModelStream"]:
        """
        Map a template using a DataFrame
        Usage:

        >>> m.map("ex:ExampleTemplate", df)

        If the template has no arguments, the df argument is not necessary.

        :param template: Template, IRI, IRI string or prefixed template name.
        :param data: DataFrame where the columns have the same names as the template arguments (when piping the output of queries back in, use SolutionMappings).
            May also be a LazyFrame, which is consumed in a single pass using collect_batches(), or an iterable of DataFrames, LazyFrames or SolutionMappings, which are mapped batch by batch.
        :param graph: The IRI of the graph to add triples to.
        :param validate_iris: Validate any IRI-columns.
        :param stream: Return a ModelStream instead of mapping into this Model. Steps added to the stream
            (update, insert, infer, write) run batch by batch when it is collected, see ModelStream.
        :return: A ModelStream when stream is set, otherwise None.
        """

    def map_json(
            self,
            path_or_string: Union[Path, str, Iterable[Union[Path, str]]],
            graph: str = None,
            uuid_namespace: str = None,
            stream: bool = False,
            documents_per_batch: int = 1,
    ) -> Optional["ModelStream"]:
        """
        Map a JSON file or string to triples.
        Usage:

        >>> m.map_json("my_doc.json")

        or:

        >>> m.map_json('{"my_key":[true, "abc"]}')

        :param path_or_string: Path to a JSON document or a JSON string, or an iterable of them (e.g. a generator
            of JSON strings), which are mapped one document at a time.
        :param graph: The IRI of the graph to add triples to. None is the default graph.
        :param uuid_namespace: Generates uuid v5 uris based on this namespace.
            With several documents, document number i uses the namespace "<uuid_namespace>/<i>",
            so that the documents do not get the same IRIs.
        :param stream: Return a ModelStream instead of mapping into this Model. Steps added to the stream
            (update, insert, infer, write) run batch by batch when it is collected, see ModelStream.
        :param documents_per_batch: With stream, the number of documents mapped into each batch.
        :return: A ModelStream when stream is set, otherwise None.
        """

    def map_xml(
            self,
            path_or_string: Union[Path, str, Iterable[Union[Path, str]]],
            graph: str = None,
            uuid_namespace: str = None,
            stream: bool = False,
            documents_per_batch: int = 1,
    ) -> Optional["ModelStream"]:
        """
        Map an XML file or string to triples.
        Usage:

        >>> m.map_xml("my_doc.xml")

        or:

        >>> m.map_xml('<root><child>value</child></root>')

        :param path_or_string: Path to a XML document or a XML string, or an iterable of them (e.g. a generator
            of XML strings), which are mapped one document at a time.
        :param graph: The IRI of the graph to add triples to. None is the default graph.
        :param uuid_namespace: Generates uuid v5 uris based on this namespace.
            With several documents, document number i uses the namespace "<uuid_namespace>/<i>",
            so that the documents do not get the same IRIs.
        :param stream: Return a ModelStream instead of mapping into this Model. Steps added to the stream
            (update, insert, infer, write) run batch by batch when it is collected, see ModelStream.
        :param documents_per_batch: With stream, the number of documents mapped into each batch.
        :return: A ModelStream when stream is set, otherwise None.
        """

    def map_triples(
            self,
            data: Union[DataFrame, LazyFrame, "SolutionMappings", Iterable[Union[DataFrame, LazyFrame, "SolutionMappings"]]],
            predicate: Union[str, IRI] = None,
            graph: str = None,
            validate_iris: bool = True,
            stream: bool = False,
    ) -> Optional["ModelStream"]:
        """
        Map a template using a DataFrame with columns subject, object and predicate
        The predicate column can also be supplied as a string if it is the same for all rows.
        Usage:

        >>> m.map_triples(df)

        :param data: DataFrame where the columns are named subject and object. May also contain a predicate-column. When piping the output of queries back in, use SolutionMappings.
            May also be a LazyFrame, which is consumed in a single pass using collect_batches(), or an iterable of DataFrames, LazyFrames or SolutionMappings, which are mapped batch by batch.
        :param predicate: The IRI of the predicate, used when data has no predicate-column.
        :param graph: The IRI of the graph to add triples to.
        :param validate_iris: Validate any IRI-columns.
        :param stream: Return a ModelStream instead of mapping into this Model. Steps added to the stream
            (update, insert, infer, write) run batch by batch when it is collected, see ModelStream.
        :return: A ModelStream when stream is set, otherwise None.
        """

    def generate_template(
            self,
            data: Union[DataFrame, "SolutionMappings"],
            primary_key_column: str,
    ) -> str:
        """
        Generate a default template from a DataFrame. This is always a dry run: nothing is mapped and the
        template is not added to the Model. The primary key column becomes the subject of one ottr:Triple per
        other column, with predicates in the urn:maplib_default: namespace. A parameter is optional when its
        column contains nulls. Map the data with the template using Model.map.
        Usage:

        >>> template_string = m.generate_template(df, "myKeyCol")
        ... print(template_string)
        ... m.map(template_string, df)

        :param data: DataFrame where the columns have the same names as the template arguments (when piping the output of queries back in, use SolutionMappings).
            To generate a template for a LazyFrame or batched input, pass a representative DataFrame, e.g. the first batch.
        :param primary_key_column: This column will be the subject of all triples in the generated template.
            It should be a String column of IRIs for the template to be mapped.
        :return: The generated template as a stOTTR string.
        """

    def map_csvw(
            self,
            source: Union[str, Path],
            metadata: Union[str, Path] = None,
            graph: str = None,
            minimal: bool = False,
            iri_mappings: Dict[str, Union[str, Path]] = None,
    ) -> str:
        """
        Convert tabular data (CSV, TSV, ...) to RDF using CSV on the Web (CSVW) metadata, see
        https://www.w3.org/TR/tabular-data-primer/ and https://www.w3.org/TR/csv2rdf/.

        The tables are first added to temporary graphs using Facade-X (as Model.map_df does),
        then a SPARQL Update generated from the metadata creates the RDF and drops the temporary graphs.
        The update is returned, so that it can be inspected or adapted for a custom mapping.

        Usage:

        >>> m.map_csvw("tree-ops.csv-metadata.json")

        or, with a CSV file, locating the metadata as described in the specification
        (``tree-ops.csv-metadata.json`` or ``csv-metadata.json`` in the same folder) or else using the header row:

        >>> m.map_csvw("tree-ops.csv")

        Documents are never fetched over the network. ``file:`` IRIs are read directly, and other IRIs
        are read from local folders given by iri_mappings:

        >>> m.map_csvw("https://example.org/data/metadata.json",
        ...            iri_mappings={"https://example.org/data/": "local_copy_of_data"})

        Warnings (e.g. cells that do not match their datatype) are issued using the Python warnings module.

        :param source: A path or IRI of a metadata document (JSON) or a tabular data file.
        :param metadata: A path or IRI of user supplied metadata, which overrides any other metadata.
        :param graph: The IRI of the graph to add triples to. None is the default graph.
        :param minimal: Use minimal mode, which only outputs the triples describing the cells.
            Otherwise, the standard mode also describes the group of tables, the tables and the rows.
        :param iri_mappings: Maps IRI prefixes to local folders. Local paths below such a folder get IRIs with the prefix,
            which are used as the base of relative URLs in the metadata.
        :return: The SPARQL Update that was used.
        """

    def map_df(
            self,
            df: DataFrame,
            graph: str = None,
            uuid_namespace: str = None,
    ):
        """
        Create a default template and map it based on a dataframe.
        Usage:

        >>> df = pl.read_csv("my_csv.csv")
        >>> m.map_df(df)

        :param df: DataFrame to map using Facade-X (using approximately the CSV-mapping)
        :param graph: The IRI of the graph to add triples to.
        :param uuid_namespace: Generates uuid v5 uris based on this namespace.
        :return: None
        """

    def map_opc_ua(
            self,
            folder: Union[str, Path],
            graph: str = None,
    ):
        """
        Map a folder of OPC UA NodeSet2XMLs to RDF.
        This folder MUST contain the Namespace 0 XML to work properly.

        Usage:
        >>> m.map_opc_ua("my_opc_ua_nodeset2_xmls", graph="urn:maplib:uagraphs")

        :param folder: OPC UA NodeSet2 XMLs are found in this folder
        :param graph: The IRI of the graph to add triples to.
        :return: None
        """

    def explore(
            self,
            host: str = "localhost",
            port: int = 8000,
            bind: str = "localhost",
            popup=False,
            fts=True,
            fts_path: str = None,
            root_path: str = "/",
            graph: str = None,
            page: str = None,
            frontend: Union[str, Path] = None,
            flight_port: int = None,
            cors_origins: List[str] = None,
    ) -> "ExploreServer":
        """Explores the model in the Model portal, served by Model.serve.

        Model.serve hosts both the SPARQL endpoint (/sparql) that every page of the portal queries
        and the portal itself (/ui/).
        >>> server = m.explore()
        >>> server.url         # the portal, open it in a browser
        >>> server.sparql_url  # the SPARQL endpoint, e.g. Model().query(..., endpoint=server.sparql_url)
        >>> server.stop()

        :param host: The host name in the URLs (the server listens on bind).
        :param port: The port of the server, 0 for any free port.
        :param bind: Bind to the following host / ip.
        :param popup: Pop up the browser window (deprecated, open server.url yourself instead).
        :param fts: Build the full text search index used by the search boxes.
        :param fts_path: Deprecated and ignored, the index needs no path.
        :param root_path: The path the server is reached under behind a proxy (JupyterHub is detected).
        :param graph: The named graph to explore, defaults to the default graph.
        :param page: Deprecated and ignored, the portal replaces the old explorer pages.
        :param frontend: A folder with a portal build (its index.html). Defaults to the
            MAPLIB_EXPLORE_FRONTEND environment variable, then the portal packaged with maplib.
        :param flight_port: Also serve Arrow Flight on this port.
        :param cors_origins: Browser origins allowed to query, e.g. a portal dev server.
        """

    def query(
            self,
            query: str,
            parameters: ParametersType = None,
            solution_mappings: bool = False,
            graph: str = None,
            return_json: bool = False,
            return_format: LiteralType["turtle", "json", "dataframe"] = "dataframe",
            include_graphs: Optional[List[str]] = None,
            max_rows: int = None,
            debug: bool = False,
            bindings: Dict[str, Union[str, IRI, Literal]] = None,
            endpoint: Optional[str] = None,
            endpoint_token: Optional[str] = None,
    ) -> DataFrame | SolutionMappings | list[DataFrame | SolutionMappings] | bool | str | None:
        """
        Query the contained knowledge graph using SPARQL
        Currently, SELECT, CONSTRUCT and ASK are supported.
        Usage:

        >>> df = model.query('''
        ... PREFIX ex:<http://example.net/ns#>
        ... SELECT ?obj1 ?obj2 WHERE {
        ...    ?obj1 ex:hasObj ?obj2
        ... }''')
        ... print(df)

        :param query: The SPARQL query string
        :param parameters: PVALUES Parameters, for each parameter, the SolutionMappings containing corresponding mappings and types.
        :param solution_mappings: Returns SolutionMappings with maplib-native formatting and with RDF typing. Useful for round-trips.
        :param graph: The IRI of the graph to query.
        :param return_json: Return JSON string.
        :param return_format: One of "dataframe" (default), "json" (SELECT and ASK only) or "turtle" (CONSTRUCT only). Ignored when return_json is set.
        :param include_graphs: IRIs of graphs whose triples are merged into the queried graph, as with FROM.
            None includes the transient graph (TRANSIENT_GRAPH), use [] to query only the graph itself.
        :param max_rows: Maximum estimated rows in result, helps avoid out-of-memory errors.
        :param debug: Why does my query have no results?
        :param bindings: Bind the variables (keys) to the following ground terms. Use IRI, Literal classes or strings (e.g. "<urn:id:abc>") as values.
        :param endpoint: Query a maplib server started with Model.serve() instead of this Model, e.g. server.url or
                         "http://host:7878". The results are the same as from Model.query() on the served Model: they
                         are sent as Arrow over plain HTTP, with maplib's own types, and zstd compressed when the
                         server is on another host. parameters and debug can not be used with an endpoint.
        :param endpoint_token: The auth_token of the server, if it has one.
        :return: DataFrame (Select), list of DataFrames (Construct) containing results or SolutionMappings when solution_mappings is set, or a bool (Ask).

        """

    def query_external(
            self,
            query: str,
            endpoint: str,
            method: LiteralType["GET", "POST"] = "GET",
            solution_mappings: bool = False,
            return_json: bool = False,
            return_format: LiteralType["turtle", "json", "dataframe"] = "dataframe",
            bindings: Dict[str, Union[str, IRI, Literal]] = None,
            headers: List[Tuple[str, str]] = None
    ) -> DataFrame | SolutionMappings | list[DataFrame | SolutionMappings] | str | None:
        """
        Query an external endpoint using SPARQL
        Currently SELECT is supported. Uses GET unless method is set to POST.
        Usage:

        >>> df = model.query_external('''
        ... PREFIX ex:<http://example.net/ns#>
        ... SELECT ?obj1 ?obj2 WHERE {
        ...    ?obj1 ex:hasObj ?obj2
        ... }''',
        ... "http://localhost:8000/query",
        ... )
        ... print(df)

        :param query: The SPARQL query string
        :param endpoint: The external HTTP SPARQL Endpoint
        :param method: The HTTP Method to use
        :param solution_mappings: Returns SolutionMappings with maplib-native formatting and with RDF typing. Useful for round-trips.
        :param return_json: Return JSON string.
        :param return_format: One of "dataframe" (default), "json" (SELECT and ASK only) or "turtle" (CONSTRUCT only). Ignored when return_json is set.
        :param bindings: Bind the variables (keys) to the following ground terms. Use IRI, Literal classes or strings (e.g. "<urn:id:abc>") as values.
        :param headers: Additional headers to include in the request
        :return: DataFrame (Select), list of DataFrames (Construct) containing results or SolutionMappings when solution_mappings is set.
        """

    def update(
            self,
            update: str,
            parameters: ParametersType = None,
            graph: str = None,
            include_graphs: Optional[List[str]] = None,
            max_rows: int = None,
            debug: bool = False,
    ):
        """
        Execute a SPARQL Update (e.g. INSERT/DELETE ... WHERE) against the graph.
        Usage:

        >>> m = Model()
        ... m.add_template(doc)
        ... # Omitted
        ... update_pizzas = '''
        ... ...'''
        ... m.update(update_pizzas)

        :param update: The SPARQL Update string
        :param parameters: PVALUES Parameters, for each parameter, the SolutionMappings containing corresponding mappings and types.
        :param graph: The IRI of the graph to update.
        :param include_graphs: IRIs of graphs whose triples are merged into the queried graph, as with FROM.
            None includes the transient graph (TRANSIENT_GRAPH), use [] to query only the graph itself.
        :param max_rows: Maximum estimated rows in result, helps avoid out-of-memory errors.
        :param debug: Why does my query have no results?
        :return: None
        """

    def insert(
            self,
            query: str,
            parameters: ParametersType = None,
            solution_mappings: bool = False,
            source_graph: str = None,
            target_graph: str = None,
            include_graphs: Optional[List[str]] = None,
            max_rows: int = None,
            debug: bool = False,
    ) -> Dict[str, Union[DataFrame, "SolutionMappings"]]:
        """
        Insert the results of a Construct query in the graph.
        Useful for being able to use the same query for inspecting what will be inserted and actually inserting.
        Usage:

        >>> m = Model()
        ... m.add_template(doc)
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
        :param parameters: PVALUES Parameters, for each parameter, the SolutionMappings containing corresponding mappings and types.
        :param solution_mappings: Returns SolutionMappings with maplib-native formatting and with RDF typing. Useful for round-trips.
        :param source_graph: The IRI of the source graph to execute the construct query.
        :param target_graph: The IRI of the target graph to insert into. Use TRANSIENT_GRAPH for triples that should
            be available for querying, validation and reasoning, but not written.
        :param include_graphs: IRIs of graphs whose triples are merged into the queried graph, as with FROM.
            None includes the transient graph (TRANSIENT_GRAPH), use [] to query only the graph itself.
        :param max_rows: Maximum estimated rows in result, helps avoid out-of-memory errors.
        :param debug: Why does my query have no results?
        :return: The newly inserted triples, keyed by predicate IRI (as SolutionMappings when solution_mappings is set).
        """

    def validate(
            self,
            shape_graph: str = None,
            data_graph: str = None,
            report_graph: Union[str, None] = "",
            inferences_graph: str = None,
            include_details: bool = False,
            include_conforms: bool = False,
            only_shapes: List[str] = None,
            deactivate_shapes: List[str] = None,
            dry_run: bool = False,
            max_rows: int = None,
            serial: bool = False,
            max_iterations: Optional[int] = 100_000,
            debug_rules: bool = False,
    ) -> ValidationReport:
        """
        Validate the contained knowledge graph using SHACL
        Assumes that the contained knowledge graph also contains SHACL Shapes.

        :param shape_graph: The IRI of the Shape Graph (defaults to the default graph).
        :param data_graph: The IRI of the Data Graph (defaults to the default graph).
        :param report_graph: When this IRI (or None for default graph) is supplied, the validation report (if any) is found in this named graph.
        :param inferences_graph: If this IRI is supplied, any inference results from sh:rule can be found in this named graph.
        :param include_details: Include details of SHACL evaluation alongside the report. Currently uses a lot of memory.
        :param include_conforms: Include those results that conformed. Also applies to details.
        :param only_shapes: Validate only these shapes, None means all shapes are validated (must be IRI, cannot be used with deactivate_shapes).
        :param deactivate_shapes: Disable validation of these shapes (must be IRI, cannot be used with only_shapes).
        :param dry_run: Only find targets of shapes, but do not validate them.
        :param max_rows: Maximum estimated rows in underlying SPARQL results, helps avoid out-of-memory errors.
        :param serial: Turns off most parallell validation of shapes.
        :param max_iterations: Maximum number of iterations for SHACL rules.
        :param debug_rules: Debug why rules returning no results do so. Included in rule log.
        :return: Validation report containing shape performance details and target counts and whether the graph conforms (report.conforms)
        """

    def read(
            self,
            file_path: Union[str, Path, bytes, bytearray, memoryview, BinaryIO, TextIO],
            format: LiteralType["ntriples", "turtle", "trig", "nquads", "rdf/xml", "cim/xml", "json-ld", "hdt"] = None,
            base_iri: str = None,
            parallel: bool = None,
            checked: bool = True,
            graph: str = None,
            replace_graph: bool = False,
            triples_batch_size: int = 10_000_000,
            known_contexts: Dict[str, str] = None,
            geo: bool = False,
            strict: bool = False,
            storage_options: Dict[str, str] = None,
    ) -> None:
        """
        Reads triples from a file path, a URL of object storage (see the Model class documentation),
        bytes in memory or a file-like object.
        You can specify the format, or it will be derived using file extension, e.g. filename.ttl or filename.nt.
        For bytes and file-like objects the format must be given, except for HDT, which is detected.
        Read into graph=TRANSIENT_GRAPH if you only want the triples to be available for further querying and
        validation, but not persisted using write-methods.

        Usage:

        >>> m.read("my_triples.ttl")
        >>> m.read(hdt_bytes, format="hdt")
        >>> with open("my_triples.nt", "rb") as f:
        ...     m.read(f, format="ntriples")

        :param file_path: The path of the file containing triples, bytes (bytes, bytearray or memoryview), or a
            file-like object with a read method, which is read to the end into memory before parsing. Text streams
            are read as UTF-8.
        :param format: One of "ntriples", "turtle", "trig", "nquads", "rdf/xml", "json-ld", "cim/xml" or "hdt", otherwise it is inferred from the file extension.
        :param base_iri: Base iri
        :param parallel: Parse triples in parallel, currently only NTRiples and Turtle. Assumes all prefixes are in the beginning of the document. Defaults to true only for NTriples.
        :param checked: Check IRIs etc.
        :param graph: The IRI of the graph to read the triples into, if None, it will be the default graph (or, for TriG, N-Quads and JSON-LD, the graphs named in the document).
        :param replace_graph: Replace the graph with these triples? Will replace the default graph if no graph is specified.
        :param triples_batch_size: Read this many triples in each batch.
        :param known_contexts: Contexts in JSON-LD documents are resolved towards this dict.
        :param geo: Only for format="cim/xml". Map the cim:PositionPoint coordinates of each cim:Location in the graph to GeoSPARQL:
                    the location becomes a geo:Feature with a geo:hasGeometry / geo:hasDefaultGeometry whose geo:asWKT is a POINT,
                    LINESTRING or POLYGON (closed rings), ordered by cim:PositionPoint.sequenceNumber. EPSG:4326 coordinates
                    are written in the GeoSPARQL default CRS84 (longitude latitude), other CRSs get their CRS IRI in the WKT literal.
        :param strict: Raise an error if a literal cannot be parsed as its datatype, e.g. "abc"^^xsd:integer. By default such triples are dropped and a warning is logged.
        :param storage_options: Options for object storage URLs, such as credentials, e.g.
            {"azure_storage_account_name": ..., "azure_storage_account_key": ...}. See the Model class documentation.
        """

    def read_template(
            self,
            file_path: Union[str, Path],
            storage_options: Dict[str, str] = None,
    ) -> None:
        """
        Reads template(s) from a file path, or a URL of object storage (see the Model class documentation).

        Usage:

        >>> m.read_template("templates.ttl")

        :param file_path: The path of the file containing templates in stOTTR format
        :param storage_options: Options for object storage URLs, such as credentials, e.g.
            {"azure_storage_account_name": ..., "azure_storage_account_key": ...}. See the Model class documentation.
        """

    def reads(
            self,
            s: str,
            format: LiteralType["ntriples", "turtle", "trig", "nquads", "rdf/xml", "cim/xml", "json-ld"],
            base_iri: str = None,
            parallel: bool = None,
            checked: bool = True,
            graph: str = None,
            replace_graph: bool = False,
            triples_batch_size: int = 10_000_000,
            known_contexts: Dict[str, str] = None,
            geo: bool = False,
            strict: bool = False,
    ) -> None:
        """
        Reads triples from a string.
        Read into graph=TRANSIENT_GRAPH if you only want the triples to be available for further querying and
        validation, but not persisted using write-methods.

        Usage:

        >>> m.reads(my_ntriples_string, format="ntriples")

        :param s: String containing serialized triples.
        :param format: One of "ntriples", "turtle", "trig", "nquads", "rdf/xml", "json-ld" or "cim/xml".
        :param base_iri: Base iri
        :param parallel: Parse triples in parallel, currently only NTRiples and Turtle. Assumes all prefixes are in the beginning of the document. Defaults to true for NTriples.
        :param checked: Check IRIs etc.
        :param graph: The IRI of the graph to read the triples into. If None, the default graph (or, for TriG, N-Quads and JSON-LD, the graphs named in the document).
        :param replace_graph: Replace the graph with these triples? Will replace the default graph if no graph is specified.
        :param triples_batch_size: Number of triples to read in each batch.
        :param known_contexts: Contexts in JSON-LD documents are resolved towards this dict.
        :param geo: Only for format="cim/xml". Map the cim:PositionPoint coordinates of each cim:Location in the graph to GeoSPARQL:
                    the location becomes a geo:Feature with a geo:hasGeometry / geo:hasDefaultGeometry whose geo:asWKT is a POINT,
                    LINESTRING or POLYGON (closed rings), ordered by cim:PositionPoint.sequenceNumber. EPSG:4326 coordinates
                    are written in the GeoSPARQL default CRS84 (longitude latitude), other CRSs get their CRS IRI in the WKT literal.
        :param strict: Raise an error if a literal cannot be parsed as its datatype, e.g. "abc"^^xsd:integer. By default such triples are dropped and a warning is logged.
        """

    def write_cim_xml(
            self,
            file_path: Union[str, Path, BinaryIO],
            profile_graph: str,
            model_iri: str = None,
            version: str = None,
            description: str = None,
            created: str = None,
            scenario_time: str = None,
            modeling_authority_set: str = None,
            prefixes: Dict[str, str] = None,
            graph: str = None,
            storage_options: Dict[str, str] = None,
    ) -> None:
        """
        Write the legacy CIM XML format, to a file path or a URL of object storage (see the Model class documentation).

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

        :param file_path: The path of the file, or a binary file-like object (see Model.write).
        :param profile_graph: The IRI of the graph containing the ontology of the CIM profile to write.
        :param model_iri: model_iri a md:FullModel. Is generated if not provided.
        :param version: model_iri md:Model.version version .
        :param description: model_iri md:Model.description description .
        :param created: model_iri md:Model.created created .
        :param scenario_time: model_iri md:Model.scenarioTime scenario_time .
        :param modeling_authority_set: model_iri md:Model.modelingAuthoritySet modeling_authority_set .
        :param prefixes: Prefixes to be used in XML export.
        :param graph: The graph to write, defaults to the default graph.
        :param storage_options: Options for object storage URLs, such as credentials, e.g.
            {"azure_storage_account_name": ..., "azure_storage_account_key": ...}. See the Model class documentation.
        """

    def write(
            self,
            file_path: Union[str, Path, BinaryIO],
            format: LiteralType["ntriples", "turtle", "trig", "nquads", "rdf/xml", "json-ld", "hdt"] = None,
            graph: Union[str, List[str]] = None,
            prefixes: Dict[str, str] = None,
            storage_options: Dict[str, str] = None,
    ) -> None:
        """
        Write the triples to the file path, URL of object storage (see the Model class documentation) or
        binary file-like object specified, in the given format. An object in object storage is uploaded as
        it is written, and appears when the write completes.

        Usage:

        >>> m.write("my_triples.nt", format="ntriples")
        >>> with open("my_triples.hdt", "wb") as f:
        ...     m.write(f, format="hdt")
        >>> m.write("my_dataset.jsonld", format="json-ld", graph=[DEFAULT_GRAPH, "http://example.com/g1"])

        :param file_path: The path of the file, or a binary file-like object: anything with a write method
            that accepts bytes, such as io.BytesIO, a file opened with "wb" or a socket file. Writes to it are
            buffered, and it is flushed but not closed. Use write_bytes to get the output as bytes.
        :param format: One of "ntriples", "turtle", "trig", "nquads", "rdf/xml", "json-ld", "hdt".
            HDT is built in memory; literals with special characters are stored N-Triples-escaped, following the Rust hdt crate.
            JSON-LD is written as a compacted document with one node object per subject, using the prefixes in its @context.
        :param graph: The IRI of the graph to write, or for "json-ld", "trig" and "nquads" a list of graph IRIs to write
            as a dataset (use DEFAULT_GRAPH for the default graph); the listed graphs must exist. In JSON-LD, the default
            graph is written at the top level and the other graphs as graph objects ({"@id": ..., "@graph": [...]}),
            while a single graph is written at the top level. For "trig" and "nquads", all graphs except the transient
            graph are written when this is None.
        :param prefixes: The prefixes that will be used in turtle, RDF/XML and JSON-LD serialization.
        :param storage_options: Options for object storage URLs, such as credentials, e.g.
            {"azure_storage_account_name": ..., "azure_storage_account_key": ...}. See the Model class documentation.
        """

    def write_bytes(
            self,
            format: LiteralType["ntriples", "turtle", "trig", "nquads", "rdf/xml", "json-ld", "hdt"] = None,
            graph: Union[str, List[str]] = None,
            prefixes: Dict[str, str] = None,
    ) -> bytes:
        """
        Write the triples to bytes in memory, in any format, including the binary HDT format.

        Usage:

        >>> b = m.write_bytes(format="hdt")

        :param format: One of "ntriples", "turtle", "trig", "nquads", "rdf/xml", "json-ld", "hdt".
        :param graph: The IRI of the graph to write, or for "json-ld", "trig" and "nquads" a list of graph IRIs to write
            as a dataset (use DEFAULT_GRAPH for the default graph); see write.
            For "trig" and "nquads", all graphs except the transient graph are written when this is None.
        :param prefixes: The prefixes used for turtle, RDF/XML and JSON-LD serialization.
        :return: The serialized triples (UTF-8 for the text formats).
        """

    def writes(
            self,
            format: LiteralType["ntriples", "turtle", "trig", "nquads", "rdf/xml", "json-ld"] = None,
            graph: Union[str, List[str]] = None,
            prefixes: Dict[str, str] = None,
    ) -> str:
        """
        Write the triples to a string in memory.

        Usage:

        >>> s = m.writes(format="turtle")

        :param format: One of "ntriples", "turtle", "trig", "nquads", "rdf/xml", "json-ld".
        :param graph: The IRI of the graph to write, or for "json-ld", "trig" and "nquads" a list of graph IRIs to write
            as a dataset (use DEFAULT_GRAPH for the default graph); see write.
            For "trig" and "nquads", all graphs except the transient graph are written when this is None.
        :param prefixes: The prefixes used for turtle, RDF/XML and JSON-LD serialization.
        :return: Triples in model in the given format (potentially a large string)
        """

    def truncate_graph(self, graph: str = None) -> None:
        """
        Removes all triples associated with the given graph from the triplestore,
        including full-text search entries.

        :param graph: The IRI of the graph to truncate.
        """

    def detach_graph(self, graph: str = None, preserve_name: bool = False) -> "Model":
        """
        Detaches and returns a named graph as their own Model object.
        The named graph is removed from the original Model.

        :param graph: The name of the graph to detach. Defaults to the default graph.
        :param preserve_name: Preserve the name of the graph in the new Model, defaults to False.
        :return: A model.
        """

    def get_predicate_iris(
            self, graph: str = None, include_graphs: Optional[List[str]] = None
    ) -> List["IRI"]:
        """
        :param graph: The graph to get the predicate iris from.
        :param include_graphs: IRIs of graphs whose predicates are included as well, e.g. [TRANSIENT_GRAPH].
        :return: The IRIs of the predicates currently in the given graph.
        """

    def get_predicate(
            self, iri: "IRI", graph: str = None, include_graphs: Optional[List[str]] = None
    ) -> List["SolutionMappings"]:
        """
        :param iri: The predicate IRI
        :param graph: The graph to get the predicate from.
        :param include_graphs: IRIs of graphs whose triples with the predicate are merged in, e.g. [TRANSIENT_GRAPH].
        :return: A list of the underlying tables that store a given predicate.
        """

    def create_index(self, options: "IndexingOptions" = None) -> None:
        """
        :param options: Indexing options
        """

    def compact(self) -> None:
        """
        Compact the stored triples, merging the segments that accumulate from repeated
        additions into a single sorted segment per predicate and type combination.
        """

    def add_graph(
            self,
            other: "Model",
            source_graph: str = None,
            target_graph: str = None,
    ) -> None:
        """
        Copy the triples of a graph in another Model into this Model.

        Usage:
        >>> m.add_graph(other_model, source_graph="urn:graph:a", target_graph="urn:graph:b")

        :param other: The Model to copy triples from.
        :param source_graph: The IRI of the graph in other to copy from. None means the default graph.
        :param target_graph: The IRI of the graph in this Model to copy into. None means the default graph.
        """

    def serve(
            self,
            host: str = "127.0.0.1",
            port: Optional[int] = 7878,
            flight_port: Optional[int] = 7879,
            allow_update: bool = False,
            arrow_http: bool = True,
            auth_token: Optional[str] = None,
            cors_origins: Optional[List[str]] = None,
            graph: Optional[str] = None,
            include_graphs: Optional[List[str]] = None,
            max_rows: Optional[int] = None,
            max_request_bytes: Optional[int] = None,
            batch_rows: Optional[int] = None,
            result_ttl: Optional[float] = None,
            arrow_compression: LiteralType["auto", "zstd", "none"] = "auto",
            static_dir: Union[str, Path] = None,
            static_path: str = "/ui",
            static_vars: Dict[str, str] = None,
            query_log_size: int = 1000,
            block: bool = False,
    ) -> "Server":
        """
        Serve the Model over the SPARQL 1.1 Protocol and Arrow Flight (maplib Enterprise Edition).
        Both endpoints run at the same time, in the background, on the Model itself: queries see
        changes made from Python, and updates through the endpoints are seen from Python.

        The SPARQL endpoints (on port):
        - /sparql: SPARQL 1.1 Protocol query and update, and the service description on GET.
          SELECT and ASK results as JSON, XML, CSV, TSV or Arrow IPC (application/vnd.apache.arrow.stream),
          CONSTRUCT and DESCRIBE results as Turtle, N-Triples, RDF/XML, JSON-LD, N-Quads or TriG.
          The format parameter (e.g. format=csv) overrides the Accept header. HTTP/1.1 and HTTP/2 (h2c) are served.
        - /query and /update: query only and update only. /update is only served with allow_update.
        - /store: the SPARQL 1.1 Graph Store HTTP Protocol (?graph=<iri> or ?default). Other paths
          are graphs identified by their URL.
        - /arrow/...: the Arrow API over plain HTTP, see arrow_http.
        - /: a query page. /health and /status.

        The Arrow Flight endpoint (on flight_port), for pyarrow.flight:
        - do_get(Ticket(query)): the result as with Model.query(), the query is SPARQL text or JSON
          {"query": ..., "graph": ..., "default_graph_uris": [...], "named_graph_uris": [...], "native_dataframe": false,
          "compression": "zstd" | "none" | "auto"}.
        - get_flight_info(FlightDescriptor.for_command(query)): runs the query and keeps the result
          for result_ttl seconds, with its schema and size, and a ticket to fetch it with do_get.
        - list_flights(): the graphs, FlightDescriptor.for_path("graph", iri) gives a graph's triples.
        - do_put(FlightDescriptor.for_command('{"template": ..., "graph": ...}'), schema): maps the
          uploaded data with a template (IRI or stOTTR document).
        - do_action: "update", "graphs", "status", "drop_result", "clear_results".

        Usage:
        >>> server = m.serve(allow_update=True)
        >>> server.url
        'http://127.0.0.1:7878/sparql'
        >>> client = pyarrow.flight.connect(server.flight_url)
        >>> table = client.do_get(pyarrow.flight.Ticket("SELECT * WHERE {?s ?p ?o}")).read_all()
        >>> server.stop()

        :param host: The address to listen on. Use "0.0.0.0" to listen on all interfaces, together with auth_token.
        :param port: The port of the SPARQL endpoints, None to not serve them, 0 for any free port.
        :param flight_port: The port of the Arrow Flight endpoint, None to not serve it, 0 for any free port.
        :param allow_update: Allow SPARQL Update, Graph Store Protocol writes and Arrow API (Flight and HTTP) writes.
        :param arrow_http: Serve the Arrow API over plain HTTP on port, for networks where gRPC (Flight) does not get
                           through, e.g. behind proxies, load balancers or firewalls: POST /arrow/query with a query
                           command as JSON gives an Arrow IPC stream, GET /arrow/graphs lists the graphs,
                           GET /arrow/graph?graph=<iri> gives a graph's triples, and POST /arrow/map?template=<iri>
                           maps an uploaded Arrow IPC stream. Model.query(endpoint=) uses it.
        :param auth_token: Require "Authorization: Bearer <auth_token>" on all requests (except /health).
                           With Flight, use the header, or client.authenticate_basic_token(user, auth_token).
        :param cors_origins: Web page origins allowed to use the endpoints, e.g. ["https://yasgui.org"], ["*"] for all.
                             None (the default) allows none, so other web sites can not read data or send updates.
        :param graph: The IRI of the graph queried when the request gives no dataset. None is the default graph.
        :param include_graphs: Graphs merged into the default graph of queries, as with Model.query.
        :param max_rows: Queries with more result rows than this fail.
        :param max_request_bytes: The largest request (body, Flight message or Flight upload) accepted, 256 MiB by default.
        :param batch_rows: Rows per Arrow record batch, 65536 by default.
        :param result_ttl: Seconds that results prepared by get_flight_info are kept, 600 by default.
        :param arrow_compression: Compression of the record batches of Arrow results (Flight and format=arrow), as
                                  defined by the Arrow IPC format, so readers such as pyarrow decompress them
                                  transparently. "auto" compresses with zstd for clients on other hosts, where the
                                  network is the bottleneck, and not for clients on the same host, where compressing
                                  costs more time than it saves. Requests can choose with "compression" in the Flight
                                  command or ticket, or the compression parameter over HTTP.
        :param static_dir: A web application to serve from this folder, e.g. a built single page application
            such as the Model portal. Files are read from disk on every request, so a rebuilt application
            is picked up by refreshing the page. Paths without a file get index.html, and "/" redirects to it.
        :param static_path: Where the application is served, by default "/ui".
        :param static_vars: Values for the <!--#echo var='name' --> directives (nginx SSI) in its HTML pages, e.g.
            the runtime configuration of the application.
        :param query_log_size: How many queries and updates the query log keeps (0 for none). GET /query-log
            gives them as JSON, with when they ran, who sent them (X-Query-Source header or Referer), status,
            rows, bytes and the time spent waiting, running and serializing, and summary statistics.
            DELETE /query-log empties it.
        :param block: Block until the server stops, Ctrl-C stops it.
        :return: The running Server.
        """

    def infer(
            self,
            rulesets: Union[str, List[str]],
            graph: str = None,
            max_iterations: Optional[int] = 100_000,
            include_graphs: Optional[List[str]] = None,
            max_rows: Optional[int] = 100_000_000,
            debug: bool = False,
    ) -> "InferenceResult":
        """
        Run the inference rules that are provided
        :param rulesets: The Datalog ruleset (a string), or a list of rulesets.
        :param graph: Apply the ruleset to this graph, defaults to the default graph, or the graph specified in the rules.
        :param max_iterations: Maximum number of iterations.
        :param include_graphs: IRIs of graphs whose triples are merged into the graph that is reasoned over, as with FROM.
            None includes the transient graph (TRANSIENT_GRAPH), use [] to query only the graph itself.
        :param max_rows: Maximum estimated rows in result, helps avoid out-of-memory errors.
        :param debug: Debugs rule bodies for executions that give no triples.
        :return: The result of inference.
        """

    def infer_rdfs(
            self,
            graph: str = None,
    ) -> int:
        """
        RDFS Inference with rules: 2, 3, 5, 6, 8, 9, 10, 11.
        The inferred triples are put in the transient graph (TRANSIENT_GRAPH), so they are not written unless
        that graph is written explicitly.
        :param graph: Apply rdfs inferencing to this graph, defaults to the default graph, or the graph specified in the rules.
        :return number of inferred triples
        """

    def size(self, graph: str = None) -> int:
        """
        Get the number of triples in a graph.

        :param graph: The named graph we are returning the size for
        :return: The number of triples in the graph.
        """

    def add_virtualization(self,
                           virtualized_database: "VirtualizedDatabase",
                           resources: Dict[str, Template],
                           ):
        """
        :param virtualized_database: We call the query-function of this object.
        :param resources: The templates associated with each resource
        """

    def add_udf(
            self,
            iri,
            func: Union[Callable[[pl.DataFrame], pl.Series], Callable[[pl.DataFrame], pl.DataFrame],],
            output_type: Union[RDFType, str, IRI],
            input_types: list[Union[RDFType, str, IRI]] = None
    ):
        """
        Add a user-defined function
        Usage:
        The user defined function that is provided should take in a Polars DataFrame as an argument.
        This dataframe will have one column for each argument ("0", "1" and so on).
        The function should add a column (e.g. "out") to the DataFrame, and return it.
        >>> def g(df: pl.DataFrame) -> pl.DataFrame:
        >>>     df = df.with_columns(
        >>>         (pl.col("0").str.contains("abc")).alias("out")
        >>>     )
        >>>     return df

        >>> m.add_udf("urn:maplib:findabc", g, xsd.boolean, [xsd.string])
        >>> result = m.query('''
        ... SELECT * {
        ... ?a <urn:maplib:hasstr> ?s1 .
        ... BIND(<urn:maplib:findabc>(?s1) AS ?found)
        ... }
        ... ''')
    ... print(result)

        :param iri: IRI of the UDF, e.g. "urn:maplib:myfunc"
        :param func: A callable (DataFrame) -> DataFrame
        :param output_type: The RDF type of the output. Either an RDFType (e.g. RDFType.IRI, RDFType.Literal(xsd.string)) or an IRI (e.g. xsd.integer)
        :param input_types: Optional list of RDF datatypes of the input columns
        """

    def list_udfs(self) -> list[str]:
        """
        :return: The list of registered UDFs
        """


class InferenceResult:
    """
    The result of running Datalog inference with Model.infer.
    Only constructed by maplib.
    """


class ExploreServer:
    """
    A running graph explorer session, returned by Model.explore.
    """

    url: str
    "The URL of the graph explorer."

    def stop(self) -> None:
        """
        Stop the graph explorer server.
        """


class Server:
    """
    A running SPARQL and Arrow Flight server, returned by Model.serve.
    Can be used as a context manager, which stops the server on exit.
    The server keeps running when the Server object is no longer referenced, until stop() or exit.
    """

    url: Optional[str]
    "The SPARQL query and update endpoint, e.g. http://127.0.0.1:7878/sparql."
    query_url: Optional[str]
    "The query endpoint."
    update_url: Optional[str]
    "The update endpoint."
    store_url: Optional[str]
    "The Graph Store Protocol endpoint."
    port: Optional[int]
    "The port of the SPARQL endpoints."
    flight_url: Optional[str]
    "The Arrow Flight endpoint, for pyarrow.flight.connect, e.g. grpc://127.0.0.1:7879."
    flight_port: Optional[int]
    "The port of the Arrow Flight endpoint."
    running: bool
    "Whether the server is running."

    def stop(self) -> None:
        """
        Stop the server. Running requests are allowed to finish.
        """

    def wait(self, timeout: Optional[float] = None) -> bool:
        """
        Block until the server stops, at most timeout seconds. Ctrl-C stops the server.

        :param timeout: Seconds to wait, None to wait until the server stops.
        :return: Whether the server has stopped.
        """

    def __enter__(self) -> "Server": ...

    def __exit__(self, *args) -> bool: ...


class MaplibException(Exception): ...


class FunctionArgumentException(Exception):
    """
    Raised when a function is called with invalid arguments.
    """


def set_memory_limit(
        max_memory_mb: Optional[int] = None,
        spill_dir: Optional[Union[str, Path]] = None,
        max_spill_mb: Optional[int] = None,
        abort_above_mb: Optional[int] = None,
) -> None:
    """Limits the memory used by queries, so that large queries spill to disk instead of
    running out of memory. Over the limit, joins, aggregations, sorts and query results
    move intermediate data to disk. The limit counts all memory used by maplib, including
    the triples of the models, which are not spilled.

    Without a call, the limit is 80% of the system memory (respecting container limits).
    Each call sets all four settings; arguments left as None get their defaults.
    The settings apply to the whole process and to queries started after the call.

    >>> import maplib
    >>> maplib.set_memory_limit(max_memory_mb=8000, spill_dir="/scratch/maplib")

    :param max_memory_mb: Spill to disk when maplib uses more than this many megabytes (10^6 bytes).
    :param spill_dir: The folder for spill files. Defaults to the system temporary folder. Set it before running any query: it is fixed at the first spill.
    :param max_spill_mb: The most megabytes to spill to disk. Defaults to 64 GB.
    :param abort_above_mb: Stop the process (with a message on stderr) when maplib uses more than this many megabytes even after spilling. Off by default. This stops the process before the operating system kills it, and is not an exception that can be caught.
    """

def memory_usage() -> int:
    """The estimated number of bytes allocated by maplib."""

def memory_limit() -> int:
    """The memory limit in bytes above which queries spill to disk. See set_memory_limit."""


def explore(
        m: "Model",
        host: str = "localhost",
        port: int = 8000,
        bind: str = "localhost",
        popup=True,
        fts=True,
):
    """Starts a graph explorer session.
    To run from Jupyter Notebook use:
    >>> from maplib import explore
    >>>
    >>> server = explore(m)
    You can later stop the server with
    >>> server.stop()

    :param m: The Model to explore
    :param host: The hostname that we will point the browser to.
    :param port: The port where the graph explorer webserver listens on.
    :param bind: Bind to the following host / ip.
    :param popup: Pop up the browser window.
    :param fts: Enable full text search indexing
    """


def generate_templates(m: Model, graph: Optional[str] = None) -> Dict[str, Template]:
    """Generate templates for instantiating the classes in an ontology

    :param m: The model where the ontology is stored. We rely on owl:Class/rdfs:Class declarations, rdfs:subClassOf, rdfs:domain, rdfs:range (also via owl:unionOf), owl:Restriction and the declared types of properties.
    :param graph: The named graph where the ontology is stored. Defaults to the default graph.

    :return A dictionary of templates for instantiating the classes in the ontology, where the keys are the class IRIs.
        For each class there is a template with the class IRI, which also produces the rdf:type triple, and one with
        the IRI of the class suffixed by "_notype", which does not. The subject is the parameter "id",
        the other (optional) parameters are named after the local names of the properties.
        Name clashes are resolved with suffixes ("_inverse" for reverse properties, then "_2", "_3", ...).
        Templates of subclasses instantiate the "_notype" templates of their superclasses,
        so all templates must be added to the Model you want to populate.

    Usage example - note that it is important to add the templates to the Model you want to populate.
    >>> from maplib import Model, generate_templates
    >>>
    >>> m_ont = Model()
    >>> m_ont.read("my_ontology.ttl")
    >>> templates = generate_templates(m_ont)
    >>> m = Model()
    >>> for t in templates.values():
    >>>     m.add_template(t)
    >>> m.map("https://example.net/MyClass", df)
    """


class VirtualizedDatabase:
    """
    A virtualized database implemented in Python.
    """

    def __init__(self,
                 database: Any,
                 resource_sql_map: Optional[Dict[str, Any]] = None,
                 sql_dialect: Optional[Union[LiteralType["duckdb", "snowflake", "bigquery", "databricks", "postgres"], Any]] = None):
        """
        Virtualize a database, so that SPARQL queries over the resources it holds are pushed down to it.

        The databases in maplib.virtualization (DuckDBDatabase, SnowflakeDatabase, BigQueryDatabase and
        DatabricksDatabase) are ready to use, and know their SQL dialect:

        >>> from maplib.virtualization import SnowflakeDatabase
        >>> vdb = VirtualizedDatabase(
        ...     SnowflakeDatabase(account="...", user="...", warehouse="...", database="...", schema="..."),
        ...     resource_sql_map={"my_resource": "SELECT sensor_id AS id, ts AS timestamp, value FROM readings"},
        ... )
        >>> m.add_virtualization(vdb, resources={"my_resource": my_template})

        Any object with a query method that takes an SQL string and returns a polars DataFrame works as the
        database, given the sql_dialect.
        For advanced use, the resource_sql_map may be omitted, in which case the VirtualizedQuery will be provided to the query method.
        The user must then translate this VirtualizedQuery (built on SPARQL Algebra) to the target query language.

        Not available in the open source build: raises NotImplementedError.

        :param database: An object with a query method.
        :param resource_sql_map: For each resource, the SQL that gives its rows: an SQL string, an sqlalchemy Table or an sqlalchemy Select.
            It must have an id column, and a column for each of the other parameters of the resource's template.
        :param sql_dialect: The SQL dialect accepted by the query method: one of "duckdb", "snowflake", "bigquery",
            "databricks" and "postgres", or an instance of an SQLDialect from maplib.virtualization to customize the SQL.
            Defaults to the sql_dialect attribute of the database.
        """

    @staticmethod
    def Cognite(client: Any,
                resource_cognite_map: Optional[Dict[str, Dict[str, str]]] = None) -> "VirtualizedDatabase":
        """
        A virtualized database backed by time series in Cognite Data Fusion, queried with a cognite-sdk client.
        The id of each time series in the knowledge graph is its external id (or its id or instance id, see below).
        Time ranges in filters are sent to CDF as start and end, and grouping by
        chrontext:FloorDateTimeToSecondsInterval with MIN, MAX, SUM, COUNT or AVG of the value uses CDF aggregates.
        The rest of the query is evaluated on the retrieved datapoints.

        Not available in the open source build: raises NotImplementedError.

        Usage:

        >>> from cognite.client import CogniteClient
        >>> vdb = VirtualizedDatabase.Cognite(CogniteClient())
        >>> m.add_virtualization(virtualized_database=vdb, resources={"my_resource": template})

        :param client: A cognite.client.CogniteClient.
        :param resource_cognite_map: Optional settings per resource: "id_type" is "external_id" (default), "id" or
            "instance_id" (written as "space:externalId"), and "timestamp" and "value" name the template parameters
            holding the timestamp and the value (default "timestamp" and "value").
        """

from datetime import datetime, date
from pathlib import Path
from typing import Any, overload, Union, List, Dict, Optional, Callable, Iterable, Literal as LiteralType, Tuple, BinaryIO, TextIO

import polars as pl
from maplib.maplib import rdf
from maplib.opcua import OPCUA

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

    def __init__(self, value: str, data_type: Optional["IRI"] = None, language: Optional[str] = None):
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


class FunctionCall:
    """
    A function call term, which computes a template argument from other terms when the
    template is expanded, e.g. building an IRI from a string column. Function calls are
    evaluated for all rows at once by maplib's SPARQL expression engine.

    Corresponds to the stOTTR syntax ``(CONCAT "http://example.net/" ?id)``.

    >>> FunctionCall("IRI", [FunctionCall("CONCAT", [Literal("http://example.net/"), Variable("id")])])
    """

    def __init__(
            self,
            function: Union[str, IRI],
            arguments: Optional[List[Union["FunctionCall", Variable, IRI, Literal, None]]] = None,
    ):
        """
        :param function: The name of a SPARQL built-in function or operator, such as "CONCAT",
                         "IRI", "STR", "IF", "+" or "=", or the IRI of a function, such as an
                         XSD datatype for casts (e.g. xsd.integer) or a function defined with
                         ``def`` in a stOTTR document.
        :param arguments: The arguments. None means ottr:none, which is unbound.
        """


class Argument:
    variable: Optional[Variable]
    "The variable, if the term is a variable."

    def __init__(
            self,
            term: Union["Argument", Variable, IRI, Literal, BlankNode, FunctionCall, None],
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
            arguments: List[Union[Argument, Variable, IRI, Literal, BlankNode, FunctionCall, None]],
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
            arguments: List[Union[Argument, Variable, IRI, Literal, BlankNode, FunctionCall, None]],
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
) -> Instance:
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
            object_sort_all: Optional[bool] = None,
            object_sort_some: Optional[List["IRI"]] = None,
            fts: Optional[bool] = None,
            fts_path: Optional[str] = None,
            geo: Optional[bool] = None,
            fts_predicates: Optional[List["IRI"]] = None,
            fts_local_names: Optional[bool] = None,
            fts_iris: Optional[bool] = None,
            vectors: Optional[bool] = None,
            vector_ann: Optional[bool] = None,
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
        :param fts_predicates: Only index the string literals of these predicates for full text search
                               (all predicates by default). Enables full text search.
        :param fts_local_names: Also index the local names of the subject IRIs of indexed triples
                                (after the last #, / or :), so that e.g. a CGMES rdf:ID is searchable.
                                They are matched with the pseudo-predicate maplib:ftsLocalName.
                                Enables full text search.
        :param fts_iris: Also index IRIs in the object position for full text search, by the whole IRI,
                         with the predicate of the triple: <http://ex.org/ns#BreakerType> has the tokens
                         http, ex, org, ns and breakertype. Enables full text search.
        :param vectors: Enable the vector index (maplib EE). Vector literals (maplib:vector, e.g. mapped
                    from Polars Array columns of numbers) are stored with their parsed vectors, and the
                    maplib:vectorSearch magic predicate finds the nearest neighbours of a query vector
                    with exact search. The maplib:cosineSimilarity, maplib:dotProduct and
                    maplib:euclideanDistance functions also work without the index, but slower.
        :param vector_ann: Also build an approximate nearest neighbour index (maplib EE), which
                    answers cosine similarity searches much faster than exact search at a small
                    loss of recall. Implies vectors=True.
        """


class QueryCacheOptions:
    """
    Options of a model's query result cache, see Model.set_query_cache.
    """

    enabled: bool
    max_entries: int
    max_bytes: int
    max_entry_bytes: int
    ttl: Optional[float]
    min_run_time: float
    cache_virtualized: bool

    def __init__(
            self,
            enabled: bool = True,
            max_entries: int = 1024,
            max_bytes: int = 256 * 1024 * 1024,
            max_entry_bytes: int = 16 * 1024 * 1024,
            ttl: Optional[float] = None,
            min_run_time: float = 0.0,
            cache_virtualized: bool = False,
    ):
        """
        :param enabled: Whether results are cached.
        :param max_entries: The largest number of results kept. The least recently used are evicted first.
        :param max_bytes: The largest total (estimated) size of the results kept, in bytes.
        :param max_entry_bytes: Results larger than this (bytes) are returned but not kept.
        :param ttl: Seconds after which a result is no longer served. None keeps it until a write changes
                    what it read, or it is evicted.
        :param min_run_time: Queries that ran faster than this (seconds) are not kept.
        :param cache_virtualized: Also keep the results of queries that read virtualized databases. The cache does
                                  not see changes in those databases, so set a ttl with this.
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
        The message column is a list of the messages of the result (one per sh:message of the shape or
        SPARQL constraint), formatted as "value"@lang for language tagged strings and as the value for strings.

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
    Model.map_default, Model.map_json and Model.map_xml. Each step returns a new ModelStream, and nothing
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
            parameters: Optional[ParametersType] = None,
            graph: Optional[str] = None,
            include_graphs: Optional[List[str]] = None,
            max_rows: Optional[int] = None,
            debug: bool = False,
    ) -> "ModelStream":
        """
        Add a SPARQL Update step, see Model.update.
        """

    def insert(
            self,
            query: str,
            parameters: Optional[ParametersType] = None,
            source_graph: Optional[str] = None,
            target_graph: Optional[str] = None,
            include_graphs: Optional[List[str]] = None,
            max_rows: Optional[int] = None,
            debug: bool = False,
    ) -> "ModelStream":
        """
        Add a step that inserts the results of a Construct query into the batch, see Model.insert.
        """

    def infer(
            self,
            rulesets: Union[str, List[str]],
            graph: Optional[str] = None,
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
            format: Optional[LiteralType["ntriples", "turtle", "nquads"]] = None,
            graph: Optional[str] = None,
            prefixes: Optional[Dict[str, str]] = None,
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
            indexing_options: Optional["IndexingOptions"] = None,
            storage_folder: Optional[Union[Path, str]] = None,
            storage: Optional[LiteralType["memory", "hybrid", "disk"]] = None,
    ) -> "Model": ...

    def serialize(self, path: Union[Path, str] = "./model.maplib", raw_db: bool = False,
                  single_file: bool = True, storage_options: Optional[Dict[str, str]] = None):
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
                    storage_folder: Optional[Union[Path, str]] = None,
                    storage: Optional[LiteralType["memory", "hybrid", "disk"]] = None,
                    storage_options: Optional[Dict[str, str]] = None,
                    cache_size: Optional[int] = None) -> "Model":
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

    def templates_to_graph(self, graph: Optional[str] = None) -> None:
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

    def add_prefixes(self, prefixes: Dict[str, str]) -> None:
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
            data: Optional[Union[DataFrame, LazyFrame, "SolutionMappings", Iterable[Union[DataFrame, LazyFrame, "SolutionMappings"]]]] = None,
            graph: Optional[str] = None,
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
            graph: Optional[str] = None,
            uuid_namespace: Optional[str] = None,
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
            graph: Optional[str] = None,
            uuid_namespace: Optional[str] = None,
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
            predicate: Optional[Union[str, IRI]] = None,
            graph: Optional[str] = None,
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

    def map_default(
            self,
            data: Union[DataFrame, LazyFrame, "SolutionMappings", Iterable[Union[DataFrame, LazyFrame, "SolutionMappings"]]],
            primary_key_column: str,
            graph: Optional[str] = None,
            validate_iris: bool = True,
            stream: bool = False,
    ) -> Union[str, "ModelStream"]:
        """
        Create a default template from the data, add it to the Model and map the data with it.
        The primary key column becomes the subject of one ottr:Triple per other column, with predicates in
        the urn:maplib_default: namespace. A parameter is optional when its column contains nulls.
        Key columns that are not String are cast to String. Use Model.generate_template for a dry run that
        returns the same template without mapping anything.
        Usage:

        >>> template_string = m.map_default(df, "myKeyCol")
        ... print(template_string)

        :param data: DataFrame where the columns have the same names as the template arguments (when piping the output of queries back in, use SolutionMappings).
            May also be a LazyFrame, which is consumed in a single pass using collect_batches(), or an iterable of DataFrames, LazyFrames or SolutionMappings with the same schema.
            In that case, the template is created from the first batch and all its parameters are optional.
        :param primary_key_column: This column will be the subject of all triples in the generated template.
        :param graph: The IRI of the graph to add triples to.
        :param validate_iris: Validate any IRI-columns.
        :param stream: Return a ModelStream instead of mapping into this Model. Steps added to the stream
            (update, insert, infer, write) run batch by batch when it is collected, see ModelStream.
            The template is then created from the first batch when the stream is collected.
        :return: The generated template, or a ModelStream when stream is set.
        """

    def generate_template(
            self,
            data: Union[DataFrame, "SolutionMappings"],
            primary_key_column: str,
    ) -> str:
        """
        Generate a default template from a DataFrame. This is the dry run of Model.map_default: it returns
        the template map_default would map with, but nothing is mapped and the template is not added to the
        Model. The primary key column becomes the subject of one ottr:Triple per other column, with predicates
        in the urn:maplib_default: namespace. A parameter is optional when its column contains nulls. Map the
        data with the template using Model.map.
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
            metadata: Optional[Union[str, Path]] = None,
            graph: Optional[str] = None,
            minimal: bool = False,
            iri_mappings: Optional[Dict[str, Union[str, Path]]] = None,
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
            graph: Optional[str] = None,
            uuid_namespace: Optional[str] = None,
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
            graph: Optional[str] = None,
    ) -> None:
        """
        Map a folder of OPC UA NodeSet2XMLs to RDF.
        This folder MUST contain the Namespace 0 XML to work properly.

        Usage:
        >>> m.map_opc_ua("my_opc_ua_nodeset2_xmls", graph="urn:maplib:uagraphs")

        To generate SHACL shapes from the types in the nodesets, see m.opcua.add_typing_shapes.

        :param folder: OPC UA NodeSet2 XMLs are found in this folder
        :param graph: The IRI of the graph to add triples to.
        :return: None
        """

    @property
    def opcua(self) -> OPCUA:
        """
        OPC UA functionality of this model, see maplib.opcua.OPCUA.

        Usage:
        >>> m.map_opc_ua("nodesets")
        >>> m.opcua.add_typing_shapes(shapes_graph="urn:shapes", instance_namespaces=["urn:my:instances"])
        """

    def map_cdf(
            self,
            client: Any,
            data_models: Optional[Any] = None,
            spaces: Optional[Union[str, List[str]]] = None,
            graph: Optional[str] = None,
            base_iri: Optional[str] = None,
            include_schema: bool = True,
            include_edges: bool = True,
            virtualize_time_series: bool = True,
    ):
        """
        Map the data model of a Cognite Data Fusion (CDF) project to RDF, using a cognite-sdk client.
        Datapoints are not read. Instead, the datapoints of time series (nodes in cdf_cdm:CogniteTimeSeries)
        are virtualized, and fetched from CDF when a query needs them (see VirtualizedDatabase.Cognite).

        - Each view becomes an rdfs:Class, with rdfs:subClassOf the views it implements.
        - Each container property mapped by a view becomes a predicate, so views that map the same container
          property share the predicate.
        - Nodes and edges become resources typed with the views they have data in.
          Direct relations are IRIs, and an edge adds a triple from its start node to its end node
          with the edge type as the predicate.
        - Each time series node gets chrontext:hasResource and chrontext:hasExternalId, so it can be queried with
          ?ts chrontext:hasDataPoint ?dp . ?dp chrontext:hasTimestamp ?t . ?dp chrontext:hasValue ?v .

        IRIs are <base_iri>node/<space>/<externalId>, <base_iri>edge/<space>/<externalId>,
        <base_iri>view/<space>/<externalId>, <base_iri>datamodel/<space>/<externalId> and
        <base_iri>property/<space>/<container>/<property>.

        Not available in the open source build: raises NotImplementedError.

        Usage:

        >>> from cognite.client import CogniteClient
        >>> m.map_cdf(CogniteClient(), data_models=[("my_space", "MyModel", "v1")])

        :param client: A cognite.client.CogniteClient.
        :param data_models: The data models (DataModelId or (space, externalId, version)) to map.
            Defaults to all data models in the project, except the global ones.
        :param spaces: Only map instances in these spaces.
        :param graph: The IRI of the graph to add triples to.
        :param base_iri: The namespace of the IRIs of CDF resources. Defaults to urn:cdf:<project>:
        :param include_schema: Map the data models, views and properties.
        :param include_edges: Map the edges of the edge connections in the views, also those without properties.
        :param virtualize_time_series: Virtualize the datapoints of the time series nodes.
            Replaces any virtualization added to this model before.
        :return: None
        """

    def map_osipi(
            self,
            url: str,
            asset_database: Optional[str] = None,
            asset_server: Optional[str] = None,
            root_element: Optional[str] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            token: Optional[str] = None,
            verify_ssl: bool = True,
            graph: Optional[str] = None,
            base_iri: Optional[str] = None,
            read_static_values: bool = True,
            default_start_time: Optional[str] = None,
            default_end_time: Optional[str] = None,
            page_size: int = 10000,
            max_concurrency: int = 8,
            timeout: Optional[float] = None,
    ):
        """
        Read an OSIsoft PI (AVEVA PI) Asset Framework model through the PI Web API, and
        virtualize the time series of its attributes so that their data points can be queried
        in SPARQL together with the model. The PI virtualization is used together with the other virtualizations
        of the Model (see add_virtualization), and replaces an earlier PI virtualization.

        The model uses the prefix osipi: <urn:maplib_osipi:> and contains:

        - osipi:AssetServer, osipi:AssetDatabase (osipi:hasDatabase),
        - an rdfs:Class for each element template (osipi:ElementTemplate, rdfs:subClassOf its base template or osipi:Element),
        - each element (osipi:Element, rdf:type its template), with osipi:hasChild / osipi:hasParent, and osipi:hasElement from the database to the top elements,
        - each attribute (osipi:Attribute), with osipi:hasAttribute from its element, osipi:hasChildAttribute / osipi:hasParentAttribute,
          and osipi:type, osipi:unit, osipi:dataReference, osipi:configString and osipi:value (the current value of attributes without data references),
        - osipi:name, rdfs:label, osipi:description, osipi:path, osipi:id, osipi:webId and osipi:category on all of these.

        Each attribute with a data reference (e.g. a PI Point) has a time series
        (chrontext:hasTimeseries) with data points that are read from the PI Web API when queried:
        chrontext:hasDataPoint, then chrontext:hasTimestamp, chrontext:hasValue (xsd:double, unbound for non-numeric values),
        osipi:hasState (digital state names and string values), osipi:isGood, osipi:isQuestionable, osipi:isSubstituted and osipi:isAnnotated.

        Bounds on the timestamp in FILTERs limit the recorded values requested from PI.
        A query without a lower bound on the timestamp fails unless default_start_time is set.

        Not available in the open source build.

        Usage:
        >>> m = Model()
        >>> m.map_osipi("https://myserver/piwebapi", asset_database="\\\\\\\\MyAFServer\\\\MyDatabase",
        ...             username="me", password="...")
        >>> df = m.query('''
        ... PREFIX osipi: <urn:maplib_osipi:>
        ... PREFIX chrontext: <https://github.com/DataTreehouse/chrontext#>
        ... PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        ... SELECT ?pump ?t ?v WHERE {
        ...     ?pump osipi:templateName "Pump" ; osipi:hasAttribute ?a .
        ...     ?a osipi:name "Flow" ; chrontext:hasTimeseries ?ts .
        ...     ?ts chrontext:hasDataPoint ?dp .
        ...     ?dp chrontext:hasTimestamp ?t ; chrontext:hasValue ?v .
        ...     FILTER(?t >= "2024-01-01T00:00:00Z"^^xsd:dateTime && ?t < "2024-01-02T00:00:00Z"^^xsd:dateTime)
        ... }''')

        :param url: The root of the PI Web API, e.g. https://myserver/piwebapi
        :param asset_database: The name or path (\\\\server\\database) of the AF database to read. All databases if None.
        :param asset_server: The name or path of the AF server to read databases from. All servers if None.
        :param root_element: Only read this element and its descendants. Either an absolute path (\\\\server\\database\\element) or a path relative to the database.
        :param username: The username for basic authentication.
        :param password: The password for basic authentication.
        :param token: A bearer token, instead of username and password.
        :param verify_ssl: Verify the certificate of the PI Web API.
        :param graph: The IRI of the graph to add the Asset Framework triples to.
        :param base_iri: The prefix of the IRIs of servers, databases, templates, elements and attributes. Defaults to urn:maplib_osipi:
        :param read_static_values: Read the current values of attributes without data references into osipi:value.
        :param default_start_time: The PI time (e.g. "*-7d" or "2024-01-01T00:00:00Z") to read from when a query has no lower bound on the timestamp.
        :param default_end_time: The PI time to read to when a query has no upper bound on the timestamp. Defaults to now ("*").
        :param page_size: The maxCount of each PI Web API request; larger results are paged.
        :param max_concurrency: The maximum number of concurrent PI Web API requests.
        :param timeout: The timeout of each PI Web API request in seconds.
        :return: None
        """

    def map_insights_hub(
            self,
            host: str,
            tenant: str,
            client_id: Optional[str] = None,
            client_secret: Optional[str] = None,
            token: Optional[str] = None,
            token_url: Optional[str] = None,
            app_name: Optional[str] = None,
            app_version: Optional[str] = None,
            user_tenant: Optional[str] = None,
            root_asset: Optional[str] = None,
            graph: Optional[str] = None,
            base_iri: Optional[str] = None,
            default_start_time: Optional[str] = None,
            default_end_time: Optional[str] = None,
            aggregate_pushdown: bool = False,
            page_size: int = 2000,
            max_concurrency: int = 8,
            timeout: Optional[float] = None,
            verify_ssl: bool = True,
    ) -> None:
        """
        Read the asset model of a Siemens Insights Hub (formerly MindSphere) tenant from Asset Management v3,
        and virtualize the time series of its variables so that their data points can be queried in SPARQL
        together with the model, from IoT Time Series v3 and IoT Time Series Aggregates v4.
        The virtualization is added to any existing virtualizations of the Model.

        The model uses the prefix insights_hub: <urn:maplib_insights_hub:> and contains:

        - each asset (insights_hub:Asset, and rdf:type its asset type), with insights_hub:hasParent / insights_hub:hasChild,
          insights_hub:assetType, insights_hub:assetId, insights_hub:typeId, insights_hub:externalId, insights_hub:twinType,
          insights_hub:timezone, and insights_hub:parentId when the parent is not read,
        - the asset types of the assets and their ancestors (insights_hub:AssetType, an rdfs:Class, rdfs:subClassOf its
          parent type or insights_hub:Asset), with insights_hub:id, insights_hub:scope, insights_hub:instantiable and insights_hub:parentType,
        - the location of an asset (insights_hub:hasLocation): insights_hub:country, region, locality, streetAddress,
          postalCode, and latitude and longitude as xsd:double,
        - the asset variables of the asset types (insights_hub:hasAssetVariable, insights_hub:AssetVariable), with their insights_hub:value,
        - the aspects of each asset (insights_hub:hasAspect, insights_hub:Aspect), with insights_hub:category (static or dynamic)
          and insights_hub:aspectType (insights_hub:AspectType),
        - the variables of each aspect (insights_hub:hasVariable, insights_hub:Variable), with insights_hub:dataType, insights_hub:unit,
          insights_hub:length, insights_hub:qualityCode, insights_hub:searchable and insights_hub:defaultValue,
        - insights_hub:name, rdfs:label and insights_hub:description.

        Each variable of a dynamic aspect has a time series (chrontext:hasTimeseries, an insights_hub:Timeseries with
        chrontext:hasExternalId "assetId/aspectName/variableName") with data points that are read when queried:
        chrontext:hasDataPoint, then chrontext:hasTimestamp, chrontext:hasValue (xsd:double, for INT, LONG, DOUBLE and
        BOOLEAN variables, where true is 1.0), insights_hub:hasStringValue (for STRING, BIG_STRING and TIMESTAMP variables)
        and insights_hub:hasQualityCode (the _qc of the variable).

        Bounds on the timestamp in FILTERs limit the raw data requested from IoT Time Series.
        A query without a lower bound on the timestamp fails unless default_start_time is set.
        With aggregate_pushdown, MIN, MAX, SUM, COUNT and AVG of chrontext:hasValue grouped by
        chrontext:FloorDateTimeToSecondsInterval(?t, seconds) are read from IoT Time Series Aggregates when the
        buckets are whole minutes dividing an hour or whole hours dividing a day, and the bounds of the time range
        are on the bucket grid. Insights Hub aggregates leave out values of bad quality, and count a value exactly
        on a bucket boundary in the preceding bucket, so the results can then differ from aggregating the raw data.

        Authenticate with service credentials (client_id and client_secret), app credentials (client_id,
        client_secret, app_name and app_version), or a token.

        Not available in the open source build.

        Usage:
        >>> m = Model()
        >>> m.map_insights_hub("gateway.eu1.mindsphere.io", "mytenant", client_id="mytenant-svc", client_secret="...")
        >>> df = m.query(\"\"\"
        ... PREFIX insights_hub: <urn:maplib_insights_hub:>
        ... PREFIX chrontext: <https://github.com/DataTreehouse/chrontext#>
        ... PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        ... SELECT ?asset ?t ?v WHERE {
        ...     ?a insights_hub:name ?asset ; insights_hub:hasAspect ?asp .
        ...     ?asp insights_hub:name "process" ; insights_hub:hasVariable ?var .
        ...     ?var insights_hub:name "flow" ; chrontext:hasTimeseries ?ts .
        ...     ?ts chrontext:hasDataPoint ?dp .
        ...     ?dp chrontext:hasTimestamp ?t ; chrontext:hasValue ?v .
        ...     FILTER(?t >= "2024-01-01T00:00:00Z"^^xsd:dateTime && ?t < "2024-01-02T00:00:00Z"^^xsd:dateTime)
        ... }\"\"\")

        :param host: The gateway of the region, e.g. gateway.eu1.mindsphere.io (https:// is added when there is no scheme).
        :param tenant: The tenant.
        :param client_id: The client id of the service or app credentials.
        :param client_secret: The client secret of the service or app credentials.
        :param token: An access token, instead of client_id and client_secret.
        :param token_url: The OAuth token endpoint for service credentials. Defaults to https://{tenant}.piam.{region}.{domain}/oauth/token, derived from the host.
        :param app_name: The app name of app credentials, which are exchanged for a token with the Technical Token Manager.
        :param app_version: The app version of app credentials.
        :param user_tenant: The user tenant of app credentials. Defaults to the tenant.
        :param root_asset: Only read this asset (an asset id) and its descendants.
        :param graph: The IRI of the graph to add the asset model to.
        :param base_iri: The prefix of the IRIs of assets, asset types, aspects and variables. Defaults to urn:maplib_insights_hub:
        :param default_start_time: The ISO 8601 timestamp (e.g. "2024-01-01T00:00:00Z") or duration before now (e.g. "P7D") to read from when a query has no lower bound on the timestamp.
        :param default_end_time: The ISO 8601 timestamp or duration before now to read to when a query has no upper bound on the timestamp. Defaults to now.
        :param aggregate_pushdown: Read time bucketed aggregates from IoT Time Series Aggregates, see above.
        :param page_size: The limit of each IoT Time Series request; larger results are paged by time.
        :param max_concurrency: The maximum number of concurrent requests in a query.
        :param timeout: The timeout of each request in seconds.
        :param verify_ssl: Verify the certificates of Insights Hub.
        :return: None
        """

    def map_pi_points(
            self,
            url: str,
            data_server: Optional[str] = None,
            name_filter: str = "*",
            source_filter: Optional[str] = None,
            point_attributes: Optional[List[str]] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            token: Optional[str] = None,
            verify_ssl: bool = True,
            graph: Optional[str] = None,
            base_iri: Optional[str] = None,
            default_start_time: Optional[str] = None,
            default_end_time: Optional[str] = None,
            page_size: int = 10000,
            max_concurrency: int = 8,
            timeout: Optional[float] = None,
    ):
        """
        Read the PI Points (tags) of an OSIsoft PI (AVEVA PI) Data Archive, the PI historian,
        through the data servers of the PI Web API, without the Asset Framework, and virtualize
        their recorded values so that their data points can be queried in SPARQL.
        The virtualization is added to any existing ones; time series added by map_osipi through
        the same PI Web API remain queryable.

        The model uses the prefix osipi: <urn:maplib_osipi:> and contains:

        - each data server (osipi:DataServer) with osipi:serverVersion and osipi:isConnected,
        - each PI Point (osipi:Point and osipi:Timeseries), with osipi:inDataServer (and osipi:hasPoint from the data server),
          osipi:descriptor (also rdfs:comment), osipi:unit (the engineering units), osipi:pointType (e.g. Float32, Int32, Digital, String),
          osipi:pointClass, osipi:digitalSetName, osipi:span, osipi:zero, osipi:displayDigits, osipi:isStep, osipi:isFuture
          and osipi:pointSource,
        - the point attributes that are read (point_attributes) as osipi:hasPointAttribute nodes (osipi:PointAttribute) with osipi:name and osipi:value,
        - osipi:name (the tag name), rdfs:label, osipi:path, osipi:id and osipi:webId on data servers and points.

        Each point is a time series with chrontext:hasResource "osipi" and chrontext:hasExternalId (the WebId of the point).
        Its data points are read from the PI Web API when queried, as for map_osipi:
        chrontext:hasDataPoint, then chrontext:hasTimestamp, chrontext:hasValue (xsd:double, unbound for non-numeric values),
        osipi:hasState (digital state names and string values), osipi:isGood, osipi:isQuestionable, osipi:isSubstituted and osipi:isAnnotated.

        Bounds on the timestamp in FILTERs limit the recorded values requested from PI.
        A query without a lower bound on the timestamp fails unless default_start_time is set.
        Aggregates are computed in the virtualization, and MIN, MAX, SUM, AVG and COUNT of the values grouped
        by chrontext:FloorDateTimeToSecondsInterval(?t, seconds) are read as PI summaries when the time range
        starts and ends on a multiple of the interval (counted from 1970-01-01T00:00:00Z) and is only filtered by time.
        This applies to map_osipi as well.

        Not available in the open source build.

        Usage:
        >>> m = Model()
        >>> m.map_pi_points("https://myserver/piwebapi", data_server="MyPIServer", name_filter="FIC*",
        ...                 username="me", password="...")
        >>> df = m.query('''
        ... PREFIX osipi: <urn:maplib_osipi:>
        ... PREFIX chrontext: <https://github.com/DataTreehouse/chrontext#>
        ... PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        ... SELECT ?tag ?bucket (AVG(?v) AS ?avg) WHERE {
        ...     ?p a osipi:Point ; osipi:name ?tag ; chrontext:hasDataPoint ?dp .
        ...     ?dp chrontext:hasTimestamp ?t ; chrontext:hasValue ?v .
        ...     BIND(chrontext:FloorDateTimeToSecondsInterval(?t, 3600) AS ?bucket)
        ...     FILTER(?t >= "2024-01-01T00:00:00Z"^^xsd:dateTime && ?t < "2024-01-02T00:00:00Z"^^xsd:dateTime)
        ... } GROUP BY ?tag ?bucket''')

        :param url: The root of the PI Web API, e.g. https://myserver/piwebapi
        :param data_server: The name or path (\\\\server) of the data server (PI Data Archive) to read points from. All data servers if None.
        :param name_filter: The name filter of the points, with the wildcards * and ?, e.g. "FIC*". All points by default.
        :param source_filter: Only read points with this point source (with wildcards).
        :param point_attributes: The point attributes to read for each point, e.g. ["pointsource", "location1", "exdesc"]. Each takes one request per point; [] reads none. Defaults to ["pointsource"].
        :param username: The username for basic authentication.
        :param password: The password for basic authentication.
        :param token: A bearer token, instead of username and password.
        :param verify_ssl: Verify the certificate of the PI Web API.
        :param graph: The IRI of the graph to add the data servers and points to.
        :param base_iri: The prefix of the IRIs of data servers and points. Defaults to urn:maplib_osipi:
        :param default_start_time: The PI time (e.g. "*-7d" or "2024-01-01T00:00:00Z") to read from when a query has no lower bound on the timestamp.
        :param default_end_time: The PI time to read to when a query has no upper bound on the timestamp. Defaults to now ("*").
        :param page_size: The maxCount of each PI Web API request; larger results are paged.
        :param max_concurrency: The maximum number of concurrent PI Web API requests.
        :param timeout: The timeout of each PI Web API request in seconds.
        :return: None
        """

    def explore(
            self,
            host: str = "localhost",
            port: int = 8000,
            bind: str = "localhost",
            popup=False,
            fts=True,
            fts_path: Optional[str] = None,
            root_path: str = "/",
            graph: Optional[str] = None,
            page: Optional[str] = None,
            frontend: Optional[Union[str, Path]] = None,
    ) -> "ExploreServer":
        """Explores the model in the Model portal (maplib Enterprise Edition, with the
        validation_portal_core package installed).

        The whole portal runs on the model in a background server: its pages, its backend (whose
        SPARQL, at server.sparql_url, is answered by the model through Model.serve) and its query log.
        >>> server = m.explore()
        >>> server.url         # the portal, open it in a browser
        >>> server.sparql_url  # its SPARQL endpoint, e.g. Model().query(..., endpoint=server.sparql_url)
        >>> server.stop()

        :param host: The host name in the URL (the server listens on bind).
        :param port: The port of the server, 0 for any free port.
        :param bind: Bind to the following host / ip.
        :param popup: Pop up the browser window (deprecated, open server.url yourself instead).
        :param fts: Build the full text search index used by the search boxes.
        :param fts_path: Deprecated and ignored, the index needs no path.
        :param root_path: The path the server is reached under behind a proxy (JupyterHub is detected).
        :param graph: Deprecated and ignored, the portal shows the whole model.
        :param page: Deprecated and ignored, the portal replaces the old explorer pages.
        :param frontend: A folder with a portal build (its index.html). Defaults to the
            MODEL_PORTAL_FRONTEND environment variable, then the frontend packaged with the portal.
        """

    @overload
    def query(
            self,
            query: str,
            parameters: Optional[ParametersType] = None,
            solution_mappings: LiteralType[False] = False,
            graph: Optional[str] = None,
            return_json: LiteralType[False] = False,
            return_format: Optional[LiteralType["dataframe"]] = "dataframe",
            include_graphs: Optional[List[str]] = None,
            max_rows: Optional[int] = None,
            debug: bool = False,
            bindings: Optional[Dict[str, Union[str, IRI, Literal]]] = None,
            endpoint: Optional[str] = None,
            endpoint_token: Optional[str] = None,
            cache: bool = True,
            timeout: Optional[float] = None,
    ) -> DataFrame | list[DataFrame] | bool | None: ...
    @overload
    def query(
            self,
            query: str,
            parameters: Optional[ParametersType] = None,
            solution_mappings: bool = False,
            graph: Optional[str] = None,
            return_json: bool = False,
            *,
            return_format: LiteralType["turtle", "trig", "json"],
            include_graphs: Optional[List[str]] = None,
            max_rows: Optional[int] = None,
            debug: bool = False,
            bindings: Optional[Dict[str, Union[str, IRI, Literal]]] = None,
            endpoint: Optional[str] = None,
            endpoint_token: Optional[str] = None,
            cache: bool = True,
            timeout: Optional[float] = None,
    ) -> str: ...
    @overload
    def query(
            self,
            query: str,
            parameters: Optional[ParametersType] = None,
            solution_mappings: bool = False,
            graph: Optional[str] = None,
            *,
            return_json: LiteralType[True],
            return_format: Optional[LiteralType["turtle", "trig", "json", "dataframe"]] = "dataframe",
            include_graphs: Optional[List[str]] = None,
            max_rows: Optional[int] = None,
            debug: bool = False,
            bindings: Optional[Dict[str, Union[str, IRI, Literal]]] = None,
            endpoint: Optional[str] = None,
            endpoint_token: Optional[str] = None,
            cache: bool = True,
            timeout: Optional[float] = None,
    ) -> str: ...
    @overload
    def query(
            self,
            query: str,
            parameters: Optional[ParametersType] = None,
            *,
            solution_mappings: LiteralType[True],
            graph: Optional[str] = None,
            return_json: LiteralType[False] = False,
            return_format: Optional[LiteralType["dataframe"]] = "dataframe",
            include_graphs: Optional[List[str]] = None,
            max_rows: Optional[int] = None,
            debug: bool = False,
            bindings: Optional[Dict[str, Union[str, IRI, Literal]]] = None,
            endpoint: Optional[str] = None,
            endpoint_token: Optional[str] = None,
            cache: bool = True,
            timeout: Optional[float] = None,
    ) -> SolutionMappings | list[SolutionMappings] | bool | None: ...
    @overload
    def query(
            self,
            query: str,
            parameters: Optional[ParametersType] = None,
            solution_mappings: bool = False,
            graph: Optional[str] = None,
            return_json: bool = False,
            return_format: Optional[LiteralType["turtle", "trig", "json", "dataframe"]] = "dataframe",
            include_graphs: Optional[List[str]] = None,
            max_rows: Optional[int] = None,
            debug: bool = False,
            bindings: Optional[Dict[str, Union[str, IRI, Literal]]] = None,
            endpoint: Optional[str] = None,
            endpoint_token: Optional[str] = None,
            cache: bool = True,
            timeout: Optional[float] = None,
    ) -> DataFrame | SolutionMappings | list[DataFrame] | list[SolutionMappings] | list[DataFrame | SolutionMappings] | bool | str | None:
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
        :param return_format: One of "dataframe" (default), "json" (SELECT and ASK only), "turtle" or "trig" (CONSTRUCT only).
            "turtle" raises an error when the CONSTRUCT result has triples in named graphs (from GRAPH blocks in the template).
            Ignored when return_json is set.
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
        :param cache: Use the model's query result cache, when it is enabled (see set_query_cache). Queries with
                      parameters or debug, and queries calling NOW(), RAND(), UUID(), STRUUID(), BNODE() or user
                      defined functions, are never cached.
        :param timeout: Seconds the query may run. When they have passed, the query stops and QueryTimeoutError
                        is raised. The query is checked between its operators, while joins produce rows, and in path
                        searches, so it usually stops well within a second of its timeout. None (the default) has
                        no timeout. With endpoint, the server stops the query, and a timeout of the server itself
                        (Model.serve(query_timeout=)) also applies.
        :return: DataFrame (Select), list of DataFrames (Construct) containing results or SolutionMappings when solution_mappings is set, or a bool (Ask).
            CONSTRUCT templates can have GRAPH blocks, e.g. CONSTRUCT { ?s ex:p ?o GRAPH ex:g { ?s ex:q ?o } GRAPH ?g { ?o ex:q ?s } } WHERE { ... }.
            The DataFrames of the triples in GRAPH blocks have an extra column, graph, with the IRI of the graph.
            When the graph is a variable, there is one DataFrame per graph that it is bound to.

        """

    def set_federation(
            self,
            allowed_endpoints: Optional[List[str]] = None,
            headers: Optional[Dict[str, Dict[str, str]]] = None,
            timeout: Optional[float] = 300.0,
            max_rows: Optional[int] = None,
            batch_size: int = 1000,
            arrow_batch_size: int = 1_000_000,
            concurrency: int = 4,
            method: LiteralType["POST", "GET"] = "POST",
            bind_join: bool = True,
            allow_load: bool = False,
            arrow: bool = True,
    ) -> None:
        """
        Configure SPARQL 1.1 Federated Query (SERVICE) and SPARQL Update LOAD for the queries and
        updates of this Model (query, update, insert, validate, ...). Replaces earlier settings.

        SERVICE <endpoint> { ... } sends its pattern to the endpoint with the SPARQL 1.1 Protocol
        and joins the results with the rest of the query. SERVICE is evaluated after the local
        patterns of its group, and the distinct values they bind for the variables it shares are
        sent with the pattern, as VALUES blocks of batch_size rows (a bind join), so that the
        endpoint only returns matching solutions. Variables that are unbound in some local
        solution, or bound to blank nodes, are joined locally instead. SERVICE SILENT gives no
        error when the endpoint fails or can not be reached, and SERVICE ?endpoint queries the
        endpoints that ?endpoint is bound to. Results can be SPARQL JSON, XML or TSV. When the
        endpoint is a maplib server (Model.serve(), maplib Enterprise Edition), the results are
        sent in maplib's own Arrow format, and after the first request the bindings too, in
        batches of arrow_batch_size rows, which is much faster for large intermediate results.

        LOAD <url> [INTO GRAPH <g>] reads an RDF file from an http(s) URL (the format is taken
        from the media type of the response, else from the extension), object storage (s3://,
        gs://, az://, as with Model.read) or a file:// URL (only when listed in
        allowed_endpoints). It is refused unless allow_load is set.

        Queries sent to a served model (Model.serve) only reach the endpoints given to
        serve(service_endpoints=...), none by default.

        Usage:

        >>> m.set_federation(
        ...     allowed_endpoints=["https://graphdb.example.org/repositories/grid", "http://localhost:7878"],
        ...     headers={"https://graphdb.example.org": {"Authorization": "Bearer ..."}},
        ... )
        ... df = m.query('''
        ... PREFIX cim: <http://iec.ch/TC57/CIM100#>
        ... SELECT ?sub ?name WHERE {
        ...     ?sub a cim:Substation .
        ...     SERVICE <https://graphdb.example.org/repositories/grid> { ?sub cim:IdentifiedObject.name ?name }
        ... }''')

        :param allowed_endpoints: The endpoints and URLs that SERVICE and LOAD may reach: URLs that start with one of these,
            matched by scheme, host and port and then whole path segments, e.g. "http://localhost:7878" allows any path on
            that server. None (the default) allows any http(s) URL (and object storage for LOAD, but not file URLs),
            [] allows none.
        :param headers: Headers to send, by endpoint prefix (matched as allowed_endpoints), e.g. for authentication.
        :param timeout: Seconds that each request may take, including reading the response. None for no timeout.
        :param max_rows: The most rows that one SERVICE may receive, an error beyond (also with SILENT).
        :param batch_size: Rows of bindings per VALUES block sent to an endpoint.
        :param arrow_batch_size: Rows of bindings per request to a maplib endpoint, sent as Arrow.
        :param concurrency: The most requests to an endpoint that run at the same time.
        :param method: Send queries with HTTP POST (the default) or GET.
        :param bind_join: Send the bindings of the local solutions with the pattern. Otherwise the pattern is sent alone
            and all its solutions are joined locally.
        :param allow_load: Allow SPARQL Update LOAD.
        :param arrow: Use maplib's Arrow format with maplib endpoints. Set it to False to query them with SPARQL JSON
            results, as other endpoints.
        """

    @overload
    def query_external(
            self,
            query: str,
            endpoint: str,
            method: LiteralType["GET", "POST"] = "GET",
            solution_mappings: LiteralType[False] = False,
            return_json: LiteralType[False] = False,
            return_format: Optional[LiteralType["dataframe"]] = "dataframe",
            bindings: Optional[Dict[str, Union[str, IRI, Literal]]] = None,
            headers: Optional[List[Tuple[str, str]]] = None
    ) -> DataFrame | list[DataFrame] | None: ...
    @overload
    def query_external(
            self,
            query: str,
            endpoint: str,
            method: LiteralType["GET", "POST"] = "GET",
            solution_mappings: bool = False,
            return_json: bool = False,
            *,
            return_format: LiteralType["turtle", "json"],
            bindings: Optional[Dict[str, Union[str, IRI, Literal]]] = None,
            headers: Optional[List[Tuple[str, str]]] = None
    ) -> str: ...
    @overload
    def query_external(
            self,
            query: str,
            endpoint: str,
            method: LiteralType["GET", "POST"] = "GET",
            solution_mappings: bool = False,
            *,
            return_json: LiteralType[True],
            return_format: Optional[LiteralType["turtle", "json", "dataframe"]] = "dataframe",
            bindings: Optional[Dict[str, Union[str, IRI, Literal]]] = None,
            headers: Optional[List[Tuple[str, str]]] = None
    ) -> str: ...
    @overload
    def query_external(
            self,
            query: str,
            endpoint: str,
            method: LiteralType["GET", "POST"] = "GET",
            *,
            solution_mappings: LiteralType[True],
            return_json: LiteralType[False] = False,
            return_format: Optional[LiteralType["dataframe"]] = "dataframe",
            bindings: Optional[Dict[str, Union[str, IRI, Literal]]] = None,
            headers: Optional[List[Tuple[str, str]]] = None
    ) -> SolutionMappings | list[SolutionMappings] | None: ...
    @overload
    def query_external(
            self,
            query: str,
            endpoint: str,
            method: LiteralType["GET", "POST"] = "GET",
            solution_mappings: bool = False,
            return_json: bool = False,
            return_format: Optional[LiteralType["turtle", "json", "dataframe"]] = "dataframe",
            bindings: Optional[Dict[str, Union[str, IRI, Literal]]] = None,
            headers: Optional[List[Tuple[str, str]]] = None
    ) -> DataFrame | SolutionMappings | list[DataFrame] | list[SolutionMappings] | list[DataFrame | SolutionMappings] | str | None:
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
            parameters: Optional[ParametersType] = None,
            graph: Optional[str] = None,
            include_graphs: Optional[List[str]] = None,
            max_rows: Optional[int] = None,
            debug: bool = False,
            timeout: Optional[float] = None,
    ) -> None:
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
        :param timeout: Seconds the update may spend evaluating its WHERE clauses, counted from when it has the
                        model to itself. When they have passed, QueryTimeoutError is raised. Each operation is
                        checked before it starts and while its WHERE clause is evaluated, and writes are never
                        stopped halfway: operations of the request that were done before the timeout stay done.
        :return: None
        """

    def insert(
            self,
            query: str,
            parameters: Optional[ParametersType] = None,
            solution_mappings: bool = False,
            source_graph: Optional[str] = None,
            target_graph: Optional[str] = None,
            include_graphs: Optional[List[str]] = None,
            max_rows: Optional[int] = None,
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
            The triples of GRAPH blocks in the CONSTRUCT template, e.g. CONSTRUCT { GRAPH ex:g { ?s ex:q ?o } GRAPH ?g { ?o ex:q ?s } },
            are inserted into the graph of the block instead.
        :param include_graphs: IRIs of graphs whose triples are merged into the queried graph, as with FROM.
            None includes the transient graph (TRANSIENT_GRAPH), use [] to query only the graph itself.
        :param max_rows: Maximum estimated rows in result, helps avoid out-of-memory errors.
        :param debug: Why does my query have no results?
        :return: The newly inserted triples, keyed by predicate IRI (as SolutionMappings when solution_mappings is set).
        """

    def validate(
            self,
            shape_graph: Optional[str] = None,
            data_graph: Optional[str] = None,
            report_graph: Union[str, None] = "",
            inferences_graph: Optional[str] = None,
            include_details: bool = False,
            include_conforms: bool = False,
            only_shapes: Optional[List[str]] = None,
            deactivate_shapes: Optional[List[str]] = None,
            dry_run: bool = False,
            max_rows: Optional[int] = None,
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
        :param inferences_graph: If this IRI is supplied, the triples inferred by the SHACL rules (SHACL Advanced Features and
            SHACL 1.2 Inference Rules) are added to this named graph. When it is the data graph, the rules add their
            triples to the data graph itself; when it is None, they go to the transient graph (TRANSIENT_GRAPH).
            Validation sees the data graph together with the inferred triples.
        :param include_details: Include details of SHACL evaluation alongside the report. Currently uses a lot of memory.
        :param include_conforms: Include those results that conformed. Also applies to details.
        :param only_shapes: Validate only these shapes, None means all shapes are validated (must be IRI, cannot be used with deactivate_shapes).
        :param deactivate_shapes: Disable validation of these shapes (must be IRI, cannot be used with only_shapes).
        :param dry_run: Only find targets of shapes, but do not validate them.
        :param max_rows: Maximum estimated rows in underlying SPARQL results, helps avoid out-of-memory errors.
        :param serial: Turns off most parallell validation of shapes.
        :param max_iterations: Maximum number of rule executions for SHACL rules.
        :param debug_rules: Debug why rules returning no results do so. Included in rule log.
        :return: Validation report containing shape performance details and target counts and whether the graph conforms (report.conforms)
        """

    def read(
            self,
            file_path: Union[str, Path, bytes, bytearray, memoryview, BinaryIO, TextIO],
            format: Optional[LiteralType["ntriples", "turtle", "trig", "nquads", "rdf/xml", "cim/xml", "json-ld", "hdt"]] = None,
            base_iri: Optional[str] = None,
            parallel: Optional[bool] = None,
            checked: bool = True,
            graph: Optional[str] = None,
            replace_graph: bool = False,
            triples_batch_size: int = 10_000_000,
            known_contexts: Optional[Dict[str, str]] = None,
            geo: bool = False,
            strict: bool = False,
            storage_options: Optional[Dict[str, str]] = None,
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
            storage_options: Optional[Dict[str, str]] = None,
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
            base_iri: Optional[str] = None,
            parallel: Optional[bool] = None,
            checked: bool = True,
            graph: Optional[str] = None,
            replace_graph: bool = False,
            triples_batch_size: int = 10_000_000,
            known_contexts: Optional[Dict[str, str]] = None,
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
            model_iri: Optional[str] = None,
            version: Optional[str] = None,
            description: Optional[str] = None,
            created: Optional[str] = None,
            scenario_time: Optional[str] = None,
            modeling_authority_set: Optional[str] = None,
            prefixes: Optional[Dict[str, str]] = None,
            graph: Optional[str] = None,
            storage_options: Optional[Dict[str, str]] = None,
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
            format: Optional[LiteralType["ntriples", "turtle", "trig", "nquads", "rdf/xml", "json-ld", "hdt"]] = None,
            graph: Optional[Union[str, List[str]]] = None,
            prefixes: Optional[Dict[str, str]] = None,
            storage_options: Optional[Dict[str, str]] = None,
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
            format: Optional[LiteralType["ntriples", "turtle", "trig", "nquads", "rdf/xml", "json-ld", "hdt"]] = None,
            graph: Optional[Union[str, List[str]]] = None,
            prefixes: Optional[Dict[str, str]] = None,
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
            format: Optional[LiteralType["ntriples", "turtle", "trig", "nquads", "rdf/xml", "json-ld"]] = None,
            graph: Optional[Union[str, List[str]]] = None,
            prefixes: Optional[Dict[str, str]] = None,
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

    def truncate_graph(self, graph: Optional[str] = None) -> None:
        """
        Removes all triples associated with the given graph from the triplestore,
        including full-text search entries.

        :param graph: The IRI of the graph to truncate.
        """

    def detach_graph(self, graph: Optional[str] = None, preserve_name: bool = False) -> "Model":
        """
        Detaches and returns a named graph as their own Model object.
        The named graph is removed from the original Model.

        :param graph: The name of the graph to detach. Defaults to the default graph.
        :param preserve_name: Preserve the name of the graph in the new Model, defaults to False.
        :return: A model.
        """

    def get_predicate_iris(
            self, graph: Optional[str] = None, include_graphs: Optional[List[str]] = None
    ) -> List["IRI"]:
        """
        :param graph: The graph to get the predicate iris from.
        :param include_graphs: IRIs of graphs whose predicates are included as well, e.g. [TRANSIENT_GRAPH].
        :return: The IRIs of the predicates currently in the given graph.
        """

    def get_predicate(
            self, iri: "IRI", graph: Optional[str] = None, include_graphs: Optional[List[str]] = None
    ) -> List["SolutionMappings"]:
        """
        :param iri: The predicate IRI
        :param graph: The graph to get the predicate from.
        :param include_graphs: IRIs of graphs whose triples with the predicate are merged in, e.g. [TRANSIENT_GRAPH].
        :return: A list of the underlying tables that store a given predicate.
        """

    def create_index(self, options: Optional["IndexingOptions"] = None) -> None:
        """
        Sets the indexing options of the model and creates the indexes for the triples already in
        it: full text search, GeoSPARQL and the object indexes of the chosen predicates. Object
        indexes of predicates that are no longer chosen are dropped. Building indexes for a large
        model can take some time.

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
            source_graph: Optional[str] = None,
            target_graph: Optional[str] = None,
    ) -> None:
        """
        Copy the triples of a graph in another Model into this Model.

        Usage:
        >>> m.add_graph(other_model, source_graph="urn:graph:a", target_graph="urn:graph:b")

        :param other: The Model to copy triples from.
        :param source_graph: The IRI of the graph in other to copy from. None means the default graph.
        :param target_graph: The IRI of the graph in this Model to copy into. None means the default graph.
        """

    def set_query_cache(self, options: Optional[Union["QueryCacheOptions", bool, None]] = None) -> None:
        """
        Sets the query result cache of the model, used by Model.query and by the servers of Model.serve.
        A result is kept until a write changes the triples it can read: writes are tracked per named graph and
        predicate, so a write to another graph or another predicate keeps it. A query with a variable predicate
        (?s ?p ?o), a negated property set or a zero-length path between variables reads every predicate of its
        graphs, and GRAPH ?g reads the graph in every graph. Changes of the prefixes, the indexing options or
        the virtualizations invalidate all results. Cached results are served without waiting for writes to
        finish elsewhere in the model.

        Usage:

        >>> m.set_query_cache(QueryCacheOptions(max_entries=2048, ttl=None))
        >>> m.query(q)  # runs the query
        >>> m.query(q)  # from the cache
        >>> m.query_cache_stats()["hits"]
        1

        :param options: QueryCacheOptions, True for the default options, False or None to turn the cache off
                        (the default).
        """

    def get_query_cache(self) -> "QueryCacheOptions":
        """
        :return: The options of the query result cache.
        """

    def query_cache_stats(self) -> Dict[str, Union[int, bool]]:
        """
        Counts of the query result cache since the model was created: hits, misses, bypassed (queries that are
        never cached), inserts, evictions (for the limits), invalidations (by writes), expirations (ttl), and its
        size: entries and bytes. Also enabled, the current version stamp and how many version cells are kept.
        """

    def clear_query_cache(self) -> None:
        """
        Removes all results from the query result cache. The options stay.
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
            static_dir: Optional[Union[str, Path]] = None,
            static_path: str = "/ui",
            static_vars: Optional[Dict[str, str]] = None,
            query_log_size: int = 1000,
            block: bool = False,
            query_cache: Optional[Union["QueryCacheOptions", bool, None]] = None,
            service_endpoints: Optional[List[str]] = None,
            allow_load: bool = False,
            query_timeout: Optional[float] = None,
            max_query_timeout: Optional[float] = None,
            max_concurrent: Optional[int] = None,
            snapshot_url: Optional[str] = None,
            snapshot_interval: Optional[float] = 900,
            storage_options: Optional[Dict[str, str]] = None,
            snapshot_format: LiteralType["model", "graphs"] = "model",
            refresh_interval: Optional[float] = 10,
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
        :param query_cache: The query result cache. None (the default) uses the model's (see set_query_cache), which
                            the server shares with Model.query. QueryCacheOptions set the model's cache. True turns it
                            on with the default options if it is off. False makes the server not use it. A response
                            says what the cache did in the X-Maplib-Cache header (hit, miss or bypass), and the
                            query log and /status report its counts.
        :param service_endpoints: Endpoints that SERVICE in queries and updates sent to the server may reach, as the
            allowed_endpoints of Model.set_federation (whose other settings apply), e.g. ["https://query.wikidata.org/sparql"],
            or ["*"] for any http(s) URL. None (the default) refuses SERVICE, so that clients can not make the server send
            requests, e.g. to hosts in its internal network.
        :param allow_load: Allow SPARQL Update LOAD through the endpoints (with allow_update), of URLs in service_endpoints.
        :param query_timeout: Seconds a query or update may take, counted from when the request arrives, None (the
                              default) for no limit. A request can ask for less with the timeout parameter (seconds,
                              e.g. /sparql?query=...&timeout=5, "timeout" in Arrow API and Flight commands, or the
                              X-Maplib-Timeout header). A query that times out is stopped and answered with HTTP 503
                              (Flight: DEADLINE_EXCEEDED). Queries are also stopped when their client disconnects.
        :param max_query_timeout: The largest timeout a request may ask for, by default query_timeout. With it, a
                                  request may ask for more than query_timeout, up to this.
        :param max_concurrent: How many queries, updates and other operations on the model run at the same time,
                               64 by default. Requests beyond this wait for their turn (their timeout counts while
                               they wait).
        :param snapshot_url: Snapshot the model to this path or URL (gs://, az://, s3://, ...) as Model.serialize does,
                             e.g. the URL it was read from with Model.deserialize: every snapshot_interval seconds if
                             it changed since the last snapshot (through the endpoints or from Python), and when the
                             server stops (Server.stop, Ctrl-C with block). A URL ending in .maplib is written as a
                             single file, others as a folder; with hybrid or disk storage the maps are written as
                             DuckDB databases (raw_db). A failed snapshot is logged and retried at the next interval.
        :param snapshot_interval: Seconds between checks for changes to snapshot, 900 by default. None snapshots only
                                  when the server stops.
        :param storage_options: Object storage options for snapshot_url, as for Model.serialize.
        :param snapshot_format: "model" (the default) writes the whole model with Model.serialize, for a single
                                writer. "graphs" keeps it as one HDT file per named graph and a manifest.json at
                                snapshot_url, see GraphSnapshots: the snapshot there is restored into the model when
                                the server starts, only the graphs that changed are written (shortly after a change
                                and every snapshot_interval), and several servers can share the URL, merging each
                                other's changes.
        :param refresh_interval: With snapshot_format="graphs": seconds between reads of other servers' snapshots,
                                 10 by default, None for never.
        :return: The running Server. With snapshot_url, Server.stop() raises if the last snapshot fails.
        """

    def infer(
            self,
            rulesets: Union[str, List[str]],
            graph: Optional[str] = None,
            max_iterations: Optional[int] = 100_000,
            include_graphs: Optional[List[str]] = None,
            max_rows: Optional[int] = 100_000_000,
            debug: bool = False,
    ) -> "InferenceResult":
        """
        Run the inference rules that are provided
        :param rulesets: The ruleset (a string), or a list of rulesets. A ruleset is written in maplib's datalog syntax,
            as SPARQL CONSTRUCT queries, or in SPARQL-RL 1.2 (RULE { ... } WHERE { ... }, maplib Enterprise Edition).
        :param graph: Apply the ruleset to this graph, defaults to the default graph, or the graph specified in the rules.
        :param max_iterations: Maximum number of rule executions.
        :param include_graphs: IRIs of graphs whose triples are merged into the graph that is reasoned over, as with FROM.
            None includes the transient graph (TRANSIENT_GRAPH), use [] to query only the graph itself.
        :param max_rows: Maximum estimated rows in result, helps avoid out-of-memory errors.
        :param debug: Debugs rule bodies for executions that give no triples.
        :return: The result of inference.
        """

    def infer_rdfs(
            self,
            graph: Optional[str] = None,
    ) -> int:
        """
        RDFS Inference with rules: 2, 3, 5, 6, 8, 9, 10, 11.
        The inferred triples are put in the transient graph (TRANSIENT_GRAPH), so they are not written unless
        that graph is written explicitly.
        :param graph: Apply rdfs inferencing to this graph, defaults to the default graph, or the graph specified in the rules.
        :return number of inferred triples
        """

    def size(self, graph: Optional[str] = None) -> int:
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
        Add a virtualized database, with the templates of the resources it holds.

        This may be called several times to use several virtualized databases together, e.g. time series in
        two different databases. Each resource belongs to one virtualized database, and the parts of a query
        that match time series of a resource are sent to the database it belongs to. The results from the
        different databases are combined, so a single query can span all of them:

        >>> m.add_virtualization(VirtualizedDatabase(snowflake_db, resource_sql_map={"sensors": sql_a}),
        ...                      resources={"sensors": sensors_template})
        >>> m.add_virtualization(VirtualizedDatabase(duckdb_db, resource_sql_map={"meters": sql_b}),
        ...                      resources={"meters": meters_template})

        Joins between time series are pushed down only when the time series are in the same database.
        Adding a resource that was added before moves it to the new virtualized database.

        :param virtualized_database: We call the query-function of this object.
        :param resources: The templates associated with each resource
        """

    def add_udf(
            self,
            iri,
            func: Union[Callable[[pl.DataFrame], pl.Series], Callable[[pl.DataFrame], pl.DataFrame],],
            output_type: Union[RDFType, str, IRI],
            input_types: Optional[list[Union[RDFType, str, IRI]]] = None
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
    The Model portal running on a model, returned by Model.explore.
    """

    url: str
    "The URL of the portal."
    api_url: str
    "The URL of the portal's backend."
    sparql_url: str
    "The SPARQL endpoint of the portal, answered by the model."

    @property
    def running(self) -> bool:
        """Whether the server is running."""

    def stop(self) -> None:
        """
        Stop the server.
        """

    def wait(self, timeout: Optional[float] = None) -> bool:
        """
        Block until the server stops (or the timeout). True if it stopped.
        """


class GraphSnapshots:
    """
    A model kept in storage as one HDT file per named graph and a manifest.json (maplib Enterprise Edition),
    which several writers, e.g. the replicas of a writable Model portal, can share::

        <url>/manifest.json                          the current snapshot
        <url>/graphs/<sha256(iri)>-<ms>-<rand>.hdt   one HDT file per named graph (and the default graph)

    Which graphs changed comes from the model's writes (from Python, through Model.serve or the portal), and only
    those are written. A snapshot publishes by replacing manifest.json last, with a conditional put. A writer that
    loses the put (another writer published first) merges the graphs both sides changed three-way at the triple
    level, a triple one side added is kept, and publishes again. Blank nodes are matched by label. Graph files are
    never overwritten, and garbage collection deletes the ones no manifest names after the grace period.

    >>> s = GraphSnapshots(m, "gs://bucket/model")
    >>> s.restore()  # reads the snapshot into m
    >>> s.start()    # snapshots, refreshes and collects garbage in the background
    >>> s.stop()     # with a last snapshot

    :param model: The model.
    :param url: Where the snapshot is kept: a path or a gs://, az://, s3:// or file:// URL.
    :param storage_options: Object storage options, as for Model.serialize.
    :param writer: This writer's name in the manifest, by default host-pid-random.
    :param grace: Seconds a file no manifest names is kept (it may be another writer's merge base); tombstones of
                  removed graphs are kept twice as long. Must be longer than twice the intervals.
    :param snapshot_interval: With start(): seconds between snapshots (a snapshot also follows debounce seconds
                              after the last change).
    :param refresh_interval: With start(): seconds between reads of other writers' snapshots, None for never.
    :param debounce: With start(): seconds after the last change before a snapshot.
    :param gc_interval: With start(): seconds between garbage collections, by default max(60, grace / 4).
    :param warn_after: Snapshots slower than this many seconds are logged as warnings.
    :param clock: A function giving the time in seconds since the epoch, for tests.
    :param max_delay: With start(): changes are published at most this many seconds after the first unpublished
                      one, so steady writes are still published.
    :param jitter: With start(): each wait gets up to this many seconds more, drawn per writer, and the periodic
                   work starts at a random point in its interval, so writers changing the model together do not
                   all publish the manifest at once.
    :param write_lock: See set_write_lock.
    :param random: A function giving random numbers in [0, 1) for the jitter, for tests.
    :param lock_timeout: Seconds a refresh waits for the write lock, see set_write_lock.
    :param on_event: Called with each event (see events) after its snapshot or refresh, from the thread that ran it.
                     By default slow snapshots (warn_after) and failures are logged as warnings, with where the time
                     went, on the "maplib.graph_snapshots" logger, and other events at debug.
    """

    url: str
    writer: str
    manifest_json: str
    pending: bool

    def __init__(
            self,
            model: Model,
            url: str,
            storage_options: Optional[Dict[str, str]] = None,
            writer: Optional[str] = None,
            grace: float = 3600,
            snapshot_interval: Optional[float] = 300,
            refresh_interval: Optional[float] = 10,
            debounce: float = 2,
            gc_interval: Optional[float] = None,
            warn_after: float = 10,
            clock: Optional[Callable[[], float]] = None,
            max_delay: float = 5,
            jitter: float = 1,
            write_lock: Optional[Any] = None,
            random: Optional[Callable[[], float]] = None,
            lock_timeout: float = 0.2,
            on_event: Optional[Callable[[Dict[str, Any]], None]] = None,
    ): ...

    behind: bool
    """Whether a graph of the last manifest read is not yet applied to the model (the write lock was taken)."""

    def events(self, since: Optional[int] = None) -> List[Dict[str, Any]]:
        """The last snapshots and refreshes (at most 1000), oldest first, with an id greater than since. Each is a
        dict with "id", "kind" ("snapshot", "refresh" or "catch_up"), "writer", "time" (epoch seconds), "s"
        (duration in seconds), "ok" (and "error"), "generation" (of the manifest after it), "applied" and
        "merged" (graphs applied to the model, and of those the ones changed here too), "lock_wait_s" and
        "lock_taken" (times the write lock was taken by someone else and the model left to catch up). A snapshot
        also has "published", "attempts", "conflicts" (manifest puts lost to another writer), "files" and
        "bytes" (graph files written) and "merged_in_files"; a refresh has "changed". Snapshots and refreshes that
        found nothing to do are left out. The same objects are logged at INFO by the maplib::graph_snapshots
        tracing target."""

    def catch_up(self) -> bool:
        """Applies what the last refresh could not because the write lock was taken, without reading the manifest
        again. Returns whether the model is now up to date."""

    def set_write_lock(self, lock: Optional[Any]) -> None:
        """The lock (e.g. the Model portal's write lock, a threading.Lock) held while other writers' graphs are
        loaded into the model, so that they land between the portal's requests; None for none. Only a refresh's
        model changes are made under it, and a refresh waits at most lock_timeout for it, then leaves the model
        to catch up later. A snapshot never takes it: a graph changed here whose newer version is not yet in the
        model is merged into a new file instead. So an upload holding the lock does not hold up a snapshot."""

    def graphs(self) -> List[str]:
        """The model's non-empty graphs (without the transient graph), the default graph as ""."""

    def restore(self) -> int:
        """Reads the snapshot at the URL (if any) into the model and returns its generation, 0 when there is
        none. A graph in the snapshot adds to a graph of the same name in the model."""

    def snapshot(self) -> bool:
        """Writes the graphs that changed and publishes the manifest. Returns whether it published."""

    def refresh(self) -> bool:
        """Reads what other writers published, merging graphs changed here too. Returns whether there was
        anything new."""

    def gc(self) -> List[str]:
        """Deletes graph files no manifest names after the grace period, and returns their URLs."""

    def start(self) -> None:
        """Snapshots, refreshes and collects garbage in a background thread."""

    def stop(self, snapshot: bool = True) -> None:
        """Stops the background thread and, with snapshot, takes a last snapshot."""


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


class QueryTimeoutError(MaplibException):
    """
    Raised when a query or update runs longer than its ``timeout``, and by
    ``Model.query(endpoint=...)`` when the server stopped the query because of its timeout.
    """


class FunctionArgumentException(Exception):
    """
    Raised when a function is called with invalid arguments.
    """


def add_triples(
        source: Model,
        target: Model,
        source_graph: Optional[str] = None,
        target_graph: Optional[str] = None,
) -> None:
    """(Zero) copy the triples from one Model into another.

    The triples are copied natively, keeping datatypes and language tags. Blank nodes of the source
    get new labels in the target, so that they stay distinct from the blank nodes of the target
    (each copy gets new ones). Source and target may be the same Model, to copy a graph into another
    graph; the blank nodes are then shared by the two graphs.

    Usage:
    >>> add_triples(source, target, source_graph="urn:graph:a", target_graph="urn:graph:b")

    :param source: The source model
    :param target: The target model
    :param source_graph: The named graph in the source model to copy from. None means default graph.
    :param target_graph: The named graph in the target model to copy into. None means default graph.
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


def generate_templates(
    m: Model,
    graph: Optional[str] = None,
    prefixes: Optional[Dict[str, str]] = None,
) -> Dict[str, Template]:
    """Generate templates for instantiating the classes in an ontology

    :param m: The model where the ontology is stored. We rely on owl:Class/rdfs:Class declarations, rdfs:subClassOf, rdfs:domain, rdfs:range (also via owl:unionOf), owl:Restriction and the declared types of properties.
    :param graph: The named graph where the ontology is stored. Defaults to the default graph.
    :param prefixes: Prefixes (prefix name to namespace IRI) used to name the parameters, e.g. {"ex": "https://example.net/"}.
        Common prefixes such as rdf, rdfs, owl, xsd, skos and dcterms are known already.
        Namespaces without a prefix get the generated prefixes ns1, ns2, ...

    :return A dictionary of templates for instantiating the classes in the ontology, where the keys are the class IRIs.
        For each class there is a template with the class IRI, which also produces the rdf:type triple, and one with
        the IRI of the class suffixed by "_notype", which does not. The subject is the parameter "id",
        the other (optional) parameters are named by the prefixed IRIs of the properties, e.g. "ex:name",
        so the columns of the data frame you map should be named like that.
        Properties that have the class as their range are included in the reverse direction, with the suffix
        "_inverse" on the local name (e.g. "ex:hasPart_inverse"). Remaining name clashes get the suffixes "_2", "_3", ...
        Templates of subclasses instantiate the "_notype" templates of their superclasses,
        so all templates must be added to the Model you want to populate.

    Usage example - note that it is important to add the templates to the Model you want to populate,
    and that the columns of the data frame are named by the prefixed IRIs of the properties.
    >>> import polars as pl
    >>> from maplib import Model, generate_templates
    >>>
    >>> m_ont = Model()
    >>> m_ont.read("my_ontology.ttl")
    >>> templates = generate_templates(m_ont, prefixes={"ex": "https://example.net/"})
    >>> m = Model()
    >>> for t in templates.values():
    >>>     m.add_template(t)
    >>> df = pl.DataFrame({"id": ["https://example.net/thing1"], "ex:name": ["Thing 1"]})
    >>> m.map("https://example.net/MyClass", df)
    """


class VirtualizedDatabase:
    """
    A virtualized database implemented in Python.
    """

    def __init__(self,
                 database: Any,
                 resource_sql_map: Optional[Dict[str, Any]] = None,
                 sql_dialect: Optional[Union[LiteralType["duckdb", "snowflake", "bigquery", "databricks", "postgres"], Any]] = None,
                 pushdowns: Optional[List[LiteralType["value_conditions", "group_by", "ordering"]]] = None):
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
        For OPC UA History Access servers, see maplib.opcua.OPCUAHistoryDatabase.
        For AspenTech InfoPlus.21 historians, see maplib.ip21.IP21Database and maplib.ip21.map_ip21.

        Not available in the open source build: raises NotImplementedError.

        :param database: An object with a query method.
        :param resource_sql_map: For each resource, the SQL that gives its rows: an SQL string, an sqlalchemy Table or an sqlalchemy Select.
            It must have an id column, and a column for each of the other parameters of the resource's template.
        :param sql_dialect: The SQL dialect accepted by the query method: one of "duckdb", "snowflake", "bigquery",
            "databricks" and "postgres", or an instance of an SQLDialect from maplib.virtualization to customize the SQL.
            Defaults to the sql_dialect attribute of the database.
        :param pushdowns: The parts of the SPARQL query that may be pushed down into the database: "value_conditions",
            "group_by" and "ordering". Defaults to the pushdowns attribute of the database if it has one, otherwise all of them.
            Conditions (value_conditions) outside of a group by are evaluated again afterwards.
            With group_by, the database must compute the aggregations exactly.
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

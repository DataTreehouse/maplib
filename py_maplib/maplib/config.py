"""
maplib's configuration as triples, in the model itself.

The configuration of a model, what it virtualizes, which indexes it has and how it caches query
results, is kept in a named graph of its own, CONFIG_GRAPH, in the namespace CONFIG
(https://datatreehouse.github.io/maplib/config#), together with its common questions (saved SPARQL
queries, CommonQuestion). It travels with the model: written, serialized
and served like any other graph, and applied with apply_config. See CONFIG_GRAPH.md for the
vocabulary.

Secrets are never stored: a secret parameter names the environment variable that holds it, read
when the configuration is applied.

>>> from maplib.config import VirtualizationConfig, VirtualizationResource, write_virtualization, apply_config
>>> write_virtualization(m, VirtualizationConfig(
...     iri="urn:example:readings",
...     database="snowflake",
...     parameters={"account": "acme", "user": "maplib", "warehouse": "wh", "database": "meters", "schema": "public"},
...     secrets={"password": "SNOWFLAKE_PASSWORD"},
...     resources=[VirtualizationResource(name="readings", sql="SELECT id, ts AS timestamp, value FROM readings",
...                                       template=READINGS_TEMPLATE)],
... ))
>>> apply_config(m)  # connects and virtualizes
"""

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Mapping, Optional

if TYPE_CHECKING:
    from maplib import IndexingOptions, Model, QueryCacheOptions

CONFIG = "https://datatreehouse.github.io/maplib/config#"
"""The namespace of maplib's configuration vocabulary."""

CONFIG_GRAPH = CONFIG + "ConfigGraph"
"""The named graph that holds the configuration of a model."""

CHRONTEXT = "https://github.com/DataTreehouse/chrontext#"

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
XSD = "http://www.w3.org/2001/XMLSchema#"

#: The supported databases: their name in the configuration, and their class in the vocabulary.
#: The SQL databases are those of maplib.virtualization; "opcua" is OPC UA Historical Access
#: (maplib.opcua.OPCUAHistoryDatabase) and "cdf" Cognite Data Fusion time series.
DATABASES: Dict[str, str] = {
    "sql": "SQLDatabase",
    "duckdb": "DuckDBDatabase",
    "snowflake": "SnowflakeDatabase",
    "bigquery": "BigQueryDatabase",
    "databricks": "DatabricksDatabase",
    "timescale": "TimescaleDatabase",
    "fabric_lakehouse": "FabricLakehouseDatabase",
    "fabric_warehouse": "FabricDatabase",
    "kusto": "KustoDatabase",
    "influxdb": "InfluxDBDatabase",
    "influxdb2": "InfluxDB2Database",
    "opcua": "OPCUADatabase",
    "cdf": "CogniteDatabase",
    "osipi": "OSIPIDatabase",
    "insights_hub": "InsightsHubDatabase",
    "pi_points": "PIPointsDatabase",
    "ip21": "IP21Database",
}

#: Databases whose resources come with them: applying reads the source's own model into the graph
#: and virtualizes its time series (OSIsoft PI: the Asset Framework, through Model.map_osipi, and
#: the PI Points of the PI Data Archive, through Model.map_pi_points; AspenTech IP.21: the tags,
#: through maplib.ip21.map_ip21).
SELF_DESCRIBING = ("osipi", "pi_points", "ip21", "insights_hub")

#: The databases whose resources are queries, kept in VirtualizationResource.sql (mcfg:sql): SQL, or
#: KQL for the KQL_DATABASES.
SQL_DATABASES = (
    "sql", "duckdb", "snowflake", "bigquery", "databricks", "timescale", "fabric_lakehouse", "fabric_warehouse", "kusto", "influxdb",
)

#: The databases queried in KQL (Kusto Query Language): Azure Data Explorer and Microsoft Fabric
#: Eventhouses (KQL databases). A resource is a KQL tabular expression, e.g.
#: "Telemetry | project id=DeviceId, timestamp=Timestamp, value=Value".
KQL_DATABASES = ("kusto",)

#: The query language of the resources of each database in SQL_DATABASES.
QUERY_LANGUAGES: Dict[str, str] = {name: "kql" if name in KQL_DATABASES else "sql" for name in SQL_DATABASES}

#: The SQL databases that speak T-SQL, which has TOP instead of LIMIT.
TSQL_DATABASES = ("fabric_warehouse",)

#: The general SQL database (any database SQLAlchemy connects to, by URL) has a dialect of choice;
#: the others have their own.
GENERAL_SQL = "sql"

#: The SQL dialects chrontext translates to, for the general SQL database. Postgres also covers the
#: databases that speak its SQL (e.g. Redshift, CockroachDB). TimescaleDB has a database of its own
#: ("timescale"), whose dialect computes time buckets with time_bucket.
SQL_DIALECTS = ("duckdb", "postgres", "snowflake", "bigquery", "databricks")

#: The usual connection parameters of each database, as maplib.virtualization takes them.
DATABASE_PARAMETERS: Dict[str, List[str]] = {
    "sql": ["url"],
    # sandbox: no access to other files than the database file and the allowed directories and
    # paths (comma separated); probe connects in the same sandbox as apply_virtualization
    "duckdb": ["database", "timezone", "sandbox", "allowed_directories", "allowed_paths"],
    "snowflake": ["account", "user", "warehouse", "database", "schema", "role", "authenticator"],
    "bigquery": ["project", "location"],
    "databricks": ["server_hostname", "http_path", "catalog", "schema"],
    # TimescaleDB (PostgreSQL, with psycopg): a libpq URL (without the password), or host, port, ...
    "timescale": ["url", "host", "port", "database", "user", "sslmode"],
    # Microsoft Fabric: a lakehouse's Delta tables read from OneLake (DuckDB SQL over its tables),
    # and a warehouse or a lakehouse's SQL analytics endpoint (T-SQL)
    "fabric_lakehouse": ["workspace", "lakehouse", "authentication", "tenant_id", "client_id", "chain", "tables"],
    "fabric_warehouse": ["server", "database", "authentication", "tenant_id", "client_id", "driver"],
    # Azure Data Explorer or a Fabric Eventhouse: the cluster (query URI) and database; a service
    # principal (tenant_id, client_id and the client_secret), else the default Azure credentials
    "kusto": ["cluster", "database", "tenant_id", "client_id", "authentication"],
    # InfluxDB 3 (SQL over Arrow Flight), and InfluxDB 2 / 1.8 (Flux), where measurement, id
    # (the tag with the external ids, or _field) and field apply to every resource that does
    # not set them as resource options
    "influxdb": ["host", "database", "org"],
    "influxdb2": ["url", "org", "bucket", "measurement", "id", "field", "timeout"],
    "opcua": ["endpoint", "username", "timeout", "timestamp_parameter", "value_parameter"],
    "cdf": ["project", "base_url", "token_url", "client_id", "scopes", "client_name"],
    "osipi": [
        "url", "asset_server", "asset_database", "root_element", "username", "verify_ssl", "graph",
        "base_iri", "default_start_time", "default_end_time", "timeout",
    ],
    "pi_points": [
        "url", "data_server", "name_filter", "source_filter", "point_attributes", "username",
        "verify_ssl", "graph", "base_iri", "default_start_time", "default_end_time", "timeout",
    ],
    "ip21": [
        "url", "data_source", "tag_filter", "username", "verify_ssl", "graph", "base_iri",
        "default_start_time", "default_end_time", "read_metadata", "max_tags", "timeout",
    ],
    # Siemens Insights Hub (formerly MindSphere): the arguments of Model.map_insights_hub
    "insights_hub": [
        "host", "tenant", "client_id", "token_url", "app_name", "app_version", "user_tenant", "root_asset",
        "graph", "base_iri", "default_start_time", "default_end_time", "aggregate_pushdown", "page_size",
        "max_concurrency", "timeout", "verify_ssl",
    ],
}

#: The usual secret parameters of each database.
DATABASE_SECRETS: Dict[str, List[str]] = {
    "sql": ["password"],
    "duckdb": [],
    "snowflake": ["password", "private_key_file_pwd", "token"],
    "bigquery": [],
    "databricks": ["access_token"],
    "timescale": ["password"],
    "fabric_lakehouse": ["client_secret"],
    "fabric_warehouse": ["client_secret"],
    "kusto": ["client_secret"],
    "influxdb": ["token"],
    "influxdb2": ["token"],
    "opcua": ["password"],
    "cdf": ["client_secret"],
    "osipi": ["password", "token"],
    "pi_points": ["password", "token"],
    "ip21": ["password", "token"],
    "insights_hub": ["client_secret", "token"],
}

#: Per resource options: for CDF, how the rows of a resource are found (cognite-sdk).
RESOURCE_OPTIONS: Dict[str, List[str]] = {
    "cdf": ["id_type", "timestamp", "value"],
    # InfluxDB 2: the measurement, the column with the external ids and the field of a resource
    "influxdb2": ["measurement", "id", "field"],
}


@dataclass
class VirtualizationResource:
    """A resource of a virtualized database: its rows (SQL) and the template they instantiate."""

    name: str
    """The resource name, the value of ct:hasResource on what it virtualizes."""
    template: str
    """The template, as a stOTTR document, or the IRI of a template already in the model."""
    sql: Optional[str] = None
    """For an SQL database: the SQL giving its rows, with columns named as the template's parameters."""
    options: Dict[str, str] = field(default_factory=dict)
    """Further settings of the resource, e.g. for CDF id_type (external_id, id or instance_id)."""
    properties: Dict[str, str] = field(default_factory=dict)
    """Static properties of the objects it virtualizes: predicate IRI to object IRI, e.g. the unit of
    a measurement (cim:Measurement.unitSymbol to cim:UnitSymbol.W). Applying the virtualization
    writes them as ordinary triples on each object with ct:hasResource the resource name (in the
    default graph), replacing the values those objects had for the predicate. Constant triples in a
    virtualized template are not supported by chrontext, so this is where such facts go."""


@dataclass
class VirtualizationConfig:
    """A virtualized database: how to connect to it, and the resources it holds."""

    iri: str
    database: str
    """One of DATABASES."""
    label: Optional[str] = None
    sql_dialect: Optional[str] = None
    """Overrides the database's dialect, one of SQL_DIALECTS."""
    parameters: Dict[str, str] = field(default_factory=dict)
    """Connection parameters, e.g. account and warehouse for Snowflake."""
    secrets: Dict[str, str] = field(default_factory=dict)
    """Secret connection parameters: the parameter name and the environment variable holding it."""
    resources: List[VirtualizationResource] = field(default_factory=list)
    enabled: bool = True


@dataclass
class IndexingConfig:
    """The indexes of the model."""

    fts: bool = False
    geo: bool = False
    #: Object indexes (sorted by object) for all suitable predicates: fast lookups by value, but
    #: about twice the memory.
    object_sort_all: bool = False
    #: Object indexes for these predicates (IRIs) only.
    object_sort_some: List[str] = field(default_factory=list)

    def any(self) -> bool:
        """Whether an index beyond the defaults is configured."""
        return self.fts or self.geo or self.object_sort_all or bool(self.object_sort_some)

    def options(self) -> "IndexingOptions":
        """The IndexingOptions that create these indexes."""
        from maplib import IRI, IndexingOptions

        return IndexingOptions(
            object_sort_all=self.object_sort_all or None,
            object_sort_some=[IRI(p) for p in self.object_sort_some] or None,
            fts=self.fts or None,
            geo=self.geo or None,
        )


@dataclass
class QueryCacheConfig:
    """The query result cache of the model (Model.set_query_cache). Results are kept until a
    write changes the triples they read, per named graph and predicate."""

    enabled: bool = True
    #: The largest number of results kept.
    max_entries: int = 1024
    #: The largest total size of the results kept, in bytes.
    max_bytes: int = 256 << 20
    #: Results larger than this (bytes) are not kept.
    max_entry_bytes: int = 16 << 20
    #: Seconds after which a result is not served, None for no limit.
    ttl: Optional[float] = None
    #: Queries faster than this (seconds) are not kept.
    min_run_time: float = 0.0
    #: Also keep results of queries over virtualized databases, whose changes the cache does not
    #: see: set a ttl with this.
    cache_virtualized: bool = False

    def options(self) -> "QueryCacheOptions":
        """The QueryCacheOptions for Model.set_query_cache."""
        from maplib import QueryCacheOptions

        return QueryCacheOptions(
            enabled=self.enabled,
            max_entries=self.max_entries,
            max_bytes=self.max_bytes,
            max_entry_bytes=self.max_entry_bytes,
            ttl=self.ttl,
            min_run_time=self.min_run_time,
            cache_virtualized=self.cache_virtualized,
        )


SHACL = "http://www.w3.org/ns/shacl#"
DCTERMS = "http://purl.org/dc/terms/"

#: The query forms a common question can have: its SHACL-AF executable class and the property that
#: holds the query (sh:select, sh:construct, sh:ask). SHACL-AF has no DESCRIBE executable, so a
#: DESCRIBE query is a mcfg:SPARQLDescribeExecutable (a subclass of sh:SPARQLExecutable) in mcfg:describe.
QUERY_FORMS: Dict[str, tuple] = {
    "select": (SHACL + "SPARQLSelectExecutable", SHACL + "select"),
    "construct": (SHACL + "SPARQLConstructExecutable", SHACL + "construct"),
    "ask": (SHACL + "SPARQLAskExecutable", SHACL + "ask"),
    "describe": (CONFIG + "SPARQLDescribeExecutable", CONFIG + "describe"),
}

#: Vocabularies common questions are often about, by name: the vocabulary (namespace) a
#: question applies to, the value of mcfg:appliesTo. CIM is CIM16 (CGMES 2.4.15), ERA the vocabulary
#: of the European Union Agency for Railways (the RINF register) and OPC UA nodes as maplib maps them.
#: maplib Enterprise Edition ships common questions for each (maplib.bundled_questions).
QUESTION_DOMAINS: Dict[str, str] = {
    "cim": "http://iec.ch/TC57/2013/CIM-schema-cim16#",
    "era": "http://data.europa.eu/949/",
    "opcua": "http://opcfoundation.org/UA/",
}

#: The names of the QUESTION_DOMAINS for people.
QUESTION_DOMAIN_LABELS: Dict[str, str] = {"cim": "CIM", "era": "ERA", "opcua": "OPC UA"}

#: The IRIs of common questions start with this.
QUESTION_IRI_PREFIX = "urn:maplib:config:question:"


@dataclass
class CommonQuestion:
    """A saved SPARQL query with a name and a description: a question people ask the model often.
    In the config graph a mcfg:CommonQuestion, which is a SHACL-AF SPARQL executable (sh:select,
    sh:construct or sh:ask holds the query)."""

    iri: str
    name: str
    """dcterms:title (and rdfs:label)."""
    query: str
    """The SPARQL query, with its PREFIX declarations: SELECT, CONSTRUCT, ASK or DESCRIBE."""
    description: str = ""
    """dcterms:description: what the question answers."""
    applies_to: Optional[str] = None
    """mcfg:appliesTo: the vocabulary (namespace IRI) of the data it is about, e.g. one of
    QUESTION_DOMAINS; None for a question about any model."""
    bundled: bool = False
    """mcfg:bundled: one of the questions maplib Enterprise Edition ships (maplib.bundled_questions)."""
    order: Optional[int] = None
    """sh:order: where it is listed, lowest first (then by name)."""

    @property
    def form(self) -> str:
        """The query form: select, construct, ask or describe."""
        return query_form(self.query)

    @property
    def domain(self) -> Optional[str]:
        """The name of its domain in QUESTION_DOMAINS (e.g. "cim"), if it is one of those."""
        return next((k for k, v in QUESTION_DOMAINS.items() if v == self.applies_to), None)


def query_form(query: str) -> str:
    """The form of a SPARQL query (select, construct, ask or describe), after its prologue and
    comments. Raises ValueError for an update or anything else that is not a query."""
    import re

    pos = 0
    prologue = re.compile(r"\s*(?:#[^\n]*(?:\n|$)|PREFIX\s+[^\s:]*:\s*<[^>]*>|BASE\s*<[^>]*>)", re.IGNORECASE)
    while True:
        m = prologue.match(query, pos)
        if not m or m.end() == pos:
            break
        pos = m.end()
    word = re.match(r"\s*([A-Za-z]+)", query[pos:])
    form = word.group(1).lower() if word else ""
    if form not in QUERY_FORMS:
        raise ValueError("A common question is a SPARQL query: SELECT, CONSTRUCT, ASK or DESCRIBE")
    return form


def question_iri(name: str) -> str:
    """A new IRI for a common question, from its name and a random part."""
    import uuid

    slug = "-".join("".join(c.lower() if c.isalnum() else " " for c in name).split())[:48] or "question"
    return f"{QUESTION_IRI_PREFIX}{slug}-{uuid.uuid4().hex[:8]}"


# ── writing ─────────────────────────────────────────────────────────


def _iri(value: str) -> str:
    if any(c in value for c in '<>"{}|^`\\ \n\t'):
        raise ValueError(f"Not an IRI: {value!r}")
    return f"<{value}>"


def _literal(value: str, datatype: Optional[str] = None) -> str:
    escaped = (
        value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
    )
    return f'"{escaped}"' + (f"^^<{datatype}>" if datatype else "")


def _boolean(value: bool) -> str:
    return _literal("true" if value else "false", XSD + "boolean")


def _slug(value: str) -> str:
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in value)


def _delete(m: "Model", iri: str):
    """Removes an item of the configuration: its triples and those of its parts (<iri>/...)."""
    _iri(iri)
    m.update(
        f"""DELETE {{ GRAPH {_iri(CONFIG_GRAPH)} {{ ?s ?p ?o }} }}
WHERE {{ GRAPH {_iri(CONFIG_GRAPH)} {{ ?s ?p ?o
  FILTER(?s = {_iri(iri)} || STRSTARTS(STR(?s), {_literal(iri + "/")})) }} }}"""
    )


def write_virtualization(m: "Model", config: VirtualizationConfig):
    """Adds a virtualized database to the configuration, or replaces the one with its IRI."""
    if config.database not in DATABASES:
        raise ValueError(f"Unknown database {config.database!r}, use one of {sorted(DATABASES)}")
    if config.database == GENERAL_SQL:
        if config.sql_dialect not in SQL_DIALECTS:
            raise ValueError(f"The general SQL database needs a dialect, one of {SQL_DIALECTS}")
        if not config.parameters.get("url"):
            raise ValueError("The general SQL database needs its SQLAlchemy URL (the url parameter)")
    elif config.sql_dialect is not None:
        raise ValueError(
            f"A {config.database} database has its own SQL dialect; use the general SQL database to choose one"
        )
    names = [r.name for r in config.resources]
    if len(set(names)) != len(names):
        raise ValueError("The resources of a virtualization must have different names")
    for r in config.resources:
        if config.database in SQL_DATABASES and not (r.sql or "").strip():
            raise ValueError(f"The resource {r.name} of a {config.database} database needs {QUERY_LANGUAGES[config.database].upper()}")
        if config.database == "influxdb2" and not (r.options.get("measurement") or config.parameters.get("measurement")):
            raise ValueError(f"The resource {r.name} of an InfluxDB 2 database needs a measurement")
        for predicate, obj in r.properties.items():
            if ":" not in predicate or ":" not in obj:
                raise ValueError(f"The properties of resource {r.name} must be absolute IRIs: {predicate} {obj}")
            _iri(predicate), _iri(obj)
    s = _iri(config.iri)
    lines = [
        f"{s} <{RDF_TYPE}> <{CONFIG}{DATABASES[config.database]}> .",
        f"{s} <{RDF_TYPE}> <{CONFIG}Virtualization> .",
        f"{s} <{CONFIG}enabled> {_boolean(config.enabled)} .",
    ]
    if config.label:
        lines.append(f"{s} <{RDFS_LABEL}> {_literal(config.label)} .")
    if config.sql_dialect:
        lines.append(f"{s} <{CONFIG}sqlDialect> {_literal(config.sql_dialect)} .")
    for key, value in sorted(config.parameters.items()):
        p = _iri(f"{config.iri}/parameter/{_slug(key)}")
        lines += [
            f"{s} <{CONFIG}parameter> {p} .",
            f"{p} <{CONFIG}key> {_literal(key)} .",
            f"{p} <{CONFIG}value> {_literal(str(value))} .",
        ]
    for key, variable in sorted(config.secrets.items()):
        p = _iri(f"{config.iri}/secret/{_slug(key)}")
        lines += [
            f"{s} <{CONFIG}secret> {p} .",
            f"{p} <{CONFIG}key> {_literal(key)} .",
            f"{p} <{CONFIG}environmentVariable> {_literal(variable)} .",
        ]
    for r in config.resources:
        p = _iri(f"{config.iri}/resource/{_slug(r.name)}")
        lines += [
            f"{s} <{CONFIG}resource> {p} .",
            f"{p} <{RDF_TYPE}> <{CONFIG}Resource> .",
            f"{p} <{CONFIG}resourceName> {_literal(r.name)} .",
        ]
        if r.sql:
            lines.append(f"{p} <{CONFIG}sql> {_literal(r.sql)} .")
        for i, (predicate, obj) in enumerate(sorted(r.properties.items())):
            o = _iri(f"{config.iri}/resource/{_slug(r.name)}/property/{i}")
            lines += [
                f"{p} <{CONFIG}property> {o} .",
                f"{o} <{CONFIG}predicate> {_iri(predicate)} .",
                f"{o} <{CONFIG}object> {_iri(obj)} .",
            ]
        for key, value in sorted(r.options.items()):
            o = _iri(f"{config.iri}/resource/{_slug(r.name)}/option/{_slug(key)}")
            lines += [
                f"{p} <{CONFIG}option> {o} .",
                f"{o} <{CONFIG}key> {_literal(key)} .",
                f"{o} <{CONFIG}value> {_literal(str(value))} .",
            ]
        template = r.template.strip()
        if template.startswith("http") or template.startswith("urn:"):
            lines.append(f"{p} <{CONFIG}template> {_iri(template)} .")
        else:
            lines.append(f"{p} <{CONFIG}templateDocument> {_literal(r.template)} .")
    _delete(m, config.iri)
    m.reads("\n".join(lines), format="ntriples", graph=CONFIG_GRAPH)


def remove_virtualization(m: "Model", iri: str):
    """Removes a virtualized database from the configuration (a virtualization already applied stays)."""
    _delete(m, iri)


def write_indexing(m: "Model", config: IndexingConfig):
    """Sets the indexes of the model in the configuration."""
    s = _iri(CONFIG + "indexing")
    _delete(m, CONFIG + "indexing")
    m.reads(
        "\n".join(
            [
                f"{s} <{RDF_TYPE}> <{CONFIG}Indexing> .",
                f"{s} <{CONFIG}fts> {_boolean(config.fts)} .",
                f"{s} <{CONFIG}geo> {_boolean(config.geo)} .",
                f"{s} <{CONFIG}objectIndexAll> {_boolean(config.object_sort_all)} .",
                *(f"{s} <{CONFIG}objectIndexPredicate> {_iri(p)} ." for p in sorted(set(config.object_sort_some))),
            ]
        ),
        format="ntriples",
        graph=CONFIG_GRAPH,
    )


def write_query_cache(m: "Model", config: QueryCacheConfig):
    """Sets the query result cache of the model in the configuration."""
    for name in ["max_entries", "max_bytes", "max_entry_bytes"]:
        if getattr(config, name) < 0:
            raise ValueError(f"{name} must not be negative")
    for name in ["ttl", "min_run_time"]:
        v = getattr(config, name)
        if v is not None and not v >= 0:
            raise ValueError(f"{name} must be a non-negative number of seconds")
    s = _iri(CONFIG + "queryCache")
    integer = lambda v: _literal(str(int(v)), XSD + "integer")  # noqa: E731
    double = lambda v: _literal(repr(float(v)), XSD + "double")  # noqa: E731
    _delete(m, CONFIG + "queryCache")
    m.reads(
        "\n".join(
            [
                f"{s} <{RDF_TYPE}> <{CONFIG}QueryCache> .",
                f"{s} <{CONFIG}enabled> {_boolean(config.enabled)} .",
                f"{s} <{CONFIG}maxEntries> {integer(config.max_entries)} .",
                f"{s} <{CONFIG}maxBytes> {integer(config.max_bytes)} .",
                f"{s} <{CONFIG}maxEntryBytes> {integer(config.max_entry_bytes)} .",
                *([f"{s} <{CONFIG}ttl> {double(config.ttl)} ."] if config.ttl is not None else []),
                f"{s} <{CONFIG}minRunTime> {double(config.min_run_time)} .",
                f"{s} <{CONFIG}cacheVirtualized> {_boolean(config.cache_virtualized)} .",
            ]
        ),
        format="ntriples",
        graph=CONFIG_GRAPH,
    )


def remove_query_cache(m: "Model"):
    """Removes the query result cache from the configuration (the model's setting stays)."""
    _delete(m, CONFIG + "queryCache")


def write_question(m: "Model", question: CommonQuestion):
    """Adds a common question to the configuration, or replaces the one with its IRI."""
    if not question.name.strip():
        raise ValueError("A common question needs a name")
    if not question.query.strip():
        raise ValueError("A common question needs a query")
    executable, holder = QUERY_FORMS[query_form(question.query)]
    s = _iri(question.iri)
    lines = [
        f"{s} <{RDF_TYPE}> <{CONFIG}CommonQuestion> .",
        f"{s} <{RDF_TYPE}> <{executable}> .",
        f"{s} <{DCTERMS}title> {_literal(question.name.strip())} .",
        f"{s} <{RDFS_LABEL}> {_literal(question.name.strip())} .",
        f"{s} <{holder}> {_literal(question.query)} .",
    ]
    if question.description.strip():
        lines.append(f"{s} <{DCTERMS}description> {_literal(question.description.strip())} .")
    if question.applies_to:
        lines.append(f"{s} <{CONFIG}appliesTo> {_iri(question.applies_to)} .")
    if question.bundled:
        lines.append(f"{s} <{CONFIG}bundled> {_boolean(True)} .")
    if question.order is not None:
        lines.append(f"{s} <{SHACL}order> {_literal(str(int(question.order)), XSD + 'integer')} .")
    _delete(m, question.iri)
    m.reads("\n".join(lines), format="ntriples", graph=CONFIG_GRAPH)


def remove_question(m: "Model", iri: str):
    """Removes a common question from the configuration."""
    _delete(m, iri)


# ── reading ─────────────────────────────────────────────────────────

_PREFIXES = f"PREFIX mcfg: <{CONFIG}>\nPREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"


def _rows(m: "Model", query: str) -> List[dict]:
    df = m.query(_PREFIXES + query)
    return [] if df is None else df.to_dicts()


def _strip(value):
    if value is None:
        return None
    value = str(value)
    if value.startswith("<") and value.endswith(">"):
        return value[1:-1]
    return value


def read_virtualizations(m: "Model") -> List[VirtualizationConfig]:
    """The virtualized databases in the configuration."""
    g = _iri(CONFIG_GRAPH)
    configs: Dict[str, VirtualizationConfig] = {}
    types = {f"{CONFIG}{cls}": name for name, cls in DATABASES.items()}
    for r in _rows(
        m,
        f"""SELECT ?v ?type ?label ?dialect ?enabled WHERE {{ GRAPH {g} {{
  ?v a mcfg:Virtualization ; a ?type .
  OPTIONAL {{ ?v rdfs:label ?label }}
  OPTIONAL {{ ?v mcfg:sqlDialect ?dialect }}
  OPTIONAL {{ ?v mcfg:enabled ?enabled }}
}} }}""",
    ):
        database = types.get(_strip(r["type"]))
        if database is None:
            continue
        iri = _strip(r["v"])
        enabled = r.get("enabled")
        configs[iri] = VirtualizationConfig(
            iri=iri,
            database=database,
            label=_strip(r.get("label")),
            sql_dialect=_strip(r.get("dialect")),
            enabled=enabled is None or enabled is True or str(enabled).lower().startswith(("true", '"true')),
        )
    for r in _rows(m, f"SELECT ?v ?key ?value WHERE {{ GRAPH {g} {{ ?v mcfg:parameter ?p . ?p mcfg:key ?key ; mcfg:value ?value }} }}"):
        c = configs.get(_strip(r["v"]))
        if c:
            c.parameters[_strip(r["key"])] = _strip(r["value"])
    for r in _rows(
        m, f"SELECT ?v ?key ?var WHERE {{ GRAPH {g} {{ ?v mcfg:secret ?p . ?p mcfg:key ?key ; mcfg:environmentVariable ?var }} }}"
    ):
        c = configs.get(_strip(r["v"]))
        if c:
            c.secrets[_strip(r["key"])] = _strip(r["var"])
    for r in _rows(
        m,
        f"""SELECT ?v ?name ?sql ?template ?document WHERE {{ GRAPH {g} {{
  ?v mcfg:resource ?r . ?r mcfg:resourceName ?name .
  OPTIONAL {{ ?r mcfg:sql ?sql }}
  OPTIONAL {{ ?r mcfg:template ?template }}
  OPTIONAL {{ ?r mcfg:templateDocument ?document }}
}} }} ORDER BY ?name""",
    ):
        c = configs.get(_strip(r["v"]))
        if c:
            c.resources.append(
                VirtualizationResource(
                    name=_strip(r["name"]),
                    sql=_strip(r.get("sql")),
                    template=_strip(r.get("document")) or _strip(r.get("template")) or "",
                )
            )
    for r in _rows(
        m,
        f"""SELECT ?v ?name ?key ?value WHERE {{ GRAPH {g} {{
  ?v mcfg:resource ?r . ?r mcfg:resourceName ?name ; mcfg:option ?o . ?o mcfg:key ?key ; mcfg:value ?value .
}} }}""",
    ):
        c = configs.get(_strip(r["v"]))
        for res in c.resources if c else []:
            if res.name == _strip(r["name"]):
                res.options[_strip(r["key"])] = _strip(r["value"])
    for r in _rows(
        m,
        f"""SELECT ?v ?name ?predicate ?object WHERE {{ GRAPH {g} {{
  ?v mcfg:resource ?r . ?r mcfg:resourceName ?name ; mcfg:property ?o . ?o mcfg:predicate ?predicate ; mcfg:object ?object .
}} }}""",
    ):
        c = configs.get(_strip(r["v"]))
        for res in c.resources if c else []:
            if res.name == _strip(r["name"]):
                res.properties[_strip(r["predicate"])] = _strip(r["object"])
    return sorted(configs.values(), key=lambda c: c.iri)


def read_indexing(m: "Model") -> IndexingConfig:
    """The indexes in the configuration (none when it does not say)."""
    rows = _rows(
        m,
        f"SELECT ?fts ?geo ?all WHERE {{ GRAPH {_iri(CONFIG_GRAPH)} {{ ?i a mcfg:Indexing . "
        "OPTIONAL { ?i mcfg:fts ?fts } OPTIONAL { ?i mcfg:geo ?geo } OPTIONAL { ?i mcfg:objectIndexAll ?all } } } LIMIT 1",
    )
    if not rows:
        return IndexingConfig()
    truthy = lambda v: v is True or (v is not None and "true" in str(v).lower())  # noqa: E731
    predicates = _rows(
        m,
        f"SELECT ?p WHERE {{ GRAPH {_iri(CONFIG_GRAPH)} {{ ?i a mcfg:Indexing ; mcfg:objectIndexPredicate ?p }} }}",
    )
    return IndexingConfig(
        fts=truthy(rows[0].get("fts")),
        geo=truthy(rows[0].get("geo")),
        object_sort_all=truthy(rows[0].get("all")),
        object_sort_some=sorted(_strip(r["p"]) for r in predicates),
    )


def read_questions(m: "Model", domain: Optional[str] = None) -> List[CommonQuestion]:
    """
    The common questions in the configuration, in their order (sh:order, then name). With a domain
    (one of QUESTION_DOMAINS, or a vocabulary IRI), only those that apply to it and those that apply
    to any model.
    """
    holders = " ".join(f"<{holder}>" for _, holder in QUERY_FORMS.values())
    rows = _rows(
        m,
        f"""PREFIX dcterms: <{DCTERMS}>
PREFIX sh: <{SHACL}>
SELECT ?q ?title ?label ?query ?description ?appliesTo ?bundled ?order WHERE {{ GRAPH {_iri(CONFIG_GRAPH)} {{
  ?q a mcfg:CommonQuestion .
  OPTIONAL {{ VALUES ?holder {{ {holders} }} ?q ?holder ?query }}
  OPTIONAL {{ ?q dcterms:title ?title }}
  OPTIONAL {{ ?q rdfs:label ?label }}
  OPTIONAL {{ ?q dcterms:description ?description }}
  OPTIONAL {{ ?q mcfg:appliesTo ?appliesTo }}
  OPTIONAL {{ ?q mcfg:bundled ?bundled }}
  OPTIONAL {{ ?q sh:order ?order }}
}} }}""",
    )
    wanted = QUESTION_DOMAINS.get(domain, domain) if domain else None
    questions: Dict[str, CommonQuestion] = {}
    for r in rows:
        iri = _strip(r["q"])
        if iri in questions or not r.get("query"):
            continue
        applies_to = _strip(r.get("appliesTo"))
        if wanted and applies_to not in (None, wanted):
            continue
        order = r.get("order")
        bundled = r.get("bundled")
        questions[iri] = CommonQuestion(
            iri=iri,
            name=_strip(r.get("title")) or _strip(r.get("label")) or iri,
            query=_strip(r["query"]),
            description=_strip(r.get("description")) or "",
            applies_to=applies_to,
            bundled=bundled is True or "true" in str(bundled).lower(),
            order=None if order is None else int(float(str(order).split("^^")[0].strip('"'))),
        )
    return sorted(
        questions.values(), key=lambda q: (q.order is None, q.applies_to or "", q.order or 0, q.name.lower(), q.iri)
    )


def read_query_cache(m: "Model") -> Optional[QueryCacheConfig]:
    """The query result cache in the configuration, None when it does not say."""
    rows = _rows(
        m,
        f"SELECT ?enabled ?entries ?bytes ?entry ?ttl ?min ?virt WHERE {{ GRAPH {_iri(CONFIG_GRAPH)} {{ ?c a mcfg:QueryCache . "
        "OPTIONAL { ?c mcfg:enabled ?enabled } OPTIONAL { ?c mcfg:maxEntries ?entries } "
        "OPTIONAL { ?c mcfg:maxBytes ?bytes } OPTIONAL { ?c mcfg:maxEntryBytes ?entry } "
        "OPTIONAL { ?c mcfg:ttl ?ttl } OPTIONAL { ?c mcfg:minRunTime ?min } "
        "OPTIONAL { ?c mcfg:cacheVirtualized ?virt } } } LIMIT 1",
    )
    if not rows:
        return None
    r = rows[0]
    d = QueryCacheConfig()
    truthy = lambda v, default: default if v is None else (v is True or "true" in str(v).lower())  # noqa: E731

    def number(v, default, kind):
        if v is None:
            return default
        text = str(v).split("^^")[0].strip('"')
        return kind(float(text)) if kind is int else kind(text)

    return QueryCacheConfig(
        enabled=truthy(r.get("enabled"), d.enabled),
        max_entries=number(r.get("entries"), d.max_entries, int),
        max_bytes=number(r.get("bytes"), d.max_bytes, int),
        max_entry_bytes=number(r.get("entry"), d.max_entry_bytes, int),
        ttl=number(r.get("ttl"), None, float),
        min_run_time=number(r.get("min"), d.min_run_time, float),
        cache_virtualized=truthy(r.get("virt"), d.cache_virtualized),
    )


# ── applying ────────────────────────────────────────────────────────


def apply_query_cache(m: "Model", config: Optional[QueryCacheConfig] = None):
    """Sets the model's query result cache to the configuration (read from the config graph when
    not given). Does nothing when the configuration has none."""
    config = config if config is not None else read_query_cache(m)
    if config is not None:
        m.set_query_cache(config.options())


def _connection(config: VirtualizationConfig, environ: Mapping[str, str]) -> Dict[str, str]:
    """The connection parameters, with the secrets from the environment."""
    kwargs: Dict[str, str] = dict(config.parameters)
    for key, variable in config.secrets.items():
        if variable not in environ:
            raise KeyError(f"The environment variable {variable} (the {key} of {config.iri}) is not set")
        kwargs[key] = environ[variable]
    return kwargs


class SQLAlchemyDatabase:
    """Any database SQLAlchemy connects to, by URL, queried with the given SQL dialect."""

    def __init__(self, url: str, sql_dialect: str, password: Optional[str] = None):
        from sqlalchemy import create_engine
        from sqlalchemy.engine import make_url

        u = make_url(url)
        if password is not None:
            u = u.set(password=password)
        self.engine = create_engine(u)
        self.sql_dialect = sql_dialect

    def query(self, sql: str):
        import polars as pl

        with self.engine.connect() as connection:
            return pl.read_database(sql, connection)


def _cognite_client(kwargs: Dict[str, str]):
    from cognite.client import ClientConfig, CogniteClient
    from cognite.client.credentials import OAuthClientCredentials

    base_url = kwargs["base_url"].rstrip("/")
    scopes = kwargs.get("scopes") or f"{base_url}/.default"
    credentials = OAuthClientCredentials(
        token_url=kwargs["token_url"],
        client_id=kwargs["client_id"],
        client_secret=kwargs["client_secret"],
        scopes=[s for s in scopes.replace(",", " ").split() if s],
    )
    return CogniteClient(
        ClientConfig(
            client_name=kwargs.get("client_name") or "maplib",
            project=kwargs["project"],
            base_url=base_url,
            credentials=credentials,
        )
    )


def _influxdb2_database(config: VirtualizationConfig, kwargs: Dict[str, str]):
    """An InfluxDB2Database: the measurement, id and field parameters apply to the resources without them."""
    from maplib import virtualization

    keys = RESOURCE_OPTIONS["influxdb2"]
    args: dict = {k: v for k, v in kwargs.items() if k not in keys}
    if "timeout" in args:
        args["timeout"] = int(float(args["timeout"]))
    defaults = {k: kwargs[k] for k in keys if kwargs.get(k)}
    resources = {}
    for r in config.resources:
        resource = {**defaults, **{k: v for k, v in r.options.items() if k in keys and v}}
        if "measurement" not in resource:
            raise ValueError(f"The resource {r.name} of {config.iri} needs a measurement")
        resources[r.name] = resource
    return virtualization.InfluxDB2Database(resources=resources, **args)


def _virtualized_database(config: VirtualizationConfig, environ: Mapping[str, str]):
    """The VirtualizedDatabase of a configuration, connected."""
    from maplib import VirtualizedDatabase

    kwargs = _connection(config, environ)
    if config.database == GENERAL_SQL:
        database = SQLAlchemyDatabase(kwargs["url"], config.sql_dialect, password=kwargs.get("password"))
        return VirtualizedDatabase(database, resource_sql_map={r.name: r.sql for r in config.resources})
    if config.database in SQL_DATABASES:
        from maplib import virtualization

        database = getattr(virtualization, DATABASES[config.database])(**kwargs)
        return VirtualizedDatabase(database, resource_sql_map={r.name: r.sql for r in config.resources})
    if config.database == "influxdb2":
        return VirtualizedDatabase(_influxdb2_database(config, kwargs))
    if config.database == "opcua":
        from maplib.opcua import OPCUAHistoryDatabase

        if "timeout" in kwargs:
            kwargs["timeout"] = float(kwargs["timeout"])
        return VirtualizedDatabase(database=OPCUAHistoryDatabase(**kwargs))
    if config.database == "cdf":
        # only cognite-sdk's own options: others (e.g. the Model portal's form state) are not its
        resource_map = {
            r.name: {k: v for k, v in r.options.items() if k in RESOURCE_OPTIONS["cdf"]}
            for r in config.resources
        }
        return VirtualizedDatabase.Cognite(_cognite_client(kwargs), resource_map)
    raise ValueError(f"Unknown database {config.database!r}")


def _templates(m: "Model", config: VirtualizationConfig) -> Dict[str, object]:
    templates = {}
    for r in config.resources:
        text = r.template.strip()
        if text.startswith("http") or text.startswith("urn:"):
            iri = text
        else:
            # the template's IRI from the document itself, so that applying again (the template is
            # in the model already, and add_template replaces it) finds it too
            from maplib import Model

            scratch = Model()
            scratch.add_template(r.template)
            defined = scratch.get_templates()
            if not defined:
                raise ValueError(f"The template of resource {r.name} of {config.iri} defines no template")
            iri = str(defined[-1].iri)
            m.add_template(r.template)
        match = [t for t in m.get_templates() if str(t.iri).strip("<>") == iri.strip("<>")]
        if not match:
            raise ValueError(f"The model has no template {iri} (resource {r.name} of {config.iri})")
        templates[r.name] = match[0]
    return templates


def _osipi_arguments(kwargs: Dict[str, str]) -> dict:
    args: dict = dict(kwargs)
    if "verify_ssl" in args:
        args["verify_ssl"] = str(args["verify_ssl"]).lower() not in ("false", "0", "no")
    if "timeout" in args:
        args["timeout"] = float(args["timeout"])
    if "point_attributes" in args:
        # stored as one string, e.g. "pointsource, location1"
        args["point_attributes"] = [a for a in str(args["point_attributes"]).replace(",", " ").split() if a]
    return args


def _pi_get(kwargs: Dict[str, str], path: str, query: Optional[Dict[str, str]] = None):
    """GET a path of the PI Web API with the configured credentials: (HTTP status, JSON)."""
    import base64
    import json
    import ssl
    import urllib.parse
    import urllib.request

    url = kwargs["url"].rstrip("/") + "/" + path
    if query:
        url += "?" + urllib.parse.urlencode(query)
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    if kwargs.get("token"):
        req.add_header("Authorization", f"Bearer {kwargs['token']}")
    elif kwargs.get("username"):
        basic = base64.b64encode(f"{kwargs['username']}:{kwargs.get('password', '')}".encode()).decode()
        req.add_header("Authorization", f"Basic {basic}")
    context = None
    if str(kwargs.get("verify_ssl", "true")).lower() in ("false", "0", "no"):
        context = ssl._create_unverified_context()
    with urllib.request.urlopen(req, timeout=float(kwargs.get("timeout") or 15), context=context) as r:
        body = r.read()
        return r.status, (json.loads(body) if body else None)


_BOOLEAN_FALSE = ("false", "0", "no")


def _ip21_arguments(kwargs: Dict[str, str]) -> dict:
    """The arguments of maplib.ip21.map_ip21 from the stored (string) parameters."""
    args: dict = dict(kwargs)
    for key in ("verify_ssl", "read_metadata"):
        if key in args:
            args[key] = str(args[key]).lower() not in _BOOLEAN_FALSE
    if "max_tags" in args:
        args["max_tags"] = int(args["max_tags"])
    if "timeout" in args:
        args["timeout"] = float(args["timeout"])
    return args


def _insights_hub_arguments(kwargs: Dict[str, str]) -> dict:
    args: dict = dict(kwargs)
    for key in ("verify_ssl", "aggregate_pushdown"):
        if key in args:
            args[key] = str(args[key]).lower() in ("true", "1", "yes")
    for key in ("page_size", "max_concurrency"):
        if key in args:
            args[key] = int(args[key])
    if "timeout" in args:
        args["timeout"] = float(args["timeout"])
    return args


def _insights_hub_token(kwargs: Dict[str, str], timeout: float) -> str:
    """An access token for Insights Hub: the given token, or one for service or app credentials."""
    import base64
    import json
    import urllib.request
    from urllib.parse import urlparse

    if kwargs.get("token"):
        return kwargs["token"]
    host = kwargs["host"].strip().rstrip("/")
    base = host if "://" in host else "https://" + host
    basic = base64.b64encode(f"{kwargs['client_id']}:{kwargs['client_secret']}".encode()).decode()
    if kwargs.get("app_name"):
        body = json.dumps({
            "appName": kwargs["app_name"], "appVersion": kwargs.get("app_version", ""),
            "hostTenant": kwargs["tenant"], "userTenant": kwargs.get("user_tenant") or kwargs["tenant"],
        }).encode()
        req = urllib.request.Request(base + "/api/technicaltokenmanager/v3/oauth/token", data=body, method="POST",
                                     headers={"X-SPACE-AUTH-KEY": "Basic " + basic,
                                              "Content-Type": "application/json"})
    else:
        token_url = kwargs.get("token_url")
        if not token_url:
            hostname = urlparse(base).hostname or ""
            if not hostname.startswith("gateway."):
                raise ValueError(f"Cannot derive the token endpoint from the host {host}; give the token_url")
            token_url = f"https://{kwargs['tenant']}.piam.{hostname[len('gateway.'):]}/oauth/token"
        req = urllib.request.Request(token_url, data=b"grant_type=client_credentials", method="POST",
                                     headers={"Authorization": "Basic " + basic,
                                              "Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())["access_token"]


def apply_virtualization(m: "Model", config: VirtualizationConfig, environ: Optional[Mapping[str, str]] = None):
    """Connects to a configured database and virtualizes its resources in the model."""
    environ = os.environ if environ is None else environ
    if config.database == "osipi":
        # reads the Asset Framework and virtualizes the time series of its attributes
        m.map_osipi(**_osipi_arguments(_connection(config, environ)))
        return
    if config.database == "insights_hub":
        # reads the assets, aspects and variables and virtualizes the time series of the variables
        m.map_insights_hub(**_insights_hub_arguments(_connection(config, environ)))
        return
    if config.database == "pi_points":
        # reads the PI Points of the PI Data Archive and virtualizes their recorded values
        m.map_pi_points(**_osipi_arguments(_connection(config, environ)))
        return
    if config.database == "ip21":
        # searches for the tags and virtualizes their history
        from maplib.ip21 import map_ip21

        map_ip21(m, **_ip21_arguments(_connection(config, environ)))
        return
    database = _virtualized_database(config, environ)
    m.add_virtualization(database, resources=_templates(m, config))
    apply_properties(m, config)


def apply_properties(m: "Model", config: VirtualizationConfig):
    """
    Writes the static properties of the resources (VirtualizationResource.properties) on the
    objects they virtualize, those with ct:hasResource the resource name, in the default graph.
    A property replaces the values the objects had for its predicate, so changing a unit and
    applying again leaves only the new one.
    """
    for r in config.resources:
        if not r.properties:
            continue
        name = _literal(r.name)
        for predicate, obj in sorted(r.properties.items()):
            m.update(
                f"""DELETE {{ ?s {_iri(predicate)} ?o }} WHERE {{ ?s <{CHRONTEXT}hasResource> {name} . ?s {_iri(predicate)} ?o }}"""
            )
            m.update(f"""INSERT {{ ?s {_iri(predicate)} {_iri(obj)} }} WHERE {{ ?s <{CHRONTEXT}hasResource> {name} }}""")


def apply_config(m: "Model", environ: Optional[Mapping[str, str]] = None) -> List[str]:
    """
    Applies the configuration in the model's config graph: creates the configured indexes, sets
    the query result cache and virtualizes the enabled databases.

    :param m: The model.
    :param environ: Where the secrets are read from, by default the environment variables.
    :return: The IRIs of the virtualizations applied.
    """
    indexing = read_indexing(m)
    if indexing.any():
        m.create_index(indexing.options())
    apply_query_cache(m)
    applied = []
    for config in read_virtualizations(m):
        if config.enabled:
            apply_virtualization(m, config, environ)
            applied.append(config.iri)
    return applied


# ── checking ────────────────────────────────────────────────────────


def probe(config: VirtualizationConfig, sql: Optional[str] = None, environ: Optional[Mapping[str, str]] = None) -> dict:
    """
    Checks a configuration against the database itself, without changing any model: connects and,
    for an SQL database, runs the SQL (LIMIT 5, in KQL: take 5) and returns its columns and a sample; for Cognite
    Data Fusion inspects the token and lists some time series; for InfluxDB without SQL, and InfluxDB 2,
    lists the measurements; for OPC UA connects to the endpoint; for IP.21 searches for some tags.

    :return: A dict with "message", and "columns" (name and polars type) and "sample" (rows) when
        there are rows to show.
    """
    environ = os.environ if environ is None else environ
    kwargs = _connection(config, environ)
    if config.database in SQL_DATABASES:
        if config.database == GENERAL_SQL:
            database = SQLAlchemyDatabase(kwargs["url"], config.sql_dialect or "postgres", password=kwargs.get("password"))
        else:
            from maplib import virtualization

            database = getattr(virtualization, DATABASES[config.database])(**kwargs)
        if config.database in KQL_DATABASES:
            query = f"({sql.strip().rstrip(';')})\n| take 5" if sql and sql.strip() else "print ok = 1"
            df = database.query(query)
            return {
                "message": f"Connected; the query gave {df.width} columns" if sql else "Connected",
                "columns": [{"name": c, "type": str(t)} for c, t in df.schema.items()],
                "sample": [[None if v is None else str(v) for v in row] for row in df.rows()],
            }
        if config.database == "fabric_lakehouse" and not (sql and sql.strip()):
            tables = sorted(database.table_urls)
            return {
                "message": f"Connected; the lakehouse has {len(tables)} tables",
                "columns": [{"name": "table", "type": "String"}],
                "sample": [[t] for t in tables],
            }
        if config.database == "influxdb" and not (sql and sql.strip()):
            df = database.query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'iox' ORDER BY table_name"
            )
            measurements = df["table_name"].to_list()
            return {
                "message": f"Connected; the database has {len(measurements)} measurements",
                "columns": [{"name": "measurement", "type": "String"}],
                "sample": [[t] for t in measurements],
            }
        if not (sql and sql.strip()):
            query = "SELECT 1 AS ok"
        elif config.database in TSQL_DATABASES:
            query = f"SELECT TOP 5 * FROM ({sql.strip().rstrip(';')}) AS q"
        else:
            query = f"SELECT * FROM ({sql.strip().rstrip(';')}) AS q LIMIT 5"
        try:
            df = database.query(query)
        finally:
            # databases with a connection of their own (TimescaleDB) are closed after the check
            close = getattr(database, "close", None)
            if callable(close):
                close()
        return {
            "message": f"Connected; the query gave {df.width} columns" if sql else "Connected",
            "columns": [{"name": c, "type": str(t)} for c, t in df.schema.items()],
            "sample": [[None if v is None else str(v) for v in row] for row in df.rows()],
        }
    if config.database == "cdf":
        client = _cognite_client(kwargs)
        token = client.iam.token.inspect()
        projects = [p.url_name for p in getattr(token, "projects", []) or []]
        if kwargs["project"] not in projects:
            raise PermissionError(
                f"The credentials work, but give no access to the project {kwargs['project']} "
                f"(they reach: {', '.join(projects) or 'no projects'})"
            )
        series = client.time_series.list(limit=5)
        return {
            "message": f"Connected to the CDF project {kwargs['project']}",
            "columns": [{"name": "external_id", "type": "String"}, {"name": "name", "type": "String"}],
            "sample": [[ts.external_id, ts.name] for ts in series],
        }
    if config.database == "osipi":
        status, _ = _pi_get(kwargs, "")
        return {"message": f"The PI Web API at {kwargs['url'].rstrip('/')}/ answers (HTTP {status})"}
    if config.database == "pi_points":
        # the data servers, and a few points of the first one that is selected
        _, body = _pi_get(kwargs, "dataservers")
        servers = (body or {}).get("Items", [])
        wanted = (kwargs.get("data_server") or "").lower()
        if wanted:
            servers = [s for s in servers if wanted in (str(s.get("Name", "")).lower(), str(s.get("Path", "")).lower())]
        if not servers:
            raise LookupError(f"The PI Web API has no data server {kwargs.get('data_server') or ''}".rstrip())
        server = servers[0]
        query = {"nameFilter": kwargs.get("name_filter") or "*", "maxCount": "5"}
        if kwargs.get("source_filter"):
            query["sourceFilter"] = kwargs["source_filter"]
        _, body = _pi_get(kwargs, f"dataservers/{server['WebId']}/points", query)
        points = (body or {}).get("Items", [])
        return {
            "message": f"Connected to the PI Data Archive {server.get('Name')}",
            "columns": [{"name": c, "type": "String"} for c in ("name", "descriptor", "point_type", "unit")],
            "sample": [
                [p.get("Name"), p.get("Descriptor") or None, p.get("PointType"), p.get("EngineeringUnits") or None]
                for p in points
            ],
        }
    if config.database == "ip21":
        from maplib.ip21 import IP21Client

        args = _ip21_arguments(kwargs)
        client = IP21Client(
            args["url"],
            args["data_source"],
            username=args.get("username"),
            password=args.get("password"),
            token=args.get("token"),
            verify_ssl=args.get("verify_ssl", True),
            timeout=args.get("timeout", 15.0),
        )
        tags = client.browse(args.get("tag_filter") or "*", max_tags=5)
        return {
            "message": f"Connected to the IP.21 data source {args['data_source']}; "
            + (f"the tag filter finds tags such as {tags[0]}" if tags else "the tag filter finds no tags"),
            "columns": [{"name": "tag", "type": "String"}],
            "sample": [[t] for t in tags],
        }
    if config.database == "influxdb2":
        database = _influxdb2_database(config, kwargs)
        bucket = kwargs["bucket"].replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$")
        tables = database.client.query_api().query(
            f'import "influxdata/influxdb/schema"\nschema.measurements(bucket: "{bucket}")', org=kwargs.get("org")
        )
        measurements = sorted({str(record.get_value()) for table in tables for record in table.records})
        return {
            "message": f"Connected; the bucket {kwargs['bucket']} has {len(measurements)} measurements",
            "columns": [{"name": "measurement", "type": "String"}],
            "sample": [[t] for t in measurements],
        }
    if config.database == "opcua":
        import socket
        from urllib.parse import urlparse

        u = urlparse(kwargs["endpoint"])
        timeout = float(kwargs.get("timeout") or 5)
        with socket.create_connection((u.hostname, u.port or 4840), timeout=timeout):
            pass
        return {"message": f"The OPC UA server at {u.hostname}:{u.port or 4840} answers"}
    if config.database == "insights_hub":
        import json
        import ssl
        import urllib.request

        timeout = float(kwargs.get("timeout") or 15)
        token = _insights_hub_token(kwargs, timeout)
        host = kwargs["host"].strip().rstrip("/")
        base = host if "://" in host else "https://" + host
        req = urllib.request.Request(base + "/api/assetmanagement/v3/assets?page=0&size=5",
                                     headers={"Accept": "application/json", "Authorization": f"Bearer {token}"})
        context = None
        if str(kwargs.get("verify_ssl", "true")).lower() in ("false", "0", "no"):
            context = ssl._create_unverified_context()
        with urllib.request.urlopen(req, timeout=timeout, context=context) as r:
            body = json.loads(r.read())
        assets = (body.get("_embedded") or {}).get("assets") or []
        total = (body.get("page") or {}).get("totalElements", len(assets))
        return {
            "message": f"Connected to the Insights Hub tenant {kwargs['tenant']}, which has {total} assets",
            "columns": [{"name": "asset_id", "type": "String"}, {"name": "name", "type": "String"},
                        {"name": "type_id", "type": "String"}],
            "sample": [[a.get("assetId"), a.get("name"), a.get("typeId")] for a in assets],
        }
    raise ValueError(f"Unknown database {config.database!r}")

# The config graph

maplib keeps the configuration of a model as triples in the model itself, in a named graph of its
own: the **config graph**. The configuration travels with the model: it is written, serialized,
served and queried like any other graph, and applied with `maplib.config.apply_config`.

| | |
|---|---|
| Namespace | `https://datatreehouse.github.io/maplib/config#` (prefix `mcfg:`) |
| Graph | `mcfg:ConfigGraph`, i.e. `https://datatreehouse.github.io/maplib/config#ConfigGraph` (`maplib.config.CONFIG_GRAPH`) |
| Python | `maplib.config` |

The namespace is separate from maplib's vocabulary (`https://datatreehouse.github.io/maplib/vocab#`,
used for e.g. full text search and validation reports): the config namespace is only for
configuration.

## Principles

- **Everything that configures maplib goes here.** New kinds of configuration get a class in the
  config namespace and an individual (or several) in the config graph, read and written by
  `maplib.config`.
- **No secrets.** A secret is configured by the name of the environment variable that holds it
  (`mcfg:secret`), read when the configuration is applied. Passwords, tokens and client secrets
  never enter the model, so a model can be shared or serialized with its configuration.
- **Replaceable items.** An item (e.g. a virtualization) is its IRI and the IRIs starting with
  `<its IRI>/`, which name its parts (parameters, resources). Writing an item replaces all of these,
  so an item is always written whole.
- **Plain SPARQL to read.** Anything that reads the configuration (e.g. the Model portal) can query
  the config graph: `SELECT * WHERE { GRAPH mcfg:ConfigGraph { ?s ?p ?o } }`.

## Virtualizations

A virtualized database: maplib (chrontext) answers the parts of SPARQL queries over its resources
by querying the database, so the data is not copied into the model.

```turtle
@prefix mcfg: <https://datatreehouse.github.io/maplib/config#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<urn:maplib:config:virtualization:readings> a mcfg:Virtualization , mcfg:SnowflakeDatabase ;
    rdfs:label "Meter readings" ;
    mcfg:enabled true ;
    mcfg:sqlDialect "snowflake" ;                     # optional, overrides the database's dialect
    mcfg:parameter <urn:maplib:config:virtualization:readings/parameter/account> ;
    mcfg:secret <urn:maplib:config:virtualization:readings/secret/password> ;
    mcfg:resource <urn:maplib:config:virtualization:readings/resource/readings> .

<urn:maplib:config:virtualization:readings/parameter/account>
    mcfg:key "account" ; mcfg:value "acme" .

<urn:maplib:config:virtualization:readings/secret/password>
    mcfg:key "password" ; mcfg:environmentVariable "SNOWFLAKE_PASSWORD" .

<urn:maplib:config:virtualization:readings/resource/readings> a mcfg:Resource ;
    mcfg:resourceName "readings" ;
    mcfg:sql "SELECT meter_id AS id, ts AS timestamp, value FROM readings" ;
    mcfg:templateDocument """@prefix ct: <https://github.com/DataTreehouse/chrontext#> . ...""" ;
    mcfg:property <urn:maplib:config:virtualization:readings/resource/readings/property/0> .

<urn:maplib:config:virtualization:readings/resource/readings/property/0>
    mcfg:predicate <http://iec.ch/TC57/2013/CIM-schema-cim16#Measurement.unitSymbol> ;
    mcfg:object <http://iec.ch/TC57/2013/CIM-schema-cim16#UnitSymbol.W> .
```

### Classes

| Class | Database | Built with |
|---|---|---|
| `mcfg:Virtualization` | every virtualized database also has this type | |
| `mcfg:SQLDatabase` | any SQL database SQLAlchemy can reach (PostgreSQL, SQL Server, Oracle, MySQL, ...), with its dialect in `mcfg:sqlDialect` | `maplib.config.SQLAlchemyDatabase` (needs `sqlalchemy` and the database's driver) |
| `mcfg:DuckDBDatabase` | DuckDB | `maplib.virtualization.DuckDBDatabase` |
| `mcfg:SnowflakeDatabase` | Snowflake | `maplib.virtualization.SnowflakeDatabase` |
| `mcfg:BigQueryDatabase` | BigQuery | `maplib.virtualization.BigQueryDatabase` |
| `mcfg:DatabricksDatabase` | Databricks SQL warehouse | `maplib.virtualization.DatabricksDatabase` |
| `mcfg:TimescaleDatabase` | TimescaleDB (PostgreSQL), with psycopg | `maplib.virtualization.TimescaleDatabase` |
| `mcfg:FabricLakehouseDatabase` | Microsoft Fabric Lakehouse, its Delta tables read from OneLake (name `fabric_lakehouse`) | `maplib.virtualization.FabricLakehouseDatabase`: DuckDB with the delta and azure extensions; each table is a view, so resource SQL is DuckDB SQL over the table names |
| `mcfg:FabricDatabase` | Microsoft Fabric Warehouse, or a Lakehouse's SQL analytics endpoint (name `fabric_warehouse`) | `maplib.virtualization.FabricDatabase`: T-SQL, with mssql-python or pyodbc |
| `mcfg:KustoDatabase` | Azure Data Explorer, or a Microsoft Fabric Eventhouse's KQL database (name `kusto`) | `maplib.virtualization.KustoDatabase`: queried in KQL with azure-kusto-data; resources are KQL tabular expressions |
| `mcfg:InfluxDBDatabase` | InfluxDB 3: Core, Enterprise, Cloud Dedicated, Cloud Serverless, Clustered (name `influxdb`) | `maplib.virtualization.InfluxDBDatabase`: SQL (Apache DataFusion) over Arrow Flight with influxdb3-python; resource SQL selects from measurements, e.g. `SELECT sensor AS id, time AS timestamp, value FROM readings` |
| `mcfg:InfluxDB2Database` | InfluxDB 2, InfluxDB Cloud (TSM) and InfluxDB 1.8 (name `influxdb2`) | `maplib.virtualization.InfluxDB2Database`: Flux with influxdb-client; resources are measurements, not SQL |
| `mcfg:OPCUADatabase` | OPC UA Historical Access | `maplib.opcua.OPCUAHistoryDatabase` |
| `mcfg:CogniteDatabase` | Cognite Data Fusion time series | `VirtualizedDatabase.Cognite` with a cognite-sdk `CogniteClient` |
| `mcfg:OSIPIDatabase` | OSIsoft (AVEVA) PI, through the PI Web API | `Model.map_osipi`: reads the Asset Framework into the model and virtualizes its time series (no resources to configure) |
| `mcfg:PIPointsDatabase` | OSIsoft (AVEVA) PI Data Archive (the PI historian), through the PI Web API | `Model.map_pi_points`: reads the PI Points (tags) into the model and virtualizes their recorded values (no resources to configure) |
| `mcfg:IP21Database` | AspenTech InfoPlus.21, through the aspenONE Process Data REST API | `maplib.ip21.map_ip21`: adds the tags found by the tag filter to the model and virtualizes their history (no resources to configure) |
| `mcfg:InsightsHubDatabase` | Siemens Insights Hub (formerly MindSphere), through Asset Management, IoT Time Series and IoT Time Series Aggregates (name `insights_hub`) | `Model.map_insights_hub`: reads the assets, asset types, aspects and variables into the model and virtualizes the time series of the variables of dynamic aspects (no resources to configure) |
| `mcfg:Resource` | a resource of a virtualized database | |
| `mcfg:Indexing` | the indexes of the model | `IndexingOptions` |
| `mcfg:QueryCache` | the query result cache of the model | `QueryCacheOptions` |

The SQL databases, the OPC UA history database and the Cognite client are only available in maplib
Enterprise Edition (and need their client libraries, e.g. `snowflake-connector-python`,
`cognite-sdk`).

### Properties

| Property | On | Value |
|---|---|---|
| `rdfs:label` | `mcfg:Virtualization` | a name for people |
| `mcfg:enabled` | `mcfg:Virtualization` | `xsd:boolean`; `apply_config` skips disabled ones |
| `mcfg:sqlDialect` | SQL databases | `"duckdb"`, `"postgres"`, `"snowflake"`, `"bigquery"` or `"databricks"` |
| `mcfg:parameter` | `mcfg:Virtualization` | a connection parameter (`mcfg:key`, `mcfg:value`) |
| `mcfg:secret` | `mcfg:Virtualization` | a secret parameter (`mcfg:key`, `mcfg:environmentVariable`) |
| `mcfg:resource` | `mcfg:Virtualization` | a `mcfg:Resource` |
| `mcfg:resourceName` | `mcfg:Resource` | the value of `ct:hasResource` on the objects it virtualizes |
| `mcfg:sql` | `mcfg:Resource` of an SQL database | the query giving its rows, with columns named as the template's parameters (an `id` column for the identifiers): SQL, or for `mcfg:KustoDatabase` a KQL tabular expression such as `Telemetry \| project id=DeviceId, timestamp=Timestamp, value=Value` (`maplib.config.QUERY_LANGUAGES`) |
| `mcfg:templateDocument` | `mcfg:Resource` | the template, as a stOTTR document |
| `mcfg:template` | `mcfg:Resource` | the IRI of a template already in the model (instead of `mcfg:templateDocument`) |
| `mcfg:option` | `mcfg:Resource` | a setting of the resource (`mcfg:key`, `mcfg:value`), e.g. for CDF |
| `mcfg:property` | `mcfg:Resource` | a static property of the objects it virtualizes (`mcfg:predicate`, `mcfg:object`), e.g. a unit; see below |
| `mcfg:predicate`, `mcfg:object` | properties | IRIs |
| `mcfg:key`, `mcfg:value` | parameters, options | strings |
| `mcfg:environmentVariable` | secrets | the name of the environment variable |

### Parameters per database

The parameters are the keyword arguments of the database's constructor.

| Database | Parameters | Secrets |
|---|---|---|
| General SQL | `url` (a SQLAlchemy URL without the password, e.g. `postgresql+psycopg://maplib@db:5432/plant`); `mcfg:sqlDialect` is required | `password` |
| DuckDB | `database` (file), `timezone`, `sandbox` (`true`: the resource SQL can not read other local files, attach databases or reach the network; recommended when users configure it), `allowed_directories`, `allowed_paths` (comma separated, still readable in the sandbox) | |
| Snowflake | `account`, `user`, `warehouse`, `database`, `schema`, `role`, `authenticator` | `password`, `private_key_file_pwd`, `token` |
| BigQuery | `project`, `location` (credentials from the environment, e.g. `GOOGLE_APPLICATION_CREDENTIALS`) | |
| Databricks | `server_hostname`, `http_path`, `catalog`, `schema` | `access_token` |
| TimescaleDB | `url` (a libpq URL such as `postgresql://reader@db:5432/plant`, without the password), or `host`, `port`, `database`, `user`; `sslmode` | `password` |
| Fabric Lakehouse | `workspace`, `lakehouse` (names or ids), `authentication` (`service_principal`, `credential_chain`, `azure_cli`, `managed_identity`), `tenant_id`, `client_id`, `chain` (for `credential_chain`: `cli`, `managed_identity`, `env` or `workload_identity`), `tables` (comma separated, default all) | `client_secret` |
| Fabric Warehouse | `server` (e.g. `<id>.datawarehouse.fabric.microsoft.com`), `database` (the warehouse or lakehouse), `authentication` (`service_principal`, `default`, `interactive`, `azure_cli`), `tenant_id`, `client_id`, `driver` (`mssql-python` or `pyodbc`) | `client_secret` |
| Azure Data Explorer / Fabric Eventhouse | `cluster` (the query URI, e.g. `https://<cluster>.<region>.kusto.windows.net` or `https://<id>.<region>.kusto.fabric.microsoft.com`), `database`, `tenant_id`, `client_id`, `authentication` (`service_principal`, `default`, `azure_cli`, `none`; by default a service principal when `client_id` and the secret are given, else the default Azure credentials) | `client_secret` |
| InfluxDB 3 | `host` (e.g. `http://influxdb:8181`), `database`, `org` (some InfluxDB Cloud versions) | `token` |
| InfluxDB 2 | `url` (e.g. `http://influxdb:8086`), `org`, `bucket` (InfluxDB 1.8: `database/retention_policy`), `measurement`, `id`, `field` (defaults for the resources, see below), `timeout` (milliseconds) | `token` (InfluxDB 1.8: `username:password`) |
| OPC UA | `endpoint`, `username`, `timeout`, `timestamp_parameter`, `value_parameter` | `password` |
| CDF | `project`, `base_url` (the cluster, e.g. `https://westeurope-1.cognitedata.com`), `token_url`, `client_id`, `scopes`, `client_name` | `client_secret` |
| OSIsoft PI | `url` (the PI Web API, e.g. `https://pi/piwebapi`), `asset_server`, `asset_database`, `root_element`, `username`, `verify_ssl`, `graph`, `base_iri`, `default_start_time`, `default_end_time`, `timeout` (the arguments of `Model.map_osipi`) | `password` or `token` |
| PI Data Archive | `url` (the PI Web API), `data_server` (name or `\\server` path), `name_filter` (e.g. `FIC*`), `source_filter` (point source), `point_attributes` (e.g. `pointsource, location1`), `username`, `verify_ssl`, `graph`, `base_iri`, `default_start_time`, `default_end_time`, `timeout` (the arguments of `Model.map_pi_points`) | `password` or `token` |
| AspenTech IP.21 | `url` (the Process Data REST API, e.g. `https://ip21/ProcessData/AtProcessDataREST.dll`), `data_source` (e.g. `IP21`), `tag_filter` (e.g. `FIC*`), `username`, `verify_ssl`, `graph`, `base_iri`, `default_start_time`, `default_end_time`, `read_metadata`, `max_tags`, `timeout` (the arguments of `maplib.ip21.map_ip21`) | `password` or `token` |
| Siemens Insights Hub | `host` (the region's gateway, e.g. `gateway.eu1.mindsphere.io`), `tenant`, `client_id`, `token_url` (default `https://{tenant}.piam.{region}.{domain}/oauth/token`), `app_name`, `app_version`, `user_tenant` (app credentials, through the Technical Token Manager), `root_asset` (an asset id), `graph`, `base_iri`, `default_start_time`, `default_end_time` (ISO 8601 timestamps, or durations before now such as `P7D`), `aggregate_pushdown` (`true` reads time bucketed aggregates from IoT Time Series Aggregates), `page_size`, `max_concurrency`, `timeout`, `verify_ssl` (the arguments of `Model.map_insights_hub`) | `client_secret` or `token` |

CDF resources take the options of cognite-sdk's resource map: `id_type` (`external_id`, `id` or
`instance_id`), and `timestamp` and `value` (the template parameters of the datapoints' timestamp
and value).

InfluxDB 2 resources are measurements: the options `measurement`, `id` (the column with the
external ids of the time series: a tag, `_field` (the default) when each field is a time series,
or `_measurement`) and `field` (only the values of this field). A resource without them takes
the connection parameters of the same names. The templates have the parameters `id`,
`timestamp` and `value`; other parameters are read from the tags with the same names.

### Static properties of resources (e.g. units)

Facts that are the same for every row of a resource, such as the unit of a measurement, are not
triples of its template: chrontext does not support constant triples in a virtualized template (a
query for them finds nothing). They are resource properties instead
(`VirtualizationResource.properties`, predicate IRI to object IRI). Applying the virtualization
writes each as an ordinary triple, in the default graph, on every object with `ct:hasResource` the
resource name, replacing the values those objects had for the predicate:

```python
CIM = "http://iec.ch/TC57/2013/CIM-schema-cim16#"
VirtualizationResource(name="readings", sql=..., template=..., properties={
    CIM + "Measurement.unitSymbol": CIM + "UnitSymbol.W",
    CIM + "Measurement.unitMultiplier": CIM + "UnitMultiplier.k",
})
```

So `?m cim:Measurement.unitSymbol ?u ; cim:Analog.AnalogValues ?p . ?p cim:AnalogValue.value ?v`
joins the static unit with the virtualized values. The properties are on the objects, not on their
data points (the data points exist only in query results); a QUDT unit (`qudt:unit`) is put on the
object too. `apply_properties(m, config)` writes them without connecting to the database.

In a virtualized template, name the data point with a variable that is not a parameter (`?point`),
not a blank node (`_:point`): with a blank node chrontext binds none of the data point's values.

## Indexing

```turtle
mcfg:indexing a mcfg:Indexing ;
    mcfg:fts true ;                    # full text search
    mcfg:geo false ;                   # GeoSPARQL (maplib EE)
    mcfg:objectIndexAll false ;        # object indexes for all suitable predicates
    mcfg:objectIndexPredicate rdfs:label , <http://example.org/serialNumber> .  # or for these only
```

| Property | Value | `IndexingOptions` |
|---|---|---|
| `mcfg:fts` | `xsd:boolean` | `fts` |
| `mcfg:geo` | `xsd:boolean` | `geo` |
| `mcfg:objectIndexAll` | `xsd:boolean`: index all suitable predicates by object (about twice the memory) | `object_sort_all` |
| `mcfg:objectIndexPredicate` | a predicate IRI to index by object (any number) | `object_sort_some` |

Subjects are always indexed. Object indexes make lookups by value fast (e.g. `?s ex:serialNumber "123"`).
Without `mcfg:objectIndexAll` or `mcfg:objectIndexPredicate`, `rdf:type` and `rdfs:label` have object
indexes. `mcfg:objectIndexPredicate` replaces these defaults, so list them too to keep them.
`apply_config` creates the configured indexes (`Model.create_index(IndexingConfig.options())`), also
for the triples already in the model, which can take some time for a large model.

## Query cache

```turtle
mcfg:queryCache a mcfg:QueryCache ;
    mcfg:enabled true ;
    mcfg:maxEntries 1024 ;              # results kept at most
    mcfg:maxBytes 268435456 ;           # their total size at most (bytes)
    mcfg:maxEntryBytes 16777216 ;       # larger results are not kept
    mcfg:ttl 300.0 ;                    # seconds a result is served at most (optional)
    mcfg:minRunTime 0.0 ;               # faster queries are not kept (seconds)
    mcfg:cacheVirtualized false .       # also keep results over virtualized databases
```

| Property | Value | `QueryCacheOptions` |
|---|---|---|
| `mcfg:enabled` | `xsd:boolean` | `enabled` |
| `mcfg:maxEntries` | `xsd:integer` | `max_entries` |
| `mcfg:maxBytes` | `xsd:integer`, bytes | `max_bytes` |
| `mcfg:maxEntryBytes` | `xsd:integer`, bytes | `max_entry_bytes` |
| `mcfg:ttl` | `xsd:double`, seconds; none for no limit | `ttl` |
| `mcfg:minRunTime` | `xsd:double`, seconds | `min_run_time` |
| `mcfg:cacheVirtualized` | `xsd:boolean` | `cache_virtualized` |

Missing properties take the defaults above (no ttl). The cache serves `Model.query` and
`Model.serve`. A result is kept until a write changes the triples it can read, per named graph and
predicate, so writes elsewhere keep it. The cache does not see changes in virtualized databases:
with `mcfg:cacheVirtualized true`, set a `mcfg:ttl`. `apply_config` sets the cache
(`Model.set_query_cache(QueryCacheConfig.options())`); without a `mcfg:QueryCache` the model's
setting stays.

## Common questions

A common question is a saved SPARQL query with a name and a description: a question people often
ask the model ("Which substations are there, and at which voltages?"). It is a SHACL Advanced
Features [SPARQL executable](https://www.w3.org/TR/shacl-af/#SPARQLExecutable), named and described
with Dublin Core terms, and linked to the vocabulary of the data it is about.

```turtle
@prefix mcfg: <https://datatreehouse.github.io/maplib/config#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

<urn:maplib:config:question:cim:lines-lengths> a mcfg:CommonQuestion , sh:SPARQLSelectExecutable ;
    dcterms:title "Lines and their lengths" ;
    rdfs:label "Lines and their lengths" ;
    dcterms:description "The AC line segments with their length (km), voltage and impedance, longest first." ;
    sh:select """PREFIX cim: <http://iec.ch/TC57/2013/CIM-schema-cim16#>
SELECT ?segment ?name ?length WHERE { ... }""" ;
    mcfg:appliesTo <http://iec.ch/TC57/2013/CIM-schema-cim16#> ;   # optional: about CIM16 data
    mcfg:bundled true ;                                            # shipped with maplib EE
    sh:order 1 .                                                   # optional: listed by order, then name
```

| Term | On | Value |
|---|---|---|
| `mcfg:CommonQuestion` | class | a common question; also typed with its SHACL-AF executable class |
| `sh:SPARQLSelectExecutable`, `sh:SPARQLConstructExecutable`, `sh:SPARQLAskExecutable` | class | the query form, with the query in `sh:select`, `sh:construct` or `sh:ask` |
| `mcfg:SPARQLDescribeExecutable` | class | a DESCRIBE query (`rdfs:subClassOf sh:SPARQLExecutable`; SHACL-AF has none), the query in `mcfg:describe` |
| `sh:select`, `sh:construct`, `sh:ask`, `mcfg:describe` | `mcfg:CommonQuestion` | the query, a string with its own `PREFIX` declarations |
| `dcterms:title` | `mcfg:CommonQuestion` | the name (also written as `rdfs:label`, for tools that show labels) |
| `dcterms:description` | `mcfg:CommonQuestion` | what it answers (optional) |
| `mcfg:appliesTo` | `mcfg:CommonQuestion` | the vocabulary (namespace IRI) of the data it is about; none for a question about any model |
| `mcfg:bundled` | `mcfg:CommonQuestion` | `xsd:boolean`: one of the questions maplib Enterprise Edition ships |
| `sh:order` | `mcfg:CommonQuestion` | `xsd:integer`: where it is listed, lowest first |

The domain is the vocabulary itself rather than a class, because not every kind of data has a class
that all of it shares (OPC UA nodes have `opc:NodeClass`, not an `rdf:type`). Three have
names in `maplib.config.QUESTION_DOMAINS`: `cim` (CIM16), `era` (the ERA vocabulary) and
`opcua` (OPC UA nodes). maplib Enterprise Edition ships common questions for these, with a way
to find which the model's data has (`maplib.bundled_questions`).

## Python

```python
from maplib.config import (
    CommonQuestion, write_question, read_questions, remove_question, question_iri,
)

write_question(m, CommonQuestion(
    iri=question_iri("Labels"),
    name="Labels",
    description="Everything with a label",
    query="PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\nSELECT ?s ?label WHERE { ?s rdfs:label ?label }",
))
for q in read_questions(m, "cim"):     # those about CIM, and those about any model
    print(q.name, q.form, m.query(q.query))
```

The configuration in general:

```python
from maplib.config import (
    CONFIG_GRAPH, VirtualizationConfig, VirtualizationResource, IndexingConfig, QueryCacheConfig,
    write_virtualization, remove_virtualization, read_virtualizations,
    write_indexing, read_indexing, write_query_cache, read_query_cache, remove_query_cache,
    apply_virtualization, apply_properties, apply_query_cache, apply_config,
)

write_virtualization(m, VirtualizationConfig(
    iri="urn:maplib:config:virtualization:plant",
    database="opcua",
    parameters={"endpoint": "opc.tcp://plant:4840/", "username": "maplib"},
    secrets={"password": "OPCUA_PASSWORD"},
    resources=[VirtualizationResource(name="history", template=HISTORY_TEMPLATE)],
))
write_indexing(m, IndexingConfig(fts=True))
write_query_cache(m, QueryCacheConfig(max_entries=2048, ttl=None))
m.write("model.trig", format="trig")   # the configuration is in the file too

m2 = Model(); m2.read("model.trig")
apply_config(m2)                       # connects (secrets from the environment) and virtualizes
```

`apply_config(m, environ=...)` takes the secrets from another mapping than `os.environ`, and raises
`KeyError` naming the variable when a secret is missing.

## The Model portal

The portal's Model › Connect page edits the config graph of the portal's model (the routes under
`/api/config`) and applies it. Its Query › Common questions page lists, runs, edits and removes the
common questions (`/api/config/questions`), and offers the bundled ones for the data the model has;
Query › SPARQL and Query › Paths save a query as a common question. Everything else reads the
config graph with SPARQL like any other graph.

# Change log

## Unreleased
### Breaking changes
- Transient triples are removed. Triples that should be available for querying, validation and reasoning, but not written, now go in a reserved named graph, `maplib.TRANSIENT_GRAPH` (`https://datatreehouse.github.io/maplib/vocab#TransientGraph`). The `transient` parameter of `map_json`, `map_xml`, `read`, `reads` and `insert` is removed: pass `graph=TRANSIENT_GRAPH` (or `target_graph=TRANSIENT_GRAPH` for `insert`) instead
- `include_transient` is replaced by `include_graphs: Optional[List[str]]` in `query`, `update`, `insert`, `infer`, `get_predicate`, `get_predicate_iris` and the corresponding `ModelStream` steps. The included graphs are merged into the queried graph, as with FROM. For `query`, `update`, `insert` and `infer`, `None` includes the transient graph (as `include_transient=True` did) and `[]` includes nothing; for `get_predicate` and `get_predicate_iris`, `None` includes nothing. `GRAPH ?g` ranges over the transient graph as over any other named graph, and `GRAPH <g>` no longer sees transient triples added to `g`
- RDFS inference (`infer_rdfs`) and SHACL rules (without an `inferences_graph` equal to the data graph) put their inferred triples in the transient graph. Writing all graphs as TriG or N-Quads leaves out the transient graph; write it with `graph=TRANSIENT_GRAPH`
- In Rust, `QuerySettings::include_transient` is replaced by `include_graphs: Vec<NamedGraph>`, and the `transient` arguments of the triplestore and model methods are removed. `NamedGraph::transient()` gives the transient graph
- The `message` column of `ValidationReport.results()` and `details()` is a list of strings, one per `sh:message` of the shape or SPARQL constraint (`"value"@lang` for language tagged strings, the value for plain strings), and null when there are none. Use e.g. `.explode("message")` to get one row per message
- `Model.serialize` writes a single file by default, and the default path of `Model.serialize` and `Model.deserialize` is `./model.maplib` instead of `./serialized_triples`. Pass `single_file=False` to write a folder as before
- `Model.map_default` is replaced by `Model.generate_template(data, primary_key_column)`, which always does a dry run: it returns the default template as a stOTTR string without mapping the data or adding the template to the model. Map the data with `m.map(template, data)`. It takes a DataFrame or SolutionMappings; the `graph`, `validate_iris` and `stream` parameters are gone, and the primary key column is no longer cast to String. In Rust, `Model::expand_default`, `expand_default_first_batch` and `expand_default_batch` are replaced by `maplib::model::default::generate_default_template`
- The `max_results` parameter of `Model.infer` and `ModelStream.infer`, and the `max_shape_constraint_results` parameter of `Model.validate`, are removed: the memory limit (`maplib.set_memory_limit`) bounds inference and validation instead. `max_iterations` and `max_rows` are unchanged. In Rust, `Model::infer` and `Model::validate` no longer take them
### Features
- Object storage: `Model.read`, `read_template`, `write`, `write_cim_xml`, `serialize` and `deserialize` take URLs of Google Cloud Storage (`gs://bucket/path`), Azure Blob Storage (`az://container/path`, `abfss://container@account.dfs.core.windows.net/path`, `https://account.blob.core.windows.net/container/path`), Amazon S3 (`s3://`), HTTP(S) and `file://`, as well as local paths. Credentials are found as the cloud SDKs find them (environment variables, the metadata server on Google Cloud, managed and workload identities on Azure), or are given with the new `storage_options` argument, with the option names of Polars' `storage_options`. Files that are read are downloaded to a temporary file first, with concurrent range requests for large files. Files that are written are uploaded as they are written, in concurrent parts, and the object only appears when the write completes
- `Model.serialize(url)` writes a single file locally and uploads it. `Model.serialize(url, single_file=False)` writes a folder to a new generation under the URL (`gen-<time>-<id>/`) and then publishes it by replacing the object `LATEST` with its name, with a conditional put: readers see the previous or the new model whole, and if another writer published to the URL in the meantime, the serialization fails instead of replacing it. The current and the previous generation are kept
- Triple tables in object storage (maplib Enterprise Edition): `Model.deserialize(url, storage="disk", storage_folder=...)` on a folder serialized to object storage does not download the triple tables, only the dictionaries and the sparse indices. Each table is downloaded to the storage folder the first time it is read. With `cache_size` (or `MAPLIB_REMOTE_CACHE_BYTES`), the least recently read downloaded tables are removed when they take more space, and downloaded again when read. Tables read in the last five minutes (`MAPLIB_REMOTE_CACHE_GRACE_SECONDS`) are kept, as running queries may read them. So a disk mode model can be served from a VM or container with a local disk smaller than the model. Triples added afterwards stay local until the model is serialized again. Serializations record the number of triples of each table for this; folders written by earlier versions are downloaded when deserialized
- In Rust, `file_io::remote` has `RemoteLocation`, `Input` and `Output` for object storage, `SerializationSource`, `SerializationTarget` and `SerializedFile` have `Remote` variants, `Triplestore` and `Model` have `serialize_triples_remote` and `deserialize_triples_remote`, and `Triples::from_serialization` takes the number of triples when it is known. The metadata of a serialization folder is written after its other files
- CSV on the Web ([#26](https://github.com/DataTreehouse/maplib/issues/26)): `Model.map_csvw(source, metadata=None, graph=None, minimal=False, iri_mappings=None)` converts tabular data to RDF as specified by the W3C CSVW recommendations ([tabular data model](https://www.w3.org/TR/tabular-data-model/), [metadata](https://www.w3.org/TR/tabular-metadata/), [csv2rdf](https://www.w3.org/TR/csv2rdf/)), in standard or minimal mode. The source is a metadata document or a CSV file, for which the metadata is located as in the specification (with the site-wide configuration `/.well-known/csvm` if it is found, else `<file>-metadata.json` and `csv-metadata.json`) or taken from the header row. Dialects, inherited properties, datatypes with formats (numbers, booleans, dates and times, regular expressions) and constraints, lists (ordered as `rdf:List`), virtual and suppressed columns, notes and common properties, row titles and foreign key validation are supported. The tables are added to temporary graphs using Facade-X, and a SPARQL Update generated from the metadata builds the output (URI templates are compiled to SPARQL expressions) and drops the temporary graphs. The update is returned. Warnings (invalid metadata properties, invalid cell values and values that maplib can not store with their datatype, such as dates with time zones, which are output as strings) are issued as Python warnings. Nothing is fetched over the network: `iri_mappings` maps IRI prefixes to local folders. The W3C CSVW RDF test suite is included in the tests, see `w3c_csvw_known_failures.txt` for the tests that do not pass. In Rust this is the new `csvw` crate and `Model::map_csvw`
- Streaming ([#87](https://github.com/DataTreehouse/maplib/issues/87)): `Model.map`, `map_triples`, `map_json` and `map_xml` take `stream=True` and then return a lazy `ModelStream`, e.g. `m.map(template, lf, stream=True).update(...).infer(...).insert(...).write("out.nt").write("out.ttl", format="turtle").collect()`. On `collect()`, each batch of the input is mapped into a fresh in-memory model with the templates, prefixes and UDFs of the model, the steps run on it in order, the `write` steps append it to their files (N-Triples, Turtle with prefixes declared once, or N-Quads), and the batch is dropped, so memory use depends on the batch size and not on the size of the input. The model's own triples are not changed. Steps only see the triples of the current batch. Blank node labels stay unique across batches
- `Model.map_json` and `Model.map_xml` also take an iterable of documents (strings or paths), e.g. a generator over the lines of a JSON Lines file, mapped one document at a time. With a `uuid_namespace`, document number i uses the namespace `<uuid_namespace>/<i>` so that documents do not get the same IRIs. With `stream=True`, `documents_per_batch` documents are mapped into each batch; mapping them into `graph=TRANSIENT_GRAPH` and inserting a CONSTRUCT query writes only the constructed triples
- Storage modes: `Model(storage="memory" | "hybrid" | "disk", storage_folder=...)`, also for `Model.deserialize`. `"memory"` keeps everything in memory, `"hybrid"` stores the dictionaries of IRIs and literals on disk (what `storage_folder` did before), and the new `"disk"` mode also stores the triple tables on disk as parquet files in `<storage_folder>/maplib_storage/triples`. Files of triple tables that are replaced (by compaction or deletes) or whose model is dropped are removed. Without `storage`, a `storage_folder` still means hybrid storage, so existing code is unaffected. Hybrid and disk storage require maplib Enterprise Edition. In Rust, `Model::new`, `Model::deserialize_triples` and `Triplestore::new` take a `StorageOptions` instead of the storage folder
- Full text search (maplib Enterprise Edition) uses DuckDB instead of tantivy. The index stores the dictionary codes of subjects, predicates and objects, and searches the strings where the dictionaries keep them: with hybrid and disk storage it joins against the on-disk dictionary tables, so no copy of the strings is made, and with memory storage it uses an in-memory DuckDB database. Matching is unchanged: case insensitive, 1.0 for a match of the whole value and 0.75 for a substring match. `fts_path` (in `IndexingOptions` and `Model.explore`) is no longer used, as the index is stored with the dictionaries
- GeoSPARQL (maplib Enterprise Edition): enable it with `IndexingOptions(geo=True)`. Geometry literals (`geo:wktLiteral`, `geo:wkbLiteral`, `geo:geoJSONLiteral`) are indexed spatially, and the GeoSPARQL functions are supported: the simple features, Egenhofer and RCC8 relations, measurements with units of measure, constructions, properties and serializations, across coordinate reference systems. Spatial filters and spatial joins use the index. `geof:relate`, the query rewrite extension and the aggregate functions are not supported yet
- Pretty Turtle output only declares the prefixes whose namespace starts a predicate, a datatype or an IRI in the model, sorted by name, instead of all known prefixes. Each prefix is checked with one lookup in the sorted IRI dictionary. The dictionary is shared by all graphs, so a prefix used only in another graph is still declared; with hybrid and disk storage all prefixes are declared as before
- `Model.serialize` writes the dictionaries of IRIs and literals as parquet files (`cats/<id>.parquet`, columns `str` and `cat`, in string order) instead of DuckDB databases, so a serialized model is parquet throughout and serialization no longer needs maplib Enterprise Edition. `Model.serialize(path, raw_db=True)` (Enterprise Edition) writes them as DuckDB databases as before: larger and slower to write, but deserializing into hybrid or disk storage uses them in place. `Model.deserialize` reads both, also folders written by earlier versions. In Rust, `serialize_triples` takes `raw_db: bool`
- `Model.map_cdf(client)` (maplib Enterprise Edition) maps the data model of a Cognite Data Fusion project to RDF using a cognite-sdk `CogniteClient`: the data models and views (as classes, with `rdfs:subClassOf` for the views they implement), the container properties the views map (as predicates, shared by the views that map them), and the nodes and edges that have data in the views. Datapoints are not read: the `cdf_cdm:CogniteTimeSeries` nodes are linked to chrontext resources, so `?ts chrontext:hasDataPoint ?dp` fetches their datapoints from CDF through `VirtualizedDatabase.cognite` when a query needs them. `data_models` and `spaces` select what to map
- `Model.serialize(path, single_file=True)` (the default) writes the model as one file, by convention with the extension `.maplib`, though any name works, also with `raw_db=True`. An interrupted serialization never leaves a partial file at `path`. `Model.deserialize` detects whether `path` is a file or a folder. A model deserialized from a file does not use the file afterwards. In Rust, `serialize_triples` takes `single_file: bool`, and `Cats::deserialize_cats` takes a `SerializationSource`
- Arithmetic and comparisons on durations, dates, dateTimes and times, as defined by the XPath operators that SPARQL refers to: `xsd:yearMonthDuration` and `xsd:dayTimeDuration` values can be added, subtracted, multiplied and divided by numbers, and divided by each other; they can be added to and subtracted from `xsd:dateTime`, `xsd:date` and (dayTimeDurations only) `xsd:time` values, and dateTimes, dates and times can be subtracted from each other, giving a dayTimeDuration. Durations are compared by value: `=` holds between durations of any duration datatype with the same months and seconds (e.g. `"P1D"^^xsd:dayTimeDuration = "PT24H"^^xsd:dayTimeDuration`), and yearMonthDurations, dayTimeDurations and times can be ordered with `<`, `<=`, `>` and `>=`. Casting to `xsd:duration`, `xsd:yearMonthDuration` and `xsd:dayTimeDuration` checks the lexical form and converts between the duration datatypes. Operations that are not defined, such as adding an `xsd:duration` or a number to a dateTime, are errors (unbound)
- `Model.read` also takes bytes (`bytes`, `bytearray` or `memoryview`, e.g. from `write_bytes`) or a file-like object with a `read` method, which is read to the end into memory (text streams as UTF-8). `format` is then required, except for HDT, which is detected from its header. Bytes objects are parsed without copying. `reads(format="hdt")` points to `read`. In Rust, `Model::read_bytes` reads from a byte slice
- `Model.write_bytes(format=...)` writes to `bytes` in memory in any format, including HDT. `Model.write`, `Model.write_cim_xml` and `ModelStream.write` also take a binary file-like object (anything with a `write` method accepting bytes, e.g. `io.BytesIO`, a file opened with `"wb"` or a socket file) instead of a path; writes are buffered, and the object is flushed but not closed. `writes(format="hdt")` points to `write_bytes`. `write_cim_xml` raises an exception instead of panicking when writing fails
- A `Model` shared between threads lets read operations run at the same time: `query`, `query_external`, `write`, `writes`, `write_bytes`, `size`, `get_predicate`, `get_predicate_iris`, `get_templates` and `list_udfs` take a shared lock, while operations that change the model (`update`, `insert`, `read`, `reads`, `map*`, `validate`, `infer`, `create_index`, `serialize`, ...) take an exclusive lock, so they wait for the reads in progress and new reads wait for them. The graph explorer answers its requests concurrently. In Rust, `Model::query`, `query_external`, `write_triples`, `write_quads`, `write_hdt`, `get_predicate` and `get_predicate_iris` and the corresponding `Triplestore` methods take `&self`
- `Model.add_graph` raises an error when given the model itself, which used to deadlock
- Memory limit: queries that would run out of memory spill to disk instead. Over the limit, which is 80% of the system memory by default (respecting container limits), joins, aggregations, sorts and query results move intermediate data to disk. `maplib.set_memory_limit(max_memory_mb=..., spill_dir=..., max_spill_mb=..., abort_above_mb=...)` sets the limit, the spill folder, the most to spill to disk, and an optional hard stop. `maplib.memory_usage()` and `maplib.memory_limit()` report the memory used and the limit. Spilling applies to the streaming engine, which collects all but small results. In Rust, `utils::memory::set_memory_limits` applies the limits, and binaries must use `polars_ooc::Allocator` as their global allocator for spilling to work
- `Model.serve()` (maplib Enterprise Edition) serves the model over the SPARQL 1.1 Protocol and Arrow Flight at the same time, in the background, on the model itself, so changes made from Python and through the endpoints are seen by both. Queries through the endpoints run at the same time as each other and as read operations from Python, while updates wait for exclusive access. It returns a `Server` with the endpoint URLs, `stop()`, `wait()` and context manager support. The SPARQL endpoints (`/sparql`, `/query`, `/update`) follow the W3C SPARQL 1.1 Protocol, including `default-graph-uri`, `named-graph-uri`, `using-graph-uri` and `using-named-graph-uri`, content negotiation (JSON, XML, CSV, TSV and Arrow IPC results, Turtle, N-Triples, RDF/XML, JSON-LD, N-Quads and TriG graphs) and the service description. `/store` implements the SPARQL 1.1 Graph Store HTTP Protocol. DESCRIBE is supported by the endpoint. The Flight endpoint runs queries from `do_get` tickets and `get_flight_info` commands, with results as the DataFrames of `Model.query()`, lists the graphs as flights, maps uploaded record batches with a template through `do_put`, and runs updates as an action. Updates are refused unless `allow_update=True`. `auth_token` requires a bearer token on both endpoints (with Flight basic auth handshake support). Other web sites can not read results or send updates unless allowed with `cors_origins`, and on loopback addresses requests with another Host are refused, against DNS rebinding. LOAD and SERVICE are refused, so clients can not make the server fetch URLs or read files. The Flight features are also served over plain HTTP at `/arrow` on the SPARQL port (`arrow_http=True`), for networks where gRPC does not get through. `Model.query(query, endpoint=server.url, endpoint_token=...)` queries a served model and returns the same results as `Model.query()` on it, with maplib's own types sent as Arrow over HTTP. Arrow results are zstd compressed within the Arrow format for clients on other hosts (`arrow_compression="auto"`, or `"zstd"` / `"none"`, also per request), and other responses are gzip compressed on request. HTTP/1.1 and HTTP/2 are served. Request sizes are limited by `max_request_bytes`. In Rust, the API is the `server` crate, the Arrow result format is in `representation::arrow_wire`, the client is in `external_sparql::maplib_endpoint`, and `Triplestore::graph_names` lists the graphs with triples
- `Model.write`, `Model.write_bytes` and `Model.writes` with `format="json-ld"` write a compacted JSON-LD 1.1 document: a `@context` with the prefixes that may be used (as for Turtle), and a `@graph` with one node object per subject. rdf:type is written as `@type`, xsd:string literals as JSON strings and other literals as value objects with their lexical form, blank nodes that are the object of one triple are nested, and RDF lists are written as `@list`. Until now, JSON-LD was written by the streaming oxjsonld serializer, which wrote one node object per triple and ignored the prefixes. A prefix whose name is the scheme of an IRI in the model (e.g. `ex` when there is an IRI `<ex:b>`) is left out of the `@context`, as the IRI would be read as a compact IRI (with hybrid and disk storage this is not checked, and writing such an IRI fails). Blank nodes used in more than one of the written graphs are always written with their label. A single `graph` is written at the top level, as the document's default graph. To write a dataset, give a list of graphs, e.g. `graph=[DEFAULT_GRAPH, "http://example.com/g1"]`: the default graph (if listed) is written at the top level and each other graph as a graph object (`{"@id": ..., "@graph": [...]}`). In Rust, `Model::write_jsonld` takes the list of graphs. The writer is tested by round trips of the W3C JSON-LD 1.1 fromRdf and toRdf tests and the W3C RDF 1.1 evaluation tests
- `graph` in `Model.write`, `Model.write_bytes` and `Model.writes` can be a list of graph IRIs for the dataset formats (JSON-LD, TriG and N-Quads), which writes exactly those graphs. The listed graphs must exist. The new constant `maplib.DEFAULT_GRAPH` names the default graph in such a list. In Rust, `Model::write_quads` and `Triplestore::write_quads` take an optional slice of graphs instead of an optional graph
- `Model.serve(static_dir=..., static_path="/ui", static_vars={...})` also serves a web application from a folder on disk, read on every request so a rebuilt application shows on refresh. Paths without a file get the folder's `index.html` (for single page applications), `/` redirects to the application, and the `<!--#echo var='name' -->` directives of nginx SSI in its HTML pages are replaced with `static_vars`. `Model.explore()` (maplib Enterprise Edition) uses it to serve the Model portal on the model, with every page querying the SPARQL endpoint; the old explorer pages and their FastAPI server are removed, and `page` and `fts_path` are deprecated and ignored
- `Model.serve` keeps a query log: `GET /query-log` gives the last `query_log_size` (default 1000, 0 for none) queries and updates as JSON, each with its text, kind, status and error, rows, bytes, media type, the time spent waiting for a slot, running and serializing, and the client and page that sent it (the `X-Query-Source` header, else the Referer), together with totals since the server started and summary statistics (percentiles, error rate, rate, most frequent queries). `?since=<id>` returns only newer entries; `DELETE /query-log` empties it. `/status` includes the totals
- OPC UA History Access as a virtualized database (maplib Enterprise Edition): `VirtualizedDatabase(database=maplib.opcua.OPCUAHistoryDatabase("opc.tcp://..."))`. The external ids of the time series are OPC UA node ids such as `ns=2;s=ts1`, and the resource templates have the parameters `id`, `timestamp` and `value`. Raw values are read with the `asyncua` package, which must be installed. Conditions on the timestamp in the top level conjunction of a filter restrict the time range read from the server; all other conditions and aggregations are evaluated by maplib
- InfluxDB as a virtualized database (maplib Enterprise Edition). InfluxDB 3 (Core, Enterprise, Cloud Dedicated, Cloud Serverless, Clustered): `VirtualizedDatabase(maplib.virtualization.InfluxDBDatabase(host=..., database=..., token=...), resource_sql_map={"r": "SELECT sensor AS id, time AS timestamp, value FROM readings"})`, queried with SQL over Arrow Flight with the `influxdb3-python` package. The new SQL dialect `InfluxDBDialect` (`"influxdb"`) follows PostgreSQL, with DataFusion's `date_bin` for `chrontext:dateBin` and `chrontext:FloorDateTimeToSecondsInterval`, timestamp literals in UTC without a time zone and `DOUBLE` for casts to doubles; timestamps without a time zone in the results are returned as UTC. InfluxDB 2, InfluxDB Cloud (TSM) and InfluxDB 1.8: `VirtualizedDatabase(maplib.virtualization.InfluxDB2Database(url=..., token=..., org=..., bucket=..., resources={"r": {"measurement": "readings", "id": "sensor", "field": "value"}}))`, queried with Flux with the `influxdb-client` package. Flux reads the time range in the top level conjunction of the filters on the timestamp and only the time series asked for; other conditions and aggregations are evaluated by maplib
- Microsoft Fabric Lakehouses as virtualized databases (maplib Enterprise Edition): `maplib.virtualization.FabricLakehouseDatabase(workspace=..., lakehouse=..., tenant_id=..., client_id=..., client_secret=...)` reads the Delta tables of a lakehouse directly from OneLake with DuckDB's `delta` and `azure` extensions, and answers the virtualized parts of queries with DuckDB SQL. Each table is a view, so resource SQL is e.g. `SELECT sensor_id AS id, ts AS timestamp, value FROM readings` (`dbo.readings` in lakehouses with schemas). The tables are found by listing the lakehouse's Tables folder, or given with `tables`. It signs in with Microsoft Entra ID: a service principal, or one Azure credential (`authentication="credential_chain"` with `chain="cli"`, `"managed_identity"`, `"env"` or `"workload_identity"`). `root` reads the tables from another folder, such as a local folder of Delta tables
- `DuckDBDatabase` works with DuckDB 2.0, where query results no longer have `.pl()`
- Microsoft Fabric Warehouses as virtualized databases (maplib Enterprise Edition): `maplib.virtualization.FabricDatabase(server=..., database=..., authentication=...)` answers the virtualized parts of queries with a Fabric Warehouse or the SQL analytics endpoint of a Lakehouse, in T-SQL. It signs in with Microsoft Entra ID through azure-identity: a service principal (`authentication="service_principal"` with `tenant_id`, `client_id` and `client_secret`), `"default"` (DefaultAzureCredential), `"interactive"` or `"azure_cli"`, and connects with mssql-python (which includes the ODBC driver) or pyodbc (`driver="pyodbc"`, with ODBC Driver 18 for SQL Server). Timestamps in the results, which Fabric stores without a time zone, are taken to be in UTC (`timezone`). The SQL is T-SQL, from the new dialects `FabricDialect` (`"fabric"`) and `MSSQLDialect` (`"mssql"`, `"sqlserver"`, `"tsql"`, for SQL Server 2022 and later and Azure SQL), which can also be used with other databases through `sql_dialect`. Limits are written as `TOP` and `OFFSET ... FETCH`, time buckets with `DATEDIFF_BIG`, `DATETRUNC` and (strides in months) `DATE_BUCKET`, `SAMPLE` as `MIN`, and conditions selected as values as bits. Durations can not be used as values, as T-SQL has no interval type. `SQLDialect` has new methods for the parts that differ in T-SQL: `ceil`, `round`, `avg_argument`, `as_value` and `offset_requires_order_by`
- `VirtualizedDatabase` takes `pushdowns`, a list of `"value_conditions"`, `"group_by"` and `"ordering"`, to choose what parts of a query are pushed down into the database. When it is not given, the `pushdowns` attribute of the database is used if it has one, otherwise all of them, as before
- Several virtualizations (maplib Enterprise Edition): each call to `Model.add_virtualization(virtualized_database, resources)` adds a virtualized database instead of replacing the previous one, so time series in different databases can be queried together. Each resource belongs to one virtualized database, and the virtualized queries over a resource are sent to the database that holds it; a query matching time series of resources in different databases sends a query to each and combines the results. Joins between time series (e.g. on a shared timestamp) are pushed down only when the time series are in the same database, and group by and slices are pushed down only for a single resource, as before. Adding a resource that was added before moves it to the new virtualized database, so calling `add_virtualization` again with the same resources still replaces them. In Rust, `ChrontextSettings` holds a list of `VirtualizationBackend`s (a virtualized database with its `Virtualization`) instead of a single database, built with `ChrontextSettings::new` or `add_virtualization`
- TimescaleDB as a virtualized database (maplib Enterprise Edition): `maplib.virtualization.TimescaleDatabase(url=..., host=..., port=..., database=..., user=..., password=..., sslmode=...)`, queried with the `psycopg` package (version 3), which must be installed. Its dialect, `TimescaleDialect` (`"timescale"`), is PostgreSQL with `chrontext:dateBin` and `chrontext:FloorDateTimeToSecondsInterval` computed by TimescaleDB's `time_bucket`, which also takes strides in months and years. The session time zone is UTC, and PostgreSQL numerics with a fractional part come back as doubles
- Siemens Insights Hub (formerly MindSphere) virtualization (maplib Enterprise Edition): `Model.map_insights_hub(host, tenant, client_id=..., client_secret=...)` reads the assets of a tenant (or of `root_asset` and its descendants) from Asset Management v3 into the graph, with their hierarchy, asset types (as classes, with their inheritance), locations, asset variables, aspects, aspect types and variables (data type, unit, quality code), and virtualizes the time series of the variables of dynamic aspects, with the external id `assetId/aspectName/variableName`. Their data points (`chrontext:hasTimestamp`, `chrontext:hasValue` for numbers and booleans, `insights_hub:hasStringValue` for strings and timestamps, `insights_hub:hasQualityCode`) are read from IoT Time Series v3 when queried, limited by the bounds on the timestamp in filters and paged by time, with concurrent requests per asset and aspect; a query without a lower bound on the timestamp needs `default_start_time` (an ISO 8601 timestamp, or a duration such as `P7D` before now). With `aggregate_pushdown=True`, MIN, MAX, SUM, COUNT and AVG of the values in buckets of `chrontext:FloorDateTimeToSecondsInterval` of whole minutes dividing an hour, or whole hours dividing a day, over time ranges on the bucket grid, are read from IoT Time Series Aggregates v4 instead (which leaves out values of bad quality, and counts a value exactly on a bucket boundary in the bucket before it). Authentication is with service credentials (OAuth client credentials at `https://{tenant}.piam.{region}.{domain}/oauth/token`, derived from a host like `gateway.eu1.mindsphere.io`, or `token_url`), app credentials through the Technical Token Manager (`app_name`, `app_version`, `user_tenant`), or a `token`. Tokens are renewed before they expire and on a 401, and throttled requests (429) are retried. Only the Python standard library is used for HTTP. In Rust, `virtualization::insights_hub::read_insights_hub` reads the asset model and `Model::add_insights_hub` adds it

### Bug fixes
- Chrontext (maplib Enterprise Edition): a UNION of static patterns joined with time series, e.g. `{ ?w a A } UNION { ?w a B } ?w :hasTimeseries ...`, no longer returns every solution twice
- `maplib.virtualization.DuckDBDatabase` works with DuckDB 2, which has no `pl()` on the result of `execute`
- JSON results of SELECT queries (`return_format="json"`) list the variables in the order of the projection, instead of an arbitrary order
- `Model.query_external` no longer panics ("entered unreachable code") on SELECT results where a variable is unbound in some rows, as with OPTIONAL. Unbound values are nulls, also before the first bound value, and a variable that is never bound is a column of nulls

### Performance
In disk mode the triple tables and their deduplication index are no longer held in memory. With few triples per distinct IRI or literal (as in the CIM data), memory use is dominated by the dictionary and reading, and disk storage saves little.

Triple table segments are compacted much faster, which matters most for datalog, where every iteration adds a small batch of triples to tables that are often large:
- Size-tiered compaction: only the trailing segments of comparable size are merged with the new triples, so a large table is no longer rewritten every 11 inserts. A table has O(log n) segments (at most 16).
- Merging sorts packed rank keys with Polars instead of a value-by-value heap merge, and builds the object sort from the merged triples instead of also reading and merging the object sorts of the segments.
- Disk mode: new triples are deduplicated against only the row groups that may hold their subjects, found through the sparse subject index, instead of reloading every stored triple of the table into an index on each insert. Small trailing segments (under 16k triples) stay in memory until merged, so small inserts don't write and rewrite small parquet files.

Query results are collected with the Polars streaming engine, which the rest of maplib already used; until now the final step of a query ran on the in-memory engine. Whole-table scans and counts, large result sets and DISTINCT are faster, while small queries are about as fast as before. `MAPLIB_POLARS_ENGINE=in-memory` switches back to the in-memory engine.

`IN` with a list of IRIs looks the values up in a hash set instead of comparing with each IRI in turn, so long lists no longer slow a query down. Property paths with `*`, `+` and `?` map the computed paths back to IRIs and literals by position instead of with joins. Patterns without shared variables (also inside `OPTIONAL`) are combined with a parallel cross join instead of a join on a constant key, which ran on one thread.

After `Model.compact()`, subjects are marked as sorted, so DISTINCT and grouping on subjects are faster.

ORDER BY on IRIs and strings no longer collects its input in memory for each sort column, and with memory storage it ranks the values batch by batch, so large sorts are several times faster and spill to disk under the memory limit. ORDER BY with LIMIT or OFFSET, and joins and aggregations, still keep their working state in memory.

Tantivy answered exact matches from its term dictionary, but scanned all strings for substring matches, which it ran whenever the exact matches did not fill the limit. DuckDB scans the strings once per query for both. Reading without full text search is unchanged.

Serialization, compared with the build before these changes on CIM N-Triples (2.2 M triples, IRI-heavy) and a synthetic literal-heavy file (4.5 M triples, 3 M distinct literals), release-test profile, medians of 3 runs alternating between builds, each run in a fresh process. Parquet is the new default, `raw_db` writes DuckDB dictionaries as before:

| Category | Workload | CIM 2.2 M: parquet / `raw_db` | Literals 4.5 M: parquet / `raw_db` |
|---|---|---|---|
| Writing | `serialize` (model already compacted) | 1.97 → 0.43 s (**-78%**) / 0.73 s (**-63%**) | 3.90 → 0.79 s (**-80%**) / 1.37 s (**-65%**) |
| Writing | Size on disk | 107 → 35 MB (**-67%**) / 107 MB | 344 → 103 MB (**-70%**) / 345 MB |
| Reading | `deserialize`, memory storage | 0.12 → 0.06 s (**-51%**) / 0.06 s (**-51%**) | 0.20 → 0.10 s (**-49%**) / 0.15 s (**-21%**) |
| Reading | `deserialize`, hybrid storage | 0.05 → 0.55 s (+1115%) / 0.01 s (unchanged, used in place) | 0.06 → 0.72 s (+1094%) / 0.01 s (unchanged, used in place) |
| Reading | `deserialize`, disk storage | 0.03 → 0.56 s (+1964%) / 0.02 s (unchanged, used in place) | 0.02 → 0.74 s (+3580%) / 0.01 s (unchanged, used in place) |
| Disk | After `deserialize` with disk storage: count per predicate | 0.079 → 0.043 s (**-46%**) | 0.081 → 0.046 s (**-44%**) |
| Disk | Same, insert 100 k triples (half new subjects) | unchanged (within noise) | 0.224 → 0.171 s (**-24%**) / 0.193 s (**-14%**) |
| SPARQL | After `deserialize`: 50 subject lookups (memory / hybrid / disk), insert (memory / hybrid) | unchanged (within noise) | unchanged (within noise) |

- Serialized triple tables are written with row groups of 16,384 rows, as disk storage writes its own triple tables, instead of 1,000 rows. Disk storage copies the files as they are on deserialization, and scans read far fewer row groups.
- Dictionaries are built into DuckDB (hybrid and disk storage, and `raw_db`) by loading the rows first and building the unique indexes afterwards, instead of inserting into an indexed table, and the dictionaries are deserialized concurrently with the triple tables.
- With hybrid and disk storage, parquet dictionaries are built into DuckDB databases (including their unique indexes) on deserialization, while `raw_db` dictionaries are used in place, so use `raw_db=True` when a serialized model is loaded into hybrid or disk storage often.

Faster reading, ORDER BY, counting and deleting (memory storage):
- Dictionary ranks (used to sort triples when they are added or compacted, and for ORDER BY) are looked up in arrays indexed by the dictionary code instead of hash maps, and the distinct codes of a batch are found with a bitset instead of a hash set. The deduplication index of triple tables uses a faster hash function.
- The deduplication index of a triple table is built when triples are first added to an existing table, not when the table is created, so tables that are only read and queried never hold it.
- Triple tables of different predicates and types are built in parallel when triples are added.
- New dictionary entries are merged into the dictionary in one pass when they are many compared with its size, instead of one at a time.
- `COUNT(*)` (or COUNT of one of the variables) over `?s ?p ?o` in a single graph is answered from the sizes of the triple tables.
- `DELETE WHERE { ?s ?p ?o }` and `DELETE WHERE { ?s <p> ?o }` (also as `DELETE { .. } WHERE { .. }` without INSERT or USING), matched in the graph they delete from, drop the triple tables without evaluating the pattern.
- Garbage collection of the dictionary after large deletes marks the used codes in a bitset instead of building range sets per table segment.

| Category | Workload | Before | After | Change |
|---|---|---|---|---|
| Reading | N-Triples, 20 M triples (4 predicates, 2 M subjects) | 19.6 / 20.1 s | 10.6 / 10.9 s | **-46%** |
| Reading | N-Triples, 8 M triples (400 predicates) | 4.50 s | 4.38 s | -3% |
| Reading | RDF/XML, CIM 2.2 M triples | 1.71 / 1.90 s | 1.23 / 1.28 s | **-28% / -33%** |
| SPARQL | ORDER BY over 5 M IRIs, LIMIT 10 | 0.29 s | 0.077 s | **-74%** |
| SPARQL | ORDER BY over 5 M string literals, LIMIT 10 | 0.73 s | 0.077 s | **-90%** |
| SPARQL | `COUNT(*)` of all 20 M triples | 0.128 s | < 1 ms | **-99.9%** |
| SPARQL Update | 30 CONSTRUCT inserts of ~2 k triples into a 5 M triple table | 4.13 s | 4.13 s | unchanged |
| SPARQL Update | `DELETE WHERE { ?s <p> ?o }`, 5 M triples (incl. garbage collection) | 0.348 s | 0.067 s | **-81%** |
| SPARQL Update | `DELETE WHERE { ?s ?p ?o }`, 15 M triples (incl. garbage collection) | 0.63 s | 0.57 s | -9% |
| Memory | Peak RSS, 20 M triple workload | 6.14 GB | 6.44 GB | +5% |

(Release-test profile, Apple M-series laptop (14 cores), medians of 3 runs alternating with the build before this change, while other jobs were running on the machine. Where two numbers are given they are from two rounds of runs. RSS after reading varied by more than ±1 GB between runs of the same build, so it is not reported; the deduplication index that is no longer built takes about 14 bytes per triple. The higher peak comes from building tables in parallel.)

In-memory dictionaries store their strings back to back in a chunked arena, with flat per-entry vectors of offsets, lengths, prefixes and codes, a hash table from strings to entries and an array from codes to entries. This replaces a `BTreeMap` from strings to codes, a reverse `HashMap` and a separate allocation per string: about 30 bytes per entry on top of the string instead of about 100. String order is no longer maintained on every insert, but computed when needed (ranks, compaction, ordered output) and cached; after inserts only the new strings are sorted and merged into the cached order. Looking up a string is a hash probe instead of a walk down a B-tree.

| Category | Workload | Before (with the changes above) | After | Change | vs. v0.21.0 |
|---|---|---|---|---|---|
| Reading | N-Triples, 20 M triples (4 predicates, 2 M subjects) | 11.7 s | 8.3 s | **-29%** | **-62%** |
| Reading | N-Triples, 8 M triples (400 predicates, ~8 M distinct strings) | 5.00 s | 2.96 s | **-41%** | **-44%** |
| Reading | RDF/XML, CIM 2.2 M triples | 1.76 s | 1.12 s | **-36%** | **-43%** |
| Memory | Peak RSS, 8 M triples | 3.33 GB | 2.44 GB | **-27%** | **-31%** |
| Memory | Peak RSS, CIM 2.2 M triples | 1.51 GB | 1.28 GB | **-15%** | **-19%** |
| Memory | Peak RSS, 20 M triple workload | 6.10 GB | 5.66 GB | **-7%** | -2% |
| SPARQL Update | `DELETE WHERE { ?s ?p ?o }`, 15 M triples (incl. garbage collection) | 0.68 s | 0.26 s | **-62%** | **-75%** |
| SPARQL | ORDER BY, COUNT, inserts, predicate deletes | | | unchanged | |

(Medians of 2 runs per variant alternating between v0.21.0, the build with the changes above and this build, under heavy memory pressure from other jobs, so RSS after reading is not reported. The 20 M triple workload peaks while parsing, before the dictionary is complete, so the dictionary saves less there.)

Faster encoding and decoding with in-memory dictionaries:
- Reading, mapping and inserting encode string columns directly with global codes: all values are looked up in the dictionary in parallel, the new ones are deduplicated in parallel and added once with consecutive codes. Before, each column got its own local dictionary, which was then merged into the global dictionary and its codes re-encoded. Hybrid and disk storage keep the previous path.
- Repeated values next to each other are looked up once, and constant columns (e.g. from templates) are encoded once instead of once per row.
- Query results and formatted values (`<iri>`, `_:blank`, `"string"`) are decoded straight into string arrays in parallel, without building a string per value first.

| Category | Workload | Before (with the changes above) | After | Change | vs. v0.21.0 |
|---|---|---|---|---|---|
| Reading | N-Triples, 20 M triples (4 predicates, 2 M subjects) | 6.49 s | 3.98 s | **-39%** | **-81%** |
| Reading | N-Triples, 8 M triples (400 predicates) | 2.26 s | 1.53 s | **-32%** | **-64%** |
| Reading | RDF/XML, CIM 2.2 M triples | 0.93 s | 0.60 s | **-35%** | **-68%** |
| Mapping | Template with 4 triples (2 constant), 3 M rows | 1.36 s | 1.09 s | **-20%** | **-89%** |
| SPARQL | SELECT 5 M rows of IRIs / string literals | 0.152 / 0.090 s | 0.121 / 0.085 s | **-21%** / -6% | **-32% / -36%** |
| Memory | Peak RSS, 8 M triples / CIM / mapping / 20 M triples | 2.58 / 1.29 / 2.38 / 5.50 GB | 2.50 / 1.30 / 2.31 / 5.69 GB | -3% / 0% / -3% / +3% | **-31% / -17% / -39%** / -3% |

(Medians of 2 runs alternating with the build before this change. The v0.21.0 column uses v0.21.0 runs from an earlier round on the same machine.)

Faster lookups, property paths and small inserts:
- Property paths (`+`, `*`, `?`) with a bound end, a constant IRI or a variable whose values are known from VALUES or earlier patterns, follow the paths from those nodes only, step by step, instead of computing the transitive closure of the whole graph and filtering it.
- The values of a pattern's variables are pushed down from solution mappings of up to 100 000 rows (was 500). Lookups of many values walk the sorted values and the sparse index together instead of searching the index once per value, and read an in-memory table with one gather instead of one slice per range. When the ranges cover more than half of a table the whole table is read, and at most 256 ranges of a stored table are read.
- A small insert or delete into a large in-memory table without a deduplication index checks only the stored triples with the same subjects, instead of indexing every triple of the table first.

| Category | Workload | Before (with the changes above) | After | Change |
|---|---|---|---|---|
| SPARQL | VALUES with 20 000 subjects joined with a 5 M triple table | 186 ms | 39 ms | **-79%** |
| SPARQL | The same, then a second pattern on the ~25 000 objects found | 213 ms | 38 ms | **-82%** |
| SPARQL | VALUES with 2 000 subjects (alone / with a second pattern) | 24 / 27 ms | 24 / 23 ms | 0% / -16% |
| Property paths | `ex:t199999 ex:parent+ ?a` in a 200 000 node tree | 319 ms | 24 ms | **-92%** |
| Property paths | `?d ex:parent* ex:t3` (65 535 results) | 158 ms | 22 ms | **-86%** |
| Property paths | `ex:parent+` from 1 000 VALUES | 206 ms | 34 ms | **-84%** |
| Inserting | 200 triples into a 20 M triple graph (first / second insert) | 7 / 16 ms | 6 / 15 ms | ~0% |
| Inserting | 30 CONSTRUCT inserts into a 20 M triple graph | 4.14 s | 4.27 s | +3% |
| Memory | Peak RSS, lookups / 20 M triples with inserts | 5.62 / 5.70 GB | 5.39 / 5.62 GB | -4% / -1% |

(Medians of 2 runs alternating with the build before this change, under heavy load from other processes; differences below ~5% are within the noise.)

Reading and writing HDT is much faster. Reading decodes each term of the HDT dictionary once and maps it directly into the model's dictionaries, then adds the triples as integer IDs in parallel batches, without building HDT query indexes. Writing deduplicates and maps triples using the model's dictionary codes, decodes only the distinct terms, and writes the triples section without building query indexes. The written files are unchanged.

### Bug fixes
- SPARQL: `+`, `-`, `*` and `/` were parsed as right associative, so `8 - 3 - 2` gave 7 and `12 / 2 / 3` gave 18. Operators of the same precedence are now left associative, as the SPARQL grammar specifies. Serialized queries put arithmetic in parentheses, so that `8 - (3 - 2)` and `(1 + 2) * 3` keep their meaning
- Ctrl-C now stops a running query. Before, a long query could keep running until it finished
- `STRDT` with `xsd:dateTime` or `xsd:dateTimeStamp` dropped the time zone, e.g. `STRDT("2015-03-15T15:02:37-05:00", xsd:dateTime)` gave `"2015-03-15T20:02:37"`. The time zone is now kept, also when the values have different time zones
- A triple pattern with the same variable in two positions (e.g. `?s <p> ?s`) panicked, in queries and in `DELETE WHERE`, when the predicate also had triples where the two positions had different types (e.g. blank node subjects with IRI objects)
- Hybrid storage: `Model.compact()` (also run by `Model.serialize()`) failed with a DuckDB "Duplicate key" error once the dictionary had to be renumbered, e.g. after more than ten inserts into the same predicate
- SHACL rules: a rule that re-asserts a triple already in the data graph no longer makes inference loop until `max_iterations` (or forever with one rule). Inserting transient triples that all already existed non-transiently dropped the earlier transient triples with the same predicate, which were then inferred again on every iteration ([#76](https://github.com/DataTreehouse/maplib/issues/76))
- SHACL rules: rules whose WHERE clause has no triple pattern on `$this` (only `OPTIONAL` / `BIND`) now also run for focus nodes that become targets of their shape after the rule first ran ([#77](https://github.com/DataTreehouse/maplib/issues/77))
- SHACL: shapes with several `sh:message` values (e.g. one per language) lost the results of property shapes, so data violating them was reported as conforming, kept only one of the messages of node shapes, and gave one result per message for SPARQL constraints. Every result now has all the messages of its shape as `sh:resultMessage` values. Property shapes with several `sh:name` or `sh:description` values also lost their constraints ([#86](https://github.com/DataTreehouse/maplib/issues/86))
- HDT: writing the same model twice could give different files, as the properties of the triples section's control information were written in random order
- HDT: literals are written with their values as they are, as hdt-java and hdt-cpp do, rather than N-Triples escaped, so other HDT tools no longer read a tab as `\t`. Reading takes a backslash in a literal of a file from hdt-java or hdt-cpp as part of the value, rather than as the start of an escape. Files written by earlier maplib versions are recognised and still read correctly. Writing a literal that contains U+0000 raises an error, as HDT cannot store it
- HDT: the header of written files describes the dataset with the same vocabulary as hdt-java and hdt-cpp: IRIs for the dataset types and the dictionary and triples formats, and `hdt:publicationInformation` for the publication information
- Full text search: with a fixed subject (`ex:a maplib:fts ?fts`), the limit was applied before filtering on the subject, so few or no results were returned. Triples deleted with SPARQL Update were still found, now they are no longer found after `Model.compact()`. An on-disk index no longer leaves a folder behind for every index created
- Pretty Turtle and RDF/XML: the rdf:first and rdf:rest triples of blank nodes that do not form a well-formed list were not written, e.g. of a list without rdf:nil, a list node with two rdf:first, or a list whose later node is also referred to by another triple (shared or cyclic lists). Only lists where each node has one rdf:first and one rdf:rest, and every node after the first is only referred to by the rdf:rest before it, are written as lists; other rdf:first and rdf:rest triples are written as any other triples
- Pretty RDF/XML of several graphs in one document (as for CIM/XML): a blank node used in more than one of the graphs could be nested or written as part of a list in one graph, so that the other graphs referred to a blank node that was not in the document. Such blank nodes are now written with their label

## v0.21.0
### Performance
Besides the changes listed below, the memory improvements earlier in this release contribute to the faster reading and SHACL validation.

- RDF/XML is parsed in parallel, by splitting the document between top-level elements. The RDF/XML parser from Oxigraph (oxrdfxml) is now included in maplib to make this possible
- RDF/XML is written with a new pretty serializer (see Features)
- CIM/XML is written by building temporary graphs with two SPARQL queries driven by the profiles (one for the instances of the profile classes, one for their profile properties) and writing them with the pretty RDF/XML serializer, instead of running queries per class and property
- Disk-backed models keep their category map databases open instead of opening a database for every lookup, which makes small queries about ten times faster
- Inserting triples no longer ranks the inserted values against the whole dictionary, which made every insert cost time proportional to the number of distinct IRIs and literals in the model. This was most of the per-iteration cost of datalog inference
- The sort order of the dictionary is cached between ORDER BY queries and datalog iterations, until new values are added
- Removed debug output that SHACL validation printed for every sh:class constraint (it also evaluated the constraint a second time)

### Breaking changes
- The `streaming` parameter is removed from `Model.query`, `Model.update`, `Model.insert`, `Model.validate`, `ValidationReport.results` and `ValidationReport.details`. Polars already runs lazy queries with its streaming engine, so the parameter had no effect (benchmarks with `streaming=True` and `streaming=False` were identical). Remove the argument from calls
- RDF/XML (and CIM/XML) is read in parallel by default (`parallel=None`). Pass `parallel=False` to read sequentially. Line numbers in parse errors may refer to the position within a chunk of the document
- RDF/XML output is now formatted differently (one element per subject, nested blank nodes), but contains the same triples
- `write_cim_xml` writes CIM/XML as IEC 61970-552 / CGMES requires (see Bug fixes). Objects are identified relative to the document (rdf:ID="_<mRID>", rdf:resource="#_<mRID>"), so read the files with the same `base_iri` as the original data, as for any CGMES file

### Features
- `write_cim_xml` uses the pretty RDF/XML serializer. The exported triples are the same as before (the FullModel header first, then one typed element per instance with the properties its class has in the profiles, literal values as plain strings), but elements and properties are ordered differently
- Pretty RDF/XML serializer, working like the pretty turtle serializer: one node element per subject with all its properties, typed node elements when the type has a prefix, blank nodes that are used once nested where they are used, RDF lists written with rdf:parseType="Collection" when possible, and all namespaces declared on the root element
- ASK queries. m.query() returns a bool, or a SPARQL JSON boolean result with return_format="json"
- isNUMERIC and RAND SPARQL functions
- SHA256, SHA384 and SHA512 SPARQL functions
- Read and write TriG and N-Quads (format "trig" / "nquads", or the .trig / .nq file extensions when reading). Writing these formats outputs all graphs unless a graph is specified

### Bug fixes
- `write_cim_xml` was not compliant with IEC 61970-552 / CGMES (also before this release):
  - Objects were written with absolute IRIs (rdf:about="urn:...:#_<mRID>"); they are now rdf:ID="_<mRID>" with rdf:resource="#_<mRID>" references, when all objects share one base IRI
  - Enumeration values (e.g. cim:WindingConnection.Y) were written as objects when the vocabulary was in the data graph
  - Objects that also had superclass types (e.g. cim:Equipment) were written once per type; they are now written once, as their most specific class
  - Association ends that the profiles mark as not used (cims:AssociationUsed "No") are never written, and the used end is derived from them when the data only has the unused end
  - Only the namespaces that are used are declared, and the default Model.created / Model.scenarioTime are UTC with whole seconds (e.g. 2021-02-02T22:28:41Z)
- Disk-backed models wrote subjects on the boundaries between internal segments twice when writing turtle or RDF/XML (giving invalid RDF/XML with a duplicate rdf:ID)
- Writing turtle could hang forever (a deadlock between the pretty turtle writer's parallelism and Polars), e.g. for CGMES/CIM models
- sh:not required every constraint of its shape to fail, instead of the shape not conforming. Combined with sh:or this gave false results, e.g. 363 false sh:or results when validating CGMES shapes against the SHACL-SHACL shapes (pyshacl agrees there are none)
- sh:not with a shape that has several constraints of which some pass and some fail (e.g. two sh:class values) treated the shape as conforming, giving false sh:not results
- Inserting triples could drop triples whose values had been re-encoded in the dictionary, as their values could not be ranked when sorting
- Removed the pyarrow <24.0.0 pin. pyarrow only publishes Python 3.15 wheels from 26.0.0, so the pin made maplib uninstallable on Python 3.15
- COUNT now returns xsd:integer (Int64) as required by the SPARQL 1.1 spec, instead of xsd:unsignedInt (UInt32)
- SUM, AVG, MIN and MAX now work when the aggregated variable has multiple types. SUM and AVG promote the numeric types in each group (integer < decimal < float < double) and are unbound if a group contains a non-numeric value, MIN and MAX use the SPARQL ordering and keep the type of the selected value
- Division follows SPARQL op:numeric-divide: integer / integer gives xsd:decimal (e.g. 5 / 2 = 2.5, previously truncated to 2.0 as xsd:double), mixed types are promoted (decimal < float < double), and decimal division by zero is an error (unbound)
- COALESCE over arguments of different types returns the first bound argument (previously a value of a later argument could be returned nondeterministically)
- Fix panic when combining lang strings with differently encoded categoricals (e.g. COALESCE(?langString, "x"@en))
- Infinite xsd:double and xsd:float values are written as INF and -INF (previously inf and -inf, which are not valid lexical forms) in query results, STR() and all RDF serializations
- DATATYPE() of an unbound value or an error is unbound (previously the datatype of the variable was returned)
- YEAR, MONTH, DAY, HOURS and MINUTES return xsd:integer and SECONDS returns xsd:decimal including fractional seconds (previously xsd:unsignedInt, dropping fractions)
- STR() of xsd:dateTime and xsd:date values gives the XSD lexical form (e.g. 2010-06-21T11:28:01+00:00 instead of 2010-06-21 11:28:01.000000+00:00)
- Triple patterns without variables (e.g. { :s :p :o }) never matched. They now give one solution if the triple exists
- GROUP_CONCAT concatenated internal codes instead of the string values, and used an empty default separator instead of a single space. It now concatenates the string values (as STR()) of any type, including lang strings
- ABS, CEIL and FLOOR work when the argument has multiple types, and CEIL and FLOOR keep the type of the argument (previously xsd:integer)
- CONCAT accepts zero or one arguments
- Casting a blank node (e.g. xsd:double(?x)) gives an unbound value instead of failing the query
- FILTER, IF, NOT (!), AND (&&) and OR (||) use the effective boolean value of their operands (SPARQL 1.1 17.2.2): numbers are true unless zero or NaN, strings are true unless empty, and other values are errors. Previously a non-boolean FILTER failed the query, and IF with an error condition took the else branch instead of being unbound
- Casts to xsd:boolean, xsd:integer, xsd:decimal, xsd:float and xsd:double follow the XPath casting rules: strings must have a valid lexical form (e.g. xsd:boolean("abc") and xsd:integer("1.5") are unbound, previously false and a number), booleans can be cast to xsd:decimal, NaN and infinite values cast to integers or decimals are unbound, and casting other types such as dateTimes to numerics is unbound
- xsd:string() of a decimal gives the canonical form (e.g. "2.5"), not the stored representation with padding zeros
- "1"^^xsd:boolean and "0"^^xsd:boolean are valid booleans (previously rejected when parsing)
- STRBEFORE and STRAFTER accept a language tagged second argument when it is compatible with the first argument, and give the simple literal "" when there is no match, as required by the SPARQL 1.1 spec
- REPLACE keeps the language tag of its first argument
- CONCAT only gives a language tagged result when all arguments have the same language tag, and is unbound when an argument is unbound or not a string
- COUNT(DISTINCT *) is supported, and COUNT(*) works for patterns without variables
- SELECT * over a pattern without variables (e.g. { :a :b :c }) no longer panics

## v0.20.32
### Features
- Turtle output for construct queries

## v0.20.30 
### Features
- Add bindings parameter that replaces variables with ground terms (safe query parameterization without special syntax)

## v0.20.29
### Bug fixes
- Fix query external for empty results, variable sequence and iri-columns. 

## v0.20.28
### Performance
- Pretty turtle write performance drastically improved (around 12x better)

## v0.20.26
### Features
- The IRI maplib:DefaultGraph is the default graph
- m.query_external(query, endpoint) lets you query an external SPARQL endpoint (currently only select and HTTP GET).

## v0.20.25
### Bug fixes
- Fix issue where count over zero solution mappings returned no rows (should return one row with zero as the count)

### Performance
- Improve deserialization performance to hybrid storage and to in memory storage

## v0.20.24
### Bug fixes
- Bug fix for serialization of lang strings
- (Attempted?) Bug fix for disk based storage of categoricals on windows

## v0.20.23
### Features
- Serialization with disk based storage of categoricals - about 40x faster than ntriples read

## v0.20.22
### Performance
- Fix performance issue for ORDER BY when cats are on disk
- Fix performance issue in queries with a lot of OPTIONAL clauses (see bugfix below)

### Bug fixes
- Fix issue where OPTIONAL clauses sometimes introduces duplicates

## v0.20.21
### Features
- Support for OPC UA mapping to RDF using m.map_opc_ua() providing the folder with NodeSet2 XMLs as an argument
- m.add_udf() allows the user to create a UDF working on Polars DataFrames where arguments form the columns. The function is callable from SPARQL.
- m.serialize("folder") and Model.deserialize("folder") serializes and deserializes the triples in a Model object super quickly (20x faster than ntriples in our bench).

### Breaking (but not worthy of major release IMO)
- IndexingOptions no longer has subject_object_index argument as this is now default.

## v0.20.20
### Bug fixes
- Avoid panic when using pyarrow >= 24.0.0
- rep = m.validate() always produces rep.results() even when report_graph is specified.

### Features
- Facade X like mapping of dataframes to graph in m.map_df()
- Target counts for shapes are now part of the SHACL report graph

## v0.20.19
### Bug fixes
- Workaround for Polars issue that could sometimes produce errors in filter not exists
- Move away from rdf:_{n} predicate to represent lists in Facade-X as it is does not play nice with our partitioning

### Features
- Prefix maplib:<https://datatreehouse.github.io/maplib/vocab#>
- maplib:uuidv5 and maplib:struuidv5 functions
- m.templates_to_graph() materializes templates as triples in graph so they can be queried (Thank you @thenonameguy !)
- m.read(), m.reads() now eats hdt and m.write() spits it out. (Thank you @sihingkk !)

## v0.20.16
### Bug fixes
- Work around Polars issue where double not exists can return erroneous results
- Turn caching mechanism for on disk SHACL back on :-)
- Fix SHACL SPARQL issue with OPTIONAL {} that could return too few results. 
- Escape certain characters after the prefix in prefixed IRIS.

### Performance
- Move to latest Polars

## v0.20.15
### Performance
- Move to A* search for optimal join sequencing - we do unfortunately not take into account nesting here yet.

## v0.20.14
### Bug fixes
- Fix issue in cross join detection / query planning.

## v0.20.13
### Bug fixes
- Bug fix for optimization for certain types of large queries with OPTIONAL and BIND.
- Improve error message when UNBOUND(?v) does not find ?v. 

## v0.20.12
### Bug fixes
- Disk based cats panicked with duplicate string (large strings)
- Sometimes tried to write triples to disk (disabled functionality for the time being)

### Performance
- Caching for SHACL rules with disk based storage improves validation performance

## v0.20.11
### Bug fixes
- Exists queries with nested subqueries could be true when they should be false

### Performance
- Improve detection of cross joins, avoiding them better

### Features
- Integrate chrontext into maplib, enabled when m.add_virtualization() has been run.

## v0.20.10
### Bugfixes
- SHACL rules: incrementally build targets not always updated
- SPARQL adding now() and a few bugfixes having to do with multiple datatypes in expressions

### Performance
- Improvements to query planning for large queries

## v0.20.8
### Bug fixes
- xsd:decimal is now represented internally as Decimal(precision=38,scale=12)
- Fix another instance of missing query parameters in debug

### Features
- max_iterations parameter for shacl rules (validate())

## v0.20.7
### Bug fixes
- Fix bug where comparison expressions involving multiple types could fail. 

## v0.20.6
### Performance
- Subject object indexing for deduplication now default on, improves load speeds for many small files
- Batch size for triples read now defaults to 10_000_000, reducing memory consumption.

## v0.20.5
### Bug fixes
- Fix issues with IF where having multiple types for "then" and "otherwise" could cause an exception
- Fix issue where THIS was not injected into SHACL SPARQL

### Features
- rules_debug in validate now prints every step so it is easier to debug recursive issues

## v0.20.4
### Bug fixes
- Fix rounding issue
- Improve error messages from SHACL rules

## v0.20.3
### Bug fixes
- Fix issue with regex and replace where pattern was of form str(iri).
- Fix issue where debug_rules could sometimes panic in validate()

## Features
- Add support for STRDT
- Add missing support for SPARQL targets in SHACL Rules

## v0.20.2
### Bug fixes
- Fix caching issue in Datalog engine
- Work around issue with timestamped dates, these are now read without the timestamp, but should be fixed properly at some point.

### Features
- SHACL rules now support recursion, and has multiple optimizations. 
- Still missing conditions, other rule types than sparql construct, and taking ordering into account. 

## v0.20.1
### Bug fixes
- Fix nondeterminism that could sometimes cause duplicate SHACL results
- SHACL could sometimes contain extra results for deeply nested shape graphs
- SHACL graph no longer includes conforms bool for results, this one is just needed when include_details=True

## v0.20.0
### Breaking changes
- map_triples, map, map_default "df"-arg renamed to "data" as it now takes alternatively a DataFrame or a SolutionMappings object, in which case types can be specified for columns. A query run with solution_mappings=True can thus be piped into these methods.  
- parameters for query and update for the PVALUES construction now is a dict with SolutionMappings values.
- ValidationReport no longer has method graph(), instead, use the report_graph argument of validate to specify a named graph for the report. This also means that validate() argument include_shapes_graph is gone.

### Features
- Rudimentary shacl rules support (only SPARQL construct), no recursion yet. Use the inferences_graph-argument to validate to persist inferred triples to a named graph.  

### Bug fixes
- Fix for #48

## v0.19.25
### Features
- SPARQL parser returns the bad part of the query

### Bug fixes
- Fix for FTS issue when enabling for multiple named graphs
- Fix lang string issue with FTS

## v0.19.24
### Bug fixes
- Fix for missing triples in pretty turtle serialization under certain conditions involving orphaned blank nodes.

## v0.19.23
### Features
- Implement md5, sha1 and encode_for_uri functions.

### Improvements
- General improvements in read / map performance
- Improvements in error handling for PValues, missing Function implementations

### Bug fixes
- Fix bug in recursive constraints in SHACL
- Issue when using Datalog with SPARQL construct syntax
- Issue with lang string literal in SPARQL construct template

## v0.19.22
### Improvements
- Graceful error handling for SPARQL queries that crash in Polars.

### Bug fixes
- Contains function in SPARQL crashed with lang strings under certain circumstances

## v0.19.20
### Improvements
- All types with string types are now internally represented as u32 (previously it was only xsd:string, blanks and iris)

### Bug fixes 
- Result path missing in SPARQL constraints

## v0.19.19
### Bug fixes
- Bug fix for issue affecting SPARQL constraints with lang string messages with unbound vars of multiple types.

## v0.19.18
### Features
- CIM XML import removes superfluous "#_" from iris. 

## v0.19.17
### Features
- Basic support for disk based storage of categorical maps.
- Fix extra characters in CIM XML URIs by default

### Bug fixes
- Multiple bugfixes for SHACL

## v0.19.14
### Features
- read() and reads() for json-ld is now supported. Note that contexts are not resolved against the internet but must be provided in a dict due to security concerns.

### Improvements
- Pretty turtle writing has been parallelized, improving performance.

## v0.19.13 
### Features
- CIM XML parsing is now natively supported

### Improvements
- Added possibility of setting number of triples per batch when reading serialization formats.
- map_json is now compliant with Facade X. 

### Bugfixes
- Fixed various cases where a panic or unwrap happens instead of a proper error. 

## v0.19.10
### Features
- m.map_json() maps a json file or string to rdf triples.
### Bugfixes
- Writing turtle could sometimes write erroneous triples. 

## v0.19.9
### Improvements
- `maplib.explore()` is deprecated, use the explore method on a `Model` instead.
- `Model.explore()` no longer supports opening a browser window automatically.
  Use `webbrowser.open(s.url, new=2)` instead. Where `s` is returned from `explore()`
- Release of pretty turtle printer. It is still missing:
  - Anonymization of blank nodes that only have a single incoming reference.
  - Support for nested lists
- Datalog inference now works on SPARQL construct in preparation for SHACL rules support
- Improvements to prefix handling:
  - Default prefixes only shadowed by adding prefixes to Model
  - Prefixes set in pretty turtle print only shadow Model prefixes

### Bugfixes
- Fixed improper serialization of e.g. newlines in new pretty turtle printer
- Fixed issue where empty triples could be added
- Fixed several issues that could lead to OOM in the SHACL engine
- Fixed panic in CIM xml exporter that should be error message

## v0.19.6
### Improvements
- IRIs are now compressed in memory
- SELECT * has column ordering according to when the variable is used in the query, not alphabetical as previously.

## v0.19.5
### Bugfixes
- Inverse predicates missing from CIM exports

### Improvements
- Setting debug=True in query now prints the debugging of why the query has no solutions.

## v0.19.4

### Bugfixes
- Fix issue where byte datatypes were incorrectly created with the polars string datatype.

### Improvements
- Template printing now uses prefixes.
- Exception handling for missing functions. 

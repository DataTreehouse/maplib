# What's new in v0.26.0

A release of maplib Enterprise Edition. It needs Python 3.10 or newer (see Breaking changes in [CHANGES.md](../CHANGES.md), which has every change).

- **Python 3.10 or newer.** maplib no longer declares Python 3.9 support; the wheels were already built for Python 3.10 and newer. The Enterprise Edition release has one wheel per platform.
- **`geof:simplify` (EE).** Simplifies a geometry with the Douglas-Peucker algorithm, as in the GeoSPARQL draft after 1.1. The Graph portal's map uses it to read lighter grid lines.
- **Fewer ids sent to virtualized databases (EE).** A query that restricts time series in one part, e.g. a usage point by its EAN before an `OPTIONAL` or a joined subquery, or with an aggregate named like a variable of its pattern, sends only the ids of the matching time series, not all of them. A query over 222,217 usage points no longer goes over Azure Data Explorer's 2 MB query limit.
- **Time zones of virtualized databases (EE).** Timestamps a database answers with in a time zone other than UTC are read as UTC instants.
- **Any CIM version (EE).** The Graph portal loads a grid in the CIM namespace of its data, CIM16 or CIM100, and `cim.add_reading_types` types reading types in the namespace it is given.
- **Graph portal (EE).** Large areas sum their time series in the database, the map reads the grid only when the view leaves what was read, and queries an endpoint dropped after a long time are not sent again.

# What's new in v0.25.3

A release of maplib Enterprise Edition with two speed-ups and three fixes. [CHANGES.md](../CHANGES.md) has every change.

- **Faster paths and aggregates.** A zero-length property path between two unbound variables, e.g. `?a ex:p* ?b`, reads the graph's nodes from the stored columns: on 10M nodes a `COUNT` over it takes 0.11 s instead of 1.22 s. `MIN` and `MAX` over a column of several types scan each group once instead of sorting it, 15 to 35 times faster.
- **Virtualized queries (EE).** With several time series resources, a virtualized query goes only to the resources whose template has its predicates, so queries without a class pattern no longer give empty rows or fail.
- **Memory limits.** With `abort_above_mb` on a machine with many cores, a large join is stopped with a `MemoryError` before the process is aborted.
- **Graph portal (EE).** The bundled portal stores the times of its model summaries as UTC without a time zone again, so it no longer rewrites them on every start.

# What's new in v0.25.2

A fix release of maplib Enterprise Edition. [CHANGES.md](../CHANGES.md) has every change.

- **Time zones of mapped date-times.** A time zone aware Polars `Datetime` column mapped to an `xsd:dateTime` or `xsd:dateTimeStamp` template parameter keeps its time zone, written as `Z`; it was dropped. A value with a time zone no longer equals the same value without one, as XSD defines them.
- **Virtualized values (EE).** A triple pattern on a value from a virtualized database whose predicate has no template, e.g. `?type cim:IdentifiedObject.name ?name` for the type of a meter event, is now evaluated on the virtualized results instead of being dropped, so its variables are bound.

# What's new in v0.25.1

A release of maplib Enterprise Edition with CIM events and two fixes for virtualized time series. [CHANGES.md](../CHANGES.md) has every change.

- **CIM events (EE).** Faults, meter events and other CIM events can stay in a database and be queried with CIM predicates only, through the event templates of `m.cim`. The usage guide shows earth faults and meter events in DuckDB, and the Graph portal shows a grid's faults and meter events in an Events tab of the asset panel and a Faults list on the map.
- **Fixes (EE).** A constant object of a virtualized triple pattern, e.g. a fault kind or an event reason, is now sent to the database as a condition; it was ignored, so every row came back. A template parameter of type `ottr:IRI` filled by the database now comes back as an IRI instead of a string, so comparisons with IRIs match.

# What's new in v0.25.0

A small release on top of v0.24.0. Features marked **EE** need [maplib Enterprise Edition](../README.md#open-source-and-enterprise-edition); [CHANGES.md](../CHANGES.md) has every change.

- **`m.cim` (EE).** The CIM grid tools are bound to the model: `m.cim.cut_to_graph(...)` cuts part of a grid out, and `m.cim.add_serves`, `m.cim.add_reading_types` and the meter reading templates work on the model directly. In open-source maplib, `m.cim` raises an `AttributeError` that says it needs maplib Enterprise Edition. `import cim` still works.
- **CIM measurement time series (EE).** The values of Analog, Discrete, Accumulator and StringMeasurement measurements, and meter readings, can stay in a database and be queried with CIM predicates only. `m.cim.measurement_template` and `m.cim.meter_readings_template` are the Model portal's CIM templates, which you give to `m.add_virtualization` for your own resources. The usage guide shows it with DuckDB.
- **Breaking (EE).** `cim` no longer has default chrontext resources, since they vary between deployments: `add_meter_readings_virtualization`, `meter_readings_resources` and `METER_READINGS_RESOURCE` are gone, and the meter reading templates, shapes and annotations take the resource name. Register resources with `m.add_virtualization(vdb, resources={name: m.cim.meter_readings_template(name)})`.
- **Fix (EE).** A query over virtualized time series whose static part is a join of joins, such as the meter readings under a root on the Model portal's Served power page, failed; it now answers.

# What's new in v0.24.0

A performance release: most SPARQL queries run on maplib's dictionary codes instead of building RDF terms, and small queries cost about half as much. It also fixes wrong or failing results for `OPTIONAL`, `MINUS`, `ORDER BY` and property paths. Features marked **EE** need [maplib Enterprise Edition](../README.md#open-source-and-enterprise-edition). [CHANGES.md](../CHANGES.md) has every change.

## Changed results

- Sequence property paths with a closure step, e.g. `?s ex:includes/rdf:rest+ ?o`, give one solution per intermediate node, as SPARQL joins the steps of a sequence. They gave each pair of ends once, so queries that relied on that now need `DISTINCT`.
- Zero-length paths whose ends are both unbound, e.g. `?s ex:p* ?o`, match every subject and object of the graph to itself, as SPARQL defines them.

## Highlights

- **Smaller fixed cost per query.** A query no longer switches to a fresh 1 GB stack, and small plans are collected on a helper thread instead of Polars' thread pool: small queries take about half as long.
- **Joins on codes.** Joins push their values into the next lookup as dictionary codes, `COUNT(*)` over stars, chains, `EXISTS`, `OPTIONAL`, `MINUS` and `UNION` is counted without building rows, and selective basic graph patterns from a constant subject or object are evaluated by index nested loops when their estimated time is below the general path's (BSBM explore mix 1.6 times as fast).
- **Statistics.** Each segment keeps a sketch of its subjects and objects, exact distinct counts and Bloom filters, used to order joins and skip segments; `Model.analyze(deep=True)` adds exact counts of the codes two tables share.
- **Counts and paths.** Whole-graph counts are answered from the stored tables, `COUNT(DISTINCT)` uses a bitset over codes and is cached, and transitive paths walk the tables' indexes on codes.
- **Updates and storage (EE).** Small updates are buffered per table, hybrid and disk dictionaries stay out of memory with a Bloom filter for absent strings, and dropped models give back their memory and threads.
- **Fixes.** `OPTIONAL` over `UNION`, `MINUS` and `OPTIONAL` on variables of several types, nested negations in linear time, `ORDER BY` across numeric datatypes, control characters in N-Triples and SPARQL JSON, and SHACL-SPARQL `MINUS` with pre-bound variables (EE).

# What's new in v0.23.1

A fix release for v0.23.0: `FILTER (NOT) EXISTS` over virtualized time series (maplib Enterprise Edition) works again, and the Linux wheels that were missing from PyPI for v0.23.0 are published. It also uses less memory and time in three places: a small `DELETE DATA` no longer takes time in proportion to the model (about 2 ms for one triple in a model of 1.4M triples, in every storage mode), writing Turtle, RDF/XML and JSON-LD no longer holds the whole output in memory (68 MB instead of 587 MB above the model for 117 MB of N-Triples), and reading files with many predicates has no fixed cost per predicate (204 MB instead of 507 MB for 2,000 predicates, and 2 to 3 times faster). See [CHANGES.md](../CHANGES.md).

# What's new in v0.23.0

A short guide to v0.23.0, which adds features and fixes to v0.22.1 and changes some APIs (see Breaking changes). Features marked **EE** need [maplib Enterprise Edition](../README.md#open-source-and-enterprise-edition); everything else is in the open-source core. [CHANGES.md](../CHANGES.md) has the full details, [API_USAGE.md](../API_USAGE.md) has worked examples, and the [documentation site](https://datatreehouse.github.io/maplib/) has both with the API reference.

## Highlights

- **Graph algorithms.** `maplib:component`, `maplib:bridge` and `maplib:articulationPoint` find connected components, bridges and articulation points as magic predicates, with `maplib:avoid` for what-if analysis.
- **CIM grids (EE).** `cim.scissors.cut_to_graph` cuts part of a grid out with SPARQL, and `cim.add_serves` reads the serves relations from several graphs.
- **Memory limits per query.** `Model.query(max_memory_mb=...)` stops a query at its own memory limit, and a timed-out or abandoned query stops at once instead of running on.
- **One set of default prefixes.** `maplib.well_known_prefixes()` is maplib's only table of defaults, with `cim` for CIM100, and prefixes can be kept in the config graph.
- **Durable portal writes (EE).** The writable Model portal answers a write only once it is published, and leaves a rebuilt default graph out of its snapshots.
- **SPARQL additions.** DESCRIBE, the XPath math functions (`math:sqrt`, `math:pow`, ...), parameterised updates and per-row REGEX patterns.
- **Faster reading.** Turtle, TriG, N-Triples, N-Quads, RDF/XML and CIM/XML parse faster, and `map_json`/`map_xml` map many documents at once.
- **Correct results.** Fixes for joins of mixed types, FILTER placement, string comparisons, GROUP BY and numeric type promotion, checked against the W3C SPARQL 1.0 and RDF 1.0 suites, which now run with the tests.

## Major features

**Graph algorithms.** `?n maplib:component ?o` (or strongly connected with `maplib:strong true`), `?a maplib:bridge ?o` and `?n maplib:articulationPoint ?o` run union-find and iterative Tarjan over the cached integer graph. Edges come from predicates or from other patterns' rows (`maplib:edgeSource`/`maplib:edgeTarget`), and `maplib:avoid` leaves nodes out without changing the model. They are checked against brute force and the LDBC Graphalytics validation graphs.

**Memory limits.** A timeout stops a runaway query on time but not its memory. `max_memory_mb` on `query` and `update`, and a default with `maplib.set_memory_limit(max_query_memory_mb=...)` that served queries **EE** use too, stop a query when it grows past its limit. Joins of timed-out or cancelled queries stop without aborting the process, and the next query waits for their memory to be freed.

**Prefixes.** `maplib.well_known_prefixes()` lists the 43 default prefixes (`cim` is CIM100, `cim16` CIM16). `maplib.config` keeps a model's own prefixes in its config graph as SHACL prefix declarations, and `generate_prefixes` suggests names for the namespaces a graph uses.

**SPARQL.** DESCRIBE returns the triples of the described IRIs. The XPath 3.1 math functions run the specification's examples as tests. `update(bindings=...)` parameterises updates, `REGEX` and `STRBEFORE`/`STRAFTER` take arguments that differ per row, and `query(bindings=...)` accepts Python `bool`, `int` and `float`.

**Performance.** The vendored oxttl parsers and the RDF/XML parser are faster; `map_json` and `map_xml` add up to a thousand documents at once (20,000 JSON documents: 36 s to 0.31 s); vector literals are stored packed (about two fifths of the memory); joins on two shared variables no longer take quadratic memory; reasoning with `owl:sameAs` uses one representative per group; small-batch steps of streams and `scratch_graphs` cost less, especially in hybrid and disk storage **EE**.

## Fixes

- Joins on variables with values of several kinds no longer match values of different kinds, which gave wrong `owl:sameAs` merges and OPTIONAL results.
- A FILTER after VALUES, a property path, GRAPH or SERVICE is no longer dropped, and a FILTER in a nested group no longer sees the enclosing group's variables.
- `EXISTS` and `NOT EXISTS` treat a variable left unbound by OPTIONAL, UNION or VALUES as free, as SPARQL 1.1 says, and use less memory.
- String comparisons and MIN/MAX of stored strings use codepoint order.
- GROUP BY a variable the grouped pattern does not bind gives one group, and a SELECT expression that is a GROUP BY expression is the group's value.
- Arithmetic follows SPARQL's numeric type promotion, without overflow on small integer types, and no longer runs away in memory on many datatypes.
- `-(0.0e0)` is negative zero, an empty `rdf:parseType="Literal"` is read, and property paths with a constant of another type give no solutions instead of panicking.
- Security: quick-xml 0.41 and zstd 0.13.4, and DuckDB 1.5.6 in maplib Enterprise Edition.

## Breaking changes

| Change | Migration |
|---|---|
| `Model.reason(profile, ...)` **EE** is now `Model.infer(profile=..., ...)` | Rename the call; the parameters and `ReasoningResult` are the same |
| `Model.update(update, graph=G)` writes to `G`, as `WITH <G>` | Updates that relied on writing to the default graph must add `GRAPH` or drop `graph=` |
| A misspelt magic predicate, or `maplib:fts` without an index, is an error | Fix the predicate name, or add the full text index |
| `STRSTARTS`, `STRENDS` and `CONTAINS` with a non-string second argument from the data are unbound | Check filters that relied on the query failing |
| `+`, `-` and `*` give the promoted XPath datatype, e.g. `xsd:integer` for two `xsd:int` | Check code that read the result's datatype |

# What's new in v0.22.1

A short guide to v0.22.1, which adds features and fixes to v0.22.0. Features marked **EE** need [maplib Enterprise Edition](../README.md#open-source-and-enterprise-edition); everything else is in the open-source core. [CHANGES.md](../CHANGES.md) has the full details, and [API_USAGE.md](../API_USAGE.md) has worked examples.

## Highlights

- **Reasoning.** `m.reason(profile="owl-rl" | "rdfs-plus" | "rdfs")` **EE** materializes OWL 2 RL, RDFS-Plus or RDFS entailments and reports violations.
- **R2RML.** `m.map_r2rml(mapping, database=...)` maps relational data with W3C R2RML mappings, column by column in parallel.
- **Microsoft Fabric.** `m.map_fabric(ontology, lakehouse=..., eventhouse=...)` **EE** maps a Fabric IQ ontology and its data, and virtualizes its time series.
- **SPARQL extensions.** Window functions, ORDER BY inside aggregates, `LATERAL`, GRAPH blocks in CONSTRUCT, and `update` reports what it changed.
- **Keyset pagination.** `m.query_pages(query, page_size=...)` iterates over large SELECT results page by page, and `serve` **EE** pages them with `page_size` and `page_token`.
- **Vector search.** `maplib:vector` literals hold embeddings, and `maplib:vectorSearch` **EE** finds the nearest ones.
- **Streams on a persistent model.** `stream=True, in_place=True` maps each batch into the model itself, so steps can join batches with the model's other graphs.
- **Outbound connection policy.** `maplib.set_outbound_policy(mode="public" | "allowlist", allow=[...])` decides which hosts maplib may connect to, against SSRF; cloud metadata services are always refused.
- **SHACL entailment.** Shapes graphs that declare `sh:entailment` RDFS or OWL **EE** are validated over the matching `reason` profile, and complex result paths are kept in reports.
- **Graph management with a storage folder.** `CLEAR`, `DROP`, `COPY`, `MOVE` and `truncate_graph` work in hybrid and disk storage **EE**.

## Major features

**Reasoning profiles (EE).** `m.reason(profile=..., graph=None, target_graph=None)` runs a built-in ruleset on the Datalog engine and returns a `ReasoningResult` with the number of inferred triples and the violations found. `"owl-rl"` implements the OWL 2 RL/RDF rules, `"rdfs-plus"` adds the common OWL property rules to RDFS, and `"rdfs"` is RDFS alone. The profiles are tested with the W3C OWL 2 RL, RDF 1.1 Semantics and SPARQL 1.1 entailment suites.

**R2RML.** `m.map_r2rml(mapping, database=None, tables=None, base_iri=None, graph=None)` runs each logical table once, against a database in `maplib.virtualization`, a DuckDB connection or polars DataFrames, and evaluates the term maps column by column in parallel. It is tested with the W3C R2RML test cases.

**Microsoft Fabric (EE).** `m.map_fabric(...)` turns a Fabric IQ ontology into OWL, maps the bound lakehouse data with a generated R2RML mapping (which it returns), and virtualizes time series in the lakehouse (SQL) or eventhouse (KQL).

**SPARQL additions.**
- Window functions: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()` and the aggregates with `OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN ...)`, in SELECT, BIND, FILTER and HAVING.
- `GROUP_CONCAT(?x; SEPARATOR=", " ORDER BY ?y)` and `SAMPLE(?x ORDER BY ?y)`.
- `LATERAL` correlated subqueries (SEP-0006), e.g. the top N per entity.
- GRAPH blocks in CONSTRUCT templates, as in Apache Jena; `insert` writes each block to its graph in one pass.
- Statistical aggregates from Apache Jena: `agg:stdev`, `agg:variance` and their sample and population variants, also as window functions.
- `m.update(...)` returns the triples really inserted and deleted per graph, and with `return_triples=True` the triples themselves.

**Vectors.** Polars array columns map to `maplib:vector` literals. With `IndexingOptions(vectors=True)` **EE**, `maplib:vectorSearch` finds the nearest vectors by cosine, dot product or Euclidean distance, exactly or with an approximate index.

**In-place streams.** `m.map(..., stream=True, in_place=True)` runs every batch in the model under its write lock. `.drop_graphs([...])` removes per-batch graphs, and `compact_every=N` compacts the model as it goes. `m.drop_graphs(...)` and `with m.scratch_graphs(...):` do the same outside streams.

**Outbound policy.** Every connection maplib makes for a configuration, a virtualized database, `query_external`, `map_osipi`, `map_insights_hub`, `map_cdf`, IP.21 **EE** or OPC UA is checked against the policy before connecting, including redirects and every address a host resolves to. Refusals raise `maplib.OutboundError`. `maplib.outbound` has the same checks for your own clients.

**Keyset pagination.** `m.query_pages(query, page_size=10000)` and `m.query_page(query, page_token=...)` return pages of a SELECT query ordered by its ORDER BY, with a token for the next page, so deep pages cost the same as the first. Pages can be SPARQL JSON results. `serve` **EE** exposes them on its endpoints, and applies the `limit` and `offset` protocol parameters.

## Fixes

- The FILTER of an OPTIONAL can use variables bound before the OPTIONAL, and variables left unbound by an OPTIONAL or UNION join correctly with the rest of their group.
- Deeply nested `REGEX`, `SUBSTR` and `REPLACE` calls parse in linear time (they could hang `map_csvw`).
- `FILTER (NOT) EXISTS` is correct next to variable predicate patterns.
- `GRAPH ?g { … }` keeps `?g` in scope on a model without named graphs, so `GROUP BY ?g` gives an empty result instead of an error.
- `COUNT(DISTINCT ?x)` no longer counts unbound as a value; `SAMPLE` skips unbound values, and `AVG` of nothing is 0.
- ORDER BY sorts numbers of different datatypes by value, and puts blank nodes before IRIs as SPARQL specifies.
- Virtualized time series queries **EE** now match materialized results for subqueries in joins, GROUP BY on timestamps, empty aggregates and repeated data point patterns.
- `GraphSnapshots` **EE** no longer duplicates graphs on restore or rewrites them all after it, and backs off when the store fails.
- Vector functions and search agree with numpy and scipy on edge cases.
- Streams hand their blank node counters back, so later streams don't reuse labels.
- `maplib.storage` gives the same version token on every backend.

## Breaking changes

| Change | Migration |
|---|---|
| `Model.update` returns the changes per graph instead of `None` | Ignore the result, or use it |
| `IRI()`, `URI()` and `fn:castToIRI` give unbound for invalid or relative IRIs | Check queries that built IRIs from unvalidated strings |
| The time series vocabulary moved from `chrontext:` to `maplib:` (`maplib:hasTimeseries`, `maplib:hasDataPoint`, ...) | Nothing for queries and data: the old IRIs are still read. Custom Python virtualized databases that compare function IRIs must compare with the `maplib:` ones |
| `AVG` of integers is `xsd:decimal` (and of floats `xsd:float`) | Check code that read the AVG column as an integer or double type |
| Rust: `UpdateResult` has `changes`, and `delete_construct_result`/`delete_triples_vec` take `keep_triples` | Pass `keep_triples=false` for the old behaviour |

# What's new in v0.22.0 (since v0.20.32)

A short guide for deciding whether to upgrade from v0.20.32 to v0.22.0. It covers v0.20.33, v0.21.0 and v0.22.0. Features marked **EE** need [maplib Enterprise Edition](../README.md#open-source-and-enterprise-edition); everything else is in the open-source core. [CHANGES.md](../CHANGES.md) has the full details, and [API_USAGE.md](../API_USAGE.md) has worked examples.

## Highlights

- **Queries no longer run out of memory.** Over a memory limit (80% of system memory by default), joins, aggregations, sorts and results spill to disk.
- **Much faster.** Turtle is read 40–60% faster, RDF/XML is written 56× faster, SHACL validation is 67–93% faster **EE**, and Datalog is about 2× faster **EE**.
- **Graphs larger than RAM.** `Model(storage="memory" | "hybrid" | "disk")` **EE** keeps dictionaries and triple tables on disk.
- **One-file models, in the open-source core.** `m.serialize()` writes a single Parquet-based `.maplib` file, locally or to `s3://`, `gs://` or `az://`.
- **More SPARQL.** Federated `SERVICE` and `LOAD`, length-constrained and shortest paths, `ASK`, date and duration arithmetic, `INSERT DATA`/`DELETE DATA`, and many W3C compliance fixes.
- **Serve and stream.** `m.serve()` **EE** exposes a model over the SPARQL 1.1 Protocol and Arrow Flight, and `stream=True` maps unbounded inputs batch by batch.
- **Timeouts.** `query` and `update` take `timeout=` and stop long queries; `serve` **EE** stops queries at a timeout and when their client disconnects.
- **Models kept in object storage.** `serve` **EE** snapshots a served model after changes, and the writable Model portal **EE** keeps one file per graph, shared by several writers.

## Major features

**Storage modes.** `Model(storage=..., storage_folder=...)`, also for `Model.deserialize`. `"memory"` is the default. `"hybrid"` **EE** keeps the IRI and literal dictionaries on disk (what `storage_folder` alone used to mean, and still does). `"disk"` **EE** also keeps the triple tables on disk as Parquet. Disk-mode subject lookups are about 10× faster than before.

**Single-file `.maplib` serialization, now open source.** Serialization is Parquet throughout, so it no longer needs EE. `serialize` writes one file by default (`./model.maplib`), never leaves a partial file behind, and is about 4× faster to write and 3× smaller than the old DuckDB dictionaries. `deserialize` reads files, folders and older serializations. `raw_db=True` **EE** keeps DuckDB dictionaries, which hybrid and disk storage can use in place.

**Object storage.** `read`, `read_template`, `write`, `write_cim_xml`, `serialize` and `deserialize` accept `s3://`, `gs://`, `az://`/`abfss://`, HTTP(S) and `file://` URLs. Credentials come from the cloud SDKs or from `storage_options`. A folder serialization publishes atomically, so readers see either the old model or the new one. **EE**: a disk-mode model deserialized from object storage downloads its triple tables only when they are first read, with an optional local cache size.

**Streaming with `ModelStream`.** `m.map(..., stream=True)` (also `map_triples`, `map_json` and `map_xml`) returns a lazy pipeline of `.update()`, `.insert()`, `.infer()` **EE** and `.write()` steps. `.collect()` runs the pipeline one batch at a time, so memory use depends on the batch size, not the input size. `map_json` and `map_xml` also take an iterable of documents, e.g. JSON Lines.

**CSV on the Web.** `m.map_csvw(source, metadata=None, ...)` implements the W3C CSVW recommendations (standard and minimal mode, dialects, datatypes, foreign keys) and is tested against the W3C test suite.

**SPARQL additions.**
- Federated query: `SERVICE`, `SERVICE SILENT` and `SERVICE ?endpoint`, evaluated as bind joins. SPARQL Update `LOAD`. Configure both with `m.set_federation(...)`; `LOAD` is off until `allow_load=True`.
- Length-constrained property paths (`ex:p{2,4}`), plus the `maplib:path` and `maplib:shortestPath` magic predicates (weights, k shortest paths, path nodes).
- `ASK`, `INSERT DATA`/`DELETE DATA`, `isNUMERIC`, `RAND`, `SHA256`/`SHA384`/`SHA512`.
- Arithmetic and comparisons on `xsd:dayTimeDuration`, `xsd:yearMonthDuration`, dates, dateTimes and times.
- Many corrections from the W3C test suites: typed `COUNT` and `SUM`/`AVG`/`MIN`/`MAX`, decimal division, `GROUP_CONCAT`, casts, effective boolean values, and left-associative arithmetic.
- Syntax errors in SPARQL and stOTTR are reported compiler-style, with the line, the source and hints.

**Formats and I/O.** HDT is read 12× and written 28× faster (see Performance). TriG and N-Quads are read and written. JSON-LD output is compacted JSON-LD 1.1. `graph=[DEFAULT_GRAPH, "http://…/g1"]` writes exactly those graphs. `m.read` takes bytes or file-like objects, and `m.write_bytes(format=...)` and `write(<binary file-like>)` write to memory. RDF/XML is read in parallel and written by a new pretty serializer.

**Query result cache.** `m.set_query_cache(QueryCacheOptions(...))` keeps query results until a write changes a graph and predicate they read. It is shared with `serve`, and `query_cache_stats()` and `clear_query_cache()` report on it and empty it. It is off by default.

**The config graph.** A model keeps its configuration (virtualized databases, indexes, the query cache) as triples in `maplib.config.CONFIG_GRAPH`. `maplib.config` writes and reads that configuration, and `apply_config(m)` connects and applies it. Secrets are never stored, only the names of the environment variables that hold them. See [CONFIG_GRAPH.md](../CONFIG_GRAPH.md).

**`serve` and `explore` (EE).** `m.serve()` serves a live model over the SPARQL 1.1 Protocol, the Graph Store Protocol and Arrow Flight, with a query log and optional static web apps. Updates, `SERVICE` and `LOAD` are refused unless you allow them. `m.query(q, endpoint=server.url)` queries another maplib server. `m.explore()` now runs the full Model portal on the model.

**GeoSPARQL and full-text search (EE).** `IndexingOptions(geo=True)` indexes WKT, WKB and GeoJSON geometries spatially and supports the GeoSPARQL relation, measurement and construction functions. Full-text search now uses DuckDB on the stored dictionaries, with options for which predicates, local names and IRIs to index.

**Time series virtualization (EE).** One model can use several virtualized databases together. New connectors: SQL databases through `maplib.virtualization` (DuckDB, Snowflake, BigQuery, Databricks, TimescaleDB, Microsoft Fabric warehouses and lakehouses, SQL Server), InfluxDB 1.8/2/3, Azure Data Explorer and Fabric Eventhouses (KQL), OPC UA History Access, AspenTech IP.21, OSIsoft PI (`map_osipi`, `map_pi_points`), Siemens Insights Hub (`map_insights_hub`) and Cognite Data Fusion (`map_cdf`). Queries over virtualized models now use the same SPARQL evaluation as the rest of maplib. `ASK` and `CONSTRUCT` work instead of panicking, and several wrong results are fixed, in `MINUS`, `LIMIT`/`OFFSET`, `FILTER NOT EXISTS` and `VALUES`. One known limitation: inside `MINUS`, a data point pattern that is not linked to a time series (with `?ts maplib:hasDataPoint ?dp`) is not virtualized, so it matches nothing.

**SHACL rules and SHACL fixes (EE).** Rules no longer loop when they re-assert existing triples (#76), and they run for focus nodes that become targets later (#77). Shapes with several `sh:message` values no longer lose results (#86). `sh:not` gives correct results. Reports include `sh:sourceConstraint`. RDFS inference follows `subClassOf` chains of any length.

**Query timeouts and cancellation.** `m.query(q, timeout=30)` and `m.update(u, timeout=30)` stop the query when its time is up and raise `maplib.QueryTimeoutError`. Queries are checked between operators, inside joins, in path searches and while results are formatted. **EE**: `m.serve(query_timeout=..., max_query_timeout=...)` sets a default and a ceiling for its endpoints' queries, and stops a query when its client disconnects.

**Raw bytes in object storage.** `maplib.storage` puts and gets bytes at the same URLs and with the same credentials as `read` and `write`: `put(url, data, if_match=..., if_none_match=...)` writes only if the object is unchanged, and returns its version token. `get`, `head`, `exists`, `delete` and `list` complete it. Conditional puts make it safe for several writers.

**Snapshots of served models (EE).** `m.serve(snapshot_url=..., snapshot_interval=900)` serializes the model to object storage when it changed, and when the server stops; an unchanged model is never written. `python -m maplib.serve` serves a serialized model configured with environment variables, as the deploy recipes in maplib Enterprise Edition do. `maplib.GraphSnapshots` keeps a model as one HDT file per named graph with a manifest: only changed graphs are written, and writers that conflict merge their changes per graph. `m.portal(writable=True)` uses it to keep the Model portal's uploads.

**Default templates.** `m.map_default(data, primary_key_column)` maps a DataFrame with a template generated from its columns, as before. The new `m.generate_template(data, primary_key_column)` is its dry run: it returns that template as stOTTR text to review or edit. `maplib.generate_templates(m)`, which generates templates from an OWL/RDFS ontology, now runs in Rust, in about 60% of the time, with the same output.

**OPC UA typing shapes (EE).** `m.opcua.add_typing_shapes()`, after `m.map_opc_ua(...)`, generates SHACL shapes that check OPC UA instances against their ObjectTypes and VariableTypes, including mandatory children. It runs in Rust, with memory linear in the number of instances.

**Thread-safe concurrent reads.** A `Model` shared between threads runs reads such as `query` and `write` at the same time, and writes wait for exclusive access. Ctrl-C stops a running query.

## Memory cap and spill to disk

maplib now caps its memory use for the whole process. By default, the cap is **80% of system memory**, and it respects container (cgroup) limits. When a query goes over the cap, its **joins, aggregations, sorts and query results** move intermediate data to disk instead of failing or being killed by the OS.

```python
import maplib
maplib.set_memory_limit(
    max_memory_mb=8000,          # spill above 8 GB
    spill_dir="/scratch/maplib", # default: system temp folder; set before the first query
    max_spill_mb=100_000,        # most to spill (default 64 GB)
    abort_above_mb=None,         # optional hard stop of the process (off by default)
)
maplib.memory_usage(), maplib.memory_limit()   # bytes in use, current limit
```

- The cap counts everything maplib allocates, including stored triples, but only query state spills.
- `ORDER BY ... LIMIT` does not spill, because it uses the faster in-memory engine. `MAPLIB_POLARS_ENGINE=streaming` makes it spill.
- **Replaces** the removed `max_results` parameter of `infer` and the removed `max_shape_constraint_results` parameter of `validate`: the memory limit now bounds inference and validation. `max_iterations` and `max_rows` are unchanged.

## Performance

These numbers compare v0.21.0 with v0.20.33 on an Apple M-series laptop:

| Workload | Change |
|---|---|
| Reading Turtle (260 MB – 2.9 GB) | 40–61% faster |
| Reading RDF/XML (CIM, 2.2 M triples) | 52% faster |
| Writing RDF/XML (CIM, 2.2 M triples) | 42 s → 0.76 s (56× faster) |
| Writing CIM/XML (`write_cim_xml`) **EE** | 50–92% faster |
| SHACL validation, in memory **EE** | 67–93% faster |
| Datalog inference, CIM enrichment **EE** | about 2× faster (12.4 s → 6.3 s) |
| Writing Turtle for some CIM models | no longer hangs |

v0.22.0 adds more on top of v0.21.0: reading N-Triples is up to 81% faster, RDF/XML reading is up to 68% faster and template mapping is up to 89% faster, with up to 39% lower peak memory. Other improvements in v0.22.0:

- Disk-mode lookups of a subject's properties are about 10× faster **EE**.
- HDT is read 12× and written 28× faster than in v0.20.32 (CIM, 2.2 M triples: 3.41 s → 0.28 s and 5.69 s → 0.20 s), without building HDT query indexes; the written files are unchanged.
- Bound property paths are 84–92% faster.
- `VALUES` joins with large tables are about 80% faster.
- `ORDER BY ... LIMIT` over millions of strings is up to 90% faster.
- `COUNT(*)` over all triples is answered immediately, from the table sizes.
- `maplib.add_triples` copies triple tables in Rust: 1.8× faster for a few predicates, 15× for thousands.
- Joins on two variables where many rows share one value no longer use memory quadratic in the rows (100 000 OPC UA instances: 0.15 GB instead of over 134 GB).

## Breaking changes

| Change | Migration |
|---|---|
| Transient triples are gone; the `transient=` parameter is removed | Pass `graph=maplib.TRANSIENT_GRAPH` (`target_graph=` for `insert`) |
| `include_transient` is removed | Use `include_graphs=[...]`. For `query`, `update`, `insert` and `infer`, `None` includes the transient graph and `[]` includes nothing |
| `infer_rdfs` and SHACL rules write inferences to the transient graph | Write them with `graph=TRANSIENT_GRAPH`; TriG/N-Quads writes of all graphs leave the transient graph out |
| The `streaming` parameter is removed (`query`, `update`, `insert`, `validate`, report methods) | Delete the argument; it had no effect |
| `max_results` (`infer`) and `max_shape_constraint_results` (`validate`) are removed | Use `maplib.set_memory_limit` |
| `serialize`/`deserialize` default to one file at `./model.maplib` (was the folder `./serialized_triples`) | Pass `single_file=False` and an explicit path to keep a folder |
| `ValidationReport` `message` column is a list of strings | Use `.explode("message")` for one row per message |
| RDF/XML and CIM/XML are read in parallel by default | Pass `parallel=False` for sequential parsing; error line numbers may be relative to a chunk |
| RDF/XML output is laid out differently (same triples) | No code change; diff-based tests may need new baselines |
| `write_cim_xml` uses `rdf:ID="_<mRID>"` as CGMES requires | Read the output with the same `base_iri` as the source data |
| `explore()`: `page`, `graph` and `fts_path` are ignored; `flight_port` and `cors_origins` are removed **EE** | Drop the arguments |
| `xsd:dateTime` values are stored as UTC instants; naive dateTimes come back UTC-aware (`datetime[μs, UTC]`) | Compare with aware datetimes, or drop the time zone in Python. Values serialize as before, and older models load the same |
| `add_triples` from another model gives the copied blank nodes new labels (same-label blank nodes in two models were merged before) | Copy within one model, or give the nodes IRIs, to keep them the same |
| SPARQL fixes change some results: `COUNT` is `xsd:integer`, `5 / 2` is `2.5`, `8 - 3 - 2` is `3` | Check queries that relied on the old behaviour |

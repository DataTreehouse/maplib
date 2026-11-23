<h1 align="center">maplib</h1>

<p align="center">
  <strong>Build, query, reason over and validate RDF knowledge graphs at DataFrame speed, from Python.</strong>
</p>

<p align="center">
  <a href="https://pypi.org/project/maplib/"><img alt="PyPI" src="https://img.shields.io/pypi/v/maplib.svg"></a>
  <a href="https://pypi.org/project/maplib/"><img alt="Python versions" src="https://img.shields.io/pypi/pyversions/maplib.svg"></a>
  <a href="LICENSE"><img alt="License: Apache 2.0" src="https://img.shields.io/badge/license-Apache%202.0-blue.svg"></a>
  <a href="https://datatreehouse.github.io/maplib/"><img alt="Documentation" src="https://img.shields.io/badge/docs-website-green.svg"></a>
</p>

maplib is a knowledge graph engine that runs inside your Python process. It turns DataFrames, CSV, JSON, XML and RDF files into a knowledge graph, which you can query with SPARQL and GeoSPARQL, enrich with rules and OWL 2 RL reasoning, validate with SHACL and write out in any standard RDF format. You don't need a server or a database: `pip install maplib`, create a `Model`, and query results come back as Polars DataFrames.

maplib is written in Rust and stores triples column-wise in [Apache Arrow](https://arrow.apache.org/) via [Polars](https://pola.rs/). Mapping and querying therefore run as vectorised, multi-threaded table operations, not one triple at a time.

## Installation

```shell
pip install maplib
```

Prebuilt wheels are available for Linux, macOS and Windows (Python 3.10+).

## Quickstart

Load a plant model, reason over it, validate it, and query it together with sensor data that stays in a database:

```python
import duckdb
from maplib import Model, VirtualizedDatabase
from maplib.virtualization import DuckDBDatabase

m = Model()

# Load: any RDF format, from a path, a URL or s3:// / gs:// / az://
m.read("ontology.ttl")                     # ex:CentrifugalPump rdfs:subClassOf ex:Pump
m.read("plant.ttl")                        # ex:pump1 a ex:CentrifugalPump ; ct:hasTimeseries ex:ts1 ...
m.read("shapes.ttl", graph="urn:shapes")   # SHACL: every ex:Pump needs an ex:ratedPower

# Infer: RDFS reasoning makes every centrifugal pump an ex:Pump
m.infer_rdfs()

# Validate with SHACL (EE)
report = m.validate(shape_graph="urn:shapes")
print(report.conforms, report.results())   # the violations, as a DataFrame

# Virtualize (EE): readings stay in DuckDB and are fetched only when a query needs them
m.add_template("""@prefix ct: <https://github.com/DataTreehouse/chrontext#> .
ct:Speed [ xsd:string ?id, xsd:dateTime ?timestamp, xsd:double ?value ] :: {
  ottr:Triple(?id, ct:hasDataPoint, ?dp) , ottr:Triple(?dp, ct:hasTimestamp, ?timestamp) ,
  ottr:Triple(?dp, ct:hasValue, ?value) } .""")
db = DuckDBDatabase(duckdb.connect("plant.duckdb"))
m.add_virtualization(VirtualizedDatabase(db, resource_sql_map={"speed": "SELECT id, timestamp, value FROM speed"}),
                     resources={"speed": m.get_templates()[0]})

# Query the graph, the inferred types and the time series in one SPARQL query; returns a Polars DataFrame
print(m.query("""
PREFIX ex: <http://example.org/>
PREFIX ct: <https://github.com/DataTreehouse/chrontext#>
SELECT ?pump (AVG(?speed) AS ?avg_speed) WHERE {
    ?pump a ex:Pump ; ct:hasTimeseries ?ts .                # ex:Pump comes from inference
    ?ts ct:hasDataPoint ?dp . ?dp ct:hasValue ?speed .      # the data points come from DuckDB
} GROUP BY ?pump"""))
```

Loading, inference and querying run on the open-source build; validation and virtualization need [Enterprise Edition](#open-source-and-enterprise-edition). To build a graph from DataFrames instead, use templates with `m.map(...)` (see [Build the graph](#build-the-graph) and [API_USAGE.md](API_USAGE.md)).

## Core concepts

- **`Model`** is the knowledge graph. It holds the triples (in a default graph and any number of named graphs), the templates and the prefixes, and every operation is a method on it.
- **Templates** are reusable patterns of triples, written in [OTTR](https://ottr.xyz/) (as text, or built in Python with `Template`, `Parameter`, `Triple` and friends). Mapping a template with a DataFrame fills each parameter from the column of the same name, for all rows at once.
- **DataFrames in, DataFrames out.** IRIs appear as `"<...>"` strings and literals as native Polars types.
- **Named graphs** keep data, ontologies, shapes and inferences apart. Triples in the reserved graph `maplib.TRANSIENT_GRAPH` take part in queries, reasoning and validation, but are not written out unless you ask for them.

## What you can do

Features marked **EE** need [maplib Enterprise Edition](#open-source-and-enterprise-edition). For worked examples of every method, see **[API_USAGE.md](API_USAGE.md)**. Each method's docstring documents its parameters (`help(Model.map)`), and the **[documentation site](https://datatreehouse.github.io/maplib/)** has the guides and the [API reference](https://datatreehouse.github.io/maplib/api/maplib/index.html) together, with search.

### Build the graph

| Method | What it does |
|---|---|
| [`m.map(template, data)`](API_USAGE.md#map) | Instantiates an OTTR template once per row of a DataFrame, LazyFrame or iterable of batches, so data larger than memory can be mapped. With `stream=True` it returns a lazy `ModelStream` instead (see below). |
| [`m.map_triples(data, predicate=None)`](API_USAGE.md#map_triples) | Adds triples straight from a DataFrame with `subject` and `object` columns (and optionally `predicate`). |
| [`m.map_default(data, primary_key_column)`](API_USAGE.md#map_default-and-generate_template) | Generates a default template from the columns, with the key column as the subject of one triple per other column, adds it to the model and maps the data with it. |
| [`m.map_df(df)`](API_USAGE.md#façade-x-map_df-map_json-and-map_xml) / [`m.map_json(...)`](API_USAGE.md#façade-x-map_df-map_json-and-map_xml) / [`m.map_xml(...)`](API_USAGE.md#façade-x-map_df-map_json-and-map_xml) | Maps a DataFrame, JSON or XML document to triples with a generic Façade-X mapping that mirrors its structure. JSON and XML also accept an iterable of documents, such as the lines of a JSON Lines file. |
| [`m.map_csvw(source, metadata=None)`](API_USAGE.md#csv-on-the-web-map_csvw) | Converts CSV and TSV files to RDF as specified by the W3C CSV on the Web recommendations, and returns the generated SPARQL Update so you can adapt it. |
| [`m.map_r2rml(mapping, database=db)`](API_USAGE.md#relational-databases-map_r2rml) | Maps relational data to RDF with a W3C R2RML mapping, given as Turtle or stored in a graph of the model (`mapping_graph`). The tables are queried through a Python database object, a DuckDB connection or given as DataFrames. |
| [`m.map_rml(mapping, sources=...)`](API_USAGE.md#csv-json-and-xml-with-rml-map_rml) | Maps CSV, JSON and XML sources to RDF with an RML mapping. JSONPath and XPath iterators and references are evaluated as SPARQL over a temporary Façade-X graph of the source. |
| [`m.map_opc_ua(folder)`](API_USAGE.md#opc-ua-nodesets-ee) **EE** | Maps a folder of OPC UA NodeSet2 XML files to RDF. `m.opcua.add_typing_shapes()` then generates SHACL shapes from their type definitions. |
| [`m.add_template(t)`](API_USAGE.md#stottr-syntax), [`m.read_template(path)`](API_USAGE.md#stottr-syntax) | Add templates from stOTTR text, a `Template` object or a file. |
| [Function calls in templates](API_USAGE.md#function-calls-in-templates) | Template arguments computed with SPARQL functions, e.g. `ottr:Triple((IRI (CONCAT "http://ex.org/" ?id)), ex:powerW, (* ?kw 1000))`, and functions defined with `def` ([API_USAGE.md](API_USAGE.md#function-calls-in-templates)). |
| [`m.get_templates()`](API_USAGE.md#inspecting-templates), [`m.templates_to_graph(graph)`](API_USAGE.md#inspecting-templates) | List the model's templates, or write them as RDF so you can query their structure with SPARQL. |
| [`maplib.generate_templates(m, graph)`](API_USAGE.md#templates-from-an-ontology-generate_templates) | Generates one template per class of an ontology from its `rdfs:subClassOf`, `rdfs:domain` and `rdfs:range`. |
| [`m.add_prefixes({...})`](API_USAGE.md#prefixes) | Registers prefixes for use in SPARQL, Datalog and OTTR. |

**Streaming.** [`m.map(..., stream=True)`](API_USAGE.md#5-streaming-with-modelstream) (also `map_triples`, `map_default`, `map_json` and `map_xml`) returns a `ModelStream`: a lazy pipeline of `.update()`, `.insert()`, `.infer()` **EE** and `.write()` steps. `.collect()` then runs the pipeline batch by batch in a fresh model, so arbitrarily large inputs go to file with bounded memory.

### Read and write RDF

| Method | What it does |
|---|---|
| [`m.read(source, format=None, graph=None)`](API_USAGE.md#formats) | Reads Turtle, N-Triples, N-Quads, TriG, RDF/XML, JSON-LD, HDT or CIM/XML **EE** from a path, an object storage URL, bytes or a file-like object. N-Triples, Turtle and RDF/XML are parsed in parallel. |
| [`m.reads(string, format)`](API_USAGE.md#formats) | Reads triples from a string. |
| [`m.write(target, format, graph=None)`](API_USAGE.md#formats) | Writes Turtle (pretty-printed), N-Triples, N-Quads, TriG, RDF/XML, JSON-LD or HDT to a path, an object storage URL or a binary file-like object. For dataset formats, `graph` can be a list of graphs. |
| [`m.writes(format)`](API_USAGE.md#bytes-and-file-like-objects), [`m.write_bytes(format)`](API_USAGE.md#bytes-and-file-like-objects) | Write to a string, or to bytes (any format, including HDT). |
| [`m.write_cim_xml(path, profile_graph)`](API_USAGE.md#cimxml-ee) **EE** | Writes IEC 61970-552 / CGMES-compliant CIM/XML, driven by the profile ontologies. |
| [`m.cim`](API_USAGE.md#cutting-a-part-of-a-cim-grid-ee) **EE** | The CIM grid tools bound to the model: cutting out part of a grid (`m.cim.cut_to_graph`), serves relations, and [time series of measurements and meter readings, and events such as earth faults and meter events](API_USAGE.md#cim-time-series-mcim-ee), virtualized from a database. |
| [`m.serialize(path)`](API_USAGE.md#serialize-and-deserialize), [`Model.deserialize(path)`](API_USAGE.md#serialize-and-deserialize) | Save and restore a whole model in maplib's own Parquet-based format, by default as one `.maplib` file, which is much faster to reload than RDF text. `raw_db=True` **EE** keeps DuckDB dictionaries for fast disk-mode loads. |

**[Object storage](API_USAGE.md#object-storage).** `read`, `read_template`, `write`, `write_cim_xml`, `serialize` and `deserialize` also take `s3://`, `gs://`, `az://`/`abfss://`, `https://` and `file://` URLs, with credentials from the environment or `storage_options`.

### Query and update with SPARQL

| Method | What it does |
|---|---|
| [`m.query(sparql)`](API_USAGE.md#8-querying) | Runs SPARQL 1.1 `SELECT` (returns a DataFrame), `CONSTRUCT` and `DESCRIBE` (a list of DataFrames of triples) or `ASK` (a bool). Options include `bindings=` for parameterised queries, `return_format="json"` or `"turtle"`, `graph=` and `include_graphs=`, and `endpoint=` to send the query to a remote SPARQL endpoint. |
| [`m.insert(construct)`](API_USAGE.md#insert) | Runs a `CONSTRUCT` and adds the resulting triples, returning only the new ones. Use `query` first to inspect the triples, then `insert` the same string to commit them. |
| [`m.update(sparql_update)`](API_USAGE.md#update) | Runs SPARQL 1.1 Update (`INSERT`/`DELETE … WHERE`, `CLEAR`, `DROP`, `LOAD`, …). |
| [`m.query_external(sparql, endpoint)`](API_USAGE.md#other-endpoints-endpoint-and-query_external) | Queries another SPARQL endpoint and returns a DataFrame. |
| [`m.set_federation(allowed_endpoints=[...])`](API_USAGE.md#federation-service-and-load) | Enables SPARQL 1.1 Federated Query (`SERVICE`) and `LOAD` for the listed endpoints, using bind joins so remote endpoints only return matching rows. |
| [`m.add_udf(iri, func, output_type)`](API_USAGE.md#add_udf-and-list_udfs), [`m.list_udfs()`](API_USAGE.md#add_udf-and-list_udfs) | Register a Polars function that SPARQL can call, and list the registered ones. |
| [`m.set_query_cache(...)`](API_USAGE.md#the-query-cache), [`get_query_cache()`](API_USAGE.md#the-query-cache), [`query_cache_stats()`](API_USAGE.md#the-query-cache), [`clear_query_cache()`](API_USAGE.md#the-query-cache) | Cache query results, invalidating each one only when a write touches the graphs and predicates the query reads. |

Beyond standard SPARQL 1.1, maplib supports:
- length-constrained paths ([`ex:p{2,4}`](API_USAGE.md#bounded-paths-nm));
- the magic predicates [`maplib:path`](API_USAGE.md#maplibpath) (hop counts and path enumeration), [`maplib:shortestPath`](API_USAGE.md#maplibshortestpath) (shortest routes with costs and nodes to avoid), and [`maplib:component`, `maplib:bridge` and `maplib:articulationPoint`](API_USAGE.md#graph-algorithms-maplibcomponent-maplibbridge-maplibarticulationpoint) (islands, single points of failure and what-ifs, over stored or derived edges);
- arithmetic on durations, dates and times;
- full-text search with [`maplib:fts`](API_USAGE.md#full-text-search-maplibfts-ee) **EE**;
- **[GeoSPARQL](API_USAGE.md#geosparql-ee)** **EE**. Enable it with `IndexingOptions(geo=True)`; `m.read(..., format="cim/xml", geo=True)` also turns CIM position points into geometries. WKT, WKB and GeoJSON geometry literals are then indexed spatially, and the GeoSPARQL functions work across coordinate reference systems: simple features, Egenhofer and RCC8 relations, distances and areas with units of measure, and constructions such as buffers and intersections. Spatial filters and spatial joins use the index.

Ctrl-C stops a running query, and syntax errors show the offending source with a hint.

### Reason and validate

| Method | What it does |
|---|---|
| [`m.infer_rdfs()`](API_USAGE.md#infer_rdfs) | Applies RDFS entailment (subclass, subproperty, domain and range) and puts the inferred triples in the transient graph. |
| [`m.infer(rules)`](API_USAGE.md#datalog-infer-ee) **EE** | Runs Datalog rules to a fixpoint. |
| [`m.infer(profile=...)`](API_USAGE.md#reasoning-profiles-inferprofile-ee) **EE** | Materializes a built-in reasoning profile with the Datalog engine: `"owl-rl"` (OWL 2 RL), `"rdfs-plus"` or `"rdfs"` (all RDFS entailment rules). Puts the inferred triples in the transient graph and returns a `ReasoningResult` with the count and the consistency violations. |
| [`m.validate(shape_graph=...)`](API_USAGE.md#shacl-validate-ee) **EE** | Validates the graph with SHACL Core and SHACL-SPARQL, evaluating shapes in parallel, and runs SHACL rules (`sh:rule`). Returns a `ValidationReport` with `.conforms`, `.results()` and `.details()` as DataFrames, and can also write the report as RDF. |

### Virtualize time series **EE**

Leave large time series in the databases that hold them, and query them in SPARQL together with the graph.

| Method | What it does |
|---|---|
| [`m.add_virtualization(db, resources)`](API_USAGE.md#virtualizeddatabase-and-add_virtualization) | Adds a virtualized database, with a template per resource. You can add several databases and query them together. Filters, grouping and ordering are pushed down to the database. |
| [`maplib.virtualization.*`](API_USAGE.md#maplibvirtualization) | Connectors for DuckDB, Snowflake, BigQuery, Databricks, TimescaleDB, InfluxDB 1.8/2/3, Microsoft Fabric (Lakehouse and Warehouse), Azure Data Explorer / Kusto, plus `maplib.opcua.OPCUAHistoryDatabase` for OPC UA Historical Access. |
| [`m.map_cdf(client)`](API_USAGE.md#industrial-sources-map_cdf-map_osipi-map_pi_points-map_insights_hub-and-maplibip21) | Maps a Cognite Data Fusion data model to RDF and virtualizes its time series. |
| [`m.map_osipi(url, ...)`](API_USAGE.md#industrial-sources-map_cdf-map_osipi-map_pi_points-map_insights_hub-and-maplibip21), [`m.map_pi_points(url, ...)`](API_USAGE.md#industrial-sources-map_cdf-map_osipi-map_pi_points-map_insights_hub-and-maplibip21) | Read an OSIsoft/AVEVA PI Asset Framework model or the PI Points of a PI Data Archive, and virtualize their recorded values. |
| [`m.map_insights_hub(host, tenant, ...)`](API_USAGE.md#industrial-sources-map_cdf-map_osipi-map_pi_points-map_insights_hub-and-maplibip21) | Reads a Siemens Insights Hub asset model and virtualizes its time series. |
| [`maplib.ip21.map_ip21(m, url, data_source)`](API_USAGE.md#industrial-sources-map_cdf-map_osipi-map_pi_points-map_insights_hub-and-maplibip21) | Adds the tags of an AspenTech InfoPlus.21 historian and virtualizes their values. |
| [`maplib.config`](API_USAGE.md#maplibconfig) | Stores a model's virtualizations, indexes, query cache settings and saved questions as triples in its own config graph, and applies them with `apply_config(m)`. Secrets are read from environment variables. |

### Serve and explore **EE**

| Method | What it does |
|---|---|
| [`m.serve(port=...)`](API_USAGE.md#serve-ee) | Serves the live model over the SPARQL 1.1 Protocol and Arrow Flight in the background. Includes optional updates, auth, CORS, a query log and static web apps, and returns a `Server`. |
| [`m.explore()`](API_USAGE.md#explore-ee) | Opens the Model portal, a browser UI for browsing, searching and querying the model, and returns an `ExploreServer` with `.url`. |

### Manage the model

| Method | What it does |
|---|---|
| [`Model(storage=..., storage_folder=...)`](API_USAGE.md#storage-modes) | Chooses `"memory"` (the default) or, with **EE**, `"hybrid"` (dictionaries on disk) or `"disk"` (triple tables on disk as Parquet too) for graphs larger than RAM. |
| [`m.size(graph)`](API_USAGE.md#introspection), [`m.get_predicate_iris()`](API_USAGE.md#introspection), [`m.get_predicate(iri)`](API_USAGE.md#introspection) | Count triples, list predicates, or get the stored tables of a predicate as DataFrames. |
| [`m.add_graph(other, ...)`](API_USAGE.md#copying-detaching-and-emptying-graphs), [`maplib.add_triples(...)`](API_USAGE.md#copying-triples-between-models-add_triples) | Copy a graph from another model into this one. |
| [`m.detach_graph(graph)`](API_USAGE.md#copying-detaching-and-emptying-graphs), [`m.truncate_graph(graph)`](API_USAGE.md#copying-detaching-and-emptying-graphs) | Split a named graph off into its own model, or empty it. |
| [`m.create_index(IndexingOptions(...))`](API_USAGE.md#indexes-and-compaction), [`m.compact()`](API_USAGE.md#indexes-and-compaction) | Build object, full-text and geo indexes, and merge stored segments. |
| [`maplib.set_memory_limit(...)`](API_USAGE.md#memory-limit), [`memory_usage()`](API_USAGE.md#memory-limit), [`memory_limit()`](API_USAGE.md#memory-limit) | Bound the memory queries may use (80% of system memory by default). Over the limit, joins, sorts and aggregations spill to disk. |

A `Model` can be shared between threads: queries and writes to file run concurrently, while changes take an exclusive lock.

## Open source and Enterprise Edition

The maplib core (everything above that isn't marked **EE**) is open source under the Apache 2.0 license and is what `pip install maplib` installs. **maplib Enterprise Edition** from [Data Treehouse](https://www.data-treehouse.com/) adds SHACL, Datalog, disk-backed storage, full-text search, GeoSPARQL, CIM/CGMES, OPC UA, time series virtualization, the SPARQL server and the Model portal. To try it, email **magnus at data-treehouse.com** or find **Data Treehouse** on LinkedIn.

## Learn more

- [Documentation site](https://datatreehouse.github.io/maplib/): the guides and the API reference, searchable
- [API usage guide with examples](API_USAGE.md)
- [API reference](https://datatreehouse.github.io/maplib/api/maplib/index.html), generated from the type stubs and docstrings
- [Changelog](CHANGES.md)
- [GitHub issues](https://github.com/DataTreehouse/maplib/issues)

To cite maplib: M. Bakken, "maplib: Interactive, Literal RDF Model Mapping for Industry," *IEEE Access*, vol. 11, 2023, doi: [10.1109/ACCESS.2023.3269093](https://ieeexplore.ieee.org/document/10106242).

## Contributing and license

Contributions are welcome: open an issue to discuss larger changes. All contributions fall under the project's [Apache 2.0 license](LICENSE), and the [Code of Conduct](CODE_OF_CONDUCT.md) applies. Third-party licenses are in [licensing/](licensing/).

Code produced since 1 August 2023 is copyright [Data Treehouse AS](https://www.data-treehouse.com/). Earlier code is copyright [Prediktor AS](https://www.prediktor.com/), was financed by [The Research Council of Norway](https://www.forskningsradet.no/en/) (grant no. 316656) and Prediktor AS as part of a PhD degree, and is archived at [github.com/magbak/maplib](https://github.com/magbak/maplib). Both are licensed under Apache 2.0 unless otherwise noted.

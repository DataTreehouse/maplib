# Change log

## Unreleased
### Breaking changes
- Transient triples are removed. Triples that should be available for querying, validation and reasoning, but not written, now go in a reserved named graph, `maplib.TRANSIENT_GRAPH` (`https://datatreehouse.github.io/maplib/vocab#TransientGraph`). The `transient` parameter of `map_json`, `map_xml`, `read`, `reads` and `insert` is removed: pass `graph=TRANSIENT_GRAPH` (or `target_graph=TRANSIENT_GRAPH` for `insert`) instead
- `include_transient` is replaced by `include_graphs: Optional[List[str]]` in `query`, `update`, `insert`, `infer`, `get_predicate`, `get_predicate_iris` and the corresponding `ModelStream` steps. The included graphs are merged into the queried graph, as with FROM. For `query`, `update`, `insert` and `infer`, `None` includes the transient graph (as `include_transient=True` did) and `[]` includes nothing; for `get_predicate` and `get_predicate_iris`, `None` includes nothing. `GRAPH ?g` ranges over the transient graph as over any other named graph, and `GRAPH <g>` no longer sees transient triples added to `g`
- RDFS inference (`infer_rdfs`) and SHACL rules (without an `inferences_graph` equal to the data graph) put their inferred triples in the transient graph. Writing all graphs as TriG or N-Quads leaves out the transient graph; write it with `graph=TRANSIENT_GRAPH`
- In Rust, `QuerySettings::include_transient` is replaced by `include_graphs: Vec<NamedGraph>`, and the `transient` arguments of the triplestore and model methods are removed. `NamedGraph::transient()` gives the transient graph
### Features
- Streaming ([#87](https://github.com/DataTreehouse/maplib/issues/87)): `Model.map`, `map_triples`, `map_default`, `map_json` and `map_xml` take `stream=True` and then return a lazy `ModelStream`, e.g. `m.map(template, lf, stream=True).update(...).infer(...).insert(...).write("out.nt").write("out.ttl", format="turtle").collect()`. On `collect()`, each batch of the input is mapped into a fresh in-memory model with the templates, prefixes and UDFs of the model, the steps run on it in order, the `write` steps append it to their files (N-Triples, Turtle with prefixes declared once, or N-Quads), and the batch is dropped, so memory use depends on the batch size and not on the size of the input. The model's own triples are not changed. Steps only see the triples of the current batch. Blank node labels stay unique across batches
- `Model.map_json` and `Model.map_xml` also take an iterable of documents (strings or paths), e.g. a generator over the lines of a JSON Lines file, mapped one document at a time. With a `uuid_namespace`, document number i uses the namespace `<uuid_namespace>/<i>` so that documents do not get the same IRIs. With `stream=True`, `documents_per_batch` documents are mapped into each batch; mapping them into `graph=TRANSIENT_GRAPH` and inserting a CONSTRUCT query writes only the constructed triples
- Storage modes: `Model(storage="memory" | "hybrid" | "disk", storage_folder=...)`, also for `Model.deserialize`. `"memory"` keeps everything in memory, `"hybrid"` stores the dictionaries of IRIs and literals on disk (what `storage_folder` did before), and the new `"disk"` mode also stores the triple tables on disk as parquet files in `<storage_folder>/maplib_storage/triples`. Files of triple tables that are replaced (by compaction or deletes) or whose model is dropped are removed. Without `storage`, a `storage_folder` still means hybrid storage, so existing code is unaffected. Hybrid and disk storage require maplib Enterprise Edition. In Rust, `Model::new`, `Model::deserialize_triples` and `Triplestore::new` take a `StorageOptions` instead of the storage folder
- Full text search (maplib Enterprise Edition) uses DuckDB instead of tantivy. The index stores the dictionary codes of subjects, predicates and objects, and searches the strings where the dictionaries keep them: with hybrid and disk storage it joins against the on-disk dictionary tables, so no copy of the strings is made, and with memory storage it uses an in-memory DuckDB database. Matching is unchanged: case insensitive, 1.0 for a match of the whole value and 0.75 for a substring match. `fts_path` (in `IndexingOptions` and `Model.explore`) is no longer used, as the index is stored with the dictionaries
- GeoSPARQL (maplib Enterprise Edition): enable it with `IndexingOptions(geo=True)`. Geometry literals (`geo:wktLiteral`, `geo:wkbLiteral`, `geo:geoJSONLiteral`) are indexed spatially, and the GeoSPARQL functions are supported: the simple features, Egenhofer and RCC8 relations, measurements with units of measure, constructions, properties and serializations, across coordinate reference systems. Spatial filters and spatial joins use the index. `geof:relate`, the query rewrite extension and the aggregate functions are not supported yet
- Pretty Turtle output only declares the prefixes whose namespace starts a predicate, a datatype or an IRI in the model, sorted by name, instead of all known prefixes. Each prefix is checked with one lookup in the sorted IRI dictionary. The dictionary is shared by all graphs, so a prefix used only in another graph is still declared; with hybrid and disk storage all prefixes are declared as before
- `Model.serialize` writes the dictionaries of IRIs and literals as parquet files (`cats/<id>.parquet`, columns `str` and `cat`, in string order) instead of DuckDB databases, so a serialized model is parquet throughout and serialization no longer needs maplib Enterprise Edition. `Model.serialize(path, raw_db=True)` (Enterprise Edition) writes them as DuckDB databases as before: larger and slower to write, but deserializing into hybrid or disk storage uses them in place. `Model.deserialize` reads both, also folders written by earlier versions. In Rust, `serialize_triples` takes `raw_db: bool`

### Performance
In disk mode the triple tables and their deduplication index are no longer held in memory. With few triples per distinct IRI or literal (as in the CIM data), memory use is dominated by the dictionary and reading, and disk storage saves little.

Triple table segments are compacted much faster, which matters most for datalog, where every iteration adds a small batch of triples to tables that are often large:
- Size-tiered compaction: only the trailing segments of comparable size are merged with the new triples, so a large table is no longer rewritten every 11 inserts. A table has O(log n) segments (at most 16).
- Merging sorts packed rank keys with Polars instead of a value-by-value heap merge, and builds the object sort from the merged triples instead of also reading and merging the object sorts of the segments.
- Disk mode: new triples are deduplicated against only the row groups that may hold their subjects, found through the sparse subject index, instead of reloading every stored triple of the table into an index on each insert. Small trailing segments (under 16k triples) stay in memory until merged, so small inserts don't write and rewrite small parquet files.

Query results are collected with the Polars streaming engine, which the rest of maplib already used; until now the final step of a query ran on the in-memory engine. Whole-table scans and counts, large result sets and DISTINCT are faster, while small queries are about as fast as before. `MAPLIB_POLARS_ENGINE=in-memory` switches back to the in-memory engine.

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

### Bug fixes
- Ctrl-C now stops a running query. Before, a long query could keep running until it finished
- Hybrid storage: `Model.compact()` (also run by `Model.serialize()`) failed with a DuckDB "Duplicate key" error once the dictionary had to be renumbered, e.g. after more than ten inserts into the same predicate
- SHACL rules: a rule that re-asserts a triple already in the data graph no longer makes inference loop until `max_iterations` (or forever with one rule). Inserting transient triples that all already existed non-transiently dropped the earlier transient triples with the same predicate, which were then inferred again on every iteration ([#76](https://github.com/DataTreehouse/maplib/issues/76))
- SHACL rules: rules whose WHERE clause has no triple pattern on `$this` (only `OPTIONAL` / `BIND`) now also run for focus nodes that become targets of their shape after the rule first ran ([#77](https://github.com/DataTreehouse/maplib/issues/77))
- Full text search: with a fixed subject (`ex:a maplib:fts ?fts`), the limit was applied before filtering on the subject, so few or no results were returned. Triples deleted with SPARQL Update were still found, now they are no longer found after `Model.compact()`. An on-disk index no longer leaves a folder behind for every index created

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

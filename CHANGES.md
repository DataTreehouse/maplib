# Change log

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

# Change log

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

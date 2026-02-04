# Change log

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

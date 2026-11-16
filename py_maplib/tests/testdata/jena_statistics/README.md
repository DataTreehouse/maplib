Tests for the statistical aggregates (`agg:stdev`, `agg:stdev_samp`, `agg:stdev_pop`,
`agg:variance`, `agg:var_samp`, `agg:var_pop`), from Apache Jena ARQ.

Copyright The Apache Software Foundation. Licensed under the Apache License, Version 2.0, see
LICENSE-APACHE.

`TestStatisticsAggregates.java` is an unchanged copy of
https://github.com/apache/jena/blob/main/jena-arq/src/test/java/org/apache/jena/sparql/expr/TestStatisticsAggregates.java
(Jena main at commit 30a6ac11, last changed in e84c6e52). Jena has no data-driven tests for these
aggregates in its testing/ tree, only this JUnit class. test_statistical_aggregates.py ports it
to Python with the same datasets, queries and expected values (with Jena's tolerance of 0.001),
and checks that the ported tests cover every test method in the Java file.

Differences:

* The `*_kw` and `*_agg` tests use Jena's ARQ syntax extensions (`STDEV(?x)`, `AGG agg:stdev(?x)`),
  which are not SPARQL 1.1 and which maplib does not parse. Only the SPARQL 1.1 forms
  (`agg:stdev(?x)` and the full IRI) are ported.
* `agg_stat_registry` checks Jena's Java aggregate registry; the port checks instead that each of
  the six IRIs, in both the `agg:` and the `afn:` namespace, parses and evaluates as an aggregate,
  and that `afn:e` (not an aggregate) does not.

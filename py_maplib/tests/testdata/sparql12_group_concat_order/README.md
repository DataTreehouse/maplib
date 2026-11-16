# SPARQL 1.2: Possible updates to SPARQL Aggregates

## Ordered `GROUP_CONCAT`

```sparql
SELECT (GROUP_CONCAT(?name; ORDER BY ?number) AS ?names)
WHERE {
	VALUES (?name ?number) {
		("four" 4)
		("one" 1)
		("three" 3)
		("two" 2)
	}
}
```

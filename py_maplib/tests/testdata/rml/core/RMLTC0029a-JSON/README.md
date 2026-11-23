## RMLTC0029a-JSON

**Title**: "Generating of triples with constant shortcut subject"

**Description**: "Test triples with a constant shortcut subject from the data"

**Default Base IRI**: http://example.com/

**Error expected?** No

**Input**
```
[
	{ "FOO": "one", "BAR": "string"},
	{ "FOO": "two", "BAR": "int"}
]

```

**Mapping**
```
@prefix ex: <http://example.com/> .
@prefix rml: <http://w3id.org/rml/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource [ a rml:LogicalSource;
      rml:referenceFormulation rml:JSONPath;
      rml:iterator "$[*]";
      rml:source [ a rml:RelativePathSource;
          rml:root rml:MappingDirectory;
          rml:path "data.json"
        ]
    ];
  rml:predicateObjectMap [
      rml:objectMap [
          rml:reference "$.FOO"
        ];
      rml:predicate ex:x
    ];
  rml:subject ex:example .
```

**Output**
```
<http://example.com/example> <http://example.com/x> "one" .
<http://example.com/example> <http://example.com/x> "two" .
```


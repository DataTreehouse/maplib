## RMLTC0025c-JSON

**Title**: "Generation of triples from arrays in subject and object"

**Description**: "Tests the generation of triples from array input data structures in subject and object"

**Default Base IRI**: http://example.com/

**Error expected?** No

**Input**
```
{
  "persons": [
    {"names":["Bob", "Smith"],"amounts":[30, 40, 50]}
  ]
}

```

**Mapping**
```
@prefix ex: <http://example.com/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix rml: <http://w3id.org/rml/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource [ a rml:LogicalSource;
      rml:iterator "$.persons[*]";
      rml:referenceFormulation rml:JSONPath;
      rml:source [ a rml:RelativePathSource;
          rml:root rml:MappingDirectory;
          rml:path "persons.json"
        ]
    ];
  rml:predicateObjectMap [
      rml:objectMap [
          rml:reference "$.amounts[*]"
        ];
      rml:predicate ex:amount
    ];
  rml:subjectMap [
      rml:template "http://example.com/Student/{$.names[*]}"
    ] .

```

**Output**
```
<http://example.com/Student/Smith> <http://example.com/amount> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.com/Student/Smith> <http://example.com/amount> "40"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.com/Student/Smith> <http://example.com/amount> "50"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.com/Student/Bob> <http://example.com/amount> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.com/Student/Bob> <http://example.com/amount> "40"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.com/Student/Bob> <http://example.com/amount> "50"^^<http://www.w3.org/2001/XMLSchema#integer> .

```


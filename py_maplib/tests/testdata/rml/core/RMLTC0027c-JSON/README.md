## RMLTC0027c-JSON

**Title**: "Generation an IRI using the term type IRI"

**Description**: "Tests the generation of triples with a IRI term type in the subject"

**Default Base IRI**: http://example.com/

**Error expected?** No

**Input**
```
{
  "students": [
    {"Name": "Zoë Krüger"}
  ]
}

```

**Mapping**
```
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rml: <http://w3id.org/rml/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource [ a rml:LogicalSource;
      rml:iterator "$.students[*]";
      rml:referenceFormulation rml:JSONPath;
      rml:source [ a rml:RelativePathSource;
          rml:root rml:MappingDirectory;
          rml:path "student.json"
        ]
    ];
  rml:predicateObjectMap [
      rml:object foaf:Person;
      rml:predicate rdf:type
    ];
  rml:subjectMap [
      rml:template "http://example.com/Person/{$.Name}";
      rml:termType rml:IRI
    ] .

```

**Output**
```
<http://example.com/Person/Zoë%20Krüger> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .


```


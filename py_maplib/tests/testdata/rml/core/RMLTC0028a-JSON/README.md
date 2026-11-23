## RMLTC0028a-JSON

**Title**: "Generation of the right datatype for a constant in the mapping"

**Description**: "Test the honoring of the datatype specified by the constant term in the mapping"

**Default Base IRI**: http://example.com/

**Error expected?** No

**Input**
```
[ { "id": "0", "foo": "bar"  } ] 

```

**Mapping**
```
@prefix rml: <http://w3id.org/rml/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
    rml:logicalSource [
      rml:referenceFormulation rml:JSONPath ;
      rml:iterator "$[*]";
      rml:source [ a rml:RelativePathSource;
        rml:root rml:MappingDirectory;
        rml:path "data.json"
      ]
    ];
    rml:subjectMap [
      rml:template "https://example.org/instances/{$.id}";
    ];
    rml:predicateObjectMap [
      rml:predicate <http://example.org/ns/p> ;
      rml:object true ; # datatype is boolean
    ] .

```

**Output**
```
<https://example.org/instances/0> <http://example.org/ns/p> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .

```


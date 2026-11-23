## RMLTC0031a-JSON

**Title**: "Generating of triples with a constant-valued language map"

**Description**: "Test triples with a constant-valued language map"

**Default Base IRI**: http://example.com/

**Error expected?** No

**Input**
```
[
	{"ID": 10,"label": "apple"},
	{ "ID": 20,"label": "pear"}
]

```

**Mapping**
```
@prefix ex: <http://example.com/> .
@prefix rml: <http://w3id.org/rml/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

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
          rml:languageMap [
              rml:constant "en"
            ];
          rml:reference "$.label"
        ];
      rml:predicate rdfs:label
    ];
  rml:subjectMap [
      rml:template "http://example.com/{$.ID}"
    ] .
```

**Output**
```
<http://example.com/10> <http://www.w3.org/2000/01/rdf-schema#label> "apple"@en .
<http://example.com/20> <http://www.w3.org/2000/01/rdf-schema#label> "pear"@en .
```


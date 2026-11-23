## RMLTC0031c-JSON

**Title**: "Generating of triples with a template-valued language map"

**Description**: "Test triples with a template-valued language map"

**Default Base IRI**: http://example.com/

**Error expected?** No

**Input**
```
[
    {
        "ID": 10,
        "label": "aubergine",
        "language": "en", 
        "region":"GB"
    },
    {
        "ID": 10,
        "label": "eggplant",
        "language": "en",
        "region": "US"
    }
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
              rml:template "{$.language}-{$.region}"
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
<http://example.com/10> <http://www.w3.org/2000/01/rdf-schema#label> "aubergine"@en-GB .
<http://example.com/10> <http://www.w3.org/2000/01/rdf-schema#label> "eggplant"@en-US .
```


## RMLTC0023f-JSON

**Title**: "Valid IRI template with backslash-escape"

**Description**: "Test handling of a valid IRI template using backslash-escape"

**Default Base IRI**: http://example.com/

**Error expected?** No

**Input**
```
{
  "students": [{
    "ID": 10,
    "{Name}":"Venus"
  }]
}

```

**Mapping**
```
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix rml: <http://w3id.org/rml/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource [ a rml:LogicalSource;
     rml:referenceFormulation rml:JSONPath;
     rml:iterator "$.students[*]";
      rml:source [ a rml:RelativePathSource;
          rml:root rml:MappingDirectory;
          rml:path "student.json"
        ]
    ];
  rml:subjectMap [
      rml:template "http://example.com/{$['\\{Name\\}']}";
      rml:class foaf:Person;
    ] .


```

**Output**
```
<http://example.com/Venus> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
```


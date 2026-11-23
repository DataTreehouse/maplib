## RMLTC0012e-JSON

**Title**: "Blank node as subject"

**Description**: "Test a subjectMap with termType BlankNode without expressionMap"

**Default Base IRI**: http://example.com/

**Error expected?** No

**Mapping**
```
@prefix rml: <http://w3id.org/rml/> .
@prefix : <http://example.org/> .

:jsonSource a rml:LogicalSource ;
  rml:source [
    a rml:RelativePathSource , rml:Source ;
    rml:root rml:MappingDirectory ;
    rml:path "people.json" ;
  ] ;
  rml:referenceFormulation rml:JSONPath ;
  rml:iterator "$[*]" .

:triplesMapPerson a rml:TriplesMap ;
  rml:logicalSource :jsonSource ;
  rml:subjectMap [
    rml:termType  rml:BlankNode ;
  ] ;
  rml:predicateObjectMap [
    rml:predicate :firstName ;
    rml:objectMap [
      rml:reference "$.firstName" ;
    ] ;
  ];   
rml:predicateObjectMap [
    rml:predicate :lastName ;
    rml:objectMap [
      rml:reference "$.lastName" ;
   ] ;
 ] . 
```

**Output**
```
_:Blank1 <http://example.org/firstName> "Alice" .
_:Blank1 <http://example.org/lastName> "Smith" .
_:Blank2 <http://example.org/firstName> "Bob" .
_:Blank2 <http://example.org/lastName> "Jones" .
```


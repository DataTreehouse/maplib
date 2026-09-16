# RDF/XSD vocabulary IRI constants (maplib-snz, continued). Mirrors
# py_maplib's xsd/rdf/rdfs/owl vocab helpers (lib/templates/src/python/
# {xsd,rdf,rdfs,owl}.rs). Names match py_maplib's exactly (e.g.
# `xsd$dateTime`, `rdf$type`).
#
# Built lazily in zzz.R's .onLoad, not at top-level source time: each entry
# is an IRI(), which validates via a compiled Rust call (validate_iri(),
# terms.rs) -- and devtools::load_all()'s R-sourcing pass does not guarantee
# the package's native library is already registered at the point vocab.R
# itself is sourced (confirmed: calling IRI() here at top level fails with
# "object 'savvy_validate_iri__impl' not found" on a fresh load_all()). 
# .onLoad always runs after useDynLib() registration completes, so building
# these there is the standard, safe place for a package constant that needs
# compiled code to construct.

#' The xsd vocabulary: a named list of IRIs.
#'
#' @format A named list of IRI objects.
#' @export
xsd <- NULL

.build_xsd_vocab <- function() {
  list(
    `boolean` = IRI("http://www.w3.org/2001/XMLSchema#boolean"),
    `byte` = IRI("http://www.w3.org/2001/XMLSchema#byte"),
    `date` = IRI("http://www.w3.org/2001/XMLSchema#date"),
    `dateTime` = IRI("http://www.w3.org/2001/XMLSchema#dateTime"),
    `dateTimeStamp` = IRI("http://www.w3.org/2001/XMLSchema#dateTimeStamp"),
    `decimal` = IRI("http://www.w3.org/2001/XMLSchema#decimal"),
    `double` = IRI("http://www.w3.org/2001/XMLSchema#double"),
    `duration` = IRI("http://www.w3.org/2001/XMLSchema#duration"),
    `float` = IRI("http://www.w3.org/2001/XMLSchema#float"),
    `int_` = IRI("http://www.w3.org/2001/XMLSchema#int"),
    `integer` = IRI("http://www.w3.org/2001/XMLSchema#integer"),
    `language` = IRI("http://www.w3.org/2001/XMLSchema#language"),
    `long` = IRI("http://www.w3.org/2001/XMLSchema#long"),
    `short` = IRI("http://www.w3.org/2001/XMLSchema#short"),
    `string` = IRI("http://www.w3.org/2001/XMLSchema#string"),
    `anyURI` = IRI("http://www.w3.org/2001/XMLSchema#anyURI"),
    `dayTimeDuration` = IRI("http://www.w3.org/2001/XMLSchema#dayTimeDuration"),
    `base64Binary` = IRI("http://www.w3.org/2001/XMLSchema#base64Binary"),
    `gDay` = IRI("http://www.w3.org/2001/XMLSchema#gDay"),
    `gMonthDay` = IRI("http://www.w3.org/2001/XMLSchema#gMonthDay"),
    `gMonth` = IRI("http://www.w3.org/2001/XMLSchema#gMonth"),
    `gYear` = IRI("http://www.w3.org/2001/XMLSchema#gYear"),
    `gYearMonth` = IRI("http://www.w3.org/2001/XMLSchema#gYearMonth"),
    `hexBinary` = IRI("http://www.w3.org/2001/XMLSchema#hexBinary"),
    `Name` = IRI("http://www.w3.org/2001/XMLSchema#Name"),
    `NCName` = IRI("http://www.w3.org/2001/XMLSchema#NCName"),
    `NMTOKEN` = IRI("http://www.w3.org/2001/XMLSchema#NMTOKEN"),
    `negativeInteger` = IRI("http://www.w3.org/2001/XMLSchema#negativeInteger"),
    `nonNegativeInteger` = IRI("http://www.w3.org/2001/XMLSchema#nonNegativeInteger"),
    `nonPositiveInteger` = IRI("http://www.w3.org/2001/XMLSchema#nonPositiveInteger"),
    `normalizedString` = IRI("http://www.w3.org/2001/XMLSchema#normalizedString"),
    `token` = IRI("http://www.w3.org/2001/XMLSchema#token"),
    `unsignedByte` = IRI("http://www.w3.org/2001/XMLSchema#unsignedByte"),
    `unsignedInt` = IRI("http://www.w3.org/2001/XMLSchema#unsignedInt"),
    `unsignedLong` = IRI("http://www.w3.org/2001/XMLSchema#unsignedLong"),
    `unsignedShort` = IRI("http://www.w3.org/2001/XMLSchema#unsignedShort"),
    `yearMonthDuration` = IRI("http://www.w3.org/2001/XMLSchema#yearMonthDuration")
  )
}

#' The rdf vocabulary: a named list of IRIs.
#'
#' @format A named list of IRI objects.
#' @export
rdf <- NULL

.build_rdf_vocab <- function() {
  list(
    `type` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
    `Alt` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#Alt"),
    `Bag` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag"),
    `first` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#first"),
    `HTML` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#HTML"),
    `langString` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"),
    `List` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#List"),
    `nil` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil"),
    `object` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#object"),
    `predicate` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate"),
    `Property` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"),
    `rest` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest"),
    `Seq` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq"),
    `Statement` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement"),
    `subject` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#subject"),
    `value` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#value"),
    `XMLLiteral` = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral")
  )
}

#' The rdfs vocabulary: a named list of IRIs.
#'
#' @format A named list of IRI objects.
#' @export
rdfs <- NULL

.build_rdfs_vocab <- function() {
  list(
    `Class` = IRI("http://www.w3.org/2000/01/rdf-schema#Class"),
    `comment` = IRI("http://www.w3.org/2000/01/rdf-schema#comment"),
    `Container` = IRI("http://www.w3.org/2000/01/rdf-schema#Container"),
    `Datatype` = IRI("http://www.w3.org/2000/01/rdf-schema#Datatype"),
    `domain` = IRI("http://www.w3.org/2000/01/rdf-schema#domain"),
    `ContainerMembershipProperty` = IRI("http://www.w3.org/2000/01/rdf-schema#ContainerMembershipProperty"),
    `isDefinedBy` = IRI("http://www.w3.org/2000/01/rdf-schema#isDefinedBy"),
    `label` = IRI("http://www.w3.org/2000/01/rdf-schema#label"),
    `Literal` = IRI("http://www.w3.org/2000/01/rdf-schema#Literal"),
    `member` = IRI("http://www.w3.org/2000/01/rdf-schema#member"),
    `range` = IRI("http://www.w3.org/2000/01/rdf-schema#range"),
    `seeAlso` = IRI("http://www.w3.org/2000/01/rdf-schema#seeAlso"),
    `subClassOf` = IRI("http://www.w3.org/2000/01/rdf-schema#subClassOf"),
    `subPropertyOf` = IRI("http://www.w3.org/2000/01/rdf-schema#subPropertyOf"),
    `Resource` = IRI("http://www.w3.org/2000/01/rdf-schema#Resource")
  )
}

#' The owl vocabulary: a named list of IRIs.
#'
#' @format A named list of IRI objects.
#' @export
owl <- NULL

.build_owl_vocab <- function() {
  list(
    `allValuesFrom` = IRI("http://www.w3.org/2002/07/owl#allValuesFrom"),
    `annotatedProperty` = IRI("http://www.w3.org/2002/07/owl#annotatedProperty"),
    `annotatedSource` = IRI("http://www.w3.org/2002/07/owl#annotatedSource"),
    `annotatedTarget` = IRI("http://www.w3.org/2002/07/owl#annotatedTarget"),
    `assertionProperty` = IRI("http://www.w3.org/2002/07/owl#assertionProperty"),
    `cardinality` = IRI("http://www.w3.org/2002/07/owl#cardinality"),
    `complementOf` = IRI("http://www.w3.org/2002/07/owl#complementOf"),
    `datatypeComplementOf` = IRI("http://www.w3.org/2002/07/owl#datatypeComplementOf"),
    `differentFrom` = IRI("http://www.w3.org/2002/07/owl#differentFrom"),
    `disjointUnionOf` = IRI("http://www.w3.org/2002/07/owl#disjointUnionOf"),
    `disjointWith` = IRI("http://www.w3.org/2002/07/owl#disjointWith"),
    `distinctMembers` = IRI("http://www.w3.org/2002/07/owl#distinctMembers"),
    `equivalentClass` = IRI("http://www.w3.org/2002/07/owl#equivalentClass"),
    `equivalentProperty` = IRI("http://www.w3.org/2002/07/owl#equivalentProperty"),
    `hasKey` = IRI("http://www.w3.org/2002/07/owl#hasKey"),
    `hasSelf` = IRI("http://www.w3.org/2002/07/owl#hasSelf"),
    `hasValue` = IRI("http://www.w3.org/2002/07/owl#hasValue"),
    `intersectionOf` = IRI("http://www.w3.org/2002/07/owl#intersectionOf"),
    `inverseOf` = IRI("http://www.w3.org/2002/07/owl#inverseOf"),
    `maxCardinality` = IRI("http://www.w3.org/2002/07/owl#maxCardinality"),
    `maxQualifiedCardinality` = IRI("http://www.w3.org/2002/07/owl#maxQualifiedCardinality"),
    `members` = IRI("http://www.w3.org/2002/07/owl#members"),
    `minCardinality` = IRI("http://www.w3.org/2002/07/owl#minCardinality"),
    `minQualifiedCardinality` = IRI("http://www.w3.org/2002/07/owl#minQualifiedCardinality"),
    `onClass` = IRI("http://www.w3.org/2002/07/owl#onClass"),
    `onDataRange` = IRI("http://www.w3.org/2002/07/owl#onDataRange"),
    `onDatatype` = IRI("http://www.w3.org/2002/07/owl#onDatatype"),
    `onProperties` = IRI("http://www.w3.org/2002/07/owl#onProperties"),
    `onProperty` = IRI("http://www.w3.org/2002/07/owl#onProperty"),
    `oneOf` = IRI("http://www.w3.org/2002/07/owl#oneOf"),
    `propertyChainAxiom` = IRI("http://www.w3.org/2002/07/owl#propertyChainAxiom"),
    `propertyDisjointWith` = IRI("http://www.w3.org/2002/07/owl#propertyDisjointWith"),
    `qualifiedCardinality` = IRI("http://www.w3.org/2002/07/owl#qualifiedCardinality"),
    `sameAs` = IRI("http://www.w3.org/2002/07/owl#sameAs"),
    `someValuesFrom` = IRI("http://www.w3.org/2002/07/owl#someValuesFrom"),
    `sourceIndividual` = IRI("http://www.w3.org/2002/07/owl#sourceIndividual"),
    `targetIndividual` = IRI("http://www.w3.org/2002/07/owl#targetIndividual"),
    `targetValue` = IRI("http://www.w3.org/2002/07/owl#targetValue"),
    `unionOf` = IRI("http://www.w3.org/2002/07/owl#unionOf"),
    `withRestrictions` = IRI("http://www.w3.org/2002/07/owl#withRestrictions"),
    `AllDifferent` = IRI("http://www.w3.org/2002/07/owl#AllDifferent"),
    `AllDisjointClasses` = IRI("http://www.w3.org/2002/07/owl#AllDisjointClasses"),
    `AllDisjointProperties` = IRI("http://www.w3.org/2002/07/owl#AllDisjointProperties"),
    `Annotation` = IRI("http://www.w3.org/2002/07/owl#Annotation"),
    `AnnotationProperty` = IRI("http://www.w3.org/2002/07/owl#AnnotationProperty"),
    `Axiom` = IRI("http://www.w3.org/2002/07/owl#Axiom"),
    `Class` = IRI("http://www.w3.org/2002/07/owl#Class"),
    `DataRange` = IRI("http://www.w3.org/2002/07/owl#DataRange"),
    `DatatypeProperty` = IRI("http://www.w3.org/2002/07/owl#DatatypeProperty"),
    `DeprecatedClass` = IRI("http://www.w3.org/2002/07/owl#DeprecatedClass"),
    `DeprecatedProperty` = IRI("http://www.w3.org/2002/07/owl#DeprecatedProperty"),
    `FunctionalProperty` = IRI("http://www.w3.org/2002/07/owl#FunctionalProperty"),
    `InverseFunctionalProperty` = IRI("http://www.w3.org/2002/07/owl#InverseFunctionalProperty"),
    `IrreflexiveProperty` = IRI("http://www.w3.org/2002/07/owl#IrreflexiveProperty"),
    `NamedIndividual` = IRI("http://www.w3.org/2002/07/owl#NamedIndividual"),
    `NegativePropertyAssertion` = IRI("http://www.w3.org/2002/07/owl#NegativePropertyAssertion"),
    `ObjectProperty` = IRI("http://www.w3.org/2002/07/owl#ObjectProperty"),
    `Ontology` = IRI("http://www.w3.org/2002/07/owl#Ontology"),
    `OntologyProperty` = IRI("http://www.w3.org/2002/07/owl#OntologyProperty"),
    `ReflexiveProperty` = IRI("http://www.w3.org/2002/07/owl#ReflexiveProperty"),
    `Restriction` = IRI("http://www.w3.org/2002/07/owl#Restriction"),
    `SymmetricProperty` = IRI("http://www.w3.org/2002/07/owl#SymmetricProperty"),
    `TransitiveProperty` = IRI("http://www.w3.org/2002/07/owl#TransitiveProperty"),
    `backwardCompatibleWith` = IRI("http://www.w3.org/2002/07/owl#backwardCompatibleWith"),
    `deprecated` = IRI("http://www.w3.org/2002/07/owl#deprecated"),
    `incompatibleWith` = IRI("http://www.w3.org/2002/07/owl#incompatibleWith"),
    `priorVersion` = IRI("http://www.w3.org/2002/07/owl#priorVersion"),
    `versionInfo` = IRI("http://www.w3.org/2002/07/owl#versionInfo"),
    `Nothing` = IRI("http://www.w3.org/2002/07/owl#Nothing"),
    `Thing` = IRI("http://www.w3.org/2002/07/owl#Thing"),
    `bottomDataProperty` = IRI("http://www.w3.org/2002/07/owl#bottomDataProperty"),
    `topDataProperty` = IRI("http://www.w3.org/2002/07/owl#topDataProperty"),
    `bottomObjectProperty` = IRI("http://www.w3.org/2002/07/owl#bottomObjectProperty"),
    `topObjectProperty` = IRI("http://www.w3.org/2002/07/owl#topObjectProperty"),
    `imports` = IRI("http://www.w3.org/2002/07/owl#imports"),
    `versionIRI` = IRI("http://www.w3.org/2002/07/owl#versionIRI"),
    `rational` = IRI("http://www.w3.org/2002/07/owl#rational"),
    `real` = IRI("http://www.w3.org/2002/07/owl#real")
  )
}


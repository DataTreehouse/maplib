# Spot-checks for vocab.R's xsd/rdf/rdfs/owl constants (maplib-snz
# continued), cross-checked against oxrdf's actual vocab.rs values when
# they were generated -- this just guards against regressions.

test_that("vocab lists have the expected size and are built at load time", {
  expect_length(xsd, 37)
  expect_length(rdf, 17)
  expect_length(rdfs, 15)
  expect_length(owl, 78)
})

test_that("xsd/rdf/rdfs/owl constants resolve to the correct IRIs", {
  expect_equal(xsd$dateTime@iri, "http://www.w3.org/2001/XMLSchema#dateTime")
  expect_equal(xsd$anyURI@iri, "http://www.w3.org/2001/XMLSchema#anyURI")
  expect_equal(xsd$int_@iri, "http://www.w3.org/2001/XMLSchema#int")
  expect_equal(rdf$type@iri, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
  expect_equal(rdf$langString@iri, "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")
  expect_equal(rdfs$subClassOf@iri, "http://www.w3.org/2000/01/rdf-schema#subClassOf")
  expect_equal(rdfs$Class@iri, "http://www.w3.org/2000/01/rdf-schema#Class")
  expect_equal(owl$Class@iri, "http://www.w3.org/2002/07/owl#Class")
  expect_equal(owl$sameAs@iri, "http://www.w3.org/2002/07/owl#sameAs")
})

test_that("vocab IRIs interoperate with Literal/Parameter", {
  lit <- Literal("42", datatype = xsd$integer)
  expect_equal(format(lit), '"42"^^IRI(http://www.w3.org/2001/XMLSchema#integer)')

  p <- Parameter(Variable("x"), rdf_type = xsd$string)
  expect_true(inherits(p, "maplibr::Parameter"))
})

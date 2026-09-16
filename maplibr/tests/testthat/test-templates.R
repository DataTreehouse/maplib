# Tests for templates.R's OTTR classes (maplib-snz continued). Includes one
# direct port from py_maplib/tests/test_basics.py -- everything else there
# needs Model.map()/query(), not implemented in maplibr yet.

test_that("Template prints identical OTTR syntax to py_maplib", {
  # Direct port of test_programmatic_model_to_string
  # (py_maplib/tests/test_basics.py:538-558).
  ex <- Prefix("http://example.net/ns#", prefix_name = "ex")
  example_template <- Template(
    iri = suf(ex, "ExampleTemplate"),
    parameters = list(),
    instances = list(
      Triple(
        suf(ex, "myObject"),
        suf(ex, "hasObj"),
        suf(ex, "myOtherObject")
      )
    )
  )
  expect_identical(
    trimws(format(example_template)),
    trimws(
      "@prefix ottr: <http://ns.ottr.xyz/0.4/> .
@prefix p0: <http://example.net/ns#> .

p0:ExampleTemplate [ ] :: {
  ottr:Triple(p0:myObject, p0:hasObj, p0:myOtherObject)
} . "
    )
  )
})

test_that("Parameter accepts an IRI or an RDFType for rdf_type", {
  p1 <- Parameter(Variable("x"), rdf_type = xsd$integer)
  expect_true(inherits(p1, "maplibr::Parameter"))

  p2 <- Parameter(Variable("y"), rdf_type = rdf_type_iri())
  expect_true(inherits(p2, "maplibr::Parameter"))

  p3 <- Parameter(Variable("z"), rdf_type = rdf_type_unknown())
  expect_true(inherits(p3, "maplibr::Parameter"))
})

test_that("rdf_type_multi() collapses to rdfs:Resource", {
  multi <- rdf_type_multi(list(rdf_type_iri(), rdf_type_literal(xsd$string)))
  expect_equal(multi@basic_type_iri@iri, "http://www.w3.org/2000/01/rdf-schema#Resource")
})

test_that("Template$instantiate() builds an Instance calling the template", {
  ex <- Prefix("http://example.org/", "ex")
  t <- Template(
    iri = suf(ex, "T"),
    parameters = list(Parameter(Variable("x"))),
    instances = list(Instance(IRI("http://ns.ottr.xyz/0.4/Triple"), list(Variable("x"), suf(ex, "p"), suf(ex, "o"))))
  )
  inst <- instantiate(t, list(suf(ex, "a")))
  expect_true(inherits(inst, "maplibr::Instance"))
  expect_equal(instance_template_iri(inst)@iri, "http://example.org/T")
})

test_that("Argument passthrough: passing an existing Argument returns it unchanged", {
  a <- Argument(Variable("x"), list_expand = TRUE)
  a2 <- Argument(a)
  expect_identical(a, a2)
})

test_that("Instance() no longer accepts a validation-bypassing .raw argument", {
  # Regression test: .raw used to be a real, exported, documented parameter
  # of Instance()'s own public constructor -- a validation-bypassing escape
  # hatch, needed internally by instantiate()/Triple() to wrap an
  # already-built RInstance, but with no business being publicly callable.
  # It's now a private, unexported subclass (.RawInstance) instead.
  expect_error(Instance(.raw = "anything"))
  expect_false(".raw" %in% names(formals(Instance)))
})

test_that("Triple() (via the private RawInstance subclass) builds a valid, checkable Instance", {
  ex <- Prefix("http://example.org/", "ex")
  inst <- Triple(suf(ex, "s"), suf(ex, "p"), suf(ex, "o"))
  expect_true(S7::S7_inherits(inst, Instance))
  expect_true(inherits(inst, "maplibr::Instance")) # base inherits() too, not just S7_inherits()
  expect_equal(instance_template_iri(inst)@iri, "http://ns.ottr.xyz/0.4/Triple")
})

# Ports test_rdfs_inference.py (py_maplib/tests/test_rdfs_inference.py)
# against the same fixture data (tests/testthat/testdata/rdfs/). An earlier
# version of this file judged this unportable without writes() gaining an
# include_transient option -- that turned out to be a misdiagnosis: the
# py_maplib test itself only ever uses query(), which already supports
# include_transient (maplib-l2j's query() port). Now genuinely portable.

read_fixture <- function(path) paste(readLines(path, warn = FALSE), collapse = "\n")

rdfs_folders <- file.path(
  test_path("testdata", "rdfs"),
  c("rdfs2", "rdfs3", "rdfs5", "rdfs6", "rdfs8", "rdfs9", "rdfs10", "rdfs11")
)

for (dir in rdfs_folders) {
  test_that(paste0("infer_rdfs() matches the expected fixture (", basename(dir), ")"), {
    m_input <- Model$new()
    m_input$reads(read_fixture(file.path(dir, "input.ttl")), format = "turtle")
    m_input$infer_rdfs()

    m_expected <- Model$new()
    m_expected$reads(read_fixture(file.path(dir, "expected.ttl")), format = "turtle")

    order_rows <- function(df) df[do.call(order, df), , drop = FALSE]
    df_inferred <- order_rows(m_input$query(
      "SELECT * WHERE { ?a ?b ?c }",
      include_transient = TRUE
    ))
    df_expected <- order_rows(m_expected$query("SELECT * WHERE { ?a ?b ?c }"))

    rownames(df_inferred) <- NULL
    rownames(df_expected) <- NULL
    attr(df_inferred, "rdf_node_types") <- NULL
    attr(df_expected, "rdf_node_types") <- NULL
    expect_equal(df_inferred, df_expected)
  })
}

# NOTE: writes() still can't serialize transient/inferred triples at all --
# Triplestore::write_triples (core engine, not this package) has no
# include_transient option, unlike the query path. That's a core-engine gap,
# not maplibr's to fix; flagged for upstream instead of worked around here.

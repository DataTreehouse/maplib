import polars as pl
from maplib import Model, Variable, Parameter, Triple, Argument, a, RDFType, Template, IRI, xsd
from rdflib import Graph, URIRef

def test_issue_48_bad_ints():
    template = Template(
        iri=IRI("https://example.com/template/Test"),
        parameters=[
            Parameter(variable=Variable("id"), rdf_type=RDFType.IRI),
            Parameter(variable=Variable("kind"), rdf_type=RDFType.Literal(xsd.string)),
            Parameter(
                variable=Variable("term_years"),
                rdf_type=RDFType.Literal(xsd.integer),
                optional=True,
            ),
            Parameter(
                variable=Variable("approvals"),
                rdf_type=RDFType.Literal(xsd.string),
                optional=True,
            ),
            Parameter(
                variable=Variable("restrictions"),
                rdf_type=RDFType.Literal(xsd.string),
                optional=True,
            ),
        ],
        instances=[
            Triple(Argument(term=Variable("id")), a, IRI("https://example.com/Test")),
            Triple(
                Argument(term=Variable("id")),
                IRI("https://example.com/kind"),
                Argument(term=Variable("kind")),
            ),
            Triple(
                Argument(term=Variable("id")),
                IRI("https://example.com/termYears"),
                Argument(term=Variable("term_years")),
            ),
            Triple(
                Argument(term=Variable("id")),
                IRI("https://example.com/approvals"),
                Argument(term=Variable("approvals")),
                list_expander="cross",
            ),
            Triple(
                Argument(term=Variable("id")),
                IRI("https://example.com/restrictions"),
                Argument(term=Variable("restrictions")),
                list_expander="cross",
            ),
        ],
    )

    rows = [
        {
            "id": "http://example.com/t/1",
            "kind": "lpa",
            "term_years": 6,
        },
        {
            "id": "http://example.com/t/1",
            "kind": "lpa",
            "term_years": 6,
            "approvals": ["lpac", "investors"],
        },
        {
            "id": "http://example.com/t/1",
            "kind": "lpa",
            "term_years": 6,
            "restrictions": ["crypto", "public-equities"],
        },
    ]

    for i, row in enumerate(rows):
        model = Model()
        model.map(template=template, data=pl.DataFrame(row), graph="http://example.com/t/1")
        print(f"--- row {i} turtle ---")
        print(model.writes(format="turtle", graph="http://example.com/t/1"))

    combined = Graph(identifier=URIRef("http://example.com/t/1"))
    for row in rows:
        model = Model()
        model.map(template=template, data=pl.DataFrame(row), graph="http://example.com/t/1")
        combined.parse(
            data=model.writes(format="turtle", graph="http://example.com/t/1"),
            format="turtle",
        )

    objs = list(
        combined.objects(
            URIRef("http://example.com/t/1"),
            URIRef("https://example.com/termYears"),
        )
    )
    assert len(objs) == 1
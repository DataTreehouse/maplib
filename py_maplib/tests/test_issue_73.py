from maplib import Model


def test_issue_73():
    model = Model()
    model.query("SELECT ?o WHERE { <https://a> !<https://p> ?o }")


def test_issue_73_insert_data():
    model = Model()
    model.update(
        'INSERT DATA { <https://z> <https://p> "z" . <https://z> <https://q> <https://o> }'
    )
    df = model.query("SELECT ?p ?o WHERE { <https://z> ?p ?o } ORDER BY ?p")
    assert df.height == 2
    assert df.get_column("o").to_list() == ['"z"', "<https://o>"]


def test_issue_73_insert_data_named_graph():
    model = Model()
    model.update('INSERT DATA { GRAPH <urn:g> { <https://z> <https://p> "z" } }')
    assert model.query("SELECT * WHERE { ?s ?p ?o }").height == 0
    df = model.query("SELECT * WHERE { ?s ?p ?o }", graph="urn:g")
    assert df.height == 1


def test_issue_73_delete_data():
    model = Model()
    model.update(
        'INSERT DATA { <https://z> <https://p> "z" . <https://z> <https://p> "y" }'
    )
    model.update('DELETE DATA { <https://z> <https://p> "z" }')
    df = model.query("SELECT ?o WHERE { <https://z> <https://p> ?o }")
    assert df.get_column("o").to_list() == ['y']
